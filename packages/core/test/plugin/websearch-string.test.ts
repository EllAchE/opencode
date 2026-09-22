import { describe, expect } from "bun:test"
import { Mcp as McpConfig, Plugin } from "@opencode/plugin/effect"
import { McpEvent } from "@opencode/schema/mcp-event"
import { Bus } from "@opencode/core/bus"
import { Integration } from "@opencode/core/integration"
import { Mcp } from "@opencode/core/mcp/index"
import { WebSearchString } from "@opencode/core/plugin/websearch/string"
import { WebSearch } from "@opencode/core/websearch"
import { Effect, Layer, Types } from "effect"
import { advance } from "../lib/clock"
import { host, integrationHost, webSearchHost } from "./host"
import { webSearchIntegrationTest } from "./websearch-fixture"

const it = webSearchIntegrationTest

describe("String web search", () => {
  it.effect("registers the MCP server and searches after OAuth connects", () =>
    Effect.gen(function* () {
      const integrations = yield* Integration.Service
      const websearch = yield* WebSearch.Service
      const configs = new Map<string, Types.DeepMutable<McpConfig.ServerConfig>>()
      const integrationID = Integration.ID.make("mcp_string")
      let call: Parameters<Mcp.Interface["callTool"]>[0] | undefined
      const mcp = Layer.mock(Mcp.Service, {
        servers: () =>
          Effect.succeed([
            {
              name: WebSearchString.server,
              status: { status: "connected" },
              integrationID,
            },
          ]),
        callTool: (input) =>
          Effect.sync(() => {
            call = input
            return {
              server: Mcp.ServerName.make(input.server),
              tool: input.name,
              isError: false,
              structured: {
                results: [
                  {
                    url: "https://effect.website",
                    title: "Effect",
                    snippet: "Effect documentation",
                    timestamp: "2026-07-25T00:00:00.000Z",
                  },
                ],
              },
              content: [],
            } satisfies Mcp.ToolResult
          }),
      })

      yield* WebSearchString.Plugin.effect(
        host({
          integration: integrationHost(integrations),
          mcp: mcpHost(configs),
          websearch: webSearchHost(websearch),
        }),
      ).pipe(Effect.provide(mcp))

      expect(configs.get(WebSearchString.server)).toEqual({
        type: "remote",
        url: WebSearchString.endpoint,
      })
      expect(yield* integrations.get(integrationID)).toMatchObject({ name: "String Web Access" })
      expect(yield* websearch.providers()).toContainEqual({
        id: WebSearch.ID.make("string"),
        name: "String Web Access",
      })
      expect(yield* websearch.query({ query: "effect typescript", providerID: WebSearch.ID.make("string") })).toEqual(
        new WebSearch.Response({
          providerID: WebSearch.ID.make("string"),
          results: [
            {
              url: "https://effect.website",
              title: "Effect",
              content: "Effect documentation",
              time: { published: Date.parse("2026-07-25T00:00:00.000Z") },
            },
          ],
        }),
      )
      expect(call).toEqual({
        server: WebSearchString.server,
        name: "web_access_search",
        args: { query: "effect typescript", searchCount: 8 },
      })
    }),
  )

  it.effect("keeps String out of provider rotation until OAuth connects", () =>
    Effect.gen(function* () {
      const integrations = yield* Integration.Service
      const websearch = yield* WebSearch.Service
      const configs = new Map<string, Types.DeepMutable<McpConfig.ServerConfig>>()
      const mcp = Layer.mock(Mcp.Service, {
        servers: () =>
          Effect.succeed([
            {
              name: WebSearchString.server,
              status: { status: "needs_auth", error: "Authentication required" },
              integrationID: Integration.ID.make("mcp_string"),
            },
          ]),
      })

      yield* WebSearchString.Plugin.effect(
        host({
          integration: integrationHost(integrations),
          mcp: mcpHost(configs),
          websearch: webSearchHost(websearch),
        }),
      ).pipe(Effect.provide(mcp))

      expect(configs.has(WebSearchString.server)).toBe(true)
      expect(yield* websearch.providers()).not.toContainEqual({
        id: WebSearch.ID.make("string"),
        name: "String Web Access",
      })
    }),
  )

  it.effect("adds String when OAuth connects after startup", () =>
    Effect.gen(function* () {
      const bus = yield* Bus.Service
      const integrations = yield* Integration.Service
      const websearch = yield* WebSearch.Service
      const configs = new Map<string, Types.DeepMutable<McpConfig.ServerConfig>>()
      let connected = false
      let reloads = 0
      const mcp = Layer.mock(Mcp.Service, {
        servers: () =>
          Effect.sync(() => [
            {
              name: WebSearchString.server,
              status: connected
                ? ({ status: "connected" } as const)
                : ({ status: "needs_auth", error: "Authentication required" } as const),
              integrationID: Integration.ID.make("mcp_string"),
            },
          ]),
      })
      const websearchDomain = webSearchHost(websearch)

      yield* WebSearchString.Plugin.effect(
        host({
          event: { subscribe: () => bus.subscribe(McpEvent.StatusChanged) },
          integration: integrationHost(integrations),
          mcp: mcpHost(configs),
          websearch: {
            ...websearchDomain,
            reload: () => websearchDomain.reload().pipe(Effect.tap(() => Effect.sync(() => reloads++))),
          },
        }),
      ).pipe(Effect.provide(mcp))

      expect(yield* websearch.providers()).not.toContainEqual({
        id: WebSearch.ID.make("string"),
        name: "String Web Access",
      })
      connected = true
      yield* bus.publish(McpEvent.StatusChanged, { server: WebSearchString.server })
      yield* advance(() => reloads === 1)
      expect(yield* websearch.providers()).toContainEqual({
        id: WebSearch.ID.make("string"),
        name: "String Web Access",
      })
    }),
  )
})

function mcpHost(configs: Map<string, Types.DeepMutable<McpConfig.ServerConfig>>): Plugin.Context["mcp"] {
  return {
    list: () => Effect.die("unused mcp.list"),
    reload: () => Effect.void,
    transform: (callback) =>
      Effect.sync(() => {
        callback({
          list: () => Array.from(configs.entries()),
          get: (name) => configs.get(name),
          set: (name, config) =>
            configs.set(name, structuredClone(config) as Types.DeepMutable<McpConfig.ServerConfig>),
          update: (name, update) => {
            const config = configs.get(name)
            if (config) update(config)
          },
          remove: (name) => configs.delete(name),
        })
        return { dispose: Effect.void }
      }),
  }
}
