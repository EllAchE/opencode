export * as WebSearchString from "./string.js"

import { define } from "@opencode/plugin/effect/plugin"
import { Effect, Option, Schema, Stream } from "effect"
import { Mcp } from "../../mcp/index.js"

export const endpoint = "https://mcp.usestring.ai/v1/mcp"
export const server = "string-web-access"

const SearchOutput = Schema.Struct({
  results: Schema.Array(
    Schema.Struct({
      url: Schema.String,
      title: Schema.String,
      snippet: Schema.String,
      timestamp: Schema.String.pipe(Schema.optional),
    }),
  ),
})
const decodeOutput = Schema.decodeUnknownOption(SearchOutput)
const decodeTextOutput = Schema.decodeUnknownOption(Schema.fromJsonString(SearchOutput))

export const Plugin = define({
  id: "opencode.websearch.string",
  effect: Effect.fn("WebSearchString.Plugin")(function* (ctx) {
    const mcp = yield* Mcp.Service
    const managed = { value: false }
    yield* ctx.mcp.transform((editor) => {
      const current = editor.get(server)
      if (current) {
        managed.value = current.type === "remote" && current.url === endpoint
        return
      }
      managed.value = true
      editor.set(server, { type: "remote", url: endpoint })
    })

    const info = (yield* mcp.servers()).find((item) => item.name === server)
    const integrationID = info?.integrationID
    if (managed.value && integrationID) {
      yield* ctx.integration.transform((editor) => {
        editor.update(integrationID, (integration) => (integration.name = "String Web Access"))
      })
    }

    const state = { connected: false }
    const refresh = mcp.servers().pipe(
      Effect.map(
        (servers) =>
          managed.value && servers.some((item) => item.name === server && item.status.status === "connected"),
      ),
      Effect.tap((connected) => Effect.sync(() => (state.connected = connected))),
      Effect.andThen(ctx.websearch.reload()),
    )
    yield* ctx.event.subscribe().pipe(
      Stream.filter((event) => event.type === "mcp.status.changed" && event.data.server === server),
      Stream.runForEach(() => refresh),
      Effect.forkScoped({ startImmediately: true }),
    )
    state.connected = managed.value && info?.status.status === "connected"

    yield* ctx.websearch.transform((editor) => {
      if (!state.connected) return
      editor.add({
        id: "string",
        name: "String Web Access",
        execute: (input) =>
          Effect.gen(function* () {
            const result = yield* mcp.callTool({
              server,
              name: "web_access_search",
              args: { query: input.query, searchCount: 8 },
            })
            if (result.isError) {
              const message = result.content
                .flatMap((item) => (item.type === "text" ? [item.text.trim()] : []))
                .find(Boolean)
              return yield* Effect.fail(new Error(message || "String Web Access search failed"))
            }
            const output = parseOutput(result)
            return (
              output?.results.map((item) => {
                const published = item.timestamp ? Date.parse(item.timestamp) : Number.NaN
                return {
                  url: item.url,
                  title: item.title,
                  ...(item.snippet ? { content: item.snippet } : {}),
                  time: Number.isFinite(published) ? { published } : {},
                }
              }) ?? []
            )
          }),
      })
    })
  }),
})

function parseOutput(result: Mcp.ToolResult) {
  const structured = Option.getOrUndefined(decodeOutput(result.structured))
  if (structured) return structured
  return result.content.flatMap((item) => {
    if (item.type !== "text") return []
    const decoded = decodeTextOutput(item.text)
    return Option.isSome(decoded) ? [decoded.value] : []
  })[0]
}
