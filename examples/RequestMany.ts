/**
 * `requestMany` streams every reply to one request. It never ends by itself, so bound it with
 * Stream operators instead of options.
 *
 * Run with a nats-server on localhost:4222 (or set NATS_URL):
 *
 * ```sh
 * node examples/RequestMany.ts
 * ```
 */
import * as NodeRuntime from "@effect/platform-node/NodeRuntime"
import { NatsConnection } from "effect-nats"
import * as Config from "effect/Config"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Stream from "effect/Stream"

interface Stock {
  readonly region: string
  readonly units: number
}

const program = Effect.gen(function* () {
  const nc = yield* NatsConnection.NatsConnection

  // Three instances of an inventory service, each answering for its region.
  for (const stock of [
    { region: "eu", units: 12 },
    { region: "us", units: 7 },
    { region: "ap", units: 3 },
  ]) {
    const sub = yield* nc.subscribe("inventory.stock")
    yield* Effect.forkScoped(
      Stream.runForEach(sub.messages, (msg) => msg.respond(JSON.stringify(stock))),
    )
  }

  // A report service that streams its answer in parts and ends with an empty
  // message, the usual "sentinel" for chunked replies.
  const report = yield* nc.subscribe("report.lines")
  yield* Effect.forkScoped(
    Stream.runForEach(report.messages, (msg) =>
      Effect.forEach(["header", "row 1", "row 2", ""], (line) => msg.respond(line)),
    ),
  )
  yield* nc.flush

  // Scatter-gather: take the first 3 replies, but wait at most 500 ms in total.
  const stock = yield* nc.requestMany("inventory.stock").pipe(
    Stream.take(3),
    Stream.interruptWhen(Effect.sleep("500 millis")),
    Stream.map((msg) => msg.json<Stock>()),
    Stream.runCollect,
  )
  const total = stock.reduce((sum, item) => sum + item.units, 0)
  yield* Console.log(`${stock.length} regions answered, ${total} units in stock`)

  // Chunked reply: read until the empty sentinel, and give up if the
  // responder goes quiet for a second.
  const lines = yield* nc.requestMany("report.lines").pipe(
    Stream.takeWhile((msg) => msg.data.length > 0),
    Stream.timeout("1 second"),
    Stream.map((msg) => msg.string()),
    Stream.runCollect,
  )
  yield* Console.log(lines)

  yield* nc.drain
})

const NatsLive = Layer.unwrap(
  Effect.map(Config.String("NATS_URL").pipe(Config.withDefault("127.0.0.1:4222")), (servers) =>
    NatsConnection.layer({ servers, name: "request-many-example" }),
  ),
)

program.pipe(Effect.scoped, Effect.provide(NatsLive), NodeRuntime.runMain)
