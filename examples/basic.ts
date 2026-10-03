/**
 * Run with a nats-server on localhost:4222 (or set NATS_URL, which may also
 * be a ws:// URL):
 *
 *   node examples/basic.ts
 */
import * as NodeRuntime from "@effect/platform-node/NodeRuntime"
import { NatsConnection } from "effect-nats"
import * as Config from "effect/Config"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Stream from "effect/Stream"

const program = Effect.gen(function*() {
  const nc = yield* NatsConnection.NatsConnection

  const service = yield* nc.subscribe("greet.*", { queue: "greeters" })
  yield* Effect.forkScoped(
    Stream.runForEach(service.messages, (msg) => msg.respond(`hello, ${msg.subject.slice("greet.".length)}`)),
  )

  const reply = yield* nc.request("greet.world", undefined, { timeout: "2 seconds" })
  yield* Console.log(reply.string())

  yield* nc.drain
})

const NatsLive = Layer.unwrap(
  Effect.map(
    Config.String("NATS_URL").pipe(Config.withDefault("127.0.0.1:4222")),
    (servers) => NatsConnection.layer({ servers, name: "example" }),
  ),
)

program.pipe(Effect.scoped, Effect.provide(NatsLive), NodeRuntime.runMain)
