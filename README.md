# effect-nats

Native [NATS](https://nats.io) client for [Effect](https://effect.website).

## Usage

```ts
import { NodeRuntime i} from "@effect/platform-node"
import { NatsConnection } from "effect-nats"
import { Effect, Stream, Console } from "effect"

const program = Effect.gen(function* () {
  const nc = yield* NatsConnection.NatsConnection

  // a service: subscriptions are scoped and unsubscribe when the scope closes
  const sub = yield* nc.subscribe("greet.*", { queue: "greeters" })
  yield* Effect.forkScoped(
    Stream.runForEach(sub.messages, (msg) => msg.respond(`hello, ${msg.subject.slice(6)}`)),
  )

  const reply = yield* nc.request("greet.world").pipe(Effect.timeout("2 seconds"))
  yield* Console.log(reply.string())

  yield* nc.publish("events.created", JSON.stringify({ id: 1 }))
  yield* nc.flush
})

program.pipe(
  Effect.scoped,
  Effect.provide(NatsConnection.layer({ servers: "127.0.0.1:4222" })),
  NodeRuntime.runMain,
)
```

- All connection protocol supported (`nats://`, `tls://`, `ws://`, `wss://`)
- All authentication supported (`nkey`, `jwt`, decentralized `creds`, `tls`)

### Connection options

## Development

```sh
bun install
# requires podman or docker. set NATS_SERVER_BIN to use local binary
bun test
bun check
bun lint
```
