# effect-nats

Native [NATS](https://nats.io) client for [Effect](https://effect.website) 4.

Implements the core NATS surface (the equivalent of
[`@nats-io/nats-core`](https://github.com/nats-io/nats.js/blob/main/core/README.md)):
publish/subscribe, wildcards, queue groups, request/reply, headers,
no-responders, flush/rtt, drain, and automatic reconnect. It speaks the NATS
wire protocol directly over `node:net`/`node:tls`, or over WebSockets
(`ws://`/`wss://`) through `effect/socket`, with no dependency besides `effect`.

This project is in its early stage.

## Usage

```ts
import * as NodeRuntime from "@effect/platform-node/NodeRuntime"
import * as Effect from "effect/Effect"
import * as Stream from "effect/Stream"
import { NatsConnection } from "effect-nats"

const program = Effect.gen(function*() {
  const nc = yield* NatsConnection.NatsConnection

  // a service: subscriptions are scoped and unsubscribe when the scope closes
  const sub = yield* nc.subscribe("greet.*", { queue: "greeters" })
  yield* Effect.forkScoped(
    Stream.runForEach(sub.messages, (msg) => msg.respond(`hello, ${msg.subject.slice(6)}`)),
  )

  const reply = yield* nc.request("greet.world", undefined, { timeout: "2 seconds" })
  console.log(reply.string())

  yield* nc.publish("events.created", JSON.stringify({ id: 1 }))
  yield* nc.flush
})

program.pipe(
  Effect.scoped,
  Effect.provide(NatsConnection.layer({ servers: "127.0.0.1:4222" })),
  NodeRuntime.runMain,
)
```

### API

`NatsConnection` service:

| Member                                             | Description                                                                          |
| -------------------------------------------------- | ------------------------------------------------------------------------------------ |
| `publish(subject, payload?, { reply, headers })`   | Fire-and-forget publish. Waits only when the socket needs to drain.                  |
| `subscribe(subject, { queue, max })`               | Scoped `Subscription` with `messages: Stream<Msg>`, `unsubscribe(max?)`, `drain`.    |
| `request(subject, payload?, { timeout, headers })` | Request/reply over a shared inbox subscription.                                      |
| `requestMany(subject, payload?, options)`          | `Stream` of responses, bounded by `maxWait`, `maxMessages`, `stall`, `sentinel`.     |
| `flush`, `rtt`                                     | Round trip to the server.                                                            |
| `drain`, `close`, `closed`                         | Graceful shutdown, immediate shutdown, and awaiting either.                          |
| `status`                                           | `Stream` of `Disconnect`, `Reconnecting`, `Reconnect`, `Error`, `Close`, ... events. |
| `info`, `server`, `stats`, `reconnect`             | Server `INFO`, current server, byte/message counters, forced reconnect.              |

Errors are tagged (`NatsError.ts`): `NatsConnectionError`,
`NatsAuthorizationError`, `NatsPermissionError`, `NatsTimeoutError`,
`NatsNoRespondersError`, `NatsInvalidArgumentError`, `NatsProtocolError`,
`NatsClosedError`.

### Connection options

`servers` (`nats://`, `tls://`, `ws://`, `wss://` or `host:port`), `name`, `user`/`pass`, `token` (credentials may also be in the URL),
`noEcho`, `tls` (`true` or `node:tls` options, plus `handshakeFirst`),
`connectTimeout`, `pingInterval`, `maxPingOut`, `reconnect`,
`maxReconnectAttempts`, `reconnectTimeWait`, `reconnectJitter`,
`reconnectBufferSize`, `noRandomize`, `ignoreClusterUpdates`, `inboxPrefix`,
and `stream` for a custom `Duplex` transport.

WebSockets use the `Socket.WebSocketConstructor` service when one is
provided (for example `NodeSocket.layerWebSocketConstructor` from
`@effect/platform-node`), otherwise `globalThis.WebSocket`.

Not implemented yet: NKey/JWT/credentials-file authentication, and
JetStream/KV/Services (separate modules in nats.js).

## Testing helpers

`effect-nats/testing` provides `TestNatsServer.layer({ config, websocket })`, which
starts a `nats-server` in a docker or podman container for tests. It reads
`CONTAINER_RUNTIME` (`docker` or `podman`), `NATS_IMAGE`, and `NATS_SERVER_BIN`
(run a local binary instead of a container) through `effect/Config`.

## Development

```sh
bun install
bun test           # starts one nats-server container with docker or podman
NATS_SERVER_BIN=$(which nats-server) bun test   # or use a local binary
bun run typecheck
bun run lint
```
