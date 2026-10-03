# Environment

Use Bun as package manager and test runner. The library itself only depends on
`effect` and Node built-ins (`node:net`, `node:tls`), so it runs on Node.

```
bun install
# type check
bun run typecheck
```

# Code

- Do NOT use section header comments (like `// ---`)
- Do not write obvious comments that restate what the code is doing
  without adding meaningful context.
- Inline logic that is 1-2 lines instead of defining a function that is only referenced once.
- Avoid const deconstruction.

## Module structure

- Module designed to be used should have no side-effects.
- Use named exports over default export.
- Private, or internal, modules live in an `internal/` subdirectory next to the public modules that use them, like `src/internal/NatsProtocol.ts`.

## Import rules

```ts
// when importing Node modules name them like such:
import * as NNet from "node:net"
// always import test module as namespace:
import * as test from "bun:test"
// import Effect sub-modules directly
import * as Stream from "effect/Stream"
// Always use extension in local module imports:
import * as Nuid from "./Nuid.ts"
```

# Modules

- Export smallest possible surface to make the module useful.
- Do NOT export types, functions, or symbols that are not used outside of the module.

# Architecture

Modeled after `@effect/sql-pg` (a native protocol client in the Effect repo):

- `src/internal/NatsProtocol.ts` is a pure wire codec: bytes in, plain data
  out. No sockets, no session state. Parsed payloads and encoded frames are
  views into pooled buffers that are never rewritten.
- `src/NatsConnection.ts` drives a Node `Duplex` directly for TCP/TLS,
  decodes frames synchronously in the `data` handler, and completes waiters
  with `Deferred.doneUnsafe` / `Queue.offerUnsafe`. Writes from the same tick
  are batched into one `socket.write`.
- WebSockets use `effect/socket` (`Socket.makeWebSocket`), adapted to a
  `Duplex` by `src/internal/WebSocketDuplex.ts` so both transports share the
  session code.
- The library only handles connections and protocol. Everything for running a
  `nats-server` lives in `src/testing/` (`TestNatsServer`, `internal/Docker.ts`).
- Read environment variables through `effect/Config`, never `process.env`.

# Effect

The Effect source code ships in `./node_modules/effect/src` for reference.
Use this to explore APIs, find usage examples, and understand implementation
details when the documentation isn't enough.

## Running tests

Integration tests need a `nats-server`, started by `TestNatsServer.layer`. By
default one container is started with docker or podman (whichever responds;
force one with `CONTAINER_RUNTIME=podman`). Set
`NATS_SERVER_BIN=/path/to/nats-server` to run a local binary instead.

```sh
# run all tests
bun test

# run specific test
bun test NatsProtocol.test.ts
```

## Writing tests

- All tests that need a server go in `test/NatsConnection.test.ts`, so a run
  starts exactly one container. Tests that restart the server go last.
- Codec tests in `test/NatsProtocol.test.ts` need no server.

```ts
// when test runs effect, wrap the entire body in Effect.gen/pipe
test.it("does something", () =>
  Effect.gen(function*() {
    const nc = yield* connect()

    test.expect(yield* nc.isClosed).toBe(false)
  }).pipe(Effect.scoped, runtime.runPromise))
```
