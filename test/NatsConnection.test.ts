/**
 * Integration tests against a real `nats-server`. Every test shares one
 * server (one container), so keep them in this file. The only exception is
 * decentralized (JWT) auth: operator mode cannot be combined with the users
 * the other tests need, so those tests start a second server.
 */
import * as NodeServices from "@effect/platform-node/NodeServices"
import * as test from "bun:test"
import * as Deferred from "effect/Deferred"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as ManagedRuntime from "effect/ManagedRuntime"
import * as Redacted from "effect/Redacted"
import * as Stream from "effect/Stream"
import * as NFs from "node:fs"
import * as NatsConnection from "../src/NatsConnection.ts"
import { NatsHeaders } from "../src/NatsHeaders.ts"
import * as TestNatsServer from "../src/testing/TestNatsServer.ts"

const fixture = (name: string) => NFs.readFileSync(new URL(`./fixtures/${name}`, import.meta.url), "utf8")

// generated with the official `nk` tool, see fixtures/README.md
const nkeyUser: { seed: string; publicKey: string } = JSON.parse(fixture("nkey-user.json"))

const serverConfig = `
max_payload: 1MB
authorization {
  users = [
    { user: "app", password: "secret" }
    { nkey: "${nkeyUser.publicKey}", permissions: { publish: { deny: ["nkey.denied"] } } }
    {
      user: "limited", password: "limited"
      permissions: {
        publish: { deny: ["forbidden.>"] }
        subscribe: { deny: ["forbidden.>"] }
      }
    }
  ]
}
no_auth_user: app
`

const runtime = ManagedRuntime.make(
  TestNatsServer.layer({ config: serverConfig, websocket: true }).pipe(Layer.provide(NodeServices.layer)),
)

test.beforeAll(() =>
  runtime.runPromise(Effect.gen(function*() {
    yield* TestNatsServer.TestNatsServer
  })), { timeout: 300_000 })

test.afterAll(() => runtime.dispose(), { timeout: 30_000 })

const connect = (config?: NatsConnection.Config) =>
  Effect.gen(function*() {
    const server = yield* TestNatsServer.TestNatsServer
    return yield* NatsConnection.make({ servers: server.url, ...config })
  })

const collect = <A, E>(stream: Stream.Stream<A, E>, n: number) => Stream.runCollect(Stream.take(stream, n))

test.describe("connect", () => {
  test.it("connects and exposes server info", () =>
    Effect
      .gen(function*() {
        const nc = yield* connect({ name: "info-test" })
        const info = yield* nc.info
        const server = yield* TestNatsServer.TestNatsServer

        test.expect(info.max_payload).toBe(1024 * 1024)
        test.expect(info.headers).toBe(true)
        test.expect(yield* nc.server).toBe(`${server.host}:${server.port}`)
        test.expect(yield* nc.isClosed).toBe(false)
      })
      .pipe(Effect.scoped, runtime.runPromise))

  test.it("fails with NatsConnectionError when no server listens", () =>
    Effect
      .gen(function*() {
        const error = yield* Effect.flip(NatsConnection.make({ servers: "127.0.0.1:1", connectTimeout: "2 seconds" }))

        test.expect(error._tag).toBe("NatsConnectionError")
      })
      .pipe(Effect.scoped, runtime.runPromise))

  test.it("authenticates with an NKey seed", () =>
    Effect
      .gen(function*() {
        const nc = yield* connect({ nkey: nkeyUser.seed })
        // only the NKey user is denied this subject, so this proves who we are
        const error = yield* Effect.flip(nc.request("nkey.denied", "x", { timeout: "5 seconds" }))

        test.expect(error._tag).toBe("NatsPermissionError")
      })
      .pipe(Effect.scoped, runtime.runPromise))

  test.it("fails with NatsAuthorizationError for an unknown NKey", () =>
    Effect
      .gen(function*() {
        // a valid user seed, but not one the server knows
        const otherSeed = fixture("user.creds").match(/^SU[A-Z2-7]+$/m)![0]
        const error = yield* Effect.flip(connect({ nkey: otherSeed }))

        test.expect(error._tag).toBe("NatsAuthorizationError")
      })
      .pipe(Effect.scoped, runtime.runPromise))

  test.it("rejects an invalid NKey seed before connecting", () =>
    Effect
      .gen(function*() {
        const error = yield* Effect.flip(connect({ nkey: nkeyUser.publicKey }))

        test.expect(error._tag).toBe("NatsInvalidArgumentError")
        test.expect(error.message).toContain("not a seed")
      })
      .pipe(Effect.scoped, runtime.runPromise))

  test.it("authenticates with user and password", () =>
    Effect
      .gen(function*() {
        const nc = yield* connect({ user: "app", pass: "secret" })

        yield* nc.flush
      })
      .pipe(Effect.scoped, runtime.runPromise))

  test.it("fails with NatsAuthorizationError on bad credentials", () =>
    Effect
      .gen(function*() {
        const error = yield* Effect.flip(connect({ user: "app", pass: "wrong" }))

        test.expect(error._tag).toBe("NatsAuthorizationError")
      })
      .pipe(Effect.scoped, runtime.runPromise))

  test.it("reads credentials from the server URL", () =>
    Effect
      .gen(function*() {
        const server = yield* TestNatsServer.TestNatsServer
        const nc = yield* NatsConnection.make({ servers: `nats://app:secret@${server.host}:${server.port}` })

        yield* nc.flush
      })
      .pipe(Effect.scoped, runtime.runPromise))

  test.it("rejects invalid config", () =>
    Effect
      .gen(function*() {
        const error = yield* Effect.flip(NatsConnection.make({ servers: "http://localhost:4222" }))

        test.expect(error._tag).toBe("NatsInvalidArgumentError")
      })
      .pipe(Effect.scoped, runtime.runPromise))
})

test.describe("publish and subscribe", () => {
  test.it("delivers string and binary payloads", () =>
    Effect
      .gen(function*() {
        const nc = yield* connect()
        const sub = yield* nc.subscribe("pubsub.basic")
        const received = yield* Effect.forkChild(collect(sub.messages, 3))

        yield* nc.publish("pubsub.basic", "hello")
        yield* nc.publish("pubsub.basic", new Uint8Array([0, 1, 2, 255]))
        yield* nc.publish("pubsub.basic")
        const msgs = yield* Fiber.join(received)

        test.expect(msgs.map((msg) => msg.subject)).toEqual(["pubsub.basic", "pubsub.basic", "pubsub.basic"])
        test.expect(msgs[0].string()).toBe("hello")
        test.expect([...msgs[1].data]).toEqual([0, 1, 2, 255])
        test.expect(msgs[2].data.length).toBe(0)
        test.expect(msgs[0].sid).toBe(sub.sid)
      })
      .pipe(Effect.scoped, runtime.runPromise))

  test.it("delivers large payloads split across many reads", () =>
    Effect
      .gen(function*() {
        const nc = yield* connect()
        const sub = yield* nc.subscribe("pubsub.large")
        const payload = new Uint8Array(900 * 1024)
        for (let i = 0; i < payload.length; i++) payload[i] = i % 251
        const received = yield* Effect.forkChild(collect(sub.messages, 3))

        for (let i = 0; i < 3; i++) yield* nc.publish("pubsub.large", payload)
        const msgs = yield* Fiber.join(received)

        for (const msg of msgs) test.expect(msg.data).toEqual(payload)
      })
      .pipe(Effect.scoped, runtime.runPromise))

  test.it("delivers many messages in order", () =>
    Effect
      .gen(function*() {
        const nc = yield* connect()
        const sub = yield* nc.subscribe("pubsub.many")
        const count = 10_000
        const received = yield* Effect.forkChild(collect(sub.messages, count))

        for (let i = 0; i < count; i++) yield* nc.publish("pubsub.many", String(i))
        const msgs = yield* Fiber.join(received)

        test.expect(msgs.map((msg) => Number(msg.string()))).toEqual(Array.from({ length: count }, (_, i) => i))
        const stats = yield* nc.stats
        test.expect(stats.inMsgs).toBe(count)
        test.expect(stats.outMsgs).toBe(count)
      })
      .pipe(Effect.scoped, runtime.runPromise))

  test.it("matches wildcard subscriptions", () =>
    Effect
      .gen(function*() {
        const nc = yield* connect()
        const single = yield* nc.subscribe("wild.*")
        const full = yield* nc.subscribe("wild.>")
        const singleMsgs = yield* Effect.forkChild(collect(single.messages, 1))
        const fullMsgs = yield* Effect.forkChild(collect(full.messages, 2))

        yield* nc.publish("wild.a.b", "deep")
        yield* nc.publish("wild.a", "shallow")

        test.expect((yield* Fiber.join(singleMsgs)).map((msg) => msg.subject)).toEqual(["wild.a"])
        test.expect((yield* Fiber.join(fullMsgs)).map((msg) => msg.subject)).toEqual(["wild.a.b", "wild.a"])
      })
      .pipe(Effect.scoped, runtime.runPromise))

  test.it("distributes queue group messages to one member each", () =>
    Effect
      .gen(function*() {
        const nc = yield* connect()
        const a = yield* nc.subscribe("queue.work", { queue: "workers" })
        const b = yield* nc.subscribe("queue.work", { queue: "workers" })
        const received = yield* Effect.forkChild(
          collect(
            Stream.merge(
              Stream.map(a.messages, (msg) => ["a", msg.string()] as const),
              Stream.map(b.messages, (msg) => ["b", msg.string()] as const),
            ),
            50,
          ),
        )

        for (let i = 0; i < 50; i++) yield* nc.publish("queue.work", String(i))
        const msgs = yield* Fiber.join(received)

        test.expect(new Set(msgs.map(([, value]) => value)).size).toBe(50)
        test.expect(a.queue).toBe("workers")
      })
      .pipe(Effect.scoped, runtime.runPromise))

  test.it("round-trips headers", () =>
    Effect
      .gen(function*() {
        const nc = yield* connect()
        const sub = yield* nc.subscribe("headers.test")
        const received = yield* Effect.forkChild(collect(sub.messages, 1))
        const headers = new NatsHeaders({ "X-Trace": "abc", "X-Multi": ["1", "2"] })

        yield* nc.publish("headers.test", "with headers", { headers })
        const [msg] = yield* Fiber.join(received)

        test.expect(msg.string()).toBe("with headers")
        test.expect(msg.headers?.get("X-Trace")).toBe("abc")
        test.expect(msg.headers?.getAll("X-Multi")).toEqual(["1", "2"])
      })
      .pipe(Effect.scoped, runtime.runPromise))

  test.it("does not echo own messages with noEcho", () =>
    Effect
      .gen(function*() {
        const nc = yield* connect({ noEcho: true })
        const other = yield* connect()
        const sub = yield* nc.subscribe("echo.test")
        yield* nc.flush

        yield* nc.publish("echo.test", "own")
        yield* nc.flush
        yield* other.publish("echo.test", "other")
        const msgs = yield* collect(sub.messages, 1)

        test.expect(msgs.map((msg) => msg.string())).toEqual(["other"])
      })
      .pipe(Effect.scoped, runtime.runPromise))

  test.it("rejects invalid subjects and oversized payloads", () =>
    Effect
      .gen(function*() {
        const nc = yield* connect()

        const badSubject = yield* Effect.flip(nc.publish("bad subject", "x"))
        const emptySubject = yield* Effect.flip(nc.publish("", "x"))
        const injected = yield* Effect.flip(nc.publish("a\r\nPUB b 1", "x"))
        const badQueue = yield* Effect.flip(nc.subscribe("ok", { queue: "bad queue" }))
        const tooLarge = yield* Effect.flip(nc.publish("big", new Uint8Array(1024 * 1024 + 1)))

        for (const error of [badSubject, emptySubject, injected, badQueue, tooLarge]) {
          test.expect(error._tag).toBe("NatsInvalidArgumentError")
        }
        // the connection is still usable
        yield* nc.flush
      })
      .pipe(Effect.scoped, runtime.runPromise))
})

test.describe("subscriptions", () => {
  test.it("ends the stream after max messages", () =>
    Effect
      .gen(function*() {
        const nc = yield* connect()
        const sub = yield* nc.subscribe("sub.max", { max: 2 })

        for (let i = 0; i < 5; i++) yield* nc.publish("sub.max", String(i))
        const msgs = yield* Stream.runCollect(sub.messages)

        test.expect(msgs.map((msg) => msg.string())).toEqual(["0", "1"])
      })
      .pipe(Effect.scoped, runtime.runPromise))

  test.it("ends the stream on unsubscribe", () =>
    Effect
      .gen(function*() {
        const nc = yield* connect()
        const sub = yield* nc.subscribe("sub.unsubscribe")
        const all = yield* Effect.forkChild(Stream.runCollect(sub.messages))

        yield* nc.publish("sub.unsubscribe", "1")
        yield* nc.flush
        yield* sub.unsubscribe()
        yield* nc.publish("sub.unsubscribe", "2")
        yield* nc.flush

        test.expect((yield* Fiber.join(all)).map((msg) => msg.string())).toEqual(["1"])
      })
      .pipe(Effect.scoped, runtime.runPromise))

  test.it("unsubscribes later with unsubscribe(max)", () =>
    Effect
      .gen(function*() {
        const nc = yield* connect()
        const sub = yield* nc.subscribe("sub.later")

        yield* nc.publish("sub.later", "1")
        yield* nc.flush
        yield* sub.unsubscribe(3)
        for (let i = 2; i <= 5; i++) yield* nc.publish("sub.later", String(i))
        const msgs = yield* Stream.runCollect(sub.messages)

        test.expect(msgs.map((msg) => msg.string())).toEqual(["1", "2", "3"])
      })
      .pipe(Effect.scoped, runtime.runPromise))

  test.it("unsubscribes when the scope closes", () =>
    Effect
      .gen(function*() {
        const nc = yield* connect()

        yield* Effect.scoped(
          Effect.gen(function*() {
            const sub = yield* nc.subscribe("sub.scoped")
            yield* Effect.forkScoped(Stream.runForEach(sub.messages, (msg) => msg.respond("pong")))
            const reply = yield* nc.request("sub.scoped")
            test.expect(reply.string()).toBe("pong")
          }),
        )
        yield* nc.flush
        const error = yield* Effect.flip(nc.request("sub.scoped"))

        test.expect(error._tag).toBe("NatsNoRespondersError")
      })
      .pipe(Effect.scoped, runtime.runPromise))

  test.it("drain delivers received messages, then ends", () =>
    Effect
      .gen(function*() {
        const nc = yield* connect()
        const sub = yield* nc.subscribe("sub.drain")

        for (let i = 0; i < 3; i++) yield* nc.publish("sub.drain", String(i))
        yield* sub.drain
        yield* nc.publish("sub.drain", "after")
        yield* nc.flush
        const msgs = yield* Stream.runCollect(sub.messages)

        test.expect(msgs.map((msg) => msg.string())).toEqual(["0", "1", "2"])
      })
      .pipe(Effect.scoped, runtime.runPromise))

  test.it("fails the stream when subscribe permission is denied", () =>
    Effect
      .gen(function*() {
        const nc = yield* connect({ user: "limited", pass: "limited" })
        const sub = yield* nc.subscribe("forbidden.sub")
        const error = yield* Effect.flip(Stream.runDrain(sub.messages))

        test.expect(error._tag).toBe("NatsPermissionError")
        if (error._tag === "NatsPermissionError") {
          test.expect(error.operation).toBe("subscription")
          test.expect(error.subject).toBe("forbidden.sub")
        }
        yield* nc.flush
      })
      .pipe(Effect.scoped, runtime.runPromise))
})

test.describe("request", () => {
  const responder = (nc: NatsConnection.NatsConnection, subject: string, queue?: string) =>
    Effect.gen(function*() {
      const sub = yield* nc.subscribe(subject, { queue })
      yield* Effect.forkScoped(
        Stream.runForEach(sub.messages, (msg) =>
          msg.respond(`echo:${msg.string()}`, {
            headers: msg.headers === undefined ? undefined : new NatsHeaders({ "X-Echo": msg.headers.get("X-In")! }),
          })),
      )
      yield* nc.flush
    })

  test.it("gets a reply from a responder on another connection", () =>
    Effect
      .gen(function*() {
        const service = yield* connect()
        const nc = yield* connect()
        yield* responder(service, "req.echo")

        const replies = yield* Effect.all(
          Array.from({ length: 20 }, (_, i) => nc.request("req.echo", String(i))),
          { concurrency: "unbounded" },
        )

        test.expect(replies.map((msg) => msg.string())).toEqual(Array.from({ length: 20 }, (_, i) => `echo:${i}`))
      })
      .pipe(Effect.scoped, runtime.runPromise))

  test.it("sends and receives headers", () =>
    Effect
      .gen(function*() {
        const nc = yield* connect()
        yield* responder(nc, "req.headers")

        const reply = yield* nc.request("req.headers", "x", { headers: new NatsHeaders({ "X-In": "42" }) })

        test.expect(reply.headers?.get("X-Echo")).toBe("42")
      })
      .pipe(Effect.scoped, runtime.runPromise))

  test.it("fails with NatsNoRespondersError without subscribers", () =>
    Effect
      .gen(function*() {
        const nc = yield* connect()
        const error = yield* Effect.flip(nc.request("req.nobody", "x"))

        test.expect(error._tag).toBe("NatsNoRespondersError")
      })
      .pipe(Effect.scoped, runtime.runPromise))

  test.it("fails with NatsTimeoutError when nobody answers", () =>
    Effect
      .gen(function*() {
        const nc = yield* connect()
        const sub = yield* nc.subscribe("req.silent")
        yield* Effect.forkScoped(Stream.runDrain(sub.messages))
        const error = yield* Effect.flip(nc.request("req.silent", "x", { timeout: "100 millis" }))

        test.expect(error._tag).toBe("NatsTimeoutError")
      })
      .pipe(Effect.scoped, runtime.runPromise))

  test.it("fails with NatsPermissionError when publish permission is denied", () =>
    Effect
      .gen(function*() {
        const nc = yield* connect({ user: "limited", pass: "limited" })
        const error = yield* Effect.flip(nc.request("forbidden.req", "x", { timeout: "5 seconds" }))

        test.expect(error._tag).toBe("NatsPermissionError")
      })
      .pipe(Effect.scoped, runtime.runPromise))

  test.it("requestMany collects up to maxMessages", () =>
    Effect
      .gen(function*() {
        const nc = yield* connect()
        for (let i = 0; i < 3; i++) yield* responder(nc, "req.many")

        const replies = yield* Stream.runCollect(
          nc.requestMany("req.many", "x", { maxMessages: 3, maxWait: "5 seconds" }),
        )

        test.expect(replies.map((msg) => msg.string())).toEqual(["echo:x", "echo:x", "echo:x"])
      })
      .pipe(Effect.scoped, runtime.runPromise))

  test.it("requestMany stops after maxWait", () =>
    Effect
      .gen(function*() {
        const nc = yield* connect()
        for (let i = 0; i < 2; i++) yield* responder(nc, "req.wait")

        const replies = yield* Stream.runCollect(nc.requestMany("req.wait", "x", { maxWait: "300 millis" }))

        test.expect(replies.length).toBe(2)
      })
      .pipe(Effect.scoped, runtime.runPromise))

  test.it("requestMany stops at a sentinel", () =>
    Effect
      .gen(function*() {
        const nc = yield* connect()
        const sub = yield* nc.subscribe("req.sentinel")
        yield* Effect.forkScoped(
          Stream.runForEach(sub.messages, (msg) =>
            Effect.andThen(
              Effect.all([msg.respond("a"), msg.respond("b")]),
              msg.respond(),
            )),
        )

        const replies = yield* Stream.runCollect(
          nc.requestMany("req.sentinel", "x", { sentinel: true, maxWait: "5 seconds" }),
        )

        test.expect(replies.map((msg) => msg.string())).toEqual(["a", "b"])
      })
      .pipe(Effect.scoped, runtime.runPromise))

  test.it("requestMany fails with NatsNoRespondersError", () =>
    Effect
      .gen(function*() {
        const nc = yield* connect()
        const error = yield* Effect.flip(Stream.runDrain(nc.requestMany("req.many.nobody", "x")))

        test.expect(error._tag).toBe("NatsNoRespondersError")
      })
      .pipe(Effect.scoped, runtime.runPromise))
})

test.describe("websocket", () => {
  const connectWebSocket = (config?: NatsConnection.Config) =>
    Effect.gen(function*() {
      const server = yield* TestNatsServer.TestNatsServer
      return yield* NatsConnection.make({ servers: server.webSocketUrl!, ...config })
    })

  test.it("connects over WebSocket", () =>
    Effect
      .gen(function*() {
        const server = yield* TestNatsServer.TestNatsServer
        const nc = yield* connectWebSocket()

        yield* nc.flush
        test.expect(yield* nc.server).toBe(server.webSocketUrl!.slice("ws://".length))
        test.expect((yield* nc.info).headers).toBe(true)
      })
      .pipe(Effect.scoped, runtime.runPromise))

  test.it("exchanges messages with TCP clients", () =>
    Effect
      .gen(function*() {
        const ws = yield* connectWebSocket()
        const tcp = yield* connect()
        const fromTcp = yield* ws.subscribe("ws.from-tcp")
        const fromWs = yield* tcp.subscribe("ws.from-ws")
        yield* Effect.all([ws.flush, tcp.flush])
        const received = yield* Effect.forkChild(
          Effect.all([collect(fromTcp.messages, 1), collect(fromWs.messages, 1)]),
        )

        yield* tcp.publish("ws.from-tcp", "hello ws", { headers: new NatsHeaders({ "X-Via": "tcp" }) })
        yield* ws.publish("ws.from-ws", "hello tcp")
        const [[toWs], [toTcp]] = yield* Fiber.join(received)

        test.expect(toWs.string()).toBe("hello ws")
        test.expect(toWs.headers?.get("X-Via")).toBe("tcp")
        test.expect(toTcp.string()).toBe("hello tcp")
      })
      .pipe(Effect.scoped, runtime.runPromise))

  test.it("serves requests and large payloads", () =>
    Effect
      .gen(function*() {
        const nc = yield* connectWebSocket()
        const sub = yield* nc.subscribe("ws.echo")
        yield* Effect.forkScoped(Stream.runForEach(sub.messages, (msg) => msg.respond(msg.data)))
        const payload = new Uint8Array(900 * 1024)
        for (let i = 0; i < payload.length; i++) payload[i] = i % 253

        const reply = yield* nc.request("ws.echo", payload, { timeout: "5 seconds" })
        const noResponders = yield* Effect.flip(nc.request("ws.nobody"))

        test.expect(reply.data).toEqual(payload)
        test.expect(noResponders._tag).toBe("NatsNoRespondersError")
      })
      .pipe(Effect.scoped, runtime.runPromise))

  test.it("authenticates with credentials in the URL", () =>
    Effect
      .gen(function*() {
        const server = yield* TestNatsServer.TestNatsServer
        const url = new URL(server.webSocketUrl!)
        url.username = "app"
        url.password = "wrong"
        const error = yield* Effect.flip(NatsConnection.make({ servers: url.href }))

        test.expect(error._tag).toBe("NatsAuthorizationError")
      })
      .pipe(Effect.scoped, runtime.runPromise))

  test.it("fails with NatsConnectionError when nothing listens", () =>
    Effect
      .gen(function*() {
        const error = yield* Effect.flip(
          NatsConnection.make({ servers: "ws://127.0.0.1:1", connectTimeout: "2 seconds" }),
        )

        test.expect(error._tag).toBe("NatsConnectionError")
      })
      .pipe(Effect.scoped, runtime.runPromise))
})

test.describe("decentralized auth", () => {
  // operator, account and users generated with `nsc`, see fixtures/README.md
  const operatorRuntime = ManagedRuntime.make(
    TestNatsServer.layer({ config: fixture("operator.conf") }).pipe(Layer.provide(NodeServices.layer)),
  )

  test.beforeAll(() =>
    operatorRuntime.runPromise(Effect.gen(function*() {
      yield* TestNatsServer.TestNatsServer
    })), { timeout: 300_000 })

  test.afterAll(() => operatorRuntime.dispose(), { timeout: 30_000 })

  const connectOperator = (config?: NatsConnection.Config) =>
    Effect.gen(function*() {
      const server = yield* TestNatsServer.TestNatsServer
      return yield* NatsConnection.make({ servers: server.url, ...config })
    })

  const userCreds = fixture("user.creds")
  const limitedCreds = fixture("limited.creds")
  const credsPart = (creds: string, index: number) => [...creds.matchAll(/-{3,}[^\n]*-{3,}\n(\S+)\n/g)][index][1]

  test.it("connects with .creds contents", () =>
    Effect
      .gen(function*() {
        const nc = yield* connectOperator({ creds: Redacted.make(userCreds) })
        const sub = yield* nc.subscribe("jwt.echo")
        yield* Effect.forkScoped(Stream.runForEach(sub.messages, (msg) => msg.respond(`echo:${msg.string()}`)))

        const reply = yield* nc.request("jwt.echo", "hi")

        test.expect(reply.string()).toBe("echo:hi")
      })
      .pipe(Effect.scoped, operatorRuntime.runPromise))

  test.it("connects with a JWT and its NKey seed", () =>
    Effect
      .gen(function*() {
        const nc = yield* connectOperator({ jwt: credsPart(userCreds, 0), nkey: credsPart(userCreds, 1) })

        yield* nc.flush
      })
      .pipe(Effect.scoped, operatorRuntime.runPromise))

  test.it("applies permissions from the user JWT", () =>
    Effect
      .gen(function*() {
        const nc = yield* connectOperator({ creds: limitedCreds })
        const error = yield* Effect.flip(nc.request("forbidden.jwt", "x", { timeout: "5 seconds" }))

        test.expect(error._tag).toBe("NatsPermissionError")
      })
      .pipe(Effect.scoped, operatorRuntime.runPromise))

  test.it("fails with NatsAuthorizationError when the seed does not match the JWT", () =>
    Effect
      .gen(function*() {
        const error = yield* Effect.flip(
          connectOperator({ jwt: credsPart(userCreds, 0), nkey: credsPart(limitedCreds, 1) }),
        )

        test.expect(error._tag).toBe("NatsAuthorizationError")
      })
      .pipe(Effect.scoped, operatorRuntime.runPromise))

  test.it("fails with NatsAuthorizationError without credentials", () =>
    Effect
      .gen(function*() {
        const error = yield* Effect.flip(connectOperator())

        test.expect(error._tag).toBe("NatsAuthorizationError")
      })
      .pipe(Effect.scoped, operatorRuntime.runPromise))

  test.it("rejects creds combined with nkey or jwt", () =>
    Effect
      .gen(function*() {
        const error = yield* Effect.flip(connectOperator({ creds: userCreds, nkey: credsPart(userCreds, 1) }))

        test.expect(error._tag).toBe("NatsInvalidArgumentError")
      })
      .pipe(Effect.scoped, operatorRuntime.runPromise))
})

test.describe("lifecycle", () => {
  test.it("flush and rtt round-trip to the server", () =>
    Effect
      .gen(function*() {
        const nc = yield* connect()

        yield* nc.flush
        const rtt = yield* nc.rtt

        test.expect(Duration.toMillis(rtt)).toBeGreaterThan(0)
      })
      .pipe(Effect.scoped, runtime.runPromise))

  test.it("drain flushes subscriptions and closes", () =>
    Effect
      .gen(function*() {
        const nc = yield* connect()
        const sub = yield* nc.subscribe("life.drain")
        const status = yield* Effect.forkChild(Stream.runCollect(nc.status))
        yield* Effect.yieldNow

        yield* nc.publish("life.drain", "1")
        yield* nc.publish("life.drain", "2")
        yield* nc.drain
        yield* nc.closed

        test.expect((yield* Stream.runCollect(sub.messages)).map((msg) => msg.string())).toEqual(["1", "2"])
        test.expect(yield* nc.isClosed).toBe(true)
        test.expect((yield* Effect.flip(nc.publish("life.drain", "3")))._tag).toBe("NatsClosedError")
        test.expect((yield* Fiber.join(status)).map((s) => s._tag)).toEqual(["Close"])
      })
      .pipe(Effect.scoped, runtime.runPromise))

  test.it("close ends subscriptions and fails pending requests", () =>
    Effect
      .gen(function*() {
        const nc = yield* connect()
        const sub = yield* nc.subscribe("life.close")
        yield* Effect.forkScoped(Stream.runDrain((yield* nc.subscribe("life.pending")).messages))
        const messages = yield* Effect.forkChild(Stream.runCollect(sub.messages))
        const pending = yield* Effect.forkChild(
          Effect.flip(nc.request("life.pending", "x", { timeout: "10 seconds" })),
        )
        yield* nc.flush

        yield* nc.close

        test.expect((yield* Fiber.join(messages)).length).toBe(0)
        test.expect((yield* Fiber.join(pending))._tag).toBe("NatsClosedError")
        yield* nc.closed
      })
      .pipe(Effect.scoped, runtime.runPromise))

  // Restarts the shared server, so it runs last.
  test.it("reconnects, resubscribes and replays buffered publishes", () =>
    Effect
      .gen(function*() {
        const server = yield* TestNatsServer.TestNatsServer
        const nc = yield* connect({ reconnectTimeWait: "100 millis", maxReconnectAttempts: -1 })
        const sub = yield* nc.subscribe("life.reconnect")
        const disconnected = yield* Deferred.make<void>()
        const reconnected = yield* Deferred.make<void>()
        yield* Effect.forkScoped(
          Stream.runForEach(nc.status, (status) =>
            status._tag === "Disconnect"
              ? Deferred.succeed(disconnected, undefined)
              : status._tag === "Reconnect"
              ? Deferred.succeed(reconnected, undefined)
              : Effect.void),
        )
        const received = yield* Effect.forkChild(collect(sub.messages, 2))
        yield* Effect.yieldNow

        yield* nc.publish("life.reconnect", "before")
        yield* nc.flush
        yield* server.stop
        yield* Deferred.await(disconnected)
        yield* nc.publish("life.reconnect", "while disconnected")
        const flushed = yield* Effect.forkChild(nc.flush)
        yield* server.start
        yield* Deferred.await(reconnected)
        yield* Fiber.join(flushed)

        test.expect((yield* Fiber.join(received)).map((msg) => msg.string())).toEqual(["before", "while disconnected"])
        test.expect(yield* nc.isClosed).toBe(false)
      })
      .pipe(Effect.scoped, runtime.runPromise), 60_000)
})
