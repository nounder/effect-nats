/**
 * Read-path benchmark: raw `node:net` `data` callbacks vs `effect/socket`.
 *
 * A separate `node` process serves pre-encoded NATS `MSG` frames over
 * loopback TCP as fast as the socket accepts them. The client decodes them
 * with the real `NatsProtocol` parser and counts messages, so only the way
 * bytes get from the socket to the parser differs between variants.
 *
 *   node bench/socket-read.ts
 *   bun bench/socket-read.ts
 */
import * as NodeSocket from "@effect/platform-node/NodeSocket"
import * as Effect from "effect/Effect"
import * as Socket from "effect/socket/Socket"
import * as Stream from "effect/Stream"
import * as NChild from "node:child_process"
import * as NNet from "node:net"
import * as NatsProtocol from "../src/internal/NatsProtocol.ts"

const HOST = "127.0.0.1"
const TARGET_BYTES = 100 * 1024 * 1024
const RUNS = 7
const PAYLOADS = [16, 128, 1024, 16 * 1024]
const SUBJECT = "bench.subject"

const frameSize = (payload: number) => `MSG ${SUBJECT} 1 ${payload}\r\n`.length + payload + 2

const serve = () => {
  const server = NNet.createServer((socket) => {
    socket.setNoDelay(true)
    socket.once("data", (request) => {
      const [payload, count] = request.toString().trim().split(" ").map(Number)
      const frame = Buffer.concat([
        Buffer.from(`MSG ${SUBJECT} 1 ${payload}\r\n`),
        Buffer.alloc(payload, 120),
        Buffer.from("\r\n"),
      ])
      const perBlock = Math.max(1, Math.floor(64 * 1024 / frame.length))
      const block = Buffer.concat(Array.from({ length: perBlock }, () => frame))
      let remaining = count
      const pump = () => {
        while (remaining > 0) {
          const n = Math.min(perBlock, remaining)
          remaining -= n
          const ok = socket.write(n === perBlock ? block : block.subarray(0, n * frame.length))
          if (!ok) return void socket.once("drain", pump)
        }
        socket.end()
      }
      pump()
    })
  })
  server.listen(0, HOST, () => process.stdout.write(`${(server.address() as NNet.AddressInfo).port}\n`))
}

interface Result {
  readonly received: number
  readonly ms: number
  readonly cpuMs: number
}

const measure = async (run: () => Promise<number>): Promise<Result> => {
  const cpu = process.cpuUsage()
  const start = performance.now()
  const received = await run()
  const ms = performance.now() - start
  const used = process.cpuUsage(cpu)
  return { received, ms, cpuMs: (used.user + used.system) / 1000 }
}

const request = (payload: number, count: number) => `${payload} ${count}\n`

/** What `NatsConnection` does today: flowing-mode `data` events. */
const raw = (port: number, payload: number, count: number) =>
  new Promise<number>((resolve, reject) => {
    const parser = NatsProtocol.makeParser()
    let received = 0
    const onMessage = (message: NatsProtocol.ServerMessage) => {
      if (message._tag === "Msg") received++
    }
    const socket = NNet.connect({ host: HOST, port, noDelay: true })
    socket.once("connect", () => socket.write(request(payload, count)))
    socket.on("data", (chunk: Uint8Array) => parser.pushEach(chunk, onMessage))
    socket.once("end", () => resolve(received))
    socket.once("error", reject)
  })

/**
 * Control: paused-mode `read()` without Effect, the read style `NodeSocket`
 * uses internally. Separates the cost of paused reads from the cost of Effect.
 */
const rawPaused = (port: number, payload: number, count: number) =>
  new Promise<number>((resolve, reject) => {
    const parser = NatsProtocol.makeParser()
    let received = 0
    const onMessage = (message: NatsProtocol.ServerMessage) => {
      if (message._tag === "Msg") received++
    }
    const socket = NNet.connect({ host: HOST, port, noDelay: true })
    socket.pause()
    socket.once("connect", () => socket.write(request(payload, count)))
    socket.on("readable", () => {
      let chunk: Uint8Array | null
      while ((chunk = socket.read()) !== null) parser.pushEach(chunk, onMessage)
    })
    socket.once("end", () => resolve(received))
    socket.once("error", reject)
  })

const isNormalClose = (error: Socket.SocketError) =>
  error.reason._tag === "SocketCloseError" && error.reason.code === 1000

/** A connection built on `effect/socket`: a fiber looping on `reader.pull`. */
const socketPull = (port: number, payload: number, count: number) =>
  Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const socket = yield* NodeSocket.makeNet({ host: HOST, port, noDelay: true })
    const reader = yield* socket.reader
    const writer = yield* socket.writer
    yield* writer.write(request(payload, count))
    const parser = NatsProtocol.makeParser()
    let received = 0
    const onMessage = (message: NatsProtocol.ServerMessage) => {
      if (message._tag === "Msg") received++
    }
    while (true) {
      const chunks = yield* reader.pull.pipe(
        Effect.catchIf(isNormalClose, () => Effect.succeed(undefined)),
      )
      if (chunks === undefined) return received
      for (const chunk of chunks) parser.pushEach(chunk as Uint8Array, onMessage)
    }
  })))

/** Idiomatic Effect streaming: `Socket.toStream` + `Stream.runForEach`. */
const socketStream = (port: number, payload: number, count: number) =>
  Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const socket = yield* NodeSocket.makeNet({ host: HOST, port, noDelay: true })
    const writer = yield* socket.writer
    // waits for the reader below to open the connection
    yield* Effect.forkScoped(writer.write(request(payload, count)))
    const parser = NatsProtocol.makeParser()
    let received = 0
    const onMessage = (message: NatsProtocol.ServerMessage) => {
      if (message._tag === "Msg") received++
    }
    yield* Socket.toStream(socket).pipe(
      Stream.runForEach((chunk) => Effect.sync(() => parser.pushEach(chunk, onMessage))),
      Effect.catchIf(isNormalClose, () => Effect.void),
    )
    return received
  })))

const variants = [
  { name: "raw `node:net` `data` events", run: raw },
  { name: "raw `node:net` paused `read()` (control)", run: rawPaused },
  { name: "`Socket` `reader.pull` loop", run: socketPull },
  { name: "`Socket.toStream` + `runForEach`", run: socketStream },
] as const

const median = (values: ReadonlyArray<number>) => {
  const sorted = [...values].sort((a, b) => a - b)
  return sorted[Math.floor(sorted.length / 2)]
}

const formatRate = (n: number) =>
  n >= 1e6 ? `${(n / 1e6).toFixed(2)} M` : n >= 1e3 ? `${(n / 1e3).toFixed(0)} k` : n.toFixed(0)

const main = async () => {
  const server = NChild.spawn("node", [process.argv[1], "serve"], { stdio: ["ignore", "pipe", "inherit"] })
  const port = await new Promise<number>((resolve) => server.stdout!.once("data", (data) => resolve(Number(data))))
  const runtime = "Bun" in globalThis ? `Bun ${(globalThis as any).Bun.version}` : `Node ${process.version}`

  console.log(`\n### ${runtime}\n`)
  console.log("| payload | variant | msgs/s | MB/s | CPU ms / 100 MB | vs raw |")
  console.log("| ---: | --- | ---: | ---: | ---: | ---: |")
  for (const payload of PAYLOADS) {
    const count = Math.floor(TARGET_BYTES / frameSize(payload))
    const results = variants.map(() => [] as Array<Result>)
    // warm up every variant, then interleave runs so drift hits all equally
    for (const variant of variants) await variant.run(port, payload, count)
    for (let run = 0; run < RUNS; run++) {
      for (let v = 0; v < variants.length; v++) {
        const result = await measure(() => variants[v].run(port, payload, count))
        if (result.received !== count) throw new Error(`${variants[v].name}: got ${result.received} of ${count}`)
        results[v].push(result)
      }
    }
    const rawRate = count / (median(results[0].map((r) => r.ms)) / 1000)
    variants.forEach((variant, v) => {
      const ms = median(results[v].map((r) => r.ms))
      const cpuMs = median(results[v].map((r) => r.cpuMs))
      const rate = count / (ms / 1000)
      const mbps = (count * frameSize(payload)) / (1024 * 1024) / (ms / 1000)
      console.log(
        `| ${payload} B | ${variant.name} | ${formatRate(rate)} | ${mbps.toFixed(0)} | ${
          (cpuMs * TARGET_BYTES / (count * frameSize(payload))).toFixed(0)
        } | ${v === 0 ? "—" : `${((rate / rawRate - 1) * 100).toFixed(1)}%`} |`,
      )
    })
  }
  server.kill()
}

if (process.argv[2] === "serve") serve()
else main()
