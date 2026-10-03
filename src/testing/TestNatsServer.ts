/**
 * Runs a `nats-server` for tests, in a docker or podman container.
 *
 * Reads from `Config`:
 *
 * - `NATS_IMAGE`: container image, defaults to `nats:2.12-alpine`. It needs a
 *   shell, so use an alpine variant.
 * - `NATS_SERVER_BIN`: path to a local `nats-server` binary to run as a child
 *   process instead of a container, for machines without a container runtime.
 * - `CONTAINER_RUNTIME`: `docker` or `podman`, otherwise whichever responds.
 */
import * as Config from "effect/Config"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as ChildProcess from "effect/process/ChildProcess"
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner"
import * as Schedule from "effect/Schedule"
import * as Scope from "effect/Scope"
import * as NFs from "node:fs"
import * as NNet from "node:net"
import * as NOs from "node:os"
import * as NPath from "node:path"
import * as Docker from "./internal/Docker.ts"

export {
  DockerError,
} from "./internal/Docker.ts"

export interface Options {
  /**
   * Server configuration file contents. `listen` (and `websocket.listen`) are
   * set by the layer, so leave them out.
   */
  readonly config?: string | undefined
  /** Also open a plain-text WebSocket listener, see `webSocketUrl`. */
  readonly websocket?: boolean | undefined
}

export interface TestNatsServerService {
  readonly host: string
  readonly port: number
  /** `nats://host:port` */
  readonly url: string
  /** `ws://host:port` when started with `websocket: true`. */
  readonly webSocketUrl: string | undefined
  /** Stops the server; clients see the connection drop. */
  readonly stop: Effect.Effect<void, Docker.DockerError>
  /** Starts a stopped server on the same ports and waits until it accepts clients. */
  readonly start: Effect.Effect<void, Docker.DockerError>
}

export class TestNatsServer
  extends Context.Service<TestNatsServer, TestNatsServerService>()("effect-nats/testing/TestNatsServer")
{}

const READY = /Server is ready/

const webSocketBlock = (listen: string) => `\nwebsocket {\n  listen: "${listen}"\n  no_tls: true\n}\n`

const container = (options: Options | undefined) =>
  Effect
    .gen(function*() {
      const docker = yield* Docker.Docker
      const image = yield* Config.String("NATS_IMAGE").pipe(Config.withDefault("nats:2.12-alpine"))
      const config = `listen: 0.0.0.0:4222\n${options?.config ?? ""}${
        options?.websocket ? webSocketBlock("0.0.0.0:8080") : ""
      }`
      const instance = yield* docker.run({
        image,
        ports: options?.websocket ? [4222, 8080] : [4222],
        env: { NATS_CONFIG: config },
        // The config travels in an env var, so no volume has to be shared with
        // the docker or podman VM.
        entrypoint: "sh",
        args: ["-c", `printf '%s' "$NATS_CONFIG" > /tmp/nats.conf && exec nats-server -c /tmp/nats.conf`],
        waitForLog: READY,
      })
      let starts = 1
      const port = instance.port(4222)
      return {
        host: instance.host,
        port,
        url: `nats://${instance.host}:${port}`,
        webSocketUrl: options?.websocket ? `ws://${instance.host}:${instance.port(8080)}` : undefined,
        stop: instance.stop,
        start: Effect.suspend(() => {
          starts++
          return Effect.andThen(instance.start, instance.waitForLog(READY, { count: starts }))
        }),
      } satisfies TestNatsServerService
    })
    .pipe(Effect.provide(Docker.layer))

const waitForPort = (port: number) =>
  Effect
    .callback<void, Docker.DockerError>((resume) => {
      const socket = NNet.connect({ host: "127.0.0.1", port })
      socket.once("data", () => {
        socket.destroy()
        resume(Effect.void)
      })
      socket.once("error", (cause) => resume(Effect.fail(new Docker.DockerError({ message: "not ready", cause }))))
      return Effect.sync(() => socket.destroy())
    })
    .pipe(Effect.retry({ schedule: Schedule.spaced("50 millis"), times: 200 }))

const localProcess = (binary: string, options: Options | undefined) =>
  Effect.gen(function*() {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
    const scope = yield* Effect.scope
    const port = yield* Docker.freePort
    const webSocketPort = options?.websocket ? yield* Docker.freePort : undefined
    const dir = NFs.mkdtempSync(NPath.join(NOs.tmpdir(), "effect-nats-"))
    yield* Effect.addFinalizer(() => Effect.sync(() => NFs.rmSync(dir, { recursive: true, force: true })))
    const configPath = NPath.join(dir, "nats.conf")
    NFs.writeFileSync(
      configPath,
      `listen: 127.0.0.1:${port}\n${options?.config ?? ""}${
        webSocketPort !== undefined ? webSocketBlock(`127.0.0.1:${webSocketPort}`) : ""
      }`,
    )

    let stopCurrent: Effect.Effect<void> = Effect.void
    const start = Effect.gen(function*() {
      const handle = yield* spawner
        .spawn(ChildProcess.make(binary, ["-c", configPath], { stdout: "ignore", stderr: "ignore" }))
        .pipe(
          // the process outlives this `start` call and is killed with the layer
          Scope.provide(scope),
          Effect.mapError((cause) => new Docker.DockerError({ message: `failed to start ${binary}`, cause })),
        )
      stopCurrent = Effect.ignore(handle.kill())
      yield* waitForPort(port)
    })
    yield* start
    return {
      host: "127.0.0.1",
      port,
      url: `nats://127.0.0.1:${port}`,
      webSocketUrl: webSocketPort !== undefined ? `ws://127.0.0.1:${webSocketPort}` : undefined,
      stop: Effect.suspend(() => stopCurrent),
      start,
    } satisfies TestNatsServerService
  })

export const layer = (
  options?: Options,
): Layer.Layer<TestNatsServer, Docker.DockerError | Config.ConfigError, ChildProcessSpawner.ChildProcessSpawner> =>
  Layer.effect(
    TestNatsServer,
    Effect.flatMap(
      Config.option(Config.NonEmptyString("NATS_SERVER_BIN")),
      (binary) => Option.isSome(binary) ? localProcess(binary.value, options) : container(options),
    ),
  )
