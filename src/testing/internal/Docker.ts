/**
 * Minimal container runner for tests. Drives the `docker` or `podman` CLI,
 * whichever is available (or the `CONTAINER_RUNTIME` config), and removes
 * containers when their scope closes.
 */
import * as Config from "effect/Config"
import * as Context from "effect/Context"
import * as Data from "effect/Data"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as ChildProcess from "effect/process/ChildProcess"
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner"
import * as Schedule from "effect/Schedule"
import type * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as NNet from "node:net"

export class DockerError extends Data.TaggedError("DockerError")<{
  readonly message: string
  readonly cause?: unknown
}> {}

export type Runtime = "docker" | "podman"

export interface RunOptions {
  readonly image: string
  readonly name?: string | undefined
  readonly env?: Readonly<Record<string, string>> | undefined
  /** Container ports to publish, each on a free port of 127.0.0.1. */
  readonly ports?: ReadonlyArray<number> | undefined
  readonly entrypoint?: string | undefined
  readonly args?: ReadonlyArray<string> | undefined
  /** Wait until the container output matches before returning. */
  readonly waitForLog?: RegExp | undefined
  /** @default "60 seconds" */
  readonly waitTimeout?: Duration.Input | undefined
}

export interface Container {
  readonly id: string
  readonly runtime: Runtime
  readonly host: string
  /** Host port published for a container port. */
  readonly port: (containerPort: number) => number
  /** Combined stdout and stderr. */
  readonly logs: Effect.Effect<string, DockerError>
  readonly stop: Effect.Effect<void, DockerError>
  readonly start: Effect.Effect<void, DockerError>
  /**
   * Waits until `pattern` appears in the logs `count` times, so a restarted
   * container can be awaited by counting past earlier matches.
   */
  readonly waitForLog: (
    pattern: RegExp,
    options?: { readonly count?: number; readonly timeout?: Duration.Input },
  ) => Effect.Effect<void, DockerError>
}

export interface DockerService {
  readonly runtime: Runtime
  readonly run: (options: RunOptions) => Effect.Effect<Container, DockerError, Scope.Scope>
}

export class Docker extends Context.Service<Docker, DockerService>()("effect-nats/testing/Docker") {}

const exec = (
  spawner: ChildProcessSpawner.ChildProcessSpawner["Service"],
  runtime: Runtime,
  args: ReadonlyArray<string>,
) =>
  Effect
    .scoped(
      Effect.gen(function*() {
        const handle = yield* spawner.spawn(ChildProcess.make(runtime, args, { stdout: "pipe", stderr: "pipe" }))
        const [stdout, stderr, exitCode] = yield* Effect.all(
          [
            handle.stdout.pipe(Stream.decodeText(), Stream.mkString),
            handle.stderr.pipe(Stream.decodeText(), Stream.mkString),
            handle.exitCode,
          ],
          { concurrency: "unbounded" },
        )
        if (exitCode !== 0) {
          return yield* new DockerError({
            message: `${runtime} ${args[0]} exited with code ${exitCode}: ${stderr.trim()}`,
          })
        }
        return { stdout, stderr }
      }),
    )
    .pipe(
      Effect.catchTag(
        "PlatformError",
        (cause) => Effect.fail(new DockerError({ message: `failed to run ${runtime} ${args[0]}`, cause })),
      ),
    )

export const freePort = Effect.callback<number, DockerError>((resume) => {
  const server = NNet.createServer()
  server.once("error", (cause) => resume(Effect.fail(new DockerError({ message: "no free port", cause }))))
  server.listen(0, "127.0.0.1", () => {
    const port = (server.address() as NNet.AddressInfo).port
    server.close(() => resume(Effect.succeed(port)))
  })
})

const isAvailable = (spawner: ChildProcessSpawner.ChildProcessSpawner["Service"], runtime: Runtime) =>
  spawner.exitCode(ChildProcess.make(runtime, ["info"], { stdout: "ignore", stderr: "ignore" })).pipe(
    Effect.map((code) => code === 0),
    Effect.orElseSucceed(() => false),
  )

/**
 * The `CONTAINER_RUNTIME` config if set, otherwise the first of `docker` and
 * `podman` whose daemon (or machine) responds.
 */
const detectRuntime: Effect.Effect<
  Runtime,
  DockerError | Config.ConfigError,
  ChildProcessSpawner.ChildProcessSpawner
> = Effect
  .gen(function*() {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
    const forced = yield* Config.option(Config.Literals(["docker", "podman"], "CONTAINER_RUNTIME"))
    if (Option.isSome(forced)) return forced.value
    for (const runtime of ["docker", "podman"] as const) {
      if (yield* isAvailable(spawner, runtime)) return runtime
    }
    return yield* new DockerError({ message: "neither docker nor podman is available" })
  })

const make = (runtime: Runtime): Effect.Effect<DockerService, never, ChildProcessSpawner.ChildProcessSpawner> =>
  Effect.gen(function*() {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
    const run = (...args: ReadonlyArray<string>) => exec(spawner, runtime, args)

    return {
      runtime,
      run: (options) =>
        Effect.gen(function*() {
          const ports = new Map<number, number>()
          for (const containerPort of options.ports ?? []) ports.set(containerPort, yield* freePort)

          const args = ["run", "-d"]
          if (options.name !== undefined) args.push("--name", options.name)
          for (const [key, value] of Object.entries(options.env ?? {})) args.push("-e", `${key}=${value}`)
          // Fixed host ports keep the mapping stable across stop/start.
          for (const [containerPort, hostPort] of ports) args.push("-p", `127.0.0.1:${hostPort}:${containerPort}`)
          if (options.entrypoint !== undefined) args.push("--entrypoint", options.entrypoint)
          args.push(options.image, ...(options.args ?? []))

          const id = yield* Effect.acquireRelease(
            Effect.map(run(...args), (result) => result.stdout.trim()),
            (id) => Effect.ignore(run("rm", "-f", "-v", id)),
          )

          const logs = Effect.map(run("logs", id), (result) => result.stdout + result.stderr)
          const container: Container = {
            id,
            runtime,
            host: "127.0.0.1",
            port: (containerPort) => {
              const hostPort = ports.get(containerPort)
              if (hostPort === undefined) throw new Error(`container port ${containerPort} is not published`)
              return hostPort
            },
            logs,
            stop: Effect.asVoid(run("stop", "-t", "1", id)),
            start: Effect.asVoid(run("start", id)),
            waitForLog: (pattern, waitOptions) => {
              const count = waitOptions?.count ?? 1
              const global = new RegExp(
                pattern.source,
                pattern.flags.includes("g") ? pattern.flags : `${pattern.flags}g`,
              )
              const check = Effect.flatMap(logs, (output) =>
                (output.match(global)?.length ?? 0) >= count
                  ? Effect.void
                  : Effect.fail(new DockerError({ message: `waiting for ${pattern}` })))
              return check.pipe(
                Effect.retry({
                  while: (error) => error.message.startsWith("waiting for"),
                  schedule: Schedule.spaced("100 millis"),
                }),
                Effect.timeoutOrElse({
                  duration: waitOptions?.timeout ?? "60 seconds",
                  orElse: () =>
                    Effect.flatMap(logs, (output) =>
                      Effect.fail(
                        new DockerError({ message: `timed out waiting for ${pattern} in logs of ${id}:\n${output}` }),
                      )),
                }),
              )
            },
          }

          if (options.waitForLog !== undefined) {
            yield* container.waitForLog(options.waitForLog, { timeout: options.waitTimeout })
          }
          return container
        }),
    } satisfies DockerService
  })

export const layer: Layer.Layer<
  Docker,
  DockerError | Config.ConfigError,
  ChildProcessSpawner.ChildProcessSpawner
> = Layer.effect(
  Docker,
  Effect.flatMap(detectRuntime, make),
)
