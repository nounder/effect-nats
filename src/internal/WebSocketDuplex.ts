import * as NStream from "node:stream"
/**
 * Exposes an `effect/socket` WebSocket as a Node `Duplex`, so WebSocket and TCP sessions share the
 * handshake, dispatch and write path.
 *
 * NATS over WebSocket carries the plain protocol byte stream in binary frames, so frame boundaries
 * carry no meaning and chunks are pushed as-is.
 */
import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import type * as Fiber from "effect/Fiber"
import * as Socket from "effect/socket/Socket"

const encoder = new TextEncoder()

const toError = (cause: Cause.Cause<unknown>): Error => {
  const error = Cause.squash(cause)
  return error instanceof Error ? error : new Error(String(error))
}

const isNormalClose = (cause: Cause.Cause<Socket.SocketError>): boolean => {
  const error = Cause.squash(cause)
  return (
    Socket.isSocketError(error) &&
    error.reason._tag === "SocketCloseError" &&
    error.reason.code === 1000
  )
}

/**
 * Emits `connect` once the WebSocket is open. Writes made before that are corked and sent on open.
 * `end()` sends a normal close frame.
 */
export const make = (
  socket: Effect.Effect<Socket.Socket>,
  runFork: <A, E>(effect: Effect.Effect<A, E>) => Fiber.Fiber<A, E>,
): NStream.Duplex => {
  let writer: Socket.Writer | undefined

  const duplex = new NStream.Duplex({
    allowHalfOpen: false,
    read() {},
    write(chunk: Uint8Array, _encoding, callback) {
      runFork(writer!.write(chunk)).addObserver((exit) =>
        callback(Exit.isFailure(exit) ? toError(exit.cause) : undefined),
      )
    },
    final(callback) {
      runFork(writer!.write(new Socket.CloseEvent(1000))).addObserver(() => callback())
    },
    destroy(error, callback) {
      reader.interruptUnsafe()
      callback(error)
    },
  })
  duplex.cork()

  const reader = runFork(
    Effect.scoped(
      Effect.gen(function* () {
        const ws = yield* socket
        const pull = (yield* ws.reader).pull
        writer = yield* ws.writer
        duplex.emit("connect")
        duplex.uncork()
        while (true) {
          for (const chunk of yield* pull)
            duplex.push(typeof chunk === "string" ? encoder.encode(chunk) : chunk)
        }
      }),
    ),
  )
  reader.addObserver((exit) => {
    if (duplex.destroyed || Exit.isSuccess(exit)) return
    const cause = exit.cause
    duplex.destroy(
      Cause.hasInterruptsOnly(cause) || isNormalClose(cause) ? undefined : toError(cause),
    )
  })
  return duplex
}
