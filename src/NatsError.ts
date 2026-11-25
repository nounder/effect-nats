import * as Data from "effect/Data"

/** Failed to establish, or lost, the connection to every configured server. */
export class NatsConnectionError extends Data.TaggedError("NatsConnectionError")<{
  readonly message: string
  readonly server?: string | undefined
  readonly cause?: unknown
}> {}

/** Server rejected the credentials sent in `CONNECT`. */
export class NatsAuthorizationError extends Data.TaggedError("NatsAuthorizationError")<{
  readonly message: string
}> {}

/** Server rejected a publish or subscribe on a subject the user has no permission for. */
export class NatsPermissionError extends Data.TaggedError("NatsPermissionError")<{
  readonly message: string
  readonly operation: "publish" | "subscription"
  readonly subject: string
  readonly queue?: string | undefined
}> {}

/** Server sent `-ERR` or the client received a frame it could not understand. */
export class NatsProtocolError extends Data.TaggedError("NatsProtocolError")<{
  readonly message: string
}> {}

/** Invalid argument passed by the caller, like an empty subject or oversized payload. */
export class NatsInvalidArgumentError extends Data.TaggedError("NatsInvalidArgumentError")<{
  readonly message: string
}> {}

/** Request was published to a subject with no subscribers. */
export class NatsNoRespondersError extends Data.TaggedError("NatsNoRespondersError")<{
  readonly subject: string
}> {
  override get message() {
    return `no responders for subject "${this.subject}"`
  }
}

/** Operation attempted on a closed or draining connection or subscription. */
export class NatsClosedError extends Data.TaggedError("NatsClosedError")<{
  readonly message: string
}> {}

export type NatsError =
  | NatsConnectionError
  | NatsAuthorizationError
  | NatsPermissionError
  | NatsProtocolError
  | NatsInvalidArgumentError
  | NatsNoRespondersError
  | NatsClosedError
