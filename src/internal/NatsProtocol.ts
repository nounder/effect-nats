/**
 * Wire codec for the NATS client protocol.
 *
 * Every function is pure: bytes in, bytes or plain data out. Nothing here
 * opens a socket, negotiates TLS, or tracks session state. Modeled after the
 * `PgProtocol` codec in `@effect/sql-pg`.
 *
 * Server frames are a CRLF-terminated control line, followed for `MSG` and
 * `HMSG` by a payload of known size and a trailing CRLF.
 *
 * Encoded frames and decoded payloads are views into pooled buffers that are
 * written once and never rewritten. They stay valid for as long as they are
 * held, but holding one keeps its whole pool buffer (at most 64 KiB, or the
 * size of one oversized message) alive.
 */
import * as Data from "effect/Data"

export const defaultMaxPayloadSize = 64 * 1024 * 1024
export const defaultMaxControlLineSize = 1024 * 1024

/** Where a parser stops growing its buffer pool. */
const maxBufferSize = 64 * 1024

export class ParseError extends Data.TaggedError("NatsProtocolParseError")<{
  readonly message: string
}> {}

export class EncodeError extends Data.TaggedError("NatsProtocolEncodeError")<{
  readonly message: string
}> {}

export interface Info {
  readonly _tag: "Info"
  readonly info: ServerInfo
}

export interface Msg {
  readonly _tag: "Msg"
  readonly subject: string
  readonly sid: number
  readonly reply: string | undefined
  /** Raw header block including the `NATS/1.0` line, or `undefined` for `MSG`. */
  readonly headers: Uint8Array | undefined
  readonly data: Uint8Array
}

export interface Ping {
  readonly _tag: "Ping"
}

export interface Pong {
  readonly _tag: "Pong"
}

export interface Ok {
  readonly _tag: "Ok"
}

export interface Err {
  readonly _tag: "Err"
  readonly message: string
}

export type ServerMessage = Info | Msg | Ping | Pong | Ok | Err

/**
 * Fields of the server `INFO` JSON this client reads. Servers send more.
 */
export interface ServerInfo {
  readonly server_id: string
  readonly server_name?: string
  readonly version: string
  readonly proto: number
  readonly host: string
  readonly port: number
  readonly max_payload: number
  readonly headers?: boolean
  readonly auth_required?: boolean
  readonly tls_required?: boolean
  readonly tls_available?: boolean
  readonly connect_urls?: ReadonlyArray<string>
  readonly ldm?: boolean
  readonly client_id?: number
  readonly client_ip?: string
  readonly nonce?: string
  readonly cluster?: string
  readonly jetstream?: boolean
}

const PING: Ping = { _tag: "Ping" }
const PONG: Pong = { _tag: "Pong" }
const OK: Ok = { _tag: "Ok" }

const CR = 13
const LF = 10
const SPACE = 32
const TAB = 9

const emptyBytes = new Uint8Array(0)

const view = (store: ArrayBufferLike, offset: number, length: number): Uint8Array =>
  length === 0 ? emptyBytes : new Uint8Array(store, offset, length)

/**
 * An incremental decoder for the server message stream.
 */
export interface Parser {
  /**
   * Decodes a chunk and returns every complete message. A partial message is
   * retained for the next call. Parse errors are terminal.
   */
  readonly push: (chunk: Uint8Array) => ReadonlyArray<ServerMessage>
  /**
   * Decodes a chunk and passes each complete message to `onMessage`
   * immediately, without collecting them first.
   */
  readonly pushEach: (chunk: Uint8Array, onMessage: (message: ServerMessage) => void) => void
}

export const makeParser = (options?: {
  readonly maxPayloadSize?: number | undefined
  readonly maxControlLineSize?: number | undefined
}): Parser => {
  const maxPayloadSize = options?.maxPayloadSize ?? defaultMaxPayloadSize
  const maxControlLineSize = options?.maxControlLineSize ?? defaultMaxControlLineSize
  let bufferSize = 8192
  let buffer = new Uint8Array(bufferSize)
  let store = buffer.buffer
  let start = 0
  let end = 0
  // bytes after `start` already scanned for LF, so a partial control line is not rescanned
  let scanned = 0
  let failed = false

  // Payload of the `MSG` or `HMSG` whose control line was read last.
  let pending = false
  let pendingSubject = ""
  let pendingSid = 0
  let pendingReply: string | undefined
  let pendingHeaderSize = 0
  let pendingTotalSize = 0

  // Bytes already handed to the caller are never overwritten, so a full
  // buffer is replaced rather than compacted. That lets payloads be views
  // instead of copies. The pool doubles up to `maxBufferSize`; a single
  // oversized message grows its buffer beyond the pool without raising it.
  const append = (chunk: Uint8Array): void => {
    if (end + chunk.length > buffer.length) {
      const held = end - start
      if (bufferSize < maxBufferSize) bufferSize *= 2
      // reserve the whole pending payload at once instead of doubling towards it
      const required = Math.max(held + chunk.length, pending ? pendingTotalSize + 2 : 0)
      let capacity = bufferSize
      while (capacity < required) capacity *= 2
      const next = new Uint8Array(capacity)
      next.set(buffer.subarray(start, end))
      buffer = next
      store = next.buffer
      start = 0
      end = held
    }
    buffer.set(chunk, end)
    end += chunk.length
  }

  const fail = (message: string): never => {
    throw new ParseError({ message })
  }

  // Splits the control line in `[from, to)` on spaces and tabs into `args`
  // as start/end offset pairs, returning the number of arguments.
  const args = new Int32Array(16)
  const splitArgs = (from: number, to: number): number => {
    let count = 0
    let i = from
    while (i < to) {
      while (i < to && (buffer[i] === SPACE || buffer[i] === TAB)) i++
      if (i >= to) break
      const argStart = i
      while (i < to && buffer[i] !== SPACE && buffer[i] !== TAB) i++
      if (count === 8) fail("too many arguments in control line")
      args[count * 2] = argStart
      args[count * 2 + 1] = i
      count++
    }
    return count
  }

  const argString = (index: number): string => decodeUtf8(buffer, args[index * 2], args[index * 2 + 1])

  const argInt = (index: number): number => {
    const from = args[index * 2]
    const to = args[index * 2 + 1]
    if (to - from > 15) fail("number too large in control line")
    let n = 0
    for (let i = from; i < to; i++) {
      const digit = buffer[i] - 48
      if (digit < 0 || digit > 9) fail(`invalid number in control line: ${decodeUtf8(buffer, from, to)}`)
      n = n * 10 + digit
    }
    return n
  }

  const controlLine = (from: number, to: number, onMessage: (message: ServerMessage) => void): void => {
    if (to === from) return
    // ASCII lowercase: `| 0x20` folds A-Z onto a-z
    const c0 = buffer[from] | 0x20
    if (c0 === 0x6d /* m */ && isOp(buffer, from, to, "msg")) {
      // MSG <subject> <sid> [reply-to] <#bytes>
      const count = splitArgs(from + 3, to)
      if (count !== 3 && count !== 4) fail(`invalid MSG: ${decodeUtf8(buffer, from, to)}`)
      pendingSubject = argString(0)
      pendingSid = argInt(1)
      pendingReply = count === 4 ? argString(2) : undefined
      pendingHeaderSize = 0
      pendingTotalSize = argInt(count - 1)
    } else if (c0 === 0x68 /* h */ && isOp(buffer, from, to, "hmsg")) {
      // HMSG <subject> <sid> [reply-to] <#header bytes> <#total bytes>
      const count = splitArgs(from + 4, to)
      if (count !== 4 && count !== 5) fail(`invalid HMSG: ${decodeUtf8(buffer, from, to)}`)
      pendingSubject = argString(0)
      pendingSid = argInt(1)
      pendingReply = count === 5 ? argString(2) : undefined
      pendingHeaderSize = argInt(count - 2)
      pendingTotalSize = argInt(count - 1)
      if (pendingHeaderSize > pendingTotalSize) fail(`invalid HMSG sizes: ${decodeUtf8(buffer, from, to)}`)
    } else if (c0 === 0x70 /* p */ && isOp(buffer, from, to, "ping")) {
      return onMessage(PING)
    } else if (c0 === 0x70 /* p */ && isOp(buffer, from, to, "pong")) {
      return onMessage(PONG)
    } else if (buffer[from] === 0x2b /* + */ && isOp(buffer, from, to, "+ok")) {
      return onMessage(OK)
    } else if (buffer[from] === 0x2d /* - */ && isOp(buffer, from, to, "-err")) {
      const message = decodeUtf8(buffer, from + 4, to).trim()
      return onMessage({ _tag: "Err", message: message.replace(/^'(.*)'$/, "$1") })
    } else if (c0 === 0x69 /* i */ && isOp(buffer, from, to, "info")) {
      let info: ServerInfo
      try {
        info = JSON.parse(decodeUtf8(buffer, from + 4, to))
      } catch (cause) {
        return fail(`invalid INFO: ${String(cause)}`)
      }
      return onMessage({ _tag: "Info", info })
    } else {
      return fail(`unknown protocol operation: ${decodeUtf8(buffer, from, Math.min(to, from + 32))}`)
    }
    if (pendingTotalSize > maxPayloadSize) {
      fail(`message size ${pendingTotalSize} exceeds maxPayloadSize ${maxPayloadSize}`)
    }
    pending = true
  }

  const parser: Parser = {
    push(chunk) {
      const messages: Array<ServerMessage> = []
      parser.pushEach(chunk, (message) => {
        messages.push(message)
      })
      return messages
    },
    pushEach(chunk, onMessage) {
      if (failed) throw new ParseError({ message: "Parser cannot be reused after a failure" })
      try {
        append(chunk)
        while (start < end) {
          if (pending) {
            const payloadEnd = start + pendingTotalSize
            if (end < payloadEnd + 2) break
            if (buffer[payloadEnd] !== CR || buffer[payloadEnd + 1] !== LF) {
              fail("message payload is not terminated by CRLF")
            }
            // `buffer` always starts at byte 0 of `store`, so offsets index both.
            const dataStart = start + pendingHeaderSize
            const message: Msg = {
              _tag: "Msg",
              subject: pendingSubject,
              sid: pendingSid,
              reply: pendingReply,
              headers: pendingHeaderSize > 0 ? view(store, start, pendingHeaderSize) : undefined,
              data: view(store, dataStart, payloadEnd - dataStart),
            }
            pending = false
            start = payloadEnd + 2
            onMessage(message)
            continue
          }

          let lf = -1
          for (let i = start + scanned; i < end; i++) {
            if (buffer[i] === LF) {
              lf = i
              break
            }
          }
          if (lf === -1) {
            scanned = end - start
            if (scanned > maxControlLineSize) fail(`control line exceeds ${maxControlLineSize} bytes`)
            break
          }
          scanned = 0
          const lineStart = start
          start = lf + 1
          controlLine(lineStart, lf > lineStart && buffer[lf - 1] === CR ? lf - 1 : lf, onMessage)
        }
      } catch (error) {
        failed = true
        throw error
      }
    },
  }
  return parser
}

const isOp = (bytes: Uint8Array, from: number, to: number, op: string): boolean => {
  const length = op.length
  if (to - from < length) return false
  for (let i = 0; i < length; i++) {
    const code = bytes[from + i]
    // fold letters only, so '+' and '-' compare as-is
    if ((code >= 0x41 && code <= 0x5a ? code | 0x20 : code) !== op.charCodeAt(i)) return false
  }
  const next = from + length
  return next === to || bytes[next] === SPACE || bytes[next] === TAB
}

const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder("utf-8", { ignoreBOM: true })

/** Below this length a per-character loop beats `TextDecoder.decode`. */
const asciiDecodeLimit = 16

/** Node's own UTF-8 decoder, faster than `TextDecoder` for short strings. */
const utf8Slice: ((this: Uint8Array, start: number, end: number) => string) | undefined = (globalThis as any)
  .Buffer
  ?.prototype
  ?.utf8Slice

export const decodeUtf8 = (bytes: Uint8Array, from: number, to: number): string => {
  const size = to - from
  if (size <= asciiDecodeLimit) {
    let text = ""
    let index = from
    for (; index < to; index++) {
      const code = bytes[index]
      if (code > 0x7f) break
      text += String.fromCharCode(code)
    }
    if (index === to) return text
  }
  if (utf8Slice !== undefined) return utf8Slice.call(bytes, from, to)
  return textDecoder.decode(bytes.subarray(from, to))
}

/** Above this length `TextEncoder.encodeInto` beats a per-character loop. */
const asciiEncodeLimit = 48

/**
 * Writes frames back to back into a pooled buffer and hands out a view of
 * each one, so encoding costs no allocation of its own. Bytes below `start`
 * have been handed out and are never rewritten; when the pool runs out it is
 * replaced rather than reused.
 */
class Writer {
  readonly poolSize: number
  bytes: Uint8Array
  start = 0
  offset = 0

  constructor(poolSize: number) {
    this.poolSize = poolSize
    this.bytes = new Uint8Array(poolSize)
  }

  reserve(size: number): void {
    if (this.offset + size <= this.bytes.length) return
    const held = this.offset - this.start
    let capacity = this.poolSize
    while (capacity < held + size) capacity *= 2
    const next = new Uint8Array(capacity)
    next.set(this.bytes.subarray(this.start, this.offset))
    this.bytes = next
    this.start = 0
    this.offset = held
  }

  /** Starts a frame, dropping anything a failed write left behind. */
  begin(): void {
    this.offset = this.start
  }

  ascii(value: string): void {
    const length = value.length
    this.reserve(length)
    const bytes = this.bytes
    const offset = this.offset
    for (let i = 0; i < length; i++) bytes[offset + i] = value.charCodeAt(i)
    this.offset = offset + length
  }

  /**
   * Writes a subject, queue group or other protocol token. Tokens are
   * separated by whitespace on the wire, so any whitespace or control
   * character would let a caller inject protocol.
   */
  token(value: string, name: string): void {
    const length = value.length
    if (length === 0) throw new EncodeError({ message: `${name} must not be empty` })
    if (length <= asciiEncodeLimit) {
      this.reserve(length)
      const bytes = this.bytes
      const start = this.offset
      let i = 0
      for (; i < length; i++) {
        const code = value.charCodeAt(i)
        if (code > 0x7f) break
        if (code <= SPACE || code === 0x7f) {
          throw new EncodeError({ message: `${name} must not contain whitespace: "${value}"` })
        }
        bytes[start + i] = code
      }
      if (i === length) {
        this.offset = start + length
        return
      }
    }
    // oxlint-disable-next-line no-control-regex -- control characters are exactly what must be rejected
    if (/[\s\x00-\x1f\x7f]/.test(value)) {
      throw new EncodeError({ message: `${name} must not contain whitespace: "${value}"` })
    }
    // UTF-8 takes at most three bytes per UTF-16 code unit
    this.reserve(length * 3)
    this.offset += textEncoder.encodeInto(value, this.bytes.subarray(this.offset)).written
  }

  int(value: number): void {
    if (value < 10) {
      this.reserve(1)
      this.bytes[this.offset++] = 48 + value
      return
    }
    this.ascii(String(value))
  }

  raw(value: Uint8Array): void {
    const length = value.length
    if (length === 0) return
    this.reserve(length)
    this.bytes.set(value, this.offset)
    this.offset += length
  }

  crlf(): void {
    this.reserve(2)
    this.bytes[this.offset++] = CR
    this.bytes[this.offset++] = LF
  }

  finish(): Uint8Array {
    const value = view(this.bytes.buffer, this.bytes.byteOffset + this.start, this.offset - this.start)
    if (this.bytes.length > this.poolSize) {
      // An oversized frame grew the pool; do not keep the rest of it around.
      this.bytes = new Uint8Array(this.poolSize)
      this.start = 0
      this.offset = 0
    } else {
      this.start = this.offset
    }
    return value
  }
}

const sharedWriter = new Writer(16 * 1024)

const constant = (value: string): Uint8Array => textEncoder.encode(value)

export const PING_FRAME: Uint8Array = constant("PING\r\n")
export const PONG_FRAME: Uint8Array = constant("PONG\r\n")

export interface Connect {
  readonly verbose: boolean
  readonly pedantic: boolean
  readonly tls_required: boolean
  readonly name?: string | undefined
  readonly lang: string
  readonly version: string
  readonly protocol: number
  readonly echo: boolean
  readonly headers: boolean
  readonly no_responders: boolean
  readonly user?: string | undefined
  readonly pass?: string | undefined
  readonly auth_token?: string | undefined
}

export const encodeConnect = (options: Connect): Uint8Array =>
  textEncoder.encode(`CONNECT ${JSON.stringify(options)}\r\n`)

/**
 * Encodes `PUB`, or `HPUB` when `headers` is given.
 *
 * Throws `EncodeError` when the subject or reply is not a valid token.
 */
export const encodePublish = (options: {
  readonly subject: string
  readonly reply?: string | undefined
  readonly headers?: Uint8Array | undefined
  readonly data: Uint8Array
}): Uint8Array => {
  const writer = sharedWriter
  writer.begin()
  const headers = options.headers
  writer.ascii(headers === undefined ? "PUB " : "HPUB ")
  writer.token(options.subject, "subject")
  writer.ascii(" ")
  if (options.reply !== undefined && options.reply !== "") {
    writer.token(options.reply, "reply subject")
    writer.ascii(" ")
  }
  if (headers !== undefined) {
    writer.int(headers.length)
    writer.ascii(" ")
    writer.int(headers.length + options.data.length)
  } else {
    writer.int(options.data.length)
  }
  writer.crlf()
  if (headers !== undefined) writer.raw(headers)
  writer.raw(options.data)
  writer.crlf()
  return writer.finish()
}

/**
 * Encodes `SUB`. Throws `EncodeError` when the subject or queue group is not a valid token.
 */
export const encodeSubscribe = (options: {
  readonly subject: string
  readonly queue?: string | undefined
  readonly sid: number
}): Uint8Array => {
  const writer = sharedWriter
  writer.begin()
  writer.ascii("SUB ")
  writer.token(options.subject, "subject")
  writer.ascii(" ")
  if (options.queue !== undefined && options.queue !== "") {
    writer.token(options.queue, "queue group")
    writer.ascii(" ")
  }
  writer.int(options.sid)
  writer.crlf()
  return writer.finish()
}

export const encodeUnsubscribe = (options: {
  readonly sid: number
  readonly max?: number | undefined
}): Uint8Array => {
  const writer = sharedWriter
  writer.begin()
  writer.ascii("UNSUB ")
  writer.int(options.sid)
  if (options.max !== undefined) {
    writer.ascii(" ")
    writer.int(options.max)
  }
  writer.crlf()
  return writer.finish()
}

/**
 * Joins frames into one buffer. Frames written back to back by the shared
 * writer are usually contiguous in one pool buffer, in which case a single
 * view spanning them is returned without copying.
 */
export const concatFrames = (frames: ReadonlyArray<Uint8Array>): Uint8Array => {
  if (frames.length === 1) return frames[0]
  const first = frames[0]
  let contiguous = true
  let length = first.length
  for (let i = 1; i < frames.length; i++) {
    const prev = frames[i - 1]
    const frame = frames[i]
    if (frame.buffer !== first.buffer || frame.byteOffset !== prev.byteOffset + prev.length) contiguous = false
    length += frame.length
  }
  if (contiguous) return new Uint8Array(first.buffer, first.byteOffset, length)
  const out = new Uint8Array(length)
  let offset = 0
  for (const frame of frames) {
    out.set(frame, offset)
    offset += frame.length
  }
  return out
}
