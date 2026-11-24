const encoder = new TextEncoder()
const decoder = new TextDecoder()

const HEADER_LINE = "NATS/1.0"

/**
 * NATS message headers. Keys are case-sensitive and may have multiple values.
 *
 * `code` and `description` carry the optional status from the header line, like `NATS/1.0 503` for
 * "no responders".
 */
export class NatsHeaders implements Iterable<[string, string]> {
  readonly #entries = new Map<string, Array<string>>()
  code = 0
  description = ""

  constructor(
    init?: Iterable<readonly [string, string]> | Record<string, string | ReadonlyArray<string>>,
  ) {
    if (init === undefined) return
    const entries =
      Symbol.iterator in init
        ? (init as Iterable<readonly [string, string]>)
        : Object.entries(init).flatMap(([key, value]) =>
            typeof value === "string"
              ? [[key, value] as const]
              : value.map((v) => [key, v] as const),
          )
    for (const [key, value] of entries) this.append(key, value)
  }

  get(key: string): string | undefined {
    return this.#entries.get(key)?.[0]
  }

  getAll(key: string): ReadonlyArray<string> {
    return this.#entries.get(key) ?? []
  }

  has(key: string): boolean {
    return this.#entries.has(key)
  }

  set(key: string, value: string): this {
    this.#entries.set(validateKey(key), [validateValue(value)])
    return this
  }

  append(key: string, value: string): this {
    const values = this.#entries.get(validateKey(key))
    if (values === undefined) this.#entries.set(key, [validateValue(value)])
    else values.push(validateValue(value))
    return this
  }

  delete(key: string): boolean {
    return this.#entries.delete(key)
  }

  keys(): IterableIterator<string> {
    return this.#entries.keys()
  }

  get size(): number {
    return this.#entries.size
  }

  get hasError(): boolean {
    return this.code >= 300
  }

  *[Symbol.iterator](): Iterator<[string, string]> {
    for (const [key, values] of this.#entries) {
      for (const value of values) yield [key, value]
    }
  }

  toString(): string {
    let out = HEADER_LINE
    if (this.code > 0) out += ` ${this.code}${this.description ? ` ${this.description}` : ""}`
    out += "\r\n"
    for (const [key, value] of this) out += `${key}: ${value}\r\n`
    return out + "\r\n"
  }

  encode(): Uint8Array {
    return encoder.encode(this.toString())
  }

  static decode(bytes: Uint8Array): NatsHeaders {
    const headers = new NatsHeaders()
    const lines = decoder.decode(bytes).split("\r\n")
    const status = lines[0].slice(HEADER_LINE.length).trim()
    if (status.length > 0) {
      const space = status.indexOf(" ")
      const code = Number.parseInt(space === -1 ? status : status.slice(0, space), 10)
      headers.code = Number.isNaN(code) ? 0 : code
      headers.description = space === -1 ? "" : status.slice(space + 1).trim()
    }
    for (let i = 1; i < lines.length; i++) {
      const line = lines[i]
      const idx = line.indexOf(":")
      if (idx <= 0) continue
      const key = line.slice(0, idx)
      const value = line.slice(idx + 1).trim()
      const values = headers.#entries.get(key)
      if (values === undefined) headers.#entries.set(key, [value])
      else values.push(value)
    }
    return headers
  }
}

const validateKey = (key: string): string => {
  // RFC 7230 token: printable ASCII except separators and ':'
  if (key.length === 0 || !/^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/.test(key)) {
    throw new TypeError(`invalid header key: "${key}"`)
  }
  return key
}

const validateValue = (value: string): string => {
  if (/[\r\n]/.test(value)) throw new TypeError(`header value must not contain CR or LF`)
  return value.trim()
}
