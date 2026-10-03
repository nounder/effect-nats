import * as test from "bun:test"
import * as NatsProtocol from "../src/internal/NatsProtocol.ts"
import { NatsHeaders } from "../src/NatsHeaders.ts"
import * as Nuid from "../src/Nuid.ts"

const bytes = (value: string) => new TextEncoder().encode(value)
const text = (value: Uint8Array) => new TextDecoder().decode(value)

const stream = [
  `INFO {"server_id":"abc","version":"2.12.0","max_payload":1048576,"headers":true}\r\n`,
  "PING\r\n",
  "MSG foo.bar 1 5\r\nhello\r\n",
  "MSG foo.bar 2 _INBOX.x 0\r\n\r\n",
  "HMSG foo 3 reply 22 27\r\nNATS/1.0\r\nX-Key: v\r\n\r\nworld\r\n",
  "HMSG foo 4 16 16\r\nNATS/1.0 503\r\n\r\n\r\n",
  "+OK\r\n",
  "-ERR 'Authorization Violation'\r\n",
  "PONG\r\n",
]
  .join("")

const summarize = (messages: ReadonlyArray<NatsProtocol.ServerMessage>): Array<unknown> =>
  messages.map((message) => {
    switch (message._tag) {
      case "Msg":
        return {
          _tag: message._tag,
          subject: message.subject,
          sid: message.sid,
          reply: message.reply,
          headers: message.headers === undefined ? undefined : text(message.headers),
          data: text(message.data),
        }
      case "Info":
        return { _tag: message._tag, server_id: message.info.server_id }
      default:
        return message
    }
  })

const expected = [
  { _tag: "Info", server_id: "abc" },
  { _tag: "Ping" },
  { _tag: "Msg", subject: "foo.bar", sid: 1, reply: undefined, headers: undefined, data: "hello" },
  { _tag: "Msg", subject: "foo.bar", sid: 2, reply: "_INBOX.x", headers: undefined, data: "" },
  { _tag: "Msg", subject: "foo", sid: 3, reply: "reply", headers: "NATS/1.0\r\nX-Key: v\r\n\r\n", data: "world" },
  { _tag: "Msg", subject: "foo", sid: 4, reply: undefined, headers: "NATS/1.0 503\r\n\r\n", data: "" },
  { _tag: "Ok" },
  { _tag: "Err", message: "Authorization Violation" },
  { _tag: "Pong" },
]

test.describe("makeParser", () => {
  test.it("decodes every server frame", () => {
    const parser = NatsProtocol.makeParser()

    test.expect(summarize(parser.push(bytes(stream)))).toEqual(expected)
  })

  test.it("decodes the same frames for every split point", () => {
    const all = bytes(stream)
    for (let split = 1; split < all.length; split++) {
      const parser = NatsProtocol.makeParser()
      const messages = [...parser.push(all.subarray(0, split)), ...parser.push(all.subarray(split))]

      test.expect(summarize(messages)).toEqual(expected)
    }
  })

  test.it("decodes byte-by-byte input", () => {
    const parser = NatsProtocol.makeParser()
    const messages: Array<NatsProtocol.ServerMessage> = []
    for (const byte of bytes(stream)) messages.push(...parser.push(new Uint8Array([byte])))

    test.expect(summarize(messages)).toEqual(expected)
  })

  test.it("keeps payload views valid after the buffer is replaced", () => {
    const parser = NatsProtocol.makeParser()
    const payload = "x".repeat(70_000)
    const first = parser.push(bytes(`MSG a 1 3\r\none\r\n`))
    parser.push(bytes(`MSG b 1 ${payload.length}\r\n${payload}\r\n`))

    test.expect(text((first[0] as NatsProtocol.Msg).data)).toBe("one")
  })

  test.it("accepts lowercase operations and tabs", () => {
    const parser = NatsProtocol.makeParser()

    test.expect(summarize(parser.push(bytes("msg\tfoo\t1\t2\r\nhi\r\nping\r\n")))).toEqual([
      { _tag: "Msg", subject: "foo", sid: 1, reply: undefined, headers: undefined, data: "hi" },
      { _tag: "Ping" },
    ])
  })

  test.it("fails on unknown operations and stays failed", () => {
    const parser = NatsProtocol.makeParser()

    test.expect(() => parser.push(bytes("BOGUS\r\n"))).toThrow(NatsProtocol.ParseError)
    test.expect(() => parser.push(bytes("PING\r\n"))).toThrow("cannot be reused")
  })

  test.it("fails on a payload without CRLF", () => {
    const parser = NatsProtocol.makeParser()

    test.expect(() => parser.push(bytes("MSG a 1 2\r\nhiXX"))).toThrow("not terminated")
  })

  test.it("enforces maxPayloadSize", () => {
    const parser = NatsProtocol.makeParser({ maxPayloadSize: 10 })

    test.expect(() => parser.push(bytes("MSG a 1 11\r\n"))).toThrow("exceeds maxPayloadSize")
  })

  test.it("enforces maxControlLineSize", () => {
    const parser = NatsProtocol.makeParser({ maxControlLineSize: 16 })

    test.expect(() => parser.push(bytes("MSG a-very-long-subject"))).toThrow("control line exceeds")
  })
})

test.describe("encode", () => {
  test.it("encodes PUB and HPUB", () => {
    test.expect(text(NatsProtocol.encodePublish({ subject: "foo", data: bytes("hello") }))).toBe(
      "PUB foo 5\r\nhello\r\n",
    )
    test.expect(text(NatsProtocol.encodePublish({ subject: "foo", reply: "bar", data: new Uint8Array() }))).toBe(
      "PUB foo bar 0\r\n\r\n",
    )
    const headers = new NatsHeaders({ A: "1" }).encode()
    test.expect(text(NatsProtocol.encodePublish({ subject: "ü", headers, data: bytes("x") }))).toBe(
      "HPUB ü 18 19\r\nNATS/1.0\r\nA: 1\r\n\r\nx\r\n",
    )
  })

  test.it("encodes SUB and UNSUB", () => {
    test.expect(text(NatsProtocol.encodeSubscribe({ subject: "foo.*", sid: 12 }))).toBe("SUB foo.* 12\r\n")
    test.expect(text(NatsProtocol.encodeSubscribe({ subject: "foo", queue: "q", sid: 1 }))).toBe("SUB foo q 1\r\n")
    test.expect(text(NatsProtocol.encodeUnsubscribe({ sid: 7 }))).toBe("UNSUB 7\r\n")
    test.expect(text(NatsProtocol.encodeUnsubscribe({ sid: 7, max: 100 }))).toBe("UNSUB 7 100\r\n")
  })

  test.it("rejects tokens that would inject protocol", () => {
    for (const subject of ["", "a b", "a\tb", "a\r\nPING", "ä b"]) {
      test.expect(() => NatsProtocol.encodePublish({ subject, data: new Uint8Array() })).toThrow(
        NatsProtocol.EncodeError,
      )
    }
    test.expect(() => NatsProtocol.encodeSubscribe({ subject: "a", queue: "q q", sid: 1 })).toThrow(
      NatsProtocol.EncodeError,
    )
  })

  test.it("joins contiguous frames without copying", () => {
    const a = NatsProtocol.encodeUnsubscribe({ sid: 1 })
    const b = NatsProtocol.encodeUnsubscribe({ sid: 2 })
    const joined = NatsProtocol.concatFrames([a, b])

    test.expect(joined.buffer).toBe(a.buffer)
    test.expect(text(joined)).toBe("UNSUB 1\r\nUNSUB 2\r\n")
    test.expect(text(NatsProtocol.concatFrames([a, NatsProtocol.PING_FRAME, b]))).toBe(
      "UNSUB 1\r\nPING\r\nUNSUB 2\r\n",
    )
  })
})

test.describe("NatsHeaders", () => {
  test.it("encodes and decodes values and status", () => {
    const headers = new NatsHeaders([["X-A", "1"], ["X-A", "2"], ["X-B", " spaced "]])
    const decoded = NatsHeaders.decode(headers.encode())

    test.expect(decoded.getAll("X-A")).toEqual(["1", "2"])
    test.expect(decoded.get("X-B")).toBe("spaced")
    test.expect(decoded.code).toBe(0)

    const status = NatsHeaders.decode(bytes("NATS/1.0 408 Request Timeout\r\n\r\n"))
    test.expect(status.code).toBe(408)
    test.expect(status.description).toBe("Request Timeout")
    test.expect(status.hasError).toBe(true)
  })

  test.it("rejects invalid keys and values", () => {
    test.expect(() => new NatsHeaders().set("bad key", "v")).toThrow()
    test.expect(() => new NatsHeaders().set("k", "a\r\nb")).toThrow()
  })
})

test.describe("Nuid", () => {
  test.it("generates unique 22 char ids", () => {
    const ids = new Set(Array.from({ length: 10_000 }, () => Nuid.next()))

    test.expect(ids.size).toBe(10_000)
    for (const id of ids) test.expect(id).toMatch(/^[0-9A-Za-z]{22}$/)
    test.expect(Nuid.createInbox()).toMatch(/^_INBOX\.[0-9A-Za-z]{22}$/)
  })
})
