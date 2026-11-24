/**
 * The subset of NATS NKeys needed to authenticate: decoding a seed, deriving its public key,
 * signing the server nonce, and reading `.creds` files.
 *
 * An NKey is base32 (RFC 4648, unpadded) of a type prefix, 32 key bytes and a little-endian
 * CRC-16/XMODEM checksum. Seeds carry a two-byte prefix that also encodes the key type. Keys are
 * Ed25519, from `node:crypto`.
 *
 * Error messages never include the seed.
 */
import * as NCrypto from "node:crypto"

const PREFIX_SEED = 18 << 3
const PREFIX_CURVE = 23 << 3
const PUBLIC_PREFIXES = new Set([
  14 << 3, // operator
  0, // account
  20 << 3, // user
  13 << 3, // server
  2 << 3, // cluster
])

// PKCS #8 header for a raw 32-byte Ed25519 private key
const PKCS8_ED25519 = Buffer.from("302e020100300506032b657004220420", "hex")

const ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"

const base32Encode = (bytes: Uint8Array): string => {
  let out = ""
  let buffer = 0
  let bits = 0
  for (const byte of bytes) {
    buffer = (buffer << 8) | byte
    bits += 8
    while (bits >= 5) {
      out += ALPHABET[(buffer >>> (bits - 5)) & 31]
      bits -= 5
    }
  }
  if (bits > 0) out += ALPHABET[(buffer << (5 - bits)) & 31]
  return out
}

const base32Decode = (text: string): Uint8Array => {
  const out: Array<number> = []
  let buffer = 0
  let bits = 0
  for (const char of text) {
    const value = ALPHABET.indexOf(char)
    if (value === -1) throw new Error("invalid NKey: not base32")
    buffer = ((buffer << 5) | value) & 0xffff
    bits += 5
    if (bits >= 8) {
      out.push((buffer >>> (bits - 8)) & 255)
      bits -= 8
    }
  }
  return Uint8Array.from(out)
}

const crc16 = (bytes: Uint8Array): number => {
  let crc = 0
  for (const byte of bytes) {
    crc ^= byte << 8
    for (let i = 0; i < 8; i++)
      crc = crc & 0x8000 ? ((crc << 1) ^ 0x1021) & 0xffff : (crc << 1) & 0xffff
  }
  return crc
}

/** Decodes base32 and verifies the trailing checksum, returning the payload. */
const decode = (text: string): Uint8Array => {
  const raw = base32Decode(text)
  if (raw.length < 4) throw new Error("invalid NKey: too short")
  const payload = raw.subarray(0, raw.length - 2)
  if (crc16(payload) !== (raw[raw.length - 2] | (raw[raw.length - 1] << 8))) {
    throw new Error("invalid NKey: checksum mismatch")
  }
  return payload
}

const encode = (prefix: number, key: Uint8Array): string => {
  const raw = new Uint8Array(key.length + 3)
  raw[0] = prefix
  raw.set(key, 1)
  const checksum = crc16(raw.subarray(0, key.length + 1))
  raw[key.length + 1] = checksum & 255
  raw[key.length + 2] = checksum >>> 8
  return base32Encode(raw)
}

export interface KeyPair {
  /** Public key, like `U...` for a user. */
  readonly publicKey: string
  /** Signs `data` and returns the signature as unpadded base64url. */
  readonly sign: (data: Uint8Array) => string
}

export const fromSeed = (seed: string): KeyPair => {
  const payload = decode(seed.trim())
  if (payload.length !== 34 || (payload[0] & 248) !== PREFIX_SEED)
    throw new Error("invalid NKey: not a seed")
  const prefix = ((payload[0] & 7) << 5) | ((payload[1] & 248) >> 3)
  if (prefix === PREFIX_CURVE) throw new Error("invalid NKey: curve (X) keys cannot sign")
  if (!PUBLIC_PREFIXES.has(prefix)) throw new Error("invalid NKey: unknown key type")
  const privateKey = NCrypto.createPrivateKey({
    key: Buffer.concat([PKCS8_ED25519, payload.subarray(2)]),
    format: "der",
    type: "pkcs8",
  })
  const jwk = NCrypto.createPublicKey(privateKey).export({ format: "jwk" })
  return {
    publicKey: encode(prefix, Buffer.from(jwk.x!, "base64url")),
    sign: (data) => NCrypto.sign(null, data, privateKey).toString("base64url"),
  }
}

/**
 * Reads the user JWT and NKey seed from `.creds` file contents, as written by `nsc generate creds`:
 * each value sits between `-----BEGIN ...-----` and `------END ...------` lines.
 */
export const parseCreds = (contents: string): { readonly jwt: string; readonly seed: string } => {
  const blocks = [...contents.matchAll(/-{3,}[^\n]*-{3,}\r?\n\s*(\S+)\s*\r?\n\s*-{3,}[^\n]*-{3,}/g)]
  if (blocks.length < 2) throw new Error("invalid creds: expected a JWT and an NKey seed")
  return { jwt: blocks[0][1], seed: blocks[1][1] }
}
