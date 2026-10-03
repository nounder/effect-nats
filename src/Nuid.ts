const DIGITS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
const BASE = 62
const PREFIX_LENGTH = 12
const SEQ_LENGTH = 10
// 10 base62 digits fit 62^10, but the sequence stays within exact float range.
const MAX_SEQ = Number.MAX_SAFE_INTEGER
const MIN_INC = 33
const MAX_INC = 333

const randomUint32 = () => crypto.getRandomValues(new Uint32Array(1))[0]

/**
 * Generator of NATS unique identifiers: a 12 char random prefix followed by a
 * 10 char sequence incremented by a random step. Same scheme as `nats.js`, so
 * ids are 22 chars long and safe to use in subject tokens.
 */
class NuidGenerator {
  #prefix = ""
  #seq = 0
  #inc = 0

  constructor() {
    this.#reset()
  }

  next(): string {
    this.#seq += this.#inc
    if (this.#seq >= MAX_SEQ) this.#reset()
    let seq = this.#seq
    let out = ""
    for (let i = 0; i < SEQ_LENGTH; i++) {
      out = DIGITS[seq % BASE] + out
      seq = Math.floor(seq / BASE)
    }
    return this.#prefix + out
  }

  #reset() {
    const bytes = new Uint8Array(PREFIX_LENGTH)
    crypto.getRandomValues(bytes)
    let prefix = ""
    for (let i = 0; i < PREFIX_LENGTH; i++) prefix += DIGITS[bytes[i] % BASE]
    this.#prefix = prefix
    this.#seq = randomUint32() * 2 ** 20
    this.#inc = MIN_INC + (randomUint32() % (MAX_INC - MIN_INC))
  }
}

const shared = new NuidGenerator()

export const next = (): string => shared.next()

export const createInbox = (prefix = "_INBOX"): string => `${prefix}.${shared.next()}`
