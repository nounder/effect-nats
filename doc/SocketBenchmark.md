# Socket read-path benchmark

**Question:** if `NatsConnection` read from an `effect/socket` `Socket`
(`NodeSocket.makeNet` + `reader.pull`) instead of raw `node:net` `data`
events, as `@effect/sql-pg` does, how much read throughput would it lose?

**Answer:** a fixed cost of about **3–6 µs per socket read** (per ~64 KiB
chunk on Node). For small messages (≤ 128 B), where parsing dominates, that is
**~2%**, within noise. For 1–16 KiB payloads it is **7–12% less throughput**
and up to **~40% more CPU per MB** on Node. Paused-mode reads themselves cost
nothing; the overhead is the Effect layer around each pull.

## Setup

- Apple M1 Max (10 cores), macOS 27.0.1, loopback TCP
- Node v24.19.0, Bun 1.4.4
- `effect` 4.0.0, `@effect/platform-node` 4.0.0 (`@effect/platform-node-shared` 4.0.0)
- Script: [`bench/socket-read.ts`](../bench/socket-read.ts)

A separate `node` process serves pre-encoded NATS `MSG` frames as fast as the
socket accepts them, so the sender is the same for every client variant. The
client decodes every frame with the real `NatsProtocol` parser and counts
messages. Only the way bytes get from the socket to the parser differs:

| variant                                  | what it is                                                                                            |
| ---------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| raw `node:net` `data` events             | what `NatsConnection` does today (same as `@effect/sql-pg`)                                           |
| raw `node:net` paused `read()` (control) | no Effect, but the paused `readable` + `read()` style that `NodeSocket` uses internally               |
| `Socket` `reader.pull` loop              | a fiber looping on `reader.pull` from `NodeSocket.makeNet`: what a `Socket`-based connection would do |
| `Socket.toStream` + `runForEach`         | idiomatic Effect streaming over the same socket, for reference                                        |

Each run transfers ~100 MiB. Every variant is warmed up once, then 7 runs are
interleaved across variants so drift affects all of them equally; tables show
medians. "CPU ms / 100 MB" is process-wide `process.cpuUsage()` (user +
system, all threads, including GC). Run-to-run differences of about ±2% are
noise: two separate Node runs agreed within 1–2 points.

## Results

### Node v24.19.0

| payload | variant                                  | msgs/s | MB/s | CPU ms / 100 MB | vs raw |
| ------: | ---------------------------------------- | -----: | ---: | --------------: | -----: |
|    16 B | raw `node:net` `data` events             | 4.47 M |  179 |             558 |      — |
|    16 B | raw `node:net` paused `read()` (control) | 4.43 M |  177 |             565 |  -1.0% |
|    16 B | `Socket` `reader.pull` loop              | 4.39 M |  176 |             572 |  -1.9% |
|    16 B | `Socket.toStream` + `runForEach`         | 4.38 M |  175 |             574 |  -2.0% |
|   128 B | raw `node:net` `data` events             | 3.82 M |  565 |             178 |      — |
|   128 B | raw `node:net` paused `read()` (control) | 3.79 M |  560 |             179 |  -0.7% |
|   128 B | `Socket` `reader.pull` loop              | 3.73 M |  552 |             182 |  -2.3% |
|   128 B | `Socket.toStream` + `runForEach`         | 3.73 M |  552 |             181 |  -2.2% |
|  1024 B | raw `node:net` `data` events             | 1.56 M | 1568 |             111 |      — |
|  1024 B | raw `node:net` paused `read()` (control) | 1.53 M | 1534 |             110 |  -2.2% |
|  1024 B | `Socket` `reader.pull` loop              | 1.44 M | 1448 |             119 |  -7.6% |
|  1024 B | `Socket.toStream` + `runForEach`         | 1.46 M | 1462 |             124 |  -6.8% |
| 16384 B | raw `node:net` `data` events             |  154 k | 2417 |              85 |      — |
| 16384 B | raw `node:net` paused `read()` (control) |  154 k | 2405 |              86 |  -0.5% |
| 16384 B | `Socket` `reader.pull` loop              |  136 k | 2130 |             121 | -11.9% |
| 16384 B | `Socket.toStream` + `runForEach`         |  141 k | 2212 |             114 |  -8.5% |

### Bun 1.4.4

| payload | variant                                  | msgs/s | MB/s | CPU ms / 100 MB | vs raw |
| ------: | ---------------------------------------- | -----: | ---: | --------------: | -----: |
|    16 B | raw `node:net` `data` events             | 6.24 M |  250 |             462 |      — |
|    16 B | raw `node:net` paused `read()` (control) | 6.17 M |  247 |             465 |  -1.2% |
|    16 B | `Socket` `reader.pull` loop              | 6.04 M |  242 |             479 |  -3.2% |
|    16 B | `Socket.toStream` + `runForEach`         | 6.08 M |  244 |             474 |  -2.6% |
|   128 B | raw `node:net` `data` events             | 5.08 M |  751 |             158 |      — |
|   128 B | raw `node:net` paused `read()` (control) | 5.08 M |  751 |             160 |   0.0% |
|   128 B | `Socket` `reader.pull` loop              | 4.97 M |  735 |             162 |  -2.2% |
|   128 B | `Socket.toStream` + `runForEach`         | 5.00 M |  739 |             161 |  -1.6% |
|  1024 B | raw `node:net` `data` events             | 2.16 M | 2164 |              57 |      — |
|  1024 B | raw `node:net` paused `read()` (control) | 2.19 M | 2200 |              54 |   1.7% |
|  1024 B | `Socket` `reader.pull` loop              | 2.02 M | 2025 |              62 |  -6.4% |
|  1024 B | `Socket.toStream` + `runForEach`         | 2.05 M | 2060 |              61 |  -4.8% |
| 16384 B | raw `node:net` `data` events             |  250 k | 3915 |              31 |      — |
| 16384 B | raw `node:net` paused `read()` (control) |  250 k | 3915 |              35 |   0.0% |
| 16384 B | `Socket` `reader.pull` loop              |  228 k | 3572 |              37 |  -8.8% |
| 16384 B | `Socket.toStream` + `runForEach`         |  227 k | 3552 |              37 |  -9.3% |

## Analysis

**The cost is per read, not per message.** On Node every variant sees the
same chunking: about 1,600 reads of ~64 KiB per 100 MiB, in flowing and paused
mode alike. The `Socket` pull loop takes about 5–6 ms longer per 100 MiB at
both 1 KiB and 16 KiB payloads, and ~10 ms at 16 B (noisier), i.e. roughly
**3–6 µs per pull**: the fiber suspending in `Effect.callback`, being resumed
by the `readable` event through the Effect scheduler, and the allocations
around it.

**Why it shows up only for larger payloads.** A 64 KiB chunk holds ~1,500
messages at 16 B but only ~4 at 16 KiB. With small messages the parser's
per-message work hides the fixed per-pull cost (~2%); with large messages
there is little work per chunk, so the same few microseconds become 7–12% of
the total, and up to ~40% more CPU per MB on Node (85 → 121 ms per 100 MB).

**Paused mode is not the cause.** The control reads the socket exactly like
`NodeSocket` (paused, `readable` + `read()`) without Effect and matches raw
`data` events within noise on both runtimes. (Bun's paused `read()` returns
larger merged buffers, ~280 KiB vs ~170 KiB per `data` event, without a
measurable cost.)

**`toStream` is no worse than a hand-written pull loop.** Idiomatic
`Socket.toStream` + `Stream.runForEach` lands within a few percent of the pull
loop, sometimes ahead.

## What it means for `effect-nats`

- Typical NATS traffic is small messages, where switching to `Socket` costs
  ~2% on this path. In the real connection each message also goes through
  `Msg` allocation, `Queue.offerUnsafe` and a consuming fiber, which this
  benchmark does not include, so the relative cost there should be smaller
  still (not measured).
- Bulk payloads (1–16 KiB and up) would lose 7–12% throughput and use more
  CPU per MB. Probably acceptable, but not free; this is a plausible part of
  why `@effect/sql-pg` reads its socket directly.
- Not measured here: the write path, TLS, and WebSockets (which have no
  raw-socket alternative to compare against). Absolute numbers are loopback
  on one machine; the relative differences are the useful part.

## Recommendation

Keep the current design: raw `node:net`/`node:tls` for TCP, `effect/socket`
for WebSocket (through `src/internal/WebSocketDuplex.ts`). Do not move TCP to
`Socket` for now.

- **The cost buys nothing yet.** Moving TCP to `Socket` would make the
  library portable beyond Node, but Node is the target for now. Today's path
  has no read overhead at all.
- **TLS would get worse.** `Socket.TlsUpgradeOptions` (used by
  `reader.upgrade` for NATS's INFO-then-upgrade flow) has no `servername`, so
  there would be no SNI and no custom `checkServerIdentity`, `minVersion`,
  ciphers and so on. Today `tls` accepts the full `node:tls` options. Losing
  SNI breaks servers behind SNI-routing proxies. Fixing it means writing our
  own TCP `Socket` or adding the option to Effect.
- **TCP would need a new required dependency**, `@effect/platform-node`
  (`NodeSocket.makeNet`). The library currently depends only on `effect`.

**When to revisit:** when the library should run outside Node (browsers,
Deno). Even then, do not move everything to `Socket`. Instead, replace the
Node `Duplex` at the transport boundary with a small internal transport
interface that has two adapters:

- raw `node:net`/`node:tls` for TCP: no read overhead, full TLS options;
- `effect/socket` for WebSocket and every other runtime.

That removes `node:stream` from the connection core while keeping TCP's speed
and TLS options. If non-Node support becomes a near-term goal, do this
refactor then rather than switching TCP to `Socket`.

## Reproduce

```sh
node bench/socket-read.ts
bun bench/socket-read.ts
```
