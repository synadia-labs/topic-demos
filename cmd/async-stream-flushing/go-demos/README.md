# Async Stream Flushing — Go Benchmarks

Two Go programs that measure the throughput and latency difference between JetStream streams using **sync** (default) vs **async** persist modes.

## Background

When a JetStream stream uses the default persist mode, the server calls `fsync` on every published message before sending the publish acknowledgement. This ensures the data is durable on disk before your publisher moves on, but it means each publish round-trip includes the cost of a disk flush.

With `PersistMode: jetstream.AsyncPersistMode`, the server sends the ack as soon as the message is written to the OS buffer — `fsync` happens in the background on its own schedule. Publishes complete faster, at the cost of a small crash-recovery window where buffered-but-not-flushed messages could be lost.

Async persist mode is only valid on single-replica file-storage streams. Replicated streams already get async-style batching through the Raft log.

---

## Programs

### `throughput/main.go` — Throughput benchmark

Publishes 50,000 × 1 KB messages sequentially to a sync stream and then to an async stream, one message at a time, waiting for each ack before sending the next. Prints a live msg/s counter while running, then a summary table with the speedup ratio.

```
Publishing to sync stream (fsync per message)...
  Sync :  50000 / 50000  (  8432 msg/s)  [5.931s]

Publishing to async stream (deferred fsync)...
  Async:  50000 / 50000  ( 44217 msg/s)  [1.131s]

  ┌─────────────────────────────────────────────┐
  │  Sync  (default)      8432 msg/s            │
  │  Async               44217 msg/s            │
  └─────────────────────────────────────────────┘

  → Async is 5.2x faster (no per-message fsync)
```

The speedup is disk-dependent. Fast NVMe SSDs have cheap `fsync` so the gap may be 1.2–2×. On cloud VMs with network-attached storage, or spinning disks, `fsync` is much slower and async mode can be 3–10× faster.

Run it:

```bash
go run ./throughput/
```

---

### `latency/main.go` — Per-message latency breakdown

Publishes 10,000 × 1 KB messages to each stream and records the round-trip time for each individual publish. Prints p50/p90/p99/max and a visual histogram showing how latencies are distributed across buckets (`< 50µs`, `< 100µs`, up to `>= 10ms`).

```
────────────────────────────────────────────────────────────
  Sync (fsync per message)
    p50:       1.823ms
    p90:       2.104ms
    p99:       3.512ms
    max:      14.201ms

    < 50µs │                                           0.0% (0)
    < 100µs│                                           0.0% (0)
    < 250µs│                                           0.2% (9)
    < 500µs│█                                          1.3% (67)
    <   1ms│████████                                  15.4% (772)
    <   5ms│████████████████████████████████████████  81.3% (4064)
    <  10ms│█                                          1.6% (82)
    >= 10ms│                                           0.1% (6)

  Async (deferred fsync)
    p50:         98µs
    p90:        187µs
    p99:        412µs
    max:       2.103ms
    ...
```

Where throughput shows aggregate throughput difference, latency shows how the distribution shifts — async mode moves the bulk of publishes from the millisecond range down into the microsecond range.

Run it:

```bash
go run ./latency/
```

---

## Prerequisites

A NATS server with JetStream enabled must be running on `nats://localhost:4222`:

```bash
nats-server -js
```

Both programs create and clean up their own streams (`SYNC_DEMO`, `ASYNC_DEMO`).
