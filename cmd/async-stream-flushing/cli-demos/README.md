# Async Stream Flushing — CLI Demo

| Field | Value |
|-------|-------|
| **Topic** | Async Stream Flushing |
| **Date** | 2026-04-07 |
| **NATS Server** | 2.12.0+ |
| **NATS CLI** | 0.3.2+ |

---

## Prerequisites

### Install

- nats-server 2.12.0+ — [install guide](https://docs.nats.io/running-a-nats-service/introduction/installation)
- nats CLI (natscli) — `go install github.com/nats-io/natscli/nats@latest`

### Server Setup

```bash
nats-server -js
```

JetStream must be enabled with the `-js` flag. The default `sync_interval` is 2 minutes, which is what we want for this demo.

---

## Steps

### Step 1: Create a stream with default (synchronous) persist mode

Create a stream with the default persist mode. Every publish will be fsynced to disk before the server sends an acknowledgement.

```bash
nats stream add SYNC_STREAM --subjects "sync.>" --storage file --replicas 1 --defaults
```

Expected output:
```
Stream SYNC_STREAM was created

Information for Stream SYNC_STREAM created 2026-04-07

              Subjects: sync.>
              Replicas: 1
               Storage: File

...
```

### Step 2: Create a stream with async persist mode

Create a second stream with `--persist-mode=async`. Publishes will be acknowledged before data is fsynced to disk.

```bash
nats stream add ASYNC_STREAM --subjects "async.>" --storage file --replicas 1 --persist-mode=async --defaults
```

Expected output:
```
Stream ASYNC_STREAM was created

Information for Stream ASYNC_STREAM created 2026-04-07

              Subjects: async.>
              Replicas: 1
               Storage: File

...
```

### Step 3: Compare stream configurations

View both streams and confirm the persist mode difference.

```bash
nats stream info SYNC_STREAM -j | grep -i persist
nats stream info ASYNC_STREAM -j | grep -i persist
```

Expected output:
```
  "persist_mode": "default",
  "persist_mode": "async",
```

The sync stream shows `default` (fsync on every write). The async stream shows `async` (deferred fsync).

### Step 4: Publish messages to both streams

Publish 100 messages to each stream and observe the timing difference.

```bash
nats pub sync.test "message {{Count}}" --count 100
nats pub async.test "message {{Count}}" --count 100
```

Expected output (per stream):
```
12:00:00 Published 11 bytes to "sync.test"
...
12:00:01 Published 13 bytes to "sync.test"
```

Both streams should receive all 100 messages. The async stream may show slightly faster completion time, but the difference is more visible with larger message counts (see Step 6).

### Step 5: Verify both streams have the same data

Confirm both streams stored all messages.

```bash
nats stream info SYNC_STREAM --state
nats stream info ASYNC_STREAM --state
```

Expected output (for each):
```
State:

             Messages: 100
                Bytes: ...
             FirstSeq: 1
              LastSeq: 100
```

Both streams hold the same 100 messages. The difference is in when the server called fsync, not whether the data is stored.

### Step 6: Benchmark sync vs. async throughput

Use the built-in benchmark to publish 50,000 messages to each stream and compare throughput.

```bash
nats bench js pub sync bench.sync --msgs 50000 --size 1KB --create --storage file --replicas 1 --purge --stream benchsync
```

Expected output:
```
Starting JetStream publish benchmark [msgs=50,000, size=1.0 KiB, storage=file, stream=benchsync, ...]

Pub stats: 50,000 msgs in ... msg/s ...
```

Now benchmark with async persist mode:

```bash
nats bench js pub sync bench.async --msgs 50000 --size 1KB --create --storage file --replicas 1 --purge --persistasync --stream benchasync
```

Expected output:
```
Starting JetStream publish benchmark [msgs=50,000, size=1.0 KiB, storage=file, stream=benchasync, ...]

Pub stats: 50,000 msgs in ... msg/s ...
```

The async benchmark should show higher msg/s throughput. The gap depends on your disk: on fast SSDs the improvement may be modest (1.2-2x), while on spinning disks or cloud storage it can be 3-5x or more because fsync is much slower on those devices.

### Step 7: Verify the constraint — async mode requires R=1

Try to create a replicated stream with async persist mode. The server will reject it.

```bash
nats stream add SHOULD_FAIL --subjects "fail.>" --storage file --replicas 3 --persist-mode=async --defaults 2>&1 || true
```

Expected output:
```
nats: error: could not create Stream: async persist mode is not supported on replicated streams (10XXX)
```

This confirms the constraint: `persist_mode: async` is only valid for single-replica streams. Replicated streams get async flushing automatically via the Raft log — the explicit setting is not needed and is rejected.

### Step 8: Verify the constraint — async mode requires file storage

Try to create a memory-storage stream with async persist mode. The server will reject it.

```bash
nats stream add SHOULD_FAIL_MEM --subjects "failmem.>" --storage memory --replicas 1 --persist-mode=async --defaults 2>&1 || true
```

Expected output:
```
nats: error: could not create Stream: async persist mode is only supported on file storage (10XXX)
```

Memory storage does not support async persist mode because it has no disk to flush — it is inherently volatile.

### Step 9: Stream report

View both streams side by side.

```bash
nats stream report
```

Expected output:
```
╭────────────────────────────────────────────────────────────╮
│                      Stream Report                         │
├──────────────┬─────────┬──────────┬────────┬───────────────┤
│ Stream       │ Storage │ Messages │ Bytes  │ Consumers     │
├──────────────┼─────────┼──────────┼────────┼───────────────┤
│ ASYNC_STREAM │ File    │ 100      │ ...    │ 0             │
│ SYNC_STREAM  │ File    │ 100      │ ...    │ 0             │
│ benchstream  │ File    │ 50000    │ ...    │ 0             │
╰──────────────┴─────────┴──────────┴────────┴───────────────╯
```

---

## Go Demo

For a complete Go program that extends this CLI demo into a real-world pattern, see the [`go-demo/`](../go-demo/) directory.

```bash
cd ../go-demo
go run main.go
```

---

## Cleanup

```bash
nats stream rm SYNC_STREAM --force
nats stream rm ASYNC_STREAM --force
nats stream rm benchsync --force
nats stream rm benchasync --force
```
