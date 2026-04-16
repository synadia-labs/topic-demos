# Async Stream Flushing — Quick Reference

## Setup

```bash
nats-server -js
```

## Steps

### 1. Create stream with default (sync) persist mode

```bash
nats stream add SYNC_STREAM --subjects "sync.>" --storage file --replicas 1 --defaults
```

### 2. Create stream with async persist mode

```bash
nats stream add ASYNC_STREAM --subjects "async.>" --storage file --replicas 1 --persist-mode=async --defaults
```

### 3. Compare persist mode in stream config

```bash
nats stream info SYNC_STREAM -j | grep -i persist
nats stream info ASYNC_STREAM -j | grep -i persist
```

### 4. Publish messages to both streams

```bash
nats pub sync.test "message {{Count}}" --count 100
nats pub async.test "message {{Count}}" --count 100
```

### 5. Verify message counts

```bash
nats stream info SYNC_STREAM --state
nats stream info ASYNC_STREAM --state
```

### 6. Benchmark sync throughput

```bash
nats bench js pub sync bench.sync --msgs 50000 --size 1KB --create --storage file --replicas 1 --purge --stream benchsync
```

### 7. Benchmark async throughput

```bash
nats bench js pub sync bench.async --msgs 50000 --size 1KB --create --storage file --replicas 1 --purge --persistasync --stream benchasync
```

### 8. Test constraint — async requires R=1

```bash
nats stream add SHOULD_FAIL --subjects "fail.>" --storage file --replicas 3 --persist-mode=async --defaults 2>&1 || true
```

### 9. Test constraint — async requires file storage

```bash
nats stream add SHOULD_FAIL_MEM --subjects "failmem.>" --storage memory --replicas 1 --persist-mode=async --defaults 2>&1 || true
```

### 10. Stream report

```bash
nats stream report
```

## Cleanup

```bash
nats stream rm SYNC_STREAM --force
nats stream rm ASYNC_STREAM --force
nats stream rm benchsync --force
nats stream rm benchasync --force
```
