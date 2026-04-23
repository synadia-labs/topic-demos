# Distributed Counter CRDT — Quick Reference

## Setup

```bash
nats-server -js
```

## Steps

### 1. Create a counter-enabled stream

```bash
nats stream add COUNTER \
  --subjects="counter.>" \
  --defaults \
  --allow-counter
```

### 2. Verify counter config

```bash
nats stream info COUNTER --json | grep -E '"allow_(msg_counter|direct)"'
```

### 3. Increment a counter

```bash
nats req counter.page.views "" -H "Nats-Incr:+1" --raw
```

### 4. Large increment (arbitrary precision)

```bash
nats req counter.page.views "" -H "Nats-Incr:+999999999999999999" --raw
```

### 5. Read the current total

```bash
nats stream get COUNTER --last-for "counter.page.views"
```

### 6. Decrements and interleaved deltas

```bash
nats req counter.page.views "" -H "Nats-Incr:-100" --raw
nats req counter.page.views "" -H "Nats-Incr:+50" --raw
nats req counter.page.views "" -H "Nats-Incr:-10" --raw
```

### 7. Multiple independent subjects

```bash
nats req counter.orders.placed "" -H "Nats-Incr:+1" --raw
nats req counter.orders.placed "" -H "Nats-Incr:+1" --raw
nats stream get COUNTER --last-for "counter.orders.placed"
```

### 8. Plain publish is rejected

```bash
nats req counter.page.views "not a counter update" --raw
```

### 9. Reset with matching negative delta

```bash
CURRENT=$(nats stream get COUNTER --last-for "counter.page.views" 2>&1 \
  | grep -oE '"val":"[^"]+"' | tail -1 | cut -d'"' -f4)
nats req counter.page.views "" -H "Nats-Incr:-$CURRENT" --raw
nats stream get COUNTER --last-for "counter.page.views"
```

## Cleanup

```bash
nats stream rm COUNTER -f
```
