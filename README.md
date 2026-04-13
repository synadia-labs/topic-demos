# NATS Topic Demos

Runnable demos for NATS JetStream features, backed by a self-contained local server harness. Each demo lives under `cmd/` and ships with a CLI walkthrough and a Go program.

## Prerequisites

| Tool | Version | Install |
|---|---|---|
| Go | 1.21+ | [go.dev/dl](https://go.dev/dl/) |
| Task | any | [taskfile.dev](https://taskfile.dev) |
| nats CLI | 0.1.6+ | `go install github.com/nats-io/natscli/nats@latest` |

`nats-server` is downloaded automatically by the Task commands — no manual install needed.

## Server Harness

Task manages the NATS server lifecycle. The server binary is fetched from `binaries.nats.dev` on first use and cached at `server/bin/`.

```bash
task server:single       # single-node JetStream (port 4222)
task server:cluster      # 3-node JetStream cluster (ports 4222–4224)
task server:super        # supercluster: 3 clusters × 3 nodes + 7 leaf nodes (16 servers)

task server:stop         # stop all running servers
task server:wipe         # stop + delete all JetStream data
task server:status       # health check all known endpoints

task server:reset:single   # wipe + restart single-node
task server:reset:cluster  # wipe + restart 3-node cluster
task server:reset:super    # wipe + restart supercluster
```

After starting, a NATS CLI context is automatically saved and selected so `nats` commands work immediately without a `--server` flag.

## Demos

### Delayed Message Scheduling

**`cmd/delayed-message-scheduling/`** — NATS JetStream Message Scheduler (requires nats-server 2.14+)

Demonstrates server-side deferred delivery without client-side polling or timers:

- **One-shot delay** (`@at <RFC3339>`) — deliver a message at an exact future time
- **Recurring schedule** (`@every <duration>`) — repeat delivery on a fixed interval
- **Subject sampling** — periodically re-publish the latest value from a high-frequency subject (data reduction)
- **Schedule cancellation** — purge a schedule subject to stop delivery

```bash
task server:single
go run ./cmd/delayed-message-scheduling
```

For a step-by-step CLI walkthrough, see [`cmd/delayed-message-scheduling/demo/README.md`](cmd/delayed-message-scheduling/demo/README.md). For a quick reference card, see [`QUICK.md`](cmd/delayed-message-scheduling/demo/QUICK.md).

## Repo Layout

```
cmd/
  delayed-message-scheduling/   # Go demo + CLI walkthrough
    main.go
    demo/
      README.md                 # step-by-step CLI walkthrough
      QUICK.md                  # quick reference card
server/
  bin/                          # downloaded nats-server binary (gitignored)
  conf/
    shared.conf                 # shared auth / accounts config
    single/                     # single-node config
    cluster/                    # 3-node cluster configs
    super/                      # supercluster + leaf node configs
  data/                         # JetStream store dirs (gitignored)
  logs/                         # server logs (gitignored)
Taskfile.yml                    # server lifecycle tasks
```
