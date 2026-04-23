# Distributed Counter CRDT — Cross-Domain Convergence Demo

Demonstrates the true CRDT property of NATS JetStream counter streams: two
independent JetStream domains accumulate counters without coordination, then
converge to the algebraic sum via cross-domain stream sourcing.

| Field           | Value |
| --------------- | ----- |
| **NATS Server** | 2.12+ |
| **nats.go**     | 1.37+ |
| **orbit.go**    | 0.1.1 |
| **Go**          | 1.22+ |
| **Topology**    | 2 standalone JetStream nodes (east/west), gateway-connected |

## Core property demonstrated

**Two domains write independently, sourcing merges them without a coordinator.**

`east` (port 4250, domain `"east"`) and `west` (port 4251, domain `"west"`) each
receive counter increments for their own subjects with no knowledge of each other.
When east is configured to source from west, the totals merge to the algebraic sum
— no master node, no two-phase commit, no shared write path.

This is the CRDT property: the merge operation (algebraic sum) is commutative,
associative, and idempotent. Arrival order doesn't matter; the result is always
correct.

### Why this differs from a single-cluster demo

In a single JetStream cluster, all writes are serialized by the Raft leader —
commutativity is trivially satisfied because there is a total order. The
interesting case is when two independent systems accumulate state separately and
must later reconcile. That is what this demo shows: genuine divergence followed
by convergence.

## Prerequisites

Ensure the shared NATS binary is available (run once from the repo root):

```bash
task server:download
```

## Run

```bash
task start   # starts east (4250) and west (4251)
task run     # runs the convergence demo
task stop    # stops servers and wipes data
```

Or to restart from a clean slate:

```bash
task reset
```

## Expected output

```
East connected: nats://localhost:4250
West connected: nats://localhost:4251
Stream "VOTES" created in both domains (counter + direct)

Writing 100 increments to east and 150 to west concurrently (no coordination)...
East sees votes.east = 100
West sees votes.west = 150
East has no view of votes.west — the domains have diverged.

Sourcing west into east's VOTES stream...
East now sees votes.west = 150 (sourced from west)
East total across both subjects: 100 + 150 = 250

Writing 50 more increments to west...
East caught up: votes.west = 200 (live propagation)

All 300 writes converged correctly.
East and west accumulated independently. Sourcing merged them without coordination.

Streams deleted. Done.
```

## What it covers

1. **Domain isolation** — same stream name (`VOTES`) created independently in two separate JetStream domains; same subject space, no conflict
2. **Concurrent uncoordinated writes** — goroutines fire increments at each domain simultaneously with no shared state
3. **Divergence is expected** — each domain has only its own partial view before sourcing
4. **Cross-domain sourcing** — `StreamSource{Name: "VOTES", Domain: "west"}` wires east to pull from west
5. **CRDT convergence** — east's view of `votes.west` converges to the correct total after sourcing
6. **Live propagation** — subsequent writes to west appear in east automatically, no reconfiguration needed

## Cleanup

```bash
task stop
```

The demo program also deletes both streams before exiting.
