# Distributed Counter CRDT — Embedded Demo

NATS server embedded directly in each app binary (`nats-server/v2`). One container per region.

Run with: `task compose`

## Topology

```
global  (8081)
├── asia    (8080)
├── america (8082)
└── europe  (8083)
    ├── spain   (8084)
    ├── france  (8085)
    └── england (8086)
```

Global aggregates from asia, america, and europe. Europe aggregates from spain, france, and england.
