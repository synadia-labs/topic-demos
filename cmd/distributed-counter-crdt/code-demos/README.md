# Distributed Counter CRDT — Embedded Demo

Two variants, same topology, same ports.

## containers/
Separate NATS server container per region alongside its app container.
Run with: `cd containers && task compose`

## embedded/
NATS server embedded directly in each app binary (`nats-server/v2`). One container per region.
Run with: `cd embedded && task compose`

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
