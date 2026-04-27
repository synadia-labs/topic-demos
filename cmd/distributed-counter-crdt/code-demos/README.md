# Distributed Counter CRDT — Embedded Demo

NATS server embedded directly in each app binary (`nats-server/v2`). One container per region.

Run with: `task compose`

## Debugging a single service

To debug one service locally while the rest run in Docker:

1. Run all services except the target: `task compose:except-<service>`  
   e.g. `task compose:except-america`

2. In VS Code, select the matching `code-demo: <service>` config in the Run & Debug panel and press F5.

The local process connects to the Docker cluster via the exposed NATS leaf ports (`localhost:7422` for global-connected nodes, `localhost:7423` for europe-connected nodes). Non-asia services use `HTTP_ADDR=:8089` to avoid conflicting with asia's host port mapping.

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
