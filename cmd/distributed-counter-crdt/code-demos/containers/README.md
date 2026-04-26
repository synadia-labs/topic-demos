# Distributed Counter CRDT — Embedded Demo

## Dashboards

| Node    | URL                   |
|---------|-----------------------|
| Global  | http://localhost:8081 |
| Asia    | http://localhost:8080 |
| America | http://localhost:8082 |
| Europe  | http://localhost:8083 |
| Spain   | http://localhost:8084 |
| France  | http://localhost:8085 |
| England | http://localhost:8086 |

## Running

```
docker compose up --build
```

## Topology

```
global
├── asia
├── america
└── europe
    ├── spain
    ├── france
    └── england
```

Global aggregates from asia, america, and europe. Europe aggregates from spain, france, and england.
