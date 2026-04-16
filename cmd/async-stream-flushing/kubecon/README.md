# Async Stream Flush — KubeCon Benchmarks

Shell and PowerShell scripts that run `nats bench` across 8 message sizes and 3 batch sizes, producing output that can be loaded into the HTML visualizers.

## R1 (single-replica, sync vs async persist mode)

- Start the cluster: `task server:super`
- Run the bench: `./bench-r1.sh > output-r1.txt` (or `.\bench-r1.ps1 > output-r1.txt`)
- Open `r1.html` in a browser and load `output-r1.txt`

## R3 (3-replica, NATS 2.11 vs 2.12)

- Start with 2.11: `task server:super VERSION=2.11.x`
- Run the bench: `./bench-r3.sh > output-r3.txt`
- Stop and wipe: `task server:stop`
- Start with 2.12: `task server:super VERSION=2.12.x`
- **Append** to the same file: `./bench-r3.sh >> output-r3.txt`
- Open `r3.html` in a browser and load `output-r3.txt`
