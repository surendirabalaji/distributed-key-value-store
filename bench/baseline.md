# Benchmark Baseline (Unoptimized)

- Git hash: d728717
- Date: 2026-04-20
- Hardware: local laptop, Ubuntu
- Cluster: 3 replicas, localhost

## Latency (single client, 1000 Puts, 950 measured after 50 warmup)

| Metric | Value (ms) |
|--------|-----------|
| avg    | 0.47      |
| p50    | 0.42      |
| p99    | 1.13      |

## Throughput — 50/50 workload (50% Put, 50% Get)

**Peak: 6,033 ops/sec at 32 clients (p50=5.00ms)**

| Clients | Throughput (ops/sec) | avg (ms) | p50 (ms) | p90 (ms) | p99 (ms) |
|---------|---------------------|----------|----------|----------|----------|
| 1       | 2,037               | 0.49     | 0.35     | 0.48     | 1.14     |
| 2       | 3,238               | 0.61     | 0.47     | 0.65     | 1.28     |
| 4       | 4,813               | 0.83     | 0.66     | 0.92     | 1.99     |
| 8       | 5,360               | 1.49     | 1.23     | 1.68     | 2.83     |
| 16      | 5,882               | 2.71     | 2.39     | 3.51     | 5.26     |
| 32      | **6,033**            | 5.29     | 5.00     | 7.13     | 9.86     |
| 64      | 5,812               | 10.98    | 10.78    | 14.58    | 19.16    |

Plot: `bench/lat-tput-50-50.png`

## Throughput — 90/10 workload (90% Get, 10% Put)

**Peak: 4,371 ops/sec at 16 clients (p50=3.33ms)**

Gets currently go through full Raft consensus (`raft_.propose()`), so read-heavy
workloads are not faster than write-heavy ones. This is the baseline for measuring
Part 3 lease-read optimizations.

| Clients | Throughput (ops/sec) | avg (ms) | p50 (ms) | p90 (ms) | p99 (ms) |
|---------|---------------------|----------|----------|----------|----------|
| 1       | 1,436               | 0.69     | 0.43     | 0.72     | 1.31     |
| 2       | 2,238               | 0.89     | 0.60     | 0.99     | 1.75     |
| 4       | 4,212               | 0.94     | 0.83     | 1.39     | 2.43     |
| 8       | 4,124               | 1.93     | 1.70     | 2.75     | 4.15     |
| 16      | **4,371**            | 3.64     | 3.33     | 5.33     | 7.79     |
| 32      | 4,236               | 7.53     | 7.22     | 11.20    | 15.49    |
| 64      | 4,085               | 15.61    | 14.81    | 22.90    | 31.21    |

Plot: `bench/lat-tput-90-10.png`

---

# After Lease Reads (Part 3, Opt 1)

- Git hash: (see commit "Part 3 opt 1: Leader-lease reads for Get")
- Date: 2026-04-21
- Same hardware/cluster as baseline above

Leader-lease fast path: Get bypasses Raft consensus when the leader holds
a valid lease (refreshed on majority AppendEntries acks, revoked on step-down).
`has_committed_current_term_` guards against reading stale inherited state.

## Latency (single client, 1000 Puts, 950 measured after 50 warmup)

Puts unchanged (still go through Raft); Get p50 improvement from warm-lease
serving follower cache hit on 1-client run.

| Metric | Baseline (ms) | Leased (ms) | Delta |
|--------|--------------|-------------|-------|
| avg    | 0.47         | 0.30        | −36%  |
| p50    | 0.42         | 0.28        | −33%  |
| p99    | 1.13         | 0.59        | −48%  |

## Throughput — 50/50 workload (50% Put, 50% Get)

**Peak: 12,654 ops/sec at 8 clients (vs baseline 6,033 @ 32 clients — 2.1×)**

| Clients | Leased (ops/sec) | Baseline (ops/sec) | Δ |
|---------|-----------------|-------------------|---|
| 1       | 2,422           | 2,037             | +19% |
| 2       | 5,161           | 3,238             | +59% |
| 4       | 6,271           | 4,813             | +30% |
| 8       | **12,654**      | 5,360             | **+136%** |
| 16      | 11,769          | 5,882             | +100% |
| 32      | 11,524          | **6,033**         | +91% |
| 64      | 11,327          | 5,812             | +95% |

Plot: `bench/lat-tput-50-50-leased.png`

## Throughput — 90/10 workload (90% Get, 10% Put)

**Peak: 30,857 ops/sec at 32 clients (vs baseline 4,371 @ 16 clients — 7.1×)**

Gets now served from leader memory; throughput scales with client concurrency
until contention on the KV mutex, then flattens.

| Clients | Leased (ops/sec) | Baseline (ops/sec) | Δ |
|---------|-----------------|-------------------|---|
| 1       | 11,676          | 1,436             | +713% |
| 2       | 5,389           | 2,238             | +141% |
| 4       | 11,153          | 4,212             | +165% |
| 8       | 13,875          | 4,124             | +236% |
| 16      | 20,255          | **4,371**         | **+363%** |
| 32      | **30,857**      | 4,236             | **+629%** |
| 64      | 26,741          | 4,085             | +554% |

Plot: `bench/lat-tput-90-10-leased.png`
