# Results

## 1. Leader election after a leader crash (5 nodes, 5-20 ms message delay)

| election_timeout_ms | no_initial_leader_in_20s | no_leader_20s_after_crash | measured | median_ms | p95_ms | max_ms |
|---|---|---|---|---|---|---|
| 150-151 | 152/300 | 110 | 38 | 1410.0 | 16500.8 | 18070.0 |
| 150-155 | 185/300 | 78 | 37 | 350.0 | 16152.0 | 19325.0 |
| 150-175 | 0/300 | 0 | 300 | 327.5 | 1155.5 | 2150.0 |
| 150-200 | 0/300 | 0 | 300 | 175.0 | 565.0 | 1050.0 |
| 150-300 | 0/300 | 0 | 300 | 190.0 | 380.3 | 630.0 |

## 2. Writes under message loss (500 writes, one every 20 ms)

| drop_rate | acked_% | p50_latency_ms | p99_latency_ms | messages_per_write | safety_errors |
|---|---|---|---|---|---|
| 0.0 | 100.0 | 19.6 | 25.9 | 92.4 | 0 |
| 0.05 | 100.0 | 21.2 | 27.9 | 57.9 | 0 |
| 0.1 | 100.0 | 22.3 | 30.2 | 41.7 | 0 |
| 0.2 | 100.0 | 24.5 | 37.0 | 26.1 | 0 |
| 0.3 | 100.0 | 27.6 | 55.8 | 18.5 | 0 |

## 3. Chaos testing

300 randomised runs (crashes, restarts, partitions, 5% loss, 300 writes each): **0 runs with safety violations**, 85975 acknowledged writes checked (18.8 s).

## 4. Bug injection (same 300 chaos runs)

| injected_bug | runs_with_violations | first_failing_run | violation_types |
|---|---|---|---|
| vote ignores log up-to-dateness | 179/300 | 0 | ['lost acknowledged write', 'state-machine safety'] |
| commit by counting old-term entries | 0/300 | None | [] |
