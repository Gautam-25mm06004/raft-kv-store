# Implementing and Stress-Testing Raft with Deterministic Fault Injection

**Gautam Jain** — IIT Bhubaneswar

## Abstract
We implement the Raft consensus algorithm with a replicated key-value state machine and test it in a deterministic
discrete-event simulator that injects message delay and loss, partitions, and crash-restarts. Across 300 randomised fault
schedules with 85,975 acknowledged writes, no safety property is violated. Injecting a single bug in the vote rule produces
violations in 60% of the same schedules, demonstrating that the harness has real detection power; an injected commit-rule
bug that requires a specific interleaving was not detected, illustrating the limits of random testing.

## 1. Implementation
Each node keeps persistent state (current term, vote, log) and volatile state (role, commit index, state machine). Nodes
are pure message handlers: they return messages to send, which keeps them deterministic and testable.
- **Election**: randomised timeout; candidates request votes with (last log term, last log index); a vote is granted at most
  once per term and only to candidates whose log is at least as up to date.
- **Replication**: AppendEntries carries (prev index, prev term); followers reject on mismatch and return a hint; the leader
  backs off next-index; followers truncate only conflicting suffixes.
- **Commit**: the leader advances its commit index to the highest index replicated on a majority whose entry is from its
  current term; a new leader appends a no-op entry to commit earlier entries promptly.
- **Clients**: requests go to a random server and are redirected to the leader; unacknowledged requests are retried.

## 2. Simulator and checks
Events (ticks every 10 ms, message deliveries with 5–20 ms delay, client requests) are processed from a priority queue with a
single seeded RNG. Faults: crash (node stops), restart (volatile state reset, log retained), partition into two groups,
heal, and independent message loss. After a run we check (i) at most one leader per term, (ii) log matching, (iii) no index
applied with different commands on different nodes, (iv) every client-acknowledged write is present in every live node's
committed log.

## 3. Results
See README. Election latency depends strongly on timeout spread: with 150–300 ms the median outage after a leader crash is
190 ms and the p95 380 ms, whereas a 1 ms spread leaves the cluster leaderless for more than 20 s in many trials. Under up to
30% message loss all writes commit, with p99 latency rising to 56 ms.

## 4. Limitations
In-process simulation rather than real sockets and disks; no log compaction/snapshots, no membership changes, no read-index
or lease reads; client retries can apply a write twice (no deduplication table), which affects exactly-once semantics but
not Raft safety.

## References
1. D. Ongaro, J. Ousterhout. In search of an understandable consensus algorithm. *USENIX ATC* 2014.
2. D. Ongaro. *Consensus: Bridging Theory and Practice*. PhD thesis, Stanford University, 2014.
3. K. Kingsbury. Jepsen analyses (jepsen.io) — methodology for testing distributed systems under faults.
