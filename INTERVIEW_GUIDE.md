# Interview guide — Raft KV store

## 30-second version
"I implemented Raft from the paper — leader election, log replication and the commit rules — with a key-value store on top.
To test it I built a deterministic simulator that injects message loss, partitions and crashes, plus a checker for Raft's
safety properties. 300 random chaos runs with 86,000 acknowledged writes found zero violations; when I deliberately broke the
voting rule, the same tests caught it in 179 of 300 runs."

## Must-know
- **Why consensus**: replicate a log so all servers apply the same commands in the same order, tolerating ⌊(n−1)/2⌋ failures.
- **Terms**: logical clock; any message with a higher term makes you step down to follower.
- **Election**: follower times out → candidate → asks for votes; majority → leader. Randomised timeouts avoid split votes.
- **Up-to-date check**: vote only for candidates whose (last term, last index) ≥ yours. This guarantees the new leader has
  every committed entry (leader completeness). Removing it lost acknowledged writes in my bug-injection test.
- **Log matching**: AppendEntries includes the previous index and term; mismatch → reject → leader backs up.
- **Commit rule**: an entry is committed once on a majority *and* from the leader's current term (Figure 8 problem).
- **Determinism**: the node returns messages instead of sending them, so the simulator controls all nondeterminism.

## Likely questions
1. *What happens in a partition?* The majority side elects a leader and makes progress; the minority leader cannot commit;
   after healing, the minority's uncommitted entries are overwritten (tested).
2. *How do you make reads linearizable?* Go through the log, or use read-index / leader leases.
3. *Why didn't chaos testing catch the Figure 8 bug?* It needs a precise sequence of leader changes; random schedules rarely
   produce it. That's why you also write targeted scenario tests or use model checking (TLA+).
4. *What's missing for production?* Snapshots/log compaction, membership changes, persistence with fsync, client dedup.
