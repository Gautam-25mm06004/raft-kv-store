import sys, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from raft import Cluster, check_safety, LEADER  # noqa
from raft.chaos import chaos_run  # noqa
from raft.node import RaftNode  # noqa


class TestRaft(unittest.TestCase):
    def test_elects_single_leader(self):
        c = Cluster(5, seed=1)
        self.assertIsNotNone(c.first_leader_time())
        c.run_until(c.now + 1000)
        self.assertEqual(sum(n.role == LEADER for n in c.nodes.values()), 1)

    def test_replicates_writes_to_all(self):
        c = Cluster(5, seed=2)
        c.first_leader_time()
        for k in range(20):
            c.submit(f"r{k}", ("set", f"x{k}", k))
        c.run_until(c.now + 2000)
        self.assertEqual(len(c.acked), 20)
        kvs = [n.kv for n in c.nodes.values()]
        self.assertTrue(all(kv == kvs[0] for kv in kvs))
        self.assertEqual(kvs[0]["x7"], 7)

    def test_leader_crash_keeps_acknowledged_writes(self):
        c = Cluster(5, seed=3)
        c.first_leader_time()
        for k in range(10):
            c.submit(f"a{k}", ("set", "a", k))
        c.run_until(c.now + 1500)
        old = c.leader()
        c.crash(old)
        c.run_until(c.now + 2000)
        new = c.leader()
        self.assertIsNotNone(new)
        self.assertNotEqual(new, old)
        self.assertEqual(len(c.acked), 10)
        last = max(c.acked.values())                    # highest log index among acknowledged writes
        self.assertEqual(c.nodes[new].kv["a"], last[2][2])
        self.assertEqual(check_safety(c), [])

    def test_minority_partition_cannot_commit(self):
        c = Cluster(5, seed=4)
        c.first_leader_time()
        L = c.leader()
        others = [i for i in c.ids if i != L]
        c.partition([[L, others[0]], others[1:]])        # old leader isolated with one follower
        c.submit("m1", ("set", "z", 1), at=c.now)
        c.nodes[L].client_request("direct", ("set", "z", "minority"), c.now)
        c.run_until(c.now + 3000)
        self.assertTrue(all(c.nodes[i].kv.get("z") != "minority" for i in others[1:]))
        maj_leader = [i for i in others[1:] if c.nodes[i].role == LEADER]
        self.assertEqual(len(maj_leader), 1)
        c.heal()
        c.run_until(c.now + 3000)
        vals = {n.kv.get("z") for n in c.nodes.values()}
        self.assertEqual(len(vals), 1)                   # everyone converged on the majority's history
        self.assertNotIn("minority", vals)
        self.assertEqual(check_safety(c), [])

    def test_chaos_runs_are_safe(self):
        for seed in range(8):
            c, errs = chaos_run(seed, duration=8000, writes=80)
            self.assertEqual(errs, [], f"seed {seed}: {errs[:3]}")

    def test_restart_recovers_state_from_log(self):
        c = Cluster(3, seed=5)
        c.first_leader_time()
        for k in range(5):
            c.submit(f"s{k}", ("set", "q", k))
        c.run_until(c.now + 1500)
        f = next(i for i in c.ids if c.nodes[i].role != LEADER)
        c.crash(f); c.restart(f)
        self.assertEqual(c.nodes[f].kv, {})              # volatile state lost
        c.run_until(c.now + 1000)
        self.assertEqual(c.nodes[f].kv.get("q"), 4)      # rebuilt from the persisted log once commit index is relearned


if __name__ == "__main__":
    unittest.main()
