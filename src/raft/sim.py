"""Deterministic discrete-event simulator: message delays, drops, network partitions, crashes and restarts.
Everything is driven by one seeded RNG, so any failing run can be replayed exactly."""
import heapq, random
from .node import RaftNode, LEADER


class Cluster:
    def __init__(self, n=5, seed=0, delay=(5, 20), drop=0.0, election_timeout=(150, 300), heartbeat=50, tick=10):
        self.rng = random.Random(seed)
        self.ids = list(range(n))
        self.nodes = {i: RaftNode(i, self.ids, random.Random(self.rng.random()), election_timeout, heartbeat) for i in self.ids}
        self.delay, self.drop, self.tick_ms = delay, drop, tick
        self.now, self.q, self.seq = 0.0, [], 0
        self.up = set(self.ids)
        self.groups = None                       # partition: list of sets, or None for fully connected
        self.leaders_by_term = {}                # term -> set of leader ids (election safety check)
        self.acked = {}                          # req_id -> (index, term, command) acknowledged to the client
        self.submit_time, self.latency = {}, []
        self.msgs_sent = 0
        for i in self.ids:
            self._push(self.rng.uniform(0, tick), "tick", i)

    # ---- event plumbing ----
    def _push(self, t, kind, payload):
        self.seq += 1
        heapq.heappush(self.q, (t, self.seq, kind, payload))

    def connected(self, a, b):
        if self.groups is None:
            return True
        return any(a in g and b in g for g in self.groups)

    def send(self, msgs):
        for m in msgs:
            self.msgs_sent += 1
            if self.rng.random() < self.drop:
                continue
            self._push(self.now + self.rng.uniform(*self.delay), "msg", m)

    def _record(self, nid):
        node = self.nodes[nid]
        if node.role == LEADER:
            self.leaders_by_term.setdefault(node.term, set()).add(nid)
        # acknowledge client writes whose entries the leader has applied
        if node.role == LEADER:
            for idx in [i for i in node.pending if i <= node.applied]:
                rid = node.pending.pop(idx)
                if node.log[idx - 1][1][0] == "set" and rid not in self.acked:
                    self.acked[rid] = (idx, node.log[idx - 1][0], node.log[idx - 1][1])
                    self.latency.append(self.now - self.submit_time[rid])

    def run_until(self, t_end):
        while self.q and self.q[0][0] <= t_end:
            t, _, kind, p = heapq.heappop(self.q)
            self.now = t
            if kind == "tick":
                if p in self.up:
                    self.send(self.nodes[p].tick(t))
                    self._record(p)
                self._push(t + self.tick_ms, "tick", p)
            elif kind == "msg":
                if p["to"] in self.up and self.connected(p["frm"], p["to"]):
                    self.send(self.nodes[p["to"]].handle(p, t))
                    self._record(p["to"])
            elif kind == "client":
                self._client(*p)
        self.now = t_end

    # ---- faults ----
    def crash(self, nid):
        self.up.discard(nid)

    def restart(self, nid):
        if nid not in self.up:
            self.up.add(nid)
            self.nodes[nid].restart(self.now)

    def partition(self, groups):
        self.groups = [set(g) for g in groups]

    def heal(self):
        self.groups = None

    # ---- clients ----
    def leader(self):
        ls = [i for i in self.up if self.nodes[i].role == LEADER]
        return max(ls, key=lambda i: self.nodes[i].term) if ls else None

    def submit(self, req_id, command, at=None, retries=40):
        self.submit_time[req_id] = self.now if at is None else at
        self._push(self.now if at is None else at, "client", (req_id, command, self.rng.choice(self.ids), retries))

    def _client(self, req_id, command, target, retries):
        if req_id in self.acked or retries <= 0:
            return
        node = self.nodes[target]
        if target in self.up:
            ok, redirect, msgs = node.client_request(req_id, command, self.now)
            self.send(msgs)
            if ok:   # wait; if not acknowledged within 500 ms, retry (the leader may have been deposed)
                self._push(self.now + 500, "client", (req_id, command, target, retries - 1))
                return
            if redirect is not None:
                self._push(self.now + 1, "client", (req_id, command, redirect, retries - 1))
                return
        self._push(self.now + 50, "client", (req_id, command, self.rng.choice(self.ids), retries - 1))

    def first_leader_time(self, t_max=5000, step=5):
        t = self.now
        while t < t_max:
            t += step
            self.run_until(t)
            if self.leader() is not None:
                return t
        return None
