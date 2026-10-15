"""One Raft server. Messages are plain dicts; the node never touches the network directly -- it returns messages to
send, which the simulator delivers (with delays, drops and partitions). Persistent state (term, vote, log) survives
crashes; volatile state is reset on restart, as in the paper (Figure 2)."""
import random

FOLLOWER, CANDIDATE, LEADER = "follower", "candidate", "leader"


class RaftNode:
    # Deliberate bug switches used to show that the chaos tests can catch real safety violations.
    BUG_IGNORE_LOG_IN_VOTE = False        # grant votes without the "candidate log is up to date" check
    BUG_COMMIT_OLD_TERM = False           # count replicas for entries from earlier terms (violates Figure 8 rule)

    def __init__(self, nid, peers, rng: random.Random, election_timeout=(150, 300), heartbeat=50):
        self.id, self.peers, self.rng = nid, [p for p in peers if p != nid], rng
        self.et_range, self.heartbeat = election_timeout, heartbeat
        # persistent
        self.term, self.voted_for, self.log = 0, None, []          # log entries: (term, command)
        self.restart(0)

    # ---- lifecycle -------------------------------------------------------------------------------------------
    def restart(self, now):
        self.role, self.leader_id = FOLLOWER, None
        self.commit, self.applied = 0, 0                             # 1-based indices; 0 = nothing
        self.kv, self.applied_log = {}, []                           # state machine (rebuilt by re-applying)
        self.votes, self.next_idx, self.match_idx = set(), {}, {}
        self.pending = {}                                            # log index -> client request id (leader only)
        self.reset_election_timer(now)
        self.last_hb = now

    def reset_election_timer(self, now):
        self.election_deadline = now + self.rng.uniform(*self.et_range)

    def last_index(self):
        return len(self.log)

    def last_term(self):
        return self.log[-1][0] if self.log else 0

    def term_at(self, i):
        return self.log[i - 1][0] if i >= 1 else 0

    # ---- timers ----------------------------------------------------------------------------------------------
    def tick(self, now):
        out = []
        if self.role != LEADER and now >= self.election_deadline:
            out += self.start_election(now)
        if self.role == LEADER and now - self.last_hb >= self.heartbeat:
            self.last_hb = now
            out += [self.append_entries_to(p) for p in self.peers]
        return out

    def start_election(self, now):
        self.role, self.term, self.voted_for = CANDIDATE, self.term + 1, self.id
        self.votes = {self.id}
        self.leader_id = None
        self.reset_election_timer(now)
        return [{"type": "RequestVote", "to": p, "frm": self.id, "term": self.term,
                 "last_index": self.last_index(), "last_term": self.last_term()} for p in self.peers]

    def become_leader(self, now):
        self.role, self.leader_id = LEADER, self.id
        self.next_idx = {p: self.last_index() + 1 for p in self.peers}
        self.match_idx = {p: 0 for p in self.peers}
        self.log.append((self.term, ("noop",)))     # commit an entry from the new term promptly (paper section 8)
        self.last_hb = now
        return [self.append_entries_to(p) for p in self.peers]

    # ---- client ----------------------------------------------------------------------------------------------
    def client_request(self, req_id, command, now):
        """Returns (accepted, redirect_leader, messages)."""
        if self.role != LEADER:
            return False, self.leader_id, []
        self.log.append((self.term, command))
        self.pending[self.last_index()] = req_id
        return True, self.id, [self.append_entries_to(p) for p in self.peers]

    # ---- replication -----------------------------------------------------------------------------------------
    def append_entries_to(self, p):
        ni = self.next_idx[p]
        prev = ni - 1
        return {"type": "AppendEntries", "to": p, "frm": self.id, "term": self.term, "prev_index": prev,
                "prev_term": self.term_at(prev), "entries": self.log[prev:prev + 64], "commit": self.commit}

    def step_down_if_newer(self, term, now):
        if term > self.term:
            self.term, self.voted_for, self.role = term, None, FOLLOWER
            self.pending.clear()
            return True
        return False

    def handle(self, m, now):
        out = []
        self.step_down_if_newer(m["term"], now)
        t = m["type"]
        if t == "RequestVote":
            up_to_date = self.BUG_IGNORE_LOG_IN_VOTE or (m["last_term"], m["last_index"]) >= (self.last_term(), self.last_index())
            grant = m["term"] == self.term and self.voted_for in (None, m["frm"]) and up_to_date
            if grant:
                self.voted_for = m["frm"]
                self.reset_election_timer(now)
            out.append({"type": "Vote", "to": m["frm"], "frm": self.id, "term": self.term, "granted": grant})
        elif t == "Vote":
            if self.role == CANDIDATE and m["term"] == self.term and m["granted"]:
                self.votes.add(m["frm"])
                if len(self.votes) > (len(self.peers) + 1) // 2:
                    out += self.become_leader(now)
        elif t == "AppendEntries":
            ok = False
            if m["term"] == self.term:
                if self.role != FOLLOWER:
                    self.role = FOLLOWER
                self.leader_id = m["frm"]
                self.reset_election_timer(now)
                pi = m["prev_index"]
                if pi <= self.last_index() and self.term_at(pi) == m["prev_term"]:
                    ok = True
                    for k, e in enumerate(m["entries"]):          # append, truncating conflicting suffixes only
                        idx = pi + 1 + k
                        if idx <= self.last_index():
                            if self.log[idx - 1][0] != e[0]:
                                del self.log[idx - 1:]
                                self.log.append(e)
                        else:
                            self.log.append(e)
                    match = pi + len(m["entries"])
                    if m["commit"] > self.commit:
                        self.commit = min(m["commit"], match)
                    self.apply()
            out.append({"type": "AppendReply", "to": m["frm"], "frm": self.id, "term": self.term, "ok": ok,
                        "match": m["prev_index"] + len(m["entries"]) if ok else 0,
                        "hint": min(self.last_index() + 1, m["prev_index"])})
        elif t == "AppendReply":
            if self.role == LEADER and m["term"] == self.term:
                p = m["frm"]
                if m["ok"]:
                    self.match_idx[p] = max(self.match_idx[p], m["match"])
                    self.next_idx[p] = self.match_idx[p] + 1
                    self.advance_commit()
                    if self.next_idx[p] <= self.last_index():
                        out.append(self.append_entries_to(p))
                else:
                    self.next_idx[p] = max(1, min(self.next_idx[p] - 1, m["hint"]))
                    out.append(self.append_entries_to(p))
        return out

    def advance_commit(self):
        for n in range(self.last_index(), self.commit, -1):
            if self.term_at(n) != self.term and not self.BUG_COMMIT_OLD_TERM:   # only count current-term entries (Fig. 8)
                break
            if 1 + sum(1 for p in self.peers if self.match_idx[p] >= n) > (len(self.peers) + 1) // 2:
                self.commit = n
                break
        self.apply()

    def apply(self):
        while self.applied < self.commit:
            self.applied += 1
            cmd = self.log[self.applied - 1][1]
            self.applied_log.append((self.applied, self.log[self.applied - 1][0], cmd))
            if cmd[0] == "set":
                self.kv[cmd[1]] = cmd[2]
            elif cmd[0] == "del":
                self.kv.pop(cmd[1], None)
