"""Raft safety properties (paper Figure 3), checked on a finished simulation."""


def check_safety(cluster):
    errors = []
    # Election Safety: at most one leader per term
    for term, ls in cluster.leaders_by_term.items():
        if len(ls) > 1:
            errors.append(f"two leaders in term {term}: {sorted(ls)}")
    # Log Matching: if two logs have an entry with the same index and term, the logs are identical up to there
    nodes = list(cluster.nodes.values())
    for a in range(len(nodes)):
        for b in range(a + 1, len(nodes)):
            la, lb = nodes[a].log, nodes[b].log
            last_same = 0
            for i in range(min(len(la), len(lb))):
                if la[i][0] == lb[i][0]:
                    last_same = i + 1
            if la[:last_same] != lb[:last_same]:
                errors.append(f"log matching violated between {nodes[a].id} and {nodes[b].id}")
    # State Machine Safety: no two nodes apply different commands at the same index
    applied = {}
    for n in nodes:
        for idx, term, cmd in n.applied_log:
            if idx in applied and applied[idx] != (term, cmd):
                errors.append(f"index {idx} applied differently: {applied[idx]} vs {(term, cmd)}")
            applied[idx] = (term, cmd)
    # Durability: every write acknowledged to a client is in the committed prefix of every live node's log
    for rid, (idx, term, cmd) in cluster.acked.items():
        for n in nodes:
            if n.id in cluster.up and n.commit >= idx and (len(n.log) < idx or n.log[idx - 1] != (term, cmd)):
                errors.append(f"acknowledged write {rid} lost on node {n.id}")
    return errors
