"""Randomised fault schedule: crashes, restarts, partitions and message loss while clients write continuously."""
from .sim import Cluster
from .checks import check_safety


def chaos_run(seed, n=5, duration=20000, drop=0.05, writes=300):
    c = Cluster(n, seed=seed, drop=drop)
    rng = c.rng
    for k in range(writes):
        c.submit(f"w{k}", ("set", f"k{k % 20}", k), at=rng.uniform(0, duration * 0.8))
    t = 0.0
    while t < duration * 0.85:
        t += rng.uniform(200, 1500)
        c.run_until(t)
        r = rng.random()
        if r < 0.3 and len(c.up) > 0:
            c.crash(rng.choice(sorted(c.up)))
        elif r < 0.6:
            for i in sorted(set(c.ids) - c.up):
                c.restart(i)
        elif r < 0.8:
            ids = c.ids[:]
            rng.shuffle(ids)
            cut = rng.randint(1, n - 1)
            c.partition([ids[:cut], ids[cut:]])
        else:
            c.heal()
    c.heal()
    for i in sorted(set(c.ids) - c.up):
        c.restart(i)
    c.run_until(duration + 3000)          # let the cluster converge
    return c, check_safety(c)
