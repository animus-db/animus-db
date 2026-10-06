# Multi-cluster simulation: two simulators in lockstep, WAN model at the client seam

`SimCluster` bakes node id == index and owns exactly one `Simulator`, so two
independent clusters cannot share one (and refactoring the index assumption
across 8k lines is the wrong cost). `animusd`'s test-only `sim_world.rs`
(`SimWorld`, `PeerBridge`, G-01 stage G-d M0) instead keeps one `Simulator` per
cluster and advances them to **identical absolute virtual times** in a fixed
quantum, pumping the WAN model between steps. What made it work:

- **External wakes are free.** `animus-sim`'s waker is an `Arc` `ArcWake` that
  pushes onto the ready queue, and `run_until` drains the ready queue before the
  timeline, so a `futures` oneshot completed by the driver *between* steps wakes
  the sender's task at the next step. No polled-mailbox fallback needed.
- **The sender stamps time.** Draw latency/loss from `sender_env.now()` plus a
  bridge-local seeded RNG; the order of draws is a function of the two seeded
  simulators, so the whole run is a pure function of one seed.
- **Delivery is exact only if base latency >= the quantum** (otherwise a
  message is due inside an already-simulated step); the bridge asserts it.
- **Never drive a member `SimCluster` directly** (`SimCluster::dynamo`/`run_for`
  advance one simulator alone and desync the clocks); run ops through the world
  driver, which spawns on the node env and steps the whole world until the
  result slot fills.
- Prove determinism by hashing both simulators' `trace_lines()` + stats + the
  bridge log, running the scenario twice per seed, and asserting different seeds
  differ (so the hash is not vacuous).
