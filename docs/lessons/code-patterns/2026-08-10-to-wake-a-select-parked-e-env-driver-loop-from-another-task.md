# To wake a `select`-parked `<E: Env>` driver loop from another task, race a `futures::task::AtomicWaker` + `AtomicBool` future — never a tokio-only primitive (`Notify`/`watch`), which SimEnv can't drive.

**To wake a `select`-parked `<E: Env>` driver loop from another task, race a
`futures::task::AtomicWaker` + `AtomicBool` future — never a tokio-only primitive
(`Notify`/`watch`), which SimEnv can't drive.** The CP data-plane driver used to
leave a freshly-proposed Raft entry parked until the next ~50ms heartbeat tick; the
fix (single-write latency, ADR 0017) has the proposer raise a flag + `wake()` and
the consensus loop race a third `select` arm that resolves on it, then
`replicate_now` immediately. `AtomicWaker` is executor-agnostic: under `SimEnv` the
synchronous `wake()` marks the driver task ready for the next run-loop poll (fully
deterministic, no wall clock); under tokio `ProdEnv` it resolves the register/wake
race. Two disciplines keep it correct: the waiting future **registers the waker
*before* checking the flag** (else a wake between check and park is lost), and
**consumes the flag** (`swap(false)`) on resolve so it doesn't busy-spin. Pair the
wake with the *consumer-side* poll it unblocks: a fast propose is pointless if the
ack path still polls on a coarse fixed interval — `animusd`'s `cp_put_local` confirm
loop was cut from a fixed 50ms to a ~200µs→5ms adaptive back-off in the same change
(median lone-write latency 52ms → 11ms, `cp_plane.rs::single_write_latency_is_low`,
a `multi_thread` `ProdEnv` liveness test — the sim can't measure real-thread
latency). (`animus-cp-data` `ProposeSignal`; `RaftCore::replicate_now`.)
