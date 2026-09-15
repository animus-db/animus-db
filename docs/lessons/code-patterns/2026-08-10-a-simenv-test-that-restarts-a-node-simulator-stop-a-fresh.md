# A `SimEnv` test that restarts a node (`Simulator::stop` + a fresh `RaftNode::start` on the same id) must reuse the *same* `StorageEngine` handle across the restart, not construct a fresh one — `MemoryEngine::new()` at the restart call site silently and completely discards everything a real (disk-backed) engine would have kept, and the bug can hide for a long time under small test workloads.

**A `SimEnv` test that restarts a node (`Simulator::stop` + a fresh
`RaftNode::start` on the same id) must reuse the *same* `StorageEngine`
handle across the restart, not construct a fresh one — `MemoryEngine::new()`
at the restart call site silently and completely discards everything a
real (disk-backed) engine would have kept, and the bug can hide for a
long time under small test workloads.** Ported en masse while mechanically
fixing every `RaftNode::start(..)` call site across
`animus-control`/`animus-cp-data`'s test suites for ADR 0038 PR3 (adding
the now-mandatory engine argument), several restart-style tests
(`restart.rs`, `schema_indexes.rs`, `control_membership.rs`) got a
throwaway `MemoryEngine::new()` at their *second* `RaftNode::start` call —
which happened to still pass, because none of those scenarios crossed the
Raft log's own compaction threshold, so the full uncompacted WAL alone was
enough to replay the whole state from scratch regardless of what the
"restarted" engine held. That's a coincidence of test scale, not a proven
property — the instant a scenario compacts before the simulated crash, a
fresh engine would silently lose the compacted prefix with no error.
**Fix pattern**: create one `MemoryEngine` per node up front (`MemoryEngine`
clones share state, exactly like a real engine reopened from the same
directory), and re-clone that *same* handle into every `RaftNode::start`
call for that node id, including at restart — never call `::new()` a
second time for an id that already existed. **General rule when adding a
mandatory storage-engine parameter to a `start`-style constructor across a
large test suite**: grep for every call site that *replaces* an existing
instance (`nodes[i] = Type::start(..)`, a second `start` for the same id)
separately from fresh first-time starts, and audit each one for whether
the "durable" resource being threaded through needs to survive that
specific call — a blind find-and-replace macro fix is exactly how this
class of bug hides.
