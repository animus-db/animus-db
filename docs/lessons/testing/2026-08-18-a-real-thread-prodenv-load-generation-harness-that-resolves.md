# A real-thread `ProdEnv` load-generation harness that resolves "the leader" once and hammers that one handle for the whole run is itself a latent bug, independent of whatever property the test exists to check

**A real-thread `ProdEnv` load-generation harness that resolves "the
leader" once and hammers that one handle for the whole run is itself a
latent bug, independent of whatever property the test exists to check** —
and a heavy multi-task `ProdEnv` load is exactly the kind of CPU
contention that triggers the ADR 0017 starved-consensus-loop election
shape it's supposed to be immune to. `crates/animus-cp-data/tests/
prod_concurrent_ts_monotonic.rs` hammered a real 3-node group with 24
concurrent client tasks against one `leader` handle captured at test
start; on a contended CI runner an election deposed it mid-run and every
task kept proposing against the now-stale handle, surfacing as
`linearizable_get ... did not return what was just put (left: None)` or
`txn_write did not complete (leader stepped down?)`. The second, subtler
bug compounded it: the put-side helper treated `put() -> Accepted{index}`
plus `engine_applied_index() >= index` as proof the write **committed** —
but `Accepted` only ever means "appended to the leader's own log," never
"committed" (this file's durable-before-visible entry, and the root
`CLAUDE.md`'s standing lesson, already say this — the harness itself
hadn't internalized its own documented rule). After an election, the
deposed leader's uncommitted entry gets truncated and the new leader's
own entries re-occupy that same index, so the index-advance wait passed
while the write never actually landed. **Fix, generalizable to any
multi-task `ProdEnv` harness driving a leaderful group under load:**
thread the whole node slice (not one handle) into every load-generating
helper; re-resolve "whoever is leader now" on *every* propose attempt and
every confirm read, never cache it across an `.await`; and confirm a
write only by reading the expected value back (never by index-advance
alone), retrying the *whole* propose on a stale/absent confirmation —
sound because retrying an already-landed write at the same key/value is
idempotent. All of it bounded by a converged-or-timeout deadline, per the
general rule above, never a fixed one-shot wait. (Issue #278 item 6.)
