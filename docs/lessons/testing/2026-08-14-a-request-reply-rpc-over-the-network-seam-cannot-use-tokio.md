# A request/reply RPC over the `Network` seam cannot use `tokio::sync:: oneshot` (or any executor-specific channel) to correlate a reply with its request — `SimEnv` callers have no tokio runtime present at all.

**A request/reply RPC over the `Network` seam cannot use `tokio::sync::
oneshot` (or any executor-specific channel) to correlate a reply with its
request — `SimEnv` callers have no tokio runtime present at all.** Building
`ClusterSegmentStore`'s K-replica `put`/`get`/`delete` fan-out (ADR 0043
§A7b), the natural shape — send a request, `await` a oneshot the reply
handler completes — compiles fine in a crate that already depends on
`tokio` (`animus-cp-data` does, for one real-thread test), but silently
never resolves under `SimEnv`: nothing in the simulator's own cooperative
executor drives a tokio channel's waker. The house pattern (already
established by `RaftKvNode`'s own `ReadProbe`/`ReadProbeAck` read-barrier
confirmation, `lib.rs`) is a **shared `Mutex<BTreeMap<req_id, Option<Reply>>>`
plus an `env.sleep`-based poll loop**: register a slot keyed by a
monotonically-increasing `req_id` before sending, have the single serving
task's dispatch loop `stash` the decoded reply into that slot by `req_id`
when it arrives (on the *same* stream the request went out on — a
request and its reply are just two variants of one wire enum, sharing one
inbox), and have the waiting call `peek` the slot in a bounded
`loop { check; if done break; if deadline break; env.sleep(POLL).await }`.
This is strictly more verbose than a oneshot but works identically under
both `SimEnv` and `ProdEnv` with no executor-specific primitive anywhere —
the same reason `AtomicWaker` (not a tokio-only waker) is what
`RaftKvNode`'s wake-on-propose signal uses. General check before reaching
for a channel/oneshot/notify primitive in a crate whose tests run under
`SimEnv`: does it come from `std`/`futures` (executor-agnostic) or a
specific async runtime crate? If the latter, it needs the poll-a-shared-
slot shape instead. (`crates/animus-cp-data/src/cluster_segment_store.rs`,
ADR 0043 round-3 PR3, 2026-08-14.)
