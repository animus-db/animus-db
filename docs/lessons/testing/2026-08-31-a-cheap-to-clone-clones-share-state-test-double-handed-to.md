# A "cheap to clone, clones share state" test double handed to code that models independent replicas silently collapses replica independence — and the symptom looks exactly like a production consensus/tracking bug (issue #488, `crates/animus-test/tests/txn_serializable.rs`).

**A "cheap to clone, clones share state" test double handed to code that
models independent replicas silently collapses replica independence —
and the symptom looks exactly like a production consensus/tracking bug
(issue #488, `crates/animus-test/tests/txn_serializable.rs`).**
`Topology::start` built **one `MemoryEngine` per tablet group and
`.clone()`d it into all 3 replica `RaftKvNode`s**; `MemoryEngine`'s own
doc comment says outright "cheap to clone; clones share state"
(`Arc<Mutex<Inner>>`), so the corpus's "3 independent replicas" secretly
read and wrote one physical store. Only the Raft layer (log, term,
leadership) and each replica's own in-memory `TxnTracker` were genuinely
per-replica. Consequence: whichever replica's apply task happened to run
first for a log index durably wrote the shared engine, so a *different*
replica's own, separately-sequenced `apply_and_compact` call for the
identical index could read back a status its own log processing hadn't
actually decided yet — silently steering it into an already-decided/
idempotent-replay no-op branch that (correctly, for a genuine replay)
skips the real `Pending -> Committed` transition and the `TxnTracker`
update that transition performs. When the two replicas that happened to
take that no-op path were exactly the two that survived a leader kill,
neither had a populated `TxnTracker::unresolved_decided` for the
transaction, so the resolver's proactive re-propose never fired and a
`KIND_LSI` derived row (materialized only inside a genuine local
`TxnResolve` apply, never re-derived from engine state) was orphaned
forever — a permanent, reproducible-at-depth divergence that looked
exactly like a leader-only/liveness gap in `animus-cp-data`'s apply
pipeline, and cost a full investigation to clear the production code
before the harness was even suspected. Fix: one `MemoryEngine::new()`
**per replica**, matching the sibling `raftkv_linearizable.rs` corpus's
`Group::start` (`factory(&sim, id)` per node id), which never shared an
engine across replicas. **General rule**: before handing the same
instance of a "cheap to clone" test double (an `Arc`-backed fake store,
an `Rc<RefCell<_>>` counter, anything whose `Clone` impl is documented or
obviously implemented as a shared-handle copy) to code meant to model N
independent participants, verify the sharing is what you actually
intend — a fixture that looks like "one engine per group, replicated
normally" reads as correct at a glance and only misbehaves under real
timing skew between replicas' own apply rates, exactly the kind of thing
fault-injection depth (not the default-depth corpus run) is what
actually exposes it.
