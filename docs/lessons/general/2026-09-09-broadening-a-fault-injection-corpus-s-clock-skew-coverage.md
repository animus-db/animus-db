# Broadening a fault-injection corpus's clock-skew coverage from uniform to per-replica differential is necessary but not sufficient — the workload's own command vocabulary gates which witnessing-gap bugs are even reachable (2026-09-09, issue #804 follow-up)

Extending `raftkv_linearizable.rs` (the leaderful-plane Elle corpus) with
per-replica differential clock skew — every replica of every scenario's
group now draws its own seed-derived skew, mirroring
`hlc_differential_skew.rs`'s regression rather than
`txn_serializable.rs`'s pre-existing *uniform* skew — surfaced two things
worth recording beyond issue #804's own two entries above.

**(1) A workload of always-succeeding commands cannot expose a
"committed-but-wrote-no-row" witnessing gap, no matter how the clocks are
skewed.** This corpus's client workload only ever calls `put`/
`linearizable_get` — both always succeed, so for every entry the
log-scanning witness (which sees every commit) and the engine-reading
witness (which sees only rows actually written) agree by construction,
fix present or not. Issue #804's whole bug class needs a command that
*commits a real, monotonicity-checked `ts` while writing no row* (a failed
`Cas`, a condition-failed batch, an aborted transaction) — differential
skew is necessary to trigger the observable symptom (a new leader minting
below the committed max) but the corpus's command vocabulary gates whether
the *precondition* for that symptom (an invisible high-ts entry) can even
exist in the log at all. Closed cheaply: `poison_cas` fires a
guaranteed-miss CAS (a 1-byte `expected` that can never equal any real
encoded value) ahead of every real write, entirely outside the Elle model
(no `Recorder` call — a guaranteed no-op reads and writes nothing the
list-append history could observe). **General lesson**: before trusting
that a broadened fault-injection knob (here, differential skew) gives a
corpus teeth against a specific bug class, check whether the corpus's own
*command* vocabulary can even produce the state that bug class needs —
a knob on the fault-injection side cannot compensate for a workload that
structurally never visits the precondition.

**(2) Even with both fixed, the corpus's organic (single/compound-fault,
one scenario per named cell) schedule did not reproduce issue #804's own
narrower same-day-correction gap (the sender-restart-with-nothing-applied-
since / receiver-restart-before-its-next-compaction edge) at
`ANIMUS_RAFTKV_SEEDS=50`, confirmed by reverting exactly those two fix
hunks (the `engine_mark` max-fold in `engine_image`'s on-demand-image
branch, and the receiver's own durable `hwm.rs` marker write in
`install_engine_image`) and re-running — genuinely still green, not merely
unverified.** That narrower gap needs the *same* node that just restarted
to *also* be the one asked to build or receive a snapshot before it
applies anything else — a compound precondition organic single-fault
injection essentially never lines up by chance, partly because Raft's own
election dynamics mean the just-restarted node is rarely who wins the very
next election either (the two live survivors can campaign immediately; the
restarted node must first recover and then still win a vote). **General
lesson**: a broadened organic fault-injection corpus and a hand-
choreographed, deterministic regression are not substitutes for each
other — the corpus's job is breadth (every existing leader-change/restart/
snapshot-catch-up cell now also runs under asymmetric clocks, for whatever
*is* reachable that way), while a narrow, multi-step-precondition bug still
needs its own scripted reproduction (`hlc_differential_skew.rs`) as the
permanent regression for that exact gap. Neither renders the other
redundant; don't expect a broadened knob alone to subsume a targeted
regression it structurally cannot reach.
