# A confirm-poll's value-equality fallback is only sound for idempotent writes — it proves visibility, not authorship (issue #469)

`animusd::write_path::poll_probe` — the shared durable-before-ack confirm
wait behind `cp_kind_local`/`cp_batch_local` — has two channels for deciding
a proposed entry landed: `classify_kind_batch_outcome` (provably *this
proposer's own* (index, term) entry applied, per PR #334's identity fix,
above) and, when that channel is `Inconclusive`, a plain value-equality
fallback — "does the key already hold the bytes I proposed?" The fallback
predates the identity channel and was never re-examined once the identity
channel existed alongside it: it answers a different question than the one
a confirm needs answered. Value equality proves the bytes are *visible
somewhere*; it says nothing about *which entry put them there*. For an
idempotent write (Put/Delete/SET/REMOVE, a set union or difference) that
distinction is moot — any entry landing those exact bytes is a legitimate
success, whoever's entry it was. For a **non-idempotent** write (a numeric
`ADD`) it is not, and the gap is exploitable, not just theoretical:
`kind_write_item_at_leader` reads the item's `old` value under
`ctx.data().rmw_lock`, deliberately released *before* proposing (issue
#285, so one slow confirm doesn't stall every other write on the node).
Two evaluators of the same key can therefore read the identical stale
`old` and compute byte-identical `new` — `add_numeric` is a pure function
of `(cur, delta)`, `encode_stored_item` a deterministic encode, nothing in
the stored bytes carries a nonce or proposer identity — each guarded by an
own-key OCC seatbelt keyed to that same stale `old`. The first such entry
to apply wins the seatbelt and writes the bytes; every later one
legitimately `ConditionFailed`s and no-ops. But in the window after the
winner's write becomes visible and before a *loser* entry's own outcome is
recorded (`classify_kind_batch_outcome` still `Inconclusive` for it), the
old ungated fallback matched on value alone and returned `Confirmed` for
the loser too — the client got a 200 for an increment that never applied,
a genuine lost acked write, causally isolated under CPU contention (~3/32
failures pre-fix on `dynamo_update_add_delete::
concurrent_increments_all_land_exactly_once`; 0/40 with both value probes
disabled; reproduces again once re-enabled).

The fix threads a `ProbeIdentity` (`ValueProves`/`RequiresOwnEntry`) into
`poll_probe` from whichever caller already knows the answer —
`dynamo::kind_write_is_idempotent`, computed once per write inside
`kind_write_item_at_leader` for its own retry-loop decision, so gating the
confirm on it added no new computation, only a second consumer of an
existing fact. `cp_batch_local` (the raw `Batch` command, structurally
Put-only — there is no `ADD` at that layer) hardcodes `ValueProves`, since
every write it can possibly confirm is idempotent by construction.
`poll_probe` itself never recomputes idempotency — it only ever consults
the gate it's handed. For `RequiresOwnEntry`, the fallback does not run at
either of its two sites in the loop (the main `Inconclusive` branch and the
`confirm_wait_is_futile` early-exit's own re-check) — not even to read the
key — so a non-idempotent write's confirm keeps polling
`classify_kind_batch_outcome` for its own entry until that resolves to
`Confirm`/`NoOp`, or the deadline ends the wait in `TimedOut`. Never a value
guess.

**General form**: when a confirm-poll (or any post-propose "did my write
land" mechanism) has both an identity-proving channel and a value-equality
fallback for when that channel is inconclusive, the fallback is sound only
for writes where any producer of the expected bytes is an acceptable
success — i.e. idempotent writes. The moment a non-idempotent write shares
the same confirm path, the fallback needs the same idempotency gate the
write's own retry logic almost certainly already computes elsewhere
(`kind_write_is_idempotent` existed and was already used for retry
correctness — issue #285's neighbor, not a new concept — well before this
gate closed the confirm-side gap). Adding an identity-proving channel to a
confirm mechanism that still has an ungated older fallback sitting beside
it does not close a false-ack hazard; it just narrows the window in which
the old fallback is the only thing left standing. Regression:
`crates/animusd/src/write_path.rs`'s in-crate `poll_probe_identity_tests` —
a `SimEnv`, single-voter `RaftKvNode<SimEnv, MemoryEngine>` harness (no
`ProdEnv`, no sockets) that proposes a second `KindBatch` carrying the exact
same bytes as an already-applied first entry, guarded by an own-key OCC
seatbelt engineered to legitimately fail once the second entry itself
applies, and drives `poll_probe` directly for that second entry's own
accepted-but-unapplied window — the entry propose and the `poll_probe` call
live in the same spawned task's first poll specifically so no other task
(in particular the apply-drain loop) can race ahead of the window under
test; an earlier draft that proposed the second entry before spawning let
the already-running apply loop drain it during the next `sim.run_for`
before `poll_probe`'s own task ever took a turn, silently passing for the
wrong reason. Proven genuinely red pre-fix (removing just the two
`identity == ProbeIdentity::ValueProves` guards, keeping everything else
unchanged, reproduces `Some(Confirmed)` on every seed) and green post-fix.
