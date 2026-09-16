# Deferring a direct engine write into a shared `pending` queue changes what a same-pass read sees — issue #834

`animus-cp-data`'s apply loop (`apply_and_compact`) coalesces a run of
committed writes into one `merge_batch` `fsync` via a loop-local
`pending: Vec<MergeOp>`, flushed by `flush_pending` at strategic points.
Converting `KvCommand::Batch` and `TxnResolve`'s commit/abort-restore
branches from a direct, immediate `storage.merge`/`merge_tombstone` call to
a `pending.push(..)` — the whole point of the change, closing an N-`fsync`-
per-entry perf defect and a missing `halted` gate — is not purely a
perf/durability-tolerance change. It also changes *when* the write becomes
visible to any other command's own `storage.get`/`get_at` read within the
same apply pass: immediately, under the old direct-write code; only after
the next `flush_pending` call, under the new queued one.

## The bug this produced

`KvCommand::TxnStage`'s own apply arm decides whether a target key already
carries a *foreign* unresolved `Intent` (`already_decided`/`blocked_by`) by
reading `storage.get` directly — with no preceding `flush_pending`, unlike
`Cas`'s and `KindEval`'s own arms (which already call `flush_pending` first,
specifically so their reads observe every earlier write in the same apply
pass — that discipline already existed, `TxnStage` just never needed it).
This was safe exactly as long as every command capable of turning an
`Intent` into a `Committed` envelope at a key applied *immediately* — true
before this change (`TxnResolve`'s commit write was the only such
transition, and it was a direct `merge`), false after (its write can now sit
un-flushed in `pending` while a *later* entry in the *same* apply pass — a
new transaction's own `TxnStage` reusing that key — reads the stale,
pre-resolve state instead).

Caught by `animus-test`'s `txn_serializable.rs` corpus at
`ANIMUS_TXN_SEEDS=5` (`participant_leader_kill_early`, seed
`2743871795844702347`): two replicas of one Raft group permanently
diverged on one key's final value (`[4, 16]` vs `[4, 16, 20]`) — not a
timing flake, since the test's own converged-or-timeout poll never closed
the gap (a genuine, provable non-convergence, the corpus's own
`check_convergence` comparing two replicas' raw `local_get`). Root-caused
with temporary `eprintln!` tracing (gated behind a throwaway env var,
printing node id / entry index / txn id at every `TxnResolve` read and
write site) rather than by inspection alone — the interaction is three
commands deep (an earlier `TxnResolve`'s commit write, a later `TxnStage`'s
conflict check, a still-later `TxnResolve`'s own idempotent-no-op read) and
not obvious from reading either arm in isolation. On the diverging replica,
the intervening `TxnStage` for the *next* transaction targeting that key
saw the *prior* transaction's still-un-flushed `Intent` (not yet turned
into `Committed` by the deferred resolve) and spuriously treated it as a
foreign-transaction block; its own intent for the new transaction's
candidate value was therefore never written, and the later `TxnResolve`
correctly found "nothing left here to resolve" and no-op'd — silently
losing the whole write, not just delaying it.

Fixed by giving `TxnStage`'s arm the identical `flush_pending`-first
discipline `Cas`/`KindEval` already have (`crates/animus-cp-data/src/
lib.rs`).

## The generalizable lesson

Before converting any direct engine write inside `apply_and_compact`'s
effects loop into a `pending`-queued one (or, more generally, before adding
*any* new write-producing arm to the loop), grep every
`storage.get`/`get_at`/`scan` call inside the whole loop for a preceding
`flush_pending`. A read-then-decide arm that skips this either already only
ever runs after a write it depends on landed some other way, or it has a
latent bug waiting for the *next* write to go from direct to deferred to
surface it — exactly what happened here. The fix generalizes: any command
whose apply-time decision depends on another command's *committed* effect
(not just "has this Raft entry applied," but "does the engine currently
reflect it") needs this discipline the moment either side of that
dependency could legitimately sit un-flushed in `pending`.

See ADR 0018's 2026-09-16 amendment for the full account (including why the
`surface_suspicious_merge_noop` soft diagnostic was deliberately dropped on
the newly-batched `TxnResolve` commit/abort-restore path rather than kept
via a new `merge_batch` trait variant) and `crates/animus-cp-data/CLAUDE.md`
for the as-built pointer.
