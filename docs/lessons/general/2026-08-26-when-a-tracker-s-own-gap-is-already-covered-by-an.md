# When a tracker's own gap is already covered by an independent safety net, "fix the gap" means "bound the retry, don't fabricate a decision" (issue #298 residuals, `unresolved_decided`)

`TxnTracker::unresolved_decided`'s own doc already documented that its
staleness is "deliberately approximate... still safe": a straggling
unresolved remote intent is resolved on demand the moment any reader hits
it (the foreign-intent read-path push, ADR 0018 §2/PR5 §3), independent of
this tracker or the background loop that walks it. `txn_resolver_loop`'s
sweep over `unresolved_decided()` had no fallback at all when its
`txn_record_view` lookup failed (e.g. the record's own tablet retiring
mid-recovery) — it just `continue`d forever, silently, with no bound.

The naive fix-by-analogy would have copied `pending_txns`'s sibling fallback
(`txn_recover`'s orphan path) verbatim: track first-seen, and past
`RECOVERY_GRACE`, *decide* something. That doesn't typecast here: the
orphan path exists because the coordinator's own verdict is genuinely
unknown, so "decide" means synthesizing a conservative abort. In
`unresolved_decided`, the verdict (`Committed`/`Aborted`) is **already
known** and carried right in the tuple — the only thing a failed lookup
withholds is `intent_spans`, the list of *which keys* to resolve. There is
no decision left to make; "decide-after-grace" would have to mean
fabricating a participant list out of nothing, which cannot be done safely.

The generalizable shape: **before extending a grace-then-act pattern to a
second call site, check whether the two sites are actually deciding the
same *kind* of thing** — one may be resolving genuine uncertainty (safe to
synthesize a conservative default) while the other is blocked purely on
*data availability* for an already-known fact (nothing safe to synthesize;
the only sound move is to stop claiming progress and let an independent
mechanism carry correctness). The actual fix here: keep retrying quietly
forever (a transient failure should still self-heal), but past grace, log +
meter *once* (a new counter, `CpTxnUnresolvedDecidedStuck`) that background
resolution has stopped making progress on this one entry, then keep
retrying without re-warning every tick — bounding operational noise and
giving observability, while leaning on the already-documented on-demand
safety net for correctness rather than duplicating it. Regression-tested
end to end by `cp_txn.rs`'s
`decided_but_unresolved_record_survives_its_own_tablet_splitting_before_resolve`
— which also surfaced, while designing it, that the *ordinary* version of
this shape (a decided record's key riding the normal split clone/trim path
onto a child) was already self-healing via `rebuild_txn_tracker`'s
group-start re-derivation from the child's own cloned engine state, so the
gap this fix closes is narrower than "any split during recovery" — it's
specifically "the lookup keeps failing for longer than a tick or two,"
which the fix bounds without needing to know why.
