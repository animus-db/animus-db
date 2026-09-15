# A trait contract designed for one architecture's semantics becomes a silent-failure footgun once a different architecture reuses the same trait with different assumptions — audit every return value the new architecture's callers now discard.

**A trait contract designed for one architecture's semantics becomes a
silent-failure footgun once a different architecture reuses the same
trait with different assumptions — audit every return value the new
architecture's callers now discard.** `StorageEngine::merge`'s "silently
no-op on a stale/duplicate write" contract (`Result<bool>`, `false` =
"did not take effect") was designed for the deleted leaderless-AP
plane's replay-tolerance semantics (ADR 0001, gone under ADR 0019),
where a stale re-application being silently ignored was exactly the
intended behavior. The CP data plane (`animus-cp-data`) inherited the
same trait and the same silent-`false` contract, but its own callers
(`TxnStage`'s intent write, `TxnResolve`'s commit/abort-restore writes,
`Cas`'s swap) all assume a write their *own* gating logic already
accepted genuinely lands — none of them ever checked the returned bool,
via a blanket `.expect(..)` that only asserted the call didn't *error*,
never that it *took effect*. This is exactly the shape that let the
write-loss bug above hide undetected: the merge silently no-op'd (fence
correctly absent pre-fix, corruption aside) while the caller's own
control flow (`StageOutcome::Staged`, a resolved commit/abort) had
already decided the write landed, computed independently of the merge's
own outcome. **The general check**: when a component built for one set
of semantics is reused by a component with different ones (a shared
trait, a shared library function, a shared protocol message), audit
every return value / outcome the *new* caller silently discards — a
contract that was safe to ignore under the old semantics may be exactly
the signal the new semantics needed. The fix here
(`surface_suspicious_merge_noop`, a metric + capped log, deliberately
*not* a hard assert — a same-value idempotent WAL-replay re-application
is a legitimate, expected `false` this distinguisher can't yet tell
apart from a genuine violation) is a permanent guard against the next
bug shaped like it, not a full fix for the root cause it happened to
hide this time. (ADR 0018 §2 write-loss amendment, torn-pair-fix stack
PR3, 2026-08-15.)
