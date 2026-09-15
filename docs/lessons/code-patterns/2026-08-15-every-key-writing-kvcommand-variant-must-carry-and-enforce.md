# Every key-writing `KvCommand` variant must carry AND enforce an apply-time `fence` — an exception "reasoned" safe on a closed-world assumption is exactly where the next caller breaks the assumption.

**Every key-writing `KvCommand` variant must carry AND enforce an
apply-time `fence` — an exception "reasoned" safe on a closed-world
assumption is exactly where the next caller breaks the assumption.**
`KvCommand::TxnResolve` was the one key-writing variant with no `fence`
at all, on the theory that "every key here was already fence-checked at
`TxnStage` time" — true for every in-crate caller, but not something the
type enforced, and `animusd`'s own coordinator (`ClientCtx::
recovery_resolve`, misrouting a resolve to the wrong tablet of a split
table by grouping participants by table name alone, no tablet
dimension) was exactly the counterexample: the wrong tablet applied the
resolve for a key it doesn't own, stamped with its own clock onto the
*same physical key* the owning tablet separately maintains (ADR 0028: a
table's tablets share one `StorageScope` prefix on a shared engine) —
an acked write silently and permanently lost. **The general rule this
generalizes**: when a command variant is deliberately left without a
safety check present on its siblings, the justification is a claim
about every *current* caller's behavior, not a property the compiler
verifies — grep for that same reasoning shape ("X can't happen because
every caller already ensures it") whenever reviewing why one variant in
a family lacks a guard the others have, and prefer adding the guard
(cheap, whole-or-nothing, matching the siblings' own shape) over trusting
the argument to stay true forever. Practically: **when adding a new
`KvCommand`/wire-command variant, grep both the relay-gate pattern
(`is_relayable_command`, `cp_serve_forwarded`, admin filters — a missed
allowlist entry) and the fence-check pattern (every other key-writing
variant's apply arm) — a missing fence is a silent data-corruption path,
not a compile error or an obvious test failure, exactly like a missing
relay-gate entry is a silent per-process-bimodal flake.** (ADR 0018 §2
write-loss amendment, torn-pair-fix stack PR3, 2026-08-15.)
