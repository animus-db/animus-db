# A commit-wait's convergence check pinned to a coarse-grained timestamp can spuriously match state from BEFORE the awaited command applied (ADR 0066, S-02 step 2 admin CRUD)

`animusd::admin`'s `POST /admin/credentials`/`/rotate` handlers propose a
`MetaCommand` and then poll `metadata_fresh()` in a loop, exactly like
`dynamo.rs::update_time_to_live`'s own commit-wait shape — but the first
cut's own convergence check compared `row.updated_at == now`, where `now`
is the credential catalog's own epoch-**seconds** convention (ADR 0066,
deliberately coarser than the millisecond `wall_ms` fields most of this
codebase's commit-waits key on). Two of this test suite's own end-to-end
tests immediately failed: a `Rotate` issued right after a `Put` — well
within the same wall-clock second in a fast test run — computed a `now`
identical to the `Put`'s own, so the very first iteration of the `Rotate`
handler's poll loop read back the **pre-rotate** row (already satisfying
`updated_at == now` purely by timestamp coincidence, before the just-issued
`propose_schema` had any chance to actually commit) and returned it as if
the rotation had already happened — a `rotation: null` response for a call
that should have opened a grace window.

This is the identical failure family `docs/adr/*`'s existing "a commit-wait
must never pin a transient status value" lesson describes (see
`crates/animusd/CLAUDE.md`'s `create_index` entry: never key a convergence
check on a value the very state you're racing against can also produce) —
but with a new trigger: **when a convergence check's own timestamp field has
coarser granularity than the interval between commands that can share it,
timestamp equality alone stops being a reliable "this specific command
applied" signal**, even with no aggregator or concurrent writer involved at
all — a single caller issuing two of its own commands back to back is
enough. Fixed by comparing **content** instead: `action_put_credential`
checks `row.secret == command_secret && row.policy == command_policy &&
row.enabled == req.enabled`; `action_rotate_credential` checks `row.secret
== command_new_secret` — the actual, unambiguous fact each command's own
apply arm is defined to produce, which a pre-mutation row cannot already
exhibit by coincidence regardless of clock resolution. **General form**:
before keying a commit-wait's convergence check on any timestamp field,
check whether two commands the same caller (or a legitimate concurrent one)
can issue are ever proposed close enough together to land in the same tick
of that field's own granularity — if so, compare the command's actual
semantic effect instead, never a timestamp it happens to also carry.
