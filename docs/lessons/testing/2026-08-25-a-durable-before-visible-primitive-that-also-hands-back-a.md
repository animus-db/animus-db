# A "durable-before-visible" primitive that also hands back a live handle to what it just made durable has an inherent asymmetry an `Err`-implies- "nothing happened" test gets wrong (2026-08-25, ADR 0058 rung 2's `LsmEngine::clone_to`).

**A "durable-before-visible" primitive that also hands back a live handle
to what it just made durable has an inherent asymmetry an `Err`-implies-
"nothing happened" test gets wrong (2026-08-25, ADR 0058 rung 2's
`LsmEngine::clone_to`).** `clone_to`'s crash-safety contract is: the
target's manifest write is the single commit point, so a fault before it
leaves nothing durable at the target. But `clone_to`'s own last step —
opening the just-committed target to return a usable engine handle — is
itself fallible disk I/O, so a fault landing *there* (after the manifest
commit already succeeded) still returns `Err` even though a fully valid
clone now exists on disk. A fault-injection test built on the assumption
"any `Err` from this call means the target must scan empty" failed
intermittently for exactly this reason once the source had more than one
SSTable (more disk ops inside `clone_to` means more chances for the fault
to land in the trailing reopen rather than the commit). The fix was
asserting the actual invariant — the target is always either **fully
absent** or **fully valid**, never partial — not the stronger, false one.
**General rule**: for any "commit, then hand back a live view of what was
committed" operation, write the crash-safety test (and the doc comment)
around "nothing or complete, never torn," not "success or no-op" — the
post-commit step can still fail on its own without meaning the commit
itself didn't happen.
