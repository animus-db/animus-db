# When you find a seam hole in one function, grep for its structural twins in the same file before you stop — issue #993

`index_drain::seal_now`'s commit-wait loop used to read the wall clock via
bare `tokio::time::Instant::now()`/`tokio::time::sleep` despite its own
`<E: Env, R: RelayClient>` signature already being generic — fixed by ADR
0061 rung G (C-07 PR 2). That PR's own investigation found the sibling
function, `pitr_seal_now` (the PITR seal arm, structurally identical to
`seal_now` down to the shape of the commit-wait loop), carried the exact
same bug — and *said so in a code comment* ("`pitr_seal_now` — `seal_now`'s
structural twin — carries the identical unfixed bug... a future rung
driving PITR sealing under `SimCluster` will hit the identical panic").
Nobody drove PITR sealing under `SimEnv` for the next several weeks, so the
comment just sat there. The next several rungs closed the surrounding
`index_drain.rs` module's Streams-arm and GSI-arm gaps one at a time and
never circled back to the sibling the first fix's own comment had already
named.

## The fix

Issue #993 finally converted `pitr_seal_now`'s loop the identical way and,
this time, put `index_drain` under `#[deny(clippy::disallowed_methods)]`
on its `mod` declaration (`lib.rs`) — the same enforcement mechanism ten
other modules in this crate already use (ADR 0061 Phase C's closing rung).
Once the module has zero remaining `tokio::time`/`tokio::spawn` sites, the
`deny` makes a *reintroduced* one a compile error, not a comment someone
has to remember to act on.

## The lesson

A "found the same bug in a sibling, didn't fix it here" comment is a
better outcome than not noticing at all, but it is still a TODO with no
enforcement — it depends on a human (or a future agent) re-reading that
exact comment at exactly the moment they're touching that exact function.
Two structural improvements close that gap for good, and both are cheap
enough to do in the same change that finds the first instance:

1. **Grep the same file for the fixed function's own structural twins
   before moving on.** A "seal_now"/"pitr_seal_now"-shaped pair (same
   parameters, same doc cross-references, same commit-wait shape) is easy
   to find with one `grep -n "fn.*<E: Env"` pass over the file you're
   already editing — cheaper than writing the deferral comment in the
   first place, and it turns a *known, named, unfixed* gap into a *fixed*
   one instead of a debt that outlives the PR that found it.
2. **Once every site in a module is clean, put the module under a
   compiler-enforced invariant** (here, `#[deny(clippy::
   disallowed_methods)]` on its own `mod` line) rather than trusting the
   next reviewer to notice a regression by eye. A lint the compiler
   enforces cannot be silently reintroduced; a comment can be, and was,
   left unread for weeks across several intervening rungs that touched
   the very same file.

This generalizes past `tokio::time`/`Env`-seam holes: whenever a fix
targets one function because it happens to be the one a test reached, the
next five minutes are worth spending checking whether the same file holds
a copy-pasted or doc-commented twin of the same shape — and, if the
module can plausibly reach "zero known holes," whether it's worth putting
under whatever enforcement mechanism the codebase already has for exactly
this class of regression.
