# A test change that was only compiled has not been tested

**What happened.** A commit made `animus cluster roll plan` re-entrant and changed
the real-process `upgrade_previous_release` test to re-ask `roll plan` after every
step and, new, to assert the plan is empty after `finalize`. The test needs an
opt-in feature and a built R-1 binary, so the commit was only compiled; CI's
`upgrade-previous-release` job was the first run, and all four variants failed
on the new post-finalize assertion.

**Root cause (generalizes).** The CLI derived the roll goal as `active + 1`. After
finalize that is a version no binary supports, so every node (range max == active)
is "not on the new binary" and the plan listed a restart of everything. The
server's own `roll.remaining` has the same shape. A goal derived from "current
state + 1" is only meaningful if some binary can reach it; the fix settles the
goal (`settle_goal`) against the asked node's, the probed nodes' and the CLI's own
ranges, and says "complete" otherwise.

**Rules.** (1) An opt-in/cross-version test whose assertions you edit must be run
locally at least once before pushing (reuse a cached R-1: `ANIMUS_UPGRADE_FROM_CACHE`
holds `<sha>/animusd`). (2) For any "next X" derivation, test the state *after*
the last X (at rest), not just before and during.
