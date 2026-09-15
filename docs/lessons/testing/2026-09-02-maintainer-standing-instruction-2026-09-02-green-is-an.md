# Maintainer standing instruction (2026-09-02): green is an invariant, flakiness is a bug, and "I didn't cause it" is not a reason to discard a failure.

**Maintainer standing instruction (2026-09-02): green is an invariant,
flakiness is a bug, and "I didn't cause it" is not a reason to discard a
failure.** Recorded here as well as in `CLAUDE.md` Session operating mode
item 4 (the binding text) and the session-start hook so it survives
context resets. The *why*: this log already holds a dozen entries where a
test "flaked" and the eventual root cause was a real defect (a
fixed-deadline assert on an eventual property, a starvation retry against
a live producer, a missed forwarding allowlist that showed up bimodally
per process, a `ResourceInUseException` on a retried `CreateTable`, an
un-jittered pair of polling loops). Every one of those was cheaper to
fix at first sight than after it had been retried past for weeks and
had trained everyone to ignore red. So the rule is mechanical: a red
gate on `main`, on a branch, or on a PR being driven is either
root-caused and fixed in this session (a pre-existing bug gets its own
PR + regression test per Conventions) or explicitly handed off as an
issue naming the test, the seed/log, and what is known — and the merge
then **waits** for that fix. Retrying until green, widening a timeout,
`#[ignore]`, quarantine, and "merge it anyway, it's just flaky" are all
bypasses, and an agent is expected to push back on them — including
when the maintainer proposes one — until the override is stated
explicitly and deliberately. The failure modes this guards against are
the ones in issue #279 (a hand-merged stack that landed groundwork
without its fix) and issue #298 (a flake carried across five shapes
before a root cause): both got expensive precisely because red was
tolerated for a while.
