# A corrected finding must be corrected where it was first written, or it gets re-filed

**What happened.** PR #748 recorded a memory plateau in
`sim_cluster_dynamo_corpus.rs`'s module doc as "filed, not fixed", with a
glibc-allocator theory. PR #753 root-caused it days later as two `Arc`
reference cycles and fixed it, and the correction was written into ADR 0061
(two amendments), `docs/roadmap.md`'s C-06 entry and `crates/animusd/CLAUDE.md`.
The source comment was never touched. A triage sweep on 2026-09-17 read the
source comment, took it at face value, and filed issue #995 for a defect that
had been closed for nine days. The fix for #995 is a doc edit.

**Why it slipped.** A source-level comment is the *nearest* documentation to
the code a reader is looking at, so it wins over the roadmap and the ADR even
when they disagree. The ADR's append-only convention ("left in place rather
than edited") is right for decisions, but it means a correction lives only
*below* the stale text; a `#[cfg(test)]` module doc has no such convention and
should simply be rewritten.

**What to do.**
- When a later PR root-causes something an earlier PR recorded as open, grep
  for that recording's distinctive phrase (here "filed, not fixed") across
  `crates/`, not only `docs/`, and rewrite the source comment in the same PR.
- A triage sweep that files an issue from a source comment should first grep
  the roadmap and ADRs for the same symptom; a correction there closes the
  issue before it is opened.
- Prefer a pointer ("see PR #N") over a second full account when the source
  comment must stay short; the pointer cannot go stale the way a retelling can.
