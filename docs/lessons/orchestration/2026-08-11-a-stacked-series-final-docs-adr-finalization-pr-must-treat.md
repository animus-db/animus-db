# A stacked series' final "docs/ADR finalization" PR must treat the stack's own shipped PR bodies (`gh pr view`) as the authoritative source for divergences from the plan — not the plan doc, and not just the final code state.

**A stacked series' final "docs/ADR finalization" PR must treat the stack's
own shipped PR bodies (`gh pr view`) as the authoritative source for
divergences from the plan — not the plan doc, and not just the final code
state.** ADR 0040's 6-PR stack had one implementer agent per PR, each of
which discovered and documented a real divergence from the delivery plan
as it built (e.g. PR4's `RegisterNode` CAS keying on `node_addrs` alone,
not `addrs`+`labels`, and its separate control-role-never-claims-`members`
fix) — but each agent's own PR body is the *only* place some of these
divergences are recorded end to end, since the shipped code and crate
`CLAUDE.md`s describe the *result* without always narrating *why it
diverged from what was planned*. Finalizing ADR 0040 (this PR) by reading
only the code + crate guides + the plan doc would have produced a
plausible-sounding but subtly wrong Decision C (the plan's original
labels-inclusive CAS design, not the shipped node-addrs-only one) — the
gap only closes by reading every prior PR's own body (`gh pr view
<N>`) for its "Deviations from the plan/brief" section before writing the
ADR's final Decision text. **General rule: when a task hands you a stack
of already-landed PRs to finalize/document, fetch and read each one's own
PR body before trusting the plan doc or the current code alone — a PR body
is where an implementer records the reasoning for a mid-flight design
change that neither the plan (written before) nor the code (silent on
*why*) captures.**
