# Fixing one bug in a multi-bug stack can unmask (or mask) another — re-measure the acceptance baseline after every layer, never assume a fix's own regression proves the end-to-end symptom is gone.

**Fixing one bug in a multi-bug stack can unmask (or mask) another —
re-measure the acceptance baseline after every layer, never assume a
fix's own regression proves the end-to-end symptom is gone.** This
incident's own three-layer history is the clearest example the repo has
of this: PR1 (a clock-witnessing runaway) fixed a real bug and made the
end-to-end torn-pair test's failure rate roughly *unchanged* (a
coincidental clock-lockstep had been *masking* a second bug); PR2 (a
read-shape race) fixed a second real bug with its own clean regression
green at depth, yet the wire-level test's failure rate stayed just as
high, because a *third*, structurally unrelated bug (this write-loss
one) was still live underneath both. Each individual PR's own dedicated
`SimEnv` regression was, correctly, green throughout — proving that
fix's own protocol-level claim sound — but none of them could have shown
the composed system was actually fixed, because each targeted a
different mechanism than the one still causing the wire-level failure.
**The general practice**: in a multi-bug investigation, treat the real
wire-level/end-to-end reproduction as the only trustworthy signal for
"is the user-visible symptom actually gone" — a per-fix unit regression
proves that fix's own mechanism, never the composition. Re-run the full
reproduction (not just the new regression) after every layer, record the
baseline number, and don't stop until it reaches the required bar (here,
0/20 solo, the strictest baseline in this stack's own history) —
anything less and a fourth bug could still be hiding under the same
symptom. (Torn-pair-fix stack, all three PRs, 2026-08-07 through
2026-08-15.)
