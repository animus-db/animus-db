# Loosening a parsed type's charset can silently turn a test's "garbled, must-be-rejected" fixture into a now-valid value the parser accepts.

**Loosening a parsed type's charset can silently turn a test's "garbled,
must-be-rejected" fixture into a now-valid value the parser accepts.**
`animusd::topology::parse_not_leader_refusal`'s garbled-hint-suffix test
used `"notanumber"` as its not-a-real-id fixture — correct while `NodeId`
parsed as `u64` (that string could never parse), silently wrong once ADR
0040 PR3 gave `NodeId` a permissive `[A-Za-z0-9._-]{1,64}` charset:
`"notanumber"` is now syntactically a perfectly valid id, so the parse
that was supposed to fail-and-fall-back to "no hint" instead succeeded,
and the test failed asserting the old ("garbled") outcome against the new
(correct) one. Fix: use a fixture with a character truly outside the new
charset (a space), not a string that merely *used to* fail a stricter
parse. **General rule: when a validated type's accepted-charset widens,
grep tests for "deliberately invalid" string literals used as negative
fixtures — a literal that was invalid only by the old, narrower rule
needs replacing, not just recompiling.** (`animusd/src/topology.rs::
not_leader_refusal_tolerates_a_garbled_hint_suffix`, ADR 0040 PR3.)
