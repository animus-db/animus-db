# A doc that says "X is rejected by validation" must name the test that proves it

ADR 0060's "Upgrades" section said a `spec.image` change was "either rejected
by the operator's own validation or requires recreating the cluster". The
Phase 1 close-out of ADR 0073 found no such validation: `validate_spec` has a
test that explicitly *allows* an image edit, and the `StatefulSet` controller
rolls it, which is the unsupported mixed-version case. The claim had survived
because it read as a safe, conservative statement and nobody re-checked it
when the surrounding policy changed.

**Rule:** when a doc or ADR says a dangerous operation is blocked, grep for the
guard (and its test) before repeating or relying on the sentence. If the guard
does not exist, say so in the doc and file the guard as work, rather than
leaving the reader to assume protection. Close-out PRs that restate a
contract should verify each "not supported" claim the same way as each
"supported" one.
