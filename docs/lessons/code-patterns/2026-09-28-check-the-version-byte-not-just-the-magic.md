# Check the version byte, not just the magic

A format decoder that validates `MAGIC` but not the adjacent version byte
silently misreads a future (or zero) version as the current one. `ADE1`'s
`scan()` had exactly this gap until ADR 0073 Phase 0 review found it, and a
"pins today's behaviour" test had been documenting it instead of failing.

- Reject a version outside `1..=CURRENT` with a distinct, loud error naming
  the format, the version found, and the supported range. It is not a torn
  file and not a foreign file: don't reuse those classifications.
- Only classify after the full header is present; a strict header prefix
  keeps its existing torn/absent classification.
- A test that documents a known gap should fail when the gap is fixed;
  prefer tracking the gap with the fix rather than a passing test.
