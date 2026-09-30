# A version range gate that drops the version is not a dispatch

Every Phase 0 decoder checks `1..=CURRENT`, errors by name outside it, and
then decodes the body with **one** function; `format::unwrap`/`decode_lines`
even return the version, and the callers bind it as `_version` and throw it
away. That is correct and loud for a format with one version, but it is not
the structure "every older version stays readable forever" needs: adding v2
means editing the single body decoder in place, and v1 survives only if the
one representative v1 fixture happens to exercise whatever changed.

- Audit a "forever readable" claim by asking two separate questions: can the
  old decoder be *deleted* silently (no: the fixture guard plus a
  directory-iterating decode test catch it), and can an old file be
  *misread* silently after a shape change (yes, if the decoder is a gate over
  one body). The fixture layer answers the first, not the second.
- Make the version reach the body: `match version { 1 => legacy::v1::..,
  N => current, found => Err(Unsupported..) }`. A bound-and-dropped version
  next to a format gate is the smell.
- A decode test that asserts *the same expected value for every fixture*, or
  asserts *the current version* for every fixture (`preamble.version ==
  spec.version`), cannot accommodate a second version: it fails on the
  retained v1 file, which is the opposite of what the guard script wants.
  Match on the file's own version and `panic!` on an unrecognised one, so
  adding a fixture forces adding its expectation.
- An untagged JSON row value (stored item, `ChangeRecord`, mirror entity
  values) is a durable format even with no version byte; it needs a fixture
  that pins its exact shape, or a `#[serde(default)]` addition is an
  unreviewed format change. See ADR 0073's "Phase 1 design" amendment.
