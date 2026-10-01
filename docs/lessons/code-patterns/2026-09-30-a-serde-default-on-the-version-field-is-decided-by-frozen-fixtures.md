# Whether a version field may have a serde default is decided by the frozen fixtures

Before deciding "drop the `"v"` default so a missing tag is an error", grep the
checked-in fixtures for the tag, including the ones that *embed* the type
(`control-wal`, `control-snapshot`, `shared-wal` embed `Metadata`). They were
written before the field existed, cannot be edited (ADR 0073), and must decode
forever, so a default on a *nested* type is forced; the outer envelope, not the
nested value, is what versions that record.

- Keep the default as the literal `1`, never the current-version constant, so a
  later bump cannot silently re-label an untagged document.
- Put the strictness on the standalone entry point (`Metadata::from_json`:
  missing `"v"` is `PreBaselineFormat`), and dispatch there with `match`.
- A type that only ever appears as a standalone document (`ClusterConfig`)
  keeps `"v"` required with no default; there is nothing to be lenient for.
