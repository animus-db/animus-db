# A required JSON `"v"` field needs a check before serde, or the error is anonymous

Making `"v"` a required field on a `serde_json` config type (ADR 0073 Phase 0,
Workstream E, `ClusterConfig`) is not enough on its own: a document without
it fails with serde's generic "missing field `v`", indistinguishable from any
other malformed config, and a future `"v": 2` document would be parsed
best-effort by the v1 struct. The named, loud errors the ADR requires
(`FormatError::PreBaselineFormat` / `UnsupportedFormatVersion`) only come from
parsing to `serde_json::Value`, inspecting `"v"`, and only then
`from_value`. Also: every struct-literal construction site (here ~75 test
files) must set the new field, so expose a `..._VERSION` constant and use it
rather than a literal `1`.

Environment note: with a small disk, `CARGO_PROFILE_DEV_DEBUG=0
CARGO_PROFILE_TEST_DEBUG=0` shrinks the ~60 animusd test binaries from ~22GB
to a few GB; a full `target/` can starve even the harness's own task-output
files (ENOSPC), which then hides command output entirely.
