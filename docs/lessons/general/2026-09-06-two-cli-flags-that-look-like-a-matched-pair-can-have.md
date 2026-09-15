# Two CLI flags that look like a matched pair can have asymmetric grammars — verify each one's own parser, not the sibling's doc comment (S-07b, `--backup-store`/`--segment-store`)

Adding `animus-operator`'s CRD surface for `animusd`'s non-S3
`--backup-store`/`--segment-store` flags (S-07b), the natural assumption
from reading `--backup-store cluster|fs:PATH` was that `--segment-store`
would accept the identical `cluster|dir:PATH` shape — the two flags are
documented side by side, configure the same kind of thing (a
`SegmentStore`-shaped trait object), and `parse_backup_store`'s own doc
comment even says `fs:`/`dir:` are "the same forms `parse_segment_store`
accepts." Reading `parse_segment_store`'s actual `match` arms
(`crates/animusd/src/main.rs`) showed otherwise: it has no `"cluster"` arm
at all — `None` (omitting the flag) is the *only* way to select its
default, and a literal `"cluster"` value falls through to the `dir:`
match arm and gets rejected as malformed. `parse_backup_store`'s own doc
comment names this precisely as one thing `--segment-store` does
differently, but it would have been easy to skip re-reading that comment
because "the same forms" reads as symmetry at a glance. The CRD's
`AnimusClusterSpec::validate_store_spec` had to encode this asymmetry
explicitly — `segmentStore: "cluster"` is a rejected value, not silently
accepted or remapped to "omit the field" — otherwise a spec written by
analogy with `backupStore: "cluster"` would pass CRD validation and then
fail at pod startup when `animusd`'s own parser rejected the flag.
**General form**: when building a second, independent syntax-checking
layer over an existing CLI parser (this crate deliberately doesn't depend
on `animusd`, so it re-implements a narrower check of the same grammar —
see `crd::S3StoreSpec::validate`'s identical posture for the `s3://` form),
never infer one flag's accepted-values grammar from a sibling flag's
`match` arms or from a comment describing them as parallel — read that
flag's own parser function directly, arm by arm. Two CLI options that look
like a matched pair from their names and shared doc prose can still differ
in exactly the one place that matters (which literal keywords each
accepts), and the mismatch only surfaces as a runtime rejection at
container start, never a compile error or a CRD-validation failure, unless
the second layer's own test suite explicitly pins the asymmetric case (see
`crd::tests::store_spec_rejects_segment_store_cluster_literal`).
