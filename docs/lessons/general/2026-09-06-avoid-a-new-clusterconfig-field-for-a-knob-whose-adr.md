# Avoid a new `ClusterConfig` field for a knob whose ADR already specifies "static, file/env-sourced" scope — the `error[E0063]` fan-out cost isn't worth paying twice (S-04 PR 2, `animusd`)

This crate's own history already names the cost of adding a field to
`ClusterConfig`: `dynamo_auth` and `cluster_settings` each triggered a
compiler-enumerated ~55-60-call-site `error[E0063]: missing field` fan-out
across every `ClusterConfig { .. }` struct literal in `src/`+`tests/`
(`crates/animusd/CLAUDE.md`'s `config.rs` entry has the blow-by-blow). S-04
PR 2 needed a place for S3 credentials to live and could have followed
`dynamo_auth`'s own precedent (an `Option<S3CredentialsConfig>` field on
`ClusterConfig`, populated from a config file's own section or a CLI flag
merged in with a "one way, not both" conflict check) — but the ADR
amendment this PR implements had already specified the credential-sourcing
posture in full: static access-key-id/secret pair, from a config file or
environment variable, mirroring ADR 0057's own `dynamo_auth` static map.
Nothing about that posture needs `ClusterConfig`'s own per-node array
shape or its "one config file describes the whole cluster" semantics — a
set of S3 credentials is either process-global or, at most, per-invocation,
never something a `ClusterConfig`'s per-node `nodes[]` entries need to vary
independently.

**The decision**: skip `ClusterConfig` entirely. `--s3-credentials PATH`
names a **standalone** JSON file (`main.rs::S3CredentialsFile`), parsed and
resolved independently of any `ClusterConfig` load, with an environment-
variable fallback when the flag is omitted. This is a deliberate departure
from the `dynamo_auth` precedent this codebase would otherwise reach for
by pattern-matching — worth stating explicitly, since "make it look like
the existing similar feature" is usually the right instinct and was
wrong here specifically because the *scope* of the two features differs
(a per-cluster credential *map*, keyed by access key id, genuinely
benefits from living in the cluster-wide config a `dynamo_auth` section
already is; a single static credential *pair* for one external system
does not). **General form**: before reaching for an existing sibling
feature's storage shape as a template, check whether its own scope
(per-node vs. per-cluster vs. per-process) actually matches the new
feature's — matching the pattern when the scope differs just imports that
pattern's own cost (here, a `ClusterConfig` field's mechanical fan-out) for
no benefit, and a real ADR that already specifies a narrower scope is
license to use a narrower, cheaper mechanism instead.
