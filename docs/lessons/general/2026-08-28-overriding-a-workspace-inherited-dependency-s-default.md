# Overriding a workspace-inherited dependency's `default-features` (ADR 0061 rung C1)

Creating `animus-node` with `animus-env = { workspace = true, default-features
= false }` fails at manifest-parse time — "`default-features = false` cannot
override workspace's `default-features`" — even though the root
`[workspace.dependencies]` entry for `animus-env` doesn't mention
`default-features` at all (so the *effective* default is already "no
features on," since `animus-env`'s own `[features]` table declares no
`default = [...]` list). Cargo's inheritance rule cares about the presence
of the override relative to what the root entry states, not about what the
resulting feature set would actually be — a member cannot set
`default-features = false` on an inherited dependency unless the root entry
already says `default-features = false` too. The fix is not to touch the
root entry (that would flip every other consumer's default, the same
one-crate-widens-scope-for-everyone hazard the ADR 0061 rung C0 entry above
warns about with feature gates generally) but to stop inheriting for that
one line: `animus-env = { path = "../animus-env", default-features = false
}`, a direct path dependency, sidesteps workspace-dependency inheritance
entirely and takes the override with no restriction. General lesson: when a
new crate needs a *stricter* feature configuration than a workspace
dependency's inherited default for one line only, reach for a direct `path`
(or `version`) dependency rather than fighting the inheritance override
rule — it's not a workaround, it's the mechanism Cargo actually offers for
"this one dependency, this one consumer, opts out of workspace defaults."
