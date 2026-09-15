# Per-crate `CLAUDE.md` guides re-drifted past the 40K-char memory-file warning by 2026-08-15, five days after the 2026-08-13 PR-changelog/ test-roster trim above — this time the dominant offender was a different failure mode: restating a module's own `//!`/rustdoc doc comment

**Per-crate `CLAUDE.md` guides re-drifted past the 40K-char memory-file
warning by 2026-08-15, five days after the 2026-08-13 PR-changelog/
test-roster trim above — this time the dominant offender was a different
failure mode: restating a module's own `//!`/rustdoc doc comment**, not
narrating PRs. `animus-control`/`animusd`/`animus-cp-data` had each grown
a bullet (`meta.rs`, `segment_janitor.rs`/`index_drain.rs`,
`cluster_segment_store.rs`/`cursor.rs`) that duplicated, near-verbatim, an
80–95-line module `//!` doc the source already carried — and `animus-dynamo`
had grown a full method/type inventory per module that `cargo doc` already
renders. The doc comment and the guide bullet then drift independently the
next time either is edited, and a reader can no longer tell which one is
current. Fix, same shape as the 2026-08-13 trim: cut a duplicated
inventory/essay to a one-or-two-line pointer bullet ("what it is + see its
`//!` doc + ADR ref"), while keeping every gotcha/failure-contract/
prohibition verbatim (compressing surrounding prose is fine; dropping the
claim itself is not). Where a genuinely non-derivable contract lived
buried inside an otherwise-duplicative section (`animusd`'s DynamoDB
Streams wire-edge contracts and sealer-knob call-site detail — real
content, just misfiled under a module that also duplicated its own doc
comment), it was moved verbatim to a dedicated companion doc
(`docs/streams-notes.md`) rather than deleted, with the guide left holding
only a pointer. Trimmed `animus-control` 48.6K→29.6K, `animusd`
87.3K→52.5K (plus the new companion doc), `animus-cp-data` 60.1K→40.5K,
`animus-dynamo` 30.1K→15.6K, `animus-storage` 31.5K→26.4K (only its
test-narration section, per the 2026-08-13 lesson's own pattern) — several
landed above their aspirational target because the crate's actual
gotcha/invariant content, kept in full per the rule above, is simply
larger than the target for that crate. **General rule for review**: a
guide addition that could be produced by pasting a module's own doc
comment, or that `cargo doc`/`ls` already renders, is a sign to point at
that source instead of copying it — reject it the same way a
PR-changelog paragraph already gets rejected. A doc comment and a guide
bullet describing the same mechanism are not two independent sources of
truth; only one of them can be current. (2026-08-15.)
