# A `Copy`-carrying `MetaCommand` variant added to a large enum needs its own biggest field boxed up front, not discovered via `clippy::large_enum_variant` (ADR 0068 §6, S-05 PR 2)

Adding `MetaCommand::BeginImport` (mirroring `BeginRestore`'s own shape,
plus a full `TableCreationParameters`-derived schema) pushed `MetaCommand`'s
in-memory size past clippy's `large_enum_variant` threshold: every other
variant stays small, so the *whole enum* — sized to its largest member,
Rust's ordinary tagged-union layout — grew to fit this one, at a real
per-value cost paid by every `MetaCommand` anywhere in the system,
including the thousands that are `NoOp`/`CasTabletReplicas`/etc. The fix
(boxing the schema field, `base_schema: Box<TableSchema>`) is mechanical
once found, but finding it required a `cargo clippy --all-targets
--all-features` pass — a plain `cargo build`/`cargo test` never surfaces
this lint at all, so a large new variant landing between clippy runs would
have shipped its size-bloat undetected until CI. **General form**: when
adding a variant to an already-large, already-established enum
(`MetaCommand`, `Operation`, `ClientRequest`, any command/message enum
with many small variants) and the new variant carries a whole nested
struct (a schema, a plan, a manifest) rather than a handful of scalars,
run clippy on it *before* considering the shape done — box the field
`clippy::large_enum_variant` names as the outlier immediately, rather than
letting review or CI catch it later. A closely related trap in the same
change: cloning an `Option<T>` field where `T: Copy` (`clippy::
clone_on_copy`) compiles and passes every test, so it's easy to write by
habit (matching the `.clone()` every *non-`Copy`* sibling field in the same
struct literal needs) and never notice — clippy catches it, but only if
run.
