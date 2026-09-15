# A field merged into a different name at the wire layer stays a silent runtime miss wherever a caller reads it by string key, not by a typed struct (docs/roadmap.md U-05, control-members panel action buttons)

Grounding the control-members Add button against `animus-cli`'s own
`run_control_add` (the task's named ground truth for the wire body) turned
up a real, previously undetected bug unrelated to this slice's own diff:
that function resolves a new voter's internal control-Raft address by
`GET`ting the new node's own `/admin/config` and reading `cfg["control"]`
as a `serde_json::Value` index. `/admin/config`'s `config_view` has not
served a top-level `control` field since ADR 0040 PR1 merged the old
`control`/`raftkv` address pair into one `addrs.internal` field — a
compiler-enumerated fan-out for every *Rust* construction site
(`error[E0063]`, per this file's own entries on that migration), but
`cfg["control"]` is a runtime string-keyed lookup into freshly-parsed JSON,
which the compiler cannot check at all. The result: `cfg["control"]` has
resolved to `None` on every call since that merge, so the 3-argument
(operator-supplied-id) form of `animus admin control-add` has been
silently broken ever since, with no test in this workspace ever
exercising that code path to catch it (grep confirmed `crates/animus-cli`
has no `tests/` directory at all).

**The general lesson**: a field rename/merge on a JSON view is only as
safe as the compiler's reach into every reader. A `#[derive(Serialize)]`
struct's own field gets `error[E0063]`'s fan-out for free at every
*construction* site in the same language; a `serde_json::Value["field"]`
read anywhere — a CLI, a dashboard's JS, a shell script — gets no signal
at all when the field it names stops existing, and keeps compiling and
running while quietly returning `None`/`null`/`undefined` forever. When
renaming or merging a field on any admin/wire JSON view, grep the whole
workspace (and any JS/shell consumer, not just other Rust crates) for the
old field's string literal, not just for the struct that used to carry it
— the "compiler enumerates every site" safety net this codebase leans on
elsewhere (port additions, `ClusterConfig` fields) does not extend past
the language boundary. Reported as a pre-existing `animus-cli` bug, not
fixed here — out of this slice's own scope, and it does not block the
dashboard's own Add control, which asks the operator for the address
directly rather than reproducing the CLI's now-broken shortcut.

**Fixed 2026-09-06** (the control-add `/admin/config` field issue):
`run_control_add`'s extraction moved into a small pure helper,
`internal_addr_from_admin_config`, reading `cfg["addrs"]["internal"]`
instead — unit-tested against both the current shape and the removed
legacy `{"control": ..}`-only shape (the latter must now error, not
silently resolve). `crates/animusd/tests/control_membership_admin.rs`
gained a real-cluster regression pinning `/admin/config`'s actual wire
shape against that same key path, closing the "no test in this workspace
ever exercised that code path" gap this entry originally named. This is
the concrete instance the general lesson above already generalizes from —
no new lesson to add here beyond it now having a regression on both
sides of the JSON boundary (a pure-function unit test for the extraction,
a real-server test for the shape it extracts from).
