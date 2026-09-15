# A default-flip cutover must audit every entry point a shared config field is documented to reach, not just the flag's own two wired call sites (C-05 PR 3, `--shared-wal` on `animusd data --config`)

`--shared-wal`'s C-05 PR 2 wired the flag through exactly two production
entry points (`--config/--node` and `--cluster N`) and documented the
narrower gap explicitly — `--cluster-control`/`--cluster-data`, `join`/
`data --seed`, and a list of narrower test wrappers, all hardcoding
`false`. What that PR's own doc did **not** flag was a third, quieter
gap: `cluster_settings.shared_wal` (the config-file field, `config.rs`)
was already documented as one of the fields "any data-hosting node
(combined or data-only)" reads — the exact same sentence `heartbeat_batch`
sits in — but `run_node_data_with_cluster_settings` (the function
`animusd data --config` actually calls) had no `shared_wal` parameter at
all. Setting `cluster_settings.shared_wal: true` in a config file handed
to a data-only node did nothing, silently, with no error — the config
section parsed fine (`#[serde(default)]`), the field was simply never
read on that code path. This wasn't caught by any test because every
existing `shared_wal`-focused test exercises the combined-mode/`--cluster
N` paths PR 2 actually wired; nothing tests `animusd data --config`
against a `cluster_settings.shared_wal` value at all, so a field that
does nothing produces no observable failure — it just quietly loses the
setting.

**Why PR 3, specifically, is when this had to surface**: the moment a
flag's *default* flips for every entry point the flag's own doc claims to
reach, an entry point that was silently ignoring the field stops being a
narrow, individually-documented scope cut and starts being a genuine
cross-deployment-shape inconsistency — a split deployment's combined/
control nodes would get the new shared-WAL default while its data-only
nodes stayed permanently on the old per-group layout with no way to opt
in at all (not even a documented workaround), the exact kind of
asymmetry a cutover PR's own soak (`cargo test --workspace`) cannot catch
by itself, because nothing exercises that specific combination.

**Fix**: `BoundDataNode::start_data_with_growth` gained the identical
trailing `shared_wal: bool` parameter (and the identical
`check_wal_layout`/`SharedWal::open`/`enable_shared_wal` call sequence)
`BoundNode::start_with_growth` already had; `run_node_data_with_cluster_
settings` and `main.rs`'s `run_data_config` now thread
`settings.shared_wal.unwrap_or(DEFAULT_SHARED_WAL)` through it, closing
the gap in the same commit as the default flip rather than leaving it for
a "future PR... if ever needed."

**General form**: when a cutover PR flips a flag's default, don't just
grep for the flag's own two or three wired call sites and confirm they
still compile — grep for every place the flag's *underlying config field*
is documented to apply (a doc comment's "applies to any X" sentence, a
struct's field-applicability list) and confirm each one actually reads
it. A field that silently does nothing is invisible to `cargo test
--workspace`, invisible to clippy, and invisible to the flag's own unit
tests (which only exercise the paths that DO read it) — the only way to
find it is to read the applicability claim against the code that's
supposed to honor it, the same "grep the code, don't trust the prose"
discipline root `CLAUDE.md`'s own closing convention already names for a
different kind of drift.
