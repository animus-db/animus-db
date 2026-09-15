# `DiskConfig::torn_tail_on_crash`/`corrupt_on_crash` were never actually exercised against a `RaftCore`-backed WAL before (ADR 0061 Decision 3, `stream_lineage_corpus.rs` fault wiring)

Both fields are documented and unit-tested in `animus-sim` itself
(`tests/disk_faults.rs`) and exercised against `animus-storage`'s
`LsmEngine` (`tests/lsm_disk_faults.rs`), but grepping the whole workspace
for either name turned up no use anywhere in `animus-control` or
`animus-cp-data` — meaning the shared Raft consensus WAL format
(`animus_control::persist::{WalRecord, PersistedState}`, used by *both* the
control plane's own `RaftCore<MetaCommand, Metadata>` and every per-tablet
`RaftKvNode`'s `RaftCore<KvCommand, KvState>`) had never actually been
crashed-and-torn under simulation before this corpus's own
`torn_tail_crash_survives_a_true_restart` cell. Worth confirming *why* it's
safe before assuming so: `PersistedState::decode`/`decode_tagged`
(`persist.rs`) frame the WAL as newline-delimited JSON and recover via
`bytes.split(b'\n').filter_map(|line| serde_json::from_slice(line).ok())`
— any line that fails to parse, not merely a strict trailing partial one,
is silently dropped. This is a *more* permissive recovery contract than
`animus-storage`'s own length-prefixed-plus-CRC WAL format (`lsm/wal.rs`),
which goes out of its way to distinguish a genuinely torn trailing record
from mid-file corruption by position (`wal_resync_point`) and hard-errors
on the latter — but it's still sound for this WAL's own recovery
discipline (an un-replayed record's effect was, by definition, never acted
on), so `torn_tail_on_crash`/`corrupt_on_crash` compose safely with it with
no risk of a hard panic or silent divergence, unlike the LSM format's own
stricter contract. The general check before wiring either fault into a new
corpus: confirm which WAL/durable-record codec the crashed component
actually uses, and whether that codec's own recovery already documents (or
can be shown to have) a total, non-panicking answer for "this record failed
to parse" — not just a trailing-partial-write answer, since `corrupt_on_
crash` can in principle land its flipped byte anywhere inside the retained
(non-dropped) tail, not only in what would otherwise have been a clean
trailing partial line.
