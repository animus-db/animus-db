# A restored/imported value must go back through the SAME envelope-wrap primitive its store's read side expects, every time this pattern is repeated (ADR 0068, S-05 PR 3 `SimEnv` corpus)

Building `export_import_fault_corpus.rs`'s import-side mirror
(`import_tick_mirror`, standing in for `animusd::import::import_tick`), the
first draft pushed `animus_item::derive_kind_writes`'s raw stored-item
bytes straight into `KvCommand::SeedBatch` — every derived write was a
byte-identical shape to what production's own driver derives, but without
production's own trailing step: `backup_codec::encode_restored_value`,
which re-wraps a physical value in the 1-byte transaction envelope tag
`animus-cp-data`'s apply path expects every merged value to carry
(`Envelope::Committed`/`Intent`, `crates/animus-cp-data/src/txn.rs`).
`SeedBatch`'s own merge writes the bytes it's given verbatim, with no
validation — so the corpus didn't get a "wrong content" failure, it got a
"corrupt engine value" panic the very next time *anything* read the row
back (`decode_envelope`'s `unknown envelope tag N`), on the very first
depth-1 run, every seed.

**This is the second time this exact hazard has been hit and fixed in this
same file family** — `backup_fault_corpus.rs`'s own restore-tick mirror
(`crates/animus-test/tests/backup_fault_corpus.rs`) already documents
finding and fixing it for the on-demand-backup restore path (ADR 0059 §7),
with its own doc comment naming `encode_restored_value`'s call as
load-bearing precisely for this reason. The import mirror didn't reuse
that lesson because its seed-derivation path (`derive_kind_writes`, a
different producer than backup's own captured-physical-bytes path) was
written fresh rather than adapted from the restore-tick precedent sitting
in a sibling file in the same crate.

**General form**: any code that constructs a `SeedRow`/calls
`propose_seed_batch` — present or future, in production or in a corpus
mirror — must re-wrap every value through `encode_restored_value` (or
whatever the current envelope-wrap primitive is named) before merging,
regardless of where the value came from (a captured physical byte string,
or freshly derived from decoded item content). `SeedBatch`'s own contract
gives no structural guarantee here — nothing type-checks a raw value
against an enveloped one, since both are `Option<Vec<u8>>` — so grep every
`propose_seed_batch`/`SeedRow` construction site when adding a new one, the
same "grep every gating match site" discipline this repo's root `CLAUDE.md`
already states for a replicated/forwarded command enum gaining a variant.
The failure mode when this is missed is maximally loud (an immediate hard
panic on first read, not a silent wrong value), which is exactly why it
was caught before the corpus was ever committed rather than shipping as a
false-negative-green test — but "loud when reached" is not the same as
"reached promptly": a producer whose own path happens not to be read back
in the same test run would have shipped silently broken until something
else finally decoded the row.
