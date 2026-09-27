# A format with no version byte in its own file may still be versioned one layer up — check the caller, not just the codec

**While inventorying every persisted/wire format for ADR 0073 (upgrade
compatibility)**, a `grep -n "VERSION"` over `animus-storage/src/lsm/
sstable.rs` found no version constant next to the footer's `MAGIC`, which
looked like an unversioned format. It isn't: `SsTableMeta::format: u32`
(`FORMAT_CURRENT = 3`) is recorded **per table in the manifest**
(`lsm.rs`), one layer above the SSTable file itself — the reader is
explicitly documented to take the format from the manifest rather than
re-reading the footer. A grep scoped to the file that writes the bytes
missed the version tag that actually governs decoding.

**What to do instead:** when auditing whether a format is versioned, also
grep the *struct that records metadata about* the format (a manifest, a
catalog row, an index) — a version tag frequently lives with the pointer
to the data, not inside the data's own header, especially in a codebase
(this one) that already has a strong "manifest is the source of truth"
convention (ADR 0008/lsm.rs). Getting this wrong in an inventory document
either overstates a real gap (recommending work that's already done) or,
worse, understates one if the asymmetry runs the other way.
