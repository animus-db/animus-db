//! The per-tablet **engine layout marker** (ADR 0073 Phase 0, workstream C,
//! layer 4): a version tag for the ADR 0050 per-tablet engine key layout —
//! `kind || logical`, with the engine-global `RESERVED_NAMESPACE` markers
//! beside it.
//!
//! # Why a reserved-namespace marker and not a kind byte
//!
//! ADR 0073 recommended "a single reserved leading byte in the kind
//! namespace". That was rejected while implementing it: the kind bytes
//! (`KIND_BASE` `0x00` .. `KIND_CURSOR` `0x04`) double as indexes into
//! `ALL_KINDS`' scope table (`install_engine_image` does
//! `kind_scopes.get(kind as usize)`), so a new kind byte would have to be
//! excluded from every all-kinds scan, image and snapshot classifier — a
//! wide, easy-to-miss change to hot code. A per-record magic would change
//! every key/value and break the `kind || logical` range-scan convention.
//! The engine already has exactly the right home for a layout tag: the
//! engine-global marker family (`applied.rs`, `hwm.rs`, `seal.rs`,
//! `ceiling.rs`, `split.rs`, `trim_marker.rs`), keyed
//! `escape(RESERVED_NAMESPACE) || escape(tag) || tablet_be`, a `0x5F`-leading
//! prefix disjoint from every kind scope, so `engine_image`, `has_data` and
//! the seal/ceiling/applied scans already skip it.
//!
//! # Format
//!
//! - key: [`layout_marker_key`]`(tablet)`.
//! - value: `b"KLY1" || epoch(u8)` — [`encode_layout_value`]. `LAYOUT_EPOCH`
//!   is `1`. A layout change (a new kind byte's meaning, a changed key
//!   encoding) bumps the epoch and adds a fixture; a decoder accepts
//!   `1..=LAYOUT_EPOCH`.
//!
//! # Check and stamp
//!
//! [`check_or_stamp`] runs in `Reconciler::ensure_engine` after a successful
//! `factory.open`, before any Raft group, `InstallSnapshot`, `SeedBatch` or
//! applied marker can write to the engine, so the stamp is the **first
//! write** on a fresh engine:
//!
//! - marker present and valid for this tablet: ok;
//! - marker present but undecodable / unknown epoch: **refuse**;
//! - marker absent, engine empty (`latest_version() == 0`): **stamp**
//!   (`put` at version 1 — durable, like every sibling marker);
//! - marker absent, engine non-empty (a pre-baseline engine, or an engine
//!   that holds only *another* tablet's marker — the key embeds the tablet
//!   id): **refuse**.
//!
//! A refusal is never routed through `ensure_engine`'s destroy-and-rebuild
//! recovery: that would erase a pre-baseline or future-version engine's
//! data. See `crates/animus-cp-data/CLAUDE.md`.
//!
//! Split children are stamped by `trim_split_child` in the same write batch
//! as the trim-completion marker (see `host.rs`), so trim completion implies
//! a layout marker.
//!
//! # Flush at stamp time (isolating the marker)
//!
//! The marker's key sorts above every kind scope, so if it shared a memtable
//! with kind rows the first flushed SSTable's `[min_key, max_key]` would
//! span up to it and `LsmEngine::clone_to_filtered`'s whole-file exclusion
//! could never drop that table from a split child (until compaction). So
//! the reconciler follows every stamp — a fresh engine's, and a split
//! child's after its trim batch — with `EngineFactory::flush_engine`, which
//! an `LsmEngine`-backed factory implements as `flush_now()`: the marker
//! (plus, for a child, the trim tombstones) lands in its own table before
//! any kind row can join the memtable. The `StorageEngine` trait has no
//! flush, hence a factory hook rather than a direct call.

use animus_control::format::FormatError;
use animus_control::syskv::RESERVED_NAMESPACE;
use animus_storage::{StorageEngine, WriteBatch};
use animus_tablet::escape;

/// Format name used in [`FormatError`]s and as the fixture directory name.
pub const LAYOUT_FORMAT: &str = "cp-engine-layout";

/// The value's 4-byte magic.
pub const LAYOUT_MAGIC: [u8; 4] = *b"KLY1";

/// The layout epoch this build writes and the newest it reads.
pub const LAYOUT_EPOCH: u8 = 1;

/// Distinct from every sibling marker's tag and every `syskv::EntityKind`
/// segment (none is a prefix of another; `escape` is prefix-free).
const LAYOUT_TAG: &[u8] = b"cp_layout";

/// The MVCC version a fresh engine's stamp is written at (the engine is
/// empty, so any version `> 0` is valid; later writes are HLC-packed and far
/// larger).
const STAMP_VERSION: u64 = 1;

/// The physical, engine-global key of `tablet`'s layout marker — same shape
/// and disjointness argument as `trim_marker::trim_marker_key`.
pub fn layout_marker_key(tablet: u64) -> Vec<u8> {
    let mut out = escape(RESERVED_NAMESPACE.as_bytes());
    out.extend_from_slice(&escape(LAYOUT_TAG));
    out.extend_from_slice(&tablet.to_be_bytes());
    out
}

/// The marker's value: `magic(4) || epoch(u8)`.
pub fn encode_layout_value() -> Vec<u8> {
    let mut out = Vec::with_capacity(5);
    out.extend_from_slice(&LAYOUT_MAGIC);
    out.push(LAYOUT_EPOCH);
    out
}

/// Decode a marker value, returning its epoch.
pub fn decode_layout_value(bytes: &[u8]) -> Result<u8, FormatError> {
    if bytes.len() < 4 || bytes[..4] != LAYOUT_MAGIC {
        return Err(FormatError::PreBaselineFormat {
            format: LAYOUT_FORMAT,
        });
    }
    if bytes.len() != 5 {
        return Err(FormatError::Malformed {
            format: LAYOUT_FORMAT,
            detail: format!("expected 5 bytes, got {}", bytes.len()),
        });
    }
    let epoch = bytes[4];
    if epoch == 0 || epoch > LAYOUT_EPOCH {
        return Err(FormatError::UnsupportedFormatVersion {
            format: LAYOUT_FORMAT,
            found: epoch,
            max_supported: LAYOUT_EPOCH,
        });
    }
    // The marker has no body beyond the epoch, so dispatch is just "is this
    // epoch one we know"; a future epoch with a body routes to its decoder
    // here, older epochs into [`legacy`].
    match epoch {
        1 => Ok(epoch),
        found => Err(FormatError::UnsupportedFormatVersion {
            format: LAYOUT_FORMAT,
            found,
            max_supported: LAYOUT_EPOCH,
        }),
    }
}

/// Retired format versions (ADR 0073 "The decoder pattern", point 4). Each
/// retired version `N` gets a submodule `legacy::vN` holding its frozen
/// decoder, the frozen shape type that decoder produces (`VNFoo`), and the
/// `From<VNFoo>` translation into the current in-memory type; the version
/// `match` in the public decoder routes to it. Kept forever, edited only by
/// mechanical compile fixes. Empty today: every version of this codec is
/// still v1, i.e. current.
mod legacy {}

/// Why [`check_or_stamp`] / [`verify`] did not return `Ok`.
#[derive(Debug)]
pub enum LayoutError {
    /// The engine's layout is not one this build may use (pre-baseline,
    /// future epoch, corrupt marker). The engine must be left untouched.
    Refused(FormatError),
    /// The engine read/write itself failed. Transient or corrupt; says
    /// nothing about the layout. The engine must be left untouched.
    Storage(String),
}

impl std::fmt::Display for LayoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LayoutError::Refused(e) => write!(f, "layout refused: {e}"),
            LayoutError::Storage(e) => write!(f, "layout marker storage error: {e}"),
        }
    }
}

/// Read and validate `tablet`'s marker: `Ok(true)` valid, `Ok(false)` absent,
/// `Err(Refused)` present but unusable.
async fn read_marker<S: StorageEngine>(engine: &S, tablet: u64) -> Result<bool, LayoutError> {
    match engine.get(&layout_marker_key(tablet)).await {
        Ok(Some(v)) => decode_layout_value(&v.value)
            // Every supported epoch means "marker present and usable"; an
            // epoch-specific check would dispatch on the returned epoch here.
            .map(|_supported_epoch| true)
            .map_err(LayoutError::Refused),
        Ok(None) => Ok(false),
        Err(e) => Err(LayoutError::Storage(e.to_string())),
    }
}

/// Require a valid marker for `tablet`; never writes.
pub async fn verify<S: StorageEngine>(engine: &S, tablet: u64) -> Result<(), LayoutError> {
    if read_marker(engine, tablet).await? {
        Ok(())
    } else {
        Err(LayoutError::Refused(FormatError::PreBaselineFormat {
            format: LAYOUT_FORMAT,
        }))
    }
}

/// The check-and-stamp described in the module doc. `Ok(true)` means this
/// call stamped a fresh engine (the caller then isolates the marker with
/// `EngineFactory::flush_engine`); `Ok(false)` means a valid marker was
/// already present.
pub async fn check_or_stamp<S: StorageEngine>(
    engine: &S,
    tablet: u64,
) -> Result<bool, LayoutError> {
    if read_marker(engine, tablet).await? {
        return Ok(false);
    }
    if engine.latest_version() != 0 {
        return Err(LayoutError::Refused(FormatError::PreBaselineFormat {
            format: LAYOUT_FORMAT,
        }));
    }
    engine
        .put(
            &layout_marker_key(tablet),
            &encode_layout_value(),
            STAMP_VERSION,
        )
        .await
        .map_err(|e| LayoutError::Storage(e.to_string()))?;
    Ok(true)
}

/// Append this build's marker for `tablet` to `batch` (used by the split
/// child's trim-completion batch).
pub fn add_stamp(batch: WriteBatch, tablet: u64) -> WriteBatch {
    batch.put(layout_marker_key(tablet), encode_layout_value())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_is_disjoint_from_sibling_markers_and_per_tablet() {
        let k = layout_marker_key(7);
        assert_ne!(k, layout_marker_key(8));
        for other in [
            crate::trim_marker::trim_marker_key(7),
            crate::hwm::hwm_marker_key(7),
            crate::ceiling::ceiling_marker_key(7),
        ] {
            assert!(!k.starts_with(&other) && !other.starts_with(&k));
        }
    }

    #[test]
    fn value_decodes_and_refuses_by_name() {
        assert_eq!(decode_layout_value(&encode_layout_value()), Ok(1));
        assert_eq!(
            decode_layout_value(b"XXXX\x01"),
            Err(FormatError::PreBaselineFormat {
                format: LAYOUT_FORMAT
            })
        );
        assert_eq!(
            decode_layout_value(b""),
            Err(FormatError::PreBaselineFormat {
                format: LAYOUT_FORMAT
            })
        );
        for bad in [0u8, 2, 255] {
            let mut v = encode_layout_value();
            v[4] = bad;
            assert_eq!(
                decode_layout_value(&v),
                Err(FormatError::UnsupportedFormatVersion {
                    format: LAYOUT_FORMAT,
                    found: bad,
                    max_supported: 1
                })
            );
        }
        assert!(matches!(
            decode_layout_value(b"KLY1\x01\x00"),
            Err(FormatError::Malformed { .. })
        ));
    }
}
