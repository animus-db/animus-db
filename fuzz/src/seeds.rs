//! Seed inputs, shared by the corpus generator (`seed-corpus`) and the stable
//! smoke test so both see exactly the same set.
//!
//! Three sources:
//! 1. `seeds.tsv` — `target<TAB>decoder-name<TAB>repo-relative path`: every
//!    file under the path (recursively) is a seed for `target`, prefixed with
//!    the `decoder-name\n` routing line (see [`crate::targets::route`]). These
//!    are the golden format fixtures (ADR 0073), read in place — no binary is
//!    copied into the repo.
//! 2. [`derived`] — slices of those fixtures aimed at inner decoders the
//!    whole-file fixtures cannot reach directly (an SSTable's block-index
//!    region and first data block, each WAL record payload, each RaftKV wire
//!    frame).
//! 3. `fuzz/seeds/<target>/*` — small hand-written text seeds for the parsers
//!    that have no fixture (expressions, PartiQL, HTTP/SigV4, ...), used
//!    verbatim.

use std::path::{Path, PathBuf};

/// One seed input for one target.
#[derive(Clone, Debug)]
pub struct Seed {
    /// Target name (a key of [`crate::targets::ALL`]).
    pub target: String,
    /// Human-readable provenance (file path or derivation), used for the
    /// corpus file name and failure messages.
    pub label: String,
    /// The exact fuzzer input.
    pub bytes: Vec<u8>,
}

/// The repository root (`fuzz/..`).
#[must_use]
pub fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("fuzz/ lives directly under the repo root")
        .to_path_buf()
}

fn files_under(path: &Path, out: &mut Vec<PathBuf>) {
    if path.is_file() {
        out.push(path.to_path_buf());
    } else if let Ok(rd) = std::fs::read_dir(path) {
        let mut entries: Vec<_> = rd.filter_map(Result::ok).map(|e| e.path()).collect();
        entries.sort();
        for e in entries {
            files_under(&e, out);
        }
    }
}

fn routed(name: &str, body: &[u8]) -> Vec<u8> {
    let mut v = name.as_bytes().to_vec();
    v.push(b'\n');
    v.extend_from_slice(body);
    v
}

fn rel(p: &Path) -> String {
    p.strip_prefix(repo_root())
        .unwrap_or(p)
        .to_string_lossy()
        .into_owned()
}

/// Seeds named by `seeds.tsv`.
fn from_manifest() -> Vec<Seed> {
    let tsv = std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("seeds.tsv"))
        .expect("fuzz/seeds.tsv");
    let mut out = Vec::new();
    for line in tsv
        .lines()
        .filter(|l| !l.trim().is_empty() && !l.starts_with('#'))
    {
        let cols: Vec<&str> = line.split('\t').collect();
        assert_eq!(
            cols.len(),
            3,
            "seeds.tsv row needs 3 tab-separated columns: {line:?}"
        );
        let mut files = Vec::new();
        files_under(&repo_root().join(cols[2]), &mut files);
        assert!(!files.is_empty(), "seeds.tsv path {} has no files", cols[2]);
        for f in files {
            let bytes = std::fs::read(&f).expect("read fixture");
            out.push(Seed {
                target: cols[0].to_owned(),
                label: rel(&f),
                bytes: routed(cols[1], &bytes),
            });
        }
    }
    out
}

fn u32_le(b: &[u8], at: usize) -> Option<usize> {
    Some(u32::from_le_bytes(b.get(at..at + 4)?.try_into().ok()?) as usize)
}

fn u64_le(b: &[u8], at: usize) -> Option<usize> {
    Some(u64::from_le_bytes(b.get(at..at + 8)?.try_into().ok()?) as usize)
}

/// Slices of the fixtures aimed at inner decoders.
fn derived() -> Vec<Seed> {
    let root = repo_root();
    let mut out = Vec::new();
    let mut push = |target: &str, label: String, name: &str, body: &[u8]| {
        out.push(Seed {
            target: target.to_owned(),
            label,
            bytes: routed(name, body),
        });
    };

    // SSTable: block-index region + the first data block (CRC stripped).
    let sst_dir = root.join("crates/animus-storage/tests/fixtures/formats/lsm-sstable");
    let mut files = Vec::new();
    files_under(&sst_dir, &mut files);
    for f in files {
        let img = std::fs::read(&f).expect("read sstable fixture");
        let n = img.len();
        if n < 24 {
            continue;
        }
        let (Some(io), Some(il)) = (u64_le(&img, n - 24), u64_le(&img, n - 16)) else {
            continue;
        };
        let Some(region) = img.get(io..io + il) else {
            continue;
        };
        push(
            "lsm_formats",
            format!("{} (index region)", rel(&f)),
            "sstable_index",
            region,
        );
        // First index entry: magic(4) ver(1) count(4) | key_len(4) key off(8) len(8) ...
        if let Some(kl) = u32_le(region, 9)
            && let (Some(off), Some(len)) = (u64_le(region, 13 + kl), u64_le(region, 21 + kl))
            && let Some(block) = img.get(off..off + len.saturating_sub(4))
        {
            push(
                "lsm_formats",
                format!("{} (first block)", rel(&f)),
                "sstable_block",
                block,
            );
        }
    }

    // LSM WAL: each record payload (frames `len u32 BE | crc u32 BE | payload`).
    let wal_dir = root.join("crates/animus-storage/tests/fixtures/formats/lsm-wal");
    let mut files = Vec::new();
    files_under(&wal_dir, &mut files);
    for f in files {
        let wal = std::fs::read(&f).expect("read wal fixture");
        let mut pos = 5;
        let mut i = 0;
        while pos + 8 <= wal.len() {
            let len = u32::from_be_bytes(wal[pos..pos + 4].try_into().expect("4 bytes")) as usize;
            let Some(payload) = wal.get(pos + 8..pos + 8 + len) else {
                break;
            };
            push(
                "lsm_formats",
                format!("{} (record {i})", rel(&f)),
                "wal_record",
                payload,
            );
            pos += 8 + len;
            i += 1;
        }
    }

    // RaftKV wire fixture: a pack of `len u32 BE | frame`.
    let wire_dir = root.join("crates/animus-cp-data/tests/fixtures/formats/raftkv-wire");
    let mut files = Vec::new();
    files_under(&wire_dir, &mut files);
    for f in files {
        let pack = std::fs::read(&f).expect("read wire fixture");
        let mut pos = 0;
        let mut i = 0;
        while pos + 4 <= pack.len() {
            let len = u32::from_be_bytes(pack[pos..pos + 4].try_into().expect("4 bytes")) as usize;
            let Some(frame) = pack.get(pos + 4..pos + 4 + len) else {
                break;
            };
            push(
                "cp_data_formats",
                format!("{} (frame {i})", rel(&f)),
                "raftkv_wire",
                frame,
            );
            pos += 4 + len;
            i += 1;
        }
    }
    out
}

/// The hand-written seeds under `fuzz/seeds/<target>/`.
fn handwritten() -> Vec<Seed> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("seeds");
    let mut files = Vec::new();
    files_under(&dir, &mut files);
    files
        .into_iter()
        .filter_map(|f| {
            let target = f.parent()?.file_name()?.to_str()?.to_owned();
            Some(Seed {
                target,
                label: rel(&f),
                bytes: std::fs::read(&f).ok()?,
            })
        })
        .collect()
}

/// Every seed for every target, in a deterministic order.
#[must_use]
pub fn all() -> Vec<Seed> {
    let mut v = from_manifest();
    v.extend(derived());
    v.extend(handwritten());
    v
}
