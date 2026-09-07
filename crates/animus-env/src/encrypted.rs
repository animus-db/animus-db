//! Encryption at rest (ADR 0069): a frame-sealing AEAD wrapper over any
//! [`Disk`] implementation, plus the per-node [`EncryptionKey`] type and the
//! directory-level "loud refusal" check ([`verify_or_init_marker`]) that
//! catches a key/data-directory mismatch before any WAL or engine file is
//! opened for use.
//!
//! # Format
//!
//! Every file this wrapper writes starts with a 21-byte header —
//! `MAGIC(4) || VERSION(1) || SALT(16)` — followed by zero or more
//! self-delimited **frames**, one per [`Disk::append`] call (or exactly one
//! for a [`Disk::replace`], which always mints a fresh header): `LEN(4, big-
//! endian u32) || CIPHERTEXT_AND_TAG(LEN bytes)`. `LEN` is the length of the
//! XChaCha20-Poly1305 ciphertext plus its 16-byte authentication tag; the
//! plaintext frame length is `LEN - 16`.
//!
//! The nonce for frame `i` of a file is `SALT || i.to_be_bytes()` (16 + 8 =
//! 24 bytes, exactly XChaCha20-Poly1305's nonce size) — deterministic and
//! unique as long as `SALT` is fresh per file generation, which this module
//! guarantees: [`Disk::append`] mints a new random salt only when the file
//! does not exist yet (frame indices increment monotonically after that, so
//! `(salt, counter)` — and therefore the nonce — never repeats for the life
//! of that file generation), and [`Disk::replace`] *always* mints a brand
//! new salt (so a compaction/manifest rewrite never reuses the nonce space
//! of whatever generation of the file preceded it, even if the new content
//! happens to start again at counter 0).
//!
//! # Loud refusal
//!
//! [`verify_or_init_marker`] is the ADR 0069 counterpart of
//! `animus_cp_data::host::check_wal_layout`'s loud layout-mismatch refusal:
//! called once, before any WAL/engine file is opened, it lists the data
//! directory and reads at most one small marker file
//! ([`MARKER_FILE`]) to decide whether the directory is "fresh", "already
//! encrypted", or "already plaintext" — and refuses (a plain `io::Error`,
//! never a silent reset or partial start) on any of the two mismatched
//! combinations: a key given against an existing plaintext directory, or a
//! missing/wrong key against an existing encrypted one. See its own doc for
//! the exact decision table and error text.
//!
//! # Torn tails
//!
//! A crash mid-`append` can leave a file whose last frame is short (the
//! length prefix or ciphertext bytes never made it to disk) or fails AEAD
//! authentication (the tag was only partially written). Once
//! [`verify_or_init_marker`] has confirmed the configured key is the right
//! one for this directory, *any* frame-parse failure encountered while
//! scanning an ordinary file can only mean "this is where the last
//! unsynced write was cut off" — never "wrong key" (that would already have
//! been caught by the marker, whose own single frame is written durably via
//! [`Disk::replace`] and therefore can never itself be torn, see
//! [`Disk::replace`]'s crash contract). So this module treats the first
//! frame-parse failure encountered during a scan, whatever its specific
//! cause, uniformly as a torn tail: everything up to (not including) that
//! frame is the valid prefix, and [`EncryptedDisk`] physically truncates the
//! file back to it (via [`Disk::replace`]) the first time that file is
//! touched after a restart — a strict subset of the byte-level torn tails
//! the WAL/LSM layers already recover from, since a whole frame (not an
//! arbitrary byte range) is always discarded as a unit.
//!
//! # Concurrency
//!
//! [`EncryptedDisk`] follows the same "cache lock never held across an
//! `.await`" discipline [`ProdEnv`](crate::ProdEnv)'s own `dir_synced`/
//! `conns` maps already use — but does **not** itself serialize concurrent
//! `append`/`replace` calls to the *same* file name: two callers racing an
//! append to one file could compute the same next frame counter and reuse a
//! nonce. This is safe today because every real caller already serializes
//! writes to a given file by construction (WAL group-commit, the control
//! apply task, SSTable/manifest writers are each single-writer-per-file) —
//! the identical precondition the plaintext `Disk` implementations already
//! rely on for `append`'s own "buffered, appended in call order" contract.
//! A future caller that breaks this precondition needs its own
//! serialization, exactly as it would today without encryption.

use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;
use std::sync::Mutex as StdMutex;

use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{Key as AeadKey, XChaCha20Poly1305, XNonce};
use zeroize::Zeroizing;

use crate::{
    BoxFuture, Clock, Disk, Env, MetricsHandle, Nanos, Network, NodeId, Rng, Spawner, UnixMillis,
};

const MAGIC: [u8; 4] = *b"ADE1";
const VERSION: u8 = 1;
const SALT_LEN: usize = 16;
const HEADER_LEN: usize = 4 + 1 + SALT_LEN; // 21
const TAG_LEN: usize = 16;
const NONCE_LEN: usize = 24;
const LEN_PREFIX: usize = 4;

/// The marker file name recording whether a data directory is encrypted
/// (ADR 0069). Filtered out of [`EncryptedDisk::list`] so it never appears
/// to a higher layer that enumerates its own kind-prefixed files.
pub const MARKER_FILE: &str = ".animus_encryption_marker";
const MARKER_PLAINTEXT: &[u8] = b"ANIMUSDB-ENCRYPTION-MARKER-V1";

/// A 256-bit AEAD key loaded from a per-node key file (`--encryption-key
/// PATH`, ADR 0069). Never printed in full (`Debug` redacts it); the byte
/// buffer is [`Zeroizing`], so it is scrubbed from memory on drop.
#[derive(Clone)]
pub struct EncryptionKey(Zeroizing<[u8; 32]>);

impl EncryptionKey {
    /// Wrap a raw 256-bit key already in memory.
    #[must_use]
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        EncryptionKey(Zeroizing::new(bytes))
    }

    /// Load a key from `path`: 64 hex characters (256 bits), optionally
    /// followed by a single trailing newline — the same "one secret, one
    /// file" shape `--dynamo-auth PATH`/`--tls-key PATH` use, chosen over
    /// PEM/JSON because a raw symmetric key has no structure to carry.
    /// Generate one with e.g. `openssl rand -hex 32 > key.hex`.
    ///
    /// Real (blocking) file I/O — like `load_dynamo_auth_file`, this runs
    /// once at CLI startup before any `Env` exists, not on a hot path
    /// through the `Disk` seam.
    pub fn load_from_file(path: &Path) -> std::io::Result<Self> {
        let text = std::fs::read_to_string(path).map_err(|e| {
            std::io::Error::new(e.kind(), format!("reading {}: {e}", path.display()))
        })?;
        let hex = text.trim();
        if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "encryption key file {} must contain exactly 64 hex characters (256 bits); \
                     generate one with `openssl rand -hex 32 > {}`",
                    path.display(),
                    path.display()
                ),
            ));
        }
        let mut bytes = [0u8; 32];
        for (i, b) in bytes.iter_mut().enumerate() {
            // Already validated as ASCII hex above, so this cannot fail.
            *b = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).expect("validated hex digits");
        }
        Ok(EncryptionKey::from_bytes(bytes))
    }

    fn cipher(&self) -> XChaCha20Poly1305 {
        XChaCha20Poly1305::new(AeadKey::from_slice(self.0.as_slice()))
    }
}

impl fmt::Debug for EncryptionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("EncryptionKey(REDACTED)")
    }
}

fn derive_nonce(salt: &[u8; SALT_LEN], counter: u64) -> [u8; NONCE_LEN] {
    let mut n = [0u8; NONCE_LEN];
    n[..SALT_LEN].copy_from_slice(salt);
    n[SALT_LEN..].copy_from_slice(&counter.to_be_bytes());
    n
}

fn random_salt<R: Rng + ?Sized>(rng: &R) -> [u8; SALT_LEN] {
    let mut s = [0u8; SALT_LEN];
    rng.fill_bytes(&mut s);
    s
}

fn make_header(salt: &[u8; SALT_LEN]) -> Vec<u8> {
    let mut h = Vec::with_capacity(HEADER_LEN);
    h.extend_from_slice(&MAGIC);
    h.push(VERSION);
    h.extend_from_slice(salt);
    h
}

/// Seal `plaintext` as frame `counter` of the file salted with `salt`:
/// `LEN(4) || ciphertext || tag`.
fn seal_frame(
    key: &EncryptionKey,
    salt: &[u8; SALT_LEN],
    counter: u64,
    plaintext: &[u8],
) -> Vec<u8> {
    let nonce = derive_nonce(salt, counter);
    let ct = key
        .cipher()
        .encrypt(XNonce::from_slice(&nonce), plaintext)
        .expect("XChaCha20-Poly1305 encryption only fails on a plaintext exceeding ~256 GiB");
    let mut framed = Vec::with_capacity(LEN_PREFIX + ct.len());
    framed.extend_from_slice(
        &u32::try_from(ct.len())
            .expect("a single Disk::append/replace call never carries >4 GiB")
            .to_be_bytes(),
    );
    framed.extend_from_slice(&ct);
    framed
}

/// Open ciphertext (tag included, no length prefix) for frame `counter`.
/// `None` on authentication failure — see the module doc's "Torn tails"
/// section for how callers interpret that once the marker has already
/// validated the key.
fn open_frame(
    key: &EncryptionKey,
    salt: &[u8; SALT_LEN],
    counter: u64,
    ciphertext: &[u8],
) -> Option<Vec<u8>> {
    let nonce = derive_nonce(salt, counter);
    key.cipher()
        .decrypt(XNonce::from_slice(&nonce), ciphertext)
        .ok()
}

#[derive(Clone)]
struct FrameEntry {
    /// Byte offset, in the raw (encrypted) file, of this frame's length
    /// prefix.
    raw_offset: u64,
    /// Total raw bytes this frame occupies, length prefix included.
    raw_len: u32,
    plain_offset: u64,
    plain_len: u32,
}

#[derive(Clone)]
struct FileIndex {
    salt: [u8; SALT_LEN],
    frames: Vec<FrameEntry>,
    plain_len: u64,
    /// Raw bytes on disk this index actually accounts for (header + every
    /// frame). `0` means "no bytes physically written yet for this
    /// generation of the file" (a brand-new salt minted for a file that
    /// does not exist yet); `Disk::append`'s next physical write prepends
    /// the header in that case.
    raw_len: u64,
}

enum Scan {
    /// The file does not exist (or is empty) — no header to speak of.
    Absent,
    /// Bytes are present but don't start with the expected magic — either a
    /// plaintext file (directory-level mismatch) or a corrupted header.
    NotEncrypted,
    /// A parseable (possibly torn) encrypted file: `index` covers every
    /// frame up to (not including) the first parse/auth failure, if any;
    /// `torn` says whether such a failure was found — safe to recover from
    /// by truncating to `index` (see the module doc's "Torn tails"
    /// section).
    Ok { index: FileIndex, torn: bool },
    /// A frame failed to parse/authenticate, and a **later** frame in the
    /// same raw file went on to authenticate successfully. A genuine
    /// crash-torn write, by definition, cuts off at the true end of the
    /// file — nothing valid can follow the tear. A failure with valid data
    /// *after* it can therefore only be real corruption of already-durable
    /// bytes (e.g. bit rot, or `Simulator::corrupt_durable` in a test), and
    /// recovery must refuse loudly rather than silently discard whatever
    /// came after the corrupted frame.
    Corrupted,
}

fn scan(key: &EncryptionKey, raw: &[u8]) -> Scan {
    if raw.is_empty() {
        return Scan::Absent;
    }
    if raw.len() < HEADER_LEN || raw[0..4] != MAGIC {
        return Scan::NotEncrypted;
    }
    let mut salt = [0u8; SALT_LEN];
    salt.copy_from_slice(&raw[5..HEADER_LEN]);

    // Only frames collected before the first failure are trustworthy; scanning
    // continues past a failure purely to tell a trailing tear from real
    // mid-file corruption (see `Scan::Corrupted`'s doc), never adding
    // anything more to `trusted`.
    let mut trusted = Vec::new();
    let mut trusted_plain_len: u64 = 0;
    let mut trusted_raw_len: u64 = HEADER_LEN as u64;

    let mut pos: usize = HEADER_LEN;
    let mut counter: u64 = 0;
    let mut failed = false;

    loop {
        if pos == raw.len() {
            break;
        }
        if pos + LEN_PREFIX > raw.len() {
            failed = true;
            break;
        }
        let ct_len =
            u32::from_be_bytes(raw[pos..pos + LEN_PREFIX].try_into().expect("4 bytes")) as usize;
        let body_start = pos + LEN_PREFIX;
        let body_end = match body_start.checked_add(ct_len) {
            Some(v) => v,
            None => {
                failed = true;
                break;
            }
        };
        if ct_len < TAG_LEN || body_end > raw.len() {
            failed = true;
            break;
        }
        match open_frame(key, &salt, counter, &raw[body_start..body_end]) {
            Some(plaintext) => {
                if failed {
                    // Authenticated *after* an earlier failure: that
                    // earlier failure cannot have been a trailing tear.
                    return Scan::Corrupted;
                }
                trusted.push(FrameEntry {
                    raw_offset: pos as u64,
                    raw_len: (LEN_PREFIX + ct_len) as u32,
                    plain_offset: trusted_plain_len,
                    plain_len: plaintext.len() as u32,
                });
                trusted_plain_len += plaintext.len() as u64;
                trusted_raw_len = body_end as u64;
                counter += 1;
                pos = body_end;
            }
            None => {
                // Keep exploring past this frame using its own (still
                // self-consistent) length field, purely to classify the
                // failure; `trusted` is not touched again.
                failed = true;
                counter += 1;
                pos = body_end;
            }
        }
    }

    Scan::Ok {
        index: FileIndex {
            salt,
            frames: trusted,
            plain_len: trusted_plain_len,
            raw_len: trusted_raw_len,
        },
        torn: failed,
    }
}

fn mixed_state_error(file: &str) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!(
            "{file} is not in the expected encrypted frame format even though this data \
             directory's {MARKER_FILE} says it is encrypted — the data directory is corrupted \
             or was partially migrated by hand; refusing to use it"
        ),
    )
}

fn corruption_error(file: &str) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!(
            "{file}: an encrypted frame failed to decrypt where a valid one was expected (corruption)"
        ),
    )
}

/// Seal `plaintext` as a whole, standalone object: a fresh header (random
/// salt) plus exactly one frame at counter 0 — the identical shape
/// [`Disk::replace`]'s own physical payload takes. Used both by the
/// marker file/object (below and in `encrypted_segment_store.rs`) and by
/// [`crate::EncryptedSegmentStore::put`] (ADR 0069 PR 2), which seals each
/// write-once object this same way: no frame index is needed since a
/// `SegmentStore` object, unlike a `Disk` file, is always read/written
/// whole, never incrementally appended.
pub(crate) fn seal_whole<R: Rng + ?Sized>(
    key: &EncryptionKey,
    rng: &R,
    plaintext: &[u8],
) -> Vec<u8> {
    let salt = random_salt(rng);
    let framed = seal_frame(key, &salt, 0, plaintext);
    let mut out = make_header(&salt);
    out.extend_from_slice(&framed);
    out
}

/// Open a whole standalone object sealed by [`seal_whole`]. `None` on any
/// parse/authentication failure — the caller decides what that means (a
/// wrong key, corruption, or genuinely not-our-format bytes). Used both by
/// the marker file/object's own authentication check and by
/// [`crate::EncryptedSegmentStore::get`], which — unlike
/// [`EncryptedDisk`]'s per-file frame scan — has no torn-tail case to
/// consider at all: a `SegmentStore` object is never incrementally
/// appended, so any failure here is either a key/store mismatch or
/// genuine corruption, never a legitimate crash-torn write in progress.
pub(crate) fn open_whole(key: &EncryptionKey, raw: &[u8]) -> Option<Vec<u8>> {
    if raw.len() <= HEADER_LEN + LEN_PREFIX || raw[0..4] != MAGIC {
        return None;
    }
    let mut salt = [0u8; SALT_LEN];
    salt.copy_from_slice(&raw[5..HEADER_LEN]);
    let ct_len =
        u32::from_be_bytes(raw[HEADER_LEN..HEADER_LEN + LEN_PREFIX].try_into().ok()?) as usize;
    let body = &raw[HEADER_LEN + LEN_PREFIX..];
    if body.len() != ct_len {
        return None;
    }
    open_frame(key, &salt, 0, body)
}

fn seal_marker<R: Rng + ?Sized>(key: &EncryptionKey, rng: &R) -> Vec<u8> {
    seal_whole(key, rng, MARKER_PLAINTEXT)
}

/// A marker is written once via [`Disk::replace`], whose crash contract
/// ("a crash before or after sees the whole old or whole new contents,
/// never a mix") guarantees it is never torn — so unlike an ordinary data
/// file, *any* parse/authentication failure here means "wrong key or
/// corrupted marker", never "torn tail".
fn marker_authenticates(key: &EncryptionKey, raw: &[u8]) -> bool {
    matches!(open_whole(key, raw), Some(pt) if pt == MARKER_PLAINTEXT)
}

/// The ADR 0069 loud-refusal check: decide (from a directory listing plus
/// at most one marker read) whether `disk`'s data directory and `key` are
/// compatible, initializing the marker on a fresh directory. Call this
/// **before** constructing an [`EncryptedDisk`]/[`EncryptedEnv`] and before
/// any WAL/engine file in the same directory is opened for use.
///
/// | directory has [`MARKER_FILE`] | `key` given | outcome |
/// |---|---|---|
/// | yes | no | **refuse**: encrypted directory, no key |
/// | yes | yes, authenticates | proceed encrypted |
/// | yes | yes, does not authenticate | **refuse**: wrong key |
/// | no, but has other files | yes | **refuse**: key against a plaintext directory |
/// | no, but has other files | no | proceed plaintext (today's behavior, untouched) |
/// | no other files (fresh/empty) | yes | initialize the marker, proceed encrypted |
/// | no other files (fresh/empty) | no | proceed plaintext (today's behavior, untouched) |
pub async fn verify_or_init_marker<D: Disk + ?Sized, R: Rng + ?Sized>(
    disk: &D,
    rng: &R,
    key: Option<&EncryptionKey>,
) -> std::io::Result<()> {
    let names = disk.list().await?;
    let marker_present = names.iter().any(|n| n == MARKER_FILE);
    let has_other_files = names.iter().any(|n| n != MARKER_FILE);
    match (marker_present, key) {
        (true, None) => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "data directory is encrypted (found {MARKER_FILE}) but no --encryption-key was \
                 given — refusing to start. Pass --encryption-key PATH with the same key this \
                 directory was created with, or point at a fresh data directory."
            ),
        )),
        (true, Some(k)) => {
            let raw = disk.read(MARKER_FILE).await?;
            if marker_authenticates(k, &raw) {
                Ok(())
            } else {
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "--encryption-key does not match the key this data directory was encrypted \
                     with — refusing to start. Use the original key, or point at a fresh data \
                     directory."
                        .to_string(),
                ))
            }
        }
        (false, Some(_)) if has_other_files => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "--encryption-key was given but this data directory already holds unencrypted \
                 files (no {MARKER_FILE} marker) — refusing to start. Encryption cannot be \
                 enabled on an existing plaintext data directory; point at a fresh data \
                 directory instead."
            ),
        )),
        (false, Some(k)) => {
            let marker = seal_marker(k, rng);
            disk.replace(MARKER_FILE, &marker).await
        }
        (false, None) => Ok(()),
    }
}

struct EncryptedDiskState<D, R> {
    inner: D,
    rng: R,
    key: EncryptionKey,
    cache: StdMutex<BTreeMap<String, FileIndex>>,
}

/// An AEAD-encrypting [`Disk`] wrapper, generic over any underlying `D:
/// Disk` (so `SimEnv`'s crash/fault corpora can drive it deterministically)
/// and any `R: Rng` source for minting per-file/per-`replace` salts. See
/// the module doc for the frame format, loud-refusal check, and torn-tail
/// recovery. Cheap to clone (an `Arc`-backed handle, like
/// [`ProdEnv`](crate::ProdEnv)/`SimEnv` themselves) — clones share the same
/// in-memory frame-index cache.
///
/// Construct only after [`verify_or_init_marker`] has confirmed `key` is
/// right for `inner`'s data directory — this type itself performs no
/// marker/loud-refusal check, only per-file frame bookkeeping.
pub struct EncryptedDisk<D, R> {
    shared: std::sync::Arc<EncryptedDiskState<D, R>>,
}

impl<D, R> Clone for EncryptedDisk<D, R> {
    fn clone(&self) -> Self {
        EncryptedDisk {
            shared: std::sync::Arc::clone(&self.shared),
        }
    }
}

impl<D: Disk, R: Rng> EncryptedDisk<D, R> {
    /// Wrap `inner`, sealing every write under `key` and drawing per-file
    /// salts from `rng`.
    pub fn new(inner: D, rng: R, key: EncryptionKey) -> Self {
        EncryptedDisk {
            shared: std::sync::Arc::new(EncryptedDiskState {
                inner,
                rng,
                key,
                cache: StdMutex::new(BTreeMap::new()),
            }),
        }
    }

    /// The wrapped disk.
    pub fn inner(&self) -> &D {
        &self.shared.inner
    }

    async fn get_index(&self, file: &str) -> std::io::Result<FileIndex> {
        if let Some(idx) = self
            .shared
            .cache
            .lock()
            .expect("encrypted disk cache poisoned")
            .get(file)
            .cloned()
        {
            return Ok(idx);
        }
        let idx = self.load_or_init_index(file).await?;
        self.shared
            .cache
            .lock()
            .expect("encrypted disk cache poisoned")
            .insert(file.to_string(), idx.clone());
        Ok(idx)
    }

    fn put_index(&self, file: &str, idx: FileIndex) {
        self.shared
            .cache
            .lock()
            .expect("encrypted disk cache poisoned")
            .insert(file.to_string(), idx);
    }

    /// Build a file's index from scratch: reads it once, and — if scanning
    /// finds a torn or trailing-garbage tail — physically truncates the
    /// file back to the last complete frame (see the module doc's "Torn
    /// tails" section) so a subsequent `append` never strands that garbage
    /// mid-file.
    async fn load_or_init_index(&self, file: &str) -> std::io::Result<FileIndex> {
        let raw = self.shared.inner.read(file).await?;
        match scan(&self.shared.key, &raw) {
            Scan::Absent => Ok(FileIndex {
                salt: random_salt(&self.shared.rng),
                frames: Vec::new(),
                plain_len: 0,
                raw_len: 0,
            }),
            Scan::NotEncrypted => Err(mixed_state_error(file)),
            Scan::Corrupted => Err(corruption_error(file)),
            Scan::Ok { index, torn } => {
                if torn || (index.raw_len as usize) < raw.len() {
                    let valid = &raw[..index.raw_len as usize];
                    self.shared.inner.replace(file, valid).await?;
                }
                Ok(index)
            }
        }
    }
}

#[async_trait::async_trait]
impl<D: Disk, R: Rng> Disk for EncryptedDisk<D, R> {
    async fn append(&self, file: &str, bytes: &[u8]) -> std::io::Result<()> {
        let idx = self.get_index(file).await?;
        let counter = idx.frames.len() as u64;
        let framed = seal_frame(&self.shared.key, &idx.salt, counter, bytes);
        let mut physical = Vec::new();
        let frame_raw_offset = if idx.raw_len == 0 {
            physical.extend_from_slice(&make_header(&idx.salt));
            HEADER_LEN as u64
        } else {
            idx.raw_len
        };
        physical.extend_from_slice(&framed);
        self.shared.inner.append(file, &physical).await?;

        let mut idx = idx;
        idx.frames.push(FrameEntry {
            raw_offset: frame_raw_offset,
            raw_len: framed.len() as u32,
            plain_offset: idx.plain_len,
            plain_len: bytes.len() as u32,
        });
        idx.plain_len += bytes.len() as u64;
        idx.raw_len = frame_raw_offset + framed.len() as u64;
        self.put_index(file, idx);
        Ok(())
    }

    async fn sync(&self, file: &str) -> std::io::Result<()> {
        self.shared.inner.sync(file).await
    }

    async fn read(&self, file: &str) -> std::io::Result<Vec<u8>> {
        let idx = self.get_index(file).await?;
        let mut out = Vec::with_capacity(idx.plain_len as usize);
        for (counter, f) in idx.frames.iter().enumerate() {
            let raw = self
                .shared
                .inner
                .read_at(file, f.raw_offset, f.raw_len as usize)
                .await?;
            if raw.len() != f.raw_len as usize || raw.len() < LEN_PREFIX {
                return Err(corruption_error(file));
            }
            let ct = &raw[LEN_PREFIX..];
            let pt = open_frame(&self.shared.key, &idx.salt, counter as u64, ct)
                .ok_or_else(|| corruption_error(file))?;
            out.extend_from_slice(&pt);
        }
        Ok(out)
    }

    async fn read_at(&self, file: &str, offset: u64, len: usize) -> std::io::Result<Vec<u8>> {
        let idx = self.get_index(file).await?;
        if offset >= idx.plain_len || len == 0 {
            return Ok(Vec::new());
        }
        let end = offset.saturating_add(len as u64).min(idx.plain_len);
        let mut out = Vec::with_capacity((end - offset) as usize);
        for (counter, f) in idx.frames.iter().enumerate() {
            let f_start = f.plain_offset;
            let f_end = f.plain_offset + u64::from(f.plain_len);
            if f_end <= offset || f_start >= end {
                continue;
            }
            let raw = self
                .shared
                .inner
                .read_at(file, f.raw_offset, f.raw_len as usize)
                .await?;
            if raw.len() != f.raw_len as usize || raw.len() < LEN_PREFIX {
                return Err(corruption_error(file));
            }
            let ct = &raw[LEN_PREFIX..];
            let pt = open_frame(&self.shared.key, &idx.salt, counter as u64, ct)
                .ok_or_else(|| corruption_error(file))?;
            let lo = offset.saturating_sub(f_start) as usize;
            let hi = (end.min(f_end) - f_start) as usize;
            out.extend_from_slice(&pt[lo..hi]);
        }
        Ok(out)
    }

    async fn size(&self, file: &str) -> std::io::Result<u64> {
        Ok(self.get_index(file).await?.plain_len)
    }

    async fn remove(&self, file: &str) -> std::io::Result<()> {
        self.shared.inner.remove(file).await?;
        self.shared
            .cache
            .lock()
            .expect("encrypted disk cache poisoned")
            .remove(file);
        Ok(())
    }

    async fn replace(&self, file: &str, bytes: &[u8]) -> std::io::Result<()> {
        let salt = random_salt(&self.shared.rng);
        let framed = seal_frame(&self.shared.key, &salt, 0, bytes);
        let mut physical = make_header(&salt);
        physical.extend_from_slice(&framed);
        self.shared.inner.replace(file, &physical).await?;
        self.put_index(
            file,
            FileIndex {
                salt,
                frames: vec![FrameEntry {
                    raw_offset: HEADER_LEN as u64,
                    raw_len: framed.len() as u32,
                    plain_offset: 0,
                    plain_len: bytes.len() as u32,
                }],
                plain_len: bytes.len() as u64,
                raw_len: (HEADER_LEN + framed.len()) as u64,
            },
        );
        Ok(())
    }

    async fn list(&self) -> std::io::Result<Vec<String>> {
        let mut names = self.shared.inner.list().await?;
        names.retain(|n| n != MARKER_FILE);
        Ok(names)
    }

    async fn link(&self, src: &str, dst: &str) -> std::io::Result<()> {
        self.shared.inner.link(src, dst).await?;
        // `dst` now shares `src`'s exact encrypted bytes (a real hard link,
        // no re-encryption) — mirror the cached index if we have one so a
        // caller that immediately reads `dst` doesn't pay a rescan; an
        // absent cache entry is rebuilt lazily on first access either way.
        let mut cache = self
            .shared
            .cache
            .lock()
            .expect("encrypted disk cache poisoned");
        match cache.get(src).cloned() {
            Some(idx) => {
                cache.insert(dst.to_string(), idx);
            }
            None => {
                cache.remove(dst);
            }
        }
        Ok(())
    }
}

/// A drop-in [`Env`] wrapper (ADR 0069): every method except the `Disk`
/// seam delegates unchanged to the wrapped `env`; `Disk` methods route
/// through an [`EncryptedDisk`] sealing every write under `key`. This is
/// what lets `LsmEngine<EncryptedEnv<SimEnv>>` reuse the crash/fault
/// corpora already written for `LsmEngine<E>` — see
/// `animus-storage/tests/lsm_crash.rs` and `tests/lsm_disk_faults.rs`.
///
/// `ProdEnv` does **not** use this wrapper directly (see ADR 0069's
/// "Wiring into `animusd`" section for why) — it composes the same
/// [`EncryptedDisk`]/[`verify_or_init_marker`] machinery internally instead,
/// behind its own `Disk` impl, so every existing concrete `ProdEnv` call
/// site in `animusd` keeps compiling unchanged.
#[derive(Clone)]
pub struct EncryptedEnv<E: Env> {
    env: E,
    disk: EncryptedDisk<E, E>,
}

impl<E: Env> EncryptedEnv<E> {
    /// Wrap `env`, verifying/initializing the directory-level marker first
    /// (see [`verify_or_init_marker`]) — loudly refuses a key/directory
    /// mismatch before returning.
    pub async fn open(env: E, key: EncryptionKey) -> std::io::Result<Self> {
        verify_or_init_marker(&env, &env, Some(&key)).await?;
        let disk = EncryptedDisk::new(env.clone(), env.clone(), key);
        Ok(EncryptedEnv { env, disk })
    }

    /// The wrapped env (e.g. to reach `SimEnv`-only test helpers).
    pub fn inner(&self) -> &E {
        &self.env
    }
}

#[async_trait::async_trait]
impl<E: Env> Clock for EncryptedEnv<E> {
    fn now(&self) -> Nanos {
        self.env.now()
    }

    fn wall_now(&self) -> UnixMillis {
        self.env.wall_now()
    }

    async fn sleep(&self, dur: std::time::Duration) {
        self.env.sleep(dur).await;
    }
}

impl<E: Env> Rng for EncryptedEnv<E> {
    fn next_u64(&self) -> u64 {
        self.env.next_u64()
    }

    fn fill_bytes(&self, dst: &mut [u8]) {
        self.env.fill_bytes(dst);
    }
}

#[async_trait::async_trait]
impl<E: Env> Network for EncryptedEnv<E> {
    async fn send_stream(&self, to: NodeId, stream: u64, payload: Vec<u8>) {
        self.env.send_stream(to, stream, payload).await;
    }

    async fn recv_stream(&self, stream: u64) -> crate::Envelope {
        self.env.recv_stream(stream).await
    }
}

#[async_trait::async_trait]
impl<E: Env> Disk for EncryptedEnv<E> {
    async fn append(&self, file: &str, bytes: &[u8]) -> std::io::Result<()> {
        self.disk.append(file, bytes).await
    }

    async fn sync(&self, file: &str) -> std::io::Result<()> {
        self.disk.sync(file).await
    }

    async fn read(&self, file: &str) -> std::io::Result<Vec<u8>> {
        self.disk.read(file).await
    }

    async fn read_at(&self, file: &str, offset: u64, len: usize) -> std::io::Result<Vec<u8>> {
        self.disk.read_at(file, offset, len).await
    }

    async fn size(&self, file: &str) -> std::io::Result<u64> {
        self.disk.size(file).await
    }

    async fn remove(&self, file: &str) -> std::io::Result<()> {
        self.disk.remove(file).await
    }

    async fn replace(&self, file: &str, bytes: &[u8]) -> std::io::Result<()> {
        self.disk.replace(file, bytes).await
    }

    async fn list(&self) -> std::io::Result<Vec<String>> {
        self.disk.list().await
    }

    async fn link(&self, src: &str, dst: &str) -> std::io::Result<()> {
        self.disk.link(src, dst).await
    }
}

impl<E: Env> Spawner for EncryptedEnv<E> {
    fn spawn(&self, fut: BoxFuture<'static, ()>) {
        self.env.spawn(fut);
    }
}

impl<E: Env> Env for EncryptedEnv<E> {
    fn node_id(&self) -> NodeId {
        self.env.node_id()
    }

    fn metrics(&self) -> MetricsHandle {
        self.env.metrics()
    }
}
