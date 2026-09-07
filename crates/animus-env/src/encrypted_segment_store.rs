//! Encryption at rest for `SegmentStore` objects (ADR 0069, S-03 PR 2):
//! [`EncryptedSegmentStore<S: SegmentStore, R: Rng>`] seals each object as
//! a whole standalone frame on [`SegmentStore::put`] (reusing
//! `encrypted.rs`'s own frame codec — one header, one frame, the identical
//! shape [`Disk::replace`](crate::Disk::replace)'s own atomic-swap payload
//! and the [`crate::MARKER_FILE`] marker already take, since a
//! `SegmentStore` object, unlike a `Disk` file, is never incrementally
//! appended — see the trait's own "write-once" contract), and opens it
//! whole on [`SegmentStore::get`]. Object ids stay plaintext — they are the
//! catalog's own keys (a backup id, a stream shard id), never sealed
//! themselves; only the bytes stored *at* an id are.
//!
//! # Key scope (ADR 0069 amendment)
//!
//! Unlike [`EncryptedDisk`](crate::EncryptedDisk), whose files a single
//! node's own process is always the sole reader and writer of, a
//! `SegmentStore` object is routinely read by a **different** node than
//! the one that wrote it: a backup captured on node A is restored on node
//! B; a PITR segment sealed by one tablet's leader is later replayed by
//! whichever node drives the restore; a DynamoDB Streams shard object is
//! read by whichever node happens to serve `GetRecords`. A strictly
//! per-node key (this ADR's PR 1 design for `Disk`) cannot work here: node
//! B would need node A's key just to read what A wrote, which defeats the
//! whole "per-node local secret" model PR 1 established. This module's
//! contract is therefore **cluster-wide key scope**: every node encrypting
//! or reading a given `fs:`/`s3://` `SegmentStore` must be configured with
//! the *same* key file — see `docs/adr/0069-encryption-at-rest.md`'s PR 2
//! amendment for the full decision record (the rejected alternative, a
//! separate `--segment-store-key`/`--backup-store-key` flag, and what a
//! mixed cluster looks like).
//!
//! # Loud refusal
//!
//! [`verify_or_init_segment_store_marker`] mirrors
//! [`crate::verify_or_init_marker`]'s three-way decision table exactly,
//! adapted to `SegmentStore`'s `put`/`get`/`list` shape instead of `Disk`'s
//! `list`/`read`/`replace`: a small marker **object**
//! ([`SEGMENT_STORE_MARKER_ID`]) records whether a store's existing
//! content is encrypted, checked once before this wrapper is constructed
//! — and, even when no key is configured at all, before an unwrapped
//! store is ever handed to a caller, so an already-encrypted store is
//! never silently treated as plaintext. It is filtered out of every
//! [`EncryptedSegmentStore::list`] result this wrapper serves, exactly
//! like [`EncryptedDisk::list`](crate::EncryptedDisk::list) hides
//! [`crate::MARKER_FILE`].
//!
//! # Authentication failure is a hard error, never a torn-tail trim
//!
//! A `SegmentStore` object is always written whole, in one `put` call (the
//! trait's own write-once contract) — there is no partial-write window
//! analogous to `Disk::append`'s incremental frames, so there is no
//! legitimate "torn tail" case for [`EncryptedSegmentStore::get`] to
//! recover from. Any failure to authenticate an existing object — the
//! wrong key, bit rot, or a plaintext object surviving from before
//! encryption was turned on — is therefore always a hard [`io::Error`]
//! naming the object id, never a silent truncation or a bare `None`.
//!
//! # Write-once at the plaintext level
//!
//! [`SegmentStore::put`]'s own contract treats an identical-content re-put
//! of an existing id as a safe no-op and a differing-content re-put as a
//! hard error. Because every `put` mints a **fresh** random salt (the
//! identical nonce-uniqueness discipline `Disk::replace` uses, and for the
//! identical reason — see `encrypted.rs`'s own "Nonce scheme" doc), two
//! `put` calls with byte-identical *plaintext* produce different
//! *ciphertext*. Comparing raw (encrypted) bytes — what the wrapped
//! store's own `put` does internally to enforce this contract for an
//! unencrypted caller — would therefore wrongly reject a legitimate
//! idempotent re-put. This wrapper instead fetches and decrypts whatever
//! is already at `id` first and compares **plaintext**, mirroring exactly
//! what `FsSegmentStore`/`S3SegmentStore` themselves already do one layer
//! down for an unencrypted store.

use std::io;
use std::sync::Arc;

use crate::encrypted::{EncryptionKey, open_whole, seal_whole};
use crate::{Rng, SegmentStore};

/// The marker object id recording whether a [`SegmentStore`]'s existing
/// content is encrypted (ADR 0069 PR 2) — the `SegmentStore` analogue of
/// [`crate::MARKER_FILE`]. Filtered out of every
/// [`EncryptedSegmentStore::list`] result.
pub const SEGMENT_STORE_MARKER_ID: &str = ".animus_segment_store_encryption_marker";
const SEGMENT_STORE_MARKER_PLAINTEXT: &[u8] = b"ANIMUSDB-SEGMENT-STORE-ENCRYPTION-MARKER-V1";

/// The ADR 0069 loud-refusal check for a [`SegmentStore`] — the exact
/// counterpart of [`crate::verify_or_init_marker`]; see that function's
/// own decision table (identical here, `Disk::list`/`read`/`replace`
/// swapped for `SegmentStore::list`/`get`/`put`).
///
/// | store has the marker | `key` given | outcome |
/// |---|---|---|
/// | yes | no | **refuse**: encrypted store, no key |
/// | yes | yes, authenticates | proceed encrypted |
/// | yes | yes, does not authenticate | **refuse**: wrong key |
/// | no, but other objects exist | yes | **refuse**: key against a plaintext store |
/// | no, but other objects exist | no | proceed plaintext (today's behavior, untouched) |
/// | no other objects (fresh) | yes | initialize the marker, proceed encrypted |
/// | no other objects (fresh) | no | proceed plaintext (today's behavior, untouched) |
///
/// Call this **before** constructing an [`EncryptedSegmentStore`] over
/// `store`, and — even when `key` is `None` — before handing an unwrapped
/// `store` to any caller, so a store that already holds encrypted objects
/// (a key was configured once, then omitted by mistake) is never silently
/// read/written as plaintext.
pub async fn verify_or_init_segment_store_marker<S: SegmentStore + ?Sized, R: Rng + ?Sized>(
    store: &S,
    rng: &R,
    key: Option<&EncryptionKey>,
) -> io::Result<()> {
    let marker = store.get(SEGMENT_STORE_MARKER_ID).await?;
    match (marker, key) {
        (Some(_), None) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "segment store is encrypted (found {SEGMENT_STORE_MARKER_ID}) but no \
                 --encryption-key was given — refusing to start. Pass --encryption-key PATH \
                 with the same key this store was created with, or point at a fresh store."
            ),
        )),
        (Some(raw), Some(k)) => {
            if matches!(open_whole(k, &raw), Some(pt) if pt == SEGMENT_STORE_MARKER_PLAINTEXT) {
                Ok(())
            } else {
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "--encryption-key does not match the key this segment store was encrypted \
                     with — refusing to start. Use the original key, or point at a fresh store."
                        .to_string(),
                ))
            }
        }
        (None, Some(k)) => {
            let others = store.list("").await?;
            if others.is_empty() {
                let marker = seal_whole(k, rng, SEGMENT_STORE_MARKER_PLAINTEXT);
                store.put(SEGMENT_STORE_MARKER_ID, &marker).await
            } else {
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "--encryption-key was given but this segment store already holds \
                         unencrypted objects (no {SEGMENT_STORE_MARKER_ID} marker) — refusing \
                         to start. Encryption cannot be enabled on an existing plaintext \
                         segment store; point at a fresh store instead."
                    ),
                ))
            }
        }
        (None, None) => Ok(()),
    }
}

/// [`EncryptedSegmentStore::get`]/`put`'s authentication-failure error:
/// always a hard error naming `id`, never a silent trim — see the module
/// doc's "Authentication failure" section.
fn auth_failure(id: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "{id}: failed to authenticate/decrypt this segment store object — either the \
             configured --encryption-key does not match the key this object was written with, \
             or the object is corrupted. Segment store objects are write-once, so this can \
             never be a torn write; refusing rather than risking silent data loss."
        ),
    )
}

/// [`EncryptedSegmentStore::put`]'s write-once violation — mirrors
/// `FsSegmentStore`/`S3SegmentStore`'s own identical error, compared at the
/// plaintext level (see the module doc's "Write-once at the plaintext
/// level" section).
fn write_once_violation(id: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::AlreadyExists,
        format!(
            "encrypted segment store write-once violation: {id:?} already holds different \
             content (every attempt must write its own unique id)"
        ),
    )
}

struct State<S, R> {
    inner: S,
    rng: R,
    key: EncryptionKey,
}

/// An AEAD-sealing [`SegmentStore`] wrapper (ADR 0069 PR 2), generic over
/// any underlying `S: SegmentStore` and any `R: Rng` salt source — the
/// `SegmentStore` sibling of [`crate::EncryptedDisk`]. Cheap to clone (an
/// `Arc`-backed handle); every clone shares the same key/rng/inner store.
///
/// Construct only via [`EncryptedSegmentStore::open`], which runs
/// [`verify_or_init_segment_store_marker`] first — a wrong or missing key
/// against an already-encrypted store (or the reverse) is refused before
/// this wrapper is ever usable.
pub struct EncryptedSegmentStore<S, R> {
    shared: Arc<State<S, R>>,
}

impl<S, R> Clone for EncryptedSegmentStore<S, R> {
    fn clone(&self) -> Self {
        EncryptedSegmentStore {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<S: SegmentStore, R: Rng> EncryptedSegmentStore<S, R> {
    /// Wrap `inner`, sealing every write under `key` and drawing per-object
    /// salts from `rng`. Verifies/initializes the store-level marker first
    /// (see [`verify_or_init_segment_store_marker`]) — loudly refuses a
    /// key/store mismatch before returning.
    pub async fn open(inner: S, rng: R, key: EncryptionKey) -> io::Result<Self> {
        verify_or_init_segment_store_marker(&inner, &rng, Some(&key)).await?;
        Ok(EncryptedSegmentStore {
            shared: Arc::new(State { inner, rng, key }),
        })
    }

    /// The wrapped store.
    pub fn inner(&self) -> &S {
        &self.shared.inner
    }
}

#[async_trait::async_trait]
impl<S: SegmentStore, R: Rng> SegmentStore for EncryptedSegmentStore<S, R> {
    async fn put(&self, id: &str, bytes: &[u8]) -> io::Result<()> {
        if let Some(existing_raw) = self.shared.inner.get(id).await? {
            let existing_plain =
                open_whole(&self.shared.key, &existing_raw).ok_or_else(|| auth_failure(id))?;
            if existing_plain == bytes {
                return Ok(());
            }
            return Err(write_once_violation(id));
        }
        let sealed = seal_whole(&self.shared.key, &self.shared.rng, bytes);
        self.shared.inner.put(id, &sealed).await
    }

    async fn get(&self, id: &str) -> io::Result<Option<Vec<u8>>> {
        match self.shared.inner.get(id).await? {
            None => Ok(None),
            Some(raw) => match open_whole(&self.shared.key, &raw) {
                Some(pt) => Ok(Some(pt)),
                None => Err(auth_failure(id)),
            },
        }
    }

    async fn delete(&self, id: &str) -> io::Result<()> {
        self.shared.inner.delete(id).await
    }

    async fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        let mut names = self.shared.inner.list(prefix).await?;
        names.retain(|n| n != SEGMENT_STORE_MARKER_ID);
        Ok(names)
    }
}
