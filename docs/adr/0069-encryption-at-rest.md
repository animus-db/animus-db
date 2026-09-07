# ADR 0069 — Encryption at rest: AEAD `Disk`-seam wrapper, per-node key file

- **Status:** Accepted — implemented (S-03 PR 1 of 3: key loading + the
  `Disk`-seam wrapper for WAL/engine files. PR 2 of 3 — `SegmentStore` —
  also implemented, see the 2026-09-06 "As-built: PR 2" amendment below.
  PR 3 of 3 — operator key-secret mount — also implemented, see the
  2026-09-07 "As-built: PR 3" amendment below. **S-03 is complete.** The
  default `cluster` segment/backup store PR 2 left untouched — issue #680
  — is closed by the 2026-09-07 "As-built: cluster store" amendment below.)
- **Date:** 2026-09-06
- **Origin:** `docs/roadmap.md`'s S-03 ("Encryption at rest")
- **Depends on:** [ADR 0003](0003-deterministic-simulation.md) (the `Env`
  seam this wrapper composes with, and the no-unseeded-randomness
  discipline its salts must honor), [ADR 0008](0008-lsm-storage-engine.md)
  (`LsmEngine`'s WAL/SSTable format, the primary consumer),
  [ADR 0028](0028-shared-storage-single-command-split.md) (`host::
  check_wal_layout`'s loud layout-mismatch refusal — the precedent this
  ADR's own loud refusal follows), [ADR 0057](0057-sigv4-client-auth.md)/
  [0064](0064-tls-on-every-port.md) (the per-node secret-file config
  pattern — `--dynamo-auth PATH`/`--tls-cert PATH` — this ADR's
  `--encryption-key PATH` copies)

## Context

Every byte AnimusDB writes to disk today — the control-plane WAL and
snapshots, each hosted tablet's own per-group Raft WAL, `LsmEngine`'s WAL
segments and SSTables, backup manifests on a local `fs:` store — is
plaintext. An operator running on shared infrastructure, a compliance
requirement, or simple defense-in-depth against a stolen disk all want the
same thing: data unreadable without a key, with no change to any of the
distributed protocol above it.

`docs/roadmap.md` names two implementation shapes to choose between:

- **(A) Byte-level AEAD at the `Disk` seam** — a generic wrapper
  implementing `Disk` over any other `Disk`, so every file any crate writes
  through the seam is covered at once.
- **(B) Block-level in `animus-storage`'s LSM** — seal SSTable blocks and
  WAL records individually, inside `LsmEngine` itself.

## Decision: (A), byte-level AEAD at the `Disk` seam

### Why (A) over (B)

(B) covers only `LsmEngine`. The control-plane WAL/snapshots
(`animus-control`), the per-group and shared CP-data WALs
(`animus-cp-data`), and backup manifests on a local `fs:` `SegmentStore`
(`animus-env::FsSegmentStore`) are all separate file formats, each written
directly through the `Disk`/`SegmentStore` seams, none of them SSTables.
Encrypting only `LsmEngine` would leave a Raft WAL — which carries every
row a client ever wrote, verbatim, before it is ever compacted into an
SSTable — in plaintext on disk. Closing that gap under (B) means
reimplementing block/record-level sealing separately in
`animus-control`, `animus-cp-data`, and `animus-env`'s segment store: three
formats, three nonce schemes, three places to get torn-tail recovery
right. (A) writes that logic once, below every one of them, and treats
`LsmEngine`'s own WAL-record/SSTable-block framing as opaque plaintext it
never has to know about.

The `Disk` contract's `read_at(offset, len)`/`size` — genuinely the hard
part for (A), since AEAD needs framed units, not arbitrary byte
offsets — turn out tractable: see "Frame format" below.

### The contract's actual difficulty

`Disk`'s eight methods (`append`, `sync`, `read`, `read_at`, `size`,
`remove`, `replace`, `list`, plus `link`) give an encrypting wrapper two
real constraints to satisfy, neither of which `SegmentStore`-style
whole-object encryption would face:

1. **`append` is incremental and durability is per-call** — a caller
   `.await`s `append` returning before calling `sync`, and expects a crash
   between the two to lose exactly the unsynced tail, nothing more,
   nothing less. An AEAD scheme that encrypts a "file" as one ciphertext
   can't support this: every `append` would have to re-encrypt the whole
   file, moving an O(1) hot-path write to O(file size).
2. **`read_at` is a random-access primitive** — `LsmEngine`'s point reads
   fetch one SSTable block via `read_at`, specifically to avoid loading
   whole files. Whole-file decryption on every `read_at` call would defeat
   that.

Both are solved the same way: **seal each `append` call as its own frame**,
never re-encrypting anything already durable, and build a **frame index**
(plaintext-offset → raw-file-offset/length) so `read_at` can locate and
decrypt only the frames a request overlaps.

### Frame format

Every file this wrapper writes starts with a 21-byte header —
`MAGIC(4) = "ADE1" || VERSION(1) || SALT(16)` — followed by zero or more
frames, each `LEN(4, big-endian u32) || CIPHERTEXT_AND_TAG(LEN bytes)`.
One `Disk::append` call seals exactly one frame; one `Disk::replace` call
mints a fresh header (fresh salt) and seals exactly one frame holding the
whole new content.

The **frame index** — built lazily by one scan of a file's raw bytes at
first access per process, and updated incrementally on every subsequent
`append`/`replace`/`remove`/`link` — maps plaintext byte ranges to
`(raw_offset, raw_len)` per frame. `read`/`read_at`/`size` are served from
this index: `size` is O(1) after the first access; `read_at` decrypts only
the frames a request's `[offset, offset+len)` range overlaps, fetching
their raw bytes via the wrapped disk's own `read_at` (never the whole
file). `append` looks up the index (or builds it, on a cold file), seals
the new frame with the next sequential counter, and appends `header?
(only on the file's first-ever write) || frame` to the wrapped disk in one
physical `append` call — the frame index update is then a cheap in-memory
push, no re-scan.

### Nonce scheme

**XChaCha20-Poly1305** (RustCrypto `chacha20poly1305`), chosen over
AES-GCM specifically for its 24-byte extended nonce: a 96-bit AES-GCM
nonce would force either a narrow per-file frame counter (risking overflow
on a long-lived, frequently-appended WAL) or a random component per frame
(reintroducing a birthday-bound collision risk this design wants to avoid
entirely, since a busy WAL segment can carry many thousands of frames over
a group's lifetime). XChaCha20-Poly1305's 192-bit nonce has room for both a
128-bit random per-file salt *and* a 64-bit monotonic frame counter with no
practical overflow and no reliance on randomness staying collision-free
across frames.

Nonce for frame `i` of a file = `SALT(16 bytes) || i.to_be_bytes()(8
bytes)`. Uniqueness holds because:

- `SALT` is minted fresh, from the seeded `Rng` seam (never `OsRng`/
  `thread_rng` outside `ProdEnv`, so a `SimEnv`-driven test stays
  seed-reproducible), the moment a file is created — the very first
  `append` on an absent file, or every `replace` call (which always
  starts a fresh generation, even when it happens to end up producing
  byte-identical plaintext to what preceded it).
- The frame counter increments strictly per `append` within one file
  generation (one salt), so `(salt, counter)` — and therefore the derived
  nonce — never repeats for the life of that generation.
- **`replace` always mints a new salt**, specifically so a WAL
  compaction or manifest rewrite can never reuse the nonce space of
  whatever generation preceded it, even when the new content happens to
  restart at counter 0.

### Loud refusal (the `check_wal_layout` precedent)

Per the task's non-negotiable behaviours, a wrong or missing key against an
existing encrypted data directory — or the reverse, a key against an
existing plaintext one — must be refused loudly, before any WAL/engine file
is opened for use, in the style of `animus_cp_data::host::
check_wal_layout`. `verify_or_init_marker` is that check: it lists the data
directory and reads **at most one** small marker file
(`.animus_encryption_marker`) to decide the directory's state, refusing on
either mismatch and initializing the marker on a genuinely fresh directory.
It never scans or opens a WAL/engine file itself.

| directory has the marker | key given | outcome |
|---|---|---|
| yes | no | **refuse**: encrypted directory, no key |
| yes | yes, authenticates | proceed encrypted |
| yes | yes, does not authenticate | **refuse**: wrong key |
| no, but other files exist | yes | **refuse**: key against a plaintext directory |
| no, but other files exist | no | proceed plaintext (untouched, today's behavior) |
| no other files (fresh) | yes | initialize the marker, proceed encrypted |
| no other files (fresh) | no | proceed plaintext (untouched, today's behavior) |

The marker is written once via `Disk::replace`, whose crash contract ("a
crash before or after sees the whole old or whole new contents, never a
mix") means it can never itself be torn — so *any* parse/authentication
failure reading it back means "wrong key or corrupted marker," never "torn
write," unlike an ordinary data file (next section). The exact refusal
texts (verified against real `ProdEnv` startup in
`crates/animus-env/src/prod.rs`'s own tests and
`crates/animusd/tests/encryption_at_rest_e2e.rs`):

```
data directory is encrypted (found .animus_encryption_marker) but no --encryption-key was
given — refusing to start. Pass --encryption-key PATH with the same key this directory was
created with, or point at a fresh data directory.
```

```
--encryption-key does not match the key this data directory was encrypted with — refusing
to start. Use the original key, or point at a fresh data directory.
```

```
--encryption-key was given but this data directory already holds unencrypted files (no
.animus_encryption_marker marker) — refusing to start. Encryption cannot be enabled on an
existing plaintext data directory; point at a fresh data directory instead.
```

The marker filename is excluded from every `Disk::list()` result this
wrapper serves, so no higher layer (an orphan sweep, `check_wal_layout`
itself, a prefix-based file discovery) ever sees an unexpected extra name
in its own directory listing.

### Torn tails vs. real corruption — the positional rule

A crash mid-`append` can leave a file's last frame short (the length
prefix or ciphertext bytes never made it to disk) or failing AEAD
authentication (the tag was only partially written). Because
`verify_or_init_marker` has already confirmed the configured key is
correct for this directory *before* any ordinary file is ever scanned, a
frame failure encountered scanning one can only mean "this is where the
last unsynced write was cut off" — never "wrong key."

But a failure can still mean two different things: a genuine crash-torn
tail (nothing valid follows it — a crash, by definition, can only ever
tear the physical *end* of a file), or real at-rest corruption of
already-durable bytes with more valid data sitting right after it (bit
rot, or a test's `Simulator::corrupt_durable`). Silently truncating on
every failure would turn the second case into silent, unbounded data
loss — discarding intact data because an *earlier* frame happened to get
corrupted. So the scan is **positional**, the identical rule
`animus-storage`'s own hand-rolled WAL-record codec already uses one layer
up (`lsm.rs`'s own doc: "distinguishing a legitimate crash-torn trailing
record from real corruption is not a magnitude check on the frame — it's
positional"): scanning continues past a failed frame (using its own,
still-self-consistent length field to locate what follows) purely to
classify it — nothing past a failure is added to the trusted index. If
that exploration finds a *later* frame that authenticates, the whole
scan reports `Corrupted` and the caller gets a hard `io::Error` instead of
a silent truncation. If nothing valid follows, it's a genuine torn tail: the
trusted index — everything up to, not including, the failed frame — is
what recovery keeps, and the file is physically truncated back to it (via
`Disk::replace`) the first time it's touched, so a subsequent `append`
never strands the discarded bytes mid-file.

This independently converges on the same design `animus-storage`'s own WAL
codec already uses for an unrelated reason (that layer's own per-record
CRC32) — strong evidence the rule is the right one for this class of
problem, not an artifact of one specific format.

### Key management and threat model

- **256-bit key, loaded from a per-node key file** — `--encryption-key
  PATH` (64 hex characters, optionally trailing newline; generate with
  `openssl rand -hex 32 > key.hex`), following the `--dynamo-auth PATH`/
  `--tls-cert PATH` pattern: a path to a local secret, never the secret
  embedded in a `ClusterConfig` JSON file or command-line argument. The
  config-file counterpart, `RoleAddrs::encryption_key_path: Option<String>`
  (`#[serde(default)]`), is per-node — like `tls`, not like
  `dynamo_auth`'s single cluster-wide field — since each node's own disk is
  independent and could in principle use a different key, though the
  common case is one key file's path repeated across every node's config
  entry (or, for `--cluster N`'s in-process dev cluster, one
  `--encryption-key PATH` flag applied uniformly).
- **What this protects against**: a stolen/lost disk, a snapshot or backup
  of a data volume exfiltrated at rest, a cloud provider or co-tenant with
  read access to underlying block storage. Any file this wrapper writes is
  unreadable without the key.
- **What this does *not* protect against, by design**: a compromised,
  *running* node — the key is loaded into process memory for the life of
  the node and every read the process serves is necessarily plaintext to
  its own caller; this is encryption **at rest**, not confidentiality
  against a live memory dump or a malicious operator with process access.
  Key **rotation** is out of scope for v1 — there is no mechanism to
  re-encrypt an existing data directory under a new key; rotating means
  standing up a fresh, differently-keyed replica and letting Raft catch it
  up (the same mechanism that already handles any other full-replica
  rebuild), then decommissioning the old one. This mirrors ADR 0064's TLS
  cert rotation story (no built-in rotation, replace-the-replica) rather
  than inventing a new one. **No key derivation, no KMS integration, no
  per-tenant keys** — one key per node, matching the "per-node local key
  file" pattern this ADR intentionally keeps as simple as `--tls-cert`.
- **Zeroization**: `EncryptionKey` wraps its 32 bytes in `zeroize::
  Zeroizing`, scrubbing them from memory on drop. `Debug` is manually
  implemented to print `EncryptionKey(REDACTED)`, never the bytes. The key
  never appears in the config dump (`GET /admin/config` reports only
  `encryption_key_path`, a path string, exactly the way `TlsSection`
  reports cert/key *paths* and never PEM bytes), any log line, any error
  message (every refusal text above names only the mismatch, never key
  material), or the operator ConfigMap (deferred to PR 3, which mounts the
  key file from a Kubernetes `Secret` — this PR leaves the config-field
  hook `encryption_key_path` ready for that mount path, with no
  ConfigMap plumbing of its own).

### Crash safety

Every property above composes with the existing crash-safety arguments one
layer up, unmodified:

- **Durable-before-visible** is unaffected — `append`'s "buffered, not yet
  durable until `sync`" contract, and `sync`'s fsync-then-ack, both pass
  straight through to the wrapped disk; this wrapper adds no buffering of
  its own.
- **`replace`'s atomic-swap contract** ("a crash before or after sees the
  whole old or whole new contents, never a mix") is preserved because this
  wrapper's own `replace` calls the wrapped disk's `replace` exactly once,
  with the complete new (header + one frame) payload already assembled in
  memory — there is no partial-write window this wrapper itself introduces.
- **A torn write never partially decrypts** — the whole point of framing at
  `append` granularity: a torn frame fails to parse or authenticate as a
  *whole unit*, so recovery either has the complete frame (fully
  authenticated, fully decryptable) or doesn't have it at all. There is no
  byte-range of a torn frame that could be misread as valid shorter
  plaintext.

## Crate placement

`crates/animus-env/src/encrypted.rs`, unconditional (no `prod` Cargo
feature) — `EncryptedDisk<D: Disk, R: Rng>` and `EncryptedEnv<E: Env>` are
generic over any `Disk`/`Env` implementation, specifically so `SimEnv`'s
crash/fault corpora can drive them deterministically with no dependency on
`ProdEnv` at all. `animus-env` already carries `async-trait`/`serde`/
`thiserror` unconditionally; adding `chacha20poly1305`/`zeroize`
unconditionally too keeps the wrapper reachable from `SimEnv`-only test
crates (`animus-sim`, `animus-storage`) with no `prod` feature enabled.

A **new sibling crate** (`animus-crypto`) was considered and rejected: the
wrapper's only real dependency beyond the AEAD/zeroize crates is
`animus-env`'s own `Disk`/`Rng`/`Env` traits — there is no meaningful
dependency-graph reason to add a third crate to the workspace for ~650
lines of code that exists entirely to compose with those traits, and doing
so would cost every consumer (this ADR's own `animus-sim`/`animus-storage`
test crates included) one more path dependency to wire for no functional
gain.

### `ProdEnv` composes the same machinery natively — it is not
`EncryptedEnv<ProdEnv>`

`ProdEnv`'s own `Disk` impl was refactored into a thin dispatch
(`DiskBackend::{Plain, Encrypted}`) over a new `RawFsDisk` (the exact
tokio::fs logic `impl Disk for ProdEnv` used to contain, moved verbatim)
composed with the *same* `EncryptedDisk<RawFsDisk, DiskSaltRng>` the
generic wrapper is built from — `DiskSaltRng` being a minimal, zero-sized
`Rng` drawing real OS randomness, deliberately **not** `ProdEnv` itself
(which would create an `Arc` reference cycle: `ProdEnv`'s `Inner` holding a
`DiskBackend::Encrypted` that in turn held a `ProdEnv` pointing back at the
same `Inner` would never be freed).

This was a deliberate choice over the mechanically simpler
`EncryptedEnv<ProdEnv>`: `animusd` — the crate that actually constructs
`ProdEnv` — hardcodes the concrete type `ProdEnv` at roughly a hundred call
sites across its ~12,000-line `lib.rs` (`field: ProdEnv`,
`RaftKvNode<ProdEnv, LsmEngine<ProdEnv>>`, `Vec<ProdEnv>`, and so on) —
`animusd` is monomorphized against `ProdEnv` directly, not generic over
`E: Env`. Replacing every one of those with `EncryptedEnv<ProdEnv>` would
be a whole-crate type-parameter rename with a large blast radius for a
change whose only purpose is routing bytes through an extra layer that
`ProdEnv` can just as well apply to itself internally. Composing the
identical `EncryptedDisk`/`verify_or_init_marker` primitives *inside*
`ProdEnv`'s own `Disk` impl gets the identical behavior — a node with no
`--encryption-key` runs `RawFsDisk`'s code path unchanged, byte-identical
to pre-ADR-0069 `ProdEnv` — with the only production wiring cost being the
handful of real `ProdEnv::bind`/`bind_with_tls` call sites gaining a new
`bind_with_tls_and_key` sibling constructor.

## Off by default; zero overhead

No `--encryption-key`/`encryption_key_path` configured anywhere means
`DiskBackend::Plain(RawFsDisk)` — the exact `tokio::fs` calls `ProdEnv`'s
`Disk` impl always made, with the dispatch macro's match arm costing one
branch per call, no allocation, no extra I/O. `verify_or_init_marker` still
runs once at bind time even with no key (to catch "directory already
encrypted, key omitted by mistake") — a single `Disk::list()` call, paid
once per node startup, never on the read/write hot path.

## Wiring into `animusd`

- `RoleAddrs::encryption_key_path: Option<String>` — a new, `#[serde(
  default)]` per-node config field, mirroring `tls`'s shape.
- `--encryption-key PATH` — a new CLI flag, parsed once in `main::run` and
  applied via `apply_encryption_key_flag` (the identical "config file and
  flag both setting it is a hard error" contract `apply_tls_flag` uses) for
  `--config FILE --node I`, and uniformly across every generated node's own
  `RoleAddrs` for `--cluster N` (`bind_cluster_with_advertise_host_and_
  key`). Rejected outright (a loud `Err`, not a silent no-op) for
  `--cluster-control`/`--cluster-data`, mirroring `--tls-*`'s own posture on
  that path.
- `Node::bind`/`bind_control`/`bind_data` each load the key
  (`EncryptionKey::load_from_file`) and call the new
  `ProdEnv::bind_with_tls_and_key` instead of `bind_with_tls` — the loud
  refusal happens inside that call, before the node binds any listener.
- **Reach gap, honestly stated** (mirroring issue #676's existing
  join/seed/dev-path gaps for other per-node flags): `--cluster-control`/
  `--cluster-data`, `animusd control`, `animusd data --config`, `animusd
  data --seed`, and `animusd join` do not yet accept `--encryption-key` —
  the same shape `--tls-*` and several other per-node flags already have on
  those entry points. Not widened here.

## Tests

- **Unit** (`crates/animus-sim/tests/encrypted_disk.rs`, 14 tests): round
  trip through every `Disk` method over `SimEnv`; frame-index rebuild after
  a fresh `EncryptedEnv::open` (simulating a restart); wrong/missing key
  refused with the exact text; a single tampered byte fails authentication;
  nonce uniqueness across appends and across a `replace` (verified as
  distinct ciphertext for identical plaintext, and a distinct salt after
  `replace`); the marker is hidden from `list()` while genuinely present on
  the raw disk underneath.
- **Crash/fault corpus**
  (`crates/animus-storage/tests/lsm_crash_encrypted.rs`, depth knob
  `ANIMUS_LSM_ENCRYPTED_SEEDS`): the sibling of `lsm_crash.rs`'s existing
  corpus, over `LsmEngine<EncryptedEnv<SimEnv>>` — synced writes and a
  flushed SSTable survive a crash + reopen; `DiskConfig::torn_tail_on_
  crash`/`corrupt_on_crash` never partially decrypt and every synced write
  survives; a wrong/missing key on reopen is the loud refusal; and — the
  property this ADR's "positional" design specifically buys — a mid-file
  corruption of an already-durable frame (`Simulator::corrupt_durable`,
  no crash involved) is a **hard error**, never a silent loss of the
  intact frames that came after it.
- **ProdEnv end-to-end** (`crates/animusd/tests/
  encryption_at_rest_e2e.rs`): a real node, real disk, real DynamoDB wire —
  write with a key, restart with the same key and read back (plus grep the
  raw data directory recursively for the plaintext value: absent), restart
  with a different key (loud refusal, exact text), restart with no key
  against the encrypted directory (loud refusal), and a key against an
  existing plaintext directory (loud refusal, the reverse mismatch).
- **`ProdEnv` unit tests** (`crates/animus-env/src/prod.rs`): the identical
  properties against a real filesystem directly through `ProdEnv::
  bind_with_tls_and_key`, plus a `no_key_writes_no_marker_and_stays_byte_
  identical` regression pinning the "off by default" contract at the byte
  level.

## Consequences

- Every file any crate writes through the `Disk` seam is covered by one
  mechanism, not reimplemented per format.
- The frame-index design trades a small per-process, per-open-file memory
  cost (frame offset/length pairs, not the frame bytes themselves) for
  avoiding whole-file decryption on every `read_at` — the right trade for
  `LsmEngine`'s point-read-heavy access pattern.
- Key rotation and per-tenant keys are explicitly deferred; a future ADR
  would need to design a re-encryption or key-versioning story if either
  becomes a real requirement — nothing in this design blocks that, but
  nothing in it builds toward it either.
- PR 2 (`SegmentStore`) is implemented (see the "As-built: PR 2" amendment
  below) for the `fs:`/`s3://` opt-in stores, under a cluster-wide (not
  per-node) key. PR 3 (operator key-secret mount) is implemented (see the
  "As-built: PR 3" amendment below) — **S-03 is complete.** The default
  replicated `cluster` store (`SegmentStoreConfig::Cluster`/
  `BackupStoreConfig::Cluster`) — PR 2's own stated scope cut, tracked as
  issue #680 — is closed by the 2026-09-07 "As-built: cluster store"
  amendment below, under the identical cluster-wide key.

## As-built: PR 2 (`SegmentStore`, 2026-09-06)

Encryption at rest for the `SegmentStore` seam (`crates/animus-env/src/
encrypted_segment_store.rs`): `EncryptedSegmentStore<S: SegmentStore, R:
Rng>` seals each object as a whole standalone frame — a fresh 21-byte
header (random salt) plus exactly one AEAD frame at counter 0, reusing
`encrypted.rs`'s own frame codec (`seal_whole`/`open_whole`, factored out
of `seal_marker`/`marker_authenticates`, which now call them) — since a
`SegmentStore` object, unlike a `Disk` file, is never incrementally
appended: it is always written whole in one `put` call (the trait's own
write-once contract) and read whole in one `get` call. No frame index is
needed, unlike `EncryptedDisk`.

### Key scope: cluster-wide, not per-node — the central decision this PR had to make

PR 1's per-node key file works because a node's own `Disk` files are
*always* read by that same node's own process — no other node ever opens
another node's WAL. A `SegmentStore` object breaks that assumption
structurally: a backup captured on node A is later restored by whichever
node happens to receive the `RestoreTableFromBackup` call (routinely a
*different* node); a PITR segment sealed by one tablet's leader is later
replayed by whichever node drives that restore; a DynamoDB Streams shard
object is read by whichever node happens to serve `GetRecords`. Two
designs were considered:

- **(a) One key, shared by every node that can reach the store** (chosen).
  Every node configured with `--backup-store fs:PATH`/`--segment-store
  s3://...` and `--encryption-key PATH` passes the *identical* key file —
  the same "one key file's path repeated across every node's config entry"
  pattern PR 1 already established for the common case, now load-bearing
  rather than merely convenient. A mismatch (a node with a different key,
  or no key, pointed at an already-marked store) is refused loudly at
  **node startup** — `build_segment_store`/`build_backup_store` call
  `verify_or_init_segment_store_marker`/`EncryptedSegmentStore::open`
  unconditionally, before the node's listeners bind — never discovered
  later as a mysterious decrypt failure deep inside a restore job.
- **(b) A separate `--segment-store-key`/`--backup-store-key` flag**,
  independent of `--encryption-key`. Rejected: it would let an operator
  encrypt the `Disk` seam and the `SegmentStore` seam under two unrelated
  keys with no structural reason to — every node in a cluster is already
  a single trust domain for this feature (PR 1's own threat model: a
  stolen disk, not a compromised live process), so a second key doesn't
  buy additional isolation, only a second secret to provision and rotate
  correctly. (b) would matter if a future requirement wanted the backup
  store's blast radius genuinely separated from a node's live disk (e.g.
  the backup bucket is handed to a different, less-trusted operator) — not
  a requirement today, and easy to add later as a distinct opt-in without
  disturbing (a)'s default.

**Threat model difference from PR 1**: the same "stolen/lost disk, not a
compromised live process" model, extended to "a stolen/lost backup bucket
or shared filesystem mount, not a compromised live process." The
cluster-wide key means every node that can decrypt the store is,
definitionally, a full trust peer for that store's contents — no
per-node compartmentalization exists (or was asked for) at this layer.

**On a mixed cluster** — some nodes configured with the shared key, others
with none or a different one, all pointed at the same `fs:`/`s3://` store —
whichever node's own local marker check runs first (at ITS OWN startup)
decides the store's fate: the first node to start against a fresh store
initializes the marker under its own key (or leaves it plaintext, if it
has none); every node started afterward with a different key or no key
is refused at ITS OWN startup, never allowed to silently read/write the
store as if the mismatch didn't exist. A cluster is therefore never
*silently* half-encrypted — a misconfigured node simply never starts,
with an error naming exactly what's wrong (see "Error texts" below). The
practical operational rule: provision the key file identically (same
content) on every node before ever pointing more than one node at the
same `fs:`/`s3://` store.

### `Cluster` (the default) is untouched — an honest scope cut, not a claim it's already covered

The default `SegmentStoreConfig::Cluster`/`BackupStoreConfig::Cluster`
(the K-replicated store, `ClusterSegmentStore<ProdEnv, FsSegmentStore>`)
is **not** wrapped by this PR. Its per-node local building block
(`FsSegmentStore`, rooted at `dir.join("segments")`/`dir.join("backups")`)
does its own raw `tokio::fs` I/O directly — it is not layered on the
`Disk` trait `EncryptedDisk` wraps, so PR 1's own `--encryption-key`
mechanism never touched it and does not "already cover" it on disk,
despite that being a plausible-sounding shorthand. Encrypting it would
mean changing `SegmentStoreHandle::Cluster`'s own concrete
`ClusterSegmentStore<ProdEnv, FsSegmentStore>` type parameter to
`ClusterSegmentStore<ProdEnv, EncryptedSegmentStore<FsSegmentStore, ..>>`
— a larger, structurally separate change (every call site that
pattern-matches the `Cluster` variant would need to thread a second
generic parameter through `animus-cp-data`'s own `cluster_segment_store`
module) with no design blocker, just out of this PR's scope. Stated here
plainly per this ADR's own discipline against silently-stale claims:
today, only the `Fs`/`S3` opt-in stores are sealed; the default replicated
store stays plaintext on disk. **Closed by the 2026-09-07 "As-built:
cluster store" amendment below (issue #680)** — the "larger, structurally
separate change" this paragraph anticipated turned out to be a single new
local-store type occupying `ClusterSegmentStore`'s own pre-existing
generic parameter, not a second generic threaded through every call site;
see that amendment for what actually landed and why the type-parameter
widening this paragraph worried about was smaller than expected.

### Loud refusal — the exact texts (`SegmentStore` counterpart of PR 1's own table)

`verify_or_init_segment_store_marker` mirrors `verify_or_init_marker`'s
table exactly, swapping `Disk::list`/`read`/`replace` for
`SegmentStore::list`/`get`/`put`, and a small marker **object**
(`.animus_segment_store_encryption_marker`, filtered out of every `list`
result this wrapper serves) for the marker **file**:

```
segment store is encrypted (found .animus_segment_store_encryption_marker) but no
--encryption-key was given — refusing to start. Pass --encryption-key PATH with the
same key this store was created with, or point at a fresh store.
```

```
--encryption-key does not match the key this segment store was encrypted with —
refusing to start. Use the original key, or point at a fresh store.
```

```
--encryption-key was given but this segment store already holds unencrypted objects
(no .animus_segment_store_encryption_marker marker) — refusing to start. Encryption
cannot be enabled on an existing plaintext segment store; point at a fresh store
instead.
```

A `get` (or `put`'s own write-once plaintext-comparison read) that fails
to authenticate an *existing* object is a separate, fourth error, always a
hard failure naming the object id — never a silent `None`/torn-tail trim,
since a `SegmentStore` object has no partial-write window to distinguish
from corruption in the first place (see the module's own doc):

```
{id}: failed to authenticate/decrypt this segment store object — either the
configured --encryption-key does not match the key this object was written with, or
the object is corrupted. Segment store objects are write-once, so this can never be a
torn write; refusing rather than risking silent data loss.
```

### Write-once at the plaintext level, not ciphertext

`SegmentStore::put`'s contract treats a same-id, same-content re-put as a
safe no-op and a same-id, different-content re-put as a hard error. A
fresh random salt is minted on **every** `put` (the identical
nonce-uniqueness discipline `Disk::replace` uses), so two `put` calls
with byte-identical plaintext produce different ciphertext —
`EncryptedSegmentStore::put` therefore fetches and decrypts whatever
already exists at `id` first and compares **plaintext**, exactly what
`FsSegmentStore`/`S3SegmentStore` themselves already do one layer down
for an unencrypted store; comparing raw (encrypted) bytes would have
wrongly rejected a legitimate idempotent retry.

### Wiring

`build_segment_store`/`build_backup_store` (`animusd::lib`) gained an
`encryption_key: Option<&EncryptionKey>` parameter and became `async`
(the marker check does real I/O). `Fs`/`S3` wrap in `EncryptedSegmentStore`
when a key is given; `Cluster` never does (see above). Even with **no**
key configured, the loud-refusal marker check still runs unconditionally
on `Fs`/`S3` (mirroring PR 1's "off by default still checks" rule) — a
store that already holds encrypted objects is never silently treated as
plaintext just because this particular node omitted `--encryption-key`.
`SegmentStoreHandle`/`BackupStoreHandle` gained one new variant each,
`EncryptedFs(EncryptedSegmentStore<FsSegmentStore, ProdEnv>)` — `ProdEnv`
itself supplies the `Rng` these salts draw from (it already implements
`Rng`, drawing real OS randomness), so no new zero-sized `Rng` type was
needed the way `DiskSaltRng` was for PR 1's `Disk` seam (no `Arc`
reference-cycle risk here — `EncryptedSegmentStore` is never nested
inside `ProdEnv`'s own `Inner`). The `S3` variant needed no new enum
arm — it already stores an `Arc<dyn SegmentStore>`, so the encrypted case
is just `Arc::new(EncryptedSegmentStore::open(raw_s3, ..))` boxed the
same way. `Node`/`BoundControlNode`/`BoundDataNode` each gained an
`encryption_key: Option<EncryptionKey>` field (a clone of the same key
`Node::bind*` already loads for `ProdEnv::bind_with_tls_and_key`), so PR
2's own wiring adds no second key-loading path — same reach as PR 1
(`--config`/`--node`, `--cluster N`); the `--cluster-control`/
`--cluster-data`/`animusd control`/`animusd data`/`animusd join` gaps PR 1
already named are unchanged, not widened.

### Tests

- `crates/animus-sim/src/segment_store.rs`'s test module: the shared
  `assert_segment_store_contract` against
  `EncryptedSegmentStore<SimSegmentStore, SimEnv>`; a plain round trip
  proving the wrapped store never sees plaintext; wrong-key-at-open
  refusal; a tampered object failing loudly and naming its id; and all
  three marker-mismatch directions plus the marker's own exclusion from
  `list`.
- `crates/animus-env/src/prod.rs`: `EncryptedSegmentStore<FsSegmentStore,
  DiskSaltRng>` against a real temp directory — the shared contract, plus
  a real-filesystem proof of the three mismatch directions and that the
  raw bytes on disk never contain the plaintext value.
- `crates/animus-env/src/s3_store.rs`: the shared contract against
  `EncryptedSegmentStore<S3SegmentStore<FakeS3>, R>` (a small deterministic
  test-only `Rng`, not `OsRng`), plus the wrong-key-refused/no-plaintext-
  in-the-bucket proof.
- `crates/animus-test/tests/segment_store_encrypted_fault_corpus.rs`
  (depth knob `ANIMUS_SEGMENT_STORE_ENCRYPTED_SEEDS`, default 1, held at
  20 in this PR's own gate run): every `SegmentFaultConfig`/unavailability
  fault the backup/PITR/export-import domain corpora already inject
  through `SimSegmentStore` composes correctly underneath
  `EncryptedSegmentStore` — an ack-lost `put`/`delete` still lands the
  plaintext state change despite the caller-visible error; an
  unavailability window fails every op with no state change, then heals;
  write-once holds at the plaintext level under fault; a mid-object
  tamper is a hard `get` error naming the id; an ack-lost fault on the
  marker object's own `put` during `open()` is recovered by retrying
  `open` (the marker landed despite the surfaced error, the identical
  ambiguity every other object tolerates); and the three marker-mismatch
  directions hold across many simulation seeds. **The three existing
  ~2,400-line domain corpora (`backup_fault_corpus.rs`,
  `pitr_fault_corpus.rs`, `export_import_fault_corpus.rs`) were
  deliberately NOT converted to run through an encrypted store** — every
  one hardcodes its shared harness functions to a concrete
  `&SimSegmentStore` parameter across dozens of functions; genericizing or
  duplicating all three is a materially larger, separate refactor with
  real regression risk against already-hard-won deterministic corpora,
  named here as an explicit follow-up rather than attempted piecemeal.
- `crates/animusd/tests/encryption_at_rest_segment_store_e2e.rs`
  (`ProdEnv`, real sockets/disk, real 2-node cluster): a keyed cluster's
  `CreateBackup` issued against node 0 followed by `RestoreTableFromBackup`
  issued against **node 1** (which never itself wrote the backup object)
  succeeds and restores the exact backup-time value, with the shared
  `fs:` backup-store directory never holding the plaintext item value at
  any point; a node started with a *different* key against that same
  (already-marked) directory is refused at its own startup, before it
  binds a single listener; and a key configured against an existing
  *plaintext* backup-store directory is refused the same way. (A
  cross-node mismatch is demonstrated at the point it is actually
  enforced — node startup — rather than by waiting out the restore
  driver's own internal retry/stuck-timeout loop, which would only
  observe the identical refusal indirectly and far more slowly.)

### Secrets never leak (unchanged from PR 1's own posture)

No key material appears in any log line, error message (every text above
names only the mismatch, never key bytes), `/admin/config` dump (still
only `encryption_key_path`, a path string), or operator ConfigMap. Nothing
in this PR changes that surface.

## As-built: PR 3 (operator key-secret mount, 2026-09-07)

`crates/animus-operator`'s reconciler now mounts the cluster-wide key PR
1/2 need onto every pod, closing S-03: `spec.encryptionKeySecretName:
Option<String>` (`crd.rs`) names a pre-existing, user-provisioned
`Secret` — the operator never generates, inspects, or stores key material
itself, only mounts what's already there — mirroring the flat, single-
field shape `spec.dynamoAuthSecretName` already uses (not the nested,
two-shape `spec.tls`/`{secretName, certManager}` pattern: there is no
second way to *obtain* this key the way cert-manager offers a second way
to *issue* a TLS cert, so a nested object would only add ceremony).

### Field shape and mount

- **Data key**: exactly one well-known key, `"key"`
  (`desired::cluster_config::ENCRYPTION_KEY_SECRET_DATA_KEY`) — the
  `Secret` must carry the raw 64-hex-character key
  (`animus_env::EncryptionKey::load_from_file`'s own format,
  `openssl rand -hex 32`) under that name; any other keys in the `Secret`
  are ignored.
- **Mount path**: `/etc/animus/encryption`
  (`desired::cluster_config::ENCRYPTION_KEY_MOUNT_DIR`), read-only, on
  every pod regardless of role — both the combined and data branches of
  `entrypoint.sh` reach a `--config`/`data --config` invocation, and
  `RoleAddrs::encryption_key_path` is read identically by `Node::bind`/
  `bind_control`/`bind_data` regardless of which one a given ordinal
  execs. `defaultMode` is set to `0o444` (world-readable, no write bit
  for anyone) rather than left at Kubernetes' own `0644` default or
  tightened further to `0o440`/`0o400`: this pod spec sets no
  `securityContext.fsGroup`, and a `Secret` volume's files are owned
  `root:root` by default, so a mode that dropped the "other" read bit
  would make the key unreadable by the non-root `animus` user the
  `animusd` image runs as (`Dockerfile`'s `USER animus:animus`) — see
  `desired::statefulset::ENCRYPTION_KEY_SECRET_DEFAULT_MODE`'s own doc
  for the full reasoning.
- **`cluster.json` wiring**: every node's `RoleAddrs` entry gets
  `encryption_key_path: "/etc/animus/encryption/key"` when the field is
  set — identical across every node by construction
  (`desired::cluster_config::encryption_key_mount_path`), the exact
  shape `spec.tls`'s own `tls_section()` already established for
  `RoleAddrs.tls`. **Never a `--encryption-key` CLI flag**: PR 1's flag
  reaches only `--config FILE --node I`/`--cluster N`, and every pod this
  operator generates already runs `--config`/`data --config` against a
  `cluster.json` whose own node entry can carry the field directly — the
  config-file route was already the *complete* route for this operator's
  own deployment shape, so there was never a reason to also emit the flag
  (which would in fact be a hard `animusd` startup error on top of an
  already-set config-file value, the identical "one way, not both"
  contract `--dynamo-auth`/`--quiesce-after` document for themselves).

### Failure semantics

**A missing or malformed `Secret` is checked live, and does NOT strip the
field.** Unlike `spec.tls`/`spec.s3`/`spec.backupStore`+`spec.
segmentStore` — each a pure, spec-*shape* check with no cluster access,
following this crate's own "no admission webhook in v1" posture — a
`Secret` *reference*'s only checkable property is whether it actually
exists, which needs a live read. `crate::controller::
validate_encryption_key_secret` calls `ClusterApi::get_secret` on every
reconcile and sets `EncryptionKeySecretInvalid`
(`crd::CONDITION_ENCRYPTION_KEY_SECRET_INVALID`) naming exactly what's
wrong (the `Secret` doesn't exist, or exists without the `"key"` data
key) — but, deliberately, does **not** fall back to reconciling as if the
field were unset, the way every other `*SpecInvalid` condition does.

The reason is a real, not merely theoretical, hazard: falling back to
"as if unset" would regenerate a plaintext `cluster.json` (no
`encryption_key_path` on any node) for a cluster whose data directory may
already be encrypted from an earlier, valid reconcile. Since the config-
hash restart annotation rolls every pod the moment the field's own
presence changes, that fallback would actively **cause** a rolling
restart into PR 1's own loud "data directory is encrypted ... but no
--encryption-key was given" refusal — a self-inflicted `CrashLoopBackOff`
the operator itself triggered, worse than the alternative. Leaving the
spec's own still-`Secret`-name-referencing desired state in place instead
means: a genuinely missing `Secret` leaves the pod `ContainerCreating`
(the volume can't mount) until it's created — harmless, and self-healing
the moment it exists, with `EncryptionKeySecretInvalid` telling the
operator why. A `Secret` that exists but lacks the `"key"` data key is
the one case a live check can catch *before* it ever reaches a broken
container (Kubernetes mounts whatever keys a `Secret` does have; a
missing `key` file inside the mount would otherwise surface only as a
plain "file not found" `animusd` startup error) — worth detecting up
front even though the mount itself still isn't stripped.

**Reversing the field** (removing `spec.encryptionKeySecretName` from an
already-encrypted cluster) is symmetric and equally uncovered by any
live check the operator can perform: the operator applies the spec as
given — the volume disappears, `cluster.json` drops `encryption_key_path`
on every node, the config-hash rolls every pod — and each restarted
`animusd` process hits PR 1's identical refusal (no key against an
already-encrypted directory) on its own, at its own startup, the correct
and only place that mismatch can be caught. The operator does not, and
structurally cannot, second-guess an operator-authored spec edit here;
this is stated plainly rather than silently assumed safe.

### Config-hash interaction (S-07d)

`desired::statefulset::RestartRelevantConfig` gained an
`encryption_key_path: Option<String>` field — `spec.tls`'s own precedent
exactly: only the field's *presence* (mapped to the same fixed mount
path every node gets, never the `Secret`'s own name) participates in the
hash. Consequences, both deliberate:

- **Adding or removing the field rolls every pod** — a running `animusd`
  reads `RoleAddrs::encryption_key_path` once at boot and cannot pick it
  up live, so this transition must trigger a restart, and does.
- **Renaming the referenced `Secret` under an unchanged field-presence
  state rolls every pod too — but not through this hash.** The mount
  path (`/etc/animus/encryption/key`) never changes, so the hash is
  unaffected by *which* `Secret` is named; the volume's own `secretName`
  changing is itself already a `spec.template` diff the `StatefulSet`
  controller catches on its own, the identical "let the thing that
  actually changed be what triggers the diff" reasoning `spec.tls`'s own
  hash-exclusion note gives.
- **Rotating the `Secret`'s own content under an unchanged name never
  rolls any pod, at either layer** — not through this hash (which never
  reads `Secret` content, only the field's presence) and not through the
  `StatefulSet`'s own template diff (the `Secret`'s *name* in the volume
  spec is unchanged). This is correct, not a gap: ADR 0069 has no
  in-place re-encryption mechanism to roll a pod *into*, so rotation
  is out of scope in v1 and a restart-on-rotation would just be a
  restart into a still-mismatched key.
- The pinned config-hash regression test
  (`desired::statefulset::tests::config_hash_pinned_for_a_fixed_
  fixture`) is **unchanged in value** despite this field's addition —
  `encryption_key_path` carries its own `#[serde(skip_serializing_if =
  "Option::is_none")]`, so a spec with the field unset (every cluster
  that predates this PR) serializes byte-identically to before the field
  existed. This was a deliberate choice over `spec.tls`'s own
  always-serialize-as-`null` shape: `tls`'s hash contribution was already
  load-bearing for every deployed cluster by the time this field was
  added, so giving the new field the identical treatment would have
  forced a one-time upgrade-triggered restart on every cluster in
  existence, encrypted or not, for no functional reason.

### A documentation/code gap found, not fixed, while landing this PR

PR 1's own "Key management and threat model" section (and this ADR's
Consequences section) states `/admin/config` reports
`encryption_key_path` "a path string, exactly the way `TlsSection`
reports cert/key *paths*". **Tracing `animusd::admin::config_view`/
`AdminInfo` while grounding this PR found that claim was never actually
implemented** — `AdminInfo` has no `encryption_key_path` (or any
encryption-related) field at all, so `/admin/config` reports nothing
about encryption either way today. This is not a security regression
(the safer direction — nothing leaks — happens to be what shipped) and
this PR does not fix it, since doing so means touching `animusd`'s own
`lib.rs`/`admin.rs`, outside an operator-only PR's scope; it is recorded
here, plainly, per this repo's own discipline against a silently-stale
claim, rather than propagated into this PR's own e2e assertions as if it
were true. `scripts/e2e-kind.sh`'s own `E2E_ENCRYPTION=1` leg checks the
*actual* invariant instead — that `GET /admin/config`'s response body
never contains the raw key hex material, which holds regardless of
whether a future PR adds the (safe, path-only) field this text describes.

### e2e-kind leg

`E2E_ENCRYPTION=1` (`.github/workflows/e2e-kind.yml`'s `e2e-kind-
encryption` job) mirrors `E2E_TLS=1`/`E2E_S3=1`'s own shape: create the
`Secret` (a freshly generated key via `openssl rand -hex 32`, never
logged), set `spec.encryptionKeySecretName` on the manifest, drive the
ordinary `CreateTable`/`PutItem`/`GetItem` sequence, then — the leg's own
proof — `kubectl exec` into the serving pod and `grep -r` its data
directory for the plaintext item value written by `PutItem` (must be
absent) and check `GET /admin/config`'s raw response body never contains
the key hex, before continuing into the existing scale-up/`controlNodes`-
growth/delete phases unchanged. **UNVERIFIED in this repository's
sandboxed dev environment**, the identical `CAP_SYS_RESOURCE` reason
`E2E_TLS`/`E2E_S3` are — `kind` cannot come up here at all (see
`crates/animus-operator/CLAUDE.md`'s e2e section), so this leg has been
written carefully and `bash -n`-checked but never run end to end
anywhere; treat a first real CI failure on the `e2e-kind-encryption` job
as this leg finding its first real bug, not as this note being wrong.

### Tests

Unit, over the fakes, mirroring the `dynamo_auth`/`tls` precedents
exactly: `desired::cluster_config::tests` (the mount path present/absent,
identical across every node regardless of which `Secret` name is
configured, and that `entrypoint.sh` never emits `--encryption-key`);
`desired::statefulset::tests` (the volume/mount present/absent with the
restricted `defaultMode`, and five config-hash cases — added, removed,
unaffected by a rename, unaffected by an unrelated nodes-only scale, and
the pinned fixture literal unchanged); `controller::tests` (the live
Secret-existence/data-key check: wired correctly when valid, the
`EncryptionKeySecretInvalid` condition set and the field NOT stripped
when the `Secret` is missing or lacks the data key, the condition
clearing once fixed, and the baseline "field unset touches nothing"
case). `cargo test -p animus-operator` — 234 lib unit tests + 5 `main.rs`
tests + the `crd_manifest_pinned` regression, all green.

### What S-03 leaves open

One named, tracked follow-up, not closable from this PR's own scope
(**issue #680**, the default replicated `cluster` segment/backup store,
was open at the time this PR landed — since closed, see the 2026-09-07
"As-built: cluster store" amendment below):

- **Issue #676** — `animusd join`/`data --seed`/`--cluster-control`+
  `--cluster-data` don't thread several per-node knobs including (since
  PR 1) `--encryption-key`; irrelevant to this operator (which never
  generates those invocations) but a real gap for a hand-run cluster
  using those entry points.

## As-built: cluster store (2026-09-07, closes issue #680)

The default `SegmentStoreConfig::Cluster`/`BackupStoreConfig::Cluster`
store — the one every node runs unless `--segment-store`/`--backup-store`
is explicitly overridden — is now sealed under `--encryption-key` too,
closing the gap PR 2's own "`Cluster` (the default) is untouched" section
(above) named and this ADR's Consequences/"What S-03 leaves open" sections
tracked as issue #680.

### The widening turned out smaller than PR 2 anticipated

PR 2's own scope-cut paragraph worried that encrypting `Cluster` would
mean "widening `ClusterSegmentStore`'s own concrete type parameter... a
larger, structurally separate change... every call site that
pattern-matches the `Cluster` variant would need to thread a second
generic parameter through `animus-cp-data`'s own `cluster_segment_store`
module." Tracing `ClusterSegmentStore<E: Env, S: SegmentStore + Clone +
Send + Sync + 'static>`'s own definition (`animus-cp-data::
cluster_segment_store`) found that concern was already moot: the type was
**already generic** over its local building block `S` — never named
concretely to `FsSegmentStore` inside the type itself, only at
`animusd`'s own two construction sites
(`SegmentStoreHandle::Cluster`/`BackupStoreHandle::Cluster`'s own field
type). Closing the gap was therefore a single new local-store type
occupying that pre-existing type parameter, entirely inside `animusd`,
with **zero changes to `animus-cp-data`** — no second generic threaded
through `cluster_segment_store.rs`, no call site there touched at all.

### `LocalSegmentStore` — the new per-node local building block

`animusd::LocalSegmentStore` (`lib.rs`, `pub(crate)`) is a small two-arm
enum implementing `SegmentStore` by delegation:

```rust
enum LocalSegmentStore {
    Plain(FsSegmentStore),
    Encrypted(EncryptedSegmentStore<FsSegmentStore, ProdEnv>),
}
```

`SegmentStoreHandle::Cluster`/`BackupStoreHandle::Cluster` changed from
`ClusterSegmentStore<ProdEnv, FsSegmentStore>` to `ClusterSegmentStore<
ProdEnv, LocalSegmentStore>` — **one variant each, never a fourth
`EncryptedCluster` arm** (the preferred shape this ADR's own follow-up
task named, over adding a sibling variant): every existing match arm
handling `SegmentStoreHandle::Cluster`/`BackupStoreHandle::Cluster` is
untouched, since the variant's own shape (one `ClusterSegmentStore`
value) didn't change, only what its type parameter now is.

`build_segment_store`/`build_backup_store`'s own `Cluster` arms both now
call a small new helper, `local_cluster_store(env, dir, encryption_key)`
— `dir` is `node_dir.join("segments")`/`node_dir.join("backups")`,
identical to before this change — which runs the **identical**
`Fs`/`S3`-arm logic these two functions already had, just factored out
once rather than duplicated a third time: `encryption_key: Some(key)`
wraps the raw `FsSegmentStore` in `EncryptedSegmentStore::open` (which
itself runs the marker check as part of opening); `None` runs
`verify_or_init_segment_store_marker` directly first (the "off by default
still checks" rule PR 2 already established for `Fs`/`S3` — an
already-encrypted local directory is never silently treated as plaintext
just because this node omitted `--encryption-key`), then returns the
plain `FsSegmentStore` unwrapped. This is the identical control flow the
`Fs` arm has always had, now shared by three arms instead of two.

### Key scope — the existing cluster-wide convention, now load-bearing for this store too

**No new decision was needed here** — PR 2's own cluster-wide key-scope
argument already covers `Cluster` by construction, more directly than it
covers `fs:`/`s3://`: every node in a deployment already shares the
identical `--encryption-key` file (the same "one key file's path repeated
across every node's config entry" convention PR 1 established), and
`ClusterSegmentStore`'s own replication (`put_replicated`/`get_from`)
moves **only bytes** between nodes over the wire (`SegmentWire::Store`/
`Fetch`, `animus-cp-data::cluster_segment_store`) — a target node's own
`local.put(id, bytes)` call receives whatever bytes the sender's own
`SegmentStore::put` produced, with no reinterpretation in between. Since
every node's own `local` is now a `LocalSegmentStore::Encrypted` sealing
under the identical cluster-wide key, the bytes crossing the wire between
any two nodes are **ciphertext end to end** — ADR 0069's PR 1 threat
model (a stolen/lost disk) extends unmodified to "a stolen/lost disk on
*any* node holding a replica," and a node configured with no key, or a
different key, can neither serve an object to a peer's `Fetch` (its own
`local.get` would fail to authenticate, or — for a genuinely mismatched
node — never even reach that point, since its own `local_cluster_store`
call refused at startup) nor accept one from a peer's `Store` (same
refusal, before it can host anything at all). A cluster mixing keyed and
unkeyed/differently-keyed nodes is never silently half-encrypted, for the
identical reason PR 2 already documented for `fs:`/`s3://`: each node's
own marker check runs independently at **its own** startup, before it
binds a single listener, so a misconfigured node simply never joins the
replica set that store's objects flow through — the marker mismatch
refusal texts are byte-identical to PR 2's own (below), since both paths
call the identical `verify_or_init_segment_store_marker`.

### Marker semantics — the identical function, two more directories

No new refusal text, and no new marker mechanism: `local_cluster_store`
calls the same `verify_or_init_segment_store_marker`/
`EncryptedSegmentStore::open` PR 2 already built, against `<node
dir>/segments`/`<node dir>/backups` — two more directories the existing
three-way decision table (PR 2's own, reproduced in this file above)
already covers correctly, with no changes to that function itself. One
structural property worth naming plainly, since it shapes how the
mismatch scenarios below had to be tested in isolation: `<node
dir>/segments`/`<node dir>/backups` are **siblings** of `<node
dir>/internal` (the directory PR 1's own `Disk`-seam marker check
scans — `Node::bind`'s `ProdEnv::bind_with_tls_and_key(.., dir.join(
"internal"), ..)` call), not the same directory, and `Disk::list()`'s own
non-recursive listing never descends into a sibling subdirectory. The two
seams therefore genuinely operate independently on disk, even though a
real operator's `--encryption-key` covers both at once — a directory that
is fresh from PR 1's own `Disk`-seam point of view (nothing under
`<node dir>/internal` yet) can still hold pre-existing, differently-keyed
or plaintext content under `<node dir>/segments`/`<node dir>/backups`,
and vice versa. This is what lets the loud-refusal tests below prove the
NEW `LocalSegmentStore` marker check specifically, rather than only ever
observing PR 1's own pre-existing `Disk`-seam refusal (which — in the
common, non-contrived case of one node's directory that has always been
managed as a whole — usually fires first regardless, since a node that
has ever written any WAL/engine content already carries PR 1's own
marker the instant a key is first configured).

### Tests

`crates/animusd/tests/encryption_at_rest_default_cluster_store_e2e.rs`
(`ProdEnv`, real sockets/disk, mirroring `encryption_at_rest_segment_
store_e2e.rs`'s own shape for the `fs:` backup store, generalized to the
**default** store and to both the segment AND backup halves at once):

- A 2-node cluster, **every store left at its default** (no
  `--segment-store`/`--backup-store` at all), started with
  `--encryption-key`: a streamed table's `PutItem` seals a shard object
  under `<node dir>/segments` on both replicas; `CreateBackup` issued
  against node 0 converges to `AVAILABLE`; `RestoreTableFromBackup`
  issued against **node 1** — which never captured the backup itself —
  succeeds and reads back the exact backup-time value, proving the
  cluster-wide key lets a peer decrypt what it never wrote (the identical
  property PR 2's own `fs:` e2e proves, now for the default store). The
  raw bytes under every node's own `segments`/`backups` directories are
  grepped for the plaintext value at three points (after the seal, after
  the backup, after the restore) and never contain it.
- **Both loud-refusal directions**, each isolated from PR 1's own
  `Disk`-seam marker per the "Marker semantics" section above (a
  hand-constructed target node directory carrying only the cluster
  store's own `segments` subdirectory content — never a top-level
  marker or WAL/engine files — so PR 1's own check takes its
  fresh-directory branch regardless, and the assertion is genuinely on
  the NEW check): a node started with no key against an already-marked
  local `segments` directory is refused with `"segment store is
  encrypted (found .animus_segment_store_encryption_marker) but no
  --encryption-key was given — refusing to start"`; a node started with
  a key against a `segments` directory already holding real (unmarked)
  plaintext objects is refused with `"--encryption-key was given but
  this segment store already holds unencrypted objects (no .
  animus_segment_store_encryption_marker marker) — refusing to
  start"`. Both texts are byte-identical to PR 2's own table, confirmed
  by construction (no new error text was written).

**Not attempted, stated plainly**: an `ANIMUS_SEGMENT_STORE_ENCRYPTED_
SEEDS` corpus cell in `crates/animus-test/tests/
segment_store_encrypted_fault_corpus.rs` was considered per this ADR's
own follow-up task and declined — that file's own harness is single-node
and has no `Network`/placement/serving-task wiring at all (it drives
`EncryptedSegmentStore` directly over a bare `SimSegmentStore`, exactly
the shape its own module doc states), so hosting a genuine
`ClusterSegmentStore` there would mean building materially new
multi-node plumbing, not a cheap addition to the existing fixture — the
identical "real regression risk, out of proportion to what's needed"
reasoning that file's own module doc already gives for not converting
the backup/PITR/export-import domain corpora. A cheaper future home
exists and is worth naming: `crates/animus-cp-data/tests/
cluster_segment_store.rs` already carries a `ClusterSegmentStore<SimEnv,
S>`-generic-shaped multi-node harness (`build_cluster`, `StaticPlacementView`,
a serving task per node) that could plausibly host `ClusterSegmentStore<
SimEnv, EncryptedSegmentStore<SimSegmentStore, SimEnv>>` at comparatively
low cost — named here as a candidate follow-up, not attempted in this
change.

### Gates

`cargo fmt --all --check`; `cargo clippy -p animus-env -p animus-cp-data
-p animusd --all-targets --all-features -- -D warnings` (zero warnings —
`animus-cp-data` needed no code change at all, only the clippy run to
confirm it); `cargo test -p animus-env`; `cargo test -p animus-cp-data
--test sharedwal_fault_corpus` and `--test cluster_segment_store` (both
green, unmodified — `ClusterSegmentStore` itself is untouched); `cargo
test -p animusd --test encryption_at_rest_default_cluster_store_e2e
--test encryption_at_rest_segment_store_e2e --test encryption_at_rest_e2e
--test streams_e2e --test dynamo_backup --test stream_janitor` (all
green); `cargo test -p animusd --lib` (201 passed, 0 failed — the
`SimEnv`-driven `ClientCtx` harnesses, `sim_cluster*` corpora, and every
other in-crate suite, none of which construct `SegmentStoreHandle::
Cluster`/`BackupStoreHandle::Cluster` and so are unaffected by the type
change, confirmed rather than assumed). `Cargo.lock` unchanged (no new
dependency) — `cargo deny check` not required.

## As-built: `--encryption-key` reach on `join`/`data --seed`/`control` (2026-09-07, issue #676)

`--encryption-key PATH` was accepted only by `--config FILE --node I` and
`--cluster N` (PR 1) — `animusd control`, `animusd join`, and `animusd
data --seed` had no CLI flag for it at all, a documented reach gap named
alongside several other per-node flags with the same shape at the time
(`--tls-*`'s own precedent). Closed:

- **`join`/`data --seed`**: both already build their own `RoleAddrs` ad hoc
  (no config file on either path — the identical shape `--tls-*` already
  has there), so the fix is purely a CLI-parser addition: `main.rs`'s
  `run_join`/`run_data`'s `--seed` branch now parse `--encryption-key
  PATH` and set `RoleAddrs::encryption_key_path` directly, with no "set
  both ways" conflict to check (there is no second source on this path).
  `Node::bind`/`Node::bind_data` already read that field unconditionally
  (PR 1's own mechanism) — no `lib.rs` change was needed for this half at
  all, only the CLI plumbing.
- **`animusd control`**: gained `--encryption-key PATH`, merged onto
  `config.nodes[index]` via `apply_encryption_key_flag` — the identical
  per-node "flag and config both set it is a hard error" contract
  `--config`/`--node`'s own combined-mode route already uses (the two
  share the same helper function). `Node::bind_control` already loads
  `RoleAddrs::encryption_key_path` into the system-keyspace engine's own
  `ProdEnv::bind_with_tls_and_key` call (PR 1's own mechanism, unchanged) —
  a control-only node's system-keyspace engine was always encryptable via
  a config file's own `nodes[index].encryption_key_path` field; the gap
  closed here is purely the CLI flag's own reach, not the mechanism.

**`--cluster-control`+`--cluster-data` is unchanged, deliberately** — it
still rejects `--encryption-key` outright (a loud `Err`), the same posture
`--tls-*` already has on that in-process dev-only path: no per-node config
entries exist there to apply the flag to, and silently downgrading a
requested-encryption cluster to plaintext is a worse failure mode than an
explicit rejection. `animusd data --config` also remains a gap — no CLI
flag of its own yet (a config file's own `nodes[index].encryption_key_path`
field still works there directly, unchanged).

Regression: `crates/animusd/tests/join_data_seed_settings_reach.rs::
join_threads_encryption_key` (mirrors `encryption_at_rest_e2e.rs`'s own
plaintext-absence proof, scoped to the joined node's own directory —
the base cluster stays unencrypted throughout, proving the key is
genuinely per-node on this path, not cluster-wide); a parser-level unit
test (`crates/animusd/src/main.rs`'s `tests` module,
`run_control_parses_encryption_key_flag`) proves `control`'s parser
recognizes the flag without needing a real bind. See
`crates/animusd/CLAUDE.md`'s own `--encryption-key` CLI-reference entry
for the current, complete per-entry-point enumeration.
