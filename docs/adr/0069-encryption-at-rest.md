# ADR 0069 — Encryption at rest: AEAD `Disk`-seam wrapper, per-node key file

- **Status:** Accepted — implemented (S-03 PR 1 of 3: key loading + the
  `Disk`-seam wrapper for WAL/engine files. PR 2 of 3 — `SegmentStore` —
  also implemented, see the 2026-09-06 "As-built: PR 2" amendment below.
  PR 3 — operator key-secret mount — pending.)
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
  per-node) key; the default replicated `cluster` store still writes
  plaintext, a stated scope cut. PR 3 (operator key-secret mount) is not
  yet implemented — the Kubernetes operator has no key-distribution
  mechanism yet.

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
store stays plaintext on disk. A future PR closing this gap is a named,
tracked follow-up, not assumed.

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
