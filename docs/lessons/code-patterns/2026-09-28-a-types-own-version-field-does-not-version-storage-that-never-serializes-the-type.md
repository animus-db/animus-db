# A type's own version field does not version storage that never serializes the type

**What happened.** ADR 0073 Phase 0 workstream B gave `Metadata` a
top-level `"v"` JSON field (`meta::METADATA_VERSION`), following the ADR's
`serde_json` "Phase 0 conventions" shape — the same treatment every other
`serde_json`-encoded format in the inventory (`ClusterConfig`, the backup
manifest, …) was getting. The obvious next assumption is that this also
versions `animus-control`'s system-keyspace mirror (`mirror.rs`/`syskv.rs`,
ADR 0038): after all, that mirror exists to durably hold `Metadata`, so
surely a version bump to `Metadata`'s own shape is visible there too.

It isn't. `Metadata` is `DRIVER_APPLIED` (ADR 0038's central fact): the
async apply task, not the sync Raft core, owns the real, mutable
`Metadata`, and the *only* things that ever get durably written are the
per-key system-keyspace rows `mirror::apply_and_derive_mirror` derives from
each applied `MetaCommand` — never a serialized `Metadata` value itself. A
real `Metadata` value is only ever serialized in two places in this
codebase, and neither is the production durability path: the WAL's
`Snapshot` record's `metadata` field, which is permanently
`Metadata::default()` and never read back (`persist.rs`'s own doc says so
plainly), and the toy non-`DRIVER_APPLIED` test state machine this crate's
`raft.rs` genericity proof exercises. So `Metadata::version` riding through
its `Serialize`/`Deserialize` impl tells the system-keyspace engine
*nothing* about its own on-disk layout — the field exists, and is
faithfully populated on every value, but the one storage layer that
actually needs versioning (a restart's `rebuild_metadata_from_engine` scan,
which has to distinguish "no format tag yet, pre-baseline" from "a version
this build doesn't know how to read") never sees it at all.

**The general shape.** A version field lives on a *type*; a version
*guarantee* is about a *storage layer*. Those coincide only when the
storage layer's actual write path serializes that type directly. The
moment a storage layer is populated by *deriving* writes from a type
(mirroring, projecting, denormalizing — any "watch this type change, write
something else out" pattern) rather than serializing the type wholesale,
the type's own version field stops being a signal that storage layer can
read. This is easy to miss specifically because the two usually *do*
coincide (most `serde_json`-backed storage in this codebase really is "one
top-level struct, serialized directly"), so the pattern that breaks it — a
`DRIVER_APPLIED` state machine whose real durability lives in a derived
per-key mirror, not a blob — is the surprising case, not the common one.

**What to do.**

- **Before assuming a type's own version field versions everything that
  eventually holds its data, trace the actual write path to disk/storage
  for the specific format you're trying to protect.** "This engine holds
  `Metadata`'s data" is not the same claim as "this engine serializes
  `Metadata`." If the write path derives per-field/per-key writes instead
  of serializing the whole value, that storage layer needs its own,
  independent version signal — a row/field that rides the *actual* writes,
  not a hitchhiker on a type that never crosses that boundary.
- **When a `DRIVER_APPLIED`-shaped design (a sync core buffering effects for
  an async apply task to write out) already exists, assume any "add a
  version field to the state machine type" task has a matching "and does
  the real storage layer need its own" follow-up question**, and check it
  explicitly rather than assuming the type-level field covers both. Here
  the fix was `mirror::SYSKV_FORMAT_VERSION_COUNTER` — an ordinary
  `EntityKind::Counter` row, written unconditionally on every durable
  apply-task batch alongside the pre-existing `_applied_index` watermark,
  checked by `rebuild_metadata_from_engine` before trusting a populated
  keyspace — a second, small, independent mechanism rather than a
  strengthening of the first.
- **Keep the two signals honestly separate in naming and behavior, even
  though they answer "the same question" at a glance.** `Metadata::version`
  and `mirror::SYSKV_MIRROR_VERSION` version genuinely different things
  (a JSON shape vs. an on-disk key layout) that happen to currently both be
  `1` and both reset together under a Phase 0-style baseline — a future
  change to one (say, a new `Metadata` field with no key-layout
  consequence) should not need to touch the other, and vice versa.
