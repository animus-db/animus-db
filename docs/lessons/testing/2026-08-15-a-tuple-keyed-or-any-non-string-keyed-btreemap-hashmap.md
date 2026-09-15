# A tuple-keyed (or any non-string-keyed) `BTreeMap`/`HashMap` field on a type that derives plain `Serialize`/`Deserialize` and gets JSON-encoded fails only at runtime, and only once the map is actually non-empty

**A tuple-keyed (or any non-string-keyed) `BTreeMap`/`HashMap` field on a
type that derives plain `Serialize`/`Deserialize` and gets JSON-encoded
fails only at runtime, and only once the map is actually non-empty**:
`serde_json`'s `MapKeySerializer` rejects any non-string map key
(`Error("key must be a string")`), but an *empty* map serializes fine (no
keys to reject), and `cargo build`/`clippy`/`fmt` all see a perfectly
ordinary `#[derive(Serialize, Deserialize)]` and say nothing. This let
`animus_control::Metadata::stream_shards: BTreeMap<(TabletId, u64),
StreamShardRow>` (ADR 0042/0043's segment catalog) ship, merge, and pass
every gate untouched for an entire round of streams work, because every
existing test that round-tripped a whole `Metadata` through JSON
(`meta::tests::metadata_round_trips_with_the_remove_member_variant_in_
scope` and its siblings) happened to leave `stream_shards` empty. The bug
was live on `main` from the moment the field was added, waiting for the
first real seal anywhere in a running cluster to blank `animusd`'s whole
`GET /admin/status` (the handler swallowed the encode error into
`Value::Null`) and panic the serving connection for any wire caller of
`ClientResponse::Status`/`WatchMetadata`'s full-clone fallback
(`write_frame` `.expect()`s the encode). The generalizable rule: a "does
this type round-trip through JSON" test must populate *every* collection
field it owns, not just exercise one representative command path; an
empty collection cannot exercise a map-key encoding rule at all, no
matter how many other fields the test fills in. The fix
(`#[serde(with = "stream_shards_codec")]`, encoding the map as a flat
`Vec<{tablet, epoch, ...row fields}>` instead) keeps the in-memory
`BTreeMap` untouched, so every `.get`/`.insert`/`.range` call site in
`animus-control` is unaffected. See `crates/animus-control/src/meta.rs`'s
`stream_shards` field doc and `crates/animus-control/CLAUDE.md`'s own
entry. (2026-08-14.)
