# Pin the bytes with a fixture, not the doc comment (stored-item tombstone)

`animus-item/src/stored.rs` documented a tombstone as `{"tombstone": true}`.
Writing the ADR 0073 P1-A `stored-item` fixture tests showed the real v1 bytes
are the bare JSON string `"tombstone"`: serde serializes a unit variant of an
externally tagged enum as a string, not an object. The prose had been wrong
since the codec was written, and a version sniff built from the prose
("untagged v1 starts with `{`") would have misclassified every tombstone.

Lesson: when designing a sniff/dispatch over an untagged serde format, derive
it from the bytes the current writer actually emits (a checked-in fixture plus
an inline exact-bytes assertion for small forms), never from a doc comment.
Unit variants serialize as strings; newtype/struct variants as objects.
