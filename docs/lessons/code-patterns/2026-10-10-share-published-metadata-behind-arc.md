# Publish shared read-mostly state behind an Arc, not a clone-on-read

Issue #1190: every reconciler wake on every node called `effective_metadata()`
(a full `Metadata` clone) and moved the tablet map into `MetadataView`, so a
one-tablet change cost O(tablets x nodes) of copying.

The fix was to publish `Arc<Metadata>` (the apply task replaces the Arc per
apply; the growth-node mirror `take()`s then `Arc::make_mut`s) and let readers
clone the Arc. Two traps: (1) `make_mut` on a value the cache still holds
always clones, so `take()` the Option first; (2) a view type that must stay
decoupled from the owner can wrap the Arc and `Deref` to the inner map
(`host::TabletMap`) so call sites keep compiling. Pin it with an
`Arc::ptr_eq` test on two idle reads. Keep the owned-clone accessor for the
many callers that mutate.
