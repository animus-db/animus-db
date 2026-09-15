# An engine precondition that was "impossible for a well-behaved caller" stops being impossible the moment the caller is a client — match the error variant, don't `.expect()` it (2026-08-19).

**An engine precondition that was "impossible for a well-behaved caller"
stops being impossible the moment the caller is a client — match the error
variant, don't `.expect()` it (2026-08-19).** `animusd`'s admin
`GET /admin/system-table` builds its scan lower bound by concatenating the
client's `after` cursor (unvalidated base64url) with a `0x00` suffix, then
scanned with `.expect("system-keyspace engine scan")`. Both `LsmEngine::scan`
and `MemoryEngine::scan` return `StorageError::InvalidRange` when
`start > end`, and the reserved namespace's end bound is a short ASCII-only
value — so any cursor decoding above it (e.g. base64url of `0xFF`) panicked
the request task, while the sibling `kind` parameter three lines up already
returned a clean 400 for the same class of hand-crafted input. Note the
contrast that makes this a pattern and not a one-off: `RaftKvNode`'s own
`local_scan` deliberately does `.scan(..).ok()`, swallowing `InvalidRange` as
an empty result — the codebase had already decided how to treat this error at
its other call sites, and this one endpoint hadn't followed. **The rule**:
wherever a `StorageEngine` scan bound is *derived from* bytes that crossed a
wire edge, treat `InvalidRange` as client input reaching a precondition, and
match the specific variant so a genuine backend fault still fails loudly
rather than being blanket-caught into a 400.
(`crates/animusd/src/admin.rs::system_table`.)
