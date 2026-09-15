# A test corpus's own workload mix can reproduce the exact bug class an invariant is built to catch, from the harness alone, with the mechanism under test fully correct.

**A test corpus's own workload mix can reproduce the exact bug class an
invariant is built to catch, from the harness alone, with the mechanism
under test fully correct.** Adding a `kind_consistency` check to
`animus-test/tests/txn_serializable.rs` (every committed transaction's
derived `KIND_LSI` row must equal its own base row) failed immediately,
consistently, for 3 of 9 keys — looking exactly like "a committed
transaction's kind write silently lost." The actual cause: the corpus's
write-only and read-modify-write transaction shapes both append to the
*same* client-owned keyspace, and only the write-only shape's own writes
had been given a kind payload — an RMW-authored append correctly updated
the base row but (by design, at the time) carried no kind payload at
all, leaving the derived row one commit stale. Diagnosed by reading the
raw stored envelope (tag byte + version) directly off both the base and
kind physical keys — confirming *both* were durably `Committed` (not one
merely inferred at read time from a still-`Pending` intent, which was
the first, wrong hypothesis) but at different versions, proving two
independently-committed writes rather than one delayed resolve. **General
form**: when extending an existing multi-shape workload harness with a
new payload on only one shape, audit every *other* shape that can touch
the same keys — a corpus's own test-design gap reproduces as a false
positive that looks identical to the real bug the invariant exists to
catch, and the fastest way to tell them apart is a raw storage-layer read
that distinguishes "durably committed, wrong content" from "still
pending, inferred at read time." (2026-08-16, `TxnStage` kind-writes
stack PR3.)
