# A marker key that must live *inside* an existing `StorageScope` (not engine-global) needs a different disjointness proof than "reserve a name no user schema may claim"

**A marker key that must live *inside* an existing `StorageScope` (not
engine-global) needs a different disjointness proof than "reserve a
name no user schema may claim"** (ADR 0018 §2/PR3, the txn record). The
range-seal/read-ceiling markers (`seal.rs`/`ceiling.rs`) prove
disjointness from every table's keys by living **outside** every scope,
under the control plane's `RESERVED_NAMESPACE` — a trick that only works
because no user table can ever be *named* that reservation. A txn
record can't use that trick: it has to be an ordinary in-scope logical
key of one specific tablet (so it replicates through that tablet's own
Raft log, ships with `engine_image`, and moves with a split/merge like
real data), and there is no analogous "reserved partition key" a table's
own row keys could be barred from — a client can pick *any* bytes for
both the partition key and the row key. The fix was to find a
**structural** invariant of the key-escaping scheme itself rather than a
registry: `animus_tablet::escape`'s encoding can only ever start a
real key's post-token suffix with `[0x00, 0x00]` (empty partition key)
or `[0x00, 0x01, ..]` (a partition key starting with a literal `0x00`
byte) — no partition key, however chosen, can make `escape(pk)`'s first
two bytes be `[0x00, X]` for `X` outside `{0x00, 0x01}`. Picking `X =
0x02` as the marker's own lead byte therefore makes it **provably**
disjoint from *every* real key sharing that token, regardless of what
the arbitrary, client-controlled row-key suffix contains — a stronger,
narrower guarantee than "no collision in practice," derived from the
encoding's own termination rule instead of a naming convention. General
lesson: when a marker must sit inside a scope whose key contents are
fully attacker/client-controlled, look for a byte-position where the
*encoding itself* (not a value convention) constrains what's possible —
a length-prefix boundary, an escape terminator, a fixed-width prefix —
and prove disjointness there; a bare "pick an unlikely-looking prefix"
approach is exactly the mistake the seal marker's own history already
contains one instance of (its retired `[0x00, 0x00]` draft, see
`seal.rs`'s doc) and would have been easy to repeat here in a
differently-shaped way. See `animus-cp-data/src/txn.rs`'s module doc for
the full proof and `tests::record_key_never_collides_with_any_escaped_pk_
plus_rk` for the case-by-case regression.
