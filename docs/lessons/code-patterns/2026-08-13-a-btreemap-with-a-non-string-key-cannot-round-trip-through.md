# A `BTreeMap` with a non-string key cannot round-trip through `serde_json` at all — and ADR 0003 actively steers you into writing one.

**A `BTreeMap` with a non-string key cannot round-trip through `serde_json`
at all — and ADR 0003 actively steers you into writing one.** The
determinism rule says "no `HashMap` in logic, use `BTreeMap`" (lint-enforced
via `clippy.toml`), and the repo convention says data-plane values
(de)serialize with `serde_json`. For a **byte-keyed** map those two rules
collide: `BTreeMap<Vec<u8>, T>` derives `Serialize` happily, compiles
clean, passes clippy — and then fails at **runtime** with
`Error("key must be a string")`, because a JSON object key must be a
string. Nothing in the type system or the gates catches it; only executing
the encode does. Hit while building ADR 0041's `IndexFootprint`
(`animus-dynamo/src/index.rs`), whose natural shape is "GSI rows keyed by
base sort key". **The fix that keeps both rules**: a `Vec<Struct>` held
**sorted by the key field**, with the ordering invariant maintained by the
single mutator (`set_item`) and lookup by `binary_search_by`. That is
deterministic by construction (the encoding cannot depend on insertion
order), JSON-native, and no slower at the sizes involved. **The
generalizable practice**: every new durable/serialized type gets an
`encode`→`decode` **round-trip unit test** in the same change, not just
tests of its constructors and accessors — a round-trip is the only thing
that executes the serializer, and for `serde_json` the serializer is where
a whole class of key-shape errors lives. A test asserting the *sort
invariant under out-of-order insertion* is worth pairing with it, since
that invariant is now what determinism rests on.
