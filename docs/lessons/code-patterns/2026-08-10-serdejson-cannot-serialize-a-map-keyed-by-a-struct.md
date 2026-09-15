# `serde_json` cannot serialize a map keyed by a struct

**`serde_json` cannot serialize a map keyed by a struct** — it fails at
*runtime*, not compile time (`expect("...serializes")` panics). A
`BTreeMap<Timestamp, _>` (or any non-string/non-integer key) in a `Serialize`
type must ride as a `Vec<(K, V)>` instead. Bit when adding a WAL `Snapshot`
record carrying `BTreeMap<TxnId, _>` (animus-consensus); integer-keyed maps
(`BTreeMap<u64, _>`) are fine (stringified), struct-keyed are not.
