# Prefer a live read of the durable layer over observation-built in-memory state.

**Prefer a live read of the durable layer over observation-built in-memory
state.** The DynamoDB edge once tracked written item keys in-memory to fake a
range scan; that set is lost on restart and stale on a follower that never saw a
write. Replacing it with the data plane's native quorum range scan
(`DataClient::scan`, reading live storage in key order) made `Query`/`Scan`
correct after a restart — and the *regression that proves it* is a scan **after a
node restart wipes the registry** (`animusd/tests/dynamo_schema.rs`), not just a
same-process scan. When you delete a derived cache, test the path that the cache
used to mask.
