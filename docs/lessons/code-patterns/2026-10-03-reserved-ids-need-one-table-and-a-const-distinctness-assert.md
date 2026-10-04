# Reserved ids minted in different crates need one table and a const distinctness assertion

Issue #1055: `HEARTBEAT_BATCH_STREAM` (animus-cp-data) and `RELAY_STREAM`
(animus-node) were both `u64::MAX - 2`. Each constant's doc listed its
"siblings" from memory, and a later doc even called the sharing deliberate
("they never coexist on one env") - until a SimCluster with heartbeat
batching enabled would put two single-consumer loops on one inbox.

Why it happened: an allocation table duplicated across several docs drifts,
and prose cannot fail a build. Fix shape: ONE table in the lowest crate that
can see every constant (`animus_node::reserved_streams`) with a
`const _: () = assert!(pairwise_distinct(&ALL_RESERVED))`; a new reserved id
must be added to the array, and a collision is then a compile error. Verify
such an assertion by temporarily reintroducing the collision.
