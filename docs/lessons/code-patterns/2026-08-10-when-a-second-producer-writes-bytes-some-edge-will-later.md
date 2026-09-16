# When a second producer writes bytes some edge will later decode, call the edge's own encoder — never re-serialize "the same shape" by hand — and regression-test by reading back through the real consumer, not the storage view.

**When a second producer writes bytes some edge will later decode, call the
edge's own encoder — never re-serialize "the same shape" by hand — and
regression-test by reading back through the real consumer, not the storage
view.** Making the admin bulk seeder write DynamoDB-compatible items, the
key was correctly built via the shared `dynamo::item_key`, but the value was
hand-rolled as `serde_json::to_vec(&item)` — which *looks* identical to what
`PutItem` stores, except the edge actually wraps every stored value in a
one-level envelope (`wire::encode_stored_item`'s `item` vs `tombstone`
variants, the `DeleteItem` sentinel), so every seeded row decoded as
"corrupt stored item". Nothing at compile time connects the two sites; the
storage-scan assertion (bytes landed, keys well-formed) passed fine. It was
caught only because the same PR added a `GetItem`-through-the-DynamoDB-edge
readback assertion. The two halves of the lesson reinforce each other: the
hand-rolled serialization is the *bug class*, and the read-back-through-the-
consumer test is the *only gate that catches it* — a byte-producer PR
without one proves layout, not compatibility. (`animusd`
`admin.rs::action_data_seed`, `dynamo.rs::item_key`,
`animus-dynamo` `wire::encode_stored_item`;
`admin_endpoint.rs::admin_seed_writes_synthetic_keys`.)
