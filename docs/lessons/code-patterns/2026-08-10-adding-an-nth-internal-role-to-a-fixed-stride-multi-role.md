# Adding an Nth internal role to a fixed-stride multi-role node is a wide but mechanical ripple — change the stride, every literal, and the arity together.

**Adding an Nth internal role to a fixed-stride multi-role node is a wide but
mechanical ripple — change the stride, every literal, and the arity together.**
`animusd` packs each node's roles into consecutive ports (`base + stride*i`); the
CP `raftkv` role bumped the stride 6→7 and touched every `RoleAddrs` literal
(config gen + 5 test sites), `peer_book`, `Node::bind`'s arity, the `[ProdEnv; N]`
shutdown array, and the conventional id base (`300+i`). A `#[serde(default)]` on
the new addr field keeps *older configs* loading, but struct **literals** still
need the field — so the compiler walks you through the sites; expect it and do
them in one pass.
