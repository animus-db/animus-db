# A `SimCluster` pre-bind dial needs a THROWAWAY bootstrap identity, distinct from the real minted one — the relay's own outbound origin is what the control leader's detector attributes a heartbeat to (ADR 0061 rung M, C-13 PR 2)

Building `SimCluster::join_via_seed`'s own discovery+claim phase, the
natural-looking shortcut is one `SimEnv`/`SimRelayClient` pair, built up
front and keyed by the joiner's own eventual node index, used for both
sourcing `NodeId::mint`'s seeded `Rng` draw AND sending the discovery/
claim relay calls — then reused for the real per-node assembly
(`heartbeat_loop`, `ClientCtx`, the reconciler) once the claim resolves.
This is unsound: the minted `NodeId` isn't known until AFTER the mint
resolves, so whatever env sourced that draw is necessarily keyed to a
DIFFERENT id than the one `MetaCommand::RegisterNode` ends up claiming. If
that same (wrongly-keyed) env is then reused to spawn `heartbeat_loop`,
every heartbeat it sends carries `env.node_id()` as its own origin — never
the id that was actually registered — so the control leader's own failure
detector tracks liveness for an id nobody registered, while the genuinely
registered id never receives a single real heartbeat and is never
promoted, forever (the "declared-but-never-booted" case `animus_control::
node::detect_loop`'s own doc already names, reached here by a fixture bug
rather than a real deployment one).

Fixed by using a THROWAWAY bootstrap identity purely as the pre-bind
entropy source and outbound relay origin (`nid(self.nodes)` — always one
index past the last real node at the moment it's drawn, so it can never
collide with a past or future real node id in this fixture), discarding
it once mint-and-claim resolves, and building a genuinely fresh env/relay
under the CLAIMED identity for everything downstream. This mirrors, one
level down in a simulator, the exact shape production's own `animus_env::
prod::PreBindRng` already has at the real CLI boundary: pre-bind entropy
is deliberately disconnected from the eventual bound identity, precisely
so a mint failure or retry never contaminates what gets bound. The general
form: whenever a fixture (or production code) needs to draw randomness or
originate network traffic *before* the identity that randomness will help
select is known, the env/transport doing the drawing/originating must be
provably discardable — reusing it "since it's already there" silently
ties a later mechanism's own identity attribution to the wrong source.
