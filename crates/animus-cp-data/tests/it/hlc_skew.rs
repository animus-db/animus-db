//! ADR 0018 §2: an HLC's causality guarantee must survive per-node clock
//! skew. This is the property the whole HLC design exists for — physical
//! time alone (Spanner's TrueTime) is unavailable under ADR 0003's
//! determinism mandate, so causality has to come from the logical component
//! tolerating a node whose clock reads wrong.
//!
//! Lives in `animus-cp-data` (not `animus-sim`, which cannot depend on it, or
//! `animus-sim`'s own test suite, which cannot see `animus_cp_data::hlc`):
//! this crate already dev-depends on both `animus_cp_data::hlc` and
//! `animus_sim`.

use std::time::Duration;

use animus_cp_data::hlc::Hlc;
use animus_env::{Clock, nid};
use animus_sim::Simulator;

/// Node A's clock reads 50ms *ahead* of the shared simulation timeline; node
/// B is unskewed (reads behind A the whole time). A mints a timestamp off its
/// fast-reading clock; B witnesses it (modelling A's message reaching B) and
/// its own next mint — still sampled off its own behind-reading clock — must
/// strictly exceed A's timestamp. Causality, not physical time, is what
/// orders the two nodes.
#[test]
fn witness_preserves_causality_despite_clock_skew() {
    let sim = Simulator::new(7);
    let node_a = nid(0);
    let node_b = nid(1);

    sim.set_clock_skew_for(node_a.clone(), 50_000_000); // A: +50ms
    // node_b is left at the default skew (zero).

    let env_a = sim.env(node_a.clone());
    let env_b = sim.env(node_b.clone());

    let clock_a = Hlc::new(node_a, Duration::from_millis(500));
    let clock_b = Hlc::new(node_b, Duration::from_millis(500));

    assert!(
        env_a.now().0 > env_b.now().0,
        "sanity: A's skewed clock must genuinely read ahead of B's"
    );

    // A mints off its fast clock.
    let ts_a = clock_a.mint(env_a.now());

    // B witnesses A's timestamp (the message-receipt HLC rule), sampling its
    // own (unskewed, behind) now().
    let witnessed = clock_b.witness(ts_a, env_b.now());
    assert!(
        witnessed > ts_a,
        "witness must exceed the timestamp it just witnessed"
    );

    // B's own next mint, still off its behind-reading clock, must strictly
    // exceed A's mint: this is the causality property clock skew must not
    // break.
    let ts_b_next = clock_b.mint(env_b.now());
    assert!(
        ts_b_next > ts_a,
        "B's next mint must strictly exceed A's timestamp despite B's wall \
         reading being behind A's the whole time"
    );
}
