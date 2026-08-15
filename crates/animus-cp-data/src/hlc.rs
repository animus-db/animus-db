//! Hybrid Logical Clock (HLC) timestamps for cross-tablet transactions (ADR
//! 0018 §2).
//!
//! A transaction commit timestamp is a `(wall_ms, logical)` pair: `wall_ms` is
//! drawn from the `Env` `Clock` seam (virtual under `SimEnv`, so it stays
//! deterministic per ADR 0003 — there is no TrueTime and no other special
//! clock), and `logical` breaks ties / preserves causality when physical time
//! does not advance between two events on the same node, or when a node
//! witnesses a remote timestamp whose wall time it hasn't caught up to yet.
//!
//! This module is **pure and I/O-free by design**: [`Hlc`] never touches an
//! `Env` or a wall clock itself. Both [`Hlc::mint`] and [`Hlc::witness`] take
//! the caller-sampled [`Nanos`] as a parameter, so the clock's own logic is
//! unit-testable with no simulator at all, and the one real time read per call
//! stays at the call site (`env.now()`) where ADR 0003 requires it.

use std::sync::Mutex;
use std::time::Duration;

use animus_env::{Nanos, NodeId};
use serde::{Deserialize, Serialize};

/// Number of bits reserved for the `logical` component when an [`HlcTimestamp`]
/// is packed into a storage-engine `u64` MVCC version (ADR 0018 §2:
/// `version = (wall_ms << LOGICAL_BITS) | logical`, with no node-id bits — see
/// [`pack`]). Budgeted at 20 bits: a wall-clock millisecond would need to see
/// over a million HLC events for a single node to exhaust it, at which point
/// [`Hlc::mint`]/[`Hlc::witness`] fall back to bumping `wall_ms` (see their
/// docs) rather than overflowing into the wall bits.
pub const LOGICAL_BITS: u32 = 20;

/// A Hybrid Logical Clock timestamp: `(wall_ms, logical)`, ordered
/// lexicographically by field declaration order (the derived [`Ord`] compares
/// `wall_ms` first, then `logical` — exactly the HLC total order).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct HlcTimestamp {
    /// Milliseconds since the `Env` `Clock`'s epoch (virtual time under
    /// `SimEnv`).
    pub wall_ms: u64,
    /// The logical tiebreak component within `wall_ms`.
    pub logical: u32,
}

impl HlcTimestamp {
    /// The zero timestamp — precedes every timestamp any [`Hlc`] mints or
    /// witnesses.
    #[must_use]
    pub fn zero() -> HlcTimestamp {
        HlcTimestamp {
            wall_ms: 0,
            logical: 0,
        }
    }
}

/// Pack an [`HlcTimestamp`] into the storage-engine `u64` MVCC version: ADR
/// 0018 §2 settled the engine version to be the HLC directly, no node-id bits
/// folded in (unlike `animus-consensus::node::mvcc_version`'s `(logical,
/// node)` encoding — string [`NodeId`]s can't be bit-packed post-ADR-0040, and
/// per-key monotonicity across concurrent writers is the transaction layer's
/// job, not this encoding's — see the timestamp-cache / write-conflict-push
/// design in later PRs of ADR 0018).
///
/// # Contract (hard-checked)
///
/// The encoding is injective only for `logical < 2^LOGICAL_BITS` and `wall_ms
/// < 2^(64 - LOGICAL_BITS)`. Outside those bounds two distinct timestamps
/// would silently collapse to the same version and per-key LWW would keep an
/// arbitrary winner — a silent-corruption failure, not a recoverable error —
/// so the guards are hard `assert!`s (they do **not** vanish in release
/// builds, unlike `debug_assert!`). Mirrors the doctrine in
/// `animus-consensus::node::mvcc_version`.
///
/// # Panics
///
/// Panics if `ts.logical >= 2^LOGICAL_BITS` or `ts.wall_ms >= 2^(64 -
/// LOGICAL_BITS)`.
#[must_use]
pub fn pack(ts: HlcTimestamp) -> u64 {
    assert!(
        ts.logical < (1 << LOGICAL_BITS),
        "HLC logical {} exceeds the {LOGICAL_BITS}-bit packed-version field; \
         the (wall_ms, logical) -> u64 encoding would collide",
        ts.logical
    );
    let wall_bits = 64 - LOGICAL_BITS;
    assert!(
        ts.wall_ms < (1 << wall_bits),
        "HLC wall_ms {} exceeds the {wall_bits}-bit packed-version field; \
         the (wall_ms, logical) -> u64 encoding would collide",
        ts.wall_ms
    );
    (ts.wall_ms << LOGICAL_BITS) | u64::from(ts.logical)
}

/// The exact inverse of [`pack`].
#[must_use]
pub fn unpack(v: u64) -> HlcTimestamp {
    let logical_mask = (1u64 << LOGICAL_BITS) - 1;
    HlcTimestamp {
        wall_ms: v >> LOGICAL_BITS,
        logical: (v & logical_mask) as u32,
    }
}

/// Bump `ts`'s `logical` component by one, carrying into `wall_ms` (and
/// resetting `logical` to `0`) on [`LOGICAL_BITS`] overflow — a pure "the
/// next value that strictly exceeds `ts`" step, with no clock state and no
/// [`Nanos`] involved at all.
///
/// Shared by two independent call sites in `animus-cp-data::lib` that each
/// need to strictly exceed a floor **without** going through [`Hlc::witness`]:
/// `next_ceiling_candidate`'s CAS-ratchet bump branch, and `mint_pushed`'s
/// no-witness write-push (ADR 0018 §2 amendment, the `mint_pushed`
/// clock-witnessing-runaway fix). Both avoid `Hlc::witness` for the identical
/// reason — the floor they must exceed is a value **deliberately shifted
/// into the future** (a `ReadCeiling` margin, or the committed ceiling
/// folded into a write's floor), and witnessing a future value would drag
/// the group's own shared clock forward to match it, poisoning every
/// ordinary `mint` right after. This function is the safe alternative: it
/// computes a value that strictly exceeds `ts` as pure arithmetic, leaving
/// `Hlc`'s own persistent `(wall_ms, logical)` state untouched.
#[must_use]
pub fn bump_strictly_above(ts: HlcTimestamp) -> HlcTimestamp {
    let bumped_logical = ts.logical.wrapping_add(1);
    if bumped_logical >= (1 << LOGICAL_BITS) {
        HlcTimestamp {
            wall_ms: ts.wall_ms + 1,
            logical: 0,
        }
    } else {
        HlcTimestamp {
            wall_ms: ts.wall_ms,
            logical: bumped_logical,
        }
    }
}

/// The mutable `(wall_ms, logical)` pair a [`Hlc`] advances.
#[derive(Clone, Copy, Debug)]
struct HlcState {
    wall_ms: u64,
    logical: u32,
}

/// A per-node Hybrid Logical Clock (ADR 0018 §2).
///
/// Interior mutability (a small `Mutex` over the `(wall_ms, logical)` pair) so
/// a shared `&self` can advance the clock, matching the `Rng` seam's
/// interior-mutability convention (`animus_env::Rng`). The node id is carried
/// for debugging/future stamping only — it does **not** enter timestamps or
/// [`pack`]/[`unpack`] (string `NodeId`s can't bit-pack; see
/// `crates/animus-consensus/src/node.rs`'s `mvcc_version` doc for why the old
/// numeric-folding trick died with ADR 0040).
///
/// **Pure-function discipline**: `Hlc` never touches an `Env` or the wall
/// clock. [`mint`](Hlc::mint) and [`witness`](Hlc::witness) both take the
/// caller-sampled `now: Nanos` (from `env.now()`) as a parameter.
pub struct Hlc {
    node: NodeId,
    max_offset: Duration,
    state: Mutex<HlcState>,
}

impl Hlc {
    /// A fresh clock for `node`, starting at [`HlcTimestamp::zero`].
    /// `max_offset` bounds the assumed clock skew across the cluster, consumed
    /// by [`uncertainty_upper`](Self::uncertainty_upper).
    #[must_use]
    pub fn new(node: NodeId, max_offset: Duration) -> Hlc {
        Hlc {
            node,
            max_offset,
            state: Mutex::new(HlcState {
                wall_ms: 0,
                logical: 0,
            }),
        }
    }

    /// The node this clock belongs to (debugging/future stamping only — never
    /// part of a minted timestamp).
    #[must_use]
    pub fn node(&self) -> &NodeId {
        &self.node
    }

    /// The configured maximum clock-offset bound.
    #[must_use]
    pub fn max_offset(&self) -> Duration {
        self.max_offset
    }

    /// `now` (nanoseconds) to whole milliseconds, floor-rounded.
    fn now_ms(now: Nanos) -> u64 {
        now.0 / 1_000_000
    }

    /// If `logical` has exceeded the [`LOGICAL_BITS`] packing budget, bump
    /// `wall_ms` by one and reset `logical` to zero. Documented fallback for a
    /// pathologically busy millisecond (over a million HLC events on one
    /// node): the pack budget is load-bearing (see [`pack`]), so overflowing
    /// it silently would be silent MVCC-version collision, not a recoverable
    /// condition — bumping the wall component instead keeps the encoding
    /// injective at the cost of one extra millisecond of ordering skew.
    fn carry_overflow(wall_ms: u64, logical: u32) -> (u64, u32) {
        if logical >= (1 << LOGICAL_BITS) {
            (wall_ms + 1, 0)
        } else {
            (wall_ms, logical)
        }
    }

    /// The HLC send/local-event rule: mint a timestamp strictly greater than
    /// every timestamp this clock has minted or witnessed so far, even if
    /// `now` regresses relative to the last mint (a physical clock reading a
    /// node observed going backwards, or simply staying flat within the same
    /// millisecond).
    ///
    /// `wall = max(now_ms(now), last.wall_ms)`; if `wall == last.wall_ms` then
    /// `logical = last.logical + 1`, else `logical = 0`.
    pub fn mint(&self, now: Nanos) -> HlcTimestamp {
        let mut st = self.state.lock().expect("Hlc mutex poisoned");
        let now_ms = Self::now_ms(now);
        let wall = now_ms.max(st.wall_ms);
        let logical = if wall == st.wall_ms {
            st.logical + 1
        } else {
            0
        };
        let (wall, logical) = Self::carry_overflow(wall, logical);
        st.wall_ms = wall;
        st.logical = logical;
        HlcTimestamp {
            wall_ms: wall,
            logical,
        }
    }

    /// The HLC receive rule (Kulkarni et al.): fold a remote timestamp into
    /// this clock so the result is strictly greater than both `remote` and
    /// every timestamp this clock has minted or witnessed so far — the
    /// causality-preservation property cross-tablet transactions need.
    ///
    /// `wall = max(now_ms(now), last.wall_ms, remote.wall_ms)`; then:
    /// - if `wall == last.wall_ms == remote.wall_ms`: `logical =
    ///   max(last.logical, remote.logical) + 1`
    /// - else if `wall == last.wall_ms`: `logical = last.logical + 1`
    /// - else if `wall == remote.wall_ms`: `logical = remote.logical + 1`
    /// - else: `logical = 0`
    pub fn witness(&self, remote: HlcTimestamp, now: Nanos) -> HlcTimestamp {
        let mut st = self.state.lock().expect("Hlc mutex poisoned");
        let now_ms = Self::now_ms(now);
        let wall = now_ms.max(st.wall_ms).max(remote.wall_ms);
        let logical = if wall == st.wall_ms && wall == remote.wall_ms {
            st.logical.max(remote.logical) + 1
        } else if wall == st.wall_ms {
            st.logical + 1
        } else if wall == remote.wall_ms {
            remote.logical + 1
        } else {
            0
        };
        let (wall, logical) = Self::carry_overflow(wall, logical);
        st.wall_ms = wall;
        st.logical = logical;
        HlcTimestamp {
            wall_ms: wall,
            logical,
        }
    }

    /// The read-uncertainty ceiling: `ts.wall_ms + max_offset` (milliseconds),
    /// logical `0`. A later PR (ADR 0018 §2's uncertainty-interval read
    /// restart) consumes this as the upper bound a read may need to wait out
    /// or restart past; kept trivial here on purpose.
    #[must_use]
    pub fn uncertainty_upper(&self, ts: HlcTimestamp) -> HlcTimestamp {
        let max_offset_ms = (self.max_offset.as_millis()).min(u128::from(u64::MAX)) as u64;
        HlcTimestamp {
            wall_ms: ts.wall_ms.saturating_add(max_offset_ms),
            logical: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use animus_env::nid;

    #[test]
    fn mint_is_monotonic_under_regressing_now() {
        let clock = Hlc::new(nid(0), Duration::from_millis(500));
        let a = clock.mint(Nanos(10_000_000)); // wall_ms = 10
        let b = clock.mint(Nanos(1_000_000)); // now regresses to wall_ms = 1
        assert!(b > a, "mint must stay monotonic when now regresses");
        assert_eq!(b.wall_ms, a.wall_ms, "wall must not regress");
        assert_eq!(
            b.logical,
            a.logical + 1,
            "logical must tick within the flat wall"
        );
    }

    #[test]
    fn mint_logical_resets_when_physical_advances() {
        let clock = Hlc::new(nid(0), Duration::from_millis(500));
        let a = clock.mint(Nanos(5_000_000)); // wall_ms = 5
        assert_eq!(a.logical, 0);
        let b = clock.mint(Nanos(5_000_000)); // flat wall -> logical ticks
        assert_eq!(b.logical, 1);
        let c = clock.mint(Nanos(6_000_000)); // physical advances -> reset
        assert_eq!(c.wall_ms, 6);
        assert_eq!(c.logical, 0);
    }

    #[test]
    fn witness_never_regresses_and_exceeds_the_remote() {
        let clock = Hlc::new(nid(0), Duration::from_millis(500));
        let local = clock.mint(Nanos(3_000_000)); // wall_ms = 3
        let remote = HlcTimestamp {
            wall_ms: 10,
            logical: 4,
        };
        let witnessed = clock.witness(remote, Nanos(1_000_000));
        assert!(witnessed > local, "witness must exceed the prior local ts");
        assert!(
            witnessed > remote,
            "witness must exceed the witnessed remote"
        );
    }

    #[test]
    fn witness_branch_both_walls_equal() {
        let clock = Hlc::new(nid(0), Duration::from_millis(500));
        let _ = clock.mint(Nanos(5_000_000)); // local wall_ms = 5, logical = 0
        let remote = HlcTimestamp {
            wall_ms: 5,
            logical: 7,
        };
        // now_ms also 5, so wall == last.wall_ms == remote.wall_ms.
        let w = clock.witness(remote, Nanos(5_000_000));
        assert_eq!(w.wall_ms, 5);
        assert_eq!(w.logical, 8); // max(0, 7) + 1
    }

    #[test]
    fn witness_branch_local_wall_wins() {
        let clock = Hlc::new(nid(0), Duration::from_millis(500));
        let local = clock.mint(Nanos(9_000_000)); // local wall_ms = 9
        assert_eq!(local.logical, 0);
        let remote = HlcTimestamp {
            wall_ms: 3,
            logical: 99,
        };
        let w = clock.witness(remote, Nanos(1_000_000)); // now behind both
        assert_eq!(w.wall_ms, 9, "local wall dominates");
        assert_eq!(w.logical, local.logical + 1);
    }

    #[test]
    fn witness_branch_remote_wall_wins() {
        let clock = Hlc::new(nid(0), Duration::from_millis(500));
        let _ = clock.mint(Nanos(2_000_000)); // local wall_ms = 2
        let remote = HlcTimestamp {
            wall_ms: 8,
            logical: 3,
        };
        let w = clock.witness(remote, Nanos(1_000_000)); // now behind both
        assert_eq!(w.wall_ms, 8, "remote wall dominates");
        assert_eq!(w.logical, remote.logical + 1);
    }

    #[test]
    fn witness_branch_now_exceeds_both() {
        let clock = Hlc::new(nid(0), Duration::from_millis(500));
        let _ = clock.mint(Nanos(2_000_000)); // local wall_ms = 2
        let remote = HlcTimestamp {
            wall_ms: 3,
            logical: 9,
        };
        let w = clock.witness(remote, Nanos(50_000_000)); // now = 50ms, ahead of both
        assert_eq!(w.wall_ms, 50);
        assert_eq!(w.logical, 0);
    }

    #[test]
    fn logical_overflow_bumps_wall() {
        let clock = Hlc::new(nid(0), Duration::from_millis(500));
        // Prime the clock at wall_ms = 0, logical = (2^LOGICAL_BITS - 1) via a
        // single witness call (remote.logical one below the budget, so the
        // witness rule's own `+1` lands exactly at the top of the budget with
        // no overflow yet), rather than looping a million mints.
        let remote = HlcTimestamp {
            wall_ms: 0,
            logical: (1 << LOGICAL_BITS) - 2,
        };
        let primed = clock.witness(remote, Nanos(0));
        assert_eq!(primed.wall_ms, 0);
        assert_eq!(primed.logical, (1 << LOGICAL_BITS) - 1);

        // One more tick at the same wall would overflow the logical budget;
        // the clock must instead bump wall_ms and reset logical.
        let next = clock.mint(Nanos(0));
        assert_eq!(next.wall_ms, 1, "overflow must bump wall_ms");
        assert_eq!(next.logical, 0, "overflow must reset logical to 0");
    }

    #[test]
    fn pack_unpack_round_trips() {
        let cases = [
            HlcTimestamp::zero(),
            HlcTimestamp {
                wall_ms: 12_345,
                logical: 0,
            },
            HlcTimestamp {
                wall_ms: 1,
                logical: (1 << LOGICAL_BITS) - 1,
            },
            HlcTimestamp {
                wall_ms: (1u64 << (64 - LOGICAL_BITS)) - 1,
                logical: 42,
            },
        ];
        for ts in cases {
            assert_eq!(unpack(pack(ts)), ts, "pack/unpack must round-trip {ts:?}");
        }
    }

    #[test]
    fn pack_is_strictly_monotone_in_wall_then_logical() {
        let a = HlcTimestamp {
            wall_ms: 5,
            logical: (1 << LOGICAL_BITS) - 1,
        };
        let b = HlcTimestamp {
            wall_ms: 6,
            logical: 0,
        };
        assert!(
            a < b,
            "test fixture must itself be ordered wall-then-logical"
        );
        assert!(
            pack(a) < pack(b),
            "pack must preserve order across a wall/logical boundary"
        );

        let c = HlcTimestamp {
            wall_ms: 6,
            logical: 1,
        };
        assert!(pack(b) < pack(c));
    }

    #[test]
    #[should_panic(expected = "exceeds the 20-bit packed-version field")]
    fn pack_panics_on_logical_overflow() {
        let _ = pack(HlcTimestamp {
            wall_ms: 0,
            logical: 1 << LOGICAL_BITS,
        });
    }

    #[test]
    #[should_panic(expected = "exceeds the 44-bit packed-version field")]
    fn pack_panics_on_wall_ms_overflow() {
        let _ = pack(HlcTimestamp {
            wall_ms: 1 << (64 - LOGICAL_BITS),
            logical: 0,
        });
    }

    #[test]
    fn uncertainty_upper_adds_max_offset() {
        let clock = Hlc::new(nid(0), Duration::from_millis(250));
        let ts = HlcTimestamp {
            wall_ms: 1_000,
            logical: 3,
        };
        let upper = clock.uncertainty_upper(ts);
        assert_eq!(upper.wall_ms, 1_250);
        assert_eq!(upper.logical, 0);
    }

    #[test]
    fn bump_strictly_above_ticks_logical_without_touching_wall() {
        let ts = HlcTimestamp {
            wall_ms: 12,
            logical: 3,
        };
        let bumped = bump_strictly_above(ts);
        assert!(bumped > ts);
        assert_eq!(bumped.wall_ms, 12);
        assert_eq!(bumped.logical, 4);
    }

    #[test]
    fn bump_strictly_above_carries_into_wall_on_logical_overflow() {
        let ts = HlcTimestamp {
            wall_ms: 7,
            logical: (1 << LOGICAL_BITS) - 1,
        };
        let bumped = bump_strictly_above(ts);
        assert!(bumped > ts);
        assert_eq!(bumped.wall_ms, 8);
        assert_eq!(bumped.logical, 0);
    }

    #[test]
    fn node_and_max_offset_accessors() {
        let clock = Hlc::new(nid(7), Duration::from_millis(500));
        assert_eq!(clock.node(), &nid(7));
        assert_eq!(clock.max_offset(), Duration::from_millis(500));
    }
}
