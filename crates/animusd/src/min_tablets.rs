//! Throughput-derived minimum tablet count (ADR 0067, W-08b).
//!
//! DynamoDB itself sizes a table's initial partition count from its
//! provisioned throughput up front: `partitions = ceil(RCU/3000 +
//! WCU/1000)` — its own documented per-partition read/write ceilings. This
//! module is the pure, unit-tested analogue: nothing here touches
//! `Metadata`, the network, disk, or any `Env` — the auto-split loop
//! (`lib.rs`'s fourth trigger arm) is the sole caller, deriving each
//! provisioned table's minimum tablet count once per tick and forking
//! toward it exactly like every other auto-split trigger. See ADR 0067
//! (docs/adr/0067-throughput-derived-minimum-tablet-count.md) for the full
//! design and rationale.

use animus_control::ProvisionedThroughput;
use animus_tablet::{KeyRange, TOKEN_BYTES};

/// A `KeyRange` boundary's leading `TOKEN_BYTES` interpreted as a big-endian
/// token (ADR 0022) — zero-padded on the right when the boundary is shorter
/// than a full token (the whole-keyspace `start = []` case) and truncated
/// when longer (an ordinary split boundary is a real row key, whose own
/// leading `TOKEN_BYTES` **are** its token by construction — anything past
/// that is the escaped partition/sort key, irrelevant here). A pure
/// approximation used only to compare/rank tablets by their rough token
/// width and to synthesize a fresh split key when a tablet has too little
/// data for [`decide::byte_weighted_median`](crate::decide) to use — never
/// a substitute for that function when real data exists.
fn leading_token(bytes: &[u8]) -> u64 {
    let mut buf = [0u8; TOKEN_BYTES];
    let n = bytes.len().min(TOKEN_BYTES);
    buf[..n].copy_from_slice(&bytes[..n]);
    u64::from_be_bytes(buf)
}

/// A tablet's key range as an inclusive-start/exclusive-end pair of tokens,
/// `end` widened to `u64::MAX` (as `u128`, so "one past the top" is
/// representable) when the range is unbounded above.
fn token_bounds(range: &KeyRange) -> (u64, u128) {
    let start = leading_token(&range.start);
    let end = match &range.end {
        Some(e) => leading_token(e) as u128,
        None => u128::from(u64::MAX) + 1,
    };
    (start, end)
}

/// A tablet's approximate width in token space — used only to rank
/// candidates ("pick the widest") never as an exact byte-range measure.
pub(crate) fn token_range_width(range: &KeyRange) -> u128 {
    let (start, end) = token_bounds(range);
    end.saturating_sub(u128::from(start))
}

/// Synthesize a split key for a tablet with too little data for
/// [`decide::byte_weighted_median`](crate::decide) to compute one from —
/// the token exactly halfway across the tablet's own range, as a plain
/// `TOKEN_BYTES`-long key (inherently token-aligned, so it clears
/// `align_split_key`'s own F11 rounding for a streamed table as a no-op).
/// Returns `None` only when the range spans a single token (or less) —
/// the accepted single-token hot-partition limit (ADR 0042 §14 Fork E) —
/// since no interior split point can exist there regardless of data.
pub(crate) fn midpoint_split_key(range: &KeyRange) -> Option<Vec<u8>> {
    let (start, end) = token_bounds(range);
    let start = u128::from(start);
    if end <= start + 1 {
        return None;
    }
    let mid = start + (end - start) / 2;
    // `mid` is strictly between `start` and `end`, both of which fit in a
    // u64 conceptually (end may be `u64::MAX + 1`, but `mid` itself can
    // never reach that since `end - start` divided by 2 keeps it below
    // `end`), so this narrowing is exact.
    let mid = u64::try_from(mid).unwrap_or(u64::MAX);
    Some(mid.to_be_bytes().to_vec())
}

/// The production default per-tablet **read**-capacity-units ceiling —
/// DynamoDB's own documented partition read ceiling. `--tablet-max-read-
/// units N` (or `cluster_settings.tablet_max_read_units`) overrides it; an
/// explicit `0` disables the read dimension of the derived minimum
/// entirely. See [`TabletCapacityCeilings`]'s own doc.
pub(crate) const DEFAULT_TABLET_MAX_READ_UNITS: u64 = 3000;

/// The write-capacity-units sibling of [`DEFAULT_TABLET_MAX_READ_UNITS`] —
/// DynamoDB's own documented partition write ceiling.
pub(crate) const DEFAULT_TABLET_MAX_WRITE_UNITS: u64 = 1000;

/// The cluster-wide per-tablet capacity ceilings [`min_tablets_for`] derives
/// a table's minimum tablet count against — DynamoDB's own partition
/// ceilings (3000 RCU / 1000 WCU) by default, overridable via
/// `--tablet-max-read-units`/`--tablet-max-write-units` (or the matching
/// `cluster_settings` config-file fields). **A `0` in either field means
/// "no ceiling in that dimension"** — that dimension contributes nothing to
/// the derived minimum (an operator may opt a dimension out entirely while
/// keeping the other; both `0` degenerates to a floor of exactly one
/// tablet, i.e. the trigger never fires). Unlike every other auto-split
/// threshold in this crate (`AutoSplitThresholds`, all `Option<u64>` and
/// opt-in), these two are always in effect at their production default —
/// see ADR 0067 Decision 2.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TabletCapacityCeilings {
    pub(crate) max_read_units: u64,
    pub(crate) max_write_units: u64,
}

impl Default for TabletCapacityCeilings {
    fn default() -> Self {
        Self {
            max_read_units: DEFAULT_TABLET_MAX_READ_UNITS,
            max_write_units: DEFAULT_TABLET_MAX_WRITE_UNITS,
        }
    }
}

/// `ceil(numerator / denominator)` in exact integer arithmetic;
/// `denominator == 0` reads as "no ceiling in this dimension" and
/// contributes `0`, never divides by zero.
fn ceil_div_u128(numerator: u128, denominator: u128) -> u128 {
    if denominator == 0 {
        0
    } else {
        numerator.div_ceil(denominator)
    }
}

/// Derive the minimum tablet count a table's provisioned throughput
/// implies under the cluster's configured per-tablet capacity ceilings —
/// DynamoDB's own formula: `max(1, ceil(RCU/max_rcu + WCU/max_wcu))`.
///
/// **Reads and writes are summed, not maxed** — a Raft group's single
/// leader serves both a tablet's read and write load, so a table's read and
/// write pressure genuinely compete for the same per-tablet capacity
/// budget rather than being independent ceilings a tablet could separately
/// max out against (see ADR 0067's Alternatives-considered section for the
/// `max()` shape this rejects, and why).
///
/// **Integer-exact — no floating point, and no separate per-term
/// rounding.** `ceil(a/b) + ceil(c/d)` is not the same value as
/// `ceil(a/b + c/d)` in general (e.g. `a=1500,b=3000,c=500,d=1000`: each
/// term is exactly `0.5`, summing to `1.0` — `ceil` of the sum is `1`, but
/// summing each term's own `ceil` first gives `1 + 1 = 2`). This function
/// combines both terms over a common denominator (`(a·d + c·b) / (b·d)`)
/// before ever rounding, computed in `u128` to stay exact for any `u64`
/// input pair.
///
/// A `0` ceiling in either dimension of `ceilings` removes that dimension
/// from the sum entirely (see [`TabletCapacityCeilings`]'s own doc); `0` in
/// both yields the floor of `1` regardless of `throughput`. **Saturates**
/// (via `u128` intermediate arithmetic, clamped to `u64::MAX` on the way
/// out) rather than overflows for a pathologically large
/// `ProvisionedThroughput` — there is no cap on the *result* itself (ADR
/// 0067 Decision 6: a legitimately huge provisioned value means legitimately
/// many tablets), only defensive arithmetic here so a hostile/malformed
/// input can never panic or wrap.
pub(crate) fn min_tablets_for(
    throughput: &ProvisionedThroughput,
    ceilings: TabletCapacityCeilings,
) -> u64 {
    let rcu = throughput.read_units as u128;
    let wcu = throughput.write_units as u128;
    let max_r = ceilings.max_read_units as u128;
    let max_w = ceilings.max_write_units as u128;

    let derived: u128 = match (max_r == 0, max_w == 0) {
        (true, true) => 0,
        (true, false) => ceil_div_u128(wcu, max_w),
        (false, true) => ceil_div_u128(rcu, max_r),
        (false, false) => {
            // a/b + c/d = (a*d + c*b) / (b*d), summed BEFORE rounding.
            let numerator = rcu
                .saturating_mul(max_w)
                .saturating_add(wcu.saturating_mul(max_r));
            let denominator = max_r.saturating_mul(max_w);
            ceil_div_u128(numerator, denominator)
        }
    };
    derived.max(1).min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn throughput(read_units: u64, write_units: u64) -> ProvisionedThroughput {
        ProvisionedThroughput {
            read_units,
            write_units,
        }
    }

    #[test]
    fn zero_throughput_floors_at_one() {
        let ceilings = TabletCapacityCeilings::default();
        assert_eq!(min_tablets_for(&throughput(0, 0), ceilings), 1);
    }

    #[test]
    fn small_throughput_under_both_ceilings_floors_at_one() {
        let ceilings = TabletCapacityCeilings::default();
        assert_eq!(min_tablets_for(&throughput(100, 50), ceilings), 1);
    }

    #[test]
    fn dynamodb_worked_example_5000_rcu_2000_wcu_is_four() {
        // ceil(5000/3000 + 2000/1000) = ceil(1.667 + 2.0) = ceil(3.667) = 4.
        let ceilings = TabletCapacityCeilings::default();
        assert_eq!(min_tablets_for(&throughput(5000, 2000), ceilings), 4);
    }

    #[test]
    fn summed_terms_round_after_combining_not_before() {
        // Each term alone is exactly 0.5 (would ceil to 1 individually,
        // summing to a wrong answer of 2) but combined they sum to exactly
        // 1.0, which ceils to 1.
        let ceilings = TabletCapacityCeilings {
            max_read_units: 3000,
            max_write_units: 1000,
        };
        assert_eq!(min_tablets_for(&throughput(1500, 500), ceilings), 1);
    }

    #[test]
    fn reads_and_writes_are_summed_not_maxed() {
        // Each dimension alone implies exactly 2 tablets (6000/3000,
        // 2000/1000) — summed (not maxed) they imply 4.
        let ceilings = TabletCapacityCeilings::default();
        assert_eq!(min_tablets_for(&throughput(6000, 2000), ceilings), 4);
    }

    #[test]
    fn zero_read_ceiling_disables_the_read_dimension() {
        let ceilings = TabletCapacityCeilings {
            max_read_units: 0,
            max_write_units: 1000,
        };
        // A huge RCU value contributes nothing; only the write term counts.
        assert_eq!(min_tablets_for(&throughput(1_000_000, 2500), ceilings), 3);
    }

    #[test]
    fn zero_write_ceiling_disables_the_write_dimension() {
        let ceilings = TabletCapacityCeilings {
            max_read_units: 3000,
            max_write_units: 0,
        };
        assert_eq!(min_tablets_for(&throughput(7500, 1_000_000), ceilings), 3);
    }

    #[test]
    fn both_ceilings_zero_is_always_one_regardless_of_throughput() {
        let ceilings = TabletCapacityCeilings {
            max_read_units: 0,
            max_write_units: 0,
        };
        assert_eq!(
            min_tablets_for(&throughput(u64::MAX, u64::MAX), ceilings),
            1
        );
    }

    #[test]
    fn saturates_rather_than_overflows_or_panics() {
        let ceilings = TabletCapacityCeilings {
            max_read_units: 1,
            max_write_units: 1,
        };
        // Deliberately pathological: this would overflow a naive u64
        // multiply (max_r * max_w == 1, fine, but rcu*max_w and wcu*max_r
        // each approach u64::MAX) — must not panic, and must saturate to a
        // huge-but-finite u64 rather than wrapping to a small one.
        let result = min_tablets_for(&throughput(u64::MAX, u64::MAX), ceilings);
        assert!(result > 1, "expected a huge derived minimum, got {result}");
    }

    #[test]
    fn exactly_divisible_needs_no_extra_tablet() {
        let ceilings = TabletCapacityCeilings::default();
        // Exactly 3000/3000 + 1000/1000 = 1.0 + 1.0 = 2.0 -> ceil = 2, not 3.
        assert_eq!(min_tablets_for(&throughput(3000, 1000), ceilings), 2);
    }

    #[test]
    fn default_ceilings_match_dynamodb() {
        let d = TabletCapacityCeilings::default();
        assert_eq!(d.max_read_units, 3000);
        assert_eq!(d.max_write_units, 1000);
    }

    #[test]
    fn midpoint_split_key_of_the_whole_keyspace_is_the_token_midpoint() {
        let range = KeyRange::whole();
        let key = midpoint_split_key(&range).expect("the whole keyspace always has room");
        assert_eq!(key.len(), 8);
        let token = u64::from_be_bytes(key.try_into().unwrap());
        // Halfway across [0, u64::MAX] (inclusive-of-MAX conceptually via
        // the u128 widening) lands just above u64::MAX / 2.
        assert!(
            token > u64::MAX / 4 && token < (u64::MAX / 4) * 3,
            "{token}"
        );
    }

    #[test]
    fn midpoint_split_key_is_strictly_between_start_and_end() {
        let range = KeyRange {
            start: 100u64.to_be_bytes().to_vec(),
            end: Some(200u64.to_be_bytes().to_vec()),
        };
        let key = midpoint_split_key(&range).unwrap();
        assert!(key.as_slice() > range.start.as_slice());
        assert!(key.as_slice() < range.end.as_deref().unwrap());
        let token = u64::from_be_bytes(key.try_into().unwrap());
        assert_eq!(token, 150);
    }

    #[test]
    fn midpoint_split_key_is_none_for_a_single_token_range() {
        let range = KeyRange {
            start: 5u64.to_be_bytes().to_vec(),
            end: Some(6u64.to_be_bytes().to_vec()),
        };
        assert_eq!(midpoint_split_key(&range), None);
    }

    #[test]
    fn midpoint_split_key_handles_a_zero_width_range_as_none() {
        let range = KeyRange {
            start: 5u64.to_be_bytes().to_vec(),
            end: Some(5u64.to_be_bytes().to_vec()),
        };
        assert_eq!(midpoint_split_key(&range), None);
    }

    #[test]
    fn token_range_width_of_a_bounded_range_matches_the_token_gap() {
        let range = KeyRange {
            start: 100u64.to_be_bytes().to_vec(),
            end: Some(300u64.to_be_bytes().to_vec()),
        };
        assert_eq!(token_range_width(&range), 200);
    }

    #[test]
    fn token_range_width_ranks_the_whole_keyspace_widest() {
        let whole = token_range_width(&KeyRange::whole());
        let narrow = token_range_width(&KeyRange {
            start: 100u64.to_be_bytes().to_vec(),
            end: Some(200u64.to_be_bytes().to_vec()),
        });
        assert!(whole > narrow);
    }
}
