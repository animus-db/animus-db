//! The one split-inheritance combinator (ADR 0046 principle 3): **a split is
//! a log cut, and every consumer offset crossing it must be inherited from a
//! basis frozen at the cut — never re-derived live from the parent's later
//! state.** [`effective`] is the shared, generic form of that rule: a
//! tablet's own value, if it has one, always wins; otherwise the value falls
//! back to whatever was frozen for it at split time. Nothing more — the
//! call site still owns *what* "own" and "frozen" mean for its own offset
//! convention (an HLC watermark, a raw cursor, or anything else).
//!
//! `Metadata::effective_stream_shard_watermark` (`animus-control`'s
//! `meta.rs`) is the first caller, ported to this combinator as a one-line
//! wrapper around its own two lookups — see that method's own doc for the
//! live-derivation bug (a parent's later seal retroactively raising a
//! child's inherited watermark) this rule exists to close.

/// The split-inheritance combinator: `own` if the tablet already has a value
/// of its own, else `frozen_basis`'s value — never a live re-derivation from
/// the parent's *current* state, since that state can change after the
/// split (ADR 0046 principle 3).
///
/// `own` is the tablet's own value (e.g. its own chain's watermark);
/// `frozen_basis` is the value captured once, at split time, from whatever
/// the parent's own effective value was at that moment.
#[must_use]
pub fn effective<T: Clone>(own: Option<T>, frozen_basis: Option<&T>) -> Option<T> {
    own.or_else(|| frozen_basis.cloned())
}

#[cfg(test)]
mod tests {
    use super::effective;

    #[test]
    fn own_value_wins_over_frozen_basis() {
        assert_eq!(effective(Some(5), Some(&1)), Some(5));
    }

    #[test]
    fn frozen_basis_used_when_own_is_absent() {
        assert_eq!(effective(None, Some(&1)), Some(1));
    }

    #[test]
    fn none_when_both_absent() {
        assert_eq!(effective::<u64>(None, None), None);
    }
}
