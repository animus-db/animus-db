//! Query sort-key conditions and a `ConditionExpression` subset for conditional
//! writes (ADR 0006). Both are **pure, deterministic** predicates over the
//! crate's [`AttributeValue`] / [`Item`] model — no I/O, no storage.
//!
//! ## Sort-key conditions
//!
//! A `Query` always pins the partition key to a single value (`pk = ..`) and may
//! additionally narrow the sort key with one of: equality (`sk = v`), a range
//! (`sk BETWEEN lo AND hi`), or a prefix (`begins_with(sk, p)`). Each maps to a
//! byte range over a partition's contiguous keyspace plus an exact predicate, so
//! a caller can scan the partition and keep the matching rows.
//!
//! ## Condition expressions
//!
//! A minimal `ConditionExpression` subset for `PutItem` / `DeleteItem`:
//! `attribute_not_exists(attr)`, `attribute_exists(attr)`, and attribute
//! equality (`attr = :v`). These are evaluated against the *current* stored item
//! (or its absence) before a write commits — the DynamoDB conditional-write
//! contract. The fuller expression grammar (AND/OR/NOT, comparators, functions)
//! is deferred.

use crate::{AttributeValue, Item};

/// The sort-key half of a `Query` key condition. The partition key is always an
/// equality (handled by the caller); this narrows within that partition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SortKeyCondition {
    /// `sk = value`.
    Equals(AttributeValue),
    /// `sk BETWEEN lo AND hi` (inclusive on both ends).
    Between(AttributeValue, AttributeValue),
    /// `begins_with(sk, prefix)` — only meaningful for string/binary sort keys.
    BeginsWith(AttributeValue),
}

impl SortKeyCondition {
    /// Whether an item's sort-key value satisfies this condition. Comparison is
    /// over the same key-bytes used for storage ordering (so it agrees with the
    /// scan range below).
    #[must_use]
    pub fn matches(&self, sort_value: &AttributeValue) -> bool {
        let v = sort_value.key_bytes();
        match self {
            SortKeyCondition::Equals(target) => v == target.key_bytes(),
            SortKeyCondition::Between(lo, hi) => {
                let (lo, hi) = (lo.key_bytes(), hi.key_bytes());
                v >= lo && v <= hi
            }
            SortKeyCondition::BeginsWith(prefix) => v.starts_with(&prefix.key_bytes()),
        }
    }
}

/// A minimal DynamoDB `ConditionExpression` subset, evaluated against the
/// item currently stored at the target key (`None` when absent).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConditionExpression {
    /// `attribute_not_exists(attr)` — true iff the item is absent or lacks `attr`.
    AttributeNotExists(String),
    /// `attribute_exists(attr)` — true iff the item is present and has `attr`.
    AttributeExists(String),
    /// `attr = value` — true iff the item is present and `attr` equals `value`.
    Equals(String, AttributeValue),
}

impl ConditionExpression {
    /// Evaluate the condition against the current item at the key (`None` when
    /// no live item exists). A write proceeds only when this returns `true`.
    #[must_use]
    pub fn evaluate(&self, current: Option<&Item>) -> bool {
        match self {
            ConditionExpression::AttributeNotExists(attr) => {
                current.is_none_or(|item| !item.contains_key(attr))
            }
            ConditionExpression::AttributeExists(attr) => {
                current.is_some_and(|item| item.contains_key(attr))
            }
            ConditionExpression::Equals(attr, value) => {
                current.is_some_and(|item| item.get(attr) == Some(value))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> AttributeValue {
        AttributeValue::S(v.into())
    }

    #[test]
    fn equals_matches_only_exact() {
        let cond = SortKeyCondition::Equals(s("b"));
        assert!(cond.matches(&s("b")));
        assert!(!cond.matches(&s("a")));
        assert!(!cond.matches(&s("bb")));
    }

    #[test]
    fn between_is_inclusive() {
        let cond = SortKeyCondition::Between(s("b"), s("d"));
        for hit in ["b", "c", "d"] {
            assert!(cond.matches(&s(hit)), "{hit}");
        }
        for miss in ["a", "e"] {
            assert!(!cond.matches(&s(miss)), "{miss}");
        }
    }

    #[test]
    fn begins_with_matches_prefix() {
        let cond = SortKeyCondition::BeginsWith(s("ab"));
        assert!(cond.matches(&s("ab")));
        assert!(cond.matches(&s("abc")));
        assert!(!cond.matches(&s("ac")));
        assert!(!cond.matches(&s("a")));
    }

    #[test]
    fn attribute_not_exists() {
        let cond = ConditionExpression::AttributeNotExists("pk".into());
        assert!(cond.evaluate(None));
        let mut item = Item::new();
        item.insert("other".into(), s("x"));
        assert!(cond.evaluate(Some(&item)));
        item.insert("pk".into(), s("k"));
        assert!(!cond.evaluate(Some(&item)));
    }

    #[test]
    fn attribute_exists_and_equals() {
        let mut item = Item::new();
        item.insert("pk".into(), s("k"));
        item.insert("v".into(), AttributeValue::N("1".into()));
        assert!(ConditionExpression::AttributeExists("pk".into()).evaluate(Some(&item)));
        assert!(!ConditionExpression::AttributeExists("pk".into()).evaluate(None));
        assert!(
            ConditionExpression::Equals("v".into(), AttributeValue::N("1".into()))
                .evaluate(Some(&item))
        );
        assert!(
            !ConditionExpression::Equals("v".into(), AttributeValue::N("2".into()))
                .evaluate(Some(&item))
        );
        assert!(
            !ConditionExpression::Equals("v".into(), AttributeValue::N("1".into())).evaluate(None)
        );
    }
}
