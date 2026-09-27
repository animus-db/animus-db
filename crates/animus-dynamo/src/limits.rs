//! The one catalogue of every DynamoDB service limit AnimusDB enforces.
//!
//! DynamoDB service limits are AWS-faithful and compiled-in; there is no
//! "unleashed" mode (ADR 0072). This module gathers every such limit in one
//! place — re-exporting the ones that already lived elsewhere in this crate
//! (or in [`animus_item`]) unchanged, and adding the constants for limits
//! this adapter does not yet enforce. **A later layer of this series wires
//! each new constant into real enforcement — an unused constant here is
//! expected for now, not a bug.**
//!
//! ## Existing limits (re-exported, defined and enforced elsewhere)
//!
//! - [`MAX_ITEM_SIZE_BYTES`], [`MAX_NESTING_DEPTH`] — from `animus_item`
//!   (ADR 0054 step 1 / ADR 0072) — the latter's `value_depth`/`item_depth`
//!   also back `animus_item::update::apply_update`'s post-fold re-check, the
//!   identical relationship `MAX_ITEM_SIZE_BYTES`/`item_size` already have.
//! - [`BATCH_WRITE_MAX_ITEMS`], [`BATCH_GET_MAX_KEYS`],
//!   [`TRANSACT_WRITE_MAX_ACTIONS`], [`TRANSACT_GET_MAX_ITEMS`],
//!   [`BATCH_EXECUTE_STATEMENT_MAX_STATEMENTS`],
//!   [`EXECUTE_TRANSACTION_MAX_STATEMENTS`], [`MAX_GSI_PER_TABLE`],
//!   [`MAX_LSI_PER_TABLE`], [`ACCOUNT_MAX_READ_CAPACITY_UNITS`],
//!   [`ACCOUNT_MAX_WRITE_CAPACITY_UNITS`], [`TABLE_MAX_READ_CAPACITY_UNITS`],
//!   [`TABLE_MAX_WRITE_CAPACITY_UNITS`] — from [`crate::wire`].
//! - [`MAX_SIGNIFICANT_DIGITS`] — from `animus_item::numkey`.
//!
//! ## New limits (see each doc comment for the AWS rule it mirrors and,
//! once wired, the enforcement site)
//!
//! - [`MAX_QUERY_SCAN_PAGE_BYTES`] is enforced (ADR 0072 layer 3) —
//!   `animusd::dynamo`'s shared `Query`/`Scan` pagination loops
//!   (`paginated_table_examine` and its two siblings) track it directly;
//!   see that crate's own doc comment for the accounting/boundary rule.
//! - [`MAX_BATCH_GET_RESPONSE_BYTES`], [`MAX_BATCH_WRITE_REQUEST_BYTES`],
//!   and [`MAX_TRANSACT_BYTES`] are enforced (ADR 0072 layer 4) —
//!   `crate::wire::decode_batch_write`/`decode_transact_write` at decode
//!   time for the two request-size caps, `animusd::dynamo::run_transact_get`
//!   against the fetched result for `TransactGetItems`' response-size cap,
//!   and `animusd::dynamo`'s `Operation::BatchGetItem` arm (not an error —
//!   pages via `UnprocessedKeys`) for `BatchGetItem`'s own response-size
//!   cap. See each constant's own doc for the exact accounting/boundary
//!   rule and, for the two currently-unreachable-via-the-wire caps, why.
//! - [`MAX_PARTITION_KEY_BYTES`], [`MAX_SORT_KEY_BYTES`],
//!   [`MAX_KEY_ATTRIBUTE_NAME_CHARS`], [`MAX_ATTRIBUTE_NAME_BYTES`],
//!   [`MAX_NESTING_DEPTH`], [`MAX_EXPRESSION_BYTES`],
//!   [`MIN_TABLE_NAME_CHARS`]/[`MAX_TABLE_NAME_CHARS`]/
//!   [`is_valid_table_or_index_name`] are enforced by a sibling layer of
//!   this same series (key/attribute/name/expression validation) — still
//!   catalogue-only from this module's own point of view.

pub use crate::wire::{
    ACCOUNT_MAX_READ_CAPACITY_UNITS, ACCOUNT_MAX_WRITE_CAPACITY_UNITS,
    BATCH_EXECUTE_STATEMENT_MAX_STATEMENTS, BATCH_GET_MAX_KEYS, BATCH_WRITE_MAX_ITEMS,
    EXECUTE_TRANSACTION_MAX_STATEMENTS, MAX_GSI_PER_TABLE, MAX_LSI_PER_TABLE,
    TABLE_MAX_READ_CAPACITY_UNITS, TABLE_MAX_WRITE_CAPACITY_UNITS, TRANSACT_GET_MAX_ITEMS,
    TRANSACT_WRITE_MAX_ACTIONS,
};
pub use animus_item::numkey::MAX_SIGNIFICANT_DIGITS;
pub use animus_item::{MAX_ITEM_SIZE_BYTES, MAX_NESTING_DEPTH};

/// AWS's partition-key attribute-value size limit: at most 2048 bytes
/// (UTF-8 for `S`, raw bytes for `B`, decimal text for `N`), for a base
/// table's own partition key and for a GSI/LSI's partition key attribute.
pub const MAX_PARTITION_KEY_BYTES: usize = 2048;

/// AWS's sort-key attribute-value size limit: at most 1024 bytes, for a base
/// table's own sort key and for a GSI/LSI's sort key attribute.
pub const MAX_SORT_KEY_BYTES: usize = 1024;

/// AWS's limit on the *name* of an attribute used as a table or index key
/// (partition or sort): at most 255 characters.
pub const MAX_KEY_ATTRIBUTE_NAME_CHARS: usize = 255;

/// AWS's limit on any attribute name: at most 65,536 bytes (64 KB) of UTF-8.
/// The minimum is 1 — an empty attribute name is never valid.
pub const MAX_ATTRIBUTE_NAME_BYTES: usize = 65_536;

/// AWS's limit on the length of a single expression string —
/// `ConditionExpression`, `UpdateExpression`, `ProjectionExpression`,
/// `FilterExpression`, or `KeyConditionExpression` — at most 4096 bytes,
/// checked independently for each expression present on a request.
pub const MAX_EXPRESSION_BYTES: usize = 4096;

/// AWS's minimum table/index name length: at least 3 characters.
pub const MIN_TABLE_NAME_CHARS: usize = 3;

/// AWS's maximum table/index name length: at most 255 characters.
pub const MAX_TABLE_NAME_CHARS: usize = 255;

/// Validates a table or index name against AWS's own rule: length in
/// `[`[`MIN_TABLE_NAME_CHARS`]`, `[`MAX_TABLE_NAME_CHARS`]`]`, and every
/// character one of `[a-zA-Z0-9_.-]`. The charset is pure ASCII, so a byte
/// count is the same as a character count here.
pub fn is_valid_table_or_index_name(name: &str) -> bool {
    let len = name.len();
    if !(MIN_TABLE_NAME_CHARS..=MAX_TABLE_NAME_CHARS).contains(&len) {
        return false;
    }
    name.bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'.' || b == b'-')
}

/// AWS's `Query`/`Scan` per-page evaluation limit: at most 1 MiB of item
/// data evaluated per page (**before** any `FilterExpression` is applied),
/// after which the call returns a `LastEvaluatedKey` rather than continuing.
///
/// **Enforced** (ADR 0072 layer 3) by `animusd::dynamo`'s shared `Query`/
/// `Scan` pagination loops (`paginated_table_examine`/
/// `paginated_kind_examine`/`paginated_kind_examine_one`) — at the
/// coordinator, composing with `Limit`; see those functions' own doc
/// comment for the exact accounting and boundary rule.
pub const MAX_QUERY_SCAN_PAGE_BYTES: usize = 1_048_576;

/// AWS's `BatchGetItem` response-size limit: at most 16 MiB of item data
/// returned in one call; any keys past that budget come back in
/// `UnprocessedKeys` instead.
///
/// **Enforced** (ADR 0072 layer 4) — not an error. `animusd::dynamo`'s
/// `Operation::BatchGetItem` arm accumulates `item_size` over fetched items
/// in request order and stops including items once the next one would push
/// the running total over this cap; every key not included — fetched-but-cut
/// or not-yet-fetched — goes to `UnprocessedKeys`, exactly like a per-key
/// throttle refusal. That arm fetches one key at a time (never a concurrent
/// fan-out), so once the budget is spent the remaining keys are never even
/// read.
pub const MAX_BATCH_GET_RESPONSE_BYTES: usize = 16_777_216;

/// AWS's `BatchWriteItem` request-size limit: at most 16 MiB total across
/// every request item in the call.
///
/// **Enforced** (ADR 0072 layer 4) by `crate::wire::decode_batch_write`
/// (via `check_batch_write_bytes`) at decode time — summing `item_size`
/// over every `PutRequest` item and `DeleteRequest` key. **Currently
/// unreachable via the wire**: `BATCH_WRITE_MAX_ITEMS` (25) ×
/// `MAX_ITEM_SIZE_BYTES` (400 KB) tops out at 10 MB, under this cap —
/// enforced anyway so the catalogue is complete and the check is in place
/// the moment either of those two caps is ever raised; see
/// `check_batch_write_bytes`'s own doc and `wire.rs`'s `byte_cap_tests`
/// module.
pub const MAX_BATCH_WRITE_REQUEST_BYTES: usize = 16_777_216;

/// AWS's `TransactWriteItems`/`TransactGetItems` request-size limit: at most
/// 4 MiB aggregate across every action/item in the call.
///
/// **Enforced** (ADR 0072 layer 4), but at two different sites since AWS's
/// own rule lands on two different sides of the two operations:
/// - `TransactWriteItems`: `crate::wire::decode_transact_write` checks it
///   at decode time against the *request* — summing `item_size` over each
///   action's `Put` item, or `Key` for `Update`/`Delete`/`ConditionCheck`.
///   Reachable: `TRANSACT_WRITE_MAX_ACTIONS` (100) × `MAX_ITEM_SIZE_BYTES`
///   (400 KB) is 40 MB, well past 4 MiB — eleven max-size `Put`s alone
///   exceed it.
/// - `TransactGetItems`: its *request* (at most 100 keys of a few KB each)
///   can never reach 4 MiB, so `animusd::dynamo::run_transact_get` checks
///   it against the **fetched result** instead, after its own quiescent
///   read — a transaction has no partial result, so the whole call fails.
pub const MAX_TRANSACT_BYTES: usize = 4_194_304;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_too_short() {
        assert!(!is_valid_table_or_index_name(""));
        assert!(!is_valid_table_or_index_name("ab"));
    }

    #[test]
    fn accepts_minimum_length() {
        assert!(is_valid_table_or_index_name("abc"));
    }

    #[test]
    fn accepts_maximum_length() {
        let name = "a".repeat(MAX_TABLE_NAME_CHARS);
        assert!(is_valid_table_or_index_name(&name));
    }

    #[test]
    fn rejects_over_maximum_length() {
        let name = "a".repeat(MAX_TABLE_NAME_CHARS + 1);
        assert!(!is_valid_table_or_index_name(&name));
    }

    #[test]
    fn rejects_invalid_characters() {
        assert!(!is_valid_table_or_index_name("bad name"));
        assert!(!is_valid_table_or_index_name("bad$name"));
        assert!(!is_valid_table_or_index_name("bad/name"));
    }

    #[test]
    fn accepts_full_charset() {
        assert!(is_valid_table_or_index_name("Valid_Table-Name.123"));
    }
}
