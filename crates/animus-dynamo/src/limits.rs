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
//! - [`MAX_ITEM_SIZE_BYTES`] — from `animus_item` (ADR 0054 step 1).
//! - [`BATCH_WRITE_MAX_ITEMS`], [`BATCH_GET_MAX_KEYS`],
//!   [`TRANSACT_WRITE_MAX_ACTIONS`], [`TRANSACT_GET_MAX_ITEMS`],
//!   [`BATCH_EXECUTE_STATEMENT_MAX_STATEMENTS`],
//!   [`EXECUTE_TRANSACTION_MAX_STATEMENTS`], [`MAX_GSI_PER_TABLE`],
//!   [`MAX_LSI_PER_TABLE`], [`ACCOUNT_MAX_READ_CAPACITY_UNITS`],
//!   [`ACCOUNT_MAX_WRITE_CAPACITY_UNITS`], [`TABLE_MAX_READ_CAPACITY_UNITS`],
//!   [`TABLE_MAX_WRITE_CAPACITY_UNITS`] — from [`crate::wire`].
//! - [`MAX_SIGNIFICANT_DIGITS`] — from `animus_item::numkey`.
//!
//! ## New limits (constants only — see each doc comment for the AWS rule it
//! mirrors; enforcement lands in later layers of this series)

pub use crate::wire::{
    ACCOUNT_MAX_READ_CAPACITY_UNITS, ACCOUNT_MAX_WRITE_CAPACITY_UNITS,
    BATCH_EXECUTE_STATEMENT_MAX_STATEMENTS, BATCH_GET_MAX_KEYS, BATCH_WRITE_MAX_ITEMS,
    EXECUTE_TRANSACTION_MAX_STATEMENTS, MAX_GSI_PER_TABLE, MAX_LSI_PER_TABLE,
    TABLE_MAX_READ_CAPACITY_UNITS, TABLE_MAX_WRITE_CAPACITY_UNITS, TRANSACT_GET_MAX_ITEMS,
    TRANSACT_WRITE_MAX_ACTIONS,
};
pub use animus_item::MAX_ITEM_SIZE_BYTES;
pub use animus_item::numkey::MAX_SIGNIFICANT_DIGITS;

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

/// AWS's limit on the nested (`List`/`Map`) depth of a single item: at most
/// 32 levels.
pub const MAX_NESTING_DEPTH: usize = 32;

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
pub const MAX_QUERY_SCAN_PAGE_BYTES: usize = 1_048_576;

/// AWS's `BatchGetItem` response-size limit: at most 16 MiB of item data
/// returned in one call; any keys past that budget come back in
/// `UnprocessedKeys` instead.
pub const MAX_BATCH_GET_RESPONSE_BYTES: usize = 16_777_216;

/// AWS's `BatchWriteItem` request-size limit: at most 16 MiB total across
/// every request item in the call.
pub const MAX_BATCH_WRITE_REQUEST_BYTES: usize = 16_777_216;

/// AWS's `TransactWriteItems`/`TransactGetItems` request-size limit: at most
/// 4 MiB aggregate across every action/item in the call.
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
