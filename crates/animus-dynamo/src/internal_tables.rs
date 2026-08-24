//! Reserved internal tables (ADR 0018's 2026-08-24 `ClientRequestToken`
//! amendment).
//!
//! An "internal table" is an **ordinary** catalog table — a real schema, real
//! tablets, TTL-reaper eligibility, everything a user table gets — that must
//! nonetheless be invisible and unreachable to any DynamoDB client:
//! `ListTables` never lists it, the Data Console never shows it, and every
//! client-facing data/DDL wire operation treats its name as if it did not
//! exist. [`is_internal_table_name`] is the single predicate every one of
//! those guards checks — a future internal table joins this same set instead
//! of growing a second, parallel check somewhere else.

use crate::index::INDEX_TABLE_SEPARATOR;

/// The `TransactWriteItems` `ClientRequestToken` idempotency-record table
/// (ADR 0018's 2026-08-24 amendment): one row per outstanding/recently-seen
/// token, `pk` = the token, TTL attribute `expires_at` — see
/// `animusd::dynamo::run_transact`'s doc for the record shape and the
/// preflight/outcome protocol built on it.
///
/// **Why this name, not the `$`-prefixed hidden-table convention a
/// materialized GSI/LSI uses** ([`crate::index::index_table_name`]): `$` is
/// rejected by `animus_control::meta::MetaCommand::CreateTableSchema`'s
/// apply arm (a hidden index table never gets a catalog schema entry of its
/// own to begin with), and the ADR 0051 TTL reaper requires **both** a
/// `table_ttl` entry **and** a `table_schema` entry to reap a table — so a
/// `$`-named table could never be TTL-reaped even if the schema guard were
/// relaxed. `__animus_txn_idempotency` is an ordinary schema-registered
/// table name that happens to pass both existing guards: it contains no
/// `$` ([`INDEX_TABLE_SEPARATOR`]), and `animus_control::syskv::
/// is_reserved_name` only tests a *different*, longer prefix
/// (`__animus_system`), so this name collides with neither.
pub const TXN_IDEMPOTENCY_TABLE: &str = "__animus_txn_idempotency";

/// Whether `name` names a reserved internal table (see the module doc):
/// invisible to `ListTables`, the Data Console, and every client-facing
/// data/DDL wire operation.
///
/// A future second internal table extends this match arm rather than
/// callers growing their own per-table check.
#[must_use]
pub fn is_internal_table_name(name: &str) -> bool {
    debug_assert!(
        !TXN_IDEMPOTENCY_TABLE.contains(INDEX_TABLE_SEPARATOR),
        "an internal table name must stay outside the hidden-index-table namespace"
    );
    name == TXN_IDEMPOTENCY_TABLE
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_named_table_is_internal() {
        assert!(is_internal_table_name(TXN_IDEMPOTENCY_TABLE));
        assert!(!is_internal_table_name("users"));
        // A prefix match must not be enough — unlike `syskv::is_reserved_name`'s
        // deliberate prefix semantics, this is an exact-name check.
        assert!(!is_internal_table_name("__animus_txn_idempotency_extra"));
        assert!(!is_internal_table_name(""));
    }

    #[test]
    fn the_internal_table_name_is_never_dollar_named() {
        // Sanity-checks the module doc's claim directly: this name must never
        // collide with the hidden-GSI/LSI-table convention, or it could never
        // pass `CreateTableSchema`'s `$`-rejection guard.
        assert!(!TXN_IDEMPOTENCY_TABLE.contains(INDEX_TABLE_SEPARATOR));
    }
}
