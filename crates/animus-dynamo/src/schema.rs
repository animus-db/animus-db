//! The bridge between the DynamoDB key model and the control plane's
//! **replicated table-schema catalog** (ADR 0013, consuming the substrate of
//! ADR 0006's per-table schema).
//!
//! `animus-dynamo`'s own [`TableSchema`](crate::TableSchema) is the DynamoDB key
//! shape: a partition (hash) key and an optional sort (range) key. The control
//! plane's [`TableSchema`](animus_control::TableSchema) is the broader,
//! adapter-shared shape: a partition key, an *ordered list* of clustering keys,
//! and typed [`ColumnDef`](animus_control::ColumnDef)s. This module is the
//! **pure, deterministic** translation between the two, so a `CreateTable` can be
//! proposed into the replicated catalog and subsequent ops can resolve their key
//! attributes back out of it.
//!
//! ## The mapping
//!
//! - A simple table → control `partition_key` + no clustering keys.
//! - A composite table → control `partition_key` + one clustering key (the sort
//!   key).
//! - Each key attribute is recorded as a typed `ColumnDef`. DynamoDB's key
//!   `AttributeType` is `S`/`N`/`B`; we carry that as the matching `ColumnType`
//!   ([`String`](animus_control::ColumnType::String) /
//!   [`Number`](animus_control::ColumnType::Number) /
//!   [`Binary`](animus_control::ColumnType::Binary)). Non-key attributes are
//!   schemaless in DynamoDB, so the catalog records *only* the key columns — that
//!   is all key resolution needs.
//!
//! Going the other way, [`to_dynamo`] reads the partition key and the first
//! clustering key (DynamoDB has at most one sort key) back out, ignoring any
//! extra clustering columns a non-DynamoDB writer (CQL) might have declared — so
//! the two adapters can coexist in one catalog without the DynamoDB edge choking
//! on a CQL table it does not own.

use animus_control::{ColumnDef, ColumnType, TableSchema as ControlSchema};

use crate::TableSchema;

/// The DynamoDB key `AttributeType` an attribute was declared with, mapped onto a
/// control-plane [`ColumnType`]. Only the three scalar key families are valid
/// DynamoDB key types; anything else is rejected by the wire decoder before it
/// reaches here, so we default an unknown to `String` (the most permissive).
#[must_use]
pub fn column_type_for(attribute_type: Option<&str>) -> ColumnType {
    match attribute_type {
        Some("N") => ColumnType::Number,
        Some("B") => ColumnType::Binary,
        // "S" and the default.
        _ => ColumnType::String,
    }
}

/// Translate a DynamoDB [`TableSchema`] (+ optional per-key `AttributeType`s,
/// keyed by attribute name) into the control plane's [`ControlSchema`] for
/// proposal into the replicated catalog.
///
/// `key_types` supplies the declared `AttributeType` for the partition/sort key
/// attributes (from `AttributeDefinitions`); a missing entry defaults to
/// `String`. The resulting control schema lists exactly the key columns.
#[must_use]
pub fn to_control(schema: &TableSchema, key_types: &[(String, String)]) -> ControlSchema {
    let ty_of = |name: &str| {
        column_type_for(
            key_types
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, t)| t.as_str()),
        )
    };
    let mut columns = vec![ColumnDef::new(
        schema.partition_key.clone(),
        ty_of(&schema.partition_key),
    )];
    let mut clustering_keys = Vec::new();
    if let Some(sk) = &schema.sort_key {
        columns.push(ColumnDef::new(sk.clone(), ty_of(sk)));
        clustering_keys.push(sk.clone());
    }
    ControlSchema::with_columns(schema.partition_key.clone(), clustering_keys, columns)
}

/// Recover the DynamoDB key shape from a control-plane [`ControlSchema`]: the
/// partition key, plus the first clustering key as the DynamoDB sort key (DynamoDB
/// has at most one). Extra clustering columns — which a CQL `CREATE TABLE` may
/// have declared in the same shared catalog — are ignored, so the DynamoDB edge
/// resolves keys for its own tables and reports a CQL-only table's extra
/// clustering columns as simply absent from the DynamoDB view.
#[must_use]
pub fn to_dynamo(schema: &ControlSchema) -> TableSchema {
    TableSchema {
        partition_key: schema.partition_key.clone(),
        sort_key: schema.clustering_keys.first().cloned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_round_trips() {
        let dynamo = TableSchema::simple("id");
        let control = to_control(&dynamo, &[("id".into(), "S".into())]);
        assert_eq!(control.partition_key, "id");
        assert!(control.clustering_keys.is_empty());
        assert_eq!(control.column("id").unwrap().ty, ColumnType::String);
        assert_eq!(to_dynamo(&control), dynamo);
    }

    #[test]
    fn composite_round_trips_with_types() {
        let dynamo = TableSchema::composite("pk", "sk");
        let control = to_control(
            &dynamo,
            &[("pk".into(), "S".into()), ("sk".into(), "N".into())],
        );
        assert_eq!(control.clustering_keys, vec!["sk".to_string()]);
        assert_eq!(control.column("sk").unwrap().ty, ColumnType::Number);
        assert!(control.validate().is_ok());
        assert_eq!(to_dynamo(&control), dynamo);
    }

    #[test]
    fn missing_attribute_type_defaults_to_string() {
        let dynamo = TableSchema::simple("id");
        let control = to_control(&dynamo, &[]);
        assert_eq!(control.column("id").unwrap().ty, ColumnType::String);
    }

    #[test]
    fn extra_clustering_columns_are_dropped_for_dynamo() {
        // A CQL table with two clustering columns is seen by the DynamoDB edge as
        // a composite table on the first clustering column.
        let control = ControlSchema::with_columns(
            "pk",
            vec!["c1".into(), "c2".into()],
            vec![
                ColumnDef::new("pk", ColumnType::String),
                ColumnDef::new("c1", ColumnType::String),
                ColumnDef::new("c2", ColumnType::String),
            ],
        );
        assert_eq!(to_dynamo(&control), TableSchema::composite("pk", "c1"));
    }
}
