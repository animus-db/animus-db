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

use animus_control::{
    ColumnDef, ColumnType, IndexDef, IndexKind, IndexProjection as ControlProjection,
    TableSchema as ControlSchema,
};

use crate::TableSchema;
use crate::registry::{GlobalSecondaryIndex, IndexProjection, LocalSecondaryIndex, SecondaryIndex};

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

/// Translate a DynamoDB [`SecondaryIndex`] declaration into the control plane's
/// replicated [`IndexDef`] (ADR 0013), so a `CreateTable`/`CreateGlobalSecondaryIndex`
/// can propose the index *definition* into the replicated catalog.
///
/// A GSI carries its own hash attribute and optional range; an LSI carries an
/// alternate sort attribute and, by the catalog's convention, hashes by
/// `base_partition_key` (DynamoDB LSIs share the base partition key).
#[must_use]
pub fn index_to_control(index: &SecondaryIndex, base_partition_key: &str) -> IndexDef {
    match index {
        SecondaryIndex::Global(g) => IndexDef {
            name: g.name.clone(),
            kind: IndexKind::Global,
            hash_attribute: g.key_attribute.clone(),
            sort_attribute: g.sort_attribute.clone(),
            projection: projection_to_control(&g.projection),
        },
        SecondaryIndex::Local(l) => IndexDef {
            name: l.name.clone(),
            kind: IndexKind::Local,
            hash_attribute: base_partition_key.to_owned(),
            sort_attribute: Some(l.sort_attribute.clone()),
            projection: projection_to_control(&l.projection),
        },
    }
}

/// Recover the DynamoDB [`SecondaryIndex`] declaration from a control-plane
/// [`IndexDef`] read out of the replicated catalog, so the wire edge can rebuild
/// its index-maintenance machinery from cluster-agreed definitions (not local
/// memory).
#[must_use]
pub fn index_to_dynamo(def: &IndexDef) -> SecondaryIndex {
    match def.kind {
        IndexKind::Global => SecondaryIndex::Global(GlobalSecondaryIndex {
            name: def.name.clone(),
            key_attribute: def.hash_attribute.clone(),
            sort_attribute: def.sort_attribute.clone(),
            projection: projection_to_dynamo(&def.projection),
        }),
        // An LSI's hash is the base partition key (carried in `hash_attribute`);
        // the dynamo `LocalSecondaryIndex` is defined by its alternate sort
        // attribute. A control LSI always has a sort attribute (validated), but be
        // defensive and fall back to the hash attribute if it is somehow absent.
        IndexKind::Local => SecondaryIndex::Local(LocalSecondaryIndex {
            name: def.name.clone(),
            sort_attribute: def
                .sort_attribute
                .clone()
                .unwrap_or_else(|| def.hash_attribute.clone()),
            projection: projection_to_dynamo(&def.projection),
        }),
    }
}

/// Translate a list of control-plane [`IndexDef`]s (as read from the replicated
/// catalog) into DynamoDB [`SecondaryIndex`] declarations, preserving order.
#[must_use]
pub fn indexes_to_dynamo(defs: &[IndexDef]) -> Vec<SecondaryIndex> {
    defs.iter().map(index_to_dynamo).collect()
}

fn projection_to_control(p: &IndexProjection) -> ControlProjection {
    match p {
        IndexProjection::All => ControlProjection::All,
        IndexProjection::KeysOnly => ControlProjection::KeysOnly,
        IndexProjection::Include(names) => ControlProjection::Include(names.clone()),
    }
}

fn projection_to_dynamo(p: &ControlProjection) -> IndexProjection {
    match p {
        ControlProjection::All => IndexProjection::All,
        ControlProjection::KeysOnly => IndexProjection::KeysOnly,
        ControlProjection::Include(names) => IndexProjection::Include(names.clone()),
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
    fn gsi_round_trips_through_control_index_def() {
        let dynamo = SecondaryIndex::Global(GlobalSecondaryIndex {
            name: "by-email".into(),
            key_attribute: "email".into(),
            sort_attribute: Some("created".into()),
            projection: IndexProjection::Include(vec!["name".into()]),
        });
        let def = index_to_control(&dynamo, "id");
        assert_eq!(def.kind, IndexKind::Global);
        assert_eq!(def.hash_attribute, "email");
        assert_eq!(def.sort_attribute.as_deref(), Some("created"));
        assert_eq!(index_to_dynamo(&def), dynamo);
    }

    #[test]
    fn lsi_round_trips_and_hashes_by_base_partition_key() {
        let dynamo = SecondaryIndex::Local(LocalSecondaryIndex {
            name: "by-ts".into(),
            sort_attribute: "ts".into(),
            projection: IndexProjection::KeysOnly,
        });
        let def = index_to_control(&dynamo, "pk");
        assert_eq!(def.kind, IndexKind::Local);
        // The LSI hashes by the base partition key in the control model.
        assert_eq!(def.hash_attribute, "pk");
        assert_eq!(def.sort_attribute.as_deref(), Some("ts"));
        assert_eq!(index_to_dynamo(&def), dynamo);
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
