//! The DynamoDB-style item API maps onto the storage core: put/get/delete and
//! ordered Query within a partition, over the in-memory engine.

use custos_dynamo::{AttributeValue as Av, Item, Table, TableSchema};
use custos_storage::MemoryEngine;

fn item(pairs: &[(&str, Av)]) -> Item {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
        .collect()
}

#[test]
fn put_get_delete_round_trip() {
    let table = Table::new(MemoryEngine::new(), TableSchema::simple("id"));
    let it = item(&[
        ("id", Av::S("u1".into())),
        ("name", Av::S("Ada".into())),
        ("admin", Av::Bool(true)),
    ]);
    table.put_item(it.clone()).unwrap();

    let got = table.get_item(&Av::S("u1".into()), None).unwrap();
    assert_eq!(got, Some(it));
    assert_eq!(table.get_item(&Av::S("nobody".into()), None).unwrap(), None);

    table.delete_item(&Av::S("u1".into()), None).unwrap();
    assert_eq!(table.get_item(&Av::S("u1".into()), None).unwrap(), None);
}

#[test]
fn put_replaces_an_existing_item() {
    let table = Table::new(MemoryEngine::new(), TableSchema::simple("id"));
    table
        .put_item(item(&[("id", Av::S("k".into())), ("v", Av::N("1".into()))]))
        .unwrap();
    table
        .put_item(item(&[("id", Av::S("k".into())), ("v", Av::N("2".into()))]))
        .unwrap();
    let got = table.get_item(&Av::S("k".into()), None).unwrap().unwrap();
    assert_eq!(got.get("v"), Some(&Av::N("2".into())));
}

#[test]
fn query_returns_a_partition_ordered_by_sort_key() {
    let table = Table::new(MemoryEngine::new(), TableSchema::composite("pk", "sk"));
    // Two partitions; insert out of order to prove ordering is by sort key.
    for sk in ["c", "a", "b"] {
        table
            .put_item(item(&[
                ("pk", Av::S("p1".into())),
                ("sk", Av::S(sk.into())),
            ]))
            .unwrap();
    }
    table
        .put_item(item(&[
            ("pk", Av::S("p2".into())),
            ("sk", Av::S("z".into())),
        ]))
        .unwrap();

    let rows = table.query(&Av::S("p1".into())).unwrap();
    let sks: Vec<_> = rows.iter().map(|r| r.get("sk").unwrap().clone()).collect();
    assert_eq!(
        sks,
        vec![Av::S("a".into()), Av::S("b".into()), Av::S("c".into())]
    );

    // The other partition is isolated.
    assert_eq!(table.query(&Av::S("p2".into())).unwrap().len(), 1);
}

#[test]
fn composite_keys_address_distinct_items() {
    let table = Table::new(MemoryEngine::new(), TableSchema::composite("pk", "sk"));
    table
        .put_item(item(&[
            ("pk", Av::S("p".into())),
            ("sk", Av::S("a".into())),
            ("n", Av::N("1".into())),
        ]))
        .unwrap();
    table
        .put_item(item(&[
            ("pk", Av::S("p".into())),
            ("sk", Av::S("b".into())),
            ("n", Av::N("2".into())),
        ]))
        .unwrap();

    let a = table
        .get_item(&Av::S("p".into()), Some(&Av::S("a".into())))
        .unwrap()
        .unwrap();
    let b = table
        .get_item(&Av::S("p".into()), Some(&Av::S("b".into())))
        .unwrap()
        .unwrap();
    assert_eq!(a.get("n"), Some(&Av::N("1".into())));
    assert_eq!(b.get("n"), Some(&Av::N("2".into())));

    // Deleting one leaves the other.
    table
        .delete_item(&Av::S("p".into()), Some(&Av::S("a".into())))
        .unwrap();
    assert!(
        table
            .get_item(&Av::S("p".into()), Some(&Av::S("a".into())))
            .unwrap()
            .is_none()
    );
    assert!(
        table
            .get_item(&Av::S("p".into()), Some(&Av::S("b".into())))
            .unwrap()
            .is_some()
    );
}

#[test]
fn missing_key_attribute_is_an_error() {
    let table = Table::new(MemoryEngine::new(), TableSchema::simple("id"));
    let err = table
        .put_item(item(&[("name", Av::S("no id".into()))]))
        .unwrap_err();
    assert!(matches!(err, custos_dynamo::DynamoError::MissingKey(k) if k == "id"));
}
