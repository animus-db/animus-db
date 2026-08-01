//! The DynamoDB-style item API maps onto the storage core: put/get/delete and
//! ordered Query within a partition, over the in-memory engine.

use custos_dynamo::{AttributeValue as Av, Item, SortKeyCondition, Table, TableSchema};
use custos_storage::MemoryEngine;
use futures::executor::block_on;

fn item(pairs: &[(&str, Av)]) -> Item {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
        .collect()
}

#[test]
fn put_get_delete_round_trip() {
    block_on(async {
        let table = Table::new(MemoryEngine::new(), TableSchema::simple("id"));
        let it = item(&[
            ("id", Av::S("u1".into())),
            ("name", Av::S("Ada".into())),
            ("admin", Av::Bool(true)),
        ]);
        table.put_item(it.clone()).await.unwrap();

        let got = table.get_item(&Av::S("u1".into()), None).await.unwrap();
        assert_eq!(got, Some(it));
        assert_eq!(
            table.get_item(&Av::S("nobody".into()), None).await.unwrap(),
            None
        );

        table.delete_item(&Av::S("u1".into()), None).await.unwrap();
        assert_eq!(
            table.get_item(&Av::S("u1".into()), None).await.unwrap(),
            None
        );
    });
}

#[test]
fn put_replaces_an_existing_item() {
    block_on(async {
        let table = Table::new(MemoryEngine::new(), TableSchema::simple("id"));
        table
            .put_item(item(&[("id", Av::S("k".into())), ("v", Av::N("1".into()))]))
            .await
            .unwrap();
        table
            .put_item(item(&[("id", Av::S("k".into())), ("v", Av::N("2".into()))]))
            .await
            .unwrap();
        let got = table
            .get_item(&Av::S("k".into()), None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.get("v"), Some(&Av::N("2".into())));
    });
}

#[test]
fn query_returns_a_partition_ordered_by_sort_key() {
    block_on(async {
        let table = Table::new(MemoryEngine::new(), TableSchema::composite("pk", "sk"));
        // Two partitions; insert out of order to prove ordering is by sort key.
        for sk in ["c", "a", "b"] {
            table
                .put_item(item(&[
                    ("pk", Av::S("p1".into())),
                    ("sk", Av::S(sk.into())),
                ]))
                .await
                .unwrap();
        }
        table
            .put_item(item(&[
                ("pk", Av::S("p2".into())),
                ("sk", Av::S("z".into())),
            ]))
            .await
            .unwrap();

        let rows = table.query(&Av::S("p1".into())).await.unwrap();
        let sks: Vec<_> = rows.iter().map(|r| r.get("sk").unwrap().clone()).collect();
        assert_eq!(
            sks,
            vec![Av::S("a".into()), Av::S("b".into()), Av::S("c".into())]
        );

        // The other partition is isolated.
        assert_eq!(table.query(&Av::S("p2".into())).await.unwrap().len(), 1);
    });
}

#[test]
fn query_with_sort_conditions_narrows_a_partition() {
    block_on(async {
        let table = Table::new(MemoryEngine::new(), TableSchema::composite("pk", "sk"));
        for sk in ["a", "ab", "abc", "b", "c"] {
            table
                .put_item(item(&[("pk", Av::S("p".into())), ("sk", Av::S(sk.into()))]))
                .await
                .unwrap();
        }
        let sks = |rows: Vec<Item>| -> Vec<String> {
            rows.iter()
                .map(|r| match r.get("sk").unwrap() {
                    Av::S(s) => s.clone(),
                    other => panic!("unexpected sk {other:?}"),
                })
                .collect()
        };

        let eq = table
            .query_with(
                &Av::S("p".into()),
                Some(&SortKeyCondition::Equals(Av::S("b".into()))),
            )
            .await
            .unwrap();
        assert_eq!(sks(eq), vec!["b".to_string()]);

        let between = table
            .query_with(
                &Av::S("p".into()),
                Some(&SortKeyCondition::Between(
                    Av::S("ab".into()),
                    Av::S("b".into()),
                )),
            )
            .await
            .unwrap();
        assert_eq!(sks(between), vec!["ab", "abc", "b"]);

        let begins = table
            .query_with(
                &Av::S("p".into()),
                Some(&SortKeyCondition::BeginsWith(Av::S("ab".into()))),
            )
            .await
            .unwrap();
        assert_eq!(sks(begins), vec!["ab", "abc"]);
    });
}

#[test]
fn composite_keys_address_distinct_items() {
    block_on(async {
        let table = Table::new(MemoryEngine::new(), TableSchema::composite("pk", "sk"));
        table
            .put_item(item(&[
                ("pk", Av::S("p".into())),
                ("sk", Av::S("a".into())),
                ("n", Av::N("1".into())),
            ]))
            .await
            .unwrap();
        table
            .put_item(item(&[
                ("pk", Av::S("p".into())),
                ("sk", Av::S("b".into())),
                ("n", Av::N("2".into())),
            ]))
            .await
            .unwrap();

        let a = table
            .get_item(&Av::S("p".into()), Some(&Av::S("a".into())))
            .await
            .unwrap()
            .unwrap();
        let b = table
            .get_item(&Av::S("p".into()), Some(&Av::S("b".into())))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(a.get("n"), Some(&Av::N("1".into())));
        assert_eq!(b.get("n"), Some(&Av::N("2".into())));

        // Deleting one leaves the other.
        table
            .delete_item(&Av::S("p".into()), Some(&Av::S("a".into())))
            .await
            .unwrap();
        assert!(
            table
                .get_item(&Av::S("p".into()), Some(&Av::S("a".into())))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            table
                .get_item(&Av::S("p".into()), Some(&Av::S("b".into())))
                .await
                .unwrap()
                .is_some()
        );
    });
}

#[test]
fn missing_key_attribute_is_an_error() {
    block_on(async {
        let table = Table::new(MemoryEngine::new(), TableSchema::simple("id"));
        let err = table
            .put_item(item(&[("name", Av::S("no id".into()))]))
            .await
            .unwrap_err();
        assert!(matches!(err, custos_dynamo::DynamoError::MissingKey(k) if k == "id"));
    });
}
