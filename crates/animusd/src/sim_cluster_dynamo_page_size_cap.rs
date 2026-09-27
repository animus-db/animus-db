//! `SimCluster`-driven end-to-end tests of the `Query`/`Scan` 1 MiB
//! **evaluated-page** cap (ADR 0072 layer 3, `animus_dynamo::limits::
//! MAX_QUERY_SCAN_PAGE_BYTES`) — the byte-budget sibling of
//! `sim_cluster_dynamo_query_pagination.rs`'s `Limit`/`ExclusiveStartKey`
//! coverage, driven through the same shared pagination primitives
//! (`crate::dynamo::paginated_table_examine`/`paginated_kind_examine`/
//! `paginated_kind_examine_one`) that now also track cumulative evaluated
//! bytes and stop a page **before** an item would push the running total
//! over the cap — see those functions' own doc for the exact accounting
//! and boundary rule.
//!
//! Every fixture item here is deliberately fat (a large `pad` attribute) so
//! a handful of them already trip the byte cap well before any item count
//! would matter — `items_per_page` below derives the exact page size from
//! `animus_item::item_size` against the fixture actually used, rather than
//! hardcoding a count, so this suite stays correct if the size formula or
//! the cap constant ever changes.
//!
//! Seed replay (repo convention): `ANIMUS_SEED=<seed> cargo test -p animusd
//! --lib <test name>`.

use std::time::Duration;

use animus_item::{AttributeValue, Item, item_size};

use super::sim_cluster::SimCluster;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// Every fixture item's `pad` attribute is this many bytes of filler — large
/// enough that a handful of items already trips `MAX_QUERY_SCAN_PAGE_BYTES`,
/// small enough to stay well under `animus_item::MAX_ITEM_SIZE_BYTES` (400
/// KB) so no single item is ever rejected by the *item*-size cap.
const PAD_LEN: usize = 300_000;

const PARTITION: &str = "p1";

/// The `i`-th fixture item: `pk = "p1"` (every item shares one partition, so
/// `Scan` and `Query` see the identical sequence), `sk = "iNNN"` (zero-padded
/// so string order == numeric order for every `i` this suite uses), `cat =
/// "X"` (the GSI hash value, shared by every item so the GSI query below
/// sees them all), and a `pad` filler attribute that makes the item's own
/// `animus_item::item_size` big enough to matter.
fn item_fixture(i: usize) -> Item {
    let mut item = Item::new();
    item.insert("pk".to_string(), AttributeValue::S(PARTITION.to_string()));
    item.insert("sk".to_string(), AttributeValue::S(format!("i{i:03}")));
    item.insert("cat".to_string(), AttributeValue::S("X".to_string()));
    item.insert("pad".to_string(), AttributeValue::S("x".repeat(PAD_LEN)));
    item
}

/// How many fixture items fit in one evaluated page at the real cap —
/// derived from the fixture's own real `animus_item::item_size`, per this
/// repo's own testing convention (size fixtures against the constant, don't
/// hardcode counts).
fn items_per_page() -> usize {
    let one = item_size(&item_fixture(0));
    animus_dynamo::limits::MAX_QUERY_SCAN_PAGE_BYTES / one
}

/// A 3-node RF3 cluster with table `events` (composite `pk`/`sk`, both `S`)
/// and a hash-only GSI `by-cat`, seeded with `total` fixture items on the
/// single shared partition `p1`.
fn setup(seed: u64, total: usize) -> SimCluster {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.CreateTable",
        br#"{"TableName":"events","AttributeDefinitions":[
                {"AttributeName":"pk","AttributeType":"S"},
                {"AttributeName":"sk","AttributeType":"S"},
                {"AttributeName":"cat","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"by-cat",
                 "KeySchema":[{"AttributeName":"cat","KeyType":"HASH"}],
                 "Projection":{"ProjectionType":"ALL"}}]}"#,
    );
    assert_eq!(status, 200, "CreateTable failed: {body} (seed={seed})");

    let pad = "x".repeat(PAD_LEN);
    for i in 0..total {
        let body = format!(
            r#"{{"TableName":"events","Item":{{
                "pk":{{"S":"{PARTITION}"}},"sk":{{"S":"i{i:03}"}},
                "cat":{{"S":"X"}},"pad":{{"S":"{pad}"}}}}}}"#
        );
        let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.PutItem", body.as_bytes());
        assert_eq!(status, 200, "PutItem(i{i:03}) failed: {resp} (seed={seed})");
    }
    cluster
}

fn json(resp: &str) -> serde_json::Value {
    serde_json::from_str(resp).unwrap_or_else(|e| panic!("not JSON: {e}: {resp}"))
}

/// The `sk` values (as plain strings) of every item in a `Query`/`Scan`
/// response's `Items` array, in response order.
fn item_sks(v: &serde_json::Value) -> Vec<String> {
    v.get("Items")
        .and_then(serde_json::Value::as_array)
        .map(|items| {
            items
                .iter()
                .map(|it| {
                    it["sk"]["S"]
                        .as_str()
                        .expect("every item carries sk")
                        .to_string()
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Drive a `Query`/`Scan`'s full `LastEvaluatedKey` pagination loop from a
/// single fixed node, collecting every page's raw JSON body. Panics if
/// pagination does not terminate within a generous page budget.
fn drain_pages(
    cluster: &mut SimCluster,
    node: u64,
    target: &str,
    request_prefix: &str,
) -> Vec<serde_json::Value> {
    let mut pages = Vec::new();
    let mut cursor: Option<serde_json::Value> = None;
    loop {
        assert!(pages.len() < 50, "pagination did not terminate: {pages:?}");
        let esk = match &cursor {
            Some(c) => format!(",\"ExclusiveStartKey\":{c}"),
            None => String::new(),
        };
        let body = format!("{request_prefix}{esk}}}");
        let (status, resp) = cluster.dynamo(node, target, body.as_bytes());
        assert_eq!(status, 200, "page failed: {resp}");
        let v = json(&resp);
        cursor = v.get("LastEvaluatedKey").cloned();
        pages.push(v);
        if cursor.is_none() {
            return pages;
        }
    }
}

/// A base-table `Scan` with no `Limit` pages purely by evaluated bytes: the
/// first page holds exactly `items_per_page()` items and carries a
/// `LastEvaluatedKey`; walking the whole chain visits every seeded item
/// exactly once, in the same ascending order a full unpaginated scan would
/// give.
#[test]
fn scan_with_no_limit_pages_by_evaluated_bytes() {
    let seed = env_seed(0xC0A6_0001);
    let per_page = items_per_page();
    let total = per_page * 2 + 1;
    let mut cluster = setup(seed, total);

    let (status, first) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Scan",
        br#"{"TableName":"events","ConsistentRead":true}"#,
    );
    assert_eq!(status, 200, "{first} (seed={seed})");
    let first = json(&first);
    assert_eq!(
        first["Count"], per_page,
        "first page should hold exactly one evaluated page's worth: {first} (seed={seed})"
    );
    assert_eq!(first["ScannedCount"], per_page, "{first} (seed={seed})");
    assert!(
        first.get("LastEvaluatedKey").is_some(),
        "unbounded Scan over more than one page's worth must be truncated: {first} (seed={seed})"
    );

    let pages = drain_pages(
        &mut cluster,
        0,
        "DynamoDB_20120810.Scan",
        r#"{"TableName":"events","ConsistentRead":true"#,
    );
    let mut all_sks: Vec<String> = pages.iter().flat_map(item_sks).collect();
    let expected: Vec<String> = (0..total).map(|i| format!("i{i:03}")).collect();
    assert_eq!(
        all_sks.len(),
        total,
        "expected every item exactly once, got: {all_sks:?} (seed={seed})"
    );
    all_sks.sort();
    let mut sorted_expected = expected.clone();
    sorted_expected.sort();
    assert_eq!(all_sks, sorted_expected, "seed={seed}");
    // The walk itself (not just the set) should visit them in ascending
    // order, one evaluated page's worth of items at a time.
    let walked: Vec<String> = pages.iter().flat_map(item_sks).collect();
    assert_eq!(walked, expected, "seed={seed}");
    assert!(
        pages.len() >= 3,
        "expected at least 3 pages ({per_page}, {per_page}, 1), got {} (seed={seed})",
        pages.len()
    );
}

/// A `Query` on a single partition has the identical property: no `Limit`,
/// paginated purely by evaluated bytes, every item visited exactly once in
/// order.
#[test]
fn query_on_single_partition_pages_by_evaluated_bytes() {
    let seed = env_seed(0xC0A6_0002);
    let per_page = items_per_page();
    let total = per_page * 2 + 1;
    let mut cluster = setup(seed, total);

    let (status, first) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}}}"#,
    );
    assert_eq!(status, 200, "{first} (seed={seed})");
    let first = json(&first);
    assert_eq!(first["Count"], per_page, "{first} (seed={seed})");
    assert!(
        first.get("LastEvaluatedKey").is_some(),
        "{first} (seed={seed})"
    );

    let pages = drain_pages(
        &mut cluster,
        0,
        "DynamoDB_20120810.Query",
        r#"{"TableName":"events","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}}"#,
    );
    let walked: Vec<String> = pages.iter().flat_map(item_sks).collect();
    let expected: Vec<String> = (0..total).map(|i| format!("i{i:03}")).collect();
    assert_eq!(walked, expected, "seed={seed}");
}

/// `Limit` and the byte cap compose: whichever stops the page first wins. A
/// `Limit` under the byte cap's own item count is unaffected by it; a
/// `Limit` over it is still capped down to `items_per_page()`.
#[test]
fn limit_and_byte_cap_compose() {
    let seed = env_seed(0xC0A6_0003);
    let per_page = items_per_page();
    assert!(
        per_page >= 2,
        "fixture must fit at least 2 items/page for this test to mean anything"
    );
    let total = per_page * 2 + 1;
    let mut cluster = setup(seed, total);

    // A Limit smaller than the byte cap wins outright.
    let small_limit = per_page - 1;
    let (status, resp) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Scan",
        format!(r#"{{"TableName":"events","ConsistentRead":true,"Limit":{small_limit}}}"#)
            .as_bytes(),
    );
    assert_eq!(status, 200, "{resp} (seed={seed})");
    let v = json(&resp);
    assert_eq!(
        v["Count"], small_limit,
        "a Limit under the byte cap should win: {v} (seed={seed})"
    );
    assert!(v.get("LastEvaluatedKey").is_some(), "{v} (seed={seed})");

    // A Limit well over the byte cap is still capped down to items_per_page.
    let (status, resp) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Scan",
        br#"{"TableName":"events","ConsistentRead":true,"Limit":100000}"#,
    );
    assert_eq!(status, 200, "{resp} (seed={seed})");
    let v = json(&resp);
    assert_eq!(
        v["Count"], per_page,
        "a Limit over the byte cap should still be bounded by it: {v} (seed={seed})"
    );
    assert!(v.get("LastEvaluatedKey").is_some(), "{v} (seed={seed})");
}

/// A `FilterExpression` that rejects almost everything still sees its page
/// cut by *evaluated* bytes, not by how much survives the filter — DynamoDB's
/// own well-known surprise: a page can come back with few or even zero
/// `Items` while still carrying a `LastEvaluatedKey`, because the filter runs
/// after pagination, not before it.
#[test]
fn filter_expression_is_cut_by_evaluated_bytes_not_by_matches() {
    let seed = env_seed(0xC0A6_0004);
    let per_page = items_per_page();
    let total = per_page * 2 + 1;
    let mut cluster = setup(seed, total);

    let (status, resp) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Scan",
        br#"{"TableName":"events","ConsistentRead":true,
            "FilterExpression":"sk = :none",
            "ExpressionAttributeValues":{":none":{"S":"nomatch"}}}"#,
    );
    assert_eq!(status, 200, "{resp} (seed={seed})");
    let v = json(&resp);
    assert_eq!(
        v["Count"], 0,
        "the filter matches nothing: {v} (seed={seed})"
    );
    assert_eq!(
        v["ScannedCount"], per_page,
        "ScannedCount still reflects the byte-capped evaluated page: {v} (seed={seed})"
    );
    assert!(
        v.get("LastEvaluatedKey").is_some(),
        "a byte-capped page must still carry LastEvaluatedKey even with zero matches: \
         {v} (seed={seed})"
    );

    // Walking the whole chain scans every item exactly once and matches
    // none of them.
    let pages = drain_pages(
        &mut cluster,
        0,
        "DynamoDB_20120810.Scan",
        r#"{"TableName":"events","ConsistentRead":true,
            "FilterExpression":"sk = :none",
            "ExpressionAttributeValues":{":none":{"S":"nomatch"}}"#,
    );
    let total_scanned: u64 = pages
        .iter()
        .map(|p| {
            p["ScannedCount"]
                .as_u64()
                .expect("ScannedCount is a number")
        })
        .sum();
    let total_count: u64 = pages
        .iter()
        .map(|p| p["Count"].as_u64().expect("Count is a number"))
        .sum();
    assert_eq!(total_scanned, total as u64, "seed={seed}");
    assert_eq!(total_count, 0, "seed={seed}");
}

/// `Select: COUNT` still pages by evaluated bytes: `Count`/`ScannedCount`
/// reflect one byte-capped page, not the whole table, and a truncated COUNT
/// page still carries `LastEvaluatedKey` — with no `Items` field at all.
#[test]
fn select_count_pages_with_count_and_last_evaluated_key() {
    let seed = env_seed(0xC0A6_0005);
    let per_page = items_per_page();
    let total = per_page * 2 + 1;
    let mut cluster = setup(seed, total);

    let (status, resp) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Scan",
        br#"{"TableName":"events","ConsistentRead":true,"Select":"COUNT"}"#,
    );
    assert_eq!(status, 200, "{resp} (seed={seed})");
    assert!(
        !resp.contains("\"Items\""),
        "COUNT must never carry Items: {resp} (seed={seed})"
    );
    let v = json(&resp);
    assert_eq!(v["Count"], per_page, "{v} (seed={seed})");
    assert_eq!(v["ScannedCount"], per_page, "{v} (seed={seed})");
    assert!(v.get("LastEvaluatedKey").is_some(), "{v} (seed={seed})");
}

/// A GSI `Query` (no `Limit`) pages by evaluated bytes exactly like the base
/// table: `by-cat`'s hidden table carries the same (`Projection: ALL`) item
/// content, so it trips the same `items_per_page()` boundary.
#[test]
fn gsi_query_pages_by_evaluated_bytes() {
    let seed = env_seed(0xC0A6_0006);
    let per_page = items_per_page();
    let total = per_page * 2 + 1;
    let mut cluster = setup(seed, total);

    let tablet = cluster
        .metadata(0)
        .tablets_for_table("events")
        .next()
        .expect("events has a tablet")
        .0
        .to_owned();
    let leader = cluster
        .leader_index_of(tablet)
        .expect("events tablet has a leader");
    cluster.drain_gsi(leader, "events");

    // Converge (ADR 0055 eventual-read replica staleness): poll a
    // cheap, well-under-the-byte-cap `Limit:1` query on `leader` itself
    // until it sees a materialized row, rather than assuming `drain_gsi`'s
    // own commit-completion implies this replica's local apply is done too.
    let mut converged = false;
    for _ in 0..80 {
        let (status, resp) = cluster.dynamo(
            leader,
            "DynamoDB_20120810.Query",
            br#"{"TableName":"events","IndexName":"by-cat",
                "KeyConditionExpression":"cat = :c",
                "ExpressionAttributeValues":{":c":{"S":"X"}},
                "Limit":1}"#,
        );
        if status == 200 && json(&resp)["Count"] == 1 {
            converged = true;
            break;
        }
        cluster.run_for(Duration::from_millis(100));
    }
    assert!(
        converged,
        "GSI never converged on its own leader (seed={seed})"
    );

    let (status, first) = cluster.dynamo(
        leader,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","IndexName":"by-cat",
            "KeyConditionExpression":"cat = :c",
            "ExpressionAttributeValues":{":c":{"S":"X"}}}"#,
    );
    assert_eq!(status, 200, "{first} (seed={seed})");
    let first = json(&first);
    assert_eq!(
        first["Count"], per_page,
        "GSI query should also cap at one evaluated page: {first} (seed={seed})"
    );
    assert!(
        first.get("LastEvaluatedKey").is_some(),
        "{first} (seed={seed})"
    );

    let pages = drain_pages(
        &mut cluster,
        leader,
        "DynamoDB_20120810.Query",
        r#"{"TableName":"events","IndexName":"by-cat",
            "KeyConditionExpression":"cat = :c",
            "ExpressionAttributeValues":{":c":{"S":"X"}}"#,
    );
    let mut all_sks: Vec<String> = pages.iter().flat_map(item_sks).collect();
    all_sks.sort();
    let mut expected: Vec<String> = (0..total).map(|i| format!("i{i:03}")).collect();
    expected.sort();
    assert_eq!(all_sks, expected, "seed={seed}");
}

/// PartiQL `SELECT` via `ExecuteStatement` reuses the identical `Query`/
/// `Scan` path, so it too pages by evaluated bytes — `LastEvaluatedKey`
/// becomes `NextToken` at that boundary (`reshape_query_scan_response_to_
/// execute_statement`).
#[test]
fn partiql_select_paginates_via_next_token() {
    let seed = env_seed(0xC0A6_0007);
    let per_page = items_per_page();
    let total = per_page * 2 + 1;
    let mut cluster = setup(seed, total);

    let (status, first) = cluster.dynamo(
        0,
        "DynamoDB_20120810.ExecuteStatement",
        br#"{"Statement":"SELECT * FROM events WHERE pk = ?",
            "Parameters":[{"S":"p1"}],"ConsistentRead":true}"#,
    );
    assert_eq!(status, 200, "{first} (seed={seed})");
    let first = json(&first);
    let first_items = first["Items"].as_array().expect("Items is an array").len();
    assert_eq!(first_items, per_page, "{first} (seed={seed})");
    let mut next_token = first
        .get("NextToken")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    assert!(next_token.is_some(), "{first} (seed={seed})");

    let mut walked: Vec<String> = item_sks(&first);
    let mut pages = 1usize;
    while let Some(token) = next_token.take() {
        assert!(pages < 50, "pagination did not terminate (seed={seed})");
        pages += 1;
        let body = format!(
            r#"{{"Statement":"SELECT * FROM events WHERE pk = ?",
                "Parameters":[{{"S":"p1"}}],"ConsistentRead":true,
                "NextToken":{}}}"#,
            serde_json::to_string(&token).expect("token encodes")
        );
        let (status, resp) =
            cluster.dynamo(0, "DynamoDB_20120810.ExecuteStatement", body.as_bytes());
        assert_eq!(status, 200, "{resp} (seed={seed})");
        let v = json(&resp);
        walked.extend(item_sks(&v));
        next_token = v
            .get("NextToken")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
    }

    let expected: Vec<String> = (0..total).map(|i| format!("i{i:03}")).collect();
    assert_eq!(walked, expected, "seed={seed}");
}
