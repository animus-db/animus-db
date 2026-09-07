//! `SimCluster`-driven end-to-end tests of `Query`'s `FilterExpression`
//! (ADR 0061 rung D3 PR 3a/3b, C-04 D3) — driven through the now-generic
//! [`crate::dynamo::run_index_query`]/[`crate::dynamo::run_index_scan`] (and
//! their GSI/LSI siblings), reached from [`crate::dynamo::dispatch_item_op`]'s
//! own `Query`/`Scan` arms for the first time (they used to reject any named
//! `index` with `unsupported_by_generic_dispatch`).
//!
//! **PR 3a** replaced five of `crates/animusd/tests/dynamo_query_filter.rs`'s
//! six tests: `filter_narrows_a_base_query_instead_of_being_ignored`,
//! `a_filtered_page_returns_fewer_than_limit_and_still_carries_a_cursor`,
//! `a_filter_matching_nothing_still_reports_what_it_evaluated`,
//! `filter_applies_to_an_lsi_query`, `attribute_exists_works_as_a_query_
//! filter`, leaving `filter_applies_to_a_gsi_query` on `ProdEnv` — it reads a
//! *materialized* GSI row, and `SimCluster` never spawned `index_drain::
//! change_consumer_loop` (the only thing that ever filled a GSI's own hidden
//! table) at the time.
//!
//! **PR 3b converts the sixth and last test too**: `[SimCluster::drain_gsi]`
//! (`sim_cluster.rs`) is a test-only stand-in for that same drain arm, so
//! `filter_applies_to_a_gsi_query` below drains on demand instead of
//! polling a background loop this fixture still never runs. This file (and
//! `crates/animusd/tests/dynamo_query_filter.rs`, now empty) is therefore
//! fully converted.
//!
//! Every LSI/base assertion here is unaffected by any of this: an LSI row is
//! written synchronously in the same Raft entry as its base row (ADR 0041
//! §2), so `SimCluster`'s hand-hosted/wire-provisioned tablets serve it
//! immediately — no drain needed, unlike the GSI test.
//!
//! Seed replay (repo convention): `ANIMUS_SEED=<seed> cargo test -p animusd
//! --lib <test name>`.

use std::time::Duration;

use animus_tablet::TabletId;

use super::sim_cluster::SimCluster;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// `table`'s own (sole) tablet id, read off node 0's own `Metadata` — see
/// `sim_cluster_dynamo_query_pagination.rs`'s identical helper for why this
/// is needed instead of `SimCluster::tablet_of` (this file's `setup` creates
/// its table through the real wire, not the hand-hosted bypass).
fn first_tablet(cluster: &SimCluster, table: &str) -> TabletId {
    cluster
        .metadata(0)
        .tablets_for_table(table)
        .next()
        .unwrap_or_else(|| panic!("{table} has no tablet"))
        .0
        .to_owned()
}

/// Poll `body`'s `Query` until `accept` holds, or panic after a bounded
/// number of attempts — the sim analogue of `dynamo_query_filter.rs::
/// await_gsi_query`'s real-socket converged-or-timeout poll.
fn await_gsi_query(
    cluster: &mut SimCluster,
    node: u64,
    body: &str,
    accept: impl Fn(&str) -> bool,
) -> String {
    let mut last = String::new();
    for _ in 0..80 {
        let (status, resp) = cluster.dynamo(node, "DynamoDB_20120810.Query", body.as_bytes());
        if status == 200 && accept(&resp) {
            return resp;
        }
        last = resp;
        cluster.run_for(Duration::from_millis(100));
    }
    panic!("gsi query never converged (last saw: {last})");
}

/// A 3-node cluster with table `events` (composite `pk`/`sk`), a hash-only
/// GSI (`by-cat`) and an LSI (`by-score`, alt-sort `score`) — mirrors
/// `dynamo_query_filter.rs::setup` exactly, six items in partition `p1`, all
/// sharing GSI hash `cat = "X"`, half `parity: even` and half `odd`.
fn setup(seed: u64) -> SimCluster {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.CreateTable",
        br#"{"TableName":"events","AttributeDefinitions":[{"AttributeName":"cat","AttributeType":"S"},{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"score","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"}],
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"by-cat",
                 "KeySchema":[{"AttributeName":"cat","KeyType":"HASH"}],
                 "Projection":{"ProjectionType":"ALL"}}],
            "LocalSecondaryIndexes":[
                {"IndexName":"by-score",
                 "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                              {"AttributeName":"score","KeyType":"RANGE"}]}]}"#,
    );
    assert_eq!(status, 200, "CreateTable failed: {body} (seed={seed})");

    for i in 0..6 {
        let parity = if i % 2 == 0 { "even" } else { "odd" };
        let body = format!(
            r#"{{"TableName":"events","Item":{{
                "pk":{{"S":"p1"}},"sk":{{"S":"a{i}"}},"cat":{{"S":"X"}},
                "score":{{"S":"s{i}"}},"parity":{{"S":"{parity}"}}}}}}"#
        );
        let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.PutItem", body.as_bytes());
        assert_eq!(status, 200, "PutItem(a{i}) failed: {resp} (seed={seed})");
    }
    cluster
}

/// Read `"Count"` / `"ScannedCount"` out of a response body.
fn counts(body: &str) -> (usize, usize) {
    let read = |field: &str| -> usize {
        let marker = format!("\"{field}\":");
        let at = body
            .find(&marker)
            .unwrap_or_else(|| panic!("no {field} in {body}"))
            + marker.len();
        body[at..]
            .chars()
            .take_while(char::is_ascii_digit)
            .collect::<String>()
            .parse()
            .unwrap_or_else(|_| panic!("unparsable {field} in {body}"))
    };
    (read("Count"), read("ScannedCount"))
}

fn extract_last_evaluated_key(body: &str) -> Option<String> {
    let marker = "\"LastEvaluatedKey\":";
    let start = body.find(marker)? + marker.len();
    let bytes = body.as_bytes();
    if bytes.get(start) != Some(&b'{') {
        return None;
    }
    let mut depth = 0usize;
    for (i, &b) in bytes[start..].iter().enumerate() {
        match b {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(body[start..start + i + 1].to_string());
                }
            }
            _ => {}
        }
    }
    None
}

/// The headline fix: a `FilterExpression` on a base `Query` is honoured.
/// Mirrors `dynamo_query_filter.rs::filter_narrows_a_base_query_instead_of_
/// being_ignored`, issued from a non-leader-hosting node index.
#[test]
fn filter_narrows_a_base_query_instead_of_being_ignored() {
    let seed = env_seed(0xE4C1_0001);
    let mut cluster = setup(seed);

    let (status, body) = cluster.dynamo(
        1,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "FilterExpression":"parity = :v",
            "ExpressionAttributeValues":{":p":{"S":"p1"},":v":{"S":"even"}}}"#,
    );
    assert_eq!(status, 200, "filtered query failed: {body} (seed={seed})");

    for kept in ["a0", "a2", "a4"] {
        assert!(
            body.contains(kept),
            "{kept} should have matched: {body} (seed={seed})"
        );
    }
    for dropped in ["a1", "a3", "a5"] {
        assert!(
            !body.contains(dropped),
            "{dropped} should have been filtered out: {body} (seed={seed})"
        );
    }
    let (count, scanned) = counts(&body);
    assert_eq!(count, 3, "Count is the post-filter total: {body}");
    assert_eq!(
        scanned, 6,
        "ScannedCount counts every item the key condition evaluated: {body}"
    );
}

/// `Limit` caps items evaluated, not returned, so a filtered page can come
/// back short and still carry a cursor; walking it visits exactly the
/// matching items once each. Mirrors `dynamo_query_filter.rs::a_filtered_
/// page_returns_fewer_than_limit_and_still_carries_a_cursor`.
#[test]
fn a_filtered_page_returns_fewer_than_limit_and_still_carries_a_cursor() {
    let seed = env_seed(0xE4C1_0002);
    let mut cluster = setup(seed);

    let (status, body) = cluster.dynamo(
        2,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "FilterExpression":"parity = :v",
            "ExpressionAttributeValues":{":p":{"S":"p1"},":v":{"S":"odd"}},
            "Limit":2}"#,
    );
    assert_eq!(
        status, 200,
        "filtered limited query failed: {body} (seed={seed})"
    );

    let (count, scanned) = counts(&body);
    assert_eq!(scanned, 2, "Limit caps items evaluated: {body}");
    assert_eq!(count, 1, "only a1 survives the filter on this page: {body}");
    assert!(body.contains("a1"), "a1 should be the kept item: {body}");
    assert!(!body.contains("\"a0\""), "a0 was filtered out: {body}");
    assert!(
        extract_last_evaluated_key(&body).is_some(),
        "a short filtered page must still carry a cursor: {body}"
    );

    let mut seen: Vec<String> = Vec::new();
    let mut cursor = extract_last_evaluated_key(&body);
    let mut kept_first_page = 1usize;
    let mut pages = 0usize;
    while let Some(c) = cursor {
        pages += 1;
        assert!(pages < 20, "pagination did not terminate (seed={seed})");
        let body = format!(
            r#"{{"TableName":"events","ConsistentRead":true,
                "KeyConditionExpression":"pk = :p",
                "FilterExpression":"parity = :v",
                "ExpressionAttributeValues":{{":p":{{"S":"p1"}},":v":{{"S":"odd"}}}},
                "Limit":2,"ExclusiveStartKey":{c}}}"#
        );
        let (status, page) =
            cluster.dynamo(pages as u64 % 3, "DynamoDB_20120810.Query", body.as_bytes());
        assert_eq!(status, 200, "cursor page failed: {page} (seed={seed})");
        for sk in ["a1", "a3", "a5"] {
            if page.contains(&format!("\"{sk}\"")) {
                seen.push(sk.to_string());
            }
        }
        kept_first_page += counts(&page).0;
        cursor = extract_last_evaluated_key(&page);
    }
    seen.sort();
    seen.dedup();
    assert_eq!(
        kept_first_page, 3,
        "the whole walk returns exactly the three odd items (seed={seed})"
    );
    assert_eq!(seen, vec!["a3".to_string(), "a5".to_string()]);
}

/// A filter matching nothing returns empty `Items` with a non-zero
/// `ScannedCount`. Mirrors `dynamo_query_filter.rs::a_filter_matching_
/// nothing_still_reports_what_it_evaluated`.
#[test]
fn a_filter_matching_nothing_still_reports_what_it_evaluated() {
    let seed = env_seed(0xE4C1_0003);
    let mut cluster = setup(seed);

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "FilterExpression":"parity = :v",
            "ExpressionAttributeValues":{":p":{"S":"p1"},":v":{"S":"neither"}}}"#,
    );
    assert_eq!(status, 200, "query failed: {body} (seed={seed})");
    let (count, scanned) = counts(&body);
    assert_eq!(count, 0, "nothing matches the filter: {body}");
    assert_eq!(scanned, 6, "but all six were evaluated: {body}");
}

/// The filter reaches an **LSI** query too — strongly consistent (commits
/// atomically with the base row), so no convergence poll is needed. Mirrors
/// `dynamo_query_filter.rs::filter_applies_to_an_lsi_query`.
#[test]
fn filter_applies_to_an_lsi_query() {
    let seed = env_seed(0xE4C1_0004);
    let mut cluster = setup(seed);

    let (status, body) = cluster.dynamo(
        1,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","IndexName":"by-score","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "FilterExpression":"parity = :v",
            "ExpressionAttributeValues":{":p":{"S":"p1"},":v":{"S":"odd"}}}"#,
    );
    assert_eq!(
        status, 200,
        "LSI filtered query failed: {body} (seed={seed})"
    );
    assert_eq!(counts(&body), (3, 6), "{body}");
    for kept in ["a1", "a3", "a5"] {
        assert!(
            body.contains(kept),
            "{kept} should have matched: {body} (seed={seed})"
        );
    }
}

/// `attribute_exists`/`attribute_not_exists` work as `Query` filters too.
/// Mirrors `dynamo_query_filter.rs::attribute_exists_works_as_a_query_
/// filter`.
#[test]
fn attribute_exists_works_as_a_query_filter() {
    let seed = env_seed(0xE4C1_0005);
    let mut cluster = setup(seed);

    let (status, present) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "FilterExpression":"attribute_exists(parity)",
            "ExpressionAttributeValues":{":p":{"S":"p1"}}}"#,
    );
    assert_eq!(status, 200, "query failed: {present} (seed={seed})");
    assert_eq!(counts(&present), (6, 6), "every item has parity: {present}");

    let (status, absent) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "FilterExpression":"attribute_not_exists(parity)",
            "ExpressionAttributeValues":{":p":{"S":"p1"}}}"#,
    );
    assert_eq!(status, 200, "query failed: {absent} (seed={seed})");
    assert_eq!(counts(&absent), (0, 6), "none lack parity: {absent}");
}

/// The filter reaches a **GSI** query too, once the GSI's own hidden table
/// has been drained. Mirrors `dynamo_query_filter.rs::
/// filter_applies_to_a_gsi_query`.
#[test]
fn filter_applies_to_a_gsi_query() {
    let seed = env_seed(0xE4C1_0006);
    let mut cluster = setup(seed);
    let tablet = first_tablet(&cluster, "events");
    let leader = cluster
        .leader_index_of(tablet)
        .expect("events tablet has a leader");
    cluster.drain_gsi(leader, "events");

    let body = await_gsi_query(
        &mut cluster,
        1,
        r#"{"TableName":"events","IndexName":"by-cat",
            "KeyConditionExpression":"cat = :c",
            "FilterExpression":"parity = :v",
            "ExpressionAttributeValues":{":c":{"S":"X"},":v":{"S":"even"}}}"#,
        |got| counts(got) == (3, 6),
    );
    for kept in ["a0", "a2", "a4"] {
        assert!(
            body.contains(kept),
            "{kept} should have matched: {body} (seed={seed})"
        );
    }
    for dropped in ["a1", "a3", "a5"] {
        assert!(
            !body.contains(dropped),
            "{dropped} should have been filtered out of the GSI page: {body} (seed={seed})"
        );
    }
}
