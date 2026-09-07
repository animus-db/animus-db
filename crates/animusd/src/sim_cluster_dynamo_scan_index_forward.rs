//! `SimCluster`-driven end-to-end tests of `Query`'s `ScanIndexForward`
//! (descending reads, and inverted pagination) over the base table and an
//! LSI (ADR 0061 rung D3 PR 3a, C-04 D3) — driven through the now-generic
//! [`crate::dynamo::run_index_query`]/[`crate::dynamo::run_lsi_query`].
//!
//! Replaces six of `crates/animusd/tests/dynamo_scan_index_forward.rs`'s
//! eight tests: `descending_query_returns_the_partition_in_reverse_sort_
//! order`, `a_descending_limit_keeps_the_highest_rows`, `descending_
//! pagination_visits_every_item_exactly_once`, `descending_applies_to_an_
//! lsi_query`, `descending_composes_with_a_filter`, `lsi_scan_index_forward_
//! orders_n_sort_keys_numerically`. **`descending_applies_to_a_gsi_query`
//! and `gsi_scan_index_forward_orders_n_sort_keys_numerically` stay on
//! `ProdEnv`** — both read a materialized GSI row, which `SimCluster` cannot
//! produce (see `sim_cluster_dynamo_query_filter.rs`'s own module doc for the
//! boundary).
//!
//! Seed replay (repo convention): `ANIMUS_SEED=<seed> cargo test -p animusd
//! --lib <test name>`.

use super::sim_cluster::SimCluster;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// A 3-node cluster with table `events` (composite `pk`/`sk`), a hash-only
/// GSI (`by-cat`) and an LSI (`by-score`) — mirrors `dynamo_scan_index_
/// forward.rs::setup`: six items, one partition, `parity` alternating
/// `even`/`odd`.
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

/// A 3-node cluster with table `readings` (composite `pk`/`sk`, `sk`
/// declared `N`), a composite GSI (`by-device-value`) and a composite LSI
/// (`by-alt`) — mirrors `dynamo_scan_index_forward.rs::setup_n_sort_keys`.
fn setup_n_sort_keys(seed: u64) -> SimCluster {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.CreateTable",
        br#"{"TableName":"readings","AttributeDefinitions":[{"AttributeName":"device","AttributeType":"S"},{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"N"},{"AttributeName":"value","AttributeType":"N"},{"AttributeName":"alt","AttributeType":"N"}],
            "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                         {"AttributeName":"sk","KeyType":"RANGE"}],
            "GlobalSecondaryIndexes":[
                {"IndexName":"by-device-value",
                 "KeySchema":[{"AttributeName":"device","KeyType":"HASH"},
                              {"AttributeName":"value","KeyType":"RANGE"}],
                 "Projection":{"ProjectionType":"ALL"}}],
            "LocalSecondaryIndexes":[
                {"IndexName":"by-alt",
                 "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                              {"AttributeName":"alt","KeyType":"RANGE"}]}]}"#,
    );
    assert_eq!(status, 200, "CreateTable failed: {body} (seed={seed})");

    for sk in ["-10", "-5", "0", "2", "10", "100", "0.5"] {
        let body = format!(
            r#"{{"TableName":"readings","Item":{{
                "pk":{{"S":"p1"}},"sk":{{"N":"{sk}"}},
                "device":{{"S":"d1"}},"value":{{"N":"{sk}"}},
                "alt":{{"N":"{sk}"}}}}}}"#
        );
        let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.PutItem", body.as_bytes());
        assert_eq!(status, 200, "PutItem(sk={sk}) failed: {resp} (seed={seed})");
    }
    cluster
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

/// The order of `sk`s in a response body, by first appearance (stopping at
/// `LastEvaluatedKey`, which is itself an item key).
fn sk_order(body: &str) -> Vec<String> {
    let mut order = Vec::new();
    let mut rest = body
        .find("\"LastEvaluatedKey\":")
        .map_or(body, |at| &body[..at]);
    while let Some(at) = rest.find("\"sk\":{\"S\":\"") {
        let after = &rest[at + "\"sk\":{\"S\":\"".len()..];
        let endq = after.find('"').expect("closing quote");
        order.push(after[..endq].to_string());
        rest = &after[endq..];
    }
    order
}

/// The `field`'s `N` values in a response body, in first-appearance order.
fn n_order(body: &str, field: &str) -> Vec<String> {
    let marker = format!("\"{field}\":{{\"N\":\"");
    let mut out = Vec::new();
    let mut rest = body
        .find("\"LastEvaluatedKey\":")
        .map_or(body, |at| &body[..at]);
    while let Some(at) = rest.find(&marker) {
        let after = &rest[at + marker.len()..];
        let endq = after.find('"').expect("closing quote");
        out.push(after[..endq].to_string());
        rest = &after[endq..];
    }
    out
}

/// `ScanIndexForward: false` returns the partition highest-sort-key first.
/// Mirrors `dynamo_scan_index_forward.rs::descending_query_returns_the_
/// partition_in_reverse_sort_order`.
#[test]
fn descending_query_returns_the_partition_in_reverse_sort_order() {
    let seed = env_seed(0xE4C4_0001);
    let mut cluster = setup(seed);

    let (status, asc) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}},
            "ConsistentRead":true}"#,
    );
    assert_eq!(status, 200, "ascending query failed: {asc} (seed={seed})");
    assert_eq!(
        sk_order(&asc),
        vec!["a0", "a1", "a2", "a3", "a4", "a5"],
        "ascending is the default: {asc}"
    );

    let (status, desc) = cluster.dynamo(
        1,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}},
            "ConsistentRead":true,
            "ScanIndexForward":false}"#,
    );
    assert_eq!(status, 200, "descending query failed: {desc} (seed={seed})");
    assert_eq!(
        sk_order(&desc),
        vec!["a5", "a4", "a3", "a2", "a1", "a0"],
        "ScanIndexForward:false reverses the sort order: {desc}"
    );
}

/// `Limit` must keep the highest rows, not the lowest reversed. Mirrors
/// `dynamo_scan_index_forward.rs::a_descending_limit_keeps_the_highest_rows`.
#[test]
fn a_descending_limit_keeps_the_highest_rows() {
    let seed = env_seed(0xE4C4_0002);
    let mut cluster = setup(seed);

    let (status, body) = cluster.dynamo(
        2,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}},
            "ScanIndexForward":false,"Limit":2}"#,
    );
    assert_eq!(
        status, 200,
        "descending limited query failed: {body} (seed={seed})"
    );
    assert_eq!(
        sk_order(&body),
        vec!["a5", "a4"],
        "the latest two, highest first: {body}"
    );
    assert!(
        !body.contains("\"a0\""),
        "the oldest item must not appear: {body}"
    );
    assert!(
        extract_last_evaluated_key(&body).is_some(),
        "a truncated descending page carries a cursor: {body}"
    );
}

/// Descending pagination walks the whole partition exactly once, in order,
/// with no duplicate and no gap. Mirrors `dynamo_scan_index_forward.rs::
/// descending_pagination_visits_every_item_exactly_once`.
#[test]
fn descending_pagination_visits_every_item_exactly_once() {
    let seed = env_seed(0xE4C4_0003);
    let mut cluster = setup(seed);

    let mut seen: Vec<String> = Vec::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0usize;
    loop {
        let node = pages as u64 % 3;
        pages += 1;
        assert!(
            pages < 20,
            "descending pagination did not terminate (seed={seed})"
        );
        let esk = match &cursor {
            Some(c) => format!(",\"ExclusiveStartKey\":{c}"),
            None => String::new(),
        };
        let body = format!(
            r#"{{"TableName":"events","KeyConditionExpression":"pk = :p",
                "ExpressionAttributeValues":{{":p":{{"S":"p1"}}}},
                "ConsistentRead":true,
                "ScanIndexForward":false,"Limit":2{esk}}}"#
        );
        let (status, resp) = cluster.dynamo(node, "DynamoDB_20120810.Query", body.as_bytes());
        assert_eq!(status, 200, "descending page failed: {resp} (seed={seed})");
        seen.extend(sk_order(&resp));
        match extract_last_evaluated_key(&resp) {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    assert_eq!(
        seen,
        vec!["a5", "a4", "a3", "a2", "a1", "a0"],
        "the descending walk yields every item once, in order (seed={seed})"
    );
}

/// Descending reaches an **LSI** query — strongly consistent, no polling
/// needed. Mirrors `dynamo_scan_index_forward.rs::descending_applies_to_an_
/// lsi_query`.
#[test]
fn descending_applies_to_an_lsi_query() {
    let seed = env_seed(0xE4C4_0004);
    let mut cluster = setup(seed);

    let (status, body) = cluster.dynamo(
        1,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","IndexName":"by-score","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}},
            "ScanIndexForward":false}"#,
    );
    assert_eq!(
        status, 200,
        "LSI descending query failed: {body} (seed={seed})"
    );
    // The LSI is sorted by `score` (s0..s5), which here tracks `sk` order.
    assert_eq!(
        sk_order(&body),
        vec!["a5", "a4", "a3", "a2", "a1", "a0"],
        "LSI descending order: {body}"
    );
}

/// Descending composes with a `FilterExpression`: the filter still runs
/// after `Limit`, so a descending page can be short and still carry a
/// cursor. Mirrors `dynamo_scan_index_forward.rs::descending_composes_with_
/// a_filter`.
#[test]
fn descending_composes_with_a_filter() {
    let seed = env_seed(0xE4C4_0005);
    let mut cluster = setup(seed);

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "FilterExpression":"parity = :v",
            "ExpressionAttributeValues":{":p":{"S":"p1"},":v":{"S":"even"}},
            "ScanIndexForward":false,"Limit":2}"#,
    );
    assert_eq!(
        status, 200,
        "descending filtered query failed: {body} (seed={seed})"
    );
    // Evaluates a5 and a4 (the top two), of which only a4 is even.
    assert_eq!(counts(&body), (1, 2), "one kept of two evaluated: {body}");
    assert_eq!(sk_order(&body), vec!["a4"], "a4 is the even one: {body}");
    assert!(
        extract_last_evaluated_key(&body).is_some(),
        "short descending filtered page still carries a cursor: {body}"
    );
}

/// LSI `ScanIndexForward` is numeric order for an `N` alt-sort key (ADR
/// 0063), strongly consistent. Mirrors `dynamo_scan_index_forward.rs::lsi_
/// scan_index_forward_orders_n_sort_keys_numerically`.
#[test]
fn lsi_scan_index_forward_orders_n_sort_keys_numerically() {
    let seed = env_seed(0xE4C4_0006);
    let mut cluster = setup_n_sort_keys(seed);

    let (status, asc) = cluster.dynamo(
        1,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"readings","IndexName":"by-alt","ConsistentRead":true,
            "ScanIndexForward":true,
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}}}"#,
    );
    assert_eq!(
        status, 200,
        "ascending LSI query failed: {asc} (seed={seed})"
    );
    assert_eq!(
        n_order(&asc, "alt"),
        vec!["-10", "-5", "0", "0.5", "2", "10", "100"],
        "LSI ScanIndexForward:true ascending numeric order: {asc}"
    );

    let (status, desc) = cluster.dynamo(
        2,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"readings","IndexName":"by-alt","ConsistentRead":true,
            "ScanIndexForward":false,
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}}}"#,
    );
    assert_eq!(
        status, 200,
        "descending LSI query failed: {desc} (seed={seed})"
    );
    assert_eq!(
        n_order(&desc, "alt"),
        vec!["100", "10", "2", "0.5", "0", "-5", "-10"],
        "LSI ScanIndexForward:false descending numeric order: {desc}"
    );

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"readings","IndexName":"by-alt","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p AND alt BETWEEN :lo AND :hi",
            "ExpressionAttributeValues":{":p":{"S":"p1"},":lo":{"N":"-5"},":hi":{"N":"2"}}}"#,
    );
    assert_eq!(status, 200, "LSI BETWEEN failed: {body} (seed={seed})");
    let mut between = n_order(&body, "alt");
    between.sort_by(|a, b| {
        a.parse::<f64>()
            .unwrap()
            .partial_cmp(&b.parse::<f64>().unwrap())
            .unwrap()
    });
    assert_eq!(
        between,
        vec!["-5", "0", "0.5", "2"],
        "LSI alt BETWEEN -5 AND 2: {body}"
    );

    let (status, body) = cluster.dynamo(
        1,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"readings","IndexName":"by-alt","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p AND alt < :v",
            "ExpressionAttributeValues":{":p":{"S":"p1"},":v":{"N":"0"}}}"#,
    );
    assert_eq!(status, 200, "LSI < failed: {body} (seed={seed})");
    let mut lt = n_order(&body, "alt");
    lt.sort_by(|a, b| {
        a.parse::<f64>()
            .unwrap()
            .partial_cmp(&b.parse::<f64>().unwrap())
            .unwrap()
    });
    assert_eq!(lt, vec!["-10", "-5"], "LSI alt < 0: {body}");

    let (status, body) = cluster.dynamo(
        2,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"readings","IndexName":"by-alt","ConsistentRead":true,
            "KeyConditionExpression":"pk = :p AND alt >= :v",
            "ExpressionAttributeValues":{":p":{"S":"p1"},":v":{"N":"10"}}}"#,
    );
    assert_eq!(status, 200, "LSI >= failed: {body} (seed={seed})");
    let mut ge = n_order(&body, "alt");
    ge.sort_by(|a, b| {
        a.parse::<f64>()
            .unwrap()
            .partial_cmp(&b.parse::<f64>().unwrap())
            .unwrap()
    });
    assert_eq!(ge, vec!["10", "100"], "LSI alt >= 10: {body}");
}
