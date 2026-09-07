//! `SimCluster`-driven end-to-end tests of `Query` pagination (`Limit`/
//! `ExclusiveStartKey`/`LastEvaluatedKey`/`Count`/`ScannedCount`, ADR 0061
//! rung D3 PR 3a/3b, C-04 D3) — driven through the now-generic
//! [`crate::dynamo::run_index_query`] and its GSI/LSI siblings.
//!
//! **PR 3a** replaced five of `crates/animusd/tests/dynamo_query_
//! pagination.rs`'s six tests: `base_query_paginates_a_partition_without_
//! duplicates_or_gaps`, `final_page_carries_no_last_evaluated_key`,
//! `pagination_composes_with_a_sort_key_condition`, `lsi_query_paginates_
//! with_the_scan_cursor_shape`, `cross_index_cursor_mismatch_is_rejected`
//! (the last **without** its "base cursor replayed against the GSI"
//! sub-case, since a GSI's own hidden table had no tablet under
//! `SimCluster` at all yet). **PR 3b converts the sixth and last test**,
//! `gsi_query_paginates_with_the_scan_cursor_shape`, and restores the
//! dropped sub-case above — both were blocked on a *materialized* GSI row,
//! which `SimCluster` could not produce until this rung's `[SimCluster::
//! drain_gsi]` (see `sim_cluster.rs`'s own doc). This file (and
//! `crates/animusd/tests/dynamo_query_pagination.rs`, now empty) is
//! therefore fully converted.
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

/// `table`'s own (sole) tablet id, read off node 0's own `Metadata` —
/// **not** `SimCluster::tablet_of`, which only ever knows about a
/// *hand-hosted* table's tablet (`SimCluster::create_table_with_
/// replication`'s own bookkeeping); every table in this file is created
/// through the real DynamoDB wire (`cluster.dynamo(0, "..CreateTable", ..)`)
/// so it never gets a `tablet_of` entry at all. Node 0 issued the
/// `CreateTable` itself, so its own metadata already reflects the freshly
/// committed tablet by the time that call returns 200.
fn first_tablet(cluster: &SimCluster, table: &str) -> TabletId {
    cluster
        .metadata(0)
        .tablets_for_table(table)
        .next()
        .unwrap_or_else(|| panic!("{table} has no tablet"))
        .0
        .to_owned()
}

/// Poll `body`'s `Query` on **every** node until `accept` holds on all of
/// them, or panic after a bounded number of attempts — the sim analogue of
/// `dynamo_query_pagination.rs::await_gsi_query_everywhere`'s real-socket
/// converged-or-timeout sweep. `SimCluster::drain_gsi` blocks until its own
/// `cp_kind_write_raw` calls commit on the tablet's *leader*, but says
/// nothing about how far a follower replica of the hidden GSI table has
/// applied — and a rotating pagination walk (below) samples a different
/// node's own replica on every page, so every node must agree before the
/// walk starts (see `dynamo_query_pagination.rs`'s own doc: this is the
/// exact shape of the CI failure PR #360 found, now guarded against here
/// too).
fn await_gsi_query_everywhere(
    cluster: &mut SimCluster,
    nodes: usize,
    body: &str,
    accept: impl Fn(&str) -> bool,
) {
    for _ in 0..80 {
        let mut converged = true;
        for n in 0..nodes as u64 {
            let (status, resp) = cluster.dynamo(n, "DynamoDB_20120810.Query", body.as_bytes());
            if status != 200 || !accept(&resp) {
                converged = false;
            }
        }
        if converged {
            return;
        }
        cluster.run_for(Duration::from_millis(100));
    }
    panic!("gsi query never converged across every node");
}

/// A 3-node cluster with table `events` (composite `pk`/`sk`), a hash-only
/// GSI (`by-cat`) and an LSI (`by-score`) — mirrors `dynamo_query_
/// pagination.rs::setup`: six items, one partition (`p1`), one shared GSI
/// hash value (`cat = "X"`).
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
        let body = format!(
            r#"{{"TableName":"events","Item":{{
                "pk":{{"S":"p1"}},"sk":{{"S":"a{i}"}},"cat":{{"S":"X"}},
                "score":{{"S":"s{i}"}}}}}}"#
        );
        let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.PutItem", body.as_bytes());
        assert_eq!(status, 200, "PutItem(a{i}) failed: {resp} (seed={seed})");
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

/// How many times `needle` occurs in `haystack` (non-overlapping).
fn count_occurrences(haystack: &str, needle: &str) -> usize {
    let mut count = 0;
    let mut rest = haystack;
    while let Some(at) = rest.find(needle) {
        count += 1;
        rest = &rest[at + needle.len()..];
    }
    count
}

/// Drive a `Query`'s full `LastEvaluatedKey` pagination loop, round-robining
/// across nodes 0/1/2 so the walk exercises the forwarded-read path too.
/// Mirrors `dynamo_query_pagination.rs::drain_query_pages`.
fn drain_query_pages(cluster: &mut SimCluster, request_prefix: &str, limit: usize) -> String {
    let mut combined = String::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0usize;
    loop {
        let node = pages as u64 % 3;
        pages += 1;
        assert!(pages < 50, "pagination did not terminate: {combined}");
        let esk = match &cursor {
            Some(c) => format!(",\"ExclusiveStartKey\":{c}"),
            None => String::new(),
        };
        let body = format!("{request_prefix},\"ConsistentRead\":true,\"Limit\":{limit}{esk}}}");
        let (status, resp) = cluster.dynamo(node, "DynamoDB_20120810.Query", body.as_bytes());
        assert_eq!(status, 200, "query page failed: {resp}");
        let items_part = resp
            .find("\"LastEvaluatedKey\":")
            .map_or(resp.as_str(), |at| &resp[..at]);
        combined.push_str(items_part);
        combined.push('\n');
        match extract_last_evaluated_key(&resp) {
            Some(next) => cursor = Some(next),
            None => return combined,
        }
    }
}

/// A base `Query` over a partition bigger than `Limit` pages cleanly: every
/// item appears in exactly one page, and only the final page omits
/// `LastEvaluatedKey`. Mirrors `dynamo_query_pagination.rs::base_query_
/// paginates_a_partition_without_duplicates_or_gaps`.
#[test]
fn base_query_paginates_a_partition_without_duplicates_or_gaps() {
    let seed = env_seed(0xE4C2_0001);
    let mut cluster = setup(seed);

    let (status, page1) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","Limit":2,"ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}}}"#,
    );
    assert_eq!(status, 200, "{page1} (seed={seed})");
    assert!(page1.contains("\"Count\":2"), "{page1}");
    assert!(page1.contains("\"ScannedCount\":2"), "{page1}");
    assert!(page1.contains("\"LastEvaluatedKey\""), "{page1}");

    let combined = drain_query_pages(
        &mut cluster,
        r#"{"TableName":"events",
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}}"#,
        2,
    );
    for i in 0..6 {
        let marker = format!(r#""sk":{{"S":"a{i}"}}"#);
        assert_eq!(
            count_occurrences(&combined, &marker),
            1,
            "expected exactly one page to carry sk=a{i}, got pages:\n{combined} (seed={seed})"
        );
    }
    assert_eq!(count_occurrences(&combined, "\"Count\":2"), 3, "{combined}");
}

/// A `Query`'s final page carries no `LastEvaluatedKey`, even when `Limit`
/// exactly matches the partition's size. Mirrors `dynamo_query_pagination.rs::
/// final_page_carries_no_last_evaluated_key`.
#[test]
fn final_page_carries_no_last_evaluated_key() {
    let seed = env_seed(0xE4C2_0002);
    let mut cluster = setup(seed);

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","Limit":6,"ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}}}"#,
    );
    assert_eq!(status, 200, "query failed: {body} (seed={seed})");
    assert!(body.contains("\"Count\":6"), "{body}");
    assert!(!body.contains("LastEvaluatedKey"), "{body}");

    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.Query",
        br#"{"TableName":"events","Limit":100,"ConsistentRead":true,
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{":p":{"S":"p1"}}}"#,
    );
    assert_eq!(status, 200, "query failed: {body} (seed={seed})");
    assert!(body.contains("\"Count\":6"), "{body}");
    assert!(!body.contains("LastEvaluatedKey"), "{body}");
}

/// A `SortKeyCondition` narrows *before* paging: walking the whole cursor
/// chain visits exactly the narrowed items, no more, no fewer. Mirrors
/// `dynamo_query_pagination.rs::pagination_composes_with_a_sort_key_
/// condition`.
#[test]
fn pagination_composes_with_a_sort_key_condition() {
    let seed = env_seed(0xE4C2_0003);
    let mut cluster = setup(seed);

    let combined = drain_query_pages(
        &mut cluster,
        r#"{"TableName":"events",
            "KeyConditionExpression":"pk = :p AND sk BETWEEN :lo AND :hi",
            "ExpressionAttributeValues":
                {":p":{"S":"p1"},":lo":{"S":"a1"},":hi":{"S":"a4"}}"#,
        2,
    );
    for i in 1..=4 {
        let marker = format!(r#""sk":{{"S":"a{i}"}}"#);
        assert_eq!(
            count_occurrences(&combined, &marker),
            1,
            "expected exactly one page to carry sk=a{i}, got pages:\n{combined} (seed={seed})"
        );
    }
    for excluded in ["a0", "a5"] {
        let marker = format!(r#""sk":{{"S":"{excluded}"}}"#);
        assert_eq!(
            count_occurrences(&combined, &marker),
            0,
            "sk={excluded} is outside the sort condition, got pages:\n{combined} (seed={seed})"
        );
    }
}

/// An **LSI** `Query` paginates with the exact same cursor shape
/// [`crate::dynamo::run_lsi_scan`] already uses (the index's own alt-sort
/// attribute plus the base table's key attributes) — strongly consistent, no
/// convergence poll needed. Mirrors `dynamo_query_pagination.rs::lsi_query_
/// paginates_with_the_scan_cursor_shape`.
#[test]
fn lsi_query_paginates_with_the_scan_cursor_shape() {
    let seed = env_seed(0xE4C2_0004);
    let mut cluster = setup(seed);

    let mut combined = String::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0usize;
    let mut saw_cursor = false;
    loop {
        let node = pages as u64 % 3;
        pages += 1;
        assert!(pages < 50, "pagination did not terminate: {combined}");
        let esk = match &cursor {
            Some(c) => format!(",\"ExclusiveStartKey\":{c}"),
            None => String::new(),
        };
        let body = format!(
            r#"{{"TableName":"events","IndexName":"by-score",
                "ConsistentRead":true,
                "KeyConditionExpression":"pk = :p",
                "ExpressionAttributeValues":{{":p":{{"S":"p1"}}}},
                "Limit":2{esk}}}"#
        );
        let (status, resp) = cluster.dynamo(node, "DynamoDB_20120810.Query", body.as_bytes());
        assert_eq!(status, 200, "lsi query page failed: {resp} (seed={seed})");
        let items_part = resp
            .find("\"LastEvaluatedKey\":")
            .map_or(resp.as_str(), |at| &resp[..at]);
        combined.push_str(items_part);
        combined.push('\n');
        match extract_last_evaluated_key(&resp) {
            Some(next) => {
                assert!(
                    next.contains("\"score\"")
                        && next.contains("\"pk\"")
                        && next.contains("\"sk\""),
                    "LSI cursor missing expected attributes: {next} (seed={seed})"
                );
                saw_cursor = true;
                cursor = Some(next);
            }
            None => break,
        }
    }
    assert!(
        saw_cursor,
        "expected at least one truncated page (seed={seed})"
    );
    for i in 0..6 {
        let marker = format!(r#""sk":{{"S":"a{i}"}}"#);
        assert_eq!(
            count_occurrences(&combined, &marker),
            1,
            "expected exactly one page to carry sk=a{i}, got pages:\n{combined} (seed={seed})"
        );
    }
}

/// A cursor built for one target (base/GSI/LSI) is rejected with
/// `ValidationException` when replayed against a different one —
/// `validate_query_cursor_shape` checks only the `ExclusiveStartKey`'s
/// attribute *names*, so this hand-crafts each cursor shape directly rather
/// than reading one back off a real GSI page. Mirrors `dynamo_query_
/// pagination.rs::cross_index_cursor_mismatch_is_rejected` — **all four**
/// sub-cases, including "base cursor replayed against the GSI": PR 3b (ADR
/// 0061 rung D3) added [`SimCluster::drain_gsi`], a test-only stand-in for
/// `index_drain::change_consumer_loop`'s GSI-drain arm that materializes a
/// hidden GSI table's tablet, closing the gap PR 3a's own version of this
/// test found and documented (`run_gsi_query`'s own
/// `!meta.has_table_tablet(&index_table)` empty-page gate used to be
/// unconditionally true here, short-circuiting every GSI query to an empty
/// `200` before `validate_query_cursor_shape` was ever reached — see PR 3a's
/// now-superseded doc, still in git history, for the original account). A
/// single `drain_gsi` call after `setup`'s writes is all that's needed: it
/// doesn't matter that the drained rows don't happen to match `by-cat`'s own
/// shape for this particular assertion — only that the hidden table has a
/// tablet at all, which is what lets the cursor-shape check run.
#[test]
fn cross_index_cursor_mismatch_is_rejected() {
    let seed = env_seed(0xE4C2_0005);
    let mut cluster = setup(seed);
    let tablet = first_tablet(&cluster, "events");
    let leader = cluster
        .leader_index_of(tablet)
        .expect("events tablet has a leader");
    cluster.drain_gsi(leader, "events");

    let base_cursor = r#"{"pk":{"S":"p1"},"sk":{"S":"a0"}}"#;
    let lsi_cursor = r#"{"score":{"S":"s0"},"pk":{"S":"p1"},"sk":{"S":"a0"}}"#;
    let gsi_cursor = r#"{"cat":{"S":"X"},"pk":{"S":"p1"},"sk":{"S":"a0"}}"#;

    // GSI cursor replayed against the base table: rejected (extra `cat`).
    let body = format!(
        r#"{{"TableName":"events","ExclusiveStartKey":{gsi_cursor},
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{{":p":{{"S":"p1"}}}}}}"#
    );
    let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.Query", body.as_bytes());
    assert_eq!(
        status, 400,
        "GSI cursor accepted on base Query: {resp} (seed={seed})"
    );
    assert!(resp.contains("ValidationException"), "got: {resp}");

    // LSI cursor replayed against the base table: rejected (extra `score`).
    let body = format!(
        r#"{{"TableName":"events","ExclusiveStartKey":{lsi_cursor},
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{{":p":{{"S":"p1"}}}}}}"#
    );
    let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.Query", body.as_bytes());
    assert_eq!(
        status, 400,
        "LSI cursor accepted on base Query: {resp} (seed={seed})"
    );
    assert!(resp.contains("ValidationException"), "got: {resp}");

    // Base cursor replayed against the GSI: rejected (missing `cat`).
    let body = format!(
        r#"{{"TableName":"events","IndexName":"by-cat","ExclusiveStartKey":{base_cursor},
            "KeyConditionExpression":"cat = :c",
            "ExpressionAttributeValues":{{":c":{{"S":"X"}}}}}}"#
    );
    let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.Query", body.as_bytes());
    assert_eq!(
        status, 400,
        "base cursor accepted on GSI Query: {resp} (seed={seed})"
    );
    assert!(resp.contains("ValidationException"), "got: {resp}");

    // GSI cursor replayed against the LSI: rejected (`cat` foreign, `score`
    // missing).
    let body = format!(
        r#"{{"TableName":"events","IndexName":"by-score","ExclusiveStartKey":{gsi_cursor},
            "KeyConditionExpression":"pk = :p",
            "ExpressionAttributeValues":{{":p":{{"S":"p1"}}}}}}"#
    );
    let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.Query", body.as_bytes());
    assert_eq!(
        status, 400,
        "GSI cursor accepted on LSI Query: {resp} (seed={seed})"
    );
    assert!(resp.contains("ValidationException"), "got: {resp}");
}

/// A GSI `Query` paginates with the same cursor shape (`{cat, pk, sk}`) as
/// its own hidden table's engine key — the GSI-half `dynamo_query_
/// pagination.rs::gsi_query_paginates_with_the_scan_cursor_shape` used to be
/// the only sub-case blocked on a materialized GSI row; `[SimCluster::
/// drain_gsi]` closes that gap (see this file's own module doc).
#[test]
fn gsi_query_paginates_with_the_scan_cursor_shape() {
    let seed = env_seed(0xE4C2_0006);
    let mut cluster = setup(seed);
    let tablet = first_tablet(&cluster, "events");
    let leader = cluster
        .leader_index_of(tablet)
        .expect("events tablet has a leader");
    cluster.drain_gsi(leader, "events");

    // Every node, not just one (ADR 0055, mirroring the `ProdEnv` original's
    // own reasoning): the walk below rotates across all three, and each now
    // answers from its own replica of the hidden GSI table — so one node
    // having converged says nothing about the next page's node.
    await_gsi_query_everywhere(
        &mut cluster,
        3,
        r#"{"TableName":"events","IndexName":"by-cat",
            "KeyConditionExpression":"cat = :c",
            "ExpressionAttributeValues":{":c":{"S":"X"}}}"#,
        |b| b.contains("\"Count\":6"),
    );

    // Drive pagination by hand so every page's raw `LastEvaluatedKey` JSON
    // can be checked for its exact attribute set (the index's own hash
    // attribute plus the base table's `pk`/`sk` — exactly
    // `gsi_key_item_of`'s shape) before moving on.
    let mut combined = String::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0usize;
    let mut saw_cursor = false;
    loop {
        let node = pages as u64 % 3;
        pages += 1;
        assert!(
            pages < 50,
            "pagination did not terminate: {combined} (seed={seed})"
        );
        let esk = match &cursor {
            Some(c) => format!(",\"ExclusiveStartKey\":{c}"),
            None => String::new(),
        };
        let body = format!(
            // Deliberately no `ConsistentRead` here, unlike the base/LSI
            // walks: a GSI rejects it (ADR 0041 §5). The convergence sweep
            // above is what makes this rotating walk stable instead.
            r#"{{"TableName":"events","IndexName":"by-cat",
                "KeyConditionExpression":"cat = :c",
                "ExpressionAttributeValues":{{":c":{{"S":"X"}}}},
                "Limit":2{esk}}}"#
        );
        let (status, resp) = cluster.dynamo(node, "DynamoDB_20120810.Query", body.as_bytes());
        assert_eq!(status, 200, "gsi query page failed: {resp} (seed={seed})");
        let items_part = resp
            .find("\"LastEvaluatedKey\":")
            .map_or(resp.as_str(), |at| &resp[..at]);
        combined.push_str(items_part);
        combined.push('\n');
        match extract_last_evaluated_key(&resp) {
            Some(next) => {
                assert!(
                    next.contains("\"cat\"") && next.contains("\"pk\"") && next.contains("\"sk\""),
                    "GSI cursor missing expected attributes: {next} (seed={seed})"
                );
                saw_cursor = true;
                cursor = Some(next);
            }
            None => break,
        }
    }
    assert!(
        saw_cursor,
        "expected at least one truncated page (seed={seed})"
    );
    for i in 0..6 {
        let marker = format!(r#""sk":{{"S":"a{i}"}}"#);
        assert_eq!(
            count_occurrences(&combined, &marker),
            1,
            "expected exactly one page to carry sk=a{i}, got pages:\n{combined} (seed={seed})"
        );
    }
}
