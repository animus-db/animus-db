//! One function per fuzz target. Each takes the raw fuzzer input and drives a
//! family of untrusted-input decoders; the property is always the same:
//! **never panic, never hang, never allocate unboundedly, and return `Ok` or a
//! named error.** libFuzzer's own harness and `tests/smoke.rs` both call these
//! — the latter on stable, over seed inputs plus seeded mutations.
//!
//! Multi-decoder targets use a **name line** routing convention: the input is
//! `<name>\n<body>`; `<name>` picks the decoder (an unknown name is hashed onto
//! one, so every input still exercises something) and `<body>` is handed to it
//! verbatim. Seeds are therefore human-readable and a dictionary of the names
//! (`fuzz/dict/`) lets libFuzzer splice them.

use std::collections::BTreeMap;

use animus_item::{AttributeValue, Item};

/// Split `data` into a decoder-name line and a body, and resolve the name to an
/// index into `names` (unknown names hash onto one so no input is wasted).
#[must_use]
pub fn route<'a>(data: &'a [u8], names: &[&str]) -> (usize, &'a [u8]) {
    let (head, body) = match data.iter().position(|&b| b == b'\n') {
        Some(i) => (&data[..i], &data[i + 1..]),
        None => (data, &[][..]),
    };
    if let Ok(name) = std::str::from_utf8(head)
        && let Some(i) = names.iter().position(|n| *n == name.trim())
    {
        return (i, body);
    }
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for b in head {
        h = (h ^ u64::from(*b)).wrapping_mul(0x0100_0000_01b3);
    }
    ((h % names.len() as u64) as usize, body)
}

/// Lossy UTF-8 view of `bytes` (every `&str` parser is exercised on invalid
/// UTF-8 input too, via replacement characters).
fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// A fixed, nested item the evaluators are run against.
fn seed_item() -> Item {
    let mut m = BTreeMap::new();
    m.insert("x".to_owned(), AttributeValue::N("7".into()));
    let mut item = Item::new();
    item.insert("pk".into(), AttributeValue::S("k".into()));
    item.insert("a".into(), AttributeValue::S("hello".into()));
    item.insert("b".into(), AttributeValue::N("10".into()));
    item.insert(
        "l".into(),
        AttributeValue::L(vec![
            AttributeValue::N("1".into()),
            AttributeValue::S("two".into()),
            AttributeValue::M(m.clone()),
        ]),
    );
    item.insert("m".into(), AttributeValue::M(m));
    item.insert(
        "ss".into(),
        AttributeValue::SS(vec!["a".into(), "b".into()]),
    );
    item.insert(
        "ns".into(),
        AttributeValue::NS(vec!["1".into(), "2".into()]),
    );
    item.insert("bin".into(), AttributeValue::B(vec![1, 2, 3]));
    item.insert("flag".into(), AttributeValue::Bool(true));
    item.insert("nul".into(), AttributeValue::Null);
    item
}

/// Re-stamp the `<crc32 hex>:` prefix of every `\n`-terminated line so a
/// mutated line body is not rejected at the checksum before the payload
/// decoder ever runs (libFuzzer cannot solve a CRC). The line framing shared
/// by the control WAL, the shared WAL and the RaftKV WAL.
#[must_use]
pub fn fix_line_crcs(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len() + 16);
    for line in data.split_inclusive(|&b| b == b'\n') {
        let (content, nl) = match line.strip_suffix(b"\n") {
            Some(c) => (c, true),
            None => (line, false),
        };
        match content.iter().position(|&b| b == b':') {
            Some(colon) if colon <= 8 => {
                let body = &content[colon + 1..];
                out.extend_from_slice(format!("{:08x}:", crc32fast::hash(body)).as_bytes());
                out.extend_from_slice(body);
            }
            _ => out.extend_from_slice(content),
        }
        if nl {
            out.push(b'\n');
        }
    }
    out
}

/// Replace the final four bytes with the little-endian CRC32 of everything
/// before them (the SSTable block-index region's trailer).
#[must_use]
pub fn fix_tail_crc_le(data: &[u8]) -> Vec<u8> {
    if data.len() < 4 {
        return data.to_vec();
    }
    let (body, _) = data.split_at(data.len() - 4);
    let mut out = body.to_vec();
    out.extend_from_slice(&crc32fast::hash(body).to_le_bytes());
    out
}

/// Wrap `body` as the single record frame (`len u32 BE | crc u32 BE | body`)
/// of an LSM WAL file with the given header version.
#[must_use]
pub fn wal_file_with_frame(version: u8, body: &[u8]) -> Vec<u8> {
    let mut out = b"LWL1".to_vec();
    out.push(version);
    out.extend_from_slice(&(body.len() as u32).to_be_bytes());
    out.extend_from_slice(&crc32fast::hash(body).to_be_bytes());
    out.extend_from_slice(body);
    out
}

// ---------------------------------------------------------------------------
// dynamo_request
// ---------------------------------------------------------------------------

const DYNAMO_OPS: &[&str] = &[
    "CreateTable",
    "UpdateTable",
    "DescribeTable",
    "DeleteTable",
    "ListTables",
    "PutItem",
    "GetItem",
    "DeleteItem",
    "Query",
    "Scan",
    "UpdateItem",
    "BatchWriteItem",
    "TransactWriteItems",
    "BatchGetItem",
    "TransactGetItems",
    "ExecuteStatement",
    "BatchExecuteStatement",
    "ExecuteTransaction",
    "UpdateTimeToLive",
    "DescribeTimeToLive",
    "UpdateContinuousBackups",
    "DescribeContinuousBackups",
    "CreateBackup",
    "DescribeBackup",
    "ListBackups",
    "DeleteBackup",
    "ExportTableToPointInTime",
    "DescribeExport",
    "ListExports",
    "ImportTable",
    "DescribeImport",
    "ListImports",
    "RestoreTableFromBackup",
    "RestoreTableToPointInTime",
    "TagResource",
    "UntagResource",
    "ListTagsOfResource",
    "DescribeLimits",
    "DescribeEndpoints",
    // DynamoDB Streams wire.
    "streams:ListStreams",
    "streams:DescribeStream",
    "streams:GetShardIterator",
    "streams:GetRecords",
    // Standalone decoders reached from the wire edge.
    "item",
    "stored_item",
    "tokens",
];

/// Run the evaluators a successfully decoded request would be handed to, so a
/// parse that succeeds is then *used* (apply/evaluate/project) too.
fn exercise(op: &animus_dynamo::wire::Operation) {
    use animus_dynamo::wire::Operation;
    let item = seed_item();
    match op {
        Operation::UpdateItem {
            actions, condition, ..
        } => {
            if let Some(c) = condition {
                let _ = c.evaluate(None);
                let _ = c.evaluate(Some(&item));
            }
            let _ = animus_dynamo::wire::apply_update(Item::new(), actions);
            let _ = animus_dynamo::wire::apply_update(item, actions);
        }
        Operation::PutItem { condition, .. } | Operation::DeleteItem { condition, .. } => {
            if let Some(c) = condition {
                let _ = c.evaluate(None);
                let _ = c.evaluate(Some(&item));
            }
        }
        Operation::GetItem { projection, .. } => {
            let _ = animus_dynamo::wire::project(projection.as_ref(), &item);
        }
        Operation::Scan {
            filter, projection, ..
        } => {
            if let Some(c) = filter {
                let _ = c.evaluate(Some(&item));
            }
            let _ = animus_dynamo::wire::project(projection.as_ref(), &item);
        }
        Operation::Query {
            filter,
            projection,
            sort_condition,
            ..
        } => {
            if let Some(c) = filter {
                let _ = c.evaluate(Some(&item));
            }
            if let Some(s) = sort_condition {
                for v in item.values() {
                    let _ = s.matches(v);
                }
            }
            let _ = animus_dynamo::wire::project(projection.as_ref(), &item);
        }
        _ => {}
    }
}

/// DynamoDB JSON request decode (`wire::decode_request` for every supported
/// operation, `streams_wire::decode_request`) plus the standalone item / stored
/// value / token / ARN parsers. Input: `<Operation>\n<JSON body>`.
pub fn dynamo_request(data: &[u8]) {
    use animus_dynamo::{streams_wire, wire};
    let (idx, body) = route(data, DYNAMO_OPS);
    let name = DYNAMO_OPS[idx];
    if let Some(op) = name.strip_prefix("streams:") {
        let _ = streams_wire::decode_request(&format!("{}{op}", streams_wire::TARGET_PREFIX), body);
        return;
    }
    match name {
        "item" => {
            if let Ok(serde_json::Value::Object(map)) = serde_json::from_slice(body)
                && let Ok(item) = wire::decode_item(&map)
            {
                // Re-encode: the encode half must accept anything decode produced.
                let _ = wire::encode_item(&item);
            }
        }
        "stored_item" => {
            let _ = wire::decode_stored_item(body);
        }
        "tokens" => {
            let s = text(body);
            let _ = streams_wire::decode_iterator(&s);
            let _ = streams_wire::parse_sequence_number(&s);
            let _ = streams_wire::parse_shard_id(&s);
            let _ = streams_wire::parse_stream_arn(&s);
            let _ = wire::parse_table_arn(&s);
        }
        op => {
            if let Ok(decoded) = wire::decode_request(&format!("DynamoDB_20120810.{op}"), body) {
                exercise(&decoded);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// dynamo_expressions
// ---------------------------------------------------------------------------

/// Structure-aware expression fuzzing: the input is up to five `\n`-separated
/// expression strings — `UpdateExpression`, `ConditionExpression`,
/// `ProjectionExpression`, `KeyConditionExpression`, `FilterExpression` — each
/// embedded (JSON-escaped, so any bytes survive) into the request shapes that
/// accept it, with a fixed set of `ExpressionAttributeNames`/`Values`. This is
/// what reaches the UpdateExpression / ConditionExpression / projection /
/// key-condition parsers, which are private to `animus-dynamo` and only
/// reachable through `decode_request`.
pub fn dynamo_expressions(data: &[u8]) {
    use serde_json::{Value, json};
    let t = text(data);
    let mut parts = t.split('\n');
    let mut next = || parts.next().filter(|s| !s.is_empty()).map(str::to_owned);
    let update = next();
    let cond = next();
    let proj = next();
    let key_cond = next();
    let filter = next();

    let names = json!({"#a": "a", "#b": "b", "#n": "m", "#l": "l"});
    let values = json!({
        ":v": {"N": "1"}, ":w": {"N": "2"}, ":s": {"S": "x"}, ":p": {"S": "k"},
        ":l": {"L": [{"N": "1"}]}, ":ss": {"SS": ["a"]}, ":b": {"BOOL": true},
        ":m": {"M": {"x": {"N": "1"}}}, ":z": {"NULL": true}, ":bin": {"B": "AQID"}
    });
    let base = |extra: Vec<(&str, Option<String>)>| -> Value {
        let mut o = json!({
            "TableName": "t",
            "Key": {"pk": {"S": "k"}},
            "ExpressionAttributeNames": names,
            "ExpressionAttributeValues": values,
        });
        for (k, v) in extra {
            if let Some(v) = v {
                o[k] = Value::String(v);
            }
        }
        o
    };
    let bodies: [(&str, Value); 7] = [
        (
            "UpdateItem",
            base(vec![
                ("UpdateExpression", update.clone()),
                ("ConditionExpression", cond.clone()),
            ]),
        ),
        (
            "GetItem",
            base(vec![("ProjectionExpression", proj.clone())]),
        ),
        (
            "PutItem",
            json!({"TableName": "t", "Item": {"pk": {"S": "k"}},
                   "ConditionExpression": cond.clone().unwrap_or_default(),
                   "ExpressionAttributeNames": names, "ExpressionAttributeValues": values}),
        ),
        (
            "DeleteItem",
            base(vec![("ConditionExpression", cond.clone())]),
        ),
        (
            "Query",
            base(vec![
                (
                    "KeyConditionExpression",
                    key_cond.clone().or(Some("pk = :p".into())),
                ),
                ("FilterExpression", filter.clone()),
                ("ProjectionExpression", proj.clone()),
            ]),
        ),
        (
            "Scan",
            base(vec![
                ("FilterExpression", filter.clone().or(cond.clone())),
                ("ProjectionExpression", proj.clone()),
            ]),
        ),
        (
            "TransactWriteItems",
            json!({"TransactItems": [{"Update": {
                "TableName": "t", "Key": {"pk": {"S": "k"}},
                "UpdateExpression": update.clone().unwrap_or_default(),
                "ConditionExpression": cond.clone().unwrap_or_default(),
                "ExpressionAttributeNames": names, "ExpressionAttributeValues": values}}]}),
        ),
    ];
    for (op, body) in bodies {
        let bytes = serde_json::to_vec(&body).expect("json value serializes");
        if let Ok(decoded) =
            animus_dynamo::wire::decode_request(&format!("DynamoDB_20120810.{op}"), &bytes)
        {
            exercise(&decoded);
        }
    }
}

// ---------------------------------------------------------------------------
// partiql
// ---------------------------------------------------------------------------

/// PartiQL lexer/parser (`parse_statement`), every lowering of whatever parsed
/// (`lower_*`, including the transactional forms), and the `NextToken` decoder.
pub fn partiql(data: &[u8]) {
    use animus_dynamo::partiql::{self, Statement};
    use animus_dynamo::wire::ReturnValuesOnConditionCheckFailure as Rvocf;
    let s = text(data);
    let _ = partiql::decode_next_token(&s, "SELECT * FROM t");
    let Ok(stmt) = partiql::parse_statement(&s) else {
        return;
    };
    let _ = stmt.table();
    let q = s.matches('?').count();
    // The statement's placeholder count is private; every `?` in the text is
    // an upper bound, so sweep the plausible parameter counts (a mismatch is a
    // named error, the matching count reaches the lowering proper).
    let counts: Vec<usize> = (0..=q.min(6)).chain([q]).collect();
    for n in counts {
        let p = vec![AttributeValue::S("x".into()); n];
        for sk in [None, Some("sk")] {
            match &stmt {
                Statement::Select(sel) => {
                    let _ = partiql::select_is_exact_key(sel, "pk", sk);
                    let _ = partiql::lower_select(sel, &p, "pk", sk, None, Some(10), true);
                    let _ = partiql::lower_select_to_transact_get(sel, &p, "pk", sk);
                }
                Statement::Insert(ins) => {
                    let _ = partiql::lower_insert(ins, &p, "pk", sk);
                    let _ =
                        partiql::lower_insert_to_transact_action(ins, &p, "pk", sk, Rvocf::AllOld);
                }
                Statement::Update(upd) => {
                    let _ = partiql::lower_update(upd, &p, "pk", sk);
                    let _ =
                        partiql::lower_update_to_transact_action(upd, &p, "pk", sk, Rvocf::None);
                }
                Statement::Delete(del) => {
                    let _ = partiql::lower_delete(del, &p, "pk", sk);
                    let _ =
                        partiql::lower_delete_to_transact_action(del, &p, "pk", sk, Rvocf::None);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// http_sigv4
// ---------------------------------------------------------------------------

/// The HTTP request parser (`animus_node::http::parse_request_head` and its
/// query/percent helpers) and, on whatever parsed, SigV4 credential parsing
/// and full verification. Input: a raw HTTP request — header block, a blank
/// line, then the body.
pub fn http_sigv4(data: &[u8]) {
    use animus_dynamo::sigv4::{self, SigV4Request};
    use animus_node::http;
    let (head, body) = match http::find_subslice(data, b"\r\n\r\n") {
        Some(i) => (&data[..i], &data[i + 4..]),
        None => match http::find_subslice(data, b"\n\n") {
            Some(i) => (&data[..i], &data[i + 2..]),
            None => (data, &[][..]),
        },
    };
    let head_text = text(head);
    let Ok(parsed) = http::parse_request_head(&head_text) else {
        return;
    };
    let _ = http::percent_decode(&parsed.path);
    let _ = http::percent_decode(&parsed.query);
    let _ = http::query_param(&parsed.query, "Action");
    let _ = http::query_param(&parsed.query, "X-Amz-Signature");
    let _ = http::reason(200);
    let req = SigV4Request {
        method: &parsed.method,
        path: &parsed.path,
        query: &parsed.query,
        headers: &parsed.headers,
        body,
    };
    let _ = sigv4::parse_credential(&req);
    let mut creds = BTreeMap::new();
    creds.insert(
        "AKIDEXAMPLE".to_owned(),
        "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".to_owned(),
    );
    // 2015-08-30T12:36:00Z, the AWS SigV4 test-suite instant.
    for now in [1_440_938_160_000u64, 0, u64::MAX] {
        let _ = sigv4::verify(&req, &creds, now);
    }
}

// ---------------------------------------------------------------------------
// item_codecs
// ---------------------------------------------------------------------------

const ITEM_CODECS: &[&str] = &[
    "stored_item",
    "change_record",
    "footprint",
    "numkey",
    "gsi_key",
    "lsi_key",
];

/// The `animus-item` stored-value codecs and key parsers (ADR 0073 formats:
/// stored item, change record; plus the footprint value, the `numkey`
/// number encoding and GSI/LSI row-key parsing).
pub fn item_codecs(data: &[u8]) {
    use animus_item::index::{ChangeRecord, ItemFootprint, parse_gsi_row_key, parse_lsi_row_key};
    let (idx, body) = route(data, ITEM_CODECS);
    match ITEM_CODECS[idx] {
        "stored_item" => {
            let _ = animus_item::stored::decode_stored_item(body);
            let _ = animus_item::stored::stored_item_version(body);
        }
        "change_record" => {
            let _ = ChangeRecord::version_of(body);
            if let Some(r) = ChangeRecord::decode(body) {
                // Round trip: whatever decodes must re-encode.
                let _ = r.encode();
            }
        }
        "footprint" => {
            let _ = animus_item::index::IndexFootprint::decode(body);
            let _ = serde_json::from_slice::<ItemFootprint>(body);
        }
        "numkey" => {
            let s = text(body);
            let _ = animus_item::numkey::encode(&s);
            if let Some(enc) = animus_item::numkey::encode_checked(&s) {
                // An encoded number must decode back.
                let _ = animus_item::numkey::decode(&enc);
            }
            let _ = animus_item::numkey::decode(body);
            let _ = animus_item::numkey::is_encoded(body);
        }
        "gsi_key" => {
            let _ = parse_gsi_row_key(body, false);
            let _ = parse_gsi_row_key(body, true);
        }
        "lsi_key" => {
            let _ = parse_lsi_row_key(body);
        }
        _ => unreachable!("route returns an index into ITEM_CODECS"),
    }
}

// ---------------------------------------------------------------------------
// net_frames
// ---------------------------------------------------------------------------

const NET_FRAMES: &[&str] = &[
    "client_request",
    "client_response",
    "handshake",
    "not_leader",
    "frame_len",
];

/// Network-facing frame decoders: the client-port `ClientRequest`/
/// `ClientResponse` JSON frames, the connection-handshake preamble (and its
/// extension area) and the not-leader refusal text parser.
pub fn net_frames(data: &[u8]) {
    use animus_env::handshake;
    let (idx, body) = route(data, NET_FRAMES);
    match NET_FRAMES[idx] {
        "client_request" => {
            let _ = animus_node::decode_client_frame::<animus_node::ClientRequest>(body);
        }
        "client_response" => {
            let _ = animus_node::decode_client_frame::<animus_node::ClientResponse>(body);
        }
        "handshake" => {
            if let Ok((pre, used)) = handshake::decode(body) {
                assert!(used <= body.len(), "decode reported {used} > input");
                let _ = handshake::check_peer(&handshake::NETWORK_PROTOCOL, &pre);
                let _ = handshake::check_peer(&handshake::CLIENT_PROTOCOL, &pre);
                let _ = handshake::parse_ext_range(&pre.extensions);
                let _ = handshake::parse_ext_build(&pre.extensions);
            }
            let _ = handshake::parse_ext_range(body);
            let _ = handshake::parse_ext_build(body);
        }
        "not_leader" => {
            let _ = animus_node::topology::parse_not_leader_refusal(&text(body));
        }
        "frame_len" => {
            if let Ok(b) = <[u8; 4]>::try_from(body.get(..4).unwrap_or(&[])) {
                let _ = animus_node::frame_payload_len(u32::from_be_bytes(b));
            }
        }
        _ => unreachable!("route returns an index into NET_FRAMES"),
    }
}

// ---------------------------------------------------------------------------
// lsm_formats
// ---------------------------------------------------------------------------

const LSM_FORMATS: &[&str] = &[
    "wal",
    "wal_record",
    "manifest",
    "sstable_block",
    "sstable_index",
    "sstable_image",
];

/// `animus-storage`'s durable formats: LSM WAL (whole file and bare record),
/// manifest, SSTable data block, block index and a whole SSTable image opened
/// and scanned through a `SimEnv` disk.
pub fn lsm_formats(data: &[u8]) {
    use animus_storage::fuzzing as f;
    let (idx, body) = route(data, LSM_FORMATS);
    match LSM_FORMATS[idx] {
        "wal" => {
            let _ = f::wal(body);
            // Same bytes as the one record of a well-framed file, so the
            // record decoder is reached past the length/CRC framing.
            for v in [1, 2] {
                let _ = f::wal(&wal_file_with_frame(v, body));
            }
        }
        "wal_record" => {
            let _ = f::wal_record(body);
        }
        "manifest" => {
            let _ = f::manifest(body);
        }
        "sstable_block" => {
            let _ = f::sstable_block(body);
        }
        "sstable_index" => {
            let _ = f::sstable_index(body);
            let _ = f::sstable_index(&fix_tail_crc_le(body));
        }
        "sstable_image" => {
            use animus_env::Disk;
            let sim = animus_sim::Simulator::new(1);
            let env = sim.env(animus_env::nid(0));
            futures::executor::block_on(async {
                if env.append("img", body).await.is_ok() && env.sync("img").await.is_ok() {
                    let _ = f::sstable_image(&env, "img", body).await;
                }
            });
        }
        _ => unreachable!("route returns an index into LSM_FORMATS"),
    }
}

// ---------------------------------------------------------------------------
// control_formats
// ---------------------------------------------------------------------------

const CONTROL_FORMATS: &[&str] = &[
    "control_wal",
    "control_snapshot",
    "shared_wal",
    "metadata",
    "syskv_key",
];

/// `animus-control`'s durable formats: the control Raft WAL (`CWL1`), the
/// shared-WAL envelope (`SWL1`), the control snapshot image, the `Metadata`
/// JSON document and the system-keyspace key decoders.
pub fn control_formats(data: &[u8]) {
    use animus_control::persist::PersistedState;
    use animus_control::{MetaCommand, Metadata, syskv};
    let (idx, body) = route(data, CONTROL_FORMATS);
    match CONTROL_FORMATS[idx] {
        "control_wal" => {
            for b in [body.to_vec(), fix_line_crcs(body)] {
                let _ = PersistedState::<MetaCommand, Metadata>::decode(&b);
                let _ = PersistedState::<MetaCommand, Metadata>::decode_with_extent(&b);
            }
        }
        "control_snapshot" => {
            let _ = animus_control::node::decode_syskv_image_bytes(body);
        }
        "shared_wal" => {
            for b in [body.to_vec(), fix_line_crcs(body)] {
                let _ = PersistedState::<MetaCommand, Metadata>::decode_tagged(&b);
                let _ = PersistedState::<MetaCommand, Metadata>::decode_tagged_with_extent(&b);
            }
        }
        "metadata" => {
            if let Ok(m) = Metadata::from_json(body) {
                // Whatever decodes must re-encode.
                let _ = serde_json::to_vec(&m);
            }
        }
        "syskv_key" => {
            let _ = syskv::decode_key(body);
            let _ = syskv::decode_stream_shard_id(body);
            let _ = syskv::decode_pitr_segment_id(body);
            let _ = syskv::decode_index_backfill_id(body);
            let _ = syskv::decode_backup_progress_id(body);
        }
        _ => unreachable!("route returns an index into CONTROL_FORMATS"),
    }
}

// ---------------------------------------------------------------------------
// cp_data_formats
// ---------------------------------------------------------------------------

const CP_DATA_FORMATS: &[&str] = &[
    "raftkv_wire",
    "raftkv_image",
    "raftkv_wal",
    "segment",
    "backup_data",
    "backup_manifest",
    "layout",
    "cursor",
    "markers",
];

/// `animus-cp-data`'s durable and wire formats: the RaftKV command codec
/// (wire frames, snapshot image, WAL), the segment codec, backup data chunks
/// and manifest, the engine key-layout marker, cursors and the engine-internal
/// marker values.
pub fn cp_data_formats(data: &[u8]) {
    use animus_cp_data::{KvCommand, KvState, backup, cursor, fuzzing, layout, segment};
    let (idx, body) = route(data, CP_DATA_FORMATS);
    match CP_DATA_FORMATS[idx] {
        "raftkv_wire" => {
            let _ = fuzzing::raftkv_wire(body);
        }
        "raftkv_image" => {
            let _ = fuzzing::raftkv_image(body);
        }
        "raftkv_wal" => {
            use animus_control::persist::PersistedState as P;
            for b in [body.to_vec(), fix_line_crcs(body)] {
                let _ = P::<KvCommand, KvState>::decode(&b);
                let _ = P::<KvCommand, KvState>::decode_tagged(&b);
            }
        }
        "segment" => {
            let _ = segment::decode(body);
            let _ = segment::decode_and_slice(body, (0, u64::MAX));
        }
        "backup_data" => {
            let _ = backup::decode_data_chunk(body);
        }
        "backup_manifest" => {
            let _ = backup::decode_manifest_object(body);
        }
        "layout" => {
            let _ = layout::decode_layout_value(body);
        }
        "cursor" => {
            let _ = cursor::parse_cursor_key(body);
            let _ = cursor::decode_watermark(body);
            let _ = cursor::decode_backfill_cursor(body);
        }
        "markers" => {
            let _ = fuzzing::engine_markers(body);
        }
        _ => unreachable!("route returns an index into CP_DATA_FORMATS"),
    }
}

// ---------------------------------------------------------------------------
// encryption_envelope
// ---------------------------------------------------------------------------

/// The fixed key the golden `encryption-envelope/v1.bin` fixture was sealed
/// under (`0, 1, .. 31`); fuzzing under it lets mutated seeds reach past the
/// header into frame parsing and authentication instead of failing on a
/// wrong key every time.
pub const FIXTURE_KEY: [u8; 32] = {
    let mut k = [0u8; 32];
    let mut i = 0;
    while i < 32 {
        k[i] = i as u8;
        i += 1;
    }
    k
};

const ENCRYPTION_ENVELOPE: &[&str] = &["file", "object"];

/// The `ADE1` encryption envelope: whole-file scan (header, version dispatch,
/// per-frame length/authentication, torn-tail vs corruption), the
/// `EncryptedDisk` read paths over a file holding the fuzzed bytes (`size`,
/// `read`, `read_at`), and the whole-object (`SegmentStore`) opener.
pub fn encryption_envelope(data: &[u8]) {
    use animus_env::{Disk, EncryptedDisk, EncryptionKey};
    let (idx, body) = route(data, ENCRYPTION_ENVELOPE);
    match ENCRYPTION_ENVELOPE[idx] {
        "file" => {
            let _ = animus_env::encrypted::fuzzing::scan_file(FIXTURE_KEY, body);
            let sim = animus_sim::Simulator::new(1);
            let env = sim.env(animus_env::nid(0));
            futures::executor::block_on(async {
                if env.append("f", body).await.is_err() || env.sync("f").await.is_err() {
                    return;
                }
                let disk = EncryptedDisk::new(
                    env.clone(),
                    env.clone(),
                    EncryptionKey::from_bytes(FIXTURE_KEY),
                );
                let size = disk.size("f").await;
                let _ = disk.read("f").await;
                if let Ok(size) = size {
                    for off in [0, 1, size / 2, size.saturating_sub(1), size, size + 1] {
                        for len in [0usize, 1, 7, 4096] {
                            let _ = disk.read_at("f", off, len).await;
                        }
                    }
                }
            });
        }
        "object" => {
            let _ = animus_env::encrypted::fuzzing::open_object(FIXTURE_KEY, body);
        }
        _ => unreachable!("route returns an index into ENCRYPTION_ENVELOPE"),
    }
}

/// A fuzz target: the raw input in, nothing out (a panic is the failure).
pub type Target = fn(&[u8]);

/// Every target, by name.
pub const ALL: &[(&str, Target)] = &[
    ("dynamo_request", dynamo_request),
    ("dynamo_expressions", dynamo_expressions),
    ("partiql", partiql),
    ("http_sigv4", http_sigv4),
    ("item_codecs", item_codecs),
    ("net_frames", net_frames),
    ("lsm_formats", lsm_formats),
    ("control_formats", control_formats),
    ("cp_data_formats", cp_data_formats),
    ("encryption_envelope", encryption_envelope),
];
