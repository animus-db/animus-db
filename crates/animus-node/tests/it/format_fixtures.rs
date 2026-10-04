//! ADR 0073 Phase 2 (P2-B): golden fixture for the client wire
//! (`ClientRequest` / `ClientResponse`, length-prefixed `serde_json`), plus the
//! per-variant gate pins.
//!
//! **`client-frame/v1.bin` is Phase 1 bytes, not "current output".** It was
//! generated from a Phase 1 build: commit `941a5ea` (the merge of #1152, the last
//! commit before any P2-A code merged), by running this module's generator in a
//! worktree of that commit. The byte-identity test then proves a P2-B build (B2)
//! emits, before the era, exactly the bytes Phase 1 did, through both the plain
//! `encode_client_frame` and the gated `encode_client_frame_gated`.
//!
//! Layout: the concatenation of `encode_client_frame(msg)` (each already a `u32`
//! big-endian length followed by the JSON) for every `ClientRequest` variant (30,
//! in declaration order) and then every `ClientResponse` variant (19). Only Phase 1
//! shapes appear: `ProposeSchema` carries a non-era command, `Status` carries an
//! era-0 `Metadata`, and `JoinInfo`/`MetadataDelta` carry no era field. Regenerating
//! an existing version is refused; a format change is a new `v<N>.bin`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use animus_control::mirror::KeyWrite;
use animus_control::schema::{ColumnType, TableSchema};
use animus_control::version::{ClusterFeatures, Gate, GateSurface};
use animus_control::{MetaCommand, Metadata, NodeStatus};
use animus_cp_data::hlc::HlcTimestamp;
use animus_cp_data::{
    ResolveOutcome, StageOutcome, TxnDecisionStatus, TxnId, TxnOutcome, TxnRecordView, TxnWrite,
};
use animus_env::nid;
use animus_node::{
    ClientRequest, ClientResponse, KindWriteBatchItem, KindWriteItemReply, KindWriteOp,
    PendingKindWrite, TxnTableWrite, decode_client_frame, encode_client_frame,
    encode_client_frame_gated,
};
use animus_tablet::KeyRange;

fn ts(n: u64) -> HlcTimestamp {
    HlcTimestamp {
        wall_ms: n,
        logical: 1,
    }
}

fn txn_id() -> TxnId {
    TxnId {
        ts: ts(100),
        node: nid(2),
    }
}

fn item() -> animus_dynamo::Item {
    let mut it = BTreeMap::new();
    it.insert(
        "id".to_string(),
        animus_dynamo::AttributeValue::S("a".into()),
    );
    it.insert(
        "n".to_string(),
        animus_dynamo::AttributeValue::N("7".into()),
    );
    it
}

fn pk() -> animus_dynamo::AttributeValue {
    animus_dynamo::AttributeValue::S("pk1".into())
}

fn sk() -> Option<animus_dynamo::AttributeValue> {
    Some(animus_dynamo::AttributeValue::N("3".into()))
}

fn hint() -> Option<(animus_env::NodeId, String)> {
    Some((nid(1), "127.0.0.1:9001".to_string()))
}

/// An era-0 `Metadata` (no version record), built by applying real commands.
fn era0_metadata() -> Metadata {
    let mut m = Metadata::default();
    for c in [
        MetaCommand::UpsertMember {
            node: nid(1),
            labels: BTreeMap::new(),
            status: NodeStatus::Active,
        },
        MetaCommand::CreateTableSchema {
            table: "orders".to_string(),
            schema: TableSchema::simple("id", ColumnType::String),
        },
    ] {
        m.apply(&c);
    }
    assert!(!m.versioning_active());
    m
}

/// One of every `ClientRequest` variant (30), fixed constants only.
fn every_request() -> Vec<(&'static str, ClientRequest)> {
    let schema_cmd = MetaCommand::CreateTableSchema {
        table: "orders".to_string(),
        schema: TableSchema::simple("id", ColumnType::String),
    };
    let batch_item = || KindWriteBatchItem {
        pk: pk(),
        sk: sk(),
        op: KindWriteOp::Delete,
        condition: None,
    };
    vec![
        ("Status", ClientRequest::Status),
        (
            "Put",
            ClientRequest::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                table: "orders".into(),
            },
        ),
        (
            "PutBatch",
            ClientRequest::PutBatch {
                entries: vec![
                    (b"k1".to_vec(), b"v1".to_vec()),
                    (b"k2".to_vec(), b"v2".to_vec()),
                ],
                table: "orders".into(),
            },
        ),
        (
            "KindWrite",
            ClientRequest::KindWrite {
                table: "orders".into(),
                writes: vec![
                    (1, b"k".to_vec(), Some(b"v".to_vec())),
                    (2, b"t".to_vec(), None),
                ],
                change_log: vec![(b"p".to_vec(), b"r".to_vec())],
                housekeeping: true,
            },
        ),
        (
            "KindScan",
            ClientRequest::KindScan {
                table: "orders".into(),
                kind: 1,
                start: b"a".to_vec(),
                end: Some(b"z".to_vec()),
                limit: Some(10),
                reverse: true,
                stale: false,
            },
        ),
        ("ForceSeal", ClientRequest::ForceSeal { tablet: 4 }),
        ("ForcePitrSeal", ClientRequest::ForcePitrSeal { tablet: 4 }),
        (
            "TriggerAutoSplit",
            ClientRequest::TriggerAutoSplit { tablet: 4 },
        ),
        (
            "StreamHotRead",
            ClientRequest::StreamHotRead {
                tablet: 4,
                from_position_hlc: 99,
                from_position_ordinal: 2,
                limit: 50,
            },
        ),
        (
            "StreamHotChangeMax",
            ClientRequest::StreamHotChangeMax { tablet: 4 },
        ),
        (
            "ClearBackfillCursor",
            ClientRequest::ClearBackfillCursor {
                tablet: 4,
                index: "gsi1".into(),
            },
        ),
        (
            "KindWriteItem",
            ClientRequest::KindWriteItem {
                table: "orders".into(),
                pk: pk(),
                sk: sk(),
                op: KindWriteOp::Put(item()),
                condition: None,
            },
        ),
        (
            "KindWriteBatch",
            ClientRequest::KindWriteBatch {
                table: "orders".into(),
                items: vec![batch_item()],
            },
        ),
        (
            "CpLeaderHintProbe",
            ClientRequest::CpLeaderHintProbe { tablet: 4 },
        ),
        (
            "Get",
            ClientRequest::Get {
                key: b"k".to_vec(),
                table: "orders".into(),
                stale: true,
            },
        ),
        (
            "GetSnapshot",
            ClientRequest::GetSnapshot {
                key: b"k".to_vec(),
                table: "orders".into(),
            },
        ),
        (
            "Delete",
            ClientRequest::Delete {
                key: b"k".to_vec(),
                table: "orders".into(),
            },
        ),
        (
            "Scan",
            ClientRequest::Scan {
                start: b"a".to_vec(),
                end: None,
                limit: None,
                reverse: false,
                table: "orders".into(),
                stale: true,
            },
        ),
        (
            "Forwarded",
            ClientRequest::Forwarded {
                request: Box::new(ClientRequest::ProposeSchema(schema_cmd.clone())),
                traceparent: Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".into()),
            },
        ),
        ("ProposeSchema", ClientRequest::ProposeSchema(schema_cmd)),
        (
            "SplitTablet",
            ClientRequest::SplitTablet {
                tablet: 4,
                split_key: b"m".to_vec(),
            },
        ),
        ("JoinInfo", ClientRequest::JoinInfo),
        (
            "WatchMetadata",
            ClientRequest::WatchMetadata { last_seen: 12 },
        ),
        (
            "Txn",
            ClientRequest::Txn {
                writes: vec![TxnTableWrite {
                    table: "orders".into(),
                    key: b"k".to_vec(),
                    value: Some(b"v".to_vec()),
                    pending: Some(PendingKindWrite {
                        pk: pk(),
                        sk: sk(),
                        op: KindWriteOp::Put(item()),
                        condition: None,
                    }),
                }],
                preconditions: vec![("orders".into(), b"k".to_vec(), None)],
                write_conditions: vec![("orders".into(), b"k".to_vec(), Some(b"v".to_vec()))],
            },
        ),
        (
            "TxnPrepare",
            ClientRequest::TxnPrepare {
                table: "orders".into(),
                anchor: Some((txn_id(), b"rk".to_vec(), "orders".into())),
                writes: vec![TxnWrite {
                    key: b"k".to_vec(),
                    value: Some(b"v".to_vec()),
                    kind_writes: vec![],
                    change_log: None,
                    stage_marker: Some((b"sm".to_vec(), b"mv".to_vec())),
                    pending: None,
                }],
                conditions: vec![(b"k".to_vec(), None)],
                participant_spans: vec![("orders".into(), KeyRange::whole())],
                pending_kind_writes: vec![PendingKindWrite {
                    pk: pk(),
                    sk: None,
                    op: KindWriteOp::Delete,
                    condition: None,
                }],
            },
        ),
        (
            "TxnDecide",
            ClientRequest::TxnDecide {
                table: "orders".into(),
                txn_id: txn_id(),
                record_key: b"rk".to_vec(),
                commit: true,
                min_commit_ts: ts(120),
                orphan_created_ts: Some(ts(90)),
            },
        ),
        (
            "TxnResolve",
            ClientRequest::TxnResolve {
                table: "orders".into(),
                txn_id: txn_id(),
                record_key: b"rk".to_vec(),
                keys: vec![b"k".to_vec()],
                outcome: TxnOutcome::Committed { commit_ts: ts(130) },
            },
        ),
        (
            "TxnStatus",
            ClientRequest::TxnStatus {
                table: "orders".into(),
                record_key: b"rk".to_vec(),
            },
        ),
        (
            "TxnRecordView",
            ClientRequest::TxnRecordView {
                table: "orders".into(),
                record_key: b"rk".to_vec(),
            },
        ),
        (
            "TxnVerify",
            ClientRequest::TxnVerify {
                table: "orders".into(),
                span: KeyRange::whole(),
                txn_id: txn_id(),
            },
        ),
    ]
}

/// One of every `ClientResponse` variant (19), fixed constants only.
fn every_response() -> Vec<(&'static str, ClientResponse)> {
    vec![
        (
            "Status",
            ClientResponse::Status {
                metadata: era0_metadata(),
                leader_hint: hint(),
                intra_leader_hint: hint(),
                watermark: 17,
                control_voters: [nid(1), nid(2), nid(3)]
                    .into_iter()
                    .collect::<BTreeSet<_>>(),
            },
        ),
        ("PutOk", ClientResponse::PutOk),
        ("Value", ClientResponse::Value(Some(b"v".to_vec()))),
        (
            "KindWriteOk",
            ClientResponse::KindWriteOk {
                old: None,
                new: Some(item()),
                collection_bytes: Some(64),
            },
        ),
        ("ConditionFailed", ClientResponse::ConditionFailed),
        (
            "KindWriteBatchOk",
            ClientResponse::KindWriteBatchOk {
                results: vec![
                    KindWriteItemReply::Ok {
                        old: Some(item()),
                        new: None,
                        collection_bytes: None,
                    },
                    KindWriteItemReply::ConditionFailed,
                    KindWriteItemReply::Rejected {
                        code: "ValidationException".into(),
                        message: "nope".into(),
                    },
                ],
            },
        ),
        ("Unresolved", ClientResponse::Unresolved),
        (
            "CpLeaderHint",
            ClientResponse::CpLeaderHint { hint: hint() },
        ),
        (
            "Pairs",
            ClientResponse::Pairs(vec![(b"k".to_vec(), b"v".to_vec())]),
        ),
        ("Error", ClientResponse::Error("boom".into())),
        (
            "JoinInfo",
            ClientResponse::JoinInfo {
                control_ids: vec![nid(1), nid(2)],
                peers: [(nid(1), "127.0.0.1:9101".to_string())]
                    .into_iter()
                    .collect(),
                client_route: [(nid(1), "127.0.0.1:9001".to_string())]
                    .into_iter()
                    .collect(),
                intra_route: [(nid(1), "127.0.0.1:9201".to_string())]
                    .into_iter()
                    .collect(),
                admin_addrs: vec!["127.0.0.1:9301".parse().unwrap()],
            },
        ),
        (
            "MetadataDelta",
            ClientResponse::MetadataDelta {
                writes: vec![
                    KeyWrite::Put(b"k".to_vec(), b"v".to_vec()),
                    KeyWrite::Delete(b"d".to_vec()),
                ],
                watermark: 18,
                leader_hint: hint(),
                intra_leader_hint: None,
                control_voters: [nid(1)].into_iter().collect(),
            },
        ),
        (
            "TxnCommitted",
            ClientResponse::TxnCommitted { commit_ts: ts(140) },
        ),
        (
            "TxnPrepared",
            ClientResponse::TxnPrepared {
                txn_id: txn_id(),
                record_key: b"rk".to_vec(),
                record_table: "orders".into(),
                ts: ts(110),
                outcome: StageOutcome::Staged,
            },
        ),
        (
            "TxnDecided",
            ClientResponse::TxnDecided {
                outcome: TxnOutcome::Aborted,
            },
        ),
        (
            "TxnStatusReply",
            ClientResponse::TxnStatusReply {
                status: TxnDecisionStatus::Pending,
            },
        ),
        (
            "TxnRecordViewReply",
            ClientResponse::TxnRecordViewReply {
                view: Some(TxnRecordView {
                    status: TxnDecisionStatus::Committed { commit_ts: ts(150) },
                    intent_spans: vec![("orders".into(), KeyRange::whole())],
                    created_ts: ts(95),
                }),
            },
        ),
        (
            "TxnVerifyReply",
            ClientResponse::TxnVerifyReply { staged: true },
        ),
        (
            "TxnResolved",
            ClientResponse::TxnResolved {
                outcome: ResolveOutcome::Resolved,
            },
        ),
    ]
}

/// Exhaustiveness guards: a new variant fails to compile here until named, and the
/// coverage test fails until the builders above include it.
fn request_name(r: &ClientRequest) -> &'static str {
    match r {
        ClientRequest::Status => "Status",
        ClientRequest::Put { .. } => "Put",
        ClientRequest::PutBatch { .. } => "PutBatch",
        ClientRequest::KindWrite { .. } => "KindWrite",
        ClientRequest::KindScan { .. } => "KindScan",
        ClientRequest::ForceSeal { .. } => "ForceSeal",
        ClientRequest::ForcePitrSeal { .. } => "ForcePitrSeal",
        ClientRequest::TriggerAutoSplit { .. } => "TriggerAutoSplit",
        ClientRequest::StreamHotRead { .. } => "StreamHotRead",
        ClientRequest::StreamHotChangeMax { .. } => "StreamHotChangeMax",
        ClientRequest::ClearBackfillCursor { .. } => "ClearBackfillCursor",
        ClientRequest::KindWriteItem { .. } => "KindWriteItem",
        ClientRequest::KindWriteBatch { .. } => "KindWriteBatch",
        ClientRequest::CpLeaderHintProbe { .. } => "CpLeaderHintProbe",
        ClientRequest::Get { .. } => "Get",
        ClientRequest::GetSnapshot { .. } => "GetSnapshot",
        ClientRequest::Delete { .. } => "Delete",
        ClientRequest::Scan { .. } => "Scan",
        ClientRequest::Forwarded { .. } => "Forwarded",
        ClientRequest::ProposeSchema(_) => "ProposeSchema",
        ClientRequest::SplitTablet { .. } => "SplitTablet",
        ClientRequest::JoinInfo => "JoinInfo",
        ClientRequest::WatchMetadata { .. } => "WatchMetadata",
        ClientRequest::Txn { .. } => "Txn",
        ClientRequest::TxnPrepare { .. } => "TxnPrepare",
        ClientRequest::TxnDecide { .. } => "TxnDecide",
        ClientRequest::TxnResolve { .. } => "TxnResolve",
        ClientRequest::TxnStatus { .. } => "TxnStatus",
        ClientRequest::TxnRecordView { .. } => "TxnRecordView",
        ClientRequest::TxnVerify { .. } => "TxnVerify",
    }
}

fn response_name(r: &ClientResponse) -> &'static str {
    match r {
        ClientResponse::Status { .. } => "Status",
        ClientResponse::PutOk => "PutOk",
        ClientResponse::Value(_) => "Value",
        ClientResponse::KindWriteOk { .. } => "KindWriteOk",
        ClientResponse::ConditionFailed => "ConditionFailed",
        ClientResponse::KindWriteBatchOk { .. } => "KindWriteBatchOk",
        ClientResponse::Unresolved => "Unresolved",
        ClientResponse::CpLeaderHint { .. } => "CpLeaderHint",
        ClientResponse::Pairs(_) => "Pairs",
        ClientResponse::Error(_) => "Error",
        ClientResponse::JoinInfo { .. } => "JoinInfo",
        ClientResponse::MetadataDelta { .. } => "MetadataDelta",
        ClientResponse::TxnCommitted { .. } => "TxnCommitted",
        ClientResponse::TxnPrepared { .. } => "TxnPrepared",
        ClientResponse::TxnDecided { .. } => "TxnDecided",
        ClientResponse::TxnStatusReply { .. } => "TxnStatusReply",
        ClientResponse::TxnRecordViewReply { .. } => "TxnRecordViewReply",
        ClientResponse::TxnVerifyReply { .. } => "TxnVerifyReply",
        ClientResponse::TxnResolved { .. } => "TxnResolved",
    }
}

fn encode_all() -> Vec<u8> {
    let mut out = Vec::new();
    for (_, r) in every_request() {
        out.extend(encode_client_frame(&r).expect("encodes"));
    }
    for (_, r) in every_response() {
        out.extend(encode_client_frame(&r).expect("encodes"));
    }
    out
}

fn split_frames(mut bytes: &[u8]) -> Vec<Vec<u8>> {
    let mut frames = Vec::new();
    while !bytes.is_empty() {
        let len = u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize;
        frames.push(bytes[4..4 + len].to_vec());
        bytes = &bytes[4 + len..];
    }
    frames
}

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/formats/client-frame")
}

#[test]
fn fixture_covers_every_client_variant_once() {
    let reqs = every_request();
    let resps = every_response();
    assert_eq!(
        reqs.len(),
        30,
        "ClientRequest has 30 variants; add the new one"
    );
    assert_eq!(
        resps.len(),
        19,
        "ClientResponse has 19 variants; add the new one"
    );
    let rq: BTreeSet<&str> = reqs.iter().map(|(n, _)| *n).collect();
    let rs: BTreeSet<&str> = resps.iter().map(|(n, _)| *n).collect();
    assert_eq!(rq.len(), reqs.len());
    assert_eq!(rs.len(), resps.len());
    for (n, r) in &reqs {
        assert_eq!(request_name(r), *n);
    }
    for (n, r) in &resps {
        assert_eq!(response_name(r), *n);
    }
}

/// The current encoders (plain and gated, under a floor and an era-0 handle)
/// reproduce the Phase 1 fixture byte for byte, and every frame decodes back to
/// an identical re-encoding.
#[test]
fn client_frames_are_byte_identical_to_the_phase1_fixture() {
    let fixture = std::fs::read(fixtures_dir().join("v1.bin")).expect("v1.bin is checked in");
    assert_eq!(encode_all(), fixture, "B2 pre-era bytes != Phase 1");

    let era0 = ClusterFeatures::new();
    era0.update(&Metadata::default());
    for (name, f) in [("floor", ClusterFeatures::new()), ("era-0", era0)] {
        let mut gated = Vec::new();
        for (_, r) in every_request() {
            gated.extend(encode_client_frame_gated(&r, &f).expect("open gate"));
        }
        for (_, r) in every_response() {
            gated.extend(encode_client_frame_gated(&r, &f).expect("open gate"));
        }
        assert_eq!(gated, fixture, "{name}: gated encoder != Phase 1");
    }

    let frames = split_frames(&fixture);
    let (nreq, nresp) = (every_request().len(), every_response().len());
    assert_eq!(frames.len(), nreq + nresp);
    for ((name, _), frame) in every_request().iter().zip(&frames) {
        let decoded: ClientRequest = decode_client_frame(frame).expect("decodes");
        assert_eq!(request_name(&decoded), *name);
        let mut re = Vec::new();
        re.extend((frame.len() as u32).to_be_bytes());
        re.extend(frame);
        assert_eq!(encode_client_frame(&decoded).unwrap(), re, "request {name}");
    }
    for ((name, _), frame) in every_response().iter().zip(&frames[nreq..]) {
        let decoded: ClientResponse = decode_client_frame(frame).expect("decodes");
        assert_eq!(response_name(&decoded), *name);
        let mut re = Vec::new();
        re.extend((frame.len() as u32).to_be_bytes());
        re.extend(frame);
        assert_eq!(
            encode_client_frame(&decoded).unwrap(),
            re,
            "response {name}"
        );
    }
    assert_eq!(
        ClusterFeatures::new().violations(GateSurface::ClientRequest),
        0
    );
}

/// Refuses to overwrite: run once from a Phase 1 build.
/// `cargo test -p animus-node --test it format_fixtures::generate -- --ignored`
#[test]
#[ignore = "fixture generator; run explicitly, never regenerates an existing fixture"]
fn generate_fixture_client_frame() {
    let dir = fixtures_dir();
    std::fs::create_dir_all(&dir).expect("create fixtures dir");
    let path = dir.join("v1.bin");
    assert!(
        std::fs::metadata(&path).is_err(),
        "{} already exists; a checked-in fixture is never regenerated in place",
        path.display()
    );
    std::fs::write(&path, encode_all()).expect("write");
}

// ---------------------------------------------------------------------------
// Gate tables (ADR 0073 Phase 2, P2-B)

fn era_report() -> MetaCommand {
    MetaCommand::ReportNodeVersion {
        node: nid(1),
        range: animus_control::version::VersionRange::new(1, 1),
        build: "t".into(),
    }
}

fn era_finalize() -> MetaCommand {
    MetaCommand::FinalizeClusterVersion {
        expected: 1,
        target: 2,
    }
}

#[test]
fn every_phase1_client_message_is_base() {
    for (n, r) in every_request() {
        assert_eq!(r.required_gate(), Gate::Base, "request {n}");
    }
    for (n, r) in every_response() {
        assert_eq!(r.required_gate(), Gate::Base, "response {n}");
    }
}

/// `ProposeSchema` takes its payload's gate and `Forwarded` recurses, so an era
/// command is Era on the relay path at any nesting depth.
#[test]
fn nested_relay_requests_take_their_payloads_gate() {
    assert_eq!(
        ClientRequest::ProposeSchema(era_finalize()).required_gate(),
        Gate::Era
    );
    assert_eq!(
        ClientRequest::ProposeSchema(era_report()).required_gate(),
        Gate::Era
    );
    let fwd = |r: ClientRequest| ClientRequest::Forwarded {
        request: Box::new(r),
        traceparent: None,
    };
    assert_eq!(
        fwd(ClientRequest::ProposeSchema(era_report())).required_gate(),
        Gate::Era
    );
    assert_eq!(
        fwd(fwd(ClientRequest::ProposeSchema(era_finalize()))).required_gate(),
        Gate::Era
    );
    assert_eq!(
        fwd(ClientRequest::ProposeSchema(MetaCommand::NoOp)).required_gate(),
        Gate::Base
    );
    assert_eq!(fwd(ClientRequest::Status).required_gate(), Gate::Base);
}

/// ADR 0073 relay decision (P2-B draft amended by P2-C): `ReportNodeVersion`
/// rides `ProposeSchema` (the data-only boot self-report needs a route);
/// `FinalizeClusterVersion` is a leader-local admin action and is NOT
/// relayable. Both are era-gated, so the gate check refuses them before the
/// era either way.
#[test]
fn era_commands_relay_classification_and_gate() {
    assert!(animus_node::is_relayable_command(&era_report()));
    assert!(!animus_node::is_relayable_command(&era_finalize()));
    for c in [era_report(), era_finalize()] {
        assert_eq!(
            animus_control::version::GatedCommand::required_gate(&c),
            Gate::Era
        );
    }
}
