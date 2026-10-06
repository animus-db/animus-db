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
    ClientRequest, ClientResponse, KindWriteBatchItem, KindWriteItemReply, KindWriteOp, MREC_PROTO,
    MrecAnswer, MrecApplyRequest, MrecApplyResponse, MrecRecord, PendingKindWrite, Surface,
    TxnTableWrite, decode_client_frame, encode_client_frame, encode_client_frame_gated, surface_of,
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
                // P2-C's additive field at its default: skipped on the wire, so
                // the bytes stay equal to the Phase 1 fixture.
                cluster_version: 0,
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
        ClientRequest::MrecApply(_) => "MrecApply",
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
        ClientResponse::MrecApply(_) => "MrecApply",
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
    // The Phase 1 baseline: 30 / 19. Post-baseline variants (MREC, G-d M3) live
    // in `mrec_messages` with their own fixture, never in `v1.bin`.
    assert_eq!(
        reqs.len(),
        30,
        "v1.bin covers the 30 Phase 1 ClientRequest variants; a new variant needs its own fixture"
    );
    assert_eq!(
        resps.len(),
        19,
        "v1.bin covers the 19 Phase 1 ClientResponse variants; a new variant needs its own fixture"
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

// ---------------------------------------------------------------------------
// Post-baseline wire shapes: the MREC replication family (ADR 0075 G-d M3).
// Class G, `Gate::MrecReplication`. Its own no-overwrite fixture
// (`v1-mrec.bin`, the `vN-<shape>` convention) so the Phase 1 `v1.bin` is
// never touched.

fn mrec_ver(wall_ms: u64) -> animus_item::MrecVersion {
    animus_item::MrecVersion {
        wall_ms,
        logical: 2,
        region_id: 0xDEAD_BEEF,
    }
}

type MrecMessages = (
    Vec<(&'static str, ClientRequest)>,
    Vec<(&'static str, ClientResponse)>,
);

fn mrec_messages() -> MrecMessages {
    let rec = |item: Option<animus_dynamo::Item>, ms| MrecRecord {
        pk: pk(),
        sk: sk(),
        item,
        ver: mrec_ver(ms),
    };
    (
        vec![
            (
                "MrecApply",
                ClientRequest::MrecApply(MrecApplyRequest {
                    proto: MREC_PROTO,
                    from_region: "eu".into(),
                    table: "orders".into(),
                    records: vec![
                        rec(Some(item()), 1_700_000_000_123),
                        rec(None, 1_700_000_000_456),
                    ],
                    control: None,
                }),
            ),
            (
                "KindWriteItem(Replicate)",
                ClientRequest::KindWriteItem {
                    table: "orders".into(),
                    pk: pk(),
                    sk: sk(),
                    op: KindWriteOp::Replicate {
                        item: Some(item()),
                        ver: mrec_ver(7),
                    },
                    condition: None,
                },
            ),
            (
                "KindWriteBatch(Replicate)",
                ClientRequest::KindWriteBatch {
                    table: "orders".into(),
                    items: vec![KindWriteBatchItem {
                        pk: pk(),
                        sk: sk(),
                        op: KindWriteOp::Replicate {
                            item: None,
                            ver: mrec_ver(8),
                        },
                        condition: None,
                    }],
                },
            ),
        ],
        vec![
            (
                "MrecApply(Answers)",
                ClientResponse::MrecApply(MrecApplyResponse::Answers(vec![
                    MrecAnswer::Applied,
                    MrecAnswer::Superseded,
                    MrecAnswer::Retry,
                    MrecAnswer::Rejected {
                        message: "bad".into(),
                    },
                ])),
            ),
            (
                "MrecApply(Refused)",
                ClientResponse::MrecApply(MrecApplyResponse::Refused {
                    message: "gate".into(),
                    retryable: true,
                }),
            ),
            (
                "KindWriteBatchOk(Superseded)",
                ClientResponse::KindWriteBatchOk {
                    results: vec![KindWriteItemReply::Superseded],
                },
            ),
        ],
    )
}

fn encode_mrec() -> Vec<u8> {
    let (reqs, resps) = mrec_messages();
    let mut out = Vec::new();
    for (_, r) in reqs {
        out.extend(encode_client_frame(&r).expect("encodes"));
    }
    for (_, r) in resps {
        out.extend(encode_client_frame(&r).expect("encodes"));
    }
    out
}

/// The MREC shapes are class G: every one needs `Gate::MrecReplication` (closed
/// on a floor handle) and the gated encoder emits them byte-identically to the
/// plain encoder once it is open.
#[test]
fn mrec_wire_shapes_are_gated_on_mrec_replication() {
    let (reqs, resps) = mrec_messages();
    let floor = ClusterFeatures::new();
    for (n, r) in &reqs {
        assert_eq!(r.required_gate(), Gate::MrecReplication, "request {n}");
        // (The gated encoder refuses these while closed; a debug build asserts
        // on that violation, so the closed state is asserted, not exercised.)
        assert!(!floor.is_open(r.required_gate()), "request {n}");
        let fwd = ClientRequest::Forwarded {
            request: Box::new(r.clone()),
            traceparent: None,
        };
        assert_eq!(fwd.required_gate(), Gate::MrecReplication, "forwarded {n}");
    }
    for (n, r) in &resps {
        // A whole-batch `Refused` is `Base`: a node whose own gate is still
        // closed must be able to say "not yet" (a debug build panics on a
        // closed-gate emit, found by `mrec_peer_transport`). Per-record
        // `Answers` need the gate.
        let want = if matches!(
            r,
            ClientResponse::MrecApply(MrecApplyResponse::Refused { .. })
        ) {
            Gate::Base
        } else {
            Gate::MrecReplication
        };
        assert_eq!(r.required_gate(), want, "response {n}");
    }
    // Content-dependent: the same carriers without a replicate stay Base.
    assert_eq!(
        ClientRequest::KindWriteBatch {
            table: "t".into(),
            items: vec![KindWriteBatchItem {
                pk: pk(),
                sk: None,
                op: KindWriteOp::Delete,
                condition: None,
            }],
        }
        .required_gate(),
        Gate::Base
    );
    assert_eq!(
        ClientResponse::KindWriteBatchOk {
            results: vec![KindWriteItemReply::ConditionFailed],
        }
        .required_gate(),
        Gate::Base
    );
    // Open gate (cluster version 3 profile): emitted, byte-equal to plain.
    let mut meta = Metadata::default();
    for c in [
        MetaCommand::UpsertMember {
            node: nid(1),
            labels: BTreeMap::new(),
            status: NodeStatus::Active,
        },
        era_report_range(1, 3),
        MetaCommand::FinalizeClusterVersion {
            expected: 1,
            target: 2,
        },
        MetaCommand::FinalizeClusterVersion {
            expected: 2,
            target: 3,
        },
    ] {
        meta.apply(&c);
    }
    assert_eq!(meta.cluster_version(), 3);
    let open = ClusterFeatures::new();
    open.update(&meta);
    assert!(open.is_open(Gate::MrecReplication));
    for (n, r) in &reqs {
        assert_eq!(
            encode_client_frame_gated(r, &open).expect("open"),
            encode_client_frame(r).unwrap(),
            "request {n}"
        );
    }
}

fn era_report_range(min: u32, max: u32) -> MetaCommand {
    MetaCommand::ReportNodeVersion {
        node: nid(1),
        range: animus_control::version::VersionRange::new(min, max),
        build: "t".into(),
    }
}

/// `MrecApply` is intra-only (a peer cluster's mutual-TLS intra dial), never a
/// client-listener request.
#[test]
fn mrec_apply_is_intra_only() {
    let (reqs, _) = mrec_messages();
    assert_eq!(surface_of(&reqs[0].1), Surface::Intra);
}

#[test]
fn mrec_frames_are_byte_identical_to_the_fixture_and_decode_back() {
    let fixture =
        std::fs::read(fixtures_dir().join("v1-mrec.bin")).expect("v1-mrec.bin is checked in");
    assert_eq!(
        encode_mrec(),
        fixture,
        "MREC frames drifted from the fixture"
    );
    let (reqs, resps) = mrec_messages();
    let frames = split_frames(&fixture);
    assert_eq!(frames.len(), reqs.len() + resps.len());
    for ((name, want), frame) in reqs.iter().zip(&frames) {
        let got: ClientRequest = decode_client_frame(frame).expect("decodes");
        assert_eq!(request_name(&got), request_name(want), "{name}");
        assert_eq!(
            encode_client_frame(&got).unwrap(),
            encode_client_frame(want).unwrap(),
            "{name}"
        );
    }
    for ((name, want), frame) in resps.iter().zip(&frames[reqs.len()..]) {
        let got: ClientResponse = decode_client_frame(frame).expect("decodes");
        assert_eq!(&got, want, "{name}");
    }
}

/// The Phase 1 fixture still decodes (an old frame never mentions a replicate),
/// and its `Delete`/`Update` ops keep their pre-MREC encoding.
#[test]
fn replicate_is_an_additive_op_the_phase1_ops_are_unchanged() {
    let put = serde_json::to_string(&KindWriteOp::Delete).unwrap();
    assert_eq!(put, "\"Delete\"");
    let rep = serde_json::to_string(&KindWriteOp::Replicate {
        item: None,
        ver: mrec_ver(1),
    })
    .unwrap();
    assert!(rep.starts_with("{\"Replicate\":"), "{rep}");
}

/// Refuses to overwrite: run once.
/// `cargo test -p animus-node --test it format_fixtures::generate_fixture_client_frame_mrec -- --ignored`
#[test]
#[ignore = "fixture generator; run explicitly, never regenerates an existing fixture"]
fn generate_fixture_client_frame_mrec() {
    let path = fixtures_dir().join("v1-mrec.bin");
    assert!(
        std::fs::metadata(&path).is_err(),
        "{} already exists; a checked-in fixture is never regenerated in place",
        path.display()
    );
    std::fs::write(&path, encode_mrec()).expect("write");
}

fn mrec_control_messages() -> Vec<(&'static str, ClientRequest)> {
    let frame = |control| {
        ClientRequest::MrecApply(MrecApplyRequest {
            proto: MREC_PROTO,
            from_region: "eu".into(),
            table: "orders".into(),
            records: Vec::new(),
            control: Some(control),
        })
    };
    vec![
        (
            "CreateReplica",
            frame(animus_node::MrecControl::CreateReplica {
                create_table: "{\"TableName\":\"orders\"}".into(),
                ttl_attribute: Some("expires".into()),
                peers: vec!["ap".into()],
            }),
        ),
        ("Leave", frame(animus_node::MrecControl::Leave)),
        (
            "AddPeer",
            frame(animus_node::MrecControl::AddPeer {
                region: "ap".into(),
            }),
        ),
    ]
}

fn encode_mrec_control() -> Vec<u8> {
    let mut out = Vec::new();
    for (_, r) in mrec_control_messages() {
        out.extend(encode_client_frame(&r).expect("encodes"));
    }
    out.extend(
        encode_client_frame(&ClientResponse::MrecApply(MrecApplyResponse::Done)).expect("encodes"),
    );
    out
}

/// G-d M4: the replica-lifecycle messages ride the existing class-G
/// `MrecApply` frame (additive `control` field): same gate, same surface, and a
/// frame written by an M3 binary (no `control`) still decodes.
#[test]
fn mrec_control_frames_are_byte_identical_to_the_fixture_and_gated() {
    let fixture = std::fs::read(fixtures_dir().join("v1-mrec-control.bin"))
        .expect("v1-mrec-control.bin is checked in");
    assert_eq!(encode_mrec_control(), fixture, "control frames drifted");
    let frames = split_frames(&fixture);
    let msgs = mrec_control_messages();
    assert_eq!(frames.len(), msgs.len() + 1);
    for ((name, want), frame) in msgs.iter().zip(&frames) {
        let got: ClientRequest = decode_client_frame(frame).expect("decodes");
        assert_eq!(
            encode_client_frame(&got).unwrap(),
            encode_client_frame(want).unwrap(),
            "{name}"
        );
        assert_eq!(want.required_gate(), Gate::MrecReplication, "{name}");
        assert_eq!(surface_of(want), Surface::Intra, "{name}");
    }
    let done = ClientResponse::MrecApply(MrecApplyResponse::Done);
    assert_eq!(done.required_gate(), Gate::MrecReplication);
    let got: ClientResponse = decode_client_frame(frames.last().unwrap()).expect("decodes");
    assert_eq!(got, done);
    // Backward compatible: the plain-batch frame carries no `control` key.
    let (reqs, _) = mrec_messages();
    let ClientRequest::MrecApply(plain) = &reqs[0].1 else {
        panic!("first MREC request is MrecApply")
    };
    assert!(
        !serde_json::to_string(plain).unwrap().contains("control"),
        "an absent control stays off the wire"
    );
}

/// Refuses to overwrite: run once.
#[test]
#[ignore = "fixture generator; run explicitly, never regenerates an existing fixture"]
fn generate_fixture_client_frame_mrec_control() {
    let path = fixtures_dir().join("v1-mrec-control.bin");
    assert!(
        std::fs::metadata(&path).is_err(),
        "{} already exists; a checked-in fixture is never regenerated in place",
        path.display()
    );
    std::fs::write(&path, encode_mrec_control()).expect("write");
}
