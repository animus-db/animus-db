//! ADR 0073 Phase 2 (P2-B): golden fixture for the control-plane `RaftMsg<MetaCommand>`
//! JSON wire format, plus the per-variant gate pins for `RaftMsg`/`MetaCommand`.
//!
//! **`control-raft-msg/v1.bin` is Phase 1 bytes, not "current output".** It was
//! generated from a Phase 1 build: commit `941a5ea` (the merge of #1152, the last
//! commit before any P2-A code merged), by running this module's generator in a
//! worktree of that commit. The byte-identity test below then proves a P2-B build
//! (B2) emits, before the era, exactly the bytes Phase 1 did (ADR 0073 section 2's
//! "B2 pre-era encodings equal the existing Phase 1 golden fixtures byte for byte").
//!
//! Layout: a sequence of frames, each a `u32` big-endian length followed by
//! `serde_json::to_vec(&RaftMsg<MetaCommand>)` (the exact bytes `node.rs` hands
//! to `env.send`). Only Phase 1 variants/commands appear (no era command).
//! Regenerating an existing version is refused; a format change is a new `v<N>.bin`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use animus_control::node::encode_syskv_image_bytes;
use animus_control::raft::LogEntry;
use animus_control::schema::{ColumnType, TableSchema};
use animus_control::version::{ClusterFeatures, Gate, GatedCommand};
use animus_control::{MetaCommand, NodeStatus, PlacementPolicy, RaftMsg};
use animus_env::nid;
use animus_tablet::{KeyRange, TabletId};

fn set(ids: &[u64]) -> BTreeSet<animus_env::NodeId> {
    ids.iter().map(|&n| nid(n)).collect()
}

fn entry(index: u64, command: MetaCommand) -> LogEntry<MetaCommand> {
    LogEntry {
        term: 2,
        index,
        command,
        config: None,
        learners: None,
    }
}

/// A spread of non-era `MetaCommand`s for `AppendEntries` to carry.
fn phase1_commands() -> Vec<MetaCommand> {
    vec![
        MetaCommand::NoOp,
        MetaCommand::UpsertMember {
            node: nid(1),
            labels: BTreeMap::new(),
            status: NodeStatus::Active,
        },
        MetaCommand::CreateTablet {
            tablet: TabletId(1),
            table: Some("orders".to_string()),
            range: KeyRange::whole(),
            replicas: vec![nid(1), nid(2)],
        },
        MetaCommand::SetTabletPolicy {
            tablet: TabletId(1),
            policy: Some(PlacementPolicy::simple("p", 2)),
        },
        MetaCommand::CreateTableSchema {
            table: "orders".to_string(),
            schema: TableSchema::simple("id", ColumnType::String),
        },
        MetaCommand::RemoveMember { node: nid(9) },
    ]
}

/// One of every `RaftMsg` variant (16), built from fixed constants only.
fn every_raft_msg() -> Vec<(&'static str, RaftMsg<MetaCommand>)> {
    let cmds = phase1_commands();
    let mut config_entry = entry(7, MetaCommand::NoOp);
    config_entry.config = Some(set(&[1, 2, 3]));
    config_entry.learners = Some(set(&[4]));
    let image = encode_syskv_image_bytes(&[
        (
            animus_control::syskv::tablet_key(TabletId(1)),
            Some(b"{\"tablet\":1}".to_vec()),
            3,
        ),
        (animus_control::syskv::schema_key("orders"), None, 2),
    ]);
    let total = image.len() as u64;
    vec![
        (
            "PreVote",
            RaftMsg::PreVote {
                term: 4,
                candidate: nid(2),
                last_log_index: 9,
                last_log_term: 3,
            },
        ),
        (
            "PreVoteResp",
            RaftMsg::PreVoteResp {
                term: 4,
                granted: true,
            },
        ),
        (
            "RequestVote",
            RaftMsg::RequestVote {
                term: 4,
                candidate: nid(2),
                last_log_index: 9,
                last_log_term: 3,
            },
        ),
        (
            "RequestVoteResp",
            RaftMsg::RequestVoteResp {
                term: 4,
                granted: false,
            },
        ),
        (
            "AppendEntries",
            RaftMsg::AppendEntries {
                term: 2,
                leader: nid(1),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: cmds
                    .iter()
                    .cloned()
                    .enumerate()
                    .map(|(i, c)| entry(i as u64 + 1, c))
                    .chain(std::iter::once(config_entry))
                    .collect(),
                leader_commit: 3,
            },
        ),
        (
            "AppendEntriesResp",
            RaftMsg::AppendEntriesResp {
                term: 2,
                success: true,
                match_index: 7,
                needs_snapshot: true,
                check_pending: true,
            },
        ),
        (
            "InstallSnapshot",
            RaftMsg::InstallSnapshot {
                term: 2,
                leader: nid(1),
                last_index: 40,
                last_term: 2,
                offset: 0,
                data: image,
                total,
                done: true,
                config: Some(set(&[1, 2, 3])),
                learners: Some(set(&[4])),
            },
        ),
        (
            "InstallSnapshotResp",
            RaftMsg::InstallSnapshotResp {
                term: 2,
                last_index: 40,
                next_offset: total,
            },
        ),
        ("Heartbeat", RaftMsg::Heartbeat { node: nid(3) }),
        ("TimeoutNow", RaftMsg::TimeoutNow { term: 5 }),
        (
            "Quiesce",
            RaftMsg::Quiesce {
                term: 5,
                commit_index: 12,
            },
        ),
        ("WakeRequest", RaftMsg::WakeRequest { term: 5 }),
        ("ClusterProbe", RaftMsg::ClusterProbe),
        (
            "ClusterProbeResp",
            RaftMsg::ClusterProbeResp {
                term: 5,
                committed_index: 12,
                config: set(&[1, 2, 3]),
                ever_heard_from_prober: true,
            },
        ),
        (
            "Removed",
            RaftMsg::Removed {
                term: 5,
                removal_index: 11,
                removal_term: 4,
                config: set(&[1, 2]),
                learners: set(&[]),
            },
        ),
        (
            "RemovedAck",
            RaftMsg::RemovedAck {
                term: 5,
                removal_index: 11,
            },
        ),
    ]
}

/// Exhaustiveness guard: a new `RaftMsg` variant fails to compile here until it
/// is named, and the fixture test fails until `every_raft_msg` includes it.
fn variant_name(m: &RaftMsg<MetaCommand>) -> &'static str {
    match m {
        RaftMsg::PreVote { .. } => "PreVote",
        RaftMsg::PreVoteResp { .. } => "PreVoteResp",
        RaftMsg::RequestVote { .. } => "RequestVote",
        RaftMsg::RequestVoteResp { .. } => "RequestVoteResp",
        RaftMsg::AppendEntries { .. } => "AppendEntries",
        RaftMsg::AppendEntriesResp { .. } => "AppendEntriesResp",
        RaftMsg::InstallSnapshot { .. } => "InstallSnapshot",
        RaftMsg::InstallSnapshotResp { .. } => "InstallSnapshotResp",
        RaftMsg::Heartbeat { .. } => "Heartbeat",
        RaftMsg::TimeoutNow { .. } => "TimeoutNow",
        RaftMsg::Quiesce { .. } => "Quiesce",
        RaftMsg::WakeRequest { .. } => "WakeRequest",
        RaftMsg::ClusterProbe => "ClusterProbe",
        RaftMsg::ClusterProbeResp { .. } => "ClusterProbeResp",
        RaftMsg::Removed { .. } => "Removed",
        RaftMsg::RemovedAck { .. } => "RemovedAck",
    }
}

fn encode_frames(msgs: &[(&'static str, RaftMsg<MetaCommand>)]) -> Vec<u8> {
    let mut out = Vec::new();
    for (_, m) in msgs {
        let body = serde_json::to_vec(m).expect("RaftMsg serializes");
        out.extend_from_slice(&(body.len() as u32).to_be_bytes());
        out.extend_from_slice(&body);
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
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/formats/control-raft-msg")
}

/// The current encoder's bytes equal the checked-in Phase 1 fixture, byte for byte,
/// regardless of the feature handle (the encoder consults none; the handle's
/// state is irrelevant to pre-era emission), and every frame decodes back to an
/// equal re-encoding.
#[test]
fn raft_msg_encoding_is_byte_identical_to_the_phase1_fixture() {
    let fixture = std::fs::read(fixtures_dir().join("v1.bin")).expect("v1.bin is checked in");
    let msgs = every_raft_msg();
    assert_eq!(encode_frames(&msgs), fixture, "B2 pre-era bytes != Phase 1");
    let frames = split_frames(&fixture);
    assert_eq!(frames.len(), msgs.len());
    for ((name, _), frame) in msgs.iter().zip(&frames) {
        let decoded: RaftMsg<MetaCommand> = serde_json::from_slice(frame).expect("decodes");
        assert_eq!(variant_name(&decoded), *name);
        assert_eq!(&serde_json::to_vec(&decoded).unwrap(), frame, "{name}");
    }
}

/// Every variant is in the fixture exactly once (a new variant must be added).
#[test]
fn fixture_covers_every_raft_msg_variant_once() {
    let names: Vec<&str> = every_raft_msg().iter().map(|(n, _)| *n).collect();
    let unique: BTreeSet<&str> = names.iter().copied().collect();
    assert_eq!(unique.len(), names.len(), "duplicate variant in fixture");
    assert_eq!(names.len(), 16, "RaftMsg has 16 variants; add the new one");
    for (n, m) in every_raft_msg() {
        assert_eq!(variant_name(&m), n);
    }
}

/// Refuses to overwrite: run once from a Phase 1 build.
/// `cargo test -p animus-control --test it raft_msg_fixture::generate -- --ignored`
#[test]
#[ignore = "fixture generator; run explicitly, never regenerates an existing fixture"]
fn generate_fixture_control_raft_msg() {
    let dir = fixtures_dir();
    std::fs::create_dir_all(&dir).expect("create fixtures dir");
    let path = dir.join("v1.bin");
    assert!(
        std::fs::metadata(&path).is_err(),
        "{} already exists; a checked-in fixture is never regenerated in place",
        path.display()
    );
    std::fs::write(&path, encode_frames(&every_raft_msg())).expect("write");
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

/// Every existing (Phase 1) `RaftMsg` variant, fed Phase 1 commands, is `Base`.
#[test]
fn every_phase1_raft_msg_is_base() {
    for (name, m) in every_raft_msg() {
        assert_eq!(m.required_gate(), Gate::Base, "{name}");
        assert_eq!(m.envelope_gate(), Gate::Base, "{name}");
    }
}

/// The sample of `MetaCommand`s the fixture uses is Base; the two era commands are Era.
#[test]
fn meta_command_gates() {
    for c in phase1_commands() {
        assert_eq!(c.required_gate(), Gate::Base, "{c:?}");
    }
    assert_eq!(era_report().required_gate(), Gate::Era);
    assert_eq!(era_finalize().required_gate(), Gate::Era);
}

/// An `AppendEntries` carrying an era command is Era (nested recursion), but its
/// *envelope* gate (what a send site checks) stays Base: entries are gated at propose.
#[test]
fn append_entries_inherits_its_entries_gates() {
    let ae = |cmds: Vec<MetaCommand>| RaftMsg::AppendEntries {
        term: 1,
        leader: nid(1),
        prev_log_index: 0,
        prev_log_term: 0,
        entries: cmds
            .into_iter()
            .enumerate()
            .map(|(i, c)| entry(i as u64 + 1, c))
            .collect(),
        leader_commit: 0,
    };
    assert_eq!(ae(vec![]).required_gate(), Gate::Base);
    assert_eq!(
        ae(vec![MetaCommand::NoOp, MetaCommand::NoOp]).required_gate(),
        Gate::Base
    );
    let mixed = ae(vec![MetaCommand::NoOp, era_report(), MetaCommand::NoOp]);
    assert_eq!(mixed.required_gate(), Gate::Era);
    assert_eq!(mixed.envelope_gate(), Gate::Base);
    assert_eq!(ae(vec![era_finalize()]).required_gate(), Gate::Era);
}

/// A closed `Era` gate is refused by `ClusterFeatures::check`, an open `Base` one
/// passes; release builds count and never panic (debug builds assert, covered in
/// `version.rs`'s own unit test).
#[test]
fn features_check_passes_base() {
    let f = ClusterFeatures::new();
    assert!(f.check(animus_control::version::GateSurface::RaftMsg, Gate::Base));
    assert_eq!(
        f.violations(animus_control::version::GateSurface::RaftMsg),
        0
    );
}
