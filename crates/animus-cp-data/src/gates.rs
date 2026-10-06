//! ADR 0073 Phase 2 (P2-B): the gate tables for the data plane's two gated
//! enums, `KvCommand` (log entries) and `KvWire` (frames), plus the helpers
//! the send/propose sites share.
//!
//! Both are **exhaustive matches with no `_` arm**: a new variant does not
//! compile until it names its gate. Every variant is [`Gate::Base`] (cluster
//! version 1: Phase 1 and B2 emit exactly these) **except** that the evaluated
//! writes (`KindEval`, `KindEvalBatch`, a `TxnStage`'s pending writes) are
//! [`Gate::MrecReplication`] when they carry MREC content (`WriteSchema.mrec`,
//! `KindEvalOp::Replicate`; ADR 0075, G-01 stage G-d): the gate is
//! content-dependent because an older voter silently ignores an unknown JSON
//! field. Every emit site routes through [`ClusterFeatures::check`].
//!
//! **Send sites check the envelope only, never the entries.** An
//! `AppendEntries`' commands are gated where they are *created* (the propose
//! sites, [`check_propose`]): a send site cannot judge them soundly, because a
//! leader's applied view lags its log and a new leader resends entries an
//! earlier leader proposed under a gate that was open then (gates only ever
//! open). [`KvWire::envelope_gate`] therefore excludes entry commands, while
//! [`KvWire::required_gate`] is the full-message gate (tests, receivers).

use animus_control::version::{ClusterFeatures, Gate, GateSurface, GatedCommand};

use crate::{KindEvalOp, KvCommand, KvWire};

impl GatedCommand for KvCommand {
    fn required_gate(&self) -> Gate {
        match self {
            // ADR 0075 (G-01 stage G-d): an entry that carries MREC content
            // (`WriteSchema.mrec`, `KindEvalOp::Replicate`) is
            // `Gate::MrecReplication`; the same variants without it stay
            // `Base`, so every ordinary table's write is unchanged. An older
            // voter silently ignores the unknown `mrec` field (serde) or fails
            // to decode the op, so the gate is the only thing that protects it.
            KvCommand::KindEval { schema, op, .. } => eval_gate(schema, op),
            KvCommand::KindEvalBatch { entries, .. } => entries
                .iter()
                .fold(Gate::Base, |g, e| g.join(eval_gate(&e.schema, &e.op))),
            KvCommand::TxnStage { writes, .. } => writes
                .iter()
                .filter_map(|w| w.pending.as_ref())
                .fold(Gate::Base, |g, p| g.join(eval_gate(&p.schema, &p.op))),
            KvCommand::Put { .. }
            | KvCommand::Batch { .. }
            | KvCommand::KindBatch { .. }
            | KvCommand::SeedBatch { .. }
            | KvCommand::Delete { .. }
            | KvCommand::Cas { .. }
            | KvCommand::Freeze { .. }
            | KvCommand::SplitTablet { .. }
            | KvCommand::ReadCeiling { .. }
            | KvCommand::TxnCommit { .. }
            | KvCommand::TxnAbort { .. }
            | KvCommand::TxnResolve { .. }
            | KvCommand::NoOp => Gate::Base,
        }
    }
}

/// The gate one evaluated write needs: `MrecReplication` when it carries MREC
/// content, `Base` otherwise.
fn eval_gate(schema: &animus_item::WriteSchema, op: &KindEvalOp) -> Gate {
    if schema.mrec.is_some() || matches!(op, KindEvalOp::Replicate { .. }) {
        Gate::MrecReplication
    } else {
        Gate::Base
    }
}

impl KvWire {
    /// The gate the frame itself needs, **excluding** the commands carried by
    /// `AppendEntries` entries (see the module doc). Exhaustive.
    pub(crate) fn envelope_gate(&self) -> Gate {
        match self {
            KvWire::Raft(m) => m.envelope_gate(),
            KvWire::ReadProbe { .. } | KvWire::ReadProbeAck { .. } => Gate::Base,
            KvWire::HeartbeatBatch(entries) => entries
                .iter()
                .fold(Gate::Base, |g, (_, m)| g.join(m.envelope_gate())),
        }
    }

    /// The full-message gate: the envelope joined with every carried entry
    /// command's gate. Exhaustive. Used by tests today; P2-C's receive-side
    /// checks are its production caller.
    #[cfg_attr(
        not(test),
        allow(dead_code, reason = "receive-side check lands with P2-C")
    )]
    pub(crate) fn required_gate(&self) -> Gate {
        match self {
            KvWire::Raft(m) => m.required_gate(),
            KvWire::ReadProbe { .. } | KvWire::ReadProbeAck { .. } => Gate::Base,
            KvWire::HeartbeatBatch(entries) => entries
                .iter()
                .fold(Gate::Base, |g, (_, m)| g.join(m.required_gate())),
        }
    }
}

/// Propose-site check for a data-plane command: `true` when the command's
/// gate is open; on `false` the caller must refuse to propose it.
pub(crate) fn check_propose(features: &ClusterFeatures, cmd: &KvCommand) -> bool {
    features.check(GateSurface::KvCommand, cmd.required_gate())
}

/// Send-site check for a [`KvWire`] frame (envelope only, see the module doc):
/// `true` when it may be sent; on `false` the caller must drop the frame.
pub(crate) fn check_send(features: &ClusterFeatures, wire: &KvWire) -> bool {
    features.check(GateSurface::KvWire, wire.envelope_gate())
}

/// Check the frame's envelope gate and encode it at the frame version the
/// sender's `features` select; `None` means the gate is closed and the caller
/// must drop the frame (never ship it). The one choke point every send site
/// uses.
pub(crate) fn encode_for_send(wire: &KvWire, features: &ClusterFeatures) -> Option<Vec<u8>> {
    check_send(features, wire).then(|| crate::codec::encode_wire(wire, features))
}

/// A `Metadata` whose era has started (one registered node with one applied
/// `ReportNodeVersion`), for tests that need an era-open `ClusterFeatures`.
#[cfg(test)]
pub(crate) fn era_on_metadata() -> animus_control::Metadata {
    use animus_control::meta::NodeAddrs;
    use animus_control::version::VersionRange;
    use animus_control::{ApplyOutcome, MetaCommand, Metadata};
    let mut m = Metadata::default();
    let node = animus_env::nid(1);
    let registered = m.apply(&MetaCommand::RegisterNode {
        node: node.clone(),
        addrs: NodeAddrs {
            internal: "i".into(),
            client: "c".into(),
            intra: "x".into(),
            admin: "a".into(),
            role: "control".into(),
        },
        labels: Default::default(),
    });
    assert_eq!(registered, ApplyOutcome::Applied);
    let reported = m.apply(&MetaCommand::ReportNodeVersion {
        node,
        range: VersionRange::new(1, 1),
        build: "t".into(),
    });
    assert_eq!(reported, ApplyOutcome::Applied);
    assert!(m.versioning_active());
    m
}

#[cfg(test)]
mod tests {
    use super::*;
    use animus_control::{LogEntry, MetaCommand, RaftMsg};
    use animus_env::nid;

    /// A toy era-gated command, standing in for a future non-Base `KvCommand`
    /// (none exists yet), to prove nested recursion through `AppendEntries`.
    #[derive(Clone, Debug)]
    struct Toy(Gate);
    impl GatedCommand for Toy {
        fn required_gate(&self) -> Gate {
            self.0
        }
    }

    fn append<C>(cmds: Vec<C>) -> RaftMsg<C> {
        RaftMsg::AppendEntries {
            term: 1,
            leader: nid(1),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: cmds
                .into_iter()
                .enumerate()
                .map(|(i, command)| LogEntry {
                    term: 1,
                    index: i as u64 + 1,
                    command,
                    config: None,
                    learners: None,
                })
                .collect(),
            leader_commit: 0,
        }
    }

    #[test]
    fn append_entries_gate_is_the_join_of_its_entries_and_the_envelope_ignores_them() {
        let m = append(vec![Toy(Gate::Base), Toy(Gate::Era)]);
        assert_eq!(m.required_gate(), Gate::Era);
        assert_eq!(m.envelope_gate(), Gate::Base);
        assert_eq!(append::<Toy>(vec![]).required_gate(), Gate::Base);
    }

    #[test]
    fn kv_wire_gates_recurse_into_raft_and_heartbeat_batches() {
        let kv = |c: KvCommand| append(vec![c]);
        assert_eq!(
            KvWire::Raft(kv(KvCommand::NoOp)).required_gate(),
            Gate::Base
        );
        assert_eq!(
            KvWire::ReadProbe { term: 1, epoch: 2 }.required_gate(),
            Gate::Base
        );
        assert_eq!(
            KvWire::ReadProbeAck { term: 1, epoch: 2 }.envelope_gate(),
            Gate::Base
        );
        assert_eq!(
            KvWire::HeartbeatBatch(vec![(1, kv(KvCommand::NoOp)), (2, append(vec![]))])
                .required_gate(),
            Gate::Base
        );
        assert_eq!(KvCommand::NoOp.required_gate(), Gate::Base);
        // `MetaCommand` is not a data-plane command, but the same `RaftMsg`
        // table serves both planes: an era control command is Era here too.
        let era = MetaCommand::FinalizeClusterVersion {
            expected: 1,
            target: 2,
        };
        assert_eq!(append(vec![era]).required_gate(), Gate::Era);
    }

    fn mrec_schema(mrec: bool) -> animus_item::WriteSchema {
        animus_item::WriteSchema {
            key: animus_item::TableSchema::simple("pk"),
            lsis: Vec::new(),
            change_records_carry_images: false,
            mrec: mrec.then_some(animus_item::MrecWriteStamp {
                region_id: 1,
                wall_ms: 2,
            }),
        }
    }

    fn kind_eval(mrec: bool, op: KindEvalOp) -> KvCommand {
        KvCommand::KindEval {
            schema: mrec_schema(mrec),
            pk: animus_item::AttributeValue::S("a".into()),
            sk: None,
            op,
            condition: None,
            ttl_expired: false,
            ts: crate::hlc::HlcTimestamp {
                wall_ms: 1,
                logical: 0,
            },
        }
    }

    /// ADR 0075 G-d, per-gate test: MREC content is `MrecReplication`, the same
    /// variants without it stay `Base`, in every carrier (`KindEval`,
    /// `KindEvalBatch`, a `TxnStage` pending write), and the propose-site
    /// predicate agrees with the gate being open or closed.
    #[test]
    fn mrec_content_needs_the_mrec_gate_in_every_carrier() {
        let ver = animus_item::MrecVersion::ZERO;
        let item = animus_item::Item::new();
        let replicate = KindEvalOp::Replicate {
            item: Some(item.clone()),
            ver,
        };
        assert_eq!(
            kind_eval(false, KindEvalOp::Put(item.clone())).required_gate(),
            Gate::Base
        );
        assert_eq!(
            kind_eval(true, KindEvalOp::Put(item.clone())).required_gate(),
            Gate::MrecReplication
        );
        assert_eq!(
            kind_eval(false, replicate.clone()).required_gate(),
            Gate::MrecReplication
        );
        let entry = |mrec: bool, op: KindEvalOp| crate::KindEvalEntry {
            schema: mrec_schema(mrec),
            pk: animus_item::AttributeValue::S("a".into()),
            sk: None,
            op,
            condition: None,
            ttl_expired: false,
        };
        let ts = crate::hlc::HlcTimestamp {
            wall_ms: 1,
            logical: 0,
        };
        let batch = |entries| KvCommand::KindEvalBatch { entries, ts };
        assert_eq!(batch(vec![]).required_gate(), Gate::Base);
        assert_eq!(
            batch(vec![entry(false, KindEvalOp::Delete)]).required_gate(),
            Gate::Base
        );
        assert_eq!(
            batch(vec![
                entry(false, KindEvalOp::Delete),
                entry(false, replicate.clone())
            ])
            .required_gate(),
            Gate::MrecReplication,
            "one MREC entry gates the whole batch"
        );
        let stage = |mrec: bool| KvCommand::TxnStage {
            txn_id: crate::txn::TxnId {
                ts,
                node: animus_env::nid(1),
            },
            record_key: Vec::new(),
            record_table: "t".into(),
            is_anchor: true,
            writes: vec![crate::txn::TxnWrite::pending_eval(
                b"k".to_vec(),
                None,
                crate::PendingTxnWrite {
                    schema: mrec_schema(mrec),
                    pk: animus_item::AttributeValue::S("a".into()),
                    sk: None,
                    op: KindEvalOp::Delete,
                    condition: None,
                    ttl_expired: false,
                },
            )],
            spans: Vec::new(),
            conditions: Vec::new(),
            ts,
        };
        assert_eq!(stage(false).required_gate(), Gate::Base);
        assert_eq!(stage(true).required_gate(), Gate::MrecReplication);
        // Propose-site predicate: closed at the floor, open at version 3. The
        // gate-closed path itself is `check`'s `debug_assert`, so the closed
        // side is asserted through `is_open`, never by calling `check_propose`.
        let floor = ClusterFeatures::new();
        assert!(!floor.is_open(Gate::MrecReplication));
        assert!(check_propose(&floor, &kind_eval(false, KindEvalOp::Delete)));
        let open = ClusterFeatures::new();
        let m3 = animus_control::Metadata {
            cluster_version: 3,
            ..Default::default()
        };
        open.update(&m3);
        assert!(open.is_open(Gate::MrecReplication));
        assert!(check_propose(&open, &kind_eval(true, replicate)));
    }

    /// A `Replicate` that reaches apply on a table whose entry carries no
    /// `mrec` context (it cannot through a correct proposer) is a
    /// deterministic rejection that writes nothing, never a panic or a
    /// versioned row on a non-MREC table.
    #[test]
    fn replicate_without_mrec_context_is_a_deterministic_rejection() {
        let op = KindEvalOp::Replicate {
            item: Some(animus_item::Item::new()),
            ver: animus_item::MrecVersion::ZERO,
        };
        let decision = crate::evaluate_kind_eval(
            &mrec_schema(false),
            &animus_item::AttributeValue::S("a".into()),
            None,
            &[0u8; 8],
            None,
            None,
            &op,
            None,
            false,
        );
        assert!(matches!(decision, crate::KindEvalDecision::Rejected { .. }));
    }

    #[test]
    fn the_checks_pass_for_base_and_count_nothing() {
        let f = ClusterFeatures::new();
        assert!(check_propose(&f, &KvCommand::NoOp));
        assert!(check_send(&f, &KvWire::ReadProbe { term: 1, epoch: 1 }));
        for &s in GateSurface::ALL {
            assert_eq!(f.violations(s), 0);
        }
    }

    /// The injected handle is the node's own (shared cell, not a copy), the
    /// default constructors start at the floor, and a 3-replica group
    /// elects, replicates and snapshots with the handle attached without a
    /// single gate violation (every send, snapshot and propose site is
    /// exercised by an ordinary write).
    #[test]
    fn an_injected_handle_is_shared_and_a_live_group_trips_no_violation() {
        use std::time::Duration;

        use animus_env::nid;
        use animus_sim::{SimEnv, Simulator};
        use animus_storage::MemoryEngine;

        use crate::{HostedOptions, RaftKvNode, StorageScope};

        let mut sim = Simulator::new(0x0B2_6A7E);
        let features = ClusterFeatures::new();
        let nodes: Vec<RaftKvNode<SimEnv, MemoryEngine>> = (0..3u64)
            .map(|i| {
                RaftKvNode::start_hosted_with_options(
                    sim.env(nid(i)),
                    vec![nid(0), nid(1), nid(2)],
                    MemoryEngine::new(),
                    StorageScope::whole(),
                    5,
                    HostedOptions {
                        features: features.clone(),
                        ..HostedOptions::default()
                    },
                )
            })
            .collect();
        let default_node = RaftKvNode::<SimEnv, MemoryEngine>::start(
            sim.env(nid(7)),
            vec![nid(7)],
            MemoryEngine::new(),
        );
        sim.run_for(Duration::from_secs(2));
        // Shared cell: feeding the injected handle is visible through the node.
        features.update(&era_on_metadata());
        assert!(nodes[0].features().is_open(Gate::Era));
        assert!(
            !default_node.features().is_open(Gate::Era),
            "a default-constructed node holds its own floor handle"
        );
        let leader = nodes
            .iter()
            .find(|n| n.is_leader())
            .expect("a leader was elected");
        for i in 0..40u8 {
            let _ = leader.put(vec![i], vec![i; 64]);
        }
        sim.run_for(Duration::from_secs(2));
        for &s in GateSurface::ALL {
            assert_eq!(features.violations(s), 0, "{s:?}");
        }
    }
}
