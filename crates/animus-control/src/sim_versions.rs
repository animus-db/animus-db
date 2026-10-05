//! Test-only binary profiles for the ADR 0073 Phase 2 mixed-version corpus
//! (P2-D). Compiled only under `cfg(any(test, feature = "sim-versions"))`.
//!
//! A [`BinaryProfile`] says which release a simulated node "is": `Phase1`
//! (today's binary: no handshake `ext`, no own range, decode-only of today's
//! variants), `B2` (range `[1,1]`) or `Release(N)` (range `[N-1, N]`).
//! [`RaftNode::set_binary_profile`](crate::RaftNode::set_binary_profile)
//! applies a profile atomically (own range + build + decode cap) and returns a
//! [`CapLog`].
//!
//! **Capped decode.** A node whose profile does not know a gate rejects, at the
//! control receive site, exactly what the real older binary would: the message
//! takes the same branch as an undecodable one (logged, dropped, nothing handed
//! to the core). Every such rejection is recorded in the [`CapLog`]; the
//! corpus's *delivery assertion* is "no node ever logs a rejection in a
//! positive cell". A premature emitter shows up as a non-empty log (and as a
//! wedged replica: the whole `AppendEntries` is lost, the leader retries the
//! same batch forever).
//!
//! **Inert unless installed.** A node with no profile installed never touches
//! this module on the receive path beyond one `Option::is_none` check.

use std::sync::{Arc, Mutex};

use animus_env::handshake::encode_ext;
use animus_env::{Nanos, NodeId};

use crate::raft::RaftMsg;
use crate::version::{ClusterVersion, Gate, VersionRange};

/// Which release a simulated node is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinaryProfile {
    /// Today's (pre-Phase-2) binary: empty `ext`, no own range, decodes only
    /// today's variants.
    Phase1,
    /// The Phase 2 release: range `[1, 1]` (a literal, see `own_range`'s
    /// pin below: it must not track `MAX_SUPPORTED`).
    B2,
    /// A later release `N`: range `[max(N-1, 1), N]`, knows every gate up to
    /// version `N`.
    Release(ClusterVersion),
}

impl BinaryProfile {
    /// The own range the node's `RaftNode` advertises (`None` = Phase 1).
    #[must_use]
    pub fn own_range(self) -> Option<VersionRange> {
        match self {
            BinaryProfile::Phase1 => None,
            // Pinned to a literal, NOT `own_range()`: `own_range()` tracks
            // `MAX_SUPPORTED`, so the first real bump (G-01 G-c, version 2)
            // would silently turn the B2 profile into a `[1, 2]` binary and
            // the mixed-version corpus would stop testing a B2 node at all.
            // B2 is the Phase 2 release as it shipped: `[1, 1]`.
            BinaryProfile::B2 => Some(VersionRange::new(1, 1)),
            BinaryProfile::Release(n) => Some(VersionRange::new(n.saturating_sub(1).max(1), n)),
        }
    }

    /// The build string.
    #[must_use]
    pub fn build(self) -> String {
        match self {
            BinaryProfile::Phase1 => "phase1".to_string(),
            BinaryProfile::B2 => "b2".to_string(),
            BinaryProfile::Release(n) => format!("r{n}"),
        }
    }

    /// The handshake `ext` the node advertises (empty for Phase 1).
    #[must_use]
    pub fn ext(self) -> Vec<u8> {
        match self.own_range() {
            None => Vec::new(),
            Some(r) => encode_ext(Some((r.min, r.max)), Some(&self.build())),
        }
    }

    /// The highest gate version this binary knows (0 = none: Phase 1).
    #[must_use]
    pub fn max_known_gate_version(self) -> ClusterVersion {
        match self {
            BinaryProfile::Phase1 => 0,
            BinaryProfile::B2 => 1,
            BinaryProfile::Release(n) => n,
        }
    }

    /// Whether this binary can decode a message that requires `g`.
    #[must_use]
    pub fn accepts(self, g: Gate) -> bool {
        // `Gate::Base` is everything Phase 1 itself emits: always decodable,
        // by every profile (including Phase 1, whose max known version is 0).
        if g == Gate::Base {
            return true;
        }
        match g.version() {
            // The era gate: every binary that knows versioning (anything but
            // Phase 1).
            None => self != BinaryProfile::Phase1,
            Some(v) => v <= self.max_known_gate_version(),
        }
    }
}

/// One rejected delivery.
#[derive(Clone, Debug)]
pub struct CapRejection {
    /// Virtual time of the receive.
    pub at: Nanos,
    /// Sender.
    pub from: NodeId,
    /// Receiver (the capped node).
    pub to: NodeId,
    /// The gate the message required.
    pub gate: Gate,
    /// Which profile rejected it.
    pub profile: BinaryProfile,
}

/// The shared rejection log of one node (cheap to clone).
#[derive(Clone, Debug, Default)]
pub struct CapLog(Arc<Mutex<Vec<CapRejection>>>);

impl CapLog {
    /// Every rejection so far.
    #[must_use]
    pub fn rejections(&self) -> Vec<CapRejection> {
        self.0.lock().expect("cap log poisoned").clone()
    }

    /// Number of rejections so far.
    #[must_use]
    pub fn count(&self) -> usize {
        self.0.lock().expect("cap log poisoned").len()
    }

    pub(crate) fn push(&self, r: CapRejection) {
        self.0.lock().expect("cap log poisoned").push(r);
    }
}

/// The installed cap of one node.
#[derive(Clone, Debug)]
pub struct SimCap {
    /// The profile the node plays.
    pub profile: BinaryProfile,
    /// Where rejections go.
    pub log: CapLog,
}

/// The `UpsertMember` label that models a **new field** on an existing
/// command: an older binary cannot decode it, but [`required_gate`]
/// (the emitter's classification) deliberately does not know it (the
/// "ungated field" negative control, N3). Value: the decimal gate version.
///
/// [`required_gate`]: crate::version::GatedCommand::required_gate
pub const SYNTHETIC_FIELD_LABEL: &str = "synthetic.field";

/// The gate the *content* of `msg` needs, read the way an older binary's
/// strict decode would: the synthetic gate/field labels of every
/// `UpsertMember` entry, whatever the emitter's classification says.
#[must_use]
pub fn content_gate(msg: &RaftMsg) -> Gate {
    let RaftMsg::AppendEntries { entries, .. } = msg else {
        return Gate::Base;
    };
    entries.iter().fold(Gate::Base, |g, e| match &e.command {
        crate::MetaCommand::UpsertMember { labels, .. } => {
            [crate::version::SYNTHETIC_GATE_LABEL, SYNTHETIC_FIELD_LABEL]
                .iter()
                .filter_map(|k| labels.get(*k)?.parse::<u32>().ok())
                .fold(g, |g, n| g.join(Gate::Synthetic(n)))
        }
        _ => g,
    })
}

impl SimCap {
    /// Whether `msg` must be rejected; logs it when so.
    pub(crate) fn rejects(&self, from: &NodeId, to: &NodeId, at: Nanos, msg: &RaftMsg) -> bool {
        // P2-B's full-message table: the envelope joined with every entry
        // command of an `AppendEntries`.
        // joined with what the *bytes* need (an older binary's strict decode
        // does not read the emitter's classification): a payload field the
        // classifier does not know about still needs its gate.
        let g = msg.required_gate().join(content_gate(msg));
        if self.profile.accepts(g) {
            return false;
        }
        self.log.push(CapRejection {
            at,
            from: from.clone(),
            to: to.clone(),
            gate: g,
            profile: self.profile,
        });
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MetaCommand;

    #[test]
    fn profile_derivations() {
        assert_eq!(BinaryProfile::Phase1.own_range(), None);
        assert!(BinaryProfile::Phase1.ext().is_empty());
        assert_eq!(BinaryProfile::B2.own_range(), Some(VersionRange::new(1, 1)));
        assert!(!BinaryProfile::B2.ext().is_empty());
        assert_eq!(
            BinaryProfile::Release(3).own_range(),
            Some(VersionRange::new(2, 3))
        );
        assert_eq!(
            BinaryProfile::Release(1).own_range(),
            Some(VersionRange::new(1, 1))
        );
    }

    /// B2 is pinned to the literal `[1, 1]`, not `own_range()`: the real
    /// `MAX_SUPPORTED` is 2 now, and a B2 that silently became `[1, 2]` would
    /// stop modelling the Phase 2 release the mixed-version corpus rolls from.
    #[test]
    fn b2_is_pinned_to_one_one_and_release_two_is_the_current_binary() {
        assert_eq!(BinaryProfile::B2.own_range(), Some(VersionRange::new(1, 1)));
        assert_eq!(
            BinaryProfile::Release(crate::version::MAX_SUPPORTED).own_range(),
            Some(crate::version::own_range()),
            "Release(MAX_SUPPORTED) is what the real binary advertises"
        );
        assert_ne!(
            BinaryProfile::B2.own_range(),
            Some(crate::version::own_range())
        );
    }

    #[test]
    fn era_gate_accepted_by_all_but_phase1() {
        assert!(BinaryProfile::Phase1.accepts(Gate::Base));
        assert!(!BinaryProfile::Phase1.accepts(Gate::Era));
        assert!(BinaryProfile::B2.accepts(Gate::Era));
        assert!(BinaryProfile::Release(2).accepts(Gate::Era));
    }

    /// Per-gate acceptance matrix (ADR 0073 P2-D): every profile against
    /// every gate, the ladder included.
    #[test]
    fn profiles_accept_gates_exactly_up_to_their_known_version() {
        let ladder = [Gate::Synthetic(2), Gate::Synthetic(3)];
        for &g in Gate::ALL.iter().chain(&ladder) {
            let row: Vec<bool> = [
                BinaryProfile::Phase1,
                BinaryProfile::B2,
                BinaryProfile::Release(2),
                BinaryProfile::Release(3),
            ]
            .iter()
            .map(|p| p.accepts(g))
            .collect();
            let want = match g {
                Gate::Base => [true, true, true, true],
                Gate::Era => [false, true, true, true],
                Gate::GlobalTables => [false, false, true, true],
                Gate::Synthetic(2) => [false, false, true, true],
                Gate::Synthetic(3) => [false, false, false, true],
                Gate::Synthetic(_) => unreachable!(),
            };
            assert_eq!(row, want, "{g:?}");
        }
    }

    /// The cap reads the content, not the classification: a field the
    /// classifier does not know still needs its gate.
    #[test]
    fn content_gate_sees_an_unclassified_field() {
        use crate::raft::LogEntry;
        let upsert = |k: &str| MetaCommand::UpsertMember {
            node: animus_env::nid(1),
            labels: [(k.to_owned(), "3".to_owned())].into(),
            status: crate::NodeStatus::Active,
        };
        let ae = |c: MetaCommand| RaftMsg::AppendEntries {
            term: 1,
            leader: animus_env::nid(0),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![LogEntry {
                term: 1,
                index: 1,
                command: c,
                config: None,
                learners: None,
            }],
            leader_commit: 0,
        };
        let field = ae(upsert(SYNTHETIC_FIELD_LABEL));
        assert_eq!(field.required_gate(), Gate::Base, "the classifier is blind");
        assert_eq!(content_gate(&field), Gate::Synthetic(3));
        let gated = ae(upsert(crate::version::SYNTHETIC_GATE_LABEL));
        assert_eq!(gated.required_gate(), Gate::Synthetic(3));
        assert_eq!(content_gate(&gated), Gate::Synthetic(3));
        assert_eq!(content_gate(&ae(upsert("other"))), Gate::Base);
    }

    #[test]
    fn required_gate_flags_only_the_era_commands() {
        use crate::raft::LogEntry;
        let entry = |command: MetaCommand| LogEntry {
            term: 1,
            index: 1,
            command,
            config: None,
            learners: None,
        };
        let ae = |e: LogEntry| RaftMsg::AppendEntries {
            term: 1,
            leader: animus_env::nid(0),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![e],
            leader_commit: 0,
        };
        let report = MetaCommand::ReportNodeVersion {
            node: animus_env::nid(1),
            range: VersionRange::new(1, 1),
            build: "b2".into(),
        };
        assert_eq!(ae(entry(report)).required_gate(), Gate::Era);
        assert_eq!(ae(entry(MetaCommand::NoOp)).required_gate(), Gate::Base);
        let hb = RaftMsg::<MetaCommand>::Heartbeat {
            node: animus_env::nid(0),
        };
        assert_eq!(hb.required_gate(), Gate::Base);
    }
}
