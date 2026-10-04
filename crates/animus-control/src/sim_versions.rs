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

use crate::MetaCommand;
use crate::raft::RaftMsg;
use crate::version::{ClusterVersion, Gate, VersionRange, own_range};

/// Which release a simulated node is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinaryProfile {
    /// Today's (pre-Phase-2) binary: empty `ext`, no own range, decodes only
    /// today's variants.
    Phase1,
    /// The Phase 2 release: range `[1, 1]` ([`own_range`]).
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
            BinaryProfile::B2 => Some(own_range()),
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
        match g.version() {
            // The era gate: every binary that knows versioning (anything but
            // Phase 1).
            None => self != BinaryProfile::Phase1,
            Some(v) => v <= self.max_known_gate_version(),
        }
    }
}

/// **The provisional single-call-site classifier P2-B replaces** with
/// `msg.required_gate()` (its exhaustive tables). Until then the only gated
/// control form is the era pair: an `AppendEntries` carrying
/// `ReportNodeVersion` or `FinalizeClusterVersion` requires `Gate::Era`.
#[must_use]
pub fn provisional_required_gate(msg: &RaftMsg) -> Option<Gate> {
    match msg {
        RaftMsg::AppendEntries { entries, .. } => entries
            .iter()
            .any(|e| {
                matches!(
                    e.command,
                    MetaCommand::ReportNodeVersion { .. }
                        | MetaCommand::FinalizeClusterVersion { .. }
                )
            })
            .then_some(Gate::Era),
        _ => None,
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

impl SimCap {
    /// Whether `msg` must be rejected; logs it when so.
    pub(crate) fn rejects(&self, from: &NodeId, to: &NodeId, at: Nanos, msg: &RaftMsg) -> bool {
        match provisional_required_gate(msg) {
            Some(g) if !self.profile.accepts(g) => {
                self.log.push(CapRejection {
                    at,
                    from: from.clone(),
                    to: to.clone(),
                    gate: g,
                    profile: self.profile,
                });
                true
            }
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn era_gate_accepted_by_all_but_phase1() {
        assert!(!BinaryProfile::Phase1.accepts(Gate::Era));
        assert!(BinaryProfile::B2.accepts(Gate::Era));
        assert!(BinaryProfile::Release(2).accepts(Gate::Era));
    }

    #[test]
    fn classifier_flags_only_the_era_commands() {
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
        assert_eq!(
            provisional_required_gate(&ae(entry(report))),
            Some(Gate::Era)
        );
        assert_eq!(
            provisional_required_gate(&ae(entry(MetaCommand::NoOp))),
            None
        );
        let hb = RaftMsg::<MetaCommand>::Heartbeat {
            node: animus_env::nid(0),
        };
        assert_eq!(provisional_required_gate(&hb), None);
    }
}
