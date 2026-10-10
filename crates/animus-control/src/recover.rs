//! Offline "force new configuration" recovery of a Raft group's durable state
//! (ADR 0077, issue #1178).
//!
//! When a majority of a group's voters is permanently gone, no
//! configuration change can commit: the surviving voters can neither elect
//! a leader nor replicate a membership entry. The only way forward is to
//! **rewrite the survivor's own durable state** so that it is the group's
//! sole voter. That is what this module does, and *only* that:
//!
//! 1. [`plan`] reads the survivor's WAL (read-only; no tail repair) and
//!    reports what the rewrite would do and what it may discard.
//! 2. [`apply`], given a [`DataLossAcknowledged`] token, backs the WAL up
//!    and appends **two ordinary records** to it: a [`WalRecord::Hard`] that
//!    bumps the term by [`RECOVERY_TERM_JUMP`], and a [`WalRecord::Append`]
//!    of a no-op log entry **carrying the new configuration** `{survivor}`
//!    (no learners) at that term. No new record shape, no new format tag
//!    (ADR 0073): a config-bearing entry is exactly what
//!    `change_membership` appends, so every decoder, `RaftCore::recovered`
//!    and the stale-voter fencing below treat it as a normal membership
//!    change that happens to be uncommitted-but-unopposed.
//!
//! On restart the survivor recovers `config = {survivor}`, wins the
//! (one-vote) election at a term above the entry's, and commits everything
//! in its log, including the entries it held but had never seen committed.
//!
//! # What is preserved, what is not
//!
//! Preserved: everything in the survivor's durable WAL and system-keyspace
//! engine, i.e. every entry it had appended, **whether or not it knew it
//! was committed**. Lost: any write the old group acknowledged that this
//! survivor never received (it was acknowledged by a majority that did not
//! include the survivor). The tool cannot know which is which; the plan
//! reports the count of tail entries whose commit status is unknown.
//!
//! # Fencing the old voters
//!
//! The other old voters must be **wiped** before they restart (ADR 0077).
//! If they come back with their state anyway, the new configuration entry
//! fences them on contact: it carries a term above anything they can have
//! reached (a jump of [`RECOVERY_TERM_JUMP`] over the highest term the
//! survivor knows), so they adopt the survivor's term and step down, and a
//! campaign by one of them is answered by the leader's explicit
//! `RaftMsg::Removed` notice, stamped with the new entry, which leaves it a
//! non-voter exactly as for any removed voter. This is best effort for a
//! stale *minority* of the old voters that can reach the survivor; two or
//! more stale voters that cannot reach it can still elect each other in the
//! old configuration, which is why the runbook says to wipe them first.
//!
//! The functions are generic over `RaftCore<C, S>` so the per-tablet
//! data-plane groups (ADR 0077 phase 2) can reuse them, but only the
//! control plane's WAL file ([`CONTROL_WAL_FILE`]) is wired today.

use std::collections::BTreeSet;
use std::io;

use animus_env::{Env, NodeId};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::format::FormatError;
use crate::persist::{PersistedState, WalRecord, WalRecoverError};
use crate::raft::{LogEntry, RaftCore, StateMachine};

/// The control plane's Raft WAL file in a node's internal-env directory.
pub const CONTROL_WAL_FILE: &str = "raft.wal";

/// How far the recovery term is placed above the highest term the survivor
/// has seen. A stale old voter that kept running can be ahead of the
/// survivor's persisted term (it may have won elections the survivor never
/// learned of); a generous jump makes the survivor's term win the
/// term comparison against any plausible amount of such drift, so the first
/// message exchange makes the stale node adopt it. Terms are `u64`; the
/// jump is not a protocol quantity and no other code depends on its value.
pub const RECOVERY_TERM_JUMP: u64 = 1 << 16;

/// Proof the caller has put the data-loss warning in front of the operator.
/// [`apply`] cannot be called without one; the CLI mints it only from the
/// explicit acknowledgement flag.
#[derive(Debug, Clone, Copy)]
pub struct DataLossAcknowledged(());

impl DataLossAcknowledged {
    /// Construct the token. The name is the acknowledgement: calling this
    /// asserts that the operator was told which writes may be lost and that
    /// every other old voter is gone or will be wiped before it restarts.
    #[must_use]
    pub const fn acknowledging_that_unseen_writes_are_lost_and_other_voters_must_be_wiped() -> Self
    {
        Self(())
    }
}

/// Why a recovery was refused or failed.
#[derive(Debug)]
pub enum ForceConfigError {
    /// The WAL failed to decode or its tail could not be repaired. Never
    /// treated as "empty": real corruption needs an operator, not a rewrite.
    Wal(WalRecoverError),
    /// The WAL could not be read or written.
    Io(io::Error),
    /// The WAL decodes to nothing: this node never ran (or was wiped).
    /// Rewriting would turn a blank disk into a one-node cluster: refused.
    EmptyState,
    /// This node is not a voter in the configuration its own log records
    /// (a learner, or not a member at all). Recovering from a non-voter is
    /// refused: pick a survivor that was a voter.
    NotAVoter {
        node: NodeId,
        voters: BTreeSet<NodeId>,
        learners: BTreeSet<NodeId>,
    },
    /// The WAL changed between [`plan`] and [`apply`] (something else is
    /// writing it, or the plan is stale): re-plan.
    PlanChanged,
}

impl std::fmt::Display for ForceConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Wal(e) => write!(f, "the WAL cannot be recovered: {e}"),
            Self::Io(e) => write!(f, "WAL I/O failed: {e}"),
            Self::EmptyState => write!(
                f,
                "the WAL is empty: this node holds no control-plane state to recover from"
            ),
            Self::NotAVoter {
                node,
                voters,
                learners,
            } => write!(
                f,
                "{node} is not a voter in its own recorded configuration \
                 (voters {voters:?}, learners {learners:?}); recover from a node that was a voter"
            ),
            Self::PlanChanged => write!(
                f,
                "the WAL changed between planning and applying; is a node still running on this directory?"
            ),
        }
    }
}

impl std::error::Error for ForceConfigError {}

impl From<io::Error> for ForceConfigError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

/// What [`apply`] would do (or did) to a survivor's WAL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForceNewConfigPlan {
    /// The survivor.
    pub node: NodeId,
    /// Term recorded in the WAL before the rewrite.
    pub old_term: u64,
    /// Highest term of any record the survivor holds (hard state or log).
    pub highest_known_term: u64,
    /// Term the rewrite records ([`RECOVERY_TERM_JUMP`] above the highest known).
    pub new_term: u64,
    /// The voters the survivor's log says the group had.
    pub old_voters: BTreeSet<NodeId>,
    /// The learners it said the group had.
    pub old_learners: BTreeSet<NodeId>,
    /// Index of the survivor's snapshot base (0 when it has none).
    pub snapshot_index: u64,
    /// Index of the survivor's last log entry (the snapshot base if the tail is empty).
    pub last_log_index: u64,
    /// Entries in the log tail after the snapshot base. Whether the old group
    /// had committed them is unknown to this node: they are kept and will be
    /// treated as committed.
    pub tail_entries: u64,
    /// Index the new configuration entry takes.
    pub config_entry_index: u64,
    /// The WAL already ends in a configuration naming only this node: nothing to do.
    pub already_single_voter: bool,
}

/// Outcome of [`apply`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForceNewConfigOutcome {
    /// The plan that was applied.
    pub plan: ForceNewConfigPlan,
    /// The name of the pre-rewrite copy of the WAL, or `None` when the plan
    /// was a no-op (`already_single_voter`).
    pub backup_file: Option<String>,
}

async fn read_wal<E: Env>(env: &E, file: &str) -> io::Result<Vec<u8>> {
    match env.read(file).await {
        Ok(bytes) => Ok(bytes),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e),
    }
}

fn plan_from_state<C, S>(
    node: &NodeId,
    initial_voters: &[NodeId],
    state: PersistedState<C, S>,
) -> Result<ForceNewConfigPlan, ForceConfigError>
where
    C: Clone + std::fmt::Debug + Serialize + DeserializeOwned,
    S: StateMachine<C> + Serialize + DeserializeOwned,
{
    if state.is_empty() {
        return Err(ForceConfigError::EmptyState);
    }
    let old_term = state.term;
    let snapshot_term = state.snapshot.as_ref().map_or(0, |(_, _, t)| *t);
    let snapshot_index = state.snapshot.as_ref().map_or(0, |(_, i, _)| *i);
    let highest_known_term = state
        .log
        .iter()
        .map(|e| e.term)
        .chain([old_term, snapshot_term])
        .max()
        .unwrap_or(0);
    let tail_entries = state.log.len() as u64;
    // Entropy is irrelevant here (no timer ever runs on this core): a
    // constant keeps the planner free of `Env` randomness (ADR 0003).
    let core =
        RaftCore::<C, S>::recovered(node.clone(), initial_voters, state, animus_env::Nanos(0), 0);
    let old_voters = core.config();
    let old_learners = core.learners();
    if !old_voters.contains(node) {
        return Err(ForceConfigError::NotAVoter {
            node: node.clone(),
            voters: old_voters,
            learners: old_learners,
        });
    }
    let last_log_index = core.last_log_index();
    let already_single_voter = old_voters.len() == 1 && old_learners.is_empty();
    Ok(ForceNewConfigPlan {
        node: node.clone(),
        old_term,
        highest_known_term,
        new_term: highest_known_term.saturating_add(RECOVERY_TERM_JUMP),
        old_voters,
        old_learners,
        snapshot_index,
        last_log_index,
        tail_entries,
        config_entry_index: last_log_index + 1,
        already_single_voter,
    })
}

/// Read the survivor's WAL at `file` and describe the rewrite. **Read-only**:
/// unlike a node's own recovery this does not repair a torn tail on disk, so
/// it is safe to run as a dry run.
///
/// `initial_voters` is the node's static bootstrap voter list (what
/// `RaftNode::start` is given); the WAL's recorded configuration overrides
/// it exactly as at a normal restart.
///
/// # Errors
/// [`ForceConfigError`]: undecodable WAL, empty WAL, or a non-voter.
pub async fn plan<E, C, S>(
    env: &E,
    file: &str,
    initial_voters: &[NodeId],
) -> Result<ForceNewConfigPlan, ForceConfigError>
where
    E: Env,
    C: Clone + std::fmt::Debug + Serialize + DeserializeOwned,
    S: StateMachine<C> + Serialize + DeserializeOwned,
{
    let bytes = read_wal(env, file).await?;
    let (records, _valid_len) = PersistedState::<C, S>::decode_with_extent(&bytes)
        .map_err(|e: FormatError| ForceConfigError::Wal(WalRecoverError::Format(e)))?;
    plan_from_state(
        &env.node_id(),
        initial_voters,
        PersistedState::replay(records),
    )
}

/// Rewrite the survivor's WAL at `file` so it is the group's only voter.
///
/// Recovers the WAL the way a node would (cutting back a torn tail),
/// re-derives the plan and refuses with [`ForceConfigError::PlanChanged`] if
/// it differs from `expected`, copies the WAL to
/// `"{file}.pre-force-new-config.{new_term}"`, then appends the term bump and
/// the configuration entry in **one append followed by an fsync**. A crash
/// before the fsync leaves a torn tail that the next recovery cuts off (and
/// re-running is then the same operation); a crash after it leaves the
/// finished state, where re-running reports `already_single_voter`.
///
/// Must only be called while no process has the directory open.
///
/// # Errors
/// [`ForceConfigError`].
pub async fn apply<E, C, S>(
    env: &E,
    file: &str,
    initial_voters: &[NodeId],
    expected: &ForceNewConfigPlan,
    _ack: DataLossAcknowledged,
) -> Result<ForceNewConfigOutcome, ForceConfigError>
where
    E: Env,
    C: Clone + std::fmt::Debug + Serialize + DeserializeOwned,
    S: StateMachine<C> + Serialize + DeserializeOwned,
{
    let records = PersistedState::<C, S>::recover(env, file)
        .await
        .map_err(ForceConfigError::Wal)?;
    let node = env.node_id();
    let fresh = plan_from_state(
        &node,
        initial_voters,
        PersistedState::<C, S>::replay(records),
    )?;
    if &fresh != expected {
        return Err(ForceConfigError::PlanChanged);
    }
    if fresh.already_single_voter {
        return Ok(ForceNewConfigOutcome {
            plan: fresh,
            backup_file: None,
        });
    }

    let original = read_wal(env, file).await?;
    let backup = format!("{file}.pre-force-new-config.{}", fresh.new_term);
    env.replace(&backup, &original).await?;

    let mut config = BTreeSet::new();
    config.insert(node.clone());
    let hard: WalRecord<C, S> = WalRecord::Hard {
        term: fresh.new_term,
        voted_for: None,
    };
    let entry: WalRecord<C, S> = WalRecord::Append(LogEntry {
        term: fresh.new_term,
        index: fresh.config_entry_index,
        command: S::noop(),
        config: Some(config),
        learners: Some(BTreeSet::new()),
    });
    let mut bytes = PersistedState::<C, S>::encode_record(&hard);
    bytes.extend(PersistedState::<C, S>::encode_record(&entry));
    env.append(file, &bytes).await?;
    env.sync(file).await?;

    Ok(ForceNewConfigOutcome {
        plan: fresh,
        backup_file: Some(backup),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::{MetaCommand, Metadata};
    use animus_env::nid;

    #[test]
    fn wal_file_name_is_the_one_the_node_driver_uses() {
        assert_eq!(CONTROL_WAL_FILE, "raft.wal");
    }

    fn entry(term: u64, index: u64) -> LogEntry<MetaCommand> {
        LogEntry {
            term,
            index,
            command: MetaCommand::NoOp,
            config: None,
            learners: None,
        }
    }

    fn state(term: u64, log: Vec<LogEntry<MetaCommand>>) -> PersistedState<MetaCommand, Metadata> {
        PersistedState::replay(
            std::iter::once(WalRecord::Hard {
                term,
                voted_for: None,
            })
            .chain(log.into_iter().map(WalRecord::Append)),
        )
    }

    fn voters(ids: &[u64]) -> Vec<NodeId> {
        ids.iter().copied().map(nid).collect()
    }

    #[test]
    fn empty_state_is_refused() {
        let err = plan_from_state::<MetaCommand, Metadata>(
            &nid(1),
            &voters(&[0, 1, 2]),
            PersistedState::default(),
        )
        .expect_err("a blank disk must not become a one-node cluster");
        assert!(matches!(err, ForceConfigError::EmptyState), "{err}");
    }

    #[test]
    fn non_voter_is_refused() {
        let err = plan_from_state(&nid(7), &voters(&[0, 1, 2]), state(3, vec![entry(3, 1)]))
            .expect_err("node 7 is not in the configuration");
        assert!(matches!(err, ForceConfigError::NotAVoter { .. }), "{err}");
    }

    #[test]
    fn plan_places_the_term_above_everything_the_survivor_knows() {
        // Hard-state term 4, but the log holds an entry of term 9: the new
        // term must clear the entry's term, not just the hard state's.
        let p = plan_from_state(
            &nid(1),
            &voters(&[0, 1, 2]),
            state(4, vec![entry(2, 1), entry(9, 2)]),
        )
        .expect("plan");
        assert_eq!(p.old_term, 4);
        assert_eq!(p.highest_known_term, 9);
        assert_eq!(p.new_term, 9 + RECOVERY_TERM_JUMP);
        assert_eq!(p.last_log_index, 2);
        assert_eq!(p.config_entry_index, 3);
        assert_eq!(p.tail_entries, 2);
        assert_eq!(p.old_voters.len(), 3);
        assert!(!p.already_single_voter);
    }
}
