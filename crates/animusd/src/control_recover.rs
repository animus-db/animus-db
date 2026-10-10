//! `animusd recover-control`: the offline force-new-configuration tool for a
//! control plane that permanently lost a majority of its voters (ADR 0077,
//! issue #1178).
//!
//! This module is the process boundary around [`animus_control::recover`]:
//! it resolves the node's identity and directory from the cluster config,
//! proves that no node is running on that directory, opens the directory
//! through the node's own `ProdEnv` disk seam (so `--encryption-key` works
//! exactly as it does for the node), prints what will happen and what may be
//! lost, and only with the explicit acknowledgement flag performs the
//! rewrite. The recovery logic itself is in `animus-control`.
//!
//! **"Is a node running here?"** There is no data-directory lock in `animusd`
//! (a running node holds none), so the check is the node's own **internal
//! listen address**: this tool binds it (through `ProdEnv::bind`, the same
//! call the node makes) and holds it for the whole run. A running node
//! already owns the port, the bind fails and the tool refuses; and while the
//! tool runs, a node started on the same config cannot bind and fails to
//! start. A node configured with a different address on the same directory is
//! not detected: the runbook says to stop the process first.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use animus_control::recover::{
    self, CONTROL_WAL_FILE, DataLossAcknowledged, ForceConfigError, ForceNewConfigPlan,
};
use animus_control::{MetaCommand, Metadata};
use animus_env::ProdEnv;

use crate::config::ClusterConfig;

/// One `recover-control` invocation.
#[derive(Debug, Clone)]
pub struct Request {
    /// The cluster config the node is started with (`--config`).
    pub config: ClusterConfig,
    /// This node's index in `config.nodes` (`--node`).
    pub index: usize,
    /// The node's data directory (`--dir`): the directory the node's
    /// `internal/` environment lives under.
    pub dir: PathBuf,
    /// `--acknowledge-data-loss`: without it the tool only prints the plan.
    pub acknowledge_data_loss: bool,
}

/// Why `recover-control` refused or failed.
#[derive(Debug)]
pub enum Error {
    /// `--node` is out of range.
    NoSuchNode(usize),
    /// The node does not run the control role: it has no control WAL.
    NotAControlNode(usize),
    /// There is no control WAL at the expected path; nothing is created.
    NoWal(PathBuf),
    /// The node's internal address is already bound: a node (or something
    /// else) is running there. Stop it first.
    NodeRunning { addr: SocketAddr, error: String },
    /// The encryption key file could not be loaded.
    Key(String),
    /// Opening the node's directory failed for another reason.
    Open(String),
    /// The recovery refused or failed.
    Recover(ForceConfigError),
    /// Dry run: the plan was printed (the string) and nothing was written.
    NotAcknowledged(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoSuchNode(i) => write!(f, "--node {i} is not in the config"),
            Self::NotAControlNode(i) => write!(
                f,
                "node {i} does not run the control role, so it has no control-plane WAL to recover"
            ),
            Self::NoWal(p) => write!(
                f,
                "no control-plane WAL at {} (wrong --dir? a node that never ran, or a wiped one, cannot be a survivor); nothing was created",
                p.display()
            ),
            Self::NodeRunning { addr, error } => write!(
                f,
                "cannot bind this node's internal address {addr} ({error}): a node appears to be running on it. Stop the animusd process first, then re-run"
            ),
            Self::Key(e) => write!(f, "loading the encryption key: {e}"),
            Self::Open(e) => write!(f, "opening the node directory: {e}"),
            Self::Recover(e) => write!(f, "{e}"),
            Self::NotAcknowledged(plan) => write!(
                f,
                "{plan}\nNothing was changed. To perform this recovery re-run the same command with --acknowledge-data-loss"
            ),
        }
    }
}

impl std::error::Error for Error {}

/// The WAL path the tool expects for a node directory (`internal/raft.wal`:
/// `Node::bind` roots the node's `ProdEnv` at `dir/internal`).
#[must_use]
pub fn wal_path(dir: &Path) -> PathBuf {
    dir.join("internal").join(CONTROL_WAL_FILE)
}

/// The human-readable plan: what will be done and what may be lost.
#[must_use]
pub fn describe(plan: &ForceNewConfigPlan, dir: &Path) -> String {
    let mut s = String::new();
    s.push_str(&format!(
        "FORCE NEW CONFIGURATION of the control plane, from survivor {} ({})\n",
        plan.node,
        dir.display()
    ));
    s.push_str(&format!(
        "  recorded voters:  {:?}{}\n",
        plan.old_voters,
        if plan.old_learners.is_empty() {
            String::new()
        } else {
            format!("  (learners {:?})", plan.old_learners)
        }
    ));
    s.push_str(&format!(
        "  this node's log:  snapshot through index {}, {} further entr{} (last index {})\n",
        plan.snapshot_index,
        plan.tail_entries,
        if plan.tail_entries == 1 { "y" } else { "ies" },
        plan.last_log_index
    ));
    if plan.already_single_voter {
        s.push_str("  this WAL already names this node as the only voter: nothing to do.\n");
        return s;
    }
    s.push_str(&format!(
        "  will do:          term {} -> {}; append one configuration entry (index {}) naming {} as the only voter\n",
        plan.old_term, plan.new_term, plan.config_entry_index, plan.node
    ));
    s.push_str("  keeps:            everything in this node's WAL and system keyspace, INCLUDING entries it holds but never saw committed (they will be treated as committed)\n");
    s.push_str("  LOSES:            any write the old group acknowledged that this node never received; the tool cannot tell which writes those are\n");
    s.push_str("  then you must:    wipe the data directory of EVERY other old voter before it runs again (a stale voter that is restarted on its old disk keeps state the recovered group does not have and must never be re-admitted), then re-add them with control-add\n");
    s
}

/// Run the recovery. `Ok` is the report to print; every refusal is an
/// [`Error`] and writes nothing.
///
/// # Errors
/// See [`Error`].
pub async fn run(req: &Request) -> Result<String, Error> {
    let addrs = req
        .config
        .nodes
        .get(req.index)
        .ok_or(Error::NoSuchNode(req.index))?;
    if !addrs.role.has_control() {
        return Err(Error::NotAControlNode(req.index));
    }
    // Refuse before touching anything: no directory is created for a node
    // that has no WAL.
    let wal = wal_path(&req.dir);
    if !wal.is_file() {
        return Err(Error::NoWal(wal));
    }
    let key = addrs
        .encryption_key_path
        .as_deref()
        .map(|p| animus_env::EncryptionKey::load_from_file(Path::new(p)))
        .transpose()
        .map_err(|e| Error::Key(e.to_string()))?;
    // The running-node guard: bind the node's own internal address and hold
    // it until the tool is done.
    let (env, _) = ProdEnv::bind_with_tls_and_key(
        addrs.id.clone(),
        addrs.internal,
        req.dir.join("internal"),
        None,
        key,
    )
    .await
    .map_err(|e| match e.kind() {
        std::io::ErrorKind::AddrInUse | std::io::ErrorKind::PermissionDenied => {
            Error::NodeRunning {
                addr: addrs.internal,
                error: e.to_string(),
            }
        }
        _ => Error::Open(e.to_string()),
    })?;

    let voters = req.config.control_ids();
    let result = async {
        let plan = recover::plan::<_, MetaCommand, Metadata>(&env, CONTROL_WAL_FILE, &voters)
            .await
            .map_err(Error::Recover)?;
        let mut report = describe(&plan, &req.dir);
        if plan.already_single_voter {
            return Ok(report);
        }
        if !req.acknowledge_data_loss {
            return Err(Error::NotAcknowledged(report));
        }
        let outcome = recover::apply::<_, MetaCommand, Metadata>(
            &env,
            CONTROL_WAL_FILE,
            &voters,
            &plan,
            DataLossAcknowledged::acknowledging_that_unseen_writes_are_lost_and_other_voters_must_be_wiped(),
        )
        .await
        .map_err(Error::Recover)?;
        report.push_str(&format!(
            "DONE. The previous WAL is kept as {}.\nStart this node with its normal command; it elects itself, then wipe the other old voters and re-add them (docs/runbook/control-plane-quorum-loss.md).\n",
            outcome.backup_file.as_deref().unwrap_or("(none)")
        ));
        Ok(report)
    }
    .await;
    env.shutdown_and_wait().await;
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use animus_control::persist::PersistedState;
    use animus_control::{LogEntry, WalRecord};
    use animus_env::nid;
    use std::collections::BTreeSet;

    fn config_on(internal: SocketAddr, nodes: usize) -> ClusterConfig {
        let mut cfg = ClusterConfig::generate(nodes, "127.0.0.1".parse().unwrap(), 40_000);
        cfg.nodes[0].internal = internal;
        cfg
    }

    /// A WAL whose recorded configuration is `voters` (an entry carrying it),
    /// as a node that ran for a while would have.
    fn write_wal(dir: &Path, voters: &[u64]) {
        let cfg: BTreeSet<_> = voters.iter().copied().map(nid).collect();
        let records: Vec<WalRecord> = vec![
            WalRecord::Hard {
                term: 3,
                voted_for: None,
            },
            WalRecord::Append(LogEntry {
                term: 3,
                index: 1,
                command: MetaCommand::NoOp,
                config: Some(cfg),
                learners: Some(BTreeSet::new()),
            }),
            WalRecord::Append(LogEntry {
                term: 3,
                index: 2,
                command: MetaCommand::NoOp,
                config: None,
                learners: None,
            }),
        ];
        let mut bytes = Vec::new();
        for r in &records {
            bytes.extend(PersistedState::encode_record(r));
        }
        std::fs::create_dir_all(dir.join("internal")).unwrap();
        std::fs::write(wal_path(dir), bytes).unwrap();
    }

    fn free_addr() -> SocketAddr {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    }

    fn request(dir: &Path, ack: bool, voters: usize) -> Request {
        Request {
            config: config_on(free_addr(), voters),
            index: 0,
            dir: dir.to_path_buf(),
            acknowledge_data_loss: ack,
        }
    }

    #[tokio::test]
    async fn without_the_acknowledgement_it_prints_the_plan_and_writes_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        write_wal(tmp.path(), &[0, 1, 2]);
        let before = std::fs::read(wal_path(tmp.path())).unwrap();
        let err = run(&request(tmp.path(), false, 3)).await.unwrap_err();
        let Error::NotAcknowledged(plan) = &err else {
            panic!("expected NotAcknowledged, got {err}");
        };
        assert!(plan.contains("LOSES"), "{plan}");
        assert!(
            plan.contains("wipe the data directory of EVERY other old voter"),
            "{plan}"
        );
        assert!(err.to_string().contains("--acknowledge-data-loss"));
        assert_eq!(std::fs::read(wal_path(tmp.path())).unwrap(), before);
        let names: Vec<_> = std::fs::read_dir(tmp.path().join("internal"))
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .filter(|n| n.starts_with("raft.wal"))
            .collect();
        assert_eq!(names, vec!["raft.wal".to_owned()], "no backup on a dry run");
    }

    #[tokio::test]
    async fn with_the_acknowledgement_it_rewrites_the_wal_to_a_single_voter() {
        let tmp = tempfile::tempdir().unwrap();
        write_wal(tmp.path(), &[0, 1, 2]);
        let before = std::fs::read(wal_path(tmp.path())).unwrap();
        let report = run(&request(tmp.path(), true, 3)).await.unwrap();
        assert!(report.contains("DONE"), "{report}");
        let after = std::fs::read(wal_path(tmp.path())).unwrap();
        assert!(after.starts_with(&before) && after.len() > before.len());
        let state = PersistedState::<MetaCommand, Metadata>::replay(
            PersistedState::<MetaCommand, Metadata>::decode(&after).unwrap(),
        );
        let last = state.log.last().unwrap();
        assert_eq!(last.config, Some([nid(0)].into_iter().collect()));
        assert_eq!(last.index, 3);
        assert!(state.term > 3);
        // The backup is the pre-rewrite WAL.
        let backup = std::fs::read_dir(tmp.path().join("internal"))
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| {
                p.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .contains("pre-force-new-config")
            })
            .expect("backup file");
        assert_eq!(std::fs::read(backup).unwrap(), before);
        // A repeat run is a no-op.
        let again = run(&request(tmp.path(), true, 3)).await.unwrap();
        assert!(again.contains("nothing to do"), "{again}");
        assert_eq!(std::fs::read(wal_path(tmp.path())).unwrap(), after);
    }

    #[tokio::test]
    async fn a_running_node_on_the_internal_address_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        write_wal(tmp.path(), &[0, 1, 2]);
        let before = std::fs::read(wal_path(tmp.path())).unwrap();
        // Something (a node) holds the internal address.
        let held = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let mut req = request(tmp.path(), true, 3);
        req.config.nodes[0].internal = held.local_addr().unwrap();
        let err = run(&req).await.unwrap_err();
        assert!(matches!(err, Error::NodeRunning { .. }), "{err}");
        assert!(err.to_string().contains("Stop the animusd process"));
        assert_eq!(std::fs::read(wal_path(tmp.path())).unwrap(), before);
    }

    #[tokio::test]
    async fn a_missing_wal_is_refused_and_nothing_is_created() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("never-ran");
        let err = run(&request(&dir, true, 3)).await.unwrap_err();
        assert!(matches!(err, Error::NoWal(_)), "{err}");
        assert!(!dir.exists(), "the refusal must not create the directory");
    }

    #[tokio::test]
    async fn a_node_that_is_not_a_voter_in_its_own_wal_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        // The WAL's recorded configuration no longer names node 0.
        write_wal(tmp.path(), &[1, 2]);
        let before = std::fs::read(wal_path(tmp.path())).unwrap();
        let err = run(&request(tmp.path(), true, 3)).await.unwrap_err();
        assert!(
            matches!(err, Error::Recover(ForceConfigError::NotAVoter { .. })),
            "{err}"
        );
        assert_eq!(std::fs::read(wal_path(tmp.path())).unwrap(), before);
    }

    #[tokio::test]
    async fn a_data_only_node_and_a_bad_index_are_refused() {
        let tmp = tempfile::tempdir().unwrap();
        write_wal(tmp.path(), &[0, 1, 2]);
        let mut req = request(tmp.path(), true, 3);
        req.index = 9;
        assert!(matches!(run(&req).await.unwrap_err(), Error::NoSuchNode(9)));
        let mut split = Request {
            config: ClusterConfig::generate_split(1, 2, "127.0.0.1".parse().unwrap(), 41_000),
            index: 1,
            dir: tmp.path().to_path_buf(),
            acknowledge_data_loss: true,
        };
        split.config.nodes[1].internal = free_addr();
        assert!(matches!(
            run(&split).await.unwrap_err(),
            Error::NotAControlNode(1)
        ));
    }
}
