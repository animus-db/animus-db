//! AnimusDB node server (`animusd`).
//!
//! Eight modes:
//!
//! ```text
//! animusd gen-config --nodes N [--host H] [--base-port P]   # print a combined-mode cluster config (JSON)
//! animusd gen-config --control-nodes N --data-nodes M [--host H] [--base-port P] # print a split-deployment config (ADR 0035)
//! animusd --config FILE --node I [--dir DIR] [--ephemeral] [--orphan-sweep-after SECS] [--stream-seal-bytes B] [--stream-seal-age SECS] [--stream-retention SECS] [--segment-store dir:PATH] [--backup-store cluster|fs:PATH] [--quiesce-after SECS] [--throttle-read-units N] [--throttle-write-units N] [--dynamo-auth PATH] # run node I of a cluster (one process)
//! animusd --cluster N [--dir DIR] [--ip ADDR] [--ephemeral] [--auto-split-bytes B] [--auto-split-change-rate RATE] [--auto-split-ops-rate RATE] [--orphan-sweep-after SECS] [--stream-seal-bytes B] [--stream-seal-age SECS] [--stream-retention SECS] [--segment-store dir:PATH] [--backup-store cluster|fs:PATH] [--quiesce-after SECS] [--throttle-read-units N] [--throttle-write-units N] [--dynamo-auth PATH] # run an N-node cluster in one process
//! animusd --cluster-control N --cluster-data M [--dir DIR] [--ip ADDR] [--ephemeral] [--auto-split-bytes B] [--auto-split-change-rate RATE] [--auto-split-ops-rate RATE] [--orphan-sweep-after SECS] [--dynamo-auth PATH] # run a whole split deployment in one process (ADR 0035)
//! animusd join --seed ADDR[,ADDR...] [--id NAME] --base-port P [--dir D] [--ephemeral] # seed/join startup (ADR 0032 PR2; ADR 0040 PR4 self-minting if --id is omitted)
//! animusd control --config FILE --node I [--dir DIR] [--ephemeral] [--orphan-sweep-after SECS] [--segment-store dir:PATH] [--backup-store cluster|fs:PATH] # run node I as a control-only node (ADR 0035 PR3)
//! animusd data --config FILE --node I [--dir DIR] [--ephemeral] [--dynamo-auth PATH] # run node I as a data-only node (ADR 0035 PR4)
//! animusd data --seed ADDR[,ADDR...] [--id NAME] --base-port P [--dir D] [--ephemeral] [--dynamo-auth PATH] # data-only seed/join (ADR 0035 PR5; ADR 0040 PR4 self-minting if --id is omitted)
//! ```
//!
//! The data replica is durable by default (an on-disk LSM under the node's data
//! dir, so values survive a restart); `--ephemeral` selects a volatile
//! in-memory engine instead.
//!
//! Per-process deployment: generate a config once, copy it to each host, and run
//! `animusd --config cluster.json --node I` with a distinct `I` per process. A
//! node that has no expanded config at all — just the **intra-cluster**
//! address (ADR 0047; not the client one — joining is a cluster-membership
//! action, so the honest seed address is the one an operator's Kubernetes
//! Service topology would keep internal) of any already-running node — can
//! instead `animusd join --seed <that address> --id NAME`, learning
//! everything else it needs from the cluster itself (ADR 0032 PR2: a real
//! data-plane member, control group unchanged, ADR 0030); a data-only node
//! has the same option (`animusd data --seed <that address> --id NAME`, ADR
//! 0035 PR5) against a separately-deployed control plane.
//! **`--node I` is gone from `join`/`data --seed` — a clean break (ADR 0040
//! PR4)**: `--id NAME` proposes a durable identity (validated,
//! `NodeId::propose`) instead of an operator-picked index; omit it (`--base-
//! port P` is then **required**, since there is no index to derive a default
//! port range from) to have this node **self-mint** its own id (ADR 0040
//! Decision B) — the control plane's registration compare-and-swap
//! (`MetaCommand::RegisterNode`, Decision C) makes uniqueness structural
//! rather than a matter of an operator not picking a duplicate index, closing
//! the residual race two simultaneous `--node`-indexed joiners could hit
//! under the old scheme. A self-minted join is **ephemeral-identity**: a
//! restart with a fresh dir mints a *new* id, unlike `--id NAME`'s durable,
//! restart-stable identity; see `crates/animusd/CLAUDE.md` and ADR 0040.
//!
//! `animusd control` runs one of a config's control-role node(s) only — no
//! storage engine, no `raftkv` env, no DynamoDB listener; `animusd data`
//! runs one of a config's data-role node(s) only — no control env, no local
//! control `RaftCore` at all, reaching a **separately-deployed** control
//! plane over the network instead (ADR 0035: control plane as a separate
//! deployment). `gen-config --control-nodes/--data-nodes` prints the
//! split-deployment config *shape* (control-role entries with no `raftkv`
//! address, data-role entries with no `control` address) both target — run
//! every control-role index with `animusd control` and every data-role index
//! with `animusd data` against the same config for a genuine split
//! deployment (see `crates/animusd/CLAUDE.md`). A mixed deployment — some
//! nodes still combined-mode (`--config FILE --node I` against a `Both`-role
//! config) — keeps working exactly as before.
//!
//! `--cluster-control N --cluster-data M` is `--cluster N`'s split-deployment
//! sibling: a whole split deployment (`N` control-only + `M` data-only
//! nodes, no combined-mode node) in **one process**, dev-convenience the same
//! way `--cluster N` is — the real multi-process shape is `animusd control`/
//! `animusd data` above. Each node still gets its own edge state and reaches
//! every other node only through real forwarding/relay/mirror paths, exactly
//! as the multi-process deployment does.
//!
//! `--orphan-sweep-after SECS` (ADR 0040 PR6) tunes the control-plane
//! leader's own volatile auto-reclaim sweep of node-identity claims that
//! never activated (a crash-mid-join, or the losing racer of two concurrent
//! omitted-id registrations) — default 600s (10 minutes) if omitted, `0`
//! disables it outright. Only meaningful on a mode that runs a local control
//! `RaftNode` (every mode above except `data`, which has none); a plain
//! `animusd data --config`/`--seed` has no orphan-sweep knob of its own since
//! it never runs one.
//!
//! `--quiesce-after SECS` (ADR 0044 phase-1 PR7) opts every **data-plane** CP
//! group into quiescence: once a group's leader has had no local activity
//! for this long (and every other entry-predicate clause holds — see
//! `animus-control::RaftCore::quiesce_entry_ok`'s own doc), it stops ticking
//! (no more Raft timers/heartbeats) until a client write, a peer message, or
//! the reconciler's proactive wake (a replica set member marked `Down`)
//! touches it again. **Defaults ON at `main::DEFAULT_QUIESCE_AFTER_SECS`
//! (5s)** — see that constant's own doc for the evidence behind this default
//! and how to override it; `0` disables the feature entirely. The CLI flag
//! itself threads through `--config`/`--node` and `--cluster N` only; a
//! documented gap for a follow-up remains on two other shapes: passed to
//! `--cluster-control`/`--cluster-data` it parses but is silently unused
//! (that path has no growth-combination wrapper to receive it yet); the
//! standalone `control`/`join` subcommands reject it outright as an unknown
//! argument (each parses its own flag set independently of `run`'s), and
//! neither runs a data-plane role that would use it anyway. **`animusd data
//! --config FILE`, however, now reaches this knob too (S-06)** — not via a
//! CLI flag of its own, but through the same `cluster_settings.
//! quiesce_after_secs` config-file section `--config`/`--node` reads (see
//! that flag's `run_single`/`run_data_config` doc and `animusd::config::
//! ClusterSettings`'s own doc for the full per-field applicability
//! breakdown, since a control-only node ignores this field entirely).
//! **A nonzero value below `animusd::MIN_QUIESCE_AFTER` (200ms, the
//! change-consumer sweep interval — issue #302 fix) is rejected at parse
//! time**, since it can reopen the stale-veto quiescence race the fix
//! closes; see that constant's own doc.
//!
//! **`cluster_settings` (S-06)**: a `ClusterConfig` file (`--config FILE`)
//! may also carry a `cluster_settings` section — the same auto-split/
//! quiesce/orphan-sweep/stream-seal knobs the CLI flags above express,
//! reachable now on every real deployment shape (`--config`/`--node`,
//! `animusd control`, `animusd data --config`), not just `--cluster N`'s
//! dev-only in-process mode. A CLI flag **and** the config file's own
//! section setting the identical field is a hard startup error (the same
//! "specify it one way, not both" contract `--dynamo-auth` already uses),
//! checked field by field — see `animusd::config::ClusterSettings`'s own
//! doc for the full field list, units, and which fields apply to which
//! role.
//!
//! `ClientCtx::trigger_split` always proposes the ADR 0058 single-entry
//! atomic in-place fork now — the original ADR 0050 copy-based
//! build/freeze/cutover workflow and its `--split-mode` flag were deleted in
//! the copy-split-deletion endgame's Layer B1 (`docs/adr/0058-*.md` rung 4).
//!
//! `--dynamo-auth PATH` (ADR 0057) points at a JSON file holding the client
//! DynamoDB port's SigV4 credential store — the same shape as a
//! `ClusterConfig`'s own `dynamo_auth` section: `{"credentials":
//! {"AKID...": "secret...", ...}}`. Absent (the default), auth is
//! **disabled** on the dynamo port — byte-identical to pre-ADR-0057
//! behavior, and every existing deployment/test/quick-start keeps working
//! unmodified. It is the only way to configure credentials on a mode with no
//! config file (`--cluster N`, `--cluster-control`/`--cluster-data`, `data
//! --seed`); `--config FILE`/`data --config FILE` can instead (or also, as
//! long as only one source supplies it) carry a `dynamo_auth` section
//! directly in the config file. Supplying credentials **both** ways — a
//! config file whose own `dynamo_auth` section is present, and this flag —
//! is a hard startup error, never a silent precedence rule. Not accepted by
//! `join`/`control` (a control-only node never binds the dynamo listener) or
//! `data --config` without also specifying `--config` (see the mode's own
//! flag list above). When set, every request on the dynamo port — item API
//! and Streams alike — must carry a valid `Authorization: AWS4-HMAC-SHA256
//! ...` header (`GET /metrics` stays unauthenticated, matching ADR 0057).
//!
//! `--tls-cert PATH --tls-key PATH --tls-ca PATH` (ADR 0064, S-01 commit 2)
//! give this **one** process its own TLS material — all three or none
//! (mutual TLS on the internal wire always needs a CA to verify peers
//! against). Absent (the default), every listener/dialer stays plain TCP,
//! byte-identical to before this ADR. Accepted on `--config`/`--node`
//! (combined mode) and `data --config`/`data --seed` — applied to this
//! process's own node entry (`--config` modes: a hard startup error if the
//! config file's own `nodes[index].tls` section is *also* present, the same
//! "specify it one way, not both" contract `--dynamo-auth` uses; `data
//! --seed`: set directly, no config file to conflict with). **Not yet
//! accepted by `--cluster N`/`--cluster-control`/`--cluster-data`/`join`/
//! `control`** — an in-process dev cluster has no per-node config entries
//! for the flag to attach to, and `join`/`control` mirror `--dynamo-auth`'s
//! own non-acceptance here; a config file whose own node entries each carry
//! a `tls` section is the way to run any of these modes under TLS today.
//! Every port a node binds gets **mutual** TLS on `internal`/`intra`,
//! **server-only** TLS on `client`/`dynamo`/`admin`/`console` (ADR 0064
//! Decision 1/2) — see `crates/animusd/CLAUDE.md`'s TLS section for the
//! full per-port table. **A cluster is either all-TLS or all-plain on the
//! internal wire** — `ClusterConfig::from_json`/`validate_tls` rejects a
//! config file whose own per-node `tls` sections disagree; that check does
//! not (and cannot) see across separate processes' own `--tls-*` flags, so
//! mixing "some processes pass the flag, others don't" against one shared,
//! TLS-less config file is a real, only-at-runtime-visible misconfiguration
//! (a failed handshake on first cross-node contact), not one this CLI can
//! catch at any single process's startup.
//!
//! `--backup-store cluster|fs:PATH` (ADR 0059 §1) selects the on-demand
//! backup subsystem's own `SegmentStore` handle — a second, independently
//! configured store alongside `--segment-store`'s streams one, sharing the
//! same two backends (`cluster`, the default K-replicated
//! `ClusterSegmentStore`; `fs:PATH`, a bare single-directory
//! `FsSegmentStore`) but never the same object namespace
//! (`animus_cp_data::backup`'s `backup/{backup_id}/...` ids). Consumed by
//! the per-tablet capture driver (`backup_capture.rs`), the completion
//! aggregator (`backup_completion.rs`), the backup/PITR janitors, and
//! restore (ADR 0059 Trains 1–3, all implemented). Threads through
//! `--config`/`--node`, `--cluster N`, **and (W-10) `animusd control`** —
//! `--cluster-control`/`--cluster-data` and the standalone `data`/`join`
//! subcommands remain a documented gap (each always gets the default
//! `Cluster` store, since neither parses this flag). **A control-only node
//! now provisions a real backup store just like a combined or data-only
//! one (W-10, ADR 0043 §A9's control-only-leader gap — closed)** — its
//! backup/PITR janitors can physically reclaim objects for as long as it
//! leads, not just mark rows. **Unlike `--segment-store dir:PATH`,
//! `--backup-store` also accepts the literal keyword `cluster`** (spelled
//! out because ADR 0059 §1 states the knob as `cluster|fs:PATH` rather than
//! "omit for cluster") — omitting the flag and passing `--backup-store
//! cluster` are equivalent.

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::process::ExitCode;
use std::time::Duration;

use animus_env::NodeId;
use animusd::config::TlsSection;
use animusd::{ClusterConfig, RoleAddrs};

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let tracer_provider = animusd::otel::init_tracing(&otel_instance_label(&args));

    let result = match args.first().map(String::as_str) {
        Some("gen-config") => gen_config(&args[1..]),
        Some("join") => run_join(&args[1..]).await,
        Some("control") => run_control(&args[1..]).await,
        Some("data") => run_data(&args[1..]).await,
        _ => run(&args).await,
    };

    // Flush any spans still buffered in the OTLP batch exporter (ADR 0027)
    // before the process exits; a no-op if export isn't configured.
    if let Some(provider) = tracer_provider
        && let Err(err) = provider.shutdown()
    {
        tracing::warn!(%err, "failed to flush OpenTelemetry tracer provider on exit");
    }

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(msg) => {
            eprintln!("animusd: {msg}\n\n{USAGE}");
            ExitCode::FAILURE
        }
    }
}

/// A short `service.instance.id` label for the OTLP `Resource` (ADR 0027):
/// this process's node index for a `--config/--node` run (`control`/`data`
/// share the same `--node I` shape) — or a cluster-level label for a
/// `--cluster N` run (which hosts several logical nodes in one process, so
/// no single node id applies at the process/resource level — per-span
/// `node_id` fields still distinguish them within a trace). `join`/`data
/// --seed` have no `--node` index at all since ADR 0040 PR4 (an explicit
/// `--id` or a self-mint); they fall through to the generic `"animusd"`
/// label below — per-span `node_id` fields still distinguish them once the
/// real id is known, same as the `--cluster N` case.
fn otel_instance_label(args: &[String]) -> String {
    if let Some(pos) = args.iter().position(|a| a == "--node")
        && let Some(index) = args.get(pos + 1)
    {
        return format!("node-{index}");
    }
    if args.iter().any(|a| a == "--cluster") {
        return "cluster".to_owned();
    }
    "animusd".to_owned()
}

const USAGE: &str = "usage:\n  \
    animusd gen-config --nodes N [--host H] [--base-port P]\n  \
    animusd gen-config --control-nodes N --data-nodes M [--host H] [--base-port P]\n  \
    animusd --config FILE --node I [--dir DIR] [--ephemeral] [--orphan-sweep-after SECS] [--stream-seal-bytes B] [--stream-seal-age SECS] [--stream-retention SECS] [--segment-store dir:PATH] [--backup-store cluster|fs:PATH] [--quiesce-after SECS] [--throttle-read-units N] [--throttle-write-units N] [--dynamo-auth PATH] [--tls-cert PATH --tls-key PATH --tls-ca PATH]\n  \
    animusd --cluster N [--dir DIR] [--ip ADDR] [--ephemeral] [--auto-split-bytes B] [--auto-split-change-rate RATE] [--auto-split-ops-rate RATE] [--orphan-sweep-after SECS] [--stream-seal-bytes B] [--stream-seal-age SECS] [--stream-retention SECS] [--segment-store dir:PATH] [--backup-store cluster|fs:PATH] [--quiesce-after SECS] [--throttle-read-units N] [--throttle-write-units N] [--dynamo-auth PATH]\n  \
    animusd --cluster-control N --cluster-data M [--dir DIR] [--ip ADDR] [--ephemeral] [--auto-split-bytes B] [--auto-split-change-rate RATE] [--auto-split-ops-rate RATE] [--orphan-sweep-after SECS] [--dynamo-auth PATH]\n  \
    animusd join --seed ADDR[,ADDR...] [--id NAME] --base-port P [--ip A] [--dir D] [--ephemeral]\n  \
    animusd control --config FILE --node I [--dir DIR] [--ephemeral] [--orphan-sweep-after SECS] [--segment-store dir:PATH] [--backup-store cluster|fs:PATH]\n  \
    animusd data --config FILE --node I [--dir DIR] [--ephemeral] [--dynamo-auth PATH] [--tls-cert PATH --tls-key PATH --tls-ca PATH]\n  \
    animusd data --seed ADDR[,ADDR...] [--id NAME] --base-port P [--ip A] [--dir D] [--ephemeral] [--dynamo-auth PATH] [--tls-cert PATH --tls-key PATH --tls-ca PATH]";

/// `gen-config`: print a generated cluster config as JSON — either combined-mode
/// (`--nodes N`) or the ADR 0035 split-deployment shape (`--control-nodes N
/// --data-nodes M`, [`ClusterConfig::generate_split`]).
fn gen_config(args: &[String]) -> Result<(), String> {
    let mut nodes: Option<usize> = None;
    let mut control_nodes: Option<usize> = None;
    let mut data_nodes: Option<usize> = None;
    let mut host: IpAddr = "127.0.0.1".parse().unwrap();
    let mut base_port: u16 = 7100;
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--nodes" => nodes = Some(parse_next(&mut it, "--nodes")?),
            "--control-nodes" => control_nodes = Some(parse_next(&mut it, "--control-nodes")?),
            "--data-nodes" => data_nodes = Some(parse_next(&mut it, "--data-nodes")?),
            "--host" => host = parse_next(&mut it, "--host")?,
            "--base-port" => base_port = parse_next(&mut it, "--base-port")?,
            other => return Err(format!("unknown gen-config argument `{other}`")),
        }
    }
    if nodes.is_some() && (control_nodes.is_some() || data_nodes.is_some()) {
        return Err("use either --nodes or --control-nodes/--data-nodes, not both".into());
    }
    if let Some(nodes) = nodes {
        if nodes == 0 {
            return Err("--nodes must be at least 1".into());
        }
        println!(
            "{}",
            ClusterConfig::generate(nodes, host, base_port).to_json()
        );
        return Ok(());
    }
    let control_n =
        control_nodes.ok_or("gen-config needs --nodes N, or --control-nodes N --data-nodes M")?;
    let data_n = data_nodes.ok_or("--control-nodes also needs --data-nodes M")?;
    if control_n == 0 || data_n == 0 {
        return Err("--control-nodes and --data-nodes must each be at least 1".into());
    }
    println!(
        "{}",
        ClusterConfig::generate_split(control_n, data_n, host, base_port).to_json()
    );
    Ok(())
}

/// `--config/--node` (per-process) or `--cluster` (in-process) run modes.
async fn run(args: &[String]) -> Result<(), String> {
    let mut config_path: Option<String> = None;
    let mut node: Option<usize> = None;
    let mut cluster: Option<usize> = None;
    let mut cluster_control: Option<usize> = None;
    let mut cluster_data: Option<usize> = None;
    let mut dir: Option<std::path::PathBuf> = None;
    let mut ip: IpAddr = "127.0.0.1".parse().unwrap();
    // Data replica engine: durable on-disk LSM by default; `--ephemeral` selects
    // the volatile in-memory engine (data does not survive restart).
    let mut backend = animusd::StorageBackend::default();
    // `--auto-split-bytes B` (ADR 0034): in `--cluster` mode, a CP-hosting
    // node auto-splits a tablet it leads once its (approximate) scoped
    // bytes exceed B (Phase 2.4) — the metric splitting is meant to bound
    // in production (snapshot/compaction/replica-move/recovery cost scales
    // with bytes). Handy for testing sharding by bulk-seeding past the
    // threshold. **The former `--auto-split K` key-count trigger was
    // removed** (bytes and, for streamed tables, `--auto-split-change-rate`
    // below cover its use cases — see the root `CLAUDE.md`'s auto-split
    // entry). This CLI flag is `--cluster N`'s only route to the knob;
    // `--config`/`--node` (real deployments) reaches it via a config file's
    // `cluster_settings.auto_split_bytes` section instead (S-06) — see this
    // file's own module doc.
    let mut auto_split_bytes: Option<u64> = None;
    // `--auto-split-change-rate RATE` (ADR 0042 §14, growth PR3 Fork F):
    // opt-in — a **streamed** led tablet whose own smoothed change-append
    // rate (bytes/sec, `/admin/metrics`'s `stream_change_rates`) sustains
    // above `RATE` triggers the same split path. Absent means disabled
    // (zero behavior change); an unstreamed table is never subject to it
    // regardless. No production-tuned default exists yet (no operational
    // data) — this flag has no default-on behavior; pick `RATE` per
    // workload.
    let mut auto_split_change_rate: Option<u64> = None;
    // `--auto-split-ops-rate RATE` (W-09, ADR 0034's own deferred bullet,
    // closed): opt-in — a led tablet whose own smoothed **write** request
    // rate (ops/sec, `/admin/metrics`'s `request_rates`) sustains above
    // `RATE` triggers the same split path. Unlike `auto_split_change_rate`,
    // this applies to **any** table, not just a streamed one — it closes
    // the "hot-but-small tablet under heavy write load never splits" gap
    // for the general case. Absent means disabled (zero behavior change);
    // no production-tuned default exists yet — pick `RATE` per workload.
    let mut auto_split_ops_rate: Option<u64> = None;
    // `--orphan-sweep-after SECS` (ADR 0040 PR6): overrides
    // `DEFAULT_ORPHAN_SWEEP_AFTER` (10 minutes) for the control-plane
    // leader's auto-reclaim sweep of never-activated members; `0` disables
    // it. Applies to every mode below that runs a local control `RaftNode`
    // (`--config`/`--node`, `--cluster`, `--cluster-control`/`--cluster-data`).
    let mut orphan_sweep_after: Option<u64> = None;
    // `--stream-seal-bytes B` / `--stream-seal-age SECS` (ADR 0042 §13): the
    // DynamoDB Streams sealer's size/age triggers — default to the ADR's own
    // production defaults (4 MiB / 4h) when omitted.
    let mut stream_seal_bytes: Option<u64> = None;
    let mut stream_seal_age_secs: Option<u64> = None;
    // `--stream-retention SECS` (ADR 0042 §13/ADR 0043 §A9, round-3 PR7):
    // the segment janitor's own retention grace period — defaults to the
    // ADR's own production default (24h) when omitted.
    let mut stream_retention_secs: Option<u64> = None;
    // `--segment-store dir:PATH` (ADR 0043 §A7b): opts out of the default
    // K-replicated `ClusterSegmentStore` into a bare, single-directory
    // `FsSegmentStore` at `PATH` — dev use, or a directory every node in the
    // cluster mounts at the identical path. See `SegmentStoreConfig`'s own
    // doc for the durability trade-off this opt-in accepts.
    let mut segment_store: Option<String> = None;
    // `--backup-store cluster|fs:PATH` (ADR 0059 §1): selects the backup
    // subsystem's own, independently-configured `SegmentStore` handle — see
    // this file's own module doc for the full knob description and
    // `animusd::BackupStoreConfig`'s doc for the durability trade-off.
    let mut backup_store: Option<String> = None;
    // `--quiesce-after SECS` (ADR 0044 phase-1 PR7): opts every data-plane CP
    // group into quiescence once it has had no local activity for this long
    // — `0` disables it entirely. Defaults ON (`DEFAULT_QUIESCE_AFTER_SECS`)
    // for every mode below that hosts a data-plane role: see this constant's
    // own doc for why (a maintainer-reviewable call, flagged there and in
    // the delivery PR body, not a settled operational fact).
    let mut quiesce_after: Option<u64> = None;
    // `--throttle-read-units N` / `--throttle-write-units N` (ADR 0065
    // §5(a), W-08 step 4): the cluster-wide default read/write
    // capacity-units budget applied to any table that has not set its own
    // `ProvisionedThroughput` — seeds `ClientCtx::throttle_defaults` at node
    // start. Absent means `PAY_PER_REQUEST` (no throttling) at this layer;
    // an individual table's own `BillingMode`/`ProvisionedThroughput`
    // (`CreateTable`/`UpdateTable`) still overrides it either way. No
    // production-tuned default exists yet — pick a value per workload.
    let mut throttle_read_units: Option<u64> = None;
    let mut throttle_write_units: Option<u64> = None;
    // `--dynamo-auth PATH` (ADR 0057): a JSON file of the same shape as a
    // `ClusterConfig`'s `dynamo_auth` section (`{"credentials": {"AKID":
    // "secret", ...}}`) — the client DynamoDB port's SigV4 credential store.
    // Absent (the default) leaves auth disabled. Needed here because
    // `--cluster`/`--cluster-control`/`--cluster-data` generate their config
    // in-process (no config **file** to carry a `dynamo_auth` section of its
    // own); `--config FILE` can also combine this flag with a config that
    // has no `dynamo_auth` section of its own, but specifying credentials
    // both ways (flag **and** the loaded file's own section) is a hard
    // startup error — see `run_single`.
    let mut dynamo_auth_path: Option<String> = None;
    // `--advertise-host NAME` (ADR 0060) — see `run_join`'s own doc.
    // `--config`/`--node`: applied to that one node's own config entry
    // (`apply_advertise_host_flag`, the same "flag and config both set it
    // is a hard error" shape `dynamo_auth_path` above uses). `--cluster N`:
    // applied to every generated node (they still each bind their own
    // distinct port, so `{host}:{port}` stays a unique identity per node).
    // `--cluster-control`/`--cluster-data`: a documented gap, matching this
    // dev-only path's existing `--quiesce-after` one — not threaded through,
    // since `run_in_process_split_cluster` has no per-node-advertise-host
    // wrapper to call.
    let mut advertise_host: Option<String> = None;
    // `--tls-cert PATH --tls-key PATH --tls-ca PATH` (ADR 0064, S-01
    // commit 2) — this node's own TLS material, applied to `config.
    // nodes[index]` (`apply_tls_flag`, the same per-node "flag and config
    // both set it is a hard error" shape as `--advertise-host`). **Only
    // `--config`/`--node` wires this through today** — `--cluster N`/
    // `--cluster-control`/`--cluster-data` (in-process dev clusters with no
    // per-node config entries to apply the flag to) reject it outright
    // rather than silently starting a plaintext cluster the operator
    // believes is TLS-enabled (a deliberate deviation from the silent-gap
    // precedent `--quiesce-after`/`--advertise-host` set on this same
    // dev-only path — TLS is security-relevant, so a wrong assumption here
    // should fail loudly, not silently downgrade).
    let mut tls_cert: Option<String> = None;
    let mut tls_key: Option<String> = None;
    let mut tls_ca: Option<String> = None;

    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--config" => config_path = Some(parse_next(&mut it, "--config")?),
            "--node" => node = Some(parse_next(&mut it, "--node")?),
            "--cluster" => cluster = Some(parse_next(&mut it, "--cluster")?),
            "--cluster-control" => {
                cluster_control = Some(parse_next(&mut it, "--cluster-control")?)
            }
            "--cluster-data" => cluster_data = Some(parse_next(&mut it, "--cluster-data")?),
            "--dir" => dir = Some(parse_next::<String>(&mut it, "--dir")?.into()),
            "--ip" => ip = parse_next(&mut it, "--ip")?,
            "--ephemeral" => backend = animusd::StorageBackend::Memory,
            "--auto-split-bytes" => {
                auto_split_bytes = Some(parse_next(&mut it, "--auto-split-bytes")?);
            }
            "--auto-split-change-rate" => {
                auto_split_change_rate = Some(parse_next(&mut it, "--auto-split-change-rate")?);
            }
            "--auto-split-ops-rate" => {
                auto_split_ops_rate = Some(parse_next(&mut it, "--auto-split-ops-rate")?);
            }
            "--orphan-sweep-after" => {
                orphan_sweep_after = Some(parse_next(&mut it, "--orphan-sweep-after")?);
            }
            "--stream-seal-bytes" => {
                stream_seal_bytes = Some(parse_next(&mut it, "--stream-seal-bytes")?);
            }
            "--stream-seal-age" => {
                stream_seal_age_secs = Some(parse_next(&mut it, "--stream-seal-age")?);
            }
            "--stream-retention" => {
                stream_retention_secs = Some(parse_next(&mut it, "--stream-retention")?);
            }
            "--segment-store" => {
                segment_store = Some(parse_next(&mut it, "--segment-store")?);
            }
            "--backup-store" => {
                backup_store = Some(parse_next(&mut it, "--backup-store")?);
            }
            "--quiesce-after" => {
                quiesce_after = Some(parse_next(&mut it, "--quiesce-after")?);
            }
            "--throttle-read-units" => {
                throttle_read_units = Some(parse_next(&mut it, "--throttle-read-units")?);
            }
            "--throttle-write-units" => {
                throttle_write_units = Some(parse_next(&mut it, "--throttle-write-units")?);
            }
            "--dynamo-auth" => {
                dynamo_auth_path = Some(parse_next(&mut it, "--dynamo-auth")?);
            }
            "--advertise-host" => {
                advertise_host = Some(parse_next::<String>(&mut it, "--advertise-host")?);
            }
            "--tls-cert" => tls_cert = Some(parse_next(&mut it, "--tls-cert")?),
            "--tls-key" => tls_key = Some(parse_next(&mut it, "--tls-key")?),
            "--tls-ca" => tls_ca = Some(parse_next(&mut it, "--tls-ca")?),
            other => return Err(format!("unknown argument `{other}`")),
        }
    }
    let tls_flag = resolve_tls_flags(tls_cert, tls_key, tls_ca)?;
    // S-06: the raw (un-defaulted) CLI values for every knob
    // `animusd::config::ClusterSettings` can also carry in a config file's
    // own `cluster_settings` section — `--config`/`--node`'s dispatch below
    // merges this against the loaded config's section (`resolve_cluster_
    // settings`, a hard error if both sides set the identical field); the
    // CLI-only dispatch branches (`--cluster N`/`--cluster-control`/
    // `--cluster-data`, no config file to merge against) apply it directly,
    // unchanged from before this section existed.
    let cli_cluster_settings = animusd::config::ClusterSettings {
        auto_split_bytes,
        auto_split_change_rate,
        auto_split_ops_rate,
        orphan_sweep_after_secs: orphan_sweep_after,
        quiesce_after_secs: quiesce_after,
        stream_seal_bytes,
        stream_seal_age_secs,
        stream_retention_secs,
        throttle_read_units,
        throttle_write_units,
    };
    let orphan_sweep_after =
        orphan_sweep_after_duration(cli_cluster_settings.orphan_sweep_after_secs);
    let stream_seal_knobs = stream_seal_knobs(
        cli_cluster_settings.stream_seal_bytes,
        cli_cluster_settings.stream_seal_age_secs,
    );
    let stream_retention = cli_cluster_settings
        .stream_retention_secs
        .map_or(animusd::DEFAULT_STREAM_RETENTION, Duration::from_secs);
    let segment_store_config = parse_segment_store(segment_store.as_deref())?;
    let backup_store_config = parse_backup_store(backup_store.as_deref())?;
    let quiesce_after = quiesce_after_duration(cli_cluster_settings.quiesce_after_secs);
    validate_quiesce_after(quiesce_after)?;
    let dynamo_auth_flag = dynamo_auth_path
        .as_deref()
        .map(load_dynamo_auth_file)
        .transpose()?;

    if cluster_control.is_some() || cluster_data.is_some() {
        if config_path.is_some() || cluster.is_some() {
            return Err(
                "use either --config, --cluster, or --cluster-control/--cluster-data, not both"
                    .into(),
            );
        }
        if tls_flag.is_some() {
            return Err("--tls-cert/--tls-key/--tls-ca are not yet supported with \
                 --cluster-control/--cluster-data — use --config/--node against a config file \
                 whose own node entries carry a tls section (ADR 0064)"
                .into());
        }
        let control_n = cluster_control.ok_or("--cluster-data also needs --cluster-control N")?;
        let data_n = cluster_data.ok_or("--cluster-control also needs --cluster-data M")?;
        // `--quiesce-after` does not thread through the split-deployment dev
        // path yet (`run_in_process_split_cluster` has no growth-combination
        // wrapper to call) — a documented gap, matching this path's existing
        // `--stream-seal-*`/`--segment-store` gap noted below.
        return run_in_process_split_cluster(
            control_n,
            data_n,
            ip,
            dir,
            backend,
            auto_split_bytes,
            auto_split_change_rate,
            auto_split_ops_rate,
            orphan_sweep_after,
            dynamo_auth_flag.map(|c| std::sync::Arc::new(c.credentials)),
        )
        .await;
    }

    match (config_path, cluster) {
        (Some(_), Some(_)) => Err("use either --config or --cluster, not both".into()),
        (Some(path), None) => {
            let index = node.ok_or("--config requires --node I")?;
            run_single(
                &path,
                index,
                dir,
                backend,
                cli_cluster_settings,
                segment_store_config,
                dynamo_auth_flag,
                backup_store_config,
                advertise_host,
                tls_flag,
            )
            .await
        }
        (None, Some(n)) => {
            if tls_flag.is_some() {
                return Err(
                    "--tls-cert/--tls-key/--tls-ca are not yet supported with --cluster N — \
                     use --config/--node against a config file whose own node entries carry a \
                     tls section (ADR 0064)"
                        .into(),
                );
            }
            run_in_process_cluster(
                n,
                ip,
                dir,
                backend,
                auto_split_bytes,
                auto_split_change_rate,
                auto_split_ops_rate,
                orphan_sweep_after,
                stream_seal_knobs,
                segment_store_config,
                stream_retention,
                quiesce_after,
                dynamo_auth_flag.map(|c| std::sync::Arc::new(c.credentials)),
                backup_store_config,
                advertise_host,
                throttle_read_units,
                throttle_write_units,
            )
            .await
        }
        (None, None) => Err("nothing to do".into()),
    }
}

/// **Default ON** at [`DEFAULT_QUIESCE_AFTER_SECS`] when `--quiesce-after` is
/// omitted (ADR 0044 phase-1 PR7) — a data-plane CP group with no local
/// activity for this long stops ticking (no more Raft timers/heartbeats)
/// until something touches it again (a client write, a peer message, or the
/// per-node reconciler waking it if its own replica set has a member marked
/// `Down`). **This default is a maintainer-reviewable call, not a settled
/// operational fact** — see this constant's own doc for the evidence behind
/// it and how to override/disable it. `0` disables the feature entirely,
/// restoring byte-identical pre-quiescence behavior.
fn quiesce_after_duration(secs: Option<u64>) -> Duration {
    Duration::from_secs(secs.unwrap_or(DEFAULT_QUIESCE_AFTER_SECS))
}

/// The default `--quiesce-after` idle threshold (ADR 0044 phase-1 PR7),
/// used whenever the flag is omitted: **5 seconds**.
///
/// **Why proposed default-ON rather than default-off**: the mechanism
/// (`animus-control`/`animus-cp-data`'s core state machine, PR3) and every
/// wake/veto/sweeper-skip path built on top of it (PR4–PR6) are exercised by
/// a seed-reproducible `SimEnv` corpus at depth, a real-thread `ProdEnv`
/// leader-kill liveness regression
/// (`animusd/tests/cp_quiescence.rs::write_after_leader_kill_of_a_quiesced_group_converges`),
/// and this crate's full existing `ProdEnv` integration suite (auto-split,
/// 2PC transactions, DynamoDB Streams end-to-end, the D8 historical
/// split-duplication adjudicator) all passing unmodified with quiescence
/// wired in — no destabilization was found. 5 seconds is short enough that a
/// freshly cold tablet's first touch after idling pays only one proactive
/// wake (`resolve_cp_route`'s wake-on-demand, PR4) plus an ordinary routing
/// round trip, and long enough that any real client traffic (or the
/// existing background sweepers, before PR5's veto/PR6's skip apply) keeps
/// a genuinely busy tablet from ever idling into quiescence in the first
/// place.
///
/// **What was *not* separately validated**: this default was not stress-
/// tested against a large (dozens-to-hundreds of tablets) fleet under
/// sustained mixed read/write load with real network latency between
/// processes — the one instrument the plan's own "one open risk" section
/// names (`--cluster 3 --auto-split-bytes` low enough to manufacture ~50
/// tablets, diffing `GET /metrics` over a 60s idle window). If a future
/// deployment's own soak testing finds this default too aggressive
/// (churning quiesce/wake cycles under light-but-steady traffic, say), the
/// fix is lowering this constant or defaulting to `0` — never changing the
/// mechanism itself, which is correct at any threshold `> 0`.
const DEFAULT_QUIESCE_AFTER_SECS: u64 = 5;

/// [`animusd::StreamSealKnobs`] from the optional `--stream-seal-bytes`/
/// `--stream-seal-age` CLI values — each independently defaults to
/// [`animusd::StreamSealKnobs::default`]'s own field when omitted.
fn stream_seal_knobs(bytes: Option<u64>, age_secs: Option<u64>) -> animusd::StreamSealKnobs {
    let default = animusd::StreamSealKnobs::default();
    animusd::StreamSealKnobs {
        seal_bytes: bytes.unwrap_or(default.seal_bytes),
        seal_age: age_secs.map_or(default.seal_age, Duration::from_secs),
    }
}

/// Parse a `--seed ADDR[,ADDR...]` CLI value into the `host:port` strings
/// `run_node_join`/`run_node_data_join` dial (ADR 0060's advertise/dial
/// split): each comma-separated entry is carried through as-is rather than
/// `.parse::<SocketAddr>()`'d — a seed may name a hostname (a Kubernetes
/// pod's stable DNS name) as well as a numeric address, and the only place
/// that ever needs to resolve one is the actual connect call
/// (`TcpStream::connect`'s own `ToSocketAddrs` impl for `&str`). Still
/// rejects an entry with no `:port` at all — a cheap shape check that names
/// an obviously-wrong value at the CLI boundary rather than as an opaque
/// connect failure deep in the join retry loop.
fn parse_seed_arg(seed_arg: &str) -> Result<Vec<String>, String> {
    seed_arg
        .split(',')
        .map(|s| {
            let s = s.trim();
            if s.contains(':') {
                Ok(s.to_string())
            } else {
                Err(format!(
                    "invalid --seed address `{s}`: expected a `host:port` string"
                ))
            }
        })
        .collect()
}

/// [`animusd::SegmentStoreConfig`] from the optional `--segment-store` CLI
/// value: absent selects the default `ClusterSegmentStore`; `dir:PATH` (the
/// only recognized form) opts into a bare `FsSegmentStore` at `PATH`.
fn parse_segment_store(value: Option<&str>) -> Result<animusd::SegmentStoreConfig, String> {
    match value {
        None => Ok(animusd::SegmentStoreConfig::default()),
        Some(v) => match v.strip_prefix("dir:") {
            Some(path) if !path.is_empty() => Ok(animusd::SegmentStoreConfig::Fs(path.into())),
            _ => Err(format!(
                "--segment-store {v:?}: only `dir:PATH` is recognized"
            )),
        },
    }
}

/// [`animusd::BackupStoreConfig`] from the optional `--backup-store` CLI
/// value (ADR 0059 §1): absent or the literal `cluster` selects the default
/// K-replicated `ClusterSegmentStore`; `fs:PATH` opts into a bare
/// `FsSegmentStore` at `PATH` instead — the same two forms
/// [`parse_segment_store`] accepts, plus the explicit `cluster` keyword the
/// ADR itself spells the knob with (`--segment-store` has no such keyword —
/// omitting it is that store's only way to select the default).
fn parse_backup_store(value: Option<&str>) -> Result<animusd::BackupStoreConfig, String> {
    match value {
        None => Ok(animusd::BackupStoreConfig::default()),
        Some("cluster") => Ok(animusd::BackupStoreConfig::Cluster),
        Some(v) => match v.strip_prefix("fs:") {
            Some(path) if !path.is_empty() => Ok(animusd::BackupStoreConfig::Fs(path.into())),
            _ => Err(format!(
                "--backup-store {v:?}: only `cluster` or `fs:PATH` is recognized"
            )),
        },
    }
}

/// Load a `--dynamo-auth PATH` file (ADR 0057): the same JSON shape as a
/// [`ClusterConfig`]'s own `dynamo_auth` section
/// (`{"credentials": {"AKID": "secret", ...}}`), validated the same way
/// (a present-but-empty credentials map is a load-time error).
fn load_dynamo_auth_file(path: &str) -> Result<animusd::DynamoAuthConfig, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("reading {path}: {e}"))?;
    let cfg: animusd::DynamoAuthConfig =
        serde_json::from_str(&text).map_err(|e| format!("parsing --dynamo-auth {path}: {e}"))?;
    cfg.validate()
        .map_err(|e| format!("--dynamo-auth {path}: {e}"))?;
    Ok(cfg)
}

/// Merge a `--dynamo-auth PATH` flag into a loaded [`ClusterConfig`] (ADR
/// 0057): if the config file's own `dynamo_auth` section is also present,
/// that is a hard startup error — no silent precedence between the two
/// sources. Otherwise the flag's credentials become the config's, exactly as
/// if the config file had carried that section itself.
fn apply_dynamo_auth_flag(
    config: &mut ClusterConfig,
    flag: Option<animusd::DynamoAuthConfig>,
) -> Result<(), String> {
    match (&config.dynamo_auth, flag) {
        (Some(_), Some(_)) => Err(
            "dynamo_auth is set both in the config file and via --dynamo-auth — specify it \
             one way, not both"
                .to_string(),
        ),
        (None, Some(flag)) => {
            config.dynamo_auth = Some(flag);
            Ok(())
        }
        (_, None) => Ok(()),
    }
}

/// Parse `--tls-cert PATH --tls-key PATH --tls-ca PATH` (ADR 0064, S-01
/// commit 2) into a [`TlsSection`] — the three flags are **all-or-none**:
/// TLS on the internal wire is always mutual (see `animus_env::tls`'s own
/// doc), so a `TlsConfig` with no CA to verify peers against is never
/// meaningful, and a cert/key with no matching CA (or vice versa) is
/// always a mistake worth failing loudly on rather than guessing.
///
/// # Errors
/// A message if exactly one or two of the three flags were given.
fn resolve_tls_flags(
    cert: Option<String>,
    key: Option<String>,
    ca: Option<String>,
) -> Result<Option<TlsSection>, String> {
    match (cert, key, ca) {
        (None, None, None) => Ok(None),
        (Some(cert_path), Some(key_path), Some(ca_path)) => Ok(Some(TlsSection {
            cert_path: cert_path.into(),
            key_path: key_path.into(),
            ca_path: Some(ca_path.into()),
        })),
        _ => Err(
            "--tls-cert, --tls-key, and --tls-ca must be given together (all three) or omitted \
             entirely — the internal wire is mutual TLS and always needs a CA to verify peers \
             against (ADR 0064)"
                .to_string(),
        ),
    }
}

/// Apply a parsed `--tls-*` flag (ADR 0064, S-01 commit 2) onto
/// `config.nodes[index]` — this process's own entry only, mirroring
/// [`apply_advertise_host_flag`]'s exact shape (**not**
/// [`apply_dynamo_auth_flag`]'s cluster-wide one): unlike a SigV4 credential
/// map, TLS material is inherently per-node (each node presents its own
/// cert), so there is no single cluster-wide field to merge into — only
/// `ca_path` is conventionally the same file on every node, and even that
/// is a deployment convention this function doesn't assume or enforce
/// (`ClusterConfig::validate_tls`, called by `from_json`, enforces the one
/// thing that *is* cluster-wide: every node has a `tls` section or none
/// does).
///
/// # Errors
/// A message if `config.nodes[index]` already carries a `tls` section
/// **and** the flag was also given — "specify it one way, not both",
/// [`apply_dynamo_auth_flag`]'s own contract, applied per-node here.
fn apply_tls_flag(
    config: &mut ClusterConfig,
    index: usize,
    flag: Option<TlsSection>,
) -> Result<(), String> {
    let entry = config
        .nodes
        .get_mut(index)
        .ok_or_else(|| format!("node index {index} out of range"))?;
    match (&entry.tls, flag) {
        (Some(_), Some(_)) => Err(format!(
            "node {index}'s tls section is set both in the config file and via --tls-* — \
             specify it one way, not both"
        )),
        (None, Some(flag)) => {
            entry.tls = Some(flag);
            Ok(())
        }
        (_, None) => Ok(()),
    }
}

/// The URL scheme a startup banner should print for a port this node bound
/// with (`true`) or without (`false`) TLS material (ADR 0064) — every
/// `println!` banner below that used to hardcode `http` regardless of
/// `spec.tls`/`--tls-*`/a config file's own `tls` section now goes through
/// this, so the two never drift again.
fn scheme(tls_enabled: bool) -> &'static str {
    if tls_enabled { "https" } else { "http" }
}

/// Apply `--advertise-host NAME` (ADR 0060) onto `config.nodes[index]`
/// (this process's own entry only — every other node's `advertise_host`
/// stays exactly what its own config entry already says): the same
/// "flag and config file both set it is a hard error, not a silent
/// precedence rule" shape [`apply_dynamo_auth_flag`] uses, so a config
/// generated once (with no `advertise_host` of its own) and started with a
/// different `--advertise-host` per pod is the common case, while a config
/// that *does* carry an explicit `advertise_host` for this entry can't be
/// silently overridden by a stale flag left on the command line.
fn apply_advertise_host_flag(
    config: &mut ClusterConfig,
    index: usize,
    flag: Option<String>,
) -> Result<(), String> {
    let entry = config
        .nodes
        .get_mut(index)
        .ok_or_else(|| format!("node index {index} out of range"))?;
    match (&entry.advertise_host, flag) {
        (Some(_), Some(_)) => Err(format!(
            "node {index}'s advertise_host is set both in the config file and via \
             --advertise-host — specify it one way, not both"
        )),
        (None, Some(flag)) => {
            entry.advertise_host = Some(flag);
            Ok(())
        }
        (_, None) => Ok(()),
    }
}

/// Merge a config file's `cluster_settings` section (S-06) with whatever
/// subset of the same knobs a CLI flag also supplied on this invocation —
/// the field-by-field version of [`apply_dynamo_auth_flag`]'s "specify it
/// one way, not both" contract: a CLI flag **and** the config's own section
/// setting the identical field is a hard startup error, but an operator may
/// still set *different* fields on each side (e.g. `--orphan-sweep-after`
/// on the CLI with `auto_split_bytes` only in the config file) — see
/// [`animusd::config::ClusterSettings`]'s own doc for the full field list
/// and per-role applicability.
///
/// `cli` carries only the fields this invocation's own CLI flags actually
/// set (every field this subcommand has no flag for stays `None`, so it can
/// never conflict) — every field not present in either source resolves to
/// `None`, exactly as omitting both the flag and the config section already
/// does.
///
/// # Errors
/// A message naming the conflicting field if both sources set it.
fn resolve_cluster_settings(
    config_settings: Option<&animusd::config::ClusterSettings>,
    cli: &animusd::config::ClusterSettings,
) -> Result<animusd::config::ClusterSettings, String> {
    let config_settings = config_settings.cloned().unwrap_or_default();
    let mut effective = config_settings.clone();
    macro_rules! merge_field {
        ($field:ident, $flag_name:literal) => {
            if let Some(v) = cli.$field {
                if config_settings.$field.is_some() {
                    return Err(format!(
                        "cluster_settings.{} is set both in the config file and via {} — \
                         specify it one way, not both",
                        stringify!($field),
                        $flag_name
                    ));
                }
                effective.$field = Some(v);
            }
        };
    }
    merge_field!(auto_split_bytes, "--auto-split-bytes");
    merge_field!(auto_split_change_rate, "--auto-split-change-rate");
    merge_field!(auto_split_ops_rate, "--auto-split-ops-rate");
    merge_field!(orphan_sweep_after_secs, "--orphan-sweep-after");
    merge_field!(quiesce_after_secs, "--quiesce-after");
    merge_field!(stream_seal_bytes, "--stream-seal-bytes");
    merge_field!(stream_seal_age_secs, "--stream-seal-age");
    merge_field!(stream_retention_secs, "--stream-retention");
    merge_field!(throttle_read_units, "--throttle-read-units");
    merge_field!(throttle_write_units, "--throttle-write-units");
    Ok(effective)
}

/// `--quiesce-after`'s floor check (issue #302), factored out so both
/// [`run`]'s CLI-only dispatch branches and [`run_single`]'s
/// config-file-merged value (S-06 — a config-supplied `quiesce_after_secs`
/// never passed through the CLI-only check below it) get the identical
/// validation.
///
/// # Errors
/// As documented on the message itself.
fn validate_quiesce_after(quiesce_after: Duration) -> Result<(), String> {
    if !quiesce_after.is_zero() && quiesce_after < animusd::MIN_QUIESCE_AFTER {
        return Err(format!(
            "--quiesce-after must be at least {} ms (animusd's change-consumer \
             sweep interval) or 0 to disable quiescence entirely; got {} ms",
            animusd::MIN_QUIESCE_AFTER.as_millis(),
            quiesce_after.as_millis()
        ));
    }
    Ok(())
}

/// Per-process: run node `index` from the config file.
///
/// `dynamo_auth_flag` (ADR 0057) is `--dynamo-auth PATH`, already loaded —
/// specifying credentials **both** ways (the flag **and** the config file's
/// own `dynamo_auth` section) is a hard startup error, not a silent
/// precedence rule.
///
/// `backup_store_config` (ADR 0059 §1) is `--backup-store cluster|fs:PATH`,
/// already parsed. Plumbing only (ADR 0059 Train 1 PR②).
///
/// `cli_settings` (S-06) is the raw `--auto-split-bytes`/`--auto-split-
/// change-rate`/`--orphan-sweep-after`/`--quiesce-after`/`--stream-seal-
/// bytes`/`--stream-seal-age`/`--stream-retention` values this invocation's
/// CLI flags actually set — merged here against the loaded config's own
/// `cluster_settings` section (a hard error, field by field, if both sides
/// set the same one — [`resolve_cluster_settings`]) before being converted
/// to the durations/knobs [`animusd::run_node_with_cluster_settings`]
/// takes. This is `--config`/`--node`'s only route to auto-split at all
/// (previously reachable solely via `--cluster N`'s dev-only in-process
/// mode).
#[allow(clippy::too_many_arguments)]
async fn run_single(
    path: &str,
    index: usize,
    dir: Option<std::path::PathBuf>,
    backend: animusd::StorageBackend,
    cli_settings: animusd::config::ClusterSettings,
    segment_store_config: animusd::SegmentStoreConfig,
    dynamo_auth_flag: Option<animusd::DynamoAuthConfig>,
    backup_store_config: animusd::BackupStoreConfig,
    advertise_host: Option<String>,
    tls_flag: Option<TlsSection>,
) -> Result<(), String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("reading {path}: {e}"))?;
    let mut config = ClusterConfig::from_json(&text).map_err(|e| format!("parsing {path}: {e}"))?;
    apply_dynamo_auth_flag(&mut config, dynamo_auth_flag)?;
    apply_advertise_host_flag(&mut config, index, advertise_host)?;
    // Deliberately NOT re-running `ClusterConfig::validate_tls` after this
    // per-node merge: that check is the whole-file, all-nodes-or-none
    // invariant (already enforced once, above, by `from_json` against the
    // config file's own baked-in `tls` sections) — a real >1-node
    // deployment where each process supplies its *own* node's cert/key via
    // this flag (rather than baking every node's section into one shared
    // file) would otherwise see every other node's entry as `None` in its
    // own local, single-process view and fail a check that has nothing to
    // do with what this process actually controls.
    apply_tls_flag(&mut config, index, tls_flag)?;
    let dir = dir.unwrap_or_else(|| std::env::temp_dir().join(format!("animusd-node-{index}")));

    let settings = resolve_cluster_settings(config.cluster_settings.as_ref(), &cli_settings)?;
    let orphan_sweep_after = orphan_sweep_after_duration(settings.orphan_sweep_after_secs);
    let stream_seal_knobs_val =
        stream_seal_knobs(settings.stream_seal_bytes, settings.stream_seal_age_secs);
    let stream_retention = settings
        .stream_retention_secs
        .map_or(animusd::DEFAULT_STREAM_RETENTION, Duration::from_secs);
    let quiesce_after = quiesce_after_duration(settings.quiesce_after_secs);
    validate_quiesce_after(quiesce_after)?;

    let node = animusd::run_node_with_cluster_settings(
        &config,
        index,
        &dir,
        backend,
        orphan_sweep_after,
        stream_seal_knobs_val,
        segment_store_config,
        stream_retention,
        quiesce_after,
        settings.auto_split_bytes,
        settings.auto_split_change_rate,
        settings.auto_split_ops_rate,
        backup_store_config,
        settings.throttle_read_units,
        settings.throttle_write_units,
    )
    .await
    .map_err(|e| format!("failed to start node {index}: {e}"))?;
    let scheme = scheme(config.nodes[index].tls.is_some());
    println!(
        "animusd: node {index}/{} up (CP) — client {} — dynamo {scheme} {} — admin {scheme}://{} — console {scheme}://{}",
        config.len(),
        node.client_addr(),
        node.dynamo_addr(),
        node.admin_addr(),
        node.console_addr(),
    );
    if let Some(b) = settings.auto_split_bytes {
        println!("animusd: node {index} auto-split at {b} bytes/tablet");
    }
    if let Some(rate) = settings.auto_split_change_rate {
        println!(
            "animusd: node {index} streamed-table auto-split ALSO fires above {rate} change-bytes/sec/tablet"
        );
    }
    if let Some(rate) = settings.auto_split_ops_rate {
        println!("animusd: node {index} auto-split ALSO fires above {rate} write-ops/sec/tablet");
    }
    if let Some(units) = settings.throttle_read_units {
        println!(
            "animusd: node {index} cluster-wide default throttle at {units} read capacity units/table"
        );
    }
    if let Some(units) = settings.throttle_write_units {
        println!(
            "animusd: node {index} cluster-wide default throttle at {units} write capacity units/table"
        );
    }
    println!("animusd: ready — Ctrl-C to stop");
    wait_for_ctrl_c().await;
    node.shutdown_graceful().await;
    Ok(())
}

/// `control`: run node `index` of `config` as a **control-only** node (ADR
/// 0035 PR3) — no CP data storage engine, no `raftkv` env, no DynamoDB
/// listener. `--ephemeral` (ADR 0038 PR2) selects a volatile in-memory
/// system-keyspace mirror engine instead of the durable on-disk default.
async fn run_control(args: &[String]) -> Result<(), String> {
    let mut config_path: Option<String> = None;
    let mut node: Option<usize> = None;
    let mut dir: Option<std::path::PathBuf> = None;
    let mut backend = animusd::StorageBackend::default();
    // See `run`'s own `--orphan-sweep-after` doc (ADR 0040 PR6) — applies
    // identically here since a control-only node runs a local `RaftNode` too.
    let mut orphan_sweep_after: Option<u64> = None;
    // `--segment-store dir:PATH` / `--backup-store cluster|fs:PATH` (W-10,
    // ADR 0043 §A9's control-only-leader gap — closed): see `run`'s own doc
    // for the full knob description. A control-only node can genuinely
    // become the control-plane leader (ADR 0035) and now provisions these
    // handles exactly like a combined or data-only node's own `--config`/
    // `--cluster N` path does.
    let mut segment_store: Option<String> = None;
    let mut backup_store: Option<String> = None;

    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--config" => config_path = Some(parse_next(&mut it, "--config")?),
            "--node" => node = Some(parse_next(&mut it, "--node")?),
            "--dir" => dir = Some(parse_next::<String>(&mut it, "--dir")?.into()),
            "--ephemeral" => backend = animusd::StorageBackend::Memory,
            "--orphan-sweep-after" => {
                orphan_sweep_after = Some(parse_next(&mut it, "--orphan-sweep-after")?);
            }
            "--segment-store" => {
                segment_store = Some(parse_next(&mut it, "--segment-store")?);
            }
            "--backup-store" => {
                backup_store = Some(parse_next(&mut it, "--backup-store")?);
            }
            other => return Err(format!("unknown control argument `{other}`")),
        }
    }
    let segment_store_config = parse_segment_store(segment_store.as_deref())?;
    let backup_store_config = parse_backup_store(backup_store.as_deref())?;
    let path = config_path.ok_or("control requires --config FILE")?;
    let index = node.ok_or("control requires --node I")?;
    let text = std::fs::read_to_string(&path).map_err(|e| format!("reading {path}: {e}"))?;
    let config = ClusterConfig::from_json(&text).map_err(|e| format!("parsing {path}: {e}"))?;
    // S-06: `cluster_settings.orphan_sweep_after_secs` reaches a control-only
    // node too — the only field of that section a control-only node can act
    // on (no data role, so every other field is silently ignored — see
    // `animusd::config::ClusterSettings`'s own doc). A conflict with
    // `--orphan-sweep-after` is a hard startup error, same shape as every
    // other cluster-settings field.
    let cli_settings = animusd::config::ClusterSettings {
        orphan_sweep_after_secs: orphan_sweep_after,
        ..Default::default()
    };
    let settings = resolve_cluster_settings(config.cluster_settings.as_ref(), &cli_settings)?;
    let orphan_sweep_after = orphan_sweep_after_duration(settings.orphan_sweep_after_secs);
    let dir = dir.unwrap_or_else(|| std::env::temp_dir().join(format!("animusd-control-{index}")));

    let node = animusd::run_node_control_with_stores(
        &config,
        index,
        &dir,
        backend,
        orphan_sweep_after,
        segment_store_config,
        backup_store_config,
        animusd::DEFAULT_STREAM_RETENTION,
    )
    .await
    .map_err(|e| format!("failed to start control node {index}: {e}"))?;
    let scheme = scheme(config.nodes[index].tls.is_some());
    println!(
        "animusd: control node {index}/{} up — client {} — admin {scheme}://{}",
        config.len(),
        node.client_addr(),
        node.admin_addr(),
    );
    println!("animusd: ready — Ctrl-C to stop");
    wait_for_ctrl_c().await;
    node.shutdown_graceful().await;
    Ok(())
}

/// `data`: run node `index` as a **data-only** node (ADR 0035 PR4, or PR5's
/// `--seed` join variant) — no control env, no local control `RaftCore`,
/// reaching the separately-deployed control plane over the network instead.
/// Either `--config FILE` (an operator-assembled `ClusterConfig` listing the
/// control deployment's addresses up front — see
/// [`animusd::run_node_data`]'s doc) or `--seed ADDR[,ADDR...]` (this node
/// discovers the control deployment from any already-running node's
/// **intra-cluster** address, ADR 0047 — joining is a cluster-membership
/// action, not an external-client one, so the honest seed address is the
/// intra one, not the client one; mirrors `animusd join`'s discovery — see
/// [`animusd::run_node_data_join`]'s doc), never both.
///
/// **`--node I` is required with `--config`** (unchanged). **`--seed` no
/// longer takes `--node I` at all (ADR 0040 PR4, clean break)**: `--id NAME`
/// proposes a durable identity ([`NodeId::propose`] validates it here, at the
/// CLI boundary); omitted, this node self-mints one (ADR 0040 Decision B).
/// `--base-port` is **required** with `--seed` either way — there is no
/// index left to derive a default port range from.
async fn run_data(args: &[String]) -> Result<(), String> {
    let mut config_path: Option<String> = None;
    let mut node: Option<usize> = None;
    let mut dir: Option<std::path::PathBuf> = None;
    let mut backend = animusd::StorageBackend::default();
    let mut seed_arg: Option<String> = None;
    let mut id: Option<String> = None;
    let mut ip: IpAddr = "127.0.0.1".parse().unwrap();
    let mut base_port: Option<u16> = None;
    // `--dynamo-auth PATH` (ADR 0057) — see `run`'s own doc for the shared
    // shape/semantics; accepted here too since a data-only node binds the
    // dynamo listener (ADR 0035 PR4).
    let mut dynamo_auth_path: Option<String> = None;
    // `--advertise-host NAME` (ADR 0060) — see `run_join`'s own doc.
    let mut advertise_host: Option<String> = None;
    // `--tls-cert PATH --tls-key PATH --tls-ca PATH` (ADR 0064, S-01 commit
    // 2) — see `run`'s own doc for the shared shape. `--config`: applied to
    // `config.nodes[index]` via `apply_tls_flag`, same as `run_single`.
    // `--seed`: this node's `RoleAddrs` is built ad hoc (no config file at
    // all), so the flag is set directly with no conflict to check against.
    let mut tls_cert: Option<String> = None;
    let mut tls_key: Option<String> = None;
    let mut tls_ca: Option<String> = None;

    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--config" => config_path = Some(parse_next(&mut it, "--config")?),
            "--node" => node = Some(parse_next(&mut it, "--node")?),
            "--dir" => dir = Some(parse_next::<String>(&mut it, "--dir")?.into()),
            "--ephemeral" => backend = animusd::StorageBackend::Memory,
            "--seed" => seed_arg = Some(parse_next::<String>(&mut it, "--seed")?),
            "--id" => id = Some(parse_next::<String>(&mut it, "--id")?),
            "--ip" => ip = parse_next(&mut it, "--ip")?,
            "--base-port" => base_port = Some(parse_next(&mut it, "--base-port")?),
            "--dynamo-auth" => {
                dynamo_auth_path = Some(parse_next(&mut it, "--dynamo-auth")?);
            }
            "--advertise-host" => {
                advertise_host = Some(parse_next::<String>(&mut it, "--advertise-host")?);
            }
            "--tls-cert" => tls_cert = Some(parse_next(&mut it, "--tls-cert")?),
            "--tls-key" => tls_key = Some(parse_next(&mut it, "--tls-key")?),
            "--tls-ca" => tls_ca = Some(parse_next(&mut it, "--tls-ca")?),
            other => return Err(format!("unknown data argument `{other}`")),
        }
    }
    let dynamo_auth_flag = dynamo_auth_path
        .as_deref()
        .map(load_dynamo_auth_file)
        .transpose()?;
    let tls_flag = resolve_tls_flags(tls_cert, tls_key, tls_ca)?;

    match (config_path, seed_arg) {
        (Some(_), Some(_)) => Err("use either --config or --seed, not both".into()),
        (Some(path), None) => {
            let index = node.ok_or("data requires --node I")?;
            run_data_config(
                &path,
                index,
                dir,
                backend,
                dynamo_auth_flag,
                advertise_host,
                tls_flag,
            )
            .await
        }
        (None, Some(seed_arg)) => {
            let id = id
                .map(|s| s.parse::<NodeId>())
                .transpose()
                .map_err(|e| format!("invalid --id: {e}"))?;
            let base_port = base_port.ok_or(
                "data --seed requires an explicit --base-port (ADR 0040: there is no \
                 --node index left to derive a default port range from)",
            )?;
            run_data_join(
                &seed_arg,
                id,
                ip,
                base_port,
                dir,
                backend,
                dynamo_auth_flag.map(|c| std::sync::Arc::new(c.credentials)),
                advertise_host,
                tls_flag,
            )
            .await
        }
        (None, None) => Err("data requires --config FILE or --seed ADDR[,ADDR...]".into()),
    }
}

/// `animusd data --config FILE --node I` (ADR 0035 PR4): the operator-assembled-config half of [`run_data`].
///
/// `dynamo_auth_flag` — see [`run_single`]'s identical doc: specifying
/// credentials both in the config file's own `dynamo_auth` section and via
/// the flag is a hard startup error.
///
/// **S-06**: `data --config` has no CLI flags of its own for any
/// `cluster_settings` field yet, so there is nothing to conflict-check here
/// — the config file's section (auto-split, quiesce, stream-seal; a
/// data-only node has no local control `RaftNode` and is never a
/// control-plane leader, so `orphan_sweep_after_secs`/`stream_retention_
/// secs` are ignored — see [`animusd::config::ClusterSettings`]'s own doc)
/// is applied straight through [`animusd::run_node_data_with_cluster_
/// settings`].
async fn run_data_config(
    path: &str,
    index: usize,
    dir: Option<std::path::PathBuf>,
    backend: animusd::StorageBackend,
    dynamo_auth_flag: Option<animusd::DynamoAuthConfig>,
    advertise_host: Option<String>,
    tls_flag: Option<TlsSection>,
) -> Result<(), String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("reading {path}: {e}"))?;
    let mut config = ClusterConfig::from_json(&text).map_err(|e| format!("parsing {path}: {e}"))?;
    apply_dynamo_auth_flag(&mut config, dynamo_auth_flag)?;
    apply_advertise_host_flag(&mut config, index, advertise_host)?;
    // See `run_single`'s identical note: deliberately not re-checking the
    // whole-config all-or-none TLS invariant after this per-node merge.
    apply_tls_flag(&mut config, index, tls_flag)?;
    let dir = dir.unwrap_or_else(|| std::env::temp_dir().join(format!("animusd-data-{index}")));

    let settings = config.cluster_settings.clone().unwrap_or_default();
    let quiesce_after = quiesce_after_duration(settings.quiesce_after_secs);
    validate_quiesce_after(quiesce_after)?;
    let stream_seal_knobs_val =
        stream_seal_knobs(settings.stream_seal_bytes, settings.stream_seal_age_secs);

    let node = animusd::run_node_data_with_cluster_settings(
        &config,
        index,
        &dir,
        backend,
        settings.auto_split_bytes,
        settings.auto_split_change_rate,
        settings.auto_split_ops_rate,
        quiesce_after,
        stream_seal_knobs_val,
        // Same documented gap as `--backup-store` (ADR 0059 §1): no
        // `--segment-store` flag reaches `animusd data --config` yet.
        animusd::SegmentStoreConfig::default(),
        settings.throttle_read_units,
        settings.throttle_write_units,
    )
    .await
    .map_err(|e| format!("failed to start data node {index}: {e}"))?;
    let scheme = scheme(config.nodes[index].tls.is_some());
    println!(
        "animusd: data node {index}/{} up (CP) — client {} — dynamo {scheme} {} — admin {scheme}://{} — console {scheme}://{}",
        config.len(),
        node.client_addr(),
        node.dynamo_addr(),
        node.admin_addr(),
        node.console_addr(),
    );
    if let Some(b) = settings.auto_split_bytes {
        println!("animusd: data node {index} auto-split at {b} bytes/tablet");
    }
    println!("animusd: ready — Ctrl-C to stop");
    wait_for_ctrl_c().await;
    node.shutdown_graceful().await;
    Ok(())
}

/// `animusd data --seed ADDR[,ADDR...] [--id NAME] --base-port P` (ADR 0035
/// PR5; ADR 0040 PR4 for the `--id`/self-mint identity shape): the seed/join
/// half of [`run_data`] — mirrors [`run_join`]'s CLI shape exactly, minus the
/// control port (a data-only `RoleAddrs` has none), using
/// [`animusd::run_node_data_join`]. `base_port` is used **literally** (no
/// index to derive it from); the CLI's data dir defaults to a name built from
/// `id` when given, else a mint-pending placeholder distinguished by
/// `base_port`.
#[allow(clippy::too_many_arguments)]
async fn run_data_join(
    seed_arg: &str,
    id: Option<NodeId>,
    ip: IpAddr,
    base_port: u16,
    dir: Option<std::path::PathBuf>,
    backend: animusd::StorageBackend,
    dynamo_auth: Option<std::sync::Arc<BTreeMap<String, String>>>,
    advertise_host: Option<String>,
    tls_flag: Option<TlsSection>,
) -> Result<(), String> {
    let seeds: Vec<String> = parse_seed_arg(seed_arg)?;
    if seeds.is_empty() {
        return Err("data --seed requires at least one address".into());
    }

    let tls_enabled = tls_flag.is_some();
    let p = |role: u16| SocketAddr::new(ip, base_port.wrapping_add(role));
    let addrs = RoleAddrs {
        // Unread placeholder: `Node::bind_data` takes the real (proposed or
        // self-minted) id as its own separate argument, never `addrs.id` —
        // see `run_node_data_join`'s doc.
        id: NodeId::new_unchecked("pending-join"),
        role: animusd::config::NodeRole::Data,
        internal: p(0),
        client: p(1),
        dynamo: p(2),
        admin: p(3),
        intra: p(4),
        console: p(5),
        advertise_host,
        // No config file exists on this join path at all (ADR 0064, S-01
        // commit 2) — the flag is this node's only source, set directly
        // with no "set both ways" conflict to check.
        tls: tls_flag,
    };
    let dir_name = id
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_else(|| format!("mint-{base_port}"));
    let dir =
        dir.unwrap_or_else(|| std::env::temp_dir().join(format!("animusd-data-join-{dir_name}")));

    let node = animusd::run_node_data_join(
        seeds,
        id,
        addrs,
        &dir,
        backend,
        BTreeMap::new(),
        dynamo_auth,
    )
    .await
    .map_err(|e| format!("failed to join as a data node: {e}"))?;
    let scheme = scheme(tls_enabled);
    println!(
        "animusd: data node joined (CP) — client {} — dynamo {scheme} {} — admin {scheme}://{} — console {scheme}://{}",
        node.client_addr(),
        node.dynamo_addr(),
        node.admin_addr(),
        node.console_addr(),
    );
    println!("animusd: ready — Ctrl-C to stop");
    wait_for_ctrl_c().await;
    node.shutdown_graceful().await;
    Ok(())
}

/// `join`: seed/join startup (ADR 0032 PR2) — a new node starts knowing only
/// its own addresses + a seed list (**intra-cluster** addresses of any
/// existing nodes, ADR 0047 — an operator/Kubernetes-operator-supplied seed
/// names the target's intra port, not its client one), learning the
/// pre-growth control group + peer/route/admin address books
/// from the cluster itself instead of an operator-assembled expanded
/// `ClusterConfig`. See [`animusd::run_node_join`]'s doc for the collision
/// guard + growth semantics this drives.
///
/// **`--node I` is gone (ADR 0040 PR4, clean break)**: `--id NAME` proposes
/// a durable identity ([`NodeId::propose`] validates it here, at the CLI
/// boundary, via its `FromStr` impl); omitted, this node self-mints one (ADR
/// 0040 Decision B — [`animusd::run_node_join`] handles both uniformly).
/// `--base-port` is **required** either way — there is no `--node`-index
/// default to fall back to.
async fn run_join(args: &[String]) -> Result<(), String> {
    let mut seed_arg: Option<String> = None;
    let mut id: Option<String> = None;
    let mut ip: IpAddr = "127.0.0.1".parse().unwrap();
    let mut base_port: Option<u16> = None;
    let mut dir: Option<std::path::PathBuf> = None;
    let mut backend = animusd::StorageBackend::default();
    // `--advertise-host NAME` (ADR 0060's advertise/dial split): this
    // node's own stable dial name, if its bind address (`--ip`, e.g. a
    // Kubernetes pod's own wildcard/pod-IP bind) isn't itself something a
    // peer can dial reliably. `None` (the default) is byte-identical to
    // before this ADR — every self-registered address is the bind address
    // itself, stringified. See `RoleAddrs::advertise_host`'s own doc.
    let mut advertise_host: Option<String> = None;

    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--seed" => seed_arg = Some(parse_next::<String>(&mut it, "--seed")?),
            "--id" => id = Some(parse_next::<String>(&mut it, "--id")?),
            "--ip" => ip = parse_next(&mut it, "--ip")?,
            "--base-port" => base_port = Some(parse_next(&mut it, "--base-port")?),
            "--dir" => dir = Some(parse_next::<String>(&mut it, "--dir")?.into()),
            "--ephemeral" => backend = animusd::StorageBackend::Memory,
            "--advertise-host" => {
                advertise_host = Some(parse_next::<String>(&mut it, "--advertise-host")?);
            }
            other => return Err(format!("unknown join argument `{other}`")),
        }
    }

    let seed_arg = seed_arg.ok_or("join requires --seed ADDR[,ADDR...]")?;
    let seeds: Vec<String> = parse_seed_arg(&seed_arg)?;
    if seeds.is_empty() {
        return Err("join requires at least one --seed address".into());
    }
    let base_port = base_port.ok_or(
        "join requires an explicit --base-port (ADR 0040: there is no --node \
         index left to derive a default port range from)",
    )?;
    let id = id
        .map(|s| s.parse::<NodeId>())
        .transpose()
        .map_err(|e| format!("invalid --id: {e}"))?;

    let p = |role: u16| SocketAddr::new(ip, base_port.wrapping_add(role));
    let addrs = RoleAddrs {
        // Unread placeholder: `Node::bind` takes the real (proposed or
        // self-minted) id as its own separate argument, never `addrs.id` —
        // see `run_node_join`'s doc.
        id: NodeId::new_unchecked("pending-join"),
        role: animusd::config::NodeRole::Both,
        internal: p(0),
        client: p(1),
        dynamo: p(2),
        admin: p(3),
        intra: p(4),
        console: p(5),
        advertise_host,
        // `join` accepts no `--tls-*` flag (like `--dynamo-auth`, it isn't
        // wired here — ADR 0064, S-01 commit 2 scope; use `--config`/
        // `--node` against a config file with a `tls` section instead).
        tls: None,
    };
    let dir_name = id
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_else(|| format!("mint-{base_port}"));
    let dir = dir.unwrap_or_else(|| std::env::temp_dir().join(format!("animusd-join-{dir_name}")));

    let node = animusd::run_node_join(seeds, id, addrs, &dir, backend, BTreeMap::new())
        .await
        .map_err(|e| format!("failed to join: {e}"))?;
    // `join` accepts no `--tls-*` flag at all (see `addrs.tls: None` above)
    // — always plain HTTP.
    let scheme = scheme(false);
    println!(
        "animusd: node joined (CP) — client {} — dynamo {scheme} {} — admin {scheme}://{} — console {scheme}://{}",
        node.client_addr(),
        node.dynamo_addr(),
        node.admin_addr(),
        node.console_addr(),
    );
    println!("animusd: ready — Ctrl-C to stop");
    wait_for_ctrl_c().await;
    node.shutdown_graceful().await;
    Ok(())
}

/// In-process: run an `n`-node cluster (dev convenience).
#[allow(clippy::too_many_arguments)]
async fn run_in_process_cluster(
    n: usize,
    ip: IpAddr,
    dir: Option<std::path::PathBuf>,
    backend: animusd::StorageBackend,
    auto_split_bytes: Option<u64>,
    auto_split_change_rate: Option<u64>,
    auto_split_ops_rate: Option<u64>,
    orphan_sweep_after: Duration,
    stream_seal_knobs: animusd::StreamSealKnobs,
    segment_store_config: animusd::SegmentStoreConfig,
    stream_retention: Duration,
    quiesce_after: Duration,
    dynamo_auth: Option<std::sync::Arc<BTreeMap<String, String>>>,
    backup_store_config: animusd::BackupStoreConfig,
    advertise_host: Option<String>,
    throttle_read_units: Option<u64>,
    throttle_write_units: Option<u64>,
) -> Result<(), String> {
    if n == 0 {
        return Err("--cluster must be at least 1".into());
    }
    let dir = dir.unwrap_or_else(|| std::env::temp_dir().join("animusd"));
    let bound = animusd::bind_cluster_with_advertise_host(n, ip, &dir, advertise_host)
        .await
        .map_err(|e| format!("failed to bind cluster: {e}"))?;
    let nodes = animusd::start_cluster_with_growth_and_quiesce_after(
        bound,
        backend,
        auto_split_bytes,
        orphan_sweep_after,
        stream_seal_knobs,
        segment_store_config,
        stream_retention,
        auto_split_change_rate,
        auto_split_ops_rate,
        quiesce_after,
        dynamo_auth,
        backup_store_config,
        throttle_read_units,
        throttle_write_units,
    )
    .await
    .map_err(|e| format!("failed to start cluster: {e}"))?;

    match auto_split_bytes {
        Some(b) => {
            println!("animusd: started {n}-node cluster (CP) — auto-split at {b} bytes/tablet")
        }
        None => println!("animusd: started {n}-node cluster (CP)"),
    }
    if let Some(rate) = auto_split_change_rate {
        println!(
            "animusd: streamed-table auto-split ALSO fires above {rate} change-bytes/sec/tablet"
        );
    }
    if let Some(rate) = auto_split_ops_rate {
        println!("animusd: auto-split ALSO fires above {rate} write-ops/sec/tablet");
    }
    if let Some(units) = throttle_read_units {
        println!("animusd: cluster-wide default throttle at {units} read capacity units/table");
    }
    if let Some(units) = throttle_write_units {
        println!("animusd: cluster-wide default throttle at {units} write capacity units/table");
    }
    // The in-process `--cluster N` dev convenience has no `--tls-*` knob of
    // its own — always plain HTTP.
    let scheme = scheme(false);
    for (i, node) in nodes.iter().enumerate() {
        println!(
            "  node {i}: client {} — dynamo {scheme} {} — admin {scheme}://{} — console {scheme}://{}",
            node.client_addr(),
            node.dynamo_addr(),
            node.admin_addr(),
            node.console_addr(),
        );
    }
    println!("animusd: ready — Ctrl-C to stop");
    wait_for_ctrl_c().await;
    for node in &nodes {
        node.shutdown_graceful().await;
    }
    Ok(())
}

/// In-process: run a whole split deployment (`control_n` control-only nodes
/// and `data_n` data-only nodes, no combined-mode node) in one process — the
/// `--cluster-control N --cluster-data M` dev-convenience sibling of
/// `--cluster N` ([`run_in_process_cluster`]), backed by
/// [`animusd::start_split_cluster_with`].
#[allow(clippy::too_many_arguments)] // mirrors `start_split_cluster_with_orphan_sweep_after`'s own arity
async fn run_in_process_split_cluster(
    control_n: usize,
    data_n: usize,
    ip: IpAddr,
    dir: Option<std::path::PathBuf>,
    backend: animusd::StorageBackend,
    auto_split_bytes: Option<u64>,
    auto_split_change_rate: Option<u64>,
    auto_split_ops_rate: Option<u64>,
    orphan_sweep_after: Duration,
    dynamo_auth: Option<std::sync::Arc<BTreeMap<String, String>>>,
) -> Result<(), String> {
    if control_n == 0 || data_n == 0 {
        return Err("--cluster-control and --cluster-data must each be at least 1".into());
    }
    let dir = dir.unwrap_or_else(|| std::env::temp_dir().join("animusd"));
    let nodes = animusd::start_split_cluster_with_growth(
        control_n,
        data_n,
        &dir,
        ip,
        backend,
        auto_split_bytes,
        orphan_sweep_after,
        auto_split_change_rate,
        auto_split_ops_rate,
        dynamo_auth,
    )
    .await
    .map_err(|e| format!("failed to start split cluster: {e}"))?;

    match auto_split_bytes {
        Some(b) => println!(
            "animusd: started split cluster ({control_n} control + {data_n} data, CP) — auto-split at {b} bytes/tablet"
        ),
        None => {
            println!("animusd: started split cluster ({control_n} control + {data_n} data, CP)")
        }
    }
    if let Some(rate) = auto_split_change_rate {
        println!(
            "animusd: streamed-table auto-split ALSO fires above {rate} change-bytes/sec/tablet"
        );
    }
    if let Some(rate) = auto_split_ops_rate {
        println!("animusd: auto-split ALSO fires above {rate} write-ops/sec/tablet");
    }
    // The in-process `--cluster-control N --cluster-data M` dev convenience
    // has no `--tls-*` knob of its own — always plain HTTP.
    let scheme = scheme(false);
    for (i, node) in nodes.iter().take(control_n).enumerate() {
        println!(
            "  control node {i}: client {} — admin {scheme}://{}",
            node.client_addr(),
            node.admin_addr(),
        );
    }
    for (i, node) in nodes.iter().skip(control_n).enumerate() {
        println!(
            "  data node {i}: client {} — dynamo {scheme} {} — admin {scheme}://{} — console {scheme}://{}",
            node.client_addr(),
            node.dynamo_addr(),
            node.admin_addr(),
            node.console_addr(),
        );
    }
    println!("animusd: ready — Ctrl-C to stop");
    wait_for_ctrl_c().await;
    for node in &nodes {
        node.shutdown_graceful().await;
    }
    Ok(())
}

/// Waits for either Ctrl-C (SIGINT, an interactive stop) or SIGTERM (a
/// Kubernetes pod's `preStop`/termination signal) before every call site runs
/// its own `shutdown_graceful()`. Without the SIGTERM half, a pod eviction
/// never reaches graceful shutdown at all — Kubernetes sends SIGTERM, not
/// SIGINT, on pod termination.
#[cfg(unix)]
async fn wait_for_ctrl_c() {
    use tokio::signal::unix::{SignalKind, signal};
    // A failure to install the SIGTERM handler is not fatal — Ctrl-C alone
    // still works — but is worth a loud warning since it silently drops the
    // pod-termination path this exists for.
    let mut sigterm = signal(SignalKind::terminate())
        .inspect_err(|e| tracing::warn!(?e, "failed to install a SIGTERM handler"))
        .ok();
    tokio::select! {
        res = tokio::signal::ctrl_c() => {
            if let Err(e) = res {
                tracing::warn!(?e, "failed to listen for Ctrl-C");
            }
        }
        _ = async {
            match &mut sigterm {
                Some(s) => { s.recv().await; }
                None => std::future::pending::<()>().await,
            }
        } => {}
    }
    println!("animusd: shutting down");
}

/// Non-unix fallback: SIGTERM has no portable equivalent outside unix, so
/// this waits on Ctrl-C alone (this workspace is linux-first — see the crate
/// guide — this branch exists only so the crate still builds elsewhere).
#[cfg(not(unix))]
async fn wait_for_ctrl_c() {
    if let Err(e) = tokio::signal::ctrl_c().await {
        tracing::warn!(?e, "failed to listen for Ctrl-C");
    }
    println!("animusd: shutting down");
}

fn parse_next<T: std::str::FromStr>(
    it: &mut std::slice::Iter<'_, String>,
    flag: &str,
) -> Result<T, String> {
    let raw = it.next().ok_or_else(|| format!("{flag} needs a value"))?;
    raw.parse()
        .map_err(|_| format!("invalid value for {flag}: `{raw}`"))
}

/// `--orphan-sweep-after SECS` (ADR 0040 PR6): `None` (the flag omitted) keeps
/// `animus_control::node::DEFAULT_ORPHAN_SWEEP_AFTER` (10 minutes);
/// `Some(0)` disables the sweep entirely, matching
/// `RaftNode::start_with_orphan_sweep_after`'s own `Duration::ZERO` contract.
fn orphan_sweep_after_duration(secs: Option<u64>) -> Duration {
    secs.map_or(
        animus_control::node::DEFAULT_ORPHAN_SWEEP_AFTER,
        Duration::from_secs,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- `--backup-store` (ADR 0059 §1) ---------------------------------

    #[test]
    fn backup_store_omitted_defaults_to_cluster() {
        assert_eq!(
            parse_backup_store(None).expect("parses"),
            animusd::BackupStoreConfig::Cluster
        );
    }

    #[test]
    fn backup_store_accepts_the_literal_cluster_keyword() {
        assert_eq!(
            parse_backup_store(Some("cluster")).expect("parses"),
            animusd::BackupStoreConfig::Cluster
        );
    }

    #[test]
    fn backup_store_accepts_fs_path() {
        assert_eq!(
            parse_backup_store(Some("fs:/var/lib/animus/backups")).expect("parses"),
            animusd::BackupStoreConfig::Fs("/var/lib/animus/backups".into())
        );
    }

    #[test]
    fn backup_store_rejects_an_empty_fs_path() {
        let err = parse_backup_store(Some("fs:")).expect_err("an empty path must be rejected");
        assert!(err.contains("--backup-store"), "{err}");
    }

    #[test]
    fn backup_store_rejects_garbage() {
        let err = parse_backup_store(Some("nonsense"))
            .expect_err("an unrecognized form must be rejected");
        assert!(err.contains("--backup-store"), "{err}");
        assert!(err.contains("nonsense"), "{err}");
    }

    #[test]
    fn backup_store_rejects_the_segment_store_dir_spelling() {
        // `--segment-store` spells its `Fs` form `dir:PATH`; `--backup-store`
        // deliberately spells it `fs:PATH` instead (and additionally accepts
        // the literal `cluster` keyword `--segment-store` has no equivalent
        // for) — the two knobs are NOT interchangeable syntax, even though
        // `BackupStoreConfig`/`SegmentStoreConfig` are shaped identically.
        let err = parse_backup_store(Some("dir:/tmp/x"))
            .expect_err("the streams knob's own `dir:PATH` spelling must not be accepted here");
        assert!(err.contains("--backup-store"), "{err}");
    }

    // --- `--segment-store` (ADR 0043 §A7b) — no prior direct coverage,
    // added alongside its new `--backup-store` sibling above so both knobs'
    // parsing has the same test shape.

    #[test]
    fn segment_store_omitted_defaults_to_cluster() {
        // `SegmentStoreConfig` derives no `PartialEq` (pre-existing, not
        // grown here) — `matches!` instead of `assert_eq!`.
        assert!(matches!(
            parse_segment_store(None).expect("parses"),
            animusd::SegmentStoreConfig::Cluster
        ));
    }

    #[test]
    fn segment_store_accepts_dir_path() {
        match parse_segment_store(Some("dir:/var/lib/animus/segments")).expect("parses") {
            animusd::SegmentStoreConfig::Fs(path) => {
                assert_eq!(path, std::path::PathBuf::from("/var/lib/animus/segments"));
            }
            other => panic!("expected Fs, got {other:?}"),
        }
    }

    #[test]
    fn segment_store_rejects_an_empty_dir_path() {
        let err = parse_segment_store(Some("dir:")).expect_err("an empty path must be rejected");
        assert!(err.contains("--segment-store"), "{err}");
    }

    #[test]
    fn segment_store_rejects_garbage() {
        let err = parse_segment_store(Some("nonsense"))
            .expect_err("an unrecognized form must be rejected");
        assert!(err.contains("--segment-store"), "{err}");
    }

    #[test]
    fn segment_store_rejects_the_backup_store_cluster_keyword() {
        // The converse of `backup_store_rejects_the_segment_store_dir_
        // spelling` above: `--segment-store` has no `cluster` keyword at
        // all (omitting the flag is its only way to select the default).
        let err = parse_segment_store(Some("cluster"))
            .expect_err("`--segment-store` has no `cluster` keyword");
        assert!(err.contains("--segment-store"), "{err}");
    }

    // --- `resolve_cluster_settings` (S-06) --------------------------------

    #[test]
    fn cluster_settings_absent_on_both_sides_resolves_to_default() {
        let cli = animusd::config::ClusterSettings::default();
        let effective = resolve_cluster_settings(None, &cli).expect("no conflict possible");
        assert_eq!(effective, animusd::config::ClusterSettings::default());
    }

    #[test]
    fn cluster_settings_cli_only_field_is_adopted() {
        let cli = animusd::config::ClusterSettings {
            auto_split_bytes: Some(1_000_000),
            ..Default::default()
        };
        let effective = resolve_cluster_settings(None, &cli).expect("no conflict");
        assert_eq!(effective.auto_split_bytes, Some(1_000_000));
    }

    #[test]
    fn cluster_settings_config_only_field_is_kept() {
        let config = animusd::config::ClusterSettings {
            quiesce_after_secs: Some(30),
            ..Default::default()
        };
        let cli = animusd::config::ClusterSettings::default();
        let effective = resolve_cluster_settings(Some(&config), &cli).expect("no conflict");
        assert_eq!(effective.quiesce_after_secs, Some(30));
    }

    #[test]
    fn cluster_settings_disjoint_fields_on_each_side_both_apply() {
        // An operator may set different knobs on the CLI vs. the config
        // file — only the *same* field on both sides is a conflict.
        let config = animusd::config::ClusterSettings {
            auto_split_bytes: Some(1_000_000),
            ..Default::default()
        };
        let cli = animusd::config::ClusterSettings {
            orphan_sweep_after_secs: Some(60),
            ..Default::default()
        };
        let effective =
            resolve_cluster_settings(Some(&config), &cli).expect("disjoint, no conflict");
        assert_eq!(effective.auto_split_bytes, Some(1_000_000));
        assert_eq!(effective.orphan_sweep_after_secs, Some(60));
    }

    #[test]
    fn cluster_settings_same_field_on_both_sides_is_a_hard_error() {
        let config = animusd::config::ClusterSettings {
            quiesce_after_secs: Some(30),
            ..Default::default()
        };
        let cli = animusd::config::ClusterSettings {
            quiesce_after_secs: Some(5),
            ..Default::default()
        };
        let err = resolve_cluster_settings(Some(&config), &cli)
            .expect_err("the same field set on both sides must be rejected");
        assert!(err.contains("quiesce_after_secs"), "{err}");
        assert!(err.contains("--quiesce-after"), "{err}");
        assert!(err.contains("one way, not both"), "{err}");
    }

    #[test]
    fn cluster_settings_ops_rate_field_on_both_sides_is_a_hard_error() {
        // W-09: the new field gets the identical merge/conflict treatment
        // as every other `cluster_settings` knob.
        let config = animusd::config::ClusterSettings {
            auto_split_ops_rate: Some(200),
            ..Default::default()
        };
        let cli = animusd::config::ClusterSettings {
            auto_split_ops_rate: Some(50),
            ..Default::default()
        };
        let err = resolve_cluster_settings(Some(&config), &cli)
            .expect_err("the same field set on both sides must be rejected");
        assert!(err.contains("auto_split_ops_rate"), "{err}");
        assert!(err.contains("--auto-split-ops-rate"), "{err}");
        assert!(err.contains("one way, not both"), "{err}");
    }

    #[test]
    fn cluster_settings_throttle_read_units_on_both_sides_is_a_hard_error() {
        // ADR 0065 §5(a), W-08 step 4: the same merge/conflict treatment as
        // every other `cluster_settings` knob.
        let config = animusd::config::ClusterSettings {
            throttle_read_units: Some(100),
            ..Default::default()
        };
        let cli = animusd::config::ClusterSettings {
            throttle_read_units: Some(50),
            ..Default::default()
        };
        let err = resolve_cluster_settings(Some(&config), &cli)
            .expect_err("the same field set on both sides must be rejected");
        assert!(err.contains("throttle_read_units"), "{err}");
        assert!(err.contains("--throttle-read-units"), "{err}");
        assert!(err.contains("one way, not both"), "{err}");
    }

    #[test]
    fn cluster_settings_throttle_write_units_on_both_sides_is_a_hard_error() {
        let config = animusd::config::ClusterSettings {
            throttle_write_units: Some(100),
            ..Default::default()
        };
        let cli = animusd::config::ClusterSettings {
            throttle_write_units: Some(50),
            ..Default::default()
        };
        let err = resolve_cluster_settings(Some(&config), &cli)
            .expect_err("the same field set on both sides must be rejected");
        assert!(err.contains("throttle_write_units"), "{err}");
        assert!(err.contains("--throttle-write-units"), "{err}");
        assert!(err.contains("one way, not both"), "{err}");
    }

    #[test]
    fn cluster_settings_conflict_check_is_per_field_not_whole_section() {
        // A conflict on one field must not be masked or triggered by an
        // unrelated field also being set on one side only.
        let config = animusd::config::ClusterSettings {
            auto_split_bytes: Some(1_000_000),
            quiesce_after_secs: Some(30),
            ..Default::default()
        };
        let cli = animusd::config::ClusterSettings {
            quiesce_after_secs: Some(5),
            ..Default::default()
        };
        let err = resolve_cluster_settings(Some(&config), &cli)
            .expect_err("quiesce_after_secs conflicts even though auto_split_bytes doesn't");
        assert!(err.contains("quiesce_after_secs"), "{err}");
        assert!(!err.contains("auto_split_bytes"), "{err}");
    }

    // --- `--tls-cert`/`--tls-key`/`--tls-ca` (ADR 0064, S-01 commit 2) ----

    #[test]
    fn resolve_tls_flags_none_given_is_none() {
        assert_eq!(resolve_tls_flags(None, None, None).unwrap(), None);
    }

    #[test]
    fn resolve_tls_flags_all_three_given_builds_a_section() {
        let section = resolve_tls_flags(
            Some("cert.pem".to_string()),
            Some("key.pem".to_string()),
            Some("ca.pem".to_string()),
        )
        .expect("all three must parse")
        .expect("must be Some");
        assert_eq!(section.cert_path, std::path::PathBuf::from("cert.pem"));
        assert_eq!(section.key_path, std::path::PathBuf::from("key.pem"));
        assert_eq!(section.ca_path, Some(std::path::PathBuf::from("ca.pem")));
    }

    #[test]
    fn resolve_tls_flags_rejects_exactly_one_given() {
        let err = resolve_tls_flags(Some("cert.pem".to_string()), None, None)
            .expect_err("cert alone must be rejected");
        assert!(err.contains("--tls-cert"), "{err}");
        assert!(err.contains("--tls-key"), "{err}");
        assert!(err.contains("--tls-ca"), "{err}");
    }

    #[test]
    fn resolve_tls_flags_rejects_exactly_two_given() {
        let err = resolve_tls_flags(
            Some("cert.pem".to_string()),
            Some("key.pem".to_string()),
            None,
        )
        .expect_err("cert+key without ca must be rejected");
        assert!(err.contains("all three"), "{err}");
    }

    fn tls_flag_for_test(tag: &str) -> TlsSection {
        TlsSection {
            cert_path: format!("{tag}.cert.pem").into(),
            key_path: format!("{tag}.key.pem").into(),
            ca_path: Some("ca.pem".into()),
        }
    }

    #[test]
    fn apply_tls_flag_sets_the_named_nodes_entry_only() {
        let mut config = ClusterConfig::generate(2, "127.0.0.1".parse().unwrap(), 7000);
        apply_tls_flag(&mut config, 0, Some(tls_flag_for_test("n0"))).expect("applies cleanly");
        assert!(config.nodes[0].tls.is_some());
        assert!(config.nodes[1].tls.is_none(), "only node 0 was targeted");
    }

    #[test]
    fn apply_tls_flag_none_is_a_no_op() {
        let mut config = ClusterConfig::generate(1, "127.0.0.1".parse().unwrap(), 7000);
        apply_tls_flag(&mut config, 0, None).expect("None must be a no-op");
        assert!(config.nodes[0].tls.is_none());
    }

    #[test]
    fn apply_tls_flag_conflicts_with_a_config_supplied_section() {
        let mut config = ClusterConfig::generate(1, "127.0.0.1".parse().unwrap(), 7000);
        config.nodes[0].tls = Some(tls_flag_for_test("from-config"));
        let err = apply_tls_flag(&mut config, 0, Some(tls_flag_for_test("from-flag")))
            .expect_err("config file's own tls section and the flag must conflict");
        assert!(err.contains("node 0"), "{err}");
        assert!(err.contains("one way, not both"), "{err}");
    }

    #[test]
    fn apply_tls_flag_rejects_an_out_of_range_index() {
        let mut config = ClusterConfig::generate(1, "127.0.0.1".parse().unwrap(), 7000);
        let err = apply_tls_flag(&mut config, 5, Some(tls_flag_for_test("n5")))
            .expect_err("index 5 is out of range for a 1-node config");
        assert!(err.contains("out of range"), "{err}");
    }
}
