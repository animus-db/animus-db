//! A mirror of `animusd::config::{ClusterConfig, RoleAddrs, NodeRole}`'s
//! serde JSON shape (see `crates/animusd/src/config.rs` and
//! `crates/animusd/src/lib.rs::RoleAddrs`), plus the `entrypoint.sh` that
//! dispatches each pod ordinal to the right `animusd` invocation.
//!
//! This crate deliberately does **not** depend on the `animusd` crate: it
//! only has to *emit JSON `animusd` can parse*, generated at a container's
//! entrypoint time from a fixed pod ordinal, so a hand-written mirror avoids
//! pulling in the whole node-server dependency tree for a build-time-only
//! JSON shape. Keeping this shape byte-field-compatible with `animusd`'s own
//! `ClusterConfig`/`RoleAddrs` is a manual invariant — see this crate's own
//! `CLAUDE.md` for the gotcha.
//!
//! **Port stride, Kubernetes-specific**: `animusd::config::ClusterConfig::
//! generate` stripes ports across nodes (`base_port + 6*i + offset`) because
//! its bare-metal/dev deployment shape can have several node processes
//! share one host IP. In Kubernetes every pod is its own network namespace
//! with its own DNS name, so there is no need to stripe — **every pod binds
//! the identical six ports** (`base_port + offset`), and pods are
//! distinguished by `advertise_host` (their own stable per-pod DNS name)
//! instead. This is what lets one `Service` per role-port apply uniformly
//! across every pod's identical `targetPort`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::crd::AnimusClusterSpec;

/// Port offsets from `base_port` (ADR 0047 stride, current post-ADR-0053
/// shape — six ports, no `cql`): `internal:0, client:1, dynamo:2, admin:3,
/// intra:4, console:5`.
pub const PORT_INTERNAL: i32 = 0;
pub const PORT_CLIENT: i32 = 1;
pub const PORT_DYNAMO: i32 = 2;
pub const PORT_ADMIN: i32 = 3;
pub const PORT_INTRA: i32 = 4;
pub const PORT_CONSOLE: i32 = 5;

/// Mirrors `animusd::config::NodeRole` — `#[serde(rename_all =
/// "lowercase")]`, `Both` is that type's `#[default]`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeRole {
    Control,
    Data,
    Both,
}

/// Mirrors `animusd::lib::RoleAddrs`'s JSON shape field-for-field
/// (`internal`/`client`/`intra`/`dynamo`/`admin`/`console` are plain
/// `"host:port"` strings — `SocketAddr`'s own serde encoding — and
/// `advertise_host` is the ADR 0060 groundwork field: `Option<String>`,
/// `#[serde(default)]` on the `animusd` side, so an operator-generated
/// config that always sets it round-trips unchanged against an `animusd`
/// build that predates the field too).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RoleAddrs {
    pub id: String,
    pub role: NodeRole,
    pub internal: String,
    pub client: String,
    pub intra: String,
    pub dynamo: String,
    pub admin: String,
    pub console: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub advertise_host: Option<String>,
}

/// Mirrors `animusd::config::ClusterConfig`'s JSON shape. `dynamo_auth` is
/// deliberately never populated by this crate — credentials are instead
/// supplied via the `--dynamo-auth PATH` flag against a mounted `Secret`
/// (see [`super::entrypoint_script`]'s doc): specifying both is a hard
/// `animusd` startup error, so leaving the config file's own section absent
/// (`#[serde(default)]` on the `animusd` side) is the only choice that
/// composes with that flag.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub nodes: Vec<RoleAddrs>,
}

/// The pod id `animusd` sees for ordinal `i` of cluster `name`: `"{name}-{i}"`.
/// A `NodeId` accepts `[A-Za-z0-9._-]{1,64}` (`animus_env::NodeId::propose`),
/// which a Kubernetes object name (RFC 1123 label / DNS subdomain) plus a
/// dash and a small integer always satisfies.
#[must_use]
pub fn node_id(cluster_name: &str, i: i32) -> String {
    format!("{cluster_name}-{i}")
}

/// Build the [`ClusterConfig`] for cluster `name` in namespace `ns`: one
/// [`RoleAddrs`] entry per pod ordinal `0..spec.nodes`, ordinals
/// `0..control_nodes` role [`NodeRole::Both`], the rest [`NodeRole::Data`].
/// Every entry binds the identical six ports (see this module's own doc for
/// why no per-node stride is needed in Kubernetes) and advertises its own
/// stable per-pod DNS name.
#[must_use]
pub fn build_cluster_config(name: &str, ns: &str, spec: &AnimusClusterSpec) -> ClusterConfig {
    let base_port = spec.base_port_or_default();
    let control_nodes = spec.control_nodes_or_default();
    let bind = |offset: i32| format!("0.0.0.0:{}", base_port + offset);

    let nodes = (0..spec.nodes)
        .map(|i| RoleAddrs {
            id: node_id(name, i),
            role: if i < control_nodes {
                NodeRole::Both
            } else {
                NodeRole::Data
            },
            internal: bind(PORT_INTERNAL),
            client: bind(PORT_CLIENT),
            intra: bind(PORT_INTRA),
            dynamo: bind(PORT_DYNAMO),
            admin: bind(PORT_ADMIN),
            console: bind(PORT_CONSOLE),
            advertise_host: Some(super::pod_fqdn(name, ns, i)),
        })
        .collect();

    ClusterConfig { nodes }
}

/// Serialize `config` to pretty JSON, the same `serde_json::to_string_pretty`
/// shape `animusd::config::ClusterConfig::to_json` produces (byte-for-byte
/// pretty-printing does not have to match — `animusd` re-parses this as
/// JSON, not text — but pretty output keeps the `ConfigMap` humane to read
/// with `kubectl get cm -o yaml`).
///
/// # Panics
/// Never in practice: every field here is plain serializable data (no
/// floats/maps with non-string keys/etc).
#[must_use]
pub fn to_json(config: &ClusterConfig) -> String {
    serde_json::to_string_pretty(config).expect("ClusterConfig serializes")
}

/// The absolute in-container path the cluster `ConfigMap` is mounted at.
pub const CONFIG_MOUNT_DIR: &str = "/etc/animus";
/// The cluster config file's name within [`CONFIG_MOUNT_DIR`].
pub const CONFIG_FILE_NAME: &str = "cluster.json";
/// The entrypoint script's name within [`CONFIG_MOUNT_DIR`].
pub const ENTRYPOINT_FILE_NAME: &str = "entrypoint.sh";
/// The data directory every container's `animusd --dir` points at — the
/// `VOLUME`/data-dir convention the `animusd` image documents.
pub const DATA_DIR: &str = "/var/lib/animus";
/// The absolute in-container path the `dynamoAuthSecretName` `Secret` (when
/// set) is mounted at, read-only.
pub const DYNAMO_AUTH_MOUNT_DIR: &str = "/etc/animus/dynamo-auth";
/// The credentials file's name within [`DYNAMO_AUTH_MOUNT_DIR`] — the shape
/// `animusd::DynamoAuthConfig` parses (`{"credentials": {"AKID...":
/// "secret...", ...}}`).
pub const DYNAMO_AUTH_FILE_NAME: &str = "credentials.json";

/// Build the `entrypoint.sh` script every pod runs (`["/bin/sh",
/// "/etc/animus/entrypoint.sh"]`): a POSIX `sh` script that derives its own
/// ordinal from `${HOSTNAME##*-}` (a `StatefulSet` pod's hostname is always
/// `{name}-{ordinal}`) and execs the right `animusd` invocation for its
/// role — the `control_nodes` threshold is **baked in at generation time**
/// (a plain shell integer comparison), not read back out of the mounted
/// config.
///
/// **Only appends a tuning flag when the target subcommand actually accepts
/// it** (verified against `crates/animusd/src/main.rs`'s own CLI parser,
/// not just its usage-string doc comment — see this crate's `CLAUDE.md` for
/// the full support table):
/// - combined role (`animusd --config FILE --node I`, ordinals
///   `< control_nodes`): `--dir`, `--ephemeral`, `--quiesce-after`,
///   `--split-mode`, `--dynamo-auth`.
/// - data role (`animusd data --config FILE --node I`, ordinals
///   `>= control_nodes`): `--dir`, `--ephemeral`, `--dynamo-auth` only —
///   **not** `--quiesce-after`/`--split-mode` (rejected as unknown
///   arguments by that subcommand today).
///
/// `spec.autoSplitBytes` is **never** emitted as a flag here on either
/// branch: neither `--config/--node` invocation accepts
/// `--auto-split-bytes` at all (only the dev-only `--cluster N` in-process
/// mode does) — see [`AnimusClusterSpec::auto_split_bytes`]'s own doc for
/// why the spec field is kept anyway.
#[must_use]
pub fn entrypoint_script(spec: &AnimusClusterSpec) -> String {
    let control_nodes = spec.control_nodes_or_default();
    let ephemeral = spec.storage.is_ephemeral();

    let mut both_flags = String::new();
    let mut data_flags = String::new();

    if ephemeral {
        both_flags.push_str(" --ephemeral");
        data_flags.push_str(" --ephemeral");
    }
    if let Some(secs) = spec.quiesce_after_secs {
        both_flags.push_str(&format!(" --quiesce-after {secs}"));
    }
    if let Some(mode) = &spec.split_mode {
        both_flags.push_str(&format!(" --split-mode {mode}"));
    }
    if spec.dynamo_auth_secret_name.is_some() {
        let flag = format!(" --dynamo-auth {DYNAMO_AUTH_MOUNT_DIR}/{DYNAMO_AUTH_FILE_NAME}");
        both_flags.push_str(&flag);
        data_flags.push_str(&flag);
    }

    format!(
        "#!/bin/sh\n\
         set -eu\n\
         # Generated by animus-operator — do not edit; changes are\n\
         # overwritten on the next reconcile.\n\
         ord=\"${{HOSTNAME##*-}}\"\n\
         cfg=\"{CONFIG_MOUNT_DIR}/{CONFIG_FILE_NAME}\"\n\
         if [ \"$ord\" -lt {control_nodes} ]; then\n\
         \x20\x20exec animusd --config \"$cfg\" --node \"$ord\" --dir {DATA_DIR}{both_flags}\n\
         else\n\
         \x20\x20exec animusd data --config \"$cfg\" --node \"$ord\" --dir {DATA_DIR}{data_flags}\n\
         fi\n"
    )
}

/// The `dynamo_auth` field is intentionally absent from [`ClusterConfig`]
/// (see its own doc); this alias documents that a caller building a
/// `Secret`-mounted credentials file must produce this exact shape.
pub type DynamoAuthCredentials = BTreeMap<String, String>;

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(nodes: i32) -> AnimusClusterSpec {
        AnimusClusterSpec {
            nodes,
            ..Default::default()
        }
    }

    #[test]
    fn three_node_golden_config() {
        let cfg = build_cluster_config("c", "ns", &spec(3));
        let value: serde_json::Value = serde_json::from_str(&to_json(&cfg)).unwrap();
        let expected = serde_json::json!({
            "nodes": [
                {
                    "id": "c-0",
                    "role": "both",
                    "internal": "0.0.0.0:14000",
                    "client": "0.0.0.0:14001",
                    "intra": "0.0.0.0:14004",
                    "dynamo": "0.0.0.0:14002",
                    "admin": "0.0.0.0:14003",
                    "console": "0.0.0.0:14005",
                    "advertise_host": "c-0.c-internal.ns.svc.cluster.local"
                },
                {
                    "id": "c-1",
                    "role": "both",
                    "internal": "0.0.0.0:14000",
                    "client": "0.0.0.0:14001",
                    "intra": "0.0.0.0:14004",
                    "dynamo": "0.0.0.0:14002",
                    "admin": "0.0.0.0:14003",
                    "console": "0.0.0.0:14005",
                    "advertise_host": "c-1.c-internal.ns.svc.cluster.local"
                },
                {
                    "id": "c-2",
                    "role": "both",
                    "internal": "0.0.0.0:14000",
                    "client": "0.0.0.0:14001",
                    "intra": "0.0.0.0:14004",
                    "dynamo": "0.0.0.0:14002",
                    "admin": "0.0.0.0:14003",
                    "console": "0.0.0.0:14005",
                    "advertise_host": "c-2.c-internal.ns.svc.cluster.local"
                }
            ]
        });
        assert_eq!(value, expected);
    }

    #[test]
    fn mixed_role_split_at_control_nodes() {
        let mut s = spec(5);
        s.control_nodes = Some(2);
        let cfg = build_cluster_config("c", "ns", &s);
        let roles: Vec<NodeRole> = cfg.nodes.iter().map(|n| n.role).collect();
        assert_eq!(
            roles,
            vec![
                NodeRole::Both,
                NodeRole::Both,
                NodeRole::Data,
                NodeRole::Data,
                NodeRole::Data,
            ]
        );
    }

    #[test]
    fn every_pod_binds_the_identical_ports_no_stride() {
        // Kubernetes-specific: unlike animusd's own bare-metal `generate`,
        // there must be no `i`-dependent port offset.
        let cfg = build_cluster_config("c", "ns", &spec(4));
        for n in &cfg.nodes {
            assert_eq!(n.internal, "0.0.0.0:14000");
            assert_eq!(n.client, "0.0.0.0:14001");
            assert_eq!(n.dynamo, "0.0.0.0:14002");
            assert_eq!(n.admin, "0.0.0.0:14003");
            assert_eq!(n.intra, "0.0.0.0:14004");
            assert_eq!(n.console, "0.0.0.0:14005");
        }
    }

    #[test]
    fn base_port_shifts_every_port_uniformly() {
        let mut s = spec(1);
        s.base_port = Some(20000);
        let cfg = build_cluster_config("c", "ns", &s);
        assert_eq!(cfg.nodes[0].internal, "0.0.0.0:20000");
        assert_eq!(cfg.nodes[0].console, "0.0.0.0:20005");
    }

    #[test]
    fn advertise_host_is_the_stable_pod_fqdn() {
        let cfg = build_cluster_config("my-cluster", "prod", &spec(2));
        assert_eq!(
            cfg.nodes[0].advertise_host.as_deref(),
            Some("my-cluster-0.my-cluster-internal.prod.svc.cluster.local")
        );
        assert_eq!(
            cfg.nodes[1].advertise_host.as_deref(),
            Some("my-cluster-1.my-cluster-internal.prod.svc.cluster.local")
        );
    }

    #[test]
    fn scale_up_config_append_preserves_existing_entries_byte_for_byte() {
        // Regenerating the config at a larger `nodes` must not perturb any
        // already-existing entry's id/role/ports/advertise_host — only
        // append new ones. This is what makes a scale-up a safe rolling
        // ConfigMap update: growth `RoleAddrs::propose`s a *new* id per
        // `animusd`'s own registration CAS, but it must be the *same* new
        // id/entry shape every time this function is called for a given
        // ordinal, which this test pins.
        let small = build_cluster_config("c", "ns", &spec(3));
        let grown = build_cluster_config("c", "ns", &spec(5));
        assert_eq!(&grown.nodes[..3], &small.nodes[..]);
        assert_eq!(grown.nodes.len(), 5);
        assert_eq!(grown.nodes[3].id, "c-3");
        assert_eq!(grown.nodes[4].id, "c-4");
    }

    #[test]
    fn scale_up_preserves_existing_roles_even_across_control_nodes_boundary() {
        // A node's role is purely `ordinal < control_nodes` — since
        // `control_nodes` is immutable (enforced by the controller, not
        // this pure function), growing `nodes` alone can never flip an
        // existing pod's role.
        let mut s3 = spec(3);
        s3.control_nodes = Some(2);
        let mut s6 = spec(6);
        s6.control_nodes = Some(2);
        let small = build_cluster_config("c", "ns", &s3);
        let grown = build_cluster_config("c", "ns", &s6);
        for (a, b) in small.nodes.iter().zip(grown.nodes.iter()) {
            assert_eq!(a, b);
        }
    }

    #[test]
    fn entrypoint_splits_both_vs_data_subcommand_at_control_nodes() {
        let mut s = spec(4);
        s.control_nodes = Some(2);
        let script = entrypoint_script(&s);
        assert!(script.contains("if [ \"$ord\" -lt 2 ]; then"));
        assert!(
            script.contains("exec animusd --config \"$cfg\" --node \"$ord\" --dir /var/lib/animus")
        );
        assert!(
            script.contains(
                "exec animusd data --config \"$cfg\" --node \"$ord\" --dir /var/lib/animus"
            )
        );
    }

    #[test]
    fn entrypoint_never_emits_auto_split_bytes() {
        let mut s = spec(3);
        s.auto_split_bytes = Some(1_000_000);
        let script = entrypoint_script(&s);
        assert!(!script.contains("auto-split-bytes"));
    }

    #[test]
    fn entrypoint_omits_quiesce_and_split_mode_on_data_branch_only() {
        let mut s = spec(4);
        s.control_nodes = Some(2);
        s.quiesce_after_secs = Some(7);
        s.split_mode = Some("inplace".to_string());
        let script = entrypoint_script(&s);
        // Split the script at the `else` to inspect each branch in isolation.
        let (both_branch, data_branch) = script.split_once("else").unwrap();
        assert!(both_branch.contains("--quiesce-after 7"));
        assert!(both_branch.contains("--split-mode inplace"));
        assert!(!data_branch.contains("--quiesce-after"));
        assert!(!data_branch.contains("--split-mode"));
    }

    #[test]
    fn entrypoint_ephemeral_flag_on_both_branches() {
        let mut s = spec(3);
        s.storage.ephemeral = Some(true);
        let script = entrypoint_script(&s);
        let (both_branch, data_branch) = script.split_once("else").unwrap();
        assert!(both_branch.contains("--ephemeral"));
        assert!(data_branch.contains("--ephemeral"));
    }

    #[test]
    fn entrypoint_dynamo_auth_flag_on_both_branches_when_secret_set() {
        let mut s = spec(3);
        s.dynamo_auth_secret_name = Some("my-dynamo-creds".to_string());
        let script = entrypoint_script(&s);
        let (both_branch, data_branch) = script.split_once("else").unwrap();
        let expected = " --dynamo-auth /etc/animus/dynamo-auth/credentials.json";
        assert!(both_branch.contains(expected));
        assert!(data_branch.contains(expected));
    }

    #[test]
    fn entrypoint_omits_dynamo_auth_flag_when_secret_absent() {
        let script = entrypoint_script(&spec(3));
        assert!(!script.contains("--dynamo-auth"));
    }

    #[test]
    fn entrypoint_is_valid_posix_sh_shape() {
        let script = entrypoint_script(&spec(3));
        assert!(script.starts_with("#!/bin/sh\n"));
        assert!(script.contains("set -eu"));
        assert!(script.contains("HOSTNAME##*-"));
    }
}
