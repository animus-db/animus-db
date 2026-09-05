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

/// Mirrors `animusd::config::ClusterSettings`'s JSON shape field-for-field
/// (S-06) — cluster-wide operational knobs (auto-split, quiesce,
/// orphan-sweep, stream-seal) `animusd` now reads from a config file's own
/// `cluster_settings` section on every deployment shape, not just
/// `--cluster N`'s dev-only in-process CLI flags. This crate only ever
/// populates the two fields the CRD exposes today
/// (`auto_split_bytes`/`quiesce_after_secs`, see
/// [`build_cluster_config`]) — the rest stay `None`, `#[serde(skip_
/// serializing_if = "Option::is_none")]` so an unset field is simply
/// absent from the emitted JSON rather than a null, exactly like every
/// other optional field this mirror already has.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ClusterSettings {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_split_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_split_change_rate: Option<u64>,
    /// W-09 (ADR 0034 amendment): the request-rate sibling of
    /// `auto_split_change_rate`. Same precedent — no `AnimusClusterSpec`
    /// field exposes this yet, so this crate never populates it; it stays
    /// `None`/absent from the emitted JSON like every other CRD-unexposed
    /// knob in this mirror.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_split_ops_rate: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub orphan_sweep_after_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quiesce_after_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_seal_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_seal_age_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_retention_secs: Option<u64>,
}

impl ClusterSettings {
    /// Whether every field is unset — used to leave [`ClusterConfig::
    /// cluster_settings`] entirely absent from the generated JSON rather
    /// than emitting an empty `"cluster_settings": {}"` object when the
    /// spec sets none of the fields this crate populates.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self == &Self::default()
    }
}

/// Mirrors `animusd::config::ClusterConfig`'s JSON shape. `dynamo_auth` is
/// deliberately never populated by this crate — credentials are instead
/// supplied via the `--dynamo-auth PATH` flag against a mounted `Secret`
/// (see [`super::entrypoint_script`]'s doc): specifying both is a hard
/// `animusd` startup error, so leaving the config file's own section absent
/// (`#[serde(default)]` on the `animusd` side) is the only choice that
/// composes with that flag.
///
/// `cluster_settings` (S-06) mirrors `animusd::config::ClusterConfig`'s own
/// field of the same name — see [`ClusterSettings`]'s own doc.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub nodes: Vec<RoleAddrs>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_settings: Option<ClusterSettings>,
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

    // S-06: the two knobs the CRD exposes today map straight onto the
    // generated config's own `cluster_settings` section — this is what
    // makes both of them reach *every* pod (combined and data-role alike),
    // closing the `--quiesce-after`/`--auto-split-bytes` gaps
    // `AnimusClusterSpec`'s own field docs used to describe (both flags
    // only ever reached combined-role pods, or nothing at all). Left
    // entirely absent (not an empty `{}` object) when the spec sets
    // neither, so an unchanged spec's generated `cluster.json` stays
    // byte-identical to before this section existed.
    let cluster_settings = ClusterSettings {
        auto_split_bytes: spec.auto_split_bytes,
        quiesce_after_secs: spec.quiesce_after_secs,
        ..ClusterSettings::default()
    };
    let cluster_settings = if cluster_settings.is_empty() {
        None
    } else {
        Some(cluster_settings)
    };

    ClusterConfig {
        nodes,
        cluster_settings,
    }
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
///   `< control_nodes`): `--dir`, `--ephemeral`, `--dynamo-auth`.
/// - data role (`animusd data --config FILE --node I`, ordinals
///   `>= control_nodes`): `--dir`, `--ephemeral`, `--dynamo-auth`.
///
/// **No `--split-mode` flag is emitted on either branch**: the flag and the
/// copy-based split workflow it selected were deleted outright
/// (2026-09-01, ADR 0058's rung 4 layer) — `animusd`'s CLI parser no longer
/// accepts it on any subcommand at all. `AnimusClusterSpec.split_mode` (and
/// this function's matching emission) was removed for the same reason
/// (#590): it used to be emitted unconditionally on the combined branch,
/// which made any spec setting `splitMode` a live pod-startup failure.
///
/// **`spec.quiesceAfterSecs`/`spec.autoSplitBytes` are never emitted as
/// flags here on either branch (S-06)** — both now reach `animusd` through
/// [`build_cluster_config`]'s own `cluster_settings` section of the
/// generated `cluster.json` instead, which every pod (combined and
/// data-role alike) reads regardless of which `animusd` subcommand it
/// execs. Emitting `--quiesce-after` here too, on top of the config
/// section, would in fact be a **hard `animusd` startup error** on the
/// combined branch — its CLI flag and the config file's own section
/// setting the same field is refused, not silently reconciled (the same
/// "specify it one way, not both" contract `--dynamo-auth` already uses on
/// this crate's own side, see this file's own doc). This is also what
/// closes `auto_split_bytes`'s pre-S-06 gap: no `animusd` subcommand ever
/// accepted `--auto-split-bytes` as a flag, only `--cluster N`'s dev-only
/// in-process mode did.
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

    // --- `cluster_settings` (S-06) ---------------------------------------

    #[test]
    fn cluster_settings_section_is_absent_when_the_spec_sets_neither_field() {
        // The default spec's generated config carries no `cluster_settings`
        // key at all — not even an empty `{}` — so it round-trips against
        // an `animusd` build that predates the section too, and an
        // unchanged spec's generated `cluster.json` stays byte-identical to
        // before S-06.
        let cfg = build_cluster_config("c", "ns", &spec(3));
        assert!(cfg.cluster_settings.is_none());
        let value: serde_json::Value = serde_json::from_str(&to_json(&cfg)).unwrap();
        assert!(
            value.get("cluster_settings").is_none(),
            "expected no cluster_settings key, got {value}"
        );
    }

    #[test]
    fn cluster_settings_ops_rate_field_is_never_populated_by_this_crate() {
        // W-09: no `AnimusClusterSpec` field exposes `auto_split_ops_rate`
        // yet (mirroring `auto_split_change_rate`'s own precedent), so it
        // stays `None` and never appears in the emitted JSON even when the
        // section is otherwise non-empty.
        let mut s = spec(3);
        s.auto_split_bytes = Some(1);
        let cfg = build_cluster_config("c", "ns", &s);
        let settings = cfg
            .cluster_settings
            .clone()
            .expect("auto_split_bytes alone is enough for the section to appear");
        assert_eq!(settings.auto_split_ops_rate, None);
        let value: serde_json::Value = serde_json::from_str(&to_json(&cfg)).unwrap();
        assert!(
            value["cluster_settings"]
                .get("auto_split_ops_rate")
                .is_none(),
            "expected no auto_split_ops_rate key, got {value}"
        );
    }

    #[test]
    fn cluster_settings_section_reflects_auto_split_and_quiesce() {
        let mut s = spec(3);
        s.auto_split_bytes = Some(50_000_000);
        s.quiesce_after_secs = Some(10);
        let cfg = build_cluster_config("c", "ns", &s);
        let value: serde_json::Value = serde_json::from_str(&to_json(&cfg)).unwrap();
        assert_eq!(
            value["cluster_settings"],
            serde_json::json!({
                "auto_split_bytes": 50_000_000,
                "quiesce_after_secs": 10
            }),
            "got {value}"
        );
    }

    #[test]
    fn cluster_settings_section_reflects_only_the_field_the_spec_sets() {
        let mut s = spec(3);
        s.quiesce_after_secs = Some(5);
        let cfg = build_cluster_config("c", "ns", &s);
        let settings = cfg
            .cluster_settings
            .expect("one field set is enough for the section to appear");
        assert_eq!(settings.quiesce_after_secs, Some(5));
        assert_eq!(settings.auto_split_bytes, None);
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
        // S-06: reaches `animusd` through the config file's own
        // `cluster_settings.auto_split_bytes` section instead — see
        // `cluster_settings_section_reflects_auto_split_and_quiesce` below.
        let mut s = spec(3);
        s.auto_split_bytes = Some(1_000_000);
        let script = entrypoint_script(&s);
        assert!(!script.contains("auto-split-bytes"));
    }

    #[test]
    fn entrypoint_never_emits_quiesce_after() {
        // S-06: like `auto_split_bytes` above, `--quiesce-after` moved from
        // a CLI flag (combined-role pods only) to the config file's own
        // `cluster_settings.quiesce_after_secs` section, which every pod
        // reads regardless of role — emitting it here too would in fact be
        // a hard `animusd` startup error on the combined branch (the same
        // field set both ways).
        let mut s = spec(3);
        s.quiesce_after_secs = Some(7);
        let script = entrypoint_script(&s);
        assert!(!script.contains("quiesce-after"));
    }

    #[test]
    fn entrypoint_never_emits_split_mode() {
        // #590: `--split-mode` and the copy-based split workflow it
        // selected were deleted from `animusd` outright (2026-09-01, ADR
        // 0058's rung 4 layer) — `animusd`'s CLI parser rejects the flag
        // as unknown on every subcommand now. `AnimusClusterSpec` no
        // longer has a `split_mode` field at all, so there is nothing for
        // this test to set on `spec` — it just pins that the token can
        // never resurface in either branch of the generated script.
        let mut s = spec(4);
        s.control_nodes = Some(2);
        let script = entrypoint_script(&s);
        assert!(!script.contains("--split-mode"));
        assert!(!script.contains("split-mode"));
    }

    /// The exhaustive set of `--flag` tokens `animusd`'s hand-rolled CLI
    /// parser accepts on the two subcommands `entrypoint_script` can ever
    /// exec (`crates/animusd/src/main.rs`, no `clap` derive — flags are
    /// matched as literal strings in each subcommand's own `while let
    /// Some(arg) = it.next()` loop):
    /// - combined role (`run`, the bare `animusd --config FILE --node I`
    ///   form, ~L369-413 as of this writing): `--config`, `--node`,
    ///   `--cluster`, `--cluster-control`, `--cluster-data`, `--dir`,
    ///   `--ip`, `--ephemeral`, `--auto-split-bytes`,
    ///   `--auto-split-change-rate`, `--orphan-sweep-after`,
    ///   `--stream-seal-bytes`, `--stream-seal-age`, `--stream-retention`,
    ///   `--segment-store`, `--backup-store`, `--quiesce-after`,
    ///   `--dynamo-auth`, `--advertise-host`.
    /// - data role (`run_data`, `animusd data ...`, ~L961-994): `--config`,
    ///   `--node`, `--dir`, `--ephemeral`, `--seed`, `--id`, `--ip`,
    ///   `--base-port`, `--dynamo-auth`, `--advertise-host`.
    ///
    /// `--split-mode` is deliberately absent from both — see main.rs's own
    /// module doc: it and the copy-based split workflow it selected were
    /// deleted outright (2026-09-01, ADR 0058's rung 4 layer). See also
    /// `crates/animus-operator/CLAUDE.md`'s CLI-flag-support table.
    const ANIMUSD_ACCEPTED_FLAGS: &[&str] = &[
        "--config",
        "--node",
        "--cluster",
        "--cluster-control",
        "--cluster-data",
        "--dir",
        "--ip",
        "--ephemeral",
        "--auto-split-bytes",
        "--auto-split-change-rate",
        "--orphan-sweep-after",
        "--stream-seal-bytes",
        "--stream-seal-age",
        "--stream-retention",
        "--segment-store",
        "--backup-store",
        "--quiesce-after",
        "--dynamo-auth",
        "--advertise-host",
        "--seed",
        "--id",
        "--base-port",
    ];

    #[test]
    fn entrypoint_flags_are_all_accepted_by_animusd() {
        // A representative spec with every optional flag-affecting field
        // set, on both the combined and data branches.
        let mut s = spec(4);
        s.control_nodes = Some(2);
        s.storage.ephemeral = Some(true);
        s.dynamo_auth_secret_name = Some("my-dynamo-creds".to_string());
        s.quiesce_after_secs = Some(7);
        s.auto_split_bytes = Some(1_000_000);
        let script = entrypoint_script(&s);

        let mut unknown = Vec::new();
        for line in script.lines() {
            for token in line.split_whitespace() {
                if token.starts_with("--") && !ANIMUSD_ACCEPTED_FLAGS.contains(&token) {
                    unknown.push(token.to_string());
                }
            }
        }
        assert!(
            unknown.is_empty(),
            "entrypoint script emitted flag(s) `animusd` does not accept: {unknown:?}\n\
             script:\n{script}"
        );
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
