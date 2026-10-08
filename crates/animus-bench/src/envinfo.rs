//! Environment and topology capture for the results file.
//!
//! Host facts come from `/proc` and `git`; cluster facts come from each
//! node's `/admin` (`/admin/config`, `/admin/status`, `/admin/raftkv`).
//! A fact `/admin` does not report is recorded as such — **never guessed**.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::client::admin_get;
use crate::cluster::Cluster;

/// The machine the load generator ran on.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostInfo {
    pub hostname: String,
    pub kernel: String,
    pub cpu_model: String,
    pub cpu_count: usize,
    pub memory_total_kb: Option<u64>,
}

fn read_trim(path: &str) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
}

impl HostInfo {
    /// Capture from `/proc`; fields that cannot be read are `"unknown"`.
    #[must_use]
    pub fn capture() -> Self {
        let cpuinfo = std::fs::read_to_string("/proc/cpuinfo").unwrap_or_default();
        let cpu_model = cpuinfo
            .lines()
            .find_map(|l| {
                let (k, v) = l.split_once(':')?;
                (k.trim() == "model name" || k.trim() == "Model").then(|| v.trim().to_owned())
            })
            .unwrap_or_else(|| "unknown".to_owned());
        let memory_total_kb = std::fs::read_to_string("/proc/meminfo").ok().and_then(|m| {
            m.lines()
                .find_map(|l| l.strip_prefix("MemTotal:"))
                .and_then(|v| v.trim().trim_end_matches("kB").trim().parse().ok())
        });
        Self {
            hostname: read_trim("/proc/sys/kernel/hostname").unwrap_or_else(|| "unknown".into()),
            kernel: read_trim("/proc/sys/kernel/osrelease").unwrap_or_else(|| "unknown".into()),
            cpu_model,
            cpu_count: std::thread::available_parallelism().map_or(0, std::num::NonZero::get),
            memory_total_kb,
        }
    }
}

/// `(git sha, dirty?)` of the working directory's repo, or the
/// `ANIMUS_BENCH_GIT_SHA` override, else `("unknown", None)`.
#[must_use]
pub fn git_state() -> (String, Option<bool>) {
    let run = |args: &[&str]| {
        std::process::Command::new("git")
            .args(args)
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
    };
    let sha = std::env::var("ANIMUS_BENCH_GIT_SHA")
        .ok()
        .or_else(|| run(&["rev-parse", "HEAD"]))
        .unwrap_or_else(|| "unknown".to_owned());
    let dirty = run(&["status", "--porcelain"]).map(|s| !s.is_empty());
    (sha, dirty)
}

/// Snapshot what `/admin` says about the cluster. `table` (if any) selects
/// the tablets whose replica placement and leaders are printed.
///
/// Every field is either read from the cluster or stated as not reported.
pub async fn capture_topology(cluster: &Cluster, table: Option<&str>) -> Value {
    let mut nodes = Vec::new();
    let mut status: Option<Value> = None;
    for n in cluster.nodes() {
        match admin_get(n.admin, "/admin/config", cluster.tls()).await {
            Ok((200, cfg)) => {
                nodes.push(json!({
                    "index": n.index,
                    "dynamo": n.dynamo.to_string(),
                    "admin": n.admin.to_string(),
                    "reachable": true,
                    "node_id": cfg["node_id"],
                    "role": cfg["role"],
                    "auth_enabled": cfg["auth_enabled"],
                    "quiesce_after_ms": cfg["quiesce_after_ms"],
                    "auto_split_bytes_threshold": cfg["auto_split_bytes_threshold"],
                    "auto_split_ops_rate_threshold": cfg["auto_split_ops_rate_threshold"],
                    "throttle_read_units": cfg["throttle_read_units"],
                    "throttle_write_units": cfg["throttle_write_units"],
                }));
                if status.is_none()
                    && let Ok((200, s)) = admin_get(n.admin, "/admin/status", cluster.tls()).await
                {
                    status = Some(s);
                }
            }
            _ => nodes.push(json!({
                "index": n.index,
                "dynamo": n.dynamo.to_string(),
                "admin": n.admin.to_string(),
                "reachable": false,
            })),
        }
    }
    let mut out = json!({
        "node_count": cluster.nodes().len(),
        "nodes": nodes,
        "encryption_at_rest": if cluster.launched_here() {
            "disabled (the bench launches nodes without --encryption-key)"
        } else {
            "unknown: not reported by /admin (a node's encryption_key_path is not exposed)"
        },
        "shared_wal": if cluster.launched_here() {
            "animusd default (no --shared-wal/--no-shared-wal passed unless listed in launch args)"
        } else {
            "unknown: not reported by /admin"
        },
        "quiesce": "see nodes[].quiesce_after_ms (null = off or not applicable)",
        "tls": tls_description(cluster),
    });
    if let Some(s) = status {
        out["membership"] = s["members"].clone();
        if let Some(t) = table {
            let tablets: Vec<Value> = s["tablets"]
                .as_object()
                .map(|m| {
                    m.values()
                        .filter(|x| x["table"].as_str() == Some(t))
                        .cloned()
                        .collect()
                })
                .unwrap_or_default();
            let mut rfs: Vec<u64> = Vec::new();
            let mut placement = Vec::new();
            for tb in &tablets {
                let id = tb["id"].as_u64();
                if let Some(rf) =
                    id.and_then(|i| s["policies"][i.to_string()]["replication_factor"].as_u64())
                {
                    rfs.push(rf);
                }
                placement.push(json!({
                    "tablet": tb["id"],
                    "replicas": tb["replicas"],
                    "state": tb["state"],
                    "range": tb["range"],
                }));
            }
            let mut leaders = Vec::new();
            for n in cluster.nodes() {
                if let Ok((200, v)) = admin_get(n.admin, "/admin/raftkv", cluster.tls()).await {
                    for g in v["groups"].as_array().into_iter().flatten() {
                        if g["is_leader"].as_bool() == Some(true)
                            && tablets.iter().any(|tb| tb["id"] == g["tablet"])
                        {
                            leaders.push(json!({"tablet": g["tablet"], "node_index": n.index, "node": g["node"]}));
                        }
                    }
                }
            }
            rfs.sort_unstable();
            rfs.dedup();
            out["table"] = json!({
                "name": t,
                "tablet_count": tablets.len(),
                "replication_factor_policy": rfs,
                "observed_replicas_per_tablet": tablets
                    .iter()
                    .map(|tb| tb["replicas"].as_array().map_or(0, Vec::len))
                    .collect::<Vec<_>>(),
                "placement": placement,
                "leaders": leaders,
            });
        }
    } else {
        out["membership"] = Value::Null;
        out["note"] = json!("no node answered /admin/status");
    }
    out
}

fn tls_description(cluster: &Cluster) -> String {
    match cluster.tls() {
        None => "off: the admin and DynamoDB ports were dialled in plain TCP".to_owned(),
        Some(t) => format!(
            "server-only TLS (rustls, ring provider) on the admin and DynamoDB ports, verified \
             against the supplied CA, server name = {}; the client presents no certificate",
            t.server_name_note()
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_info_reports_something_sane() {
        let h = HostInfo::capture();
        assert!(h.cpu_count >= 1);
        assert!(!h.kernel.is_empty());
    }
}
