//! This node's own topology labels (G-01 stage G-a): the flag/file inputs
//! (`--label key=value`, `--labels-file PATH`, `--labels-file-annotations`,
//! `--labels-wait-secs N`) merged over a config entry's `labels`.
//!
//! Process-boundary startup code (real file I/O and a real-time bounded wait),
//! deliberately **not** one of the `#[deny(clippy::disallowed_methods)]`
//! client-path modules in `lib.rs`.
//!
//! # File formats
//!
//! One `key=value` per line; blank lines and `#` comments ignored; a value may
//! be unquoted or double-quoted (`key="value"`, with `\"`, `\\`, `\n`, `\t`
//! escapes) — the latter is what the Kubernetes downward API writes for
//! `metadata.labels`/`metadata.annotations`.
//!
//! # Annotation mode (the operator's path)
//!
//! The downward API cannot read *node* labels, so `animus-operator` resolves a
//! scheduled pod's node and patches the node's region/zone onto the pod as
//! annotations (ADR 0060's 2026-10-04 amendment); the pod projects
//! `metadata.annotations` to a file, which carries *every* annotation of the
//! pod. With `--labels-file-annotations` only the keys of [`ANNOTATION_LABELS`]
//! are kept, translated to their canonical label keys; everything else in the
//! file is ignored (otherwise unrelated annotations would become member
//! labels). The marker annotation [`RESOLVED_ANNOTATION`] says "the operator
//! has finished resolving this pod's node, even if the node had no topology
//! labels" and is what the startup wait polls for.

use std::collections::BTreeMap;
use std::time::Duration;

use animus_placement::{REGION_LABEL, ZONE_LABEL};

/// Annotation key (on the pod) → canonical member-label key.
pub const ANNOTATION_LABELS: [(&str, &str); 2] = [
    ("animus.io/topology-region", REGION_LABEL),
    ("animus.io/topology-zone", ZONE_LABEL),
];

/// Pod annotation the operator sets once it has resolved the pod's node,
/// whether or not the node carried any topology labels.
pub const RESOLVED_ANNOTATION: &str = "animus.io/topology-resolved";

/// Parse one `key=value` file body (see the module doc). Malformed lines (no
/// `=`, empty key, bad quoting) are an error naming the line.
///
/// # Errors
/// A message naming the offending 1-based line.
pub fn parse_labels_text(text: &str) -> Result<BTreeMap<String, String>, String> {
    let mut out = BTreeMap::new();
    for (i, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (k, v) = line
            .split_once('=')
            .ok_or_else(|| format!("line {}: expected key=value, got `{line}`", i + 1))?;
        let k = k.trim();
        if k.is_empty() {
            return Err(format!("line {}: empty key", i + 1));
        }
        let v = unquote(v.trim()).map_err(|e| format!("line {}: {e}", i + 1))?;
        out.insert(k.to_owned(), v);
    }
    Ok(out)
}

fn unquote(v: &str) -> Result<String, String> {
    let Some(rest) = v.strip_prefix('"') else {
        return Ok(v.to_owned());
    };
    let inner = rest
        .strip_suffix('"')
        .ok_or_else(|| format!("unterminated quoted value `{v}`"))?;
    let mut out = String::new();
    let mut chars = inner.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('"') => out.push('"'),
            Some('\\') => out.push('\\'),
            Some('n') => out.push('\n'),
            Some('t') => out.push('\t'),
            other => return Err(format!("bad escape `\\{}` in `{v}`", other.unwrap_or(' '))),
        }
    }
    Ok(out)
}

/// Project a parsed pod-annotations file onto member labels: keep only
/// [`ANNOTATION_LABELS`] keys (non-empty values), translated to the
/// canonical label keys.
#[must_use]
pub fn labels_from_annotations(annotations: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    ANNOTATION_LABELS
        .iter()
        .filter_map(|(ann, label)| {
            annotations
                .get(*ann)
                .filter(|v| !v.is_empty())
                .map(|v| ((*label).to_owned(), v.clone()))
        })
        .collect()
}

/// The parsed label-related command-line inputs of one startup subcommand.
#[derive(Clone, Debug, Default)]
pub struct LabelFlags {
    /// Repeated `--label key=value` (wins over file and config).
    pub labels: BTreeMap<String, String>,
    /// `--labels-file PATH`.
    pub file: Option<String>,
    /// `--labels-file-annotations`: treat the file as a pod-annotations
    /// projection (module doc).
    pub annotations: bool,
    /// `--labels-wait-secs N`: how long to wait for the file to be ready
    /// (`0`, the default, never waits).
    pub wait_secs: u64,
}

impl LabelFlags {
    /// Record one `--label key=value` argument.
    ///
    /// # Errors
    /// If `kv` has no `=` or an empty key.
    pub fn add_label(&mut self, kv: &str) -> Result<(), String> {
        let (k, v) = kv
            .split_once('=')
            .ok_or_else(|| format!("--label expects key=value, got `{kv}`"))?;
        if k.trim().is_empty() {
            return Err("--label: empty key".into());
        }
        self.labels.insert(k.trim().to_owned(), v.to_owned());
        Ok(())
    }

    /// Whether any label input was given.
    #[must_use]
    pub fn is_set(&self) -> bool {
        !self.labels.is_empty() || self.file.is_some()
    }

    /// Resolve this node's labels: `config_labels` (the config entry's own),
    /// overlaid by the labels file, overlaid by `--label` flags. With a file
    /// and `wait_secs > 0`, polls (500 ms) until the file is ready — present
    /// and, in annotation mode, carrying [`RESOLVED_ANNOTATION`] — or the
    /// timeout elapses, then proceeds with whatever is there and logs a
    /// warning (a node must still come up if the operator is slow; its labels
    /// can then only be filled in by a restart, see ADR 0005's amendment).
    ///
    /// # Errors
    /// A malformed labels file (a missing file after the wait is only a
    /// warning).
    pub async fn resolve(
        &self,
        config_labels: &BTreeMap<String, String>,
    ) -> Result<BTreeMap<String, String>, String> {
        let mut out = config_labels.clone();
        if let Some(path) = &self.file {
            let deadline = std::time::Instant::now() + Duration::from_secs(self.wait_secs);
            let from_file = loop {
                let ready = match std::fs::read_to_string(path) {
                    Ok(text) => {
                        let parsed = parse_labels_text(&text)
                            .map_err(|e| format!("--labels-file {path}: {e}"))?;
                        let ok = if self.annotations {
                            parsed.contains_key(RESOLVED_ANNOTATION)
                        } else {
                            !parsed.is_empty()
                        };
                        Some((ok, parsed))
                    }
                    Err(_) => None,
                };
                match ready {
                    Some((true, parsed)) => break Some(parsed),
                    other => {
                        if std::time::Instant::now() >= deadline {
                            eprintln!(
                                "animusd: warning: --labels-file {path} not ready after {}s; \
                                 starting with whatever labels are available (topology labels \
                                 may be missing from this node's registration)",
                                self.wait_secs
                            );
                            break other.map(|(_, p)| p);
                        }
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                }
            };
            if let Some(parsed) = from_file {
                let labels = if self.annotations {
                    labels_from_annotations(&parsed)
                } else {
                    parsed
                };
                out.extend(labels);
            }
        }
        out.extend(self.labels.clone());
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn parses_quoted_unquoted_comments_and_blanks() {
        let text = "# a comment\n\nzone=us-east-1a\nregion=\"us-east-1\"\n  spaced = \"a \\\"b\\\" c\"  \n";
        assert_eq!(
            parse_labels_text(text).unwrap(),
            m(&[
                ("zone", "us-east-1a"),
                ("region", "us-east-1"),
                ("spaced", "a \"b\" c")
            ])
        );
    }

    #[test]
    fn rejects_malformed_lines() {
        assert!(parse_labels_text("novalue").unwrap_err().contains("line 1"));
        assert!(parse_labels_text("=x").unwrap_err().contains("empty key"));
        assert!(
            parse_labels_text("a=\"x")
                .unwrap_err()
                .contains("unterminated")
        );
        assert!(
            parse_labels_text("a=\"\\q\"")
                .unwrap_err()
                .contains("escape")
        );
    }

    #[test]
    fn annotation_projection_keeps_only_topology_keys_translated() {
        let ann = parse_labels_text(
            "kubectl.kubernetes.io/last-applied=\"{}\"\nanimus.io/topology-zone=\"z1\"\n\
             animus.io/topology-region=\"r1\"\nanimus.io/topology-resolved=\"true\"\n",
        )
        .unwrap();
        assert_eq!(
            labels_from_annotations(&ann),
            m(&[(ZONE_LABEL, "z1"), (REGION_LABEL, "r1")])
        );
        // A node with no topology labels: resolved marker only -> no labels.
        let ann = m(&[(RESOLVED_ANNOTATION, "true")]);
        assert!(labels_from_annotations(&ann).is_empty());
    }

    #[test]
    fn add_label_validates() {
        let mut f = LabelFlags::default();
        f.add_label("a=b=c").unwrap();
        assert_eq!(f.labels, m(&[("a", "b=c")]));
        assert!(f.add_label("nokv").is_err());
        assert!(f.add_label("=v").is_err());
    }

    #[tokio::test]
    async fn resolve_merges_config_then_file_then_flags() {
        let dir = std::env::temp_dir().join(format!("animusd-labels-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("l");
        std::fs::write(
            &path,
            "animus.io/topology-zone=\"fz\"\nanimus.io/topology-resolved=\"true\"\n",
        )
        .unwrap();
        let mut f = LabelFlags {
            file: Some(path.to_string_lossy().into_owned()),
            annotations: true,
            ..Default::default()
        };
        f.add_label("flag=1").unwrap();
        let got = f
            .resolve(&m(&[(ZONE_LABEL, "cfg"), ("cfg", "1")]))
            .await
            .unwrap();
        assert_eq!(got, m(&[(ZONE_LABEL, "fz"), ("cfg", "1"), ("flag", "1")]));
        // Flag beats file.
        f.add_label(&format!("{ZONE_LABEL}=flagz")).unwrap();
        assert_eq!(
            f.resolve(&BTreeMap::new()).await.unwrap()[ZONE_LABEL],
            "flagz"
        );
        // Not-ready file with wait 0 proceeds (warning) with config only.
        std::fs::write(&path, "other=x\n").unwrap();
        f.labels.clear();
        assert!(f.resolve(&BTreeMap::new()).await.unwrap().is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn resolve_waits_for_the_file_then_picks_it_up() {
        let dir = std::env::temp_dir().join(format!("animusd-labels-w-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("l");
        let f = LabelFlags {
            file: Some(path.to_string_lossy().into_owned()),
            annotations: true,
            wait_secs: 30,
            ..Default::default()
        };
        let p2 = path.clone();
        let writer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(700)).await;
            std::fs::write(
                p2,
                "animus.io/topology-zone=\"late\"\nanimus.io/topology-resolved=\"true\"\n",
            )
            .unwrap();
        });
        let got = f.resolve(&BTreeMap::new()).await.unwrap();
        writer.await.unwrap();
        assert_eq!(got[ZONE_LABEL], "late");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
