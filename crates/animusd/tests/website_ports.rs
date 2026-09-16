//! Drift guard for `website/`'s port documentation (issue #850): the website
//! is part of the documentation (root `CLAUDE.md`'s Conventions), and its
//! three pages that document the per-node port layout must stay in sync with
//! [`ClusterConfig::generate`]'s real **six**-port stride byte-for-byte.
//!
//! `architecture.html`/`install.html`/`how-it-works.html` used to describe a
//! seven-port layout with a fictitious "reserved" slot at `+3`, pushing every
//! documented port from `admin` onward one higher than what actually binds
//! (and pointing the `animusd join --seed` walkthrough at the console port
//! instead of the intra port). This test parses the `<td>+N</td> … <PORT>`
//! table rows out of the two pages that carry a literal port table and checks
//! them against [`ClusterConfig::generate(3, host, 7100)`]'s own addresses,
//! and additionally rejects the literal "seven ports"/"reserved" phrasing
//! that caused the drift, across all three pages.

use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};

use animusd::ClusterConfig;

/// `website/`, resolved relative to this crate's manifest dir
/// (`crates/animusd`) rather than the process cwd, so the test works
/// regardless of where `cargo test` is invoked from.
fn website_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../website")
        .canonicalize()
        .expect("website/ must exist two levels up from crates/animusd")
}

fn read_website_file(name: &str) -> String {
    let path = website_dir().join(name);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("failed to read {path:?}: {e}"))
}

/// The first run of exactly four ASCII digits in `s` (i.e. a bare `71NN`
/// port literal, however it's wrapped in markup around it).
fn first_four_digit_run(s: &str) -> Option<u16> {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i].is_ascii_digit() {
            let start = i;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
            if i - start == 4 {
                return s[start..i].parse().ok();
            }
        } else {
            i += 1;
        }
    }
    None
}

/// Every `<td>+N</td> …` row's `(offset, port)` pair, for `N` in `0..=5`.
/// Matches both `<td>7100</td>` (architecture.html) and
/// `<td><code>7100</code></td>` (install.html) — whatever markup wraps the
/// port literal, it takes the first bare four-digit run after the offset
/// cell on the same line.
fn offset_port_pairs(html: &str) -> Vec<(u16, u16)> {
    let mut pairs = Vec::new();
    for line in html.lines() {
        for offset in 0u16..=5 {
            let marker = format!("<td>+{offset}</td>");
            if let Some(pos) = line.find(&marker) {
                let after = &line[pos + marker.len()..];
                if let Some(port) = first_four_digit_run(after) {
                    pairs.push((offset, port));
                }
            }
        }
    }
    pairs
}

/// The six ports `ClusterConfig::generate` assigns node 0 at `base_port`,
/// in the same `internal, client, dynamo, admin, intra, console` order the
/// website tables document (offsets `0..=5`).
fn expected_offset_ports(base_port: u16) -> Vec<(u16, u16)> {
    let host: IpAddr = "127.0.0.1".parse().unwrap();
    let cfg = ClusterConfig::generate(3, host, base_port);
    let node0 = &cfg.nodes[0];
    let port = |a: SocketAddr| a.port();
    vec![
        (0, port(node0.internal)),
        (1, port(node0.client)),
        (2, port(node0.dynamo)),
        (3, port(node0.admin)),
        (4, port(node0.intra)),
        (5, port(node0.console)),
    ]
}

fn assert_table_matches_generate(page: &str, html: &str) {
    let expected = expected_offset_ports(7100);
    let actual = offset_port_pairs(html);
    assert_eq!(
        actual, expected,
        "{page}'s port table's +N/port pairs must match \
         ClusterConfig::generate(3, host, 7100)'s node-0 addresses \
         (internal, client, dynamo, admin, intra, console) — got {actual:?}, \
         want {expected:?}. If this fails, the code's real port stride \
         changed (or the page drifted from it again) — update the page \
         from ClusterConfig::generate, don't hand-edit the numbers."
    );
}

fn assert_no_seven_port_language(page: &str, html: &str) {
    let lower = html.to_lowercase();
    assert!(
        !lower.contains("seven port"),
        "{page} still claims a seven-port layout; the real stride is six \
         (crates/animusd/src/config.rs::ClusterConfig::generate)"
    );
    assert!(
        !lower.contains("(reserved)"),
        "{page} still documents a fictitious reserved port slot; \
         ClusterConfig::generate has no reserved slot"
    );
    assert!(
        !html.contains(">Reserved<"),
        "{page} still has a bare \"Reserved\" table cell for a port slot \
         that doesn't exist in ClusterConfig::generate"
    );
}

#[test]
fn architecture_html_port_table_matches_generate() {
    let html = read_website_file("architecture.html");
    assert_table_matches_generate("architecture.html", &html);
    assert_no_seven_port_language("architecture.html", &html);
}

#[test]
fn install_html_port_table_matches_generate() {
    let html = read_website_file("install.html");
    assert_table_matches_generate("install.html", &html);
    assert_no_seven_port_language("install.html", &html);
    // install.html's base-port walkthrough math (node 1, node 2): the
    // ADR 0047/0053 six-port stride, not the old seven-port one.
    assert!(
        html.contains("7106") && html.contains("7112"),
        "install.html's multi-node-per-host base-port example should show \
         node 1 at 7106 and node 2 at 7112 (base 7100 + 6*i), not the \
         seven-stride 7107/7114"
    );
    assert!(
        !html.contains("7107") && !html.contains("7114"),
        "install.html still has the stale seven-stride base-port math \
         (7107/7114)"
    );
    // The join walkthrough must seed against the intra port (offset +4,
    // 7104), never the console port (offset +5, 7105) it used to point at.
    assert!(
        html.contains("--seed 10.0.0.1:7104"),
        "install.html's `animusd join --seed` example must point at node \
         0's intra port (7104), not a stale offset"
    );
}

#[test]
fn how_it_works_html_has_no_seven_port_language() {
    let html = read_website_file("how-it-works.html");
    assert_no_seven_port_language("how-it-works.html", &html);
    assert!(
        html.to_lowercase().contains("six port"),
        "how-it-works.html should say \"six ports\"/\"six consecutive \
         ports\", matching ClusterConfig::generate's real stride"
    );
}
