"use strict";
// Shared state, fetch helpers, formatting utilities, theme, and tab routing
// for animusd admin (ADR 0021's "AnimusDB Console"). `dashboard_overview.js`, `dashboard_placement.js`,
// `dashboard_tablets.js`, `dashboard_txns.js`, `dashboard_streams.js`, `dashboard_browser.js`, and
// `dashboard_storage.js` load after this file and call into it (STATE, $, esc, getJSON, postJSON,
// pill, consoleLink, bytes, humanBytes, tokenBound, b64url, nodeIdOf, cpGroupsByTablet,
// txnViewsByTablet, autoSplitThresholds, tabletStatus, worstTabletStatus, statusDotClass, computeHealth,
// activateTab, gotoStorage, splitHiddenTable);
// nothing here calls into them except `render()`, the single per-refresh
// entry point every view's render function hangs off of.
const SEED = window.location.origin;
const $ = (id) => document.getElementById(id);
const esc = (s) => String(s).replace(/[&<>"]/g, (c) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

// State assembled each refresh.
let STATE = { status: null, nodes: [], peersErr: null };

// ---- this node's own role (ADR 0035 PR7) ----
// `SELF` is this node's own `/admin/config`+`/admin/raft`+`/admin/raftkv`+
// `/admin/health` — fetched directly against `SEED` (never a peer), so
// deriving the role and gating the sidebar never waits on a slow/unreachable
// OTHER node the way the full cluster-wide fan-out in `loadAll()` below can.
// `ROLE` defaults to "combined" (the superset of tabs) until the first
// `loadSelf()` resolves, so an early deep-link / popstate before that first
// resolve degrades to "show every tab", not "show none".
let ROLE = "combined";
let SELF = { ok: false, base: SEED };

async function loadSelf() {
  try {
    // `metricsHistory` (docs/roadmap.md U-01) backs the Overview read-path
    // sparklines — this node's own `/admin/metrics/history` ring buffer,
    // fetched alongside everything else `SELF` already carries. Per-node,
    // not cluster-aggregated (the same "one sink" caveat `/admin/metrics`
    // itself carries) — deliberately, since a sparkline of a SUM across
    // nodes would hide which node is actually doing the work.
    const [config, raft, raftkv, health, metricsHistory] = await Promise.all([
      getJSON(SEED, "/admin/config"),
      getJSON(SEED, "/admin/raft").catch(() => null),
      getJSON(SEED, "/admin/raftkv").catch(() => null),
      getJSON(SEED, "/admin/health").catch(() => null),
      getJSON(SEED, "/admin/metrics/history").catch(() => null),
    ]);
    SELF = { base: SEED, config, raft, raftkv, health, metricsHistory, ok: true };
    ROLE = config.role || "combined";
  } catch (e) {
    SELF = { base: SEED, ok: false, error: String(e) };
  }
  applyRoleGating();
  return ROLE;
}

// ---- theme ----
// A UI preference, not data — persisted client-side only. Both palettes
// (light default, dark "ink") are fully defined in tokens.css; this just
// picks which one applies. Three choices: "light"/"dark" pin an explicit
// palette via `data-theme`; "system" clears the attribute entirely so
// tokens.css's `prefers-color-scheme` media query decides (and keeps
// deciding live — no JS listener needed, the media query re-evaluates on
// its own when the OS preference changes). Light is the default when
// nothing is stored yet (ADR 0056 flips the old dark-by-default).
const THEME_KEY = "animusd-admin-theme";
function applyThemeChoice(choice) {
  const root = document.documentElement;
  if (choice === "light" || choice === "dark") root.dataset.theme = choice;
  else delete root.dataset.theme;
  document.querySelectorAll(".theme-switch button[data-theme-choice]").forEach((b) => {
    b.classList.toggle("active", b.dataset.themeChoice === choice);
  });
}
function initTheme() {
  const saved = localStorage.getItem(THEME_KEY);
  applyThemeChoice(saved === "dark" || saved === "system" ? saved : "light");
}
function wireThemeSwitch(containerId) {
  const el = $(containerId);
  if (!el) return;
  el.querySelectorAll("button[data-theme-choice]").forEach((b) => {
    b.addEventListener("click", () => {
      localStorage.setItem(THEME_KEY, b.dataset.themeChoice);
      applyThemeChoice(b.dataset.themeChoice);
    });
  });
}

// Fetch JSON from `base + path`; throws on network/HTTP error.
async function getJSON(base, path) {
  const r = await fetch(base + path, { headers: { "accept": "application/json" } });
  if (!r.ok) throw new Error(r.status + " " + r.statusText);
  return r.json();
}

// POST JSON; returns {status, body} (body parsed as JSON when possible).
async function postJSON(base, path, obj) {
  const r = await fetch(base + path, {
    method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify(obj),
  });
  const text = await r.text();
  let body; try { body = JSON.parse(text); } catch { body = text; }
  return { status: r.status, body };
}

// Turn a "host:port" admin address into a fetchable origin. A wildcard bind host
// (0.0.0.0 / ::) isn't dialable from the browser — fall back to the page's host.
function baseFor(addr) {
  let host = addr, port = "";
  const i = addr.lastIndexOf(":");
  if (i >= 0) { host = addr.slice(0, i); port = addr.slice(i + 1); }
  host = host.replace(/^\[|\]$/g, "");
  if (host === "0.0.0.0" || host === "::" || host === "") host = window.location.hostname;
  const h = host.includes(":") ? "[" + host + "]" : host;
  return "http://" + h + (port ? ":" + port : "");
}

// Unpadded base64url of a byte array — the same encoding the server uses for
// binary partition tokens (admin.rs::token_base64).
function b64url(arr) {
  return btoa(arr.map((b) => String.fromCharCode(b)).join(""))
    .replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

// Render a byte sequence (JSON array of u8, or a string) for humans. Mirrors the
// server's key_display (admin.rs): a key with a binary 8-byte token prefix shows
// as <token-base64>:<remainder as text>; any other binary sequence shows whole as
// URL-safe base64.
function bytes(v) {
  if (v == null) return "∞";
  if (typeof v === "string") return v === "" ? "«start»" : v;
  if (Array.isArray(v)) {
    if (v.length === 0) return "«start»";
    const printable = (b) => b >= 32 && b < 127;
    if (v.every(printable)) return v.map((b) => String.fromCharCode(b)).join("");
    if (v.length >= 8 && !v.slice(0, 8).every(printable)) {
      return b64url(v.slice(0, 8)) + ":" + v.slice(8).map((b) => String.fromCharCode(b)).join("");
    }
    return b64url(v);
  }
  return String(v);
}

// A byte *count* (e.g. a tablet's total size) as a human-readable string
// ("842 B" / "1.2 KB" / "3.4 MB") — distinct from `bytes()` above, which
// renders a byte *array* (a key/value's raw content) for display. `null`
// passes through as `null` so callers can render their own placeholder.
function humanBytes(n) {
  if (n == null) return null;
  if (n < 1024) return `${n} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let v = n;
  let i = -1;
  do {
    v /= 1024;
    i++;
  } while (v >= 1024 && i < units.length - 1);
  return `${v.toFixed(v < 10 ? 2 : 1)} ${units[i]}`;
}

// A small, dependency-free inline-SVG sparkline (docs/roadmap.md U-01) — no
// canvas, no charting library (ADR 0021 §1's "no build toolchain/CDN" rules
// out pulling one in for this). `values` is a plain array of numbers, oldest
// first — the same order `/admin/metrics/history`'s `samples` ring already
// comes in, so a caller passes a plain `.map()` over it with no reshaping.
// Renders one `<polyline>` scaled to fit `w`x`h` (defaults 90x24, sized for a
// stat-tile-sized slot); a flat/empty/single-point series draws a flat
// mid-height line rather than dividing by zero — a real min==max is not an
// error, just nothing to show relative motion for. Uses `currentColor` so a
// call site colors it via CSS (`.sparkline`) rather than baking a palette
// choice in here.
function sparkline(values, w, h) {
  w = w || 90;
  h = h || 24;
  if (!values || values.length === 0) return `<svg width="${w}" height="${h}" class="sparkline"></svg>`;
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min;
  const n = values.length;
  const pts = values.map((v, i) => {
    const x = n === 1 ? w / 2 : (i / (n - 1)) * (w - 2) + 1;
    const y = span === 0 ? h / 2 : h - 1 - ((v - min) / span) * (h - 2);
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(" ");
  return `<svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}" class="sparkline">
    <polyline points="${esc(pts)}" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linejoin="round" stroke-linecap="round"></polyline>
  </svg>`;
}

// A tablet range boundary on the hash ring (ADR 0022): the first 8 key bytes are
// the big-endian Murmur3 partition token, shown as URL-safe base64 — matching the
// token prefix in the key views, so a boundary can be eyeballed against browsed
// keys; "…" marks elided bytes past the token. An empty start / null end is the
// ring's edge — the caller passes the all-00 / all-ff token.
function tokenBound(v, fill) {
  if (v == null || v === "") return fill;
  const arr = Array.isArray(v) ? v : Array.from(String(v), (c) => c.charCodeAt(0) & 0xff);
  if (arr.length === 0) return fill;
  const tok = b64url(arr.slice(0, 8));
  return arr.length > 8 ? `${tok}…` : tok;
}

function pill(cls, text) { return `<span class="pill ${esc(cls)}">${esc(text)}</span>`; }
function dot(cls) { return `<span class="dot ${esc(cls)}"></span>`; }

// A node id, truncated with an ellipsis where its column/card is narrow, full
// value always available on hover (ADR 0040 PR5): pre-ADR-0040 ids were short
// integers (`"3"`) that never needed this; a self-minted id is a 22-char
// base64url string (`NodeId::mint`, ADR 0040 Decision B) that can otherwise
// blow out a table column or wrap a card header. `extraCls` appends any
// additional classes the call site already used on its own `<span>`
// (`"mono"`, `"node"`, …) so this is a drop-in replacement, not a rewrite of
// surrounding layout. CSS (`.id-trunc`) does the actual clipping; the
// `title` attribute is the full id, so a real mouse hover (or a screen
// reader) always sees it uncut even when the rendered text doesn't.
function idSpan(id, extraCls) {
  const cls = "id-trunc" + (extraCls ? " " + extraCls : "");
  return `<span class="${cls}" title="${esc(id)}">${esc(id)}</span>`;
}

// A "console →" link to another node's OWN admin console, from its admin
// `base` origin (already resolved by `loadAll()`'s fan-out). Empty when the
// base is unknown (unreachable node) or is this very page's own origin —
// linking a node to itself is noise, not navigation. Opens in a new tab
// (same pattern as the Node view's "Open cluster console" link) so the
// current console's state isn't lost by hopping.
function consoleLink(base, id) {
  if (!base || base === SEED) return "";
  return `<a class="link-text" href="${esc(base)}/admin/ui/overview" target="_blank" rel="noopener"
    title="open node ${esc(id)}'s own console">console →</a>`;
}

// ---- shared data-derivation helpers (used by more than one view) ----

// A GSI is materialized as a hidden table named `<base>$<index>` (ADR
// 0041) — its own ordinary entry in `status.schemas.tables` and
// `status.tablets[*].table`, with no back-pointer to its base. User table
// names can't contain `$` (enforced at create), so splitting on the first
// one is unambiguous. Returns `null` for an ordinary (non-hidden) table
// name. LSIs never get a hidden table (they're kind scopes inside the base
// table's own tablets), so this only ever matches a GSI. The one rule
// every view (`browser`/`tablets`/`overview`) derives its grouping from, so
// they can't disagree on what counts as "hidden."
function splitHiddenTable(name) {
  const i = name.indexOf("$");
  return i < 0 ? null : { base: name.slice(0, i), index: name.slice(i + 1) };
}

function nodeById(id) {
  return STATE.nodes.find((n) => n.ok && n.config && n.config.node_id === id);
}
// The id a CP-group/member row is keyed by (ADR 0040 PR1: one identity per
// node — was the raftkv id specifically, back when a node could have a
// distinct control id; every node now has exactly one id, so this and
// `nodeDisplayId` below are the same lookup).
function nodeIdOf(n) { return n.config ? n.config.node_id : "?"; }
// A human-facing "which node is this" label for an ARBITRARY node.
function nodeDisplayId(n) {
  const c = n && n.config;
  if (!c) return "?";
  return c.node_id != null ? c.node_id : "?";
}

// Collect every hosted CP group across reachable nodes, indexed by tablet id.
// Each group carries its own owning node's id (`g.node`, from `CpRaftView`)
// — resolve that to the real node, never the admin node that happened to
// answer the request. Under `--cluster N` dev mode every node's
// `/admin/raftkv` response lists *every* replica cluster-wide (the shared
// `ClusterEdgeState`), so tagging a group with whichever node's fetch
// returned it — rather than the group's real owner — mis-associates it: a
// `.find()` matching on that tag then lands on the wrong group's state,
// which is what made every replica dot in a tablet's row echo the same
// (first) group's leader/follower status. Dedupe by (tablet, owning node) so
// a group is counted once regardless of how many admin ports reported it.
function cpGroupsByTablet() {
  const map = {};
  const seen = new Set();
  for (const n of STATE.nodes) {
    if (!n.ok || !n.raftkv || !n.raftkv.groups) continue;
    for (const g of n.raftkv.groups) {
      const key = g.tablet + ":" + g.node;
      if (seen.has(key)) continue;
      seen.add(key);
      const node = nodeById(g.node) || { config: { node_id: g.node }, ok: false };
      (map[g.tablet] = map[g.tablet] || []).push({ node, g });
    }
  }
  return map;
}

// Collect every hosted CP group's transaction-tracker view (ADR 0018 §2/PR7,
// docs/roadmap.md U-01) across reachable nodes, indexed by tablet id — the
// identical fan-out/dedupe shape `cpGroupsByTablet` uses just above (`/admin/
// txns` iterates the same `ctx.edge.hosted_groups()` `/admin/raftkv` does, so
// under `--cluster N` dev mode every node's response lists every replica
// cluster-wide too, and a group must be counted once regardless of how many
// admin ports reported it).
function txnViewsByTablet() {
  const map = {};
  const seen = new Set();
  for (const n of STATE.nodes) {
    if (!n.ok || !n.txns || !n.txns.groups) continue;
    for (const tv of n.txns.groups) {
      const key = tv.tablet + ":" + tv.node;
      if (seen.has(key)) continue;
      seen.add(key);
      const node = nodeById(tv.node) || { config: { node_id: tv.node }, ok: false };
      (map[tv.tablet] = map[tv.tablet] || []).push({ node, tv });
    }
  }
  return map;
}

// The `--auto-split-bytes B` (bytes, ADR 0034) threshold, from any
// reachable node's `/admin/config` — a single flag set, so every node
// agrees. `null` if the trigger isn't enabled for this run. (The former
// key-count `--auto-split K` trigger was removed — bytes and, for streamed
// tables, `--auto-split-change-rate` are the only remaining triggers.)
function autoSplitThresholds() {
  const n = STATE.nodes.find((x) => x.ok && x.config);
  return {
    bytes: n ? n.config.auto_split_bytes_threshold : null,
  };
}

// A tablet's derived status — health here means "is the data at risk", not
// "is anything in transition" (see `computeHealth`'s doc below for the full
// philosophy). `gs` is `cpGroupsByTablet()[id] || []`. Reads
// `STATE.status.members` directly (like `cpGroupsByTablet` reads `STATE`)
// rather than taking it as a parameter, so every call site stays a plain
// `tabletStatus(t, gs)`.
//
// Ladder (first match wins):
//   1. `quorum-lost`      — fewer than a quorum of the tablet's ASSIGNED
//      replicas are on a live node. The group cannot commit, and one more
//      failure loses data. Always critical.
//   2. `under-replicated` — some assigned replica's node is genuinely down
//      (not just mid-election/mid-formation). Redundancy is actually
//      reduced; repair is pending. Degrades health.
//   3. `healthy`          — a leader is elected and every configured replica
//      is currently hosted-and-reachable.
//   4. `forming`          — every assigned replica's node is alive, but the
//      group hasn't converged yet (no leader elected, or fewer hosted groups
//      than configured). This is a transition — split-child formation, first
//      election on a freshly-provisioned table, rebalance/repair catch-up,
//      reconciler lag, or an admin-port fan-out gap — never a data-risk
//      state (ADR 0028: split/provision are a single control-plane
//      command with no data-plane half; the data already sits safely on the
//      source replicas' shared storage engines while the new group stands
//      itself up). Does NOT degrade health by itself; see the
//      `overdueFormingCount` guardrail in `computeHealth`.
function tabletStatus(t, gs) {
  const members = (STATE.status && STATE.status.members) || {};
  const membersEmpty = Object.keys(members).length === 0;
  const replicas = t.replicas || [];
  const configured = replicas.length;
  // A replica's node is "live" if it isn't known to be Down — `Joining`/
  // `Leaving` still means the node is up and its engine durable, just
  // bootstrapping or draining. An empty members map (very early startup,
  // before the first heartbeat round) can't distinguish live from dead, so
  // it treats every assigned replica as live rather than screaming
  // quorum-lost before the cluster has even reported in.
  const isLive = (id) => membersEmpty || (!!members[id] && members[id].status !== "Down");
  const liveAssigned = replicas.filter(isLive).length;
  const quorum = Math.floor(configured / 2) + 1;
  const hasLeader = gs.some((x) => x.g.is_leader);
  const hostedLive = gs.length;

  if (configured > 0 && liveAssigned < quorum) return "quorum-lost";
  if (liveAssigned < configured) return "under-replicated";
  if (hasLeader && hostedLive >= configured) return "healthy";
  return "forming";
}

// Ranks `tabletStatus` outputs worst-to-best, for rollups that need "the
// worst status among several tablets" (e.g. the Overview per-table summary).
const TABLET_STATUS_RANK = { "quorum-lost": 3, "under-replicated": 2, forming: 1, healthy: 0 };
function worstTabletStatus(statuses) {
  return statuses.reduce((worst, s) => (TABLET_STATUS_RANK[s] > TABLET_STATUS_RANK[worst] ? s : worst), "healthy");
}

function statusDotClass(status) {
  if (status === "healthy" || status === "Active") return "ok-dot";
  if (status === "under-replicated" || status === "Leaving" || status === "Joining") return "warn-dot";
  if (status === "quorum-lost" || status === "Down") return "bad-dot";
  return "dim-dot"; // forming, and anything unrecognized
}

// Module-level guardrail state for the "overdue forming" escalation below:
// how long (wall-clock) each tablet has been continuously `forming`. This is
// browser UI state, not simulation/protocol logic, so `Date.now()` is
// intentional here — the repo's Env-determinism rule (ADR 0003) scopes to
// deterministic Rust crate logic (`SimEnv`/`ProdEnv`), not this client-side
// SPA, which has no replay/seed story to preserve. Do not "fix" this into an
// injected clock.
const FORMING_SINCE = new Map();
const FORMING_OVERDUE_MS = 60_000;

// Derive an at-a-glance health rollup from data `loadAll()` already fetched —
// no new requests.
//
// ---- Philosophy: health ≈ "is the data at risk", not "is anything in
// transition" ----
// A tablet mid-formation — a split-child forming its Raft group, a freshly
// created table's first election, a rebalance/repair move catching up, or a
// reconciler/admin-fan-out lag — is not a data-risk state as long as every
// replica assigned to it is on a live node: ADR 0028 makes split/
// provision a single control-plane command with no data-plane half, so the
// data already sits safely in the source replicas' shared storage engines
// the whole time the new group is standing itself up. That is exactly what
// `tabletStatus`'s `forming` state captures, and it must not degrade the
// cluster's health pill — otherwise every routine split reads as an outage.
// What DOES mean data is at risk: an assigned replica's node actually being
// `Down` (`under-replicated` — redundancy genuinely reduced) or a tablet
// dropping below quorum (`quorum-lost` — cannot commit, one more failure
// loses data; always critical). A lingering `Down` MEMBER that no tablet
// still depends on (every tablet it used to replicate has been repaired onto
// a spare) is also not degrading by itself — the failure detector/reconciler
// can keep retrying it for a while after the actual data-loss risk is gone.
// `downCount` is still surfaced (nodes tile, banner text) for visibility.
//
// ---- Overdue-forming guardrail ----
// A formation that never converges (e.g. a stuck election, a wedged
// reconciler) is a real problem and must not hide behind "it's just forming"
// forever. `FORMING_SINCE` tracks, per tablet, when it was first observed
// forming (cleared the moment it leaves forming or the tablet vanishes); a
// tablet forming for longer than `FORMING_OVERDUE_MS` (60s) counts toward
// `overdueFormingCount`, which DOES degrade health.
function computeHealth() {
  const members = (STATE.status && STATE.status.members) || {};
  const memberIds = Object.keys(members);
  const downCount = Object.values(members).filter((m) => m.status === "Down").length;

  const tablets = (STATE.status && STATE.status.tablets) || {};
  const tabletIds = Object.keys(tablets);
  const groups = cpGroupsByTablet();

  let quorumLostCount = 0, underReplicatedCount = 0, formingCount = 0, overdueFormingCount = 0;
  const now = Date.now();
  const stillForming = new Set();
  for (const id of tabletIds) {
    const st = tabletStatus(tablets[id], groups[id] || []);
    if (st === "quorum-lost") quorumLostCount++;
    else if (st === "under-replicated") underReplicatedCount++;
    else if (st === "forming") {
      formingCount++;
      stillForming.add(id);
      if (!FORMING_SINCE.has(id)) FORMING_SINCE.set(id, now);
      if (now - FORMING_SINCE.get(id) > FORMING_OVERDUE_MS) overdueFormingCount++;
    }
  }
  // Forget any tablet no longer forming — recovered, or gone entirely
  // (split/drop) — so a later re-entry into forming starts a fresh
  // clock instead of reusing a stale timestamp from a previous episode.
  for (const id of FORMING_SINCE.keys()) {
    if (!stillForming.has(id)) FORMING_SINCE.delete(id);
  }

  const controlLeader = STATE.nodes.find((n) => n.ok && n.raft && n.raft.is_leader);

  let status = "healthy";
  if (underReplicatedCount > 0 || overdueFormingCount > 0) status = "degraded";
  if (!controlLeader || quorumLostCount > 0) status = "critical";

  return {
    status, controlLeader,
    downCount, totalNodes: memberIds.length || STATE.nodes.length,
    quorumLostCount, underReplicatedCount, formingCount, overdueFormingCount,
    totalTablets: tabletIds.length,
  };
}

function renderHealthPill() {
  const h = computeHealth();
  let label;
  if (h.status === "critical") {
    label = !h.controlLeader ? "No control leader"
      : `${h.quorumLostCount} tablet${h.quorumLostCount === 1 ? "" : "s"} quorum-lost`;
  } else if (h.status === "degraded") {
    const issues = h.underReplicatedCount + h.overdueFormingCount;
    label = `Degraded · ${issues} issue${issues === 1 ? "" : "s"}`;
  } else {
    label = h.formingCount ? `Healthy · ${h.formingCount} forming` : "Healthy";
  }
  $("health-pill").className = "health-pill " + h.status;
  $("health-pill").innerHTML = `<span class="dot"></span><span class="label">${esc(label)}</span>`;
}

function renderSidebarFoot() {
  const tablets = (STATE.status && STATE.status.tablets) || {};
  const members = (STATE.status && STATE.status.members) || {};
  const nodeCount = Object.keys(members).length || STATE.nodes.length;
  $("sidebar-foot").innerHTML =
    `<div>${esc(nodeCount)} node(s)</div><div>${esc(Object.keys(tablets).length)} tablet(s)</div>`;
}

async function loadAll() {
  // This node's own view first (fast, local-only) — see `SELF`'s doc above
  // for why this must never wait on the slower peer fan-out below.
  await loadSelf();
  let peers;
  try {
    peers = await getJSON(SEED, "/admin/peers");
  } catch (e) {
    peers = { admin_addrs: [SEED.replace(/^https?:\/\//, "")] };
    STATE.peersErr = String(e);
  }
  let status = null;
  try { status = await getJSON(SEED, "/admin/status"); } catch (e) { /* shown per-panel */ }

  const addrs = (peers.admin_addrs && peers.admin_addrs.length) ? peers.admin_addrs
    : [SEED.replace(/^https?:\/\//, "")];
  // Per-address role, straight from `/admin/peers`' own `peers` field (ADR
  // 0035 residual follow-up — role now replicates alongside the address, see
  // `admin.rs::peers_view`) rather than only ever knowable once THAT node's
  // own `/admin/config` fetch below succeeds. Kept as a fallback on `node`
  // (not overwriting `config.role` when the fetch does succeed) so a peer
  // whose own fan-out fails/is still in flight still carries a role a
  // consumer can use (e.g. tagging a down control node in the Overview list,
  // or picking a console link target without waiting on that node itself).
  const roleByAddr = {};
  for (const p of (peers.peers || [])) roleByAddr[p.admin] = p.role;
  const nodes = await Promise.all(addrs.map(async (addr) => {
    const base = baseFor(addr);
    const node = { addr, base, ok: false, role: roleByAddr[addr] };
    try {
      // `metrics` (ADR 0021 Streams tab, the dashboard's first `/admin/metrics`
      // consumer) fans out alongside the other per-node views already fetched
      // here — `.catch(() => null)` like `raft`/`raftkv`/`health`, so one
      // unreachable/older node degrades to "no metrics for this node" rather
      // than failing the whole fan-out.
      // `txns` (ADR 0018 §2/PR7, docs/roadmap.md U-01) fans out alongside
      // `raftkv` the same way `metrics` does — `.catch(() => null)` so one
      // unreachable/older node degrades to "no txn view for this node"
      // rather than failing the whole fan-out.
      const [config, raft, raftkv, txns, health, metrics] = await Promise.all([
        getJSON(base, "/admin/config"),
        getJSON(base, "/admin/raft").catch(() => null),
        getJSON(base, "/admin/raftkv").catch(() => null),
        getJSON(base, "/admin/txns").catch(() => null),
        getJSON(base, "/admin/health").catch(() => null),
        getJSON(base, "/admin/metrics").catch(() => null),
      ]);
      Object.assign(node, { config, raft, raftkv, txns, health, metrics, ok: true });
    } catch (e) {
      node.error = String(e);
    }
    return node;
  }));

  STATE = { status, nodes, peersErr: STATE.peersErr };
  render();
  $("updated").textContent = "updated " + new Date().toLocaleTimeString();
}

function render() {
  applyRoleGating();
  renderHealthPill();
  renderSidebarFoot();
  renderOverview();
  renderPlacement();
  renderTablets();
  renderTxns();
  renderStreams();
  renderStorageSelectors();
  renderBrowserTables();
  renderSeedTables();
  renderNode();
  // Rebuild the Dynamo editor's skeleton only when the effective table
  // changed (first load, table created/dropped, or a manual switch) — never
  // on a routine refresh with the same selection, so in-progress edits survive.
  if (dyTable !== lastRenderedDyTable) {
    lastRenderedDyTable = dyTable;
    dynOnTable();
  }
}

// ---- tab routing (ADR 0021 follow-up 7: real URLs, refresh/back/forward preserve the tab) ----
// One flat set of views, presented as a sidebar (not a top nav row — the
// animusd admin design's shell). Each keeps its own id and `/admin/ui/<tab>`
// path (the server's `is_ui_path` just prefix-matches, and existing
// bookmarks/tests target these exact leaves).
//
// ADR 0035 PR7: which tabs this node shows is gated on its OWN role (`SELF`/
// `ROLE`, from `/admin/config`) — a control-only node keeps exactly today's
// five-tab cluster Console; a data-only node gets a dedicated Node view (plus
// Data Browser — browsing through your own data edge is node-dedicated UX)
// instead of the cluster-wide views it can't usefully render (it hosts no
// control-plane Raft state and, being a single node, has nothing to place/
// balance); a combined node gets everything, with Node appended last since a
// combined node is also a data node. Each role list's first entry is that
// role's default tab (`tabFromPath`'s fallback, `activateTab`'s
// role-mismatch fallback below) — control/combined default to "overview"
// (unchanged), data defaults to "node".
// "streams" now shows on **every** role, including control-only (follow-up
// to ADR 0042/0043's original data/combined-only gating). A control-only
// node holds the full replicated `Metadata` — schemas (incl. stream specs)
// and the `stream_shards` segment catalog — so the stream list and its
// shard-chain detail (both pure functions of `Metadata`/`DescribeStream`,
// verified against a live split cluster) render there truthfully; only the
// live-tail poller (`GetShardIterator`/`GetRecords`'s open-shard path, and
// `GetRecords`' sealed path) needs a genuine local CP data plane to serve —
// `dashboard_streams.js`'s own doc covers exactly what degrades and why.
// "txns" (ADR 0018 §2/PR7, docs/roadmap.md U-01) is gated exactly like
// "tablets": both are cluster-wide views built from a per-node fan-out
// (`/admin/txns`, mirroring `/admin/raftkv`) cross-referenced against the
// full tablet map in replicated `Metadata` — a data-only node has no local
// control-plane Raft state to derive that map from, so it gets neither tab,
// the same reasoning `tablets`' own placement in this table already
// documents.
const ROLE_TABS = {
  control: ["overview", "placement", "tablets", "txns", "browser", "streams", "storage"],
  combined: ["overview", "placement", "tablets", "txns", "browser", "streams", "storage", "node"],
  data: ["node", "browser", "streams"],
};
// The currently-visible tab set — starts as the superset (`combined`) until
// `loadSelf()` resolves this node's own role; see `ROLE`'s doc above.
let TABS = ROLE_TABS.combined;

function tabFromPath(path) {
  const m = /^\/admin\/ui\/([^/?#]+)/.exec(path);
  return m && TABS.includes(m[1]) ? m[1] : TABS[0];
}

// Recompute `TABS` from this node's own `ROLE`, show/hide the sidebar's nav
// links to match, and correct the active tab if it's no longer valid for
// this role — a role-inappropriate deep link (e.g. `/admin/ui/placement` on a
// data-only node), or a tab picked from the default superset before the first
// `loadSelf()` resolved. Idempotent; safe to call on every `loadSelf()`/
// `render()` cycle.
function applyRoleGating() {
  TABS = ROLE_TABS[ROLE] || ROLE_TABS.combined;
  document.querySelectorAll(".sidebar button.navlink").forEach((b) => {
    b.style.display = TABS.includes(b.dataset.tab) ? "" : "none";
  });
  if (!TABS.includes(activeTab)) activateTab(TABS[0], { silent: true });
}

let activeTab = TABS[0];

// Storage-tab deep-linking: the selected tablet + node ride along as
// `?tablet=&node=` on the `/admin/ui/storage` URL, so a refresh or a
// back/forward navigation lands back on the same detail instead of just the
// bare tab. The node identifier is its stable admin `base` origin — the same
// value already used as the `<option value>` in `updateStorageNodeOptions` —
// so no separate id scheme is needed. This is also what the Tablets detail
// panel's "Open in Storage →" link targets.
let pendingStorageParams = null;

function paramsFromLocation() {
  const p = new URLSearchParams(window.location.search);
  const tablet = p.get("tablet");
  const node = p.get("node");
  return (tablet || node) ? { tablet, node } : null;
}

// The storage tab's query string, from `override` if given, otherwise from
// the selects' current values (so a plain tab switch preserves whatever was
// already picked).
function storageQuery(override) {
  const tablet = (override && override.tablet != null) ? override.tablet : $("st-tablet").value;
  const node = (override && override.node != null) ? override.node : $("st-node").value;
  if (!tablet && !node) return "";
  const p = new URLSearchParams();
  if (tablet) p.set("tablet", tablet);
  if (node) p.set("node", node);
  return "?" + p.toString();
}

// Re-sync the address bar with the Storage tab's current selection. Called
// whenever the tablet/node dropdowns change while that tab is active, so
// manual browsing (not just a cross-link jump) is also bookmarkable.
function syncStorageUrl() {
  if (activeTab !== "storage") return;
  history.replaceState({ tab: "storage" }, "", "/admin/ui/storage" + storageQuery());
}

// Apply a pending `{tablet, node}` (from a deep-link URL or a cross-view jump)
// to the Storage selects and load its detail. Consumed once — a routine
// refresh afterward must not keep re-forcing the selection over a manual
// change, the same discipline `lastRenderedDyTable` uses for the Dynamo editor.
function applyPendingStorageParams() {
  if (!pendingStorageParams) return;
  const { tablet, node } = pendingStorageParams;
  pendingStorageParams = null;
  if (tablet != null) {
    const tsel = $("st-tablet");
    if ([...tsel.options].some((o) => o.value === String(tablet))) tsel.value = String(tablet);
  }
  updateStorageNodeOptions();
  if (node != null) {
    const nsel = $("st-node");
    if ([...nsel.options].some((o) => o.value === node)) nsel.value = node;
  }
  loadStorage();
}

// Jump to the Storage tab pre-selecting `tablet` and/or `node` (either may be
// `null` to leave that selector as-is) — used by cross-links from the
// Tablets/Placement/Overview views so "which tablet/node is this" never means
// re-picking the same ids in two dropdowns by hand.
function gotoStorage(tablet, node) {
  const params = { tablet: tablet != null ? String(tablet) : null, node: node || null };
  activateTab("storage", { push: true, storage: params });
  pendingStorageParams = params;
  applyPendingStorageParams();
}

// Show `tab` and, unless `opts.silent` (used from the popstate handler, where the
// browser already changed the URL), sync the address bar: `push` adds a history
// entry (nav click), otherwise the URL is normalized in place (initial load).
// `opts.storage` overrides the Storage tab's query params (a cross-link jump);
// otherwise they're read from the selects' current values.
function activateTab(tab, opts = {}) {
  if (!TABS.includes(tab)) tab = TABS[0];
  activeTab = tab;
  document.querySelectorAll(".sidebar button.navlink").forEach((x) => x.classList.toggle("active", x.dataset.tab === tab));
  document.querySelectorAll("main section").forEach((x) => x.classList.toggle("active", x.id === tab));
  if (!opts.silent) {
    const query = tab === "storage" ? storageQuery(opts.storage) : "";
    const url = "/admin/ui/" + tab + query;
    if (opts.push) history.pushState({ tab }, "", url);
    else history.replaceState({ tab }, "", url);
  }
}
