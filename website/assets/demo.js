/* AnimusDB in-browser demo (demo.html). Vanilla, no dependencies.
   Loads the wasm build of the real request path on "Start cluster", then
   drives its virtual clock from requestAnimationFrame. Real time lives only
   here: the page measures elapsed milliseconds and passes them to
   cluster.tick(); the wasm never reads a clock. */
(function () {
  "use strict";

  var T = "DynamoDB_20120810.";
  var SPEEDS = [0.25, 0.5, 1, 2, 5, 10, 25];
  var MAX_EVENTS = 40;

  var EXAMPLES = [
    { id: "create", label: "CreateTable (orders)", target: T + "CreateTable", body: {
      TableName: "orders",
      BillingMode: "PAY_PER_REQUEST",
      AttributeDefinitions: [
        { AttributeName: "customer_id", AttributeType: "S" },
        { AttributeName: "order_id", AttributeType: "S" }
      ],
      KeySchema: [
        { AttributeName: "customer_id", KeyType: "HASH" },
        { AttributeName: "order_id", KeyType: "RANGE" }
      ]
    } },
    { id: "put", label: "PutItem", target: T + "PutItem", body: {
      TableName: "orders",
      Item: {
        customer_id: { S: "c-1" },
        order_id: { S: "o-1001" },
        total: { N: "42" },
        status: { S: "paid" }
      }
    } },
    { id: "put2", label: "PutItem (second order)", target: T + "PutItem", body: {
      TableName: "orders",
      Item: {
        customer_id: { S: "c-1" },
        order_id: { S: "o-1002" },
        total: { N: "17" },
        status: { S: "open" }
      }
    } },
    { id: "get", label: "GetItem (consistent)", target: T + "GetItem", body: {
      TableName: "orders",
      Key: { customer_id: { S: "c-1" }, order_id: { S: "o-1001" } },
      ConsistentRead: true
    } },
    { id: "query", label: "Query (one customer)", target: T + "Query", body: {
      TableName: "orders",
      KeyConditionExpression: "customer_id = :c",
      ExpressionAttributeValues: { ":c": { S: "c-1" } },
      ConsistentRead: true
    } },
    { id: "scan", label: "Scan", target: T + "Scan", body: {
      TableName: "orders",
      ConsistentRead: true
    } },
    { id: "partiql", label: "PartiQL ExecuteStatement", target: T + "ExecuteStatement", body: {
      Statement: "SELECT * FROM \"orders\" WHERE customer_id = ?",
      Parameters: [{ S: "c-1" }],
      ConsistentRead: true
    } },
    { id: "list", label: "ListTables", target: T + "ListTables", body: {} },
    { id: "describe", label: "DescribeTable", target: T + "DescribeTable", body: { TableName: "orders" } },
    { id: "delete", label: "DeleteItem", target: T + "DeleteItem", body: {
      TableName: "orders",
      Key: { customer_id: { S: "c-1" }, order_id: { S: "o-1002" } }
    } }
  ];

  function $(id) { return document.getElementById(id); }

  var mod = null;          // the wasm-bindgen module
  var cluster = null;      // AnimusCluster
  var paused = false;
  var speedIdx = 2;
  var lastFrame = 0;
  var lastStatusAt = 0;
  var lastStatusJson = "";
  var status = null;
  var nodeCount = 0;
  var eventsBuf = [];
  var prevStatus = null;
  var rafId = 0;

  // ---- formatting ---------------------------------------------------------
  function fmtClock(ms) {
    return (ms / 1000).toFixed(3) + " s";
  }
  function fmtSpeed(i) {
    var s = SPEEDS[i];
    return (s < 1 ? String(s) : String(s)) + "x";
  }
  function el(tag, cls, text) {
    var e = document.createElement(tag);
    if (cls) { e.className = cls; }
    if (text !== undefined) { e.textContent = text; }
    return e;
  }

  // ---- events log ---------------------------------------------------------
  function logEvent(text) {
    var now = status ? status.virtual_time_ms : 0;
    eventsBuf.push({ t: now, text: text });
    if (eventsBuf.length > MAX_EVENTS) { eventsBuf.shift(); }
    renderEvents();
  }
  function renderEvents() {
    var ol = $("events");
    ol.textContent = "";
    for (var i = eventsBuf.length - 1; i >= 0; i--) {
      var li = el("li");
      li.appendChild(el("span", "t", "[" + fmtClock(eventsBuf[i].t) + "] "));
      li.appendChild(document.createTextNode(eventsBuf[i].text));
      ol.appendChild(li);
    }
  }

  // ---- node cards ---------------------------------------------------------
  var cards = [];
  function buildCards(n) {
    var grid = $("node-grid");
    grid.textContent = "";
    cards = [];
    for (var i = 0; i < n; i++) { cards.push(makeCard(i, grid)); }
    fillNodeSelects(n);
    nodeCount = n;
  }
  function makeCard(i, grid) {
    var root = el("div", "node");
    root.setAttribute("role", "listitem");
    var h = el("h4");
    h.appendChild(el("span", null, "Node " + i));
    var st = el("span", "state up", "up");
    h.appendChild(st);
    var badges = el("div", "badges");
    var tabs = el("p", "tabs");
    var acts = el("div", "acts");
    var crash = el("button", "btn small", "Crash");
    crash.type = "button";
    crash.setAttribute("aria-label", "Crash node " + i);
    var restart = el("button", "btn small", "Restart");
    restart.type = "button";
    restart.setAttribute("aria-label", "Restart node " + i);
    crash.addEventListener("click", function () { act(function () { cluster.crash(i); }, "node " + i + " crashed"); });
    restart.addEventListener("click", function () { act(function () { cluster.restart(i); }, "node " + i + " restarted"); });
    acts.appendChild(crash);
    acts.appendChild(restart);
    root.appendChild(h);
    root.appendChild(badges);
    root.appendChild(tabs);
    root.appendChild(acts);
    grid.appendChild(root);
    return { root: root, st: st, badges: badges, tabs: tabs, crash: crash, restart: restart };
  }
  function fillNodeSelects(n) {
    ["part-a", "part-b", "send-node"].forEach(function (id) {
      var sel = $(id);
      var keep = sel.value;
      sel.textContent = "";
      for (var i = 0; i < n; i++) {
        var o = el("option", null, "Node " + i);
        o.value = String(i);
        sel.appendChild(o);
      }
      if (keep !== "" && Number(keep) < n) { sel.value = keep; }
    });
    if ($("part-a").value === $("part-b").value && n > 1) { $("part-b").value = "1"; }
  }

  function act(fn, message) {
    if (!cluster) { return; }
    try {
      fn();
      logEvent(message);
      refreshStatus(true);
    } catch (e) {
      logEvent("error: " + (e && e.message ? e.message : String(e)));
    }
  }

  // ---- status -> DOM ------------------------------------------------------
  function refreshStatus(force) {
    if (!cluster) { return; }
    var json = cluster.status();
    $("clock").textContent = fmtClock(JSON.parse(json).virtual_time_ms);
    if (!force && json === lastStatusJson) { return; }
    lastStatusJson = json;
    status = JSON.parse(json);
    render(status);
    diffEvents(prevStatus, status);
    prevStatus = status;
  }

  function diffEvents(prev, cur) {
    if (!prev) { return; }
    if (prev.control_leader !== cur.control_leader) {
      logEvent(cur.control_leader === null ? "control plane has no leader" : "control leader is node " + cur.control_leader);
    }
    var prevLeaders = {};
    prev.tablets.forEach(function (t) { prevLeaders[t.id] = t.leader_node; });
    cur.tablets.forEach(function (t) {
      if (!(t.id in prevLeaders)) {
        logEvent("tablet " + t.id + " (" + (t.table || "?") + ") created");
      } else if (prevLeaders[t.id] !== t.leader_node) {
        logEvent("tablet " + t.id + " leader: " + (t.leader_node === null ? "none" : "node " + t.leader_node));
      }
    });
    if (cur.nodes.length > prev.nodes.length) {
      logEvent("node " + (cur.nodes.length - 1) + " joined");
    }
  }

  function render(s) {
    if (s.nodes.length !== nodeCount) { buildCards(s.nodes.length); }
    $("ctl-leader").textContent = s.control_leader === null ? "none" : "node " + s.control_leader;
    $("table-count").textContent = String(s.tables.length);
    $("tablet-count").textContent = String(s.tablets.length);
    $("pending").textContent = String(s.pending_requests);
    s.nodes.forEach(function (n, i) {
      var c = cards[i];
      c.root.className = "node" + (n.up ? "" : " down") + (n.control_leader ? " leader" : "");
      c.st.textContent = n.up ? "up" : "down";
      c.st.className = "state " + (n.up ? "up" : "dn");
      c.badges.textContent = n.control_leader ? "control leader" : "";
      c.tabs.textContent = "";
      if (!n.up) {
        c.tabs.textContent = "Not running.";
      } else {
        c.tabs.appendChild(document.createTextNode("Hosts " + n.hosted_tablets.length + ", leads " + n.led_tablets.length + " tablets"));
        if (n.led_tablets.length) {
          c.tabs.appendChild(document.createElement("br"));
          c.tabs.appendChild(el("span", "mono", "leads: " + n.led_tablets.join(", ")));
        }
      }
      c.crash.disabled = !n.up;
      c.restart.disabled = n.up;
    });
    var parts = s.partitions.map(function (p) { return "node " + p[0] + " and node " + p[1]; });
    $("partition-list").textContent = parts.length ? "Partitioned: " + parts.join("; ") + "." : "No partitions.";
    var body = $("tablet-rows");
    body.textContent = "";
    if (!s.tablets.length) {
      var tr0 = el("tr");
      var td0 = el("td", "muted", "No tables yet. Run CreateTable below.");
      td0.colSpan = 5;
      tr0.appendChild(td0);
      body.appendChild(tr0);
    }
    s.tablets.forEach(function (t) {
      var tr = el("tr");
      tr.appendChild(el("td", "mono", String(t.id)));
      tr.appendChild(el("td", null, t.table || ""));
      tr.appendChild(el("td", "mono", t.replicas.join(", ")));
      tr.appendChild(el("td", "mono", t.leader_node === null ? "none" : "node " + t.leader_node));
      tr.appendChild(el("td", null, t.state));
      body.appendChild(tr);
    });
  }

  // ---- the pump -----------------------------------------------------------
  function frame(now) {
    rafId = requestAnimationFrame(frame);
    var dt = lastFrame ? now - lastFrame : 0;
    lastFrame = now;
    if (!cluster) { return; }
    try {
      if (!paused) {
        cluster.tick(dt, SPEEDS[speedIdx]);
      }
      if (now - lastStatusAt > 200) {
        lastStatusAt = now;
        refreshStatus(false);
      }
    } catch (e) {
      fatal(e);
    }
  }

  // A Rust panic aborts the wasm instance, and the instance cannot be reused.
  // Say so and ask for a reload instead of failing silently.
  function fatal(e) {
    cluster = null;
    var err = $("load-error");
    err.textContent = "The cluster hit an internal error and stopped (" + (e && e.message ? e.message : String(e)) +
      "). Reload the page to start over; the browser console has the details.";
    err.hidden = false;
    $("start").disabled = true;
  }

  // ---- console ------------------------------------------------------------
  function currentExample() {
    var id = $("example").value;
    for (var i = 0; i < EXAMPLES.length; i++) { if (EXAMPLES[i].id === id) { return EXAMPLES[i]; } }
    return EXAMPLES[0];
  }
  function loadExample() {
    var ex = currentExample();
    $("target").textContent = ex.target;
    $("body").value = JSON.stringify(ex.body, null, 2);
  }
  function showResponse(headHtml, text) {
    var head = $("resp-head");
    head.textContent = "";
    headHtml(head);
    $("resp").textContent = text;
  }
  function prettyBody(b) {
    try { return JSON.stringify(JSON.parse(b), null, 2); } catch (e) { return b; }
  }
  function send() {
    if (!cluster) { return; }
    var body = $("body").value;
    try { JSON.parse(body); } catch (e) {
      showResponse(function (h) { h.appendChild(el("span", "bad", "Request is not valid JSON: " + e.message)); }, "");
      return;
    }
    var node = Number($("send-node").value);
    var target = $("target").textContent;
    var t0 = JSON.parse(cluster.status()).virtual_time_ms;
    showResponse(function (h) { h.appendChild(el("span", "muted", "Sent to node " + node + ", waiting" + (paused ? " (paused: resume to let it run)" : "") + "...")); }, "");
    var thisCluster = cluster;
    cluster.request(node, target, body).then(function (r) {
      if (cluster !== thisCluster) { return; }
      var t1 = JSON.parse(cluster.status()).virtual_time_ms;
      var good = r.status >= 200 && r.status < 300;
      showResponse(function (h) {
        h.appendChild(el("span", good ? "ok" : "bad", "HTTP " + r.status));
        h.appendChild(document.createTextNode(" from node " + node + " after about " + (t1 - t0) + " virtual ms (one frame is the resolution)"));
      }, prettyBody(r.body));
      logEvent(target.replace(T, "") + " to node " + node + ": " + r.status);
      refreshStatus(true);
    }, function (e) {
      showResponse(function (h) { h.appendChild(el("span", "bad", "Not sent: " + (e && e.message ? e.message : String(e)))); }, "");
    });
  }

  // ---- start --------------------------------------------------------------
  function nextFrame() { return new Promise(function (r) { requestAnimationFrame(function () { r(); }); }); }

  async function start(ev) {
    ev.preventDefault();
    var btn = $("start");
    var err = $("load-error");
    err.hidden = true;
    btn.disabled = true;
    var nodes = Number($("nodes-in").value);
    var seed = Number($("seed-in").value);
    if (!(seed >= 0) || Math.floor(seed) !== seed || seed > 9007199254740991) {
      err.textContent = "Seed must be a whole number from 0 up.";
      err.hidden = false;
      btn.disabled = false;
      return;
    }
    try {
      btn.textContent = "Loading...";
      if (!mod) {
        mod = await import("../demo/pkg/animus_web.js");
        await mod.default();
      }
      btn.textContent = "Starting cluster...";
      await nextFrame();
      await nextFrame();
      if (cluster) { cluster.free(); cluster = null; }
      cluster = new mod.AnimusCluster(seed, nodes);
    } catch (e) {
      err.textContent = "Could not start the cluster: " + (e && e.message ? e.message : String(e)) +
        ". The demo needs a browser with WebAssembly, and a build of the site that includes demo/pkg/.";
      err.hidden = false;
      btn.disabled = false;
      btn.textContent = cluster ? "Rebuild cluster" : "Start cluster";
      return;
    }
    status = null; prevStatus = null; lastStatusJson = ""; eventsBuf = []; nodeCount = 0;
    paused = false;
    $("pause").textContent = "Pause";
    $("pause").setAttribute("aria-pressed", "false");
    $("live").hidden = false;
    $("resp").textContent = "";
    $("resp-head").textContent = "";
    $("resp-head").appendChild(el("span", "muted", "No request sent yet."));
    refreshStatus(true);
    logEvent("cluster started: " + nodes + " nodes, seed " + seed);
    lastFrame = 0;
    if (!rafId) { rafId = requestAnimationFrame(frame); }
    btn.disabled = false;
    btn.textContent = "Rebuild cluster";
    $("start-note").textContent = "Rebuilding discards the current cluster and its data.";
    document.documentElement.setAttribute("data-demo-ready", "1");
  }

  document.addEventListener("DOMContentLoaded", function () {
    var sel = $("example");
    EXAMPLES.forEach(function (ex) {
      var o = el("option", null, ex.label);
      o.value = ex.id;
      sel.appendChild(o);
    });
    loadExample();
    sel.addEventListener("change", loadExample);
    $("setup").addEventListener("submit", start);
    $("send").addEventListener("click", send);
    $("speed").addEventListener("input", function () {
      speedIdx = Number($("speed").value);
      var label = fmtSpeed(speedIdx);
      $("speed-out").textContent = label;
      $("speed").setAttribute("aria-valuetext", label);
    });
    $("speed").max = String(SPEEDS.length - 1);
    $("speed").value = String(speedIdx);
    $("speed-out").textContent = fmtSpeed(speedIdx);
    $("pause").addEventListener("click", function () {
      paused = !paused;
      $("pause").textContent = paused ? "Resume" : "Pause";
      $("pause").setAttribute("aria-pressed", String(paused));
      logEvent(paused ? "paused" : "resumed");
    });
    $("partition").addEventListener("click", function () {
      var a = Number($("part-a").value), b = Number($("part-b").value);
      act(function () { cluster.partition(a, b); }, "partitioned node " + a + " from node " + b);
    });
    $("heal").addEventListener("click", function () {
      act(function () { cluster.healAll(); }, "healed all partitions and crashed nodes");
    });
    $("grow").addEventListener("click", function () {
      var b = $("grow");
      b.disabled = true;
      // Joining runs virtual seconds of work synchronously; let the label paint first.
      requestAnimationFrame(function () {
        act(function () { cluster.grow(); }, "added a node");
        b.disabled = false;
      });
    });
  });
})();
