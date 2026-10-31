/* Loader for the two real animusd web UIs inside demo.html (ADR 0082, 2026-10-31
   amendment): the data console (?app=console) and the operator dashboard
   (?app=dashboard). Their HTML/CSS/JS are copied unmodified from crates/animusd/src
   into demo/apps/<app>/ by scripts/build-web-demo.sh.

   This frame is same-origin with demo.html, which owns the cluster
   (window.parent.animusDemo). The loader rewrites the UI document to be
   self-contained (inline CSS and JS), points the address bar at the UI's own
   path (history.replaceState, so its location.pathname routing works), and
   prepends a shim that:
     - marks the document `data-embedded` (the UIs hide their own brand, theme switch
       and warnings, see "Embedded mode" in crates/animusd/CLAUDE.md) and copies the
       shell's theme onto it; the shell keeps it current (demo.js mirrors later changes);
     - answers fetch() of /console/api/* and /admin/* from the in-tab cluster;
     - turns navigations to /console/ui/* and /admin/ui/* (links, and the
       `location.href =` assignments the console makes) into a reload of this
       loader, because a static host has no such pages. */
(function () {
  "use strict";

  var DEFAULT_PATH = { console: "/console/ui/tables", dashboard: "/admin/ui/overview" };
  var FRAME_PATH = location.pathname;

  function fail(text) {
    var m = document.getElementById("msg");
    if (m) { m.textContent = text; }
  }

  // Runs inside the rewritten document, before any UI script. Stringified below,
  // so it must not close over anything: its inputs arrive as arguments.
  function shim(app, framePath) {
    var root = document.documentElement;
    root.setAttribute("data-embedded", "1");
    try {
      var theme = window.parent.document.documentElement.getAttribute("data-theme");
      if (theme) { root.setAttribute("data-theme", theme); }
    } catch (e) { /* cross-origin parent: keep the system theme */ }
    var api = function () { return window.parent && window.parent.animusDemo; };
    var realFetch = window.fetch.bind(window);
    var isApi = function (p) {
      return p.indexOf("/console/api/") === 0 || (p.indexOf("/admin/") === 0 && p.indexOf("/admin/ui/") !== 0);
    };
    var isUi = function (p) { return p.indexOf("/console/ui") === 0 || p.indexOf("/admin/ui") === 0; };
    window.fetch = function (input, init) {
      var req = input && typeof input === "object" && "url" in input ? input : null;
      var url = new URL(req ? req.url : String(input), location.href);
      if (!isApi(url.pathname)) { return realFetch(input, init); }
      var a = api();
      if (!a || !a.ready()) {
        return Promise.reject(new TypeError("the demo cluster is not running (start it on the demo page)"));
      }
      var method = ((init && init.method) || (req && req.method) || "GET").toUpperCase();
      var body = init && init.body != null ? String(init.body) : "";
      return a.http(a.nodeForHost(url.host === location.host ? "" : url.hostname), method, url.pathname + url.search, body)
        .then(function (r) {
          return new Response(r.body, { status: r.status, headers: { "content-type": r.contentType || "application/json" } });
        });
    };
    function go(target) {
      var u = new URL(target, location.href);
      if (u.pathname === location.pathname && u.search === location.search && u.hash) {
        location.hash = u.hash; // an in-page anchor: let the browser scroll
        return;
      }
      location.replace(framePath + "?app=" + app + "&path=" + encodeURIComponent(u.pathname + u.search + u.hash));
    }
    window.__animusGo = go;
    document.addEventListener("click", function (e) {
      if (e.defaultPrevented || e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) { return; }
      var a = e.target && e.target.closest ? e.target.closest("a[href]") : null;
      if (!a) { return; }
      var u = new URL(a.getAttribute("href"), location.href);
      if (!isUi(u.pathname)) { return; }
      e.preventDefault();
      go(u.href);
    }, true);
  }

  function inlineAssets(html, files, app) {
    // <link rel="stylesheet" href="/x/y.css"> and <script src="/x/y.js"></script>,
    // by basename, from the copies the build script made.
    html = html.replace(/<link\s+rel="stylesheet"\s+href="([^"]+)"\s*\/?>/g, function (m, href) {
      var css = files[href.split("/").pop()];
      return css === undefined ? m : "<style>" + css + "</style>";
    });
    html = html.replace(/<script\s+src="([^"]+)"\s*><\/script>/g, function (m, src) {
      var js = files[src.split("/").pop()];
      if (js === undefined) { return m; }
      // `location.href = x` (and the inline onclick the console renders) would leave
      // the page for a path a static host does not have; route it through the shim.
      js = js.replace(/window\.location\.href\s*=(?!=)\s*/g, "window.__animusHref = ");
      return "<script>" + js.replace(/<\/script/gi, "<\\/script") + "</script>";
    });
    return html;
  }

  async function text(url) {
    var r = await fetch(url);
    if (!r.ok) { throw new Error(url + ": HTTP " + r.status); }
    return r.text();
  }

  async function main() {
    var q = new URLSearchParams(location.search);
    var app = q.get("app") === "dashboard" ? "dashboard" : "console";
    var path = q.get("path") || DEFAULT_PATH[app];
    var parentApi = window.parent !== window ? window.parent.animusDemo : null;
    if (!parentApi || !parentApi.ready()) {
      fail("Start the demo cluster on the demo page first, then open this view from there.");
      return;
    }
    var base = "demo/apps/" + app + "/";
    var html = await text(base + "index.html");
    var names = [];
    html.replace(/(?:href|src)="([^"]+\.(?:css|js))"/g, function (m, f) { names.push(f.split("/").pop()); return m; });
    var bodies = await Promise.all(names.map(function (n) { return text(base + n); }));
    var files = {};
    names.forEach(function (n, i) { files[n] = bodies[i]; });
    html = inlineAssets(html, files, app);
    var shimTag = "<script>(" + shim.toString() + ")(" + JSON.stringify(app) + "," + JSON.stringify(FRAME_PATH) + ");" +
      // setter half of the `location.href =` rewrite
      "Object.defineProperty(window,'__animusHref',{set:function(v){window.__animusGo(v);}});</script>";
    html = html.replace(/<head>/i, function () { return "<head>" + shimTag; });
    history.replaceState(null, "", path);
    document.open();
    document.write(html);
    document.close();
  }

  main().catch(function (e) {
    fail("Could not load the interface: " + (e && e.message ? e.message : String(e)) +
      ". The demo build (scripts/build-web-demo.sh) writes it to demo/apps/.");
  });
})();
