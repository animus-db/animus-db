/* AnimusDB site behaviour — vanilla, no dependencies, no build step.
   Three small things: theme switch (three-state, persisted),
   copy-to-clipboard on code blocks, and scrollspy for the docs sidebar.
   Small-viewport nav is CSS-only (wrap + horizontal scroll) — no JS. */
(function () {
  "use strict";

  // ---- theme -------------------------------------------------------------
  // Three explicit, persisted states: "light", "dark", "system". Light is
  // the default when nothing is stored yet — system only takes over once a
  // viewer chooses it, at which point data-theme is removed entirely and
  // prefers-color-scheme decides.
  var STORE = "animusdb-theme";
  function stored() {
    try {
      var v = localStorage.getItem(STORE);
      return (v === "light" || v === "dark" || v === "system") ? v : "light";
    } catch (e) { return "light"; }
  }
  function apply(choice) {
    if (choice === "light" || choice === "dark") { document.documentElement.setAttribute("data-theme", choice); }
    else { document.documentElement.removeAttribute("data-theme"); }
  }
  // Applied immediately (before DOMContentLoaded, since this script is
  // deferred but still runs pre-paint) so there is no flash of the wrong theme.
  apply(stored());

  document.addEventListener("DOMContentLoaded", function () {
    var switchEl = document.querySelector(".theme-switch");
    if (switchEl) {
      var choices = Array.prototype.slice.call(switchEl.querySelectorAll("button[data-theme-choice]"));
      var syncPressed = function (active) {
        choices.forEach(function (b) {
          b.setAttribute("aria-pressed", String(b.getAttribute("data-theme-choice") === active));
        });
      };
      syncPressed(stored());
      choices.forEach(function (b) {
        b.addEventListener("click", function () {
          var choice = b.getAttribute("data-theme-choice");
          apply(choice);
          try { localStorage.setItem(STORE, choice); } catch (e) { /* private mode */ }
          syncPressed(choice);
        });
      });
    }

    // ---- copy buttons ----------------------------------------------------
    document.querySelectorAll(".code").forEach(function (block) {
      var pre = block.querySelector("pre");
      if (!pre) return;
      var btn = document.createElement("button");
      btn.className = "copy-btn";
      btn.type = "button";
      btn.textContent = "Copy";
      btn.setAttribute("aria-label", "Copy code to clipboard");
      btn.addEventListener("click", function () {
        // Strip shell prompts so a copied snippet pastes as runnable commands.
        var text = Array.prototype.map.call(pre.querySelectorAll(".line"), function (l) {
          return l.textContent;
        }).join("\n");
        if (!text) { text = pre.textContent; }
        text = text.replace(/^[$#] ?/gm, "").trim();
        var done = function () {
          btn.textContent = "Copied";
          btn.classList.add("done");
          setTimeout(function () { btn.textContent = "Copy"; btn.classList.remove("done"); }, 1600);
        };
        if (navigator.clipboard && navigator.clipboard.writeText) {
          navigator.clipboard.writeText(text).then(done, function () { btn.textContent = "Press ⌘C"; });
        } else {
          var ta = document.createElement("textarea");
          ta.value = text;
          document.body.appendChild(ta);
          ta.select();
          try { document.execCommand("copy"); done(); } catch (e) { /* nothing to do */ }
          document.body.removeChild(ta);
        }
      });
      block.appendChild(btn);
    });

    // ---- docs scrollspy --------------------------------------------------
    var links = document.querySelectorAll(".doc-nav a[href^='#']");
    if (!links.length || !("IntersectionObserver" in window)) return;
    var byId = {};
    var targets = [];
    links.forEach(function (a) {
      var el = document.getElementById(a.getAttribute("href").slice(1));
      if (el) { byId[el.id] = a; targets.push(el); }
    });
    var visible = new Set();
    var obs = new IntersectionObserver(function (entries) {
      entries.forEach(function (entry) {
        if (entry.isIntersecting) { visible.add(entry.target.id); }
        else { visible.delete(entry.target.id); }
      });
      var first = targets.filter(function (t) { return visible.has(t.id); })[0];
      if (!first) return;
      links.forEach(function (a) { a.classList.remove("active"); });
      byId[first.id].classList.add("active");
    }, { rootMargin: "-80px 0px -70% 0px", threshold: 0 });
    targets.forEach(function (t) { obs.observe(t); });
  });
})();
