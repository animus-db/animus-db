/* AnimusDB site behaviour — vanilla, no dependencies, no build step.
   Four small things: theme toggle (persisted), mobile nav, copy-to-clipboard
   on code blocks, and scrollspy for the docs sidebar. */
(function () {
  "use strict";

  // ---- theme -------------------------------------------------------------
  var STORE = "animusdb-theme";
  function stored() {
    try { return localStorage.getItem(STORE); } catch (e) { return null; }
  }
  function apply(theme) {
    if (theme) { document.documentElement.setAttribute("data-theme", theme); }
    else { document.documentElement.removeAttribute("data-theme"); }
  }
  function current() {
    var explicit = document.documentElement.getAttribute("data-theme");
    if (explicit) return explicit;
    return window.matchMedia("(prefers-color-scheme: light)").matches ? "light" : "dark";
  }
  apply(stored());

  document.addEventListener("DOMContentLoaded", function () {
    var btn = document.querySelector(".theme-btn");
    if (btn) {
      btn.addEventListener("click", function () {
        var next = current() === "dark" ? "light" : "dark";
        apply(next);
        try { localStorage.setItem(STORE, next); } catch (e) { /* private mode */ }
        btn.setAttribute("aria-label", "Switch to " + (next === "dark" ? "light" : "dark") + " theme");
      });
    }

    // ---- mobile nav ------------------------------------------------------
    var toggle = document.querySelector(".nav-toggle");
    var nav = document.querySelector("nav.main");
    if (toggle && nav) {
      toggle.addEventListener("click", function () {
        var open = nav.classList.toggle("open");
        toggle.setAttribute("aria-expanded", String(open));
      });
      nav.addEventListener("click", function (e) {
        if (e.target.tagName === "A") {
          nav.classList.remove("open");
          toggle.setAttribute("aria-expanded", "false");
        }
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
