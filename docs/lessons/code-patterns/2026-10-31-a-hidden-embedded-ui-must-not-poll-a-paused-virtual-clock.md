# A hidden embedded UI must not poll a virtual clock that is meant to be paused

When the demo's operator dashboard moved into an app shell it kept its 2 s auto-refresh while its
view was hidden. In the browser demo, a UI request made while the clock is paused runs the clock at
1x until it finishes (so a paused tab does not hang a UI), so the hidden poll silently un-paused the
cluster and `demo-browser-smoke.mjs` ("pause holds the clock") failed. A fault-feedback nudge on a
timer (`setTimeout` after Crash) had the same effect when the dashboard was not the active view.

Rule: an embedded UI that polls takes a host-set "hidden" flag (`data-embedded-hidden` on `<html>`)
and skips the poll; any host-side timer that pokes a frame checks the active view first; a view
refreshes when it is shown. More generally, a keep-alive rule ("requests advance time") needs every
background requester gated on visibility, not just the foreground ones. Test the invariant (pause
holds the clock) with every UI loaded, not just the shell.

Also: embedded mode belongs in the real UI sources behind one attribute, never as a patched copy in
the demo build, so the served UIs and the demo cannot drift.
