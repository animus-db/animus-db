# A form input inside a subtree that is rebuilt from scratch on every poll tick needs its own persisted-and-listened-to variable, not a recomputed default (docs/roadmap.md U-05, tablet-detail action buttons)

Adding Split/Reconfigure's text inputs to the Tablets tab's `#tb-detail`
card surfaced a form-persistence hazard the dashboard's two existing
precedents don't actually cover. `dashboard_core.js::render()` already
documents "rebuild the Dynamo editor's skeleton only when the effective
table changed... never on a routine refresh with the same selection, so
in-progress edits survive" — but that works by gating the *whole rebuild*
behind `dyTable !== lastRenderedDyTable`. The create-table form is a
second precedent, but it isn't rebuilt at all — it's static HTML in
`dashboard.html`, touched only by direct DOM reads/writes. `#tb-detail` is
neither: `renderTabletDetail` reconstructs its entire `innerHTML` on
*every* call, and it is called on every `renderTablets()` tick (this tab's
existing ~5s `loadAll()` poll cadence) regardless of which card is open or
whether anything about the tablet actually changed — by design, since the
lineage panel beside it (U-05's second slice) deliberately needs that same
per-tick rebuild to pick up a background split. Naively setting an
input's `value=` from "the tablet's current replicas" (Reconfigure) or a
plain empty string (Split) on every render would silently erase whatever
an operator had half-typed, every single poll interval — a real, current
bug in a first draft of this change caught by asking "what happens if I
start typing during a refresh," not by any test.

**The fix generalizes past this one card**: give the value its own
module-level variable, attach a plain `input` event listener (re-attached
each rebuild, since the whole subtree is fresh DOM) that keeps the
variable in sync on every keystroke, and read *that* variable back when
rebuilding — never recompute a "default" unconditionally. A default (here,
the tablet's live `replicas`) is applied only when the variable is in its
freshly-reset sentinel state (`null`, set by the selection-change handler,
not by the render function), so it seeds the field once per selection and
then gets out of the way. Any future gated-action input living inside a
poll-rebuilt card (the Node-tab and control-members-panel button PRs this
slice's own roadmap item queues up next) needs the identical treatment —
"does this subtree get rebuilt on a timer regardless of user activity" is
the question to ask before adding any editable control to it, not just
before adding a *button*.
