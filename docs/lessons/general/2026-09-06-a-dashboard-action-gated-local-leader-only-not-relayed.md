# A dashboard action gated "local-leader-only, not relayed" server-side needs its own leader-address resolver — and for the CONTROL leader specifically, the existing cross-node fan-out already has it for free (docs/roadmap.md U-05, Node-tab action buttons)

The tablet-family buttons (previous slice) already established "target
whichever address the route actually requires, not a uniform `SEED`" —
Flush/Compact/Reconfigure resolve the tablet's own CP leader via
`tbLeaderBase`. The Node-tab family (`/admin/drain`, `/admin/member/
remove`) needed the identical discipline for a DIFFERENT leader: both
routes are `ClientCtx`-documented **local-control-leader-only, not
relayed** — posting to the wrong node doesn't forward, it just 409s. The
easy mistake here is reaching for a new probe (e.g. fetching `/admin/
control/members` and cross-referencing against something) when the
control leader is already sitting in data every view already has:
`STATE.nodes` (the same cross-node `/admin/peers` + per-node `/admin/*`
fan-out `loadAll()` performs for every tab) carries each node's own
`/admin/raft.is_leader` — the exact field `dashboard_core.js::
computeHealth()`'s own `controlLeader` already reads for the health pill.
A one-line `STATE.nodes.find(n => n.ok && n.raft && n.raft.is_leader)`
answers "which admin address do I post this to" with zero new requests,
mirroring `cpGroupsByTablet()`'s own "the data to answer this is already
in `STATE`, just index into it differently" precedent.

**General form**: before adding a leader-resolution helper for a new
gated action, check `STATE`/`SELF`'s existing shape for the fact you need
first — a per-node liveness/leadership flag from the standard fan-out is
usually already there, one `.find()` away, and reaching past it to build a
second live probe (or worse, a second admin route) duplicates work the
polling loop is already doing on your behalf. This generalizes past
control-plane leadership specifically: any "is this the X leader/owner"
question a dashboard action needs answered is worth checking against
`STATE.nodes`/`STATE.status` before writing a new fetch for it.
