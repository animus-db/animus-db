# A CSS/pill class name that is also read as a domain-semantic status token invites silent scope creep: reusing one for an unrelated visual purpose quietly gives that purpose the first status's meaning.

**A CSS/pill class name that is also read as a domain-semantic status token
invites silent scope creep: reusing one for an unrelated visual purpose
quietly gives that purpose the first status's meaning.** The Tablets view's
"over auto-split threshold" indicator was implemented as `pill("under-
replicated", "over " + threshold)` — reusing the `under-replicated` status
class purely because it happened to render orange. That's harmless-looking
until something (a filter, a rollup, a screenshot-driven bug report) reads
the class name as "this tablet actually lost redundancy," which it didn't —
it was just big. Fixed by introducing a presentation-only `.warn` pill class
distinct from any status the health rollup ever computes, so a "just a
warning color" use can never be mistaken for a data-risk status again.
**When a class/enum name serves double duty as both a CSS selector and a
domain value some other code branches on, a "just reuse it for the color"
shortcut is a latent correctness bug, not a style nit — give purely-visual
reuses their own name.** (`animusd::dashboard_tablets.js`, `dashboard.css`'s
`.warn`/`.forming`/`.quorum-lost` classes.)
