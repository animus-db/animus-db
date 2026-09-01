# ADR 0056 — One design system for the website and the in-product consoles ("Readout")

- **Status:** Accepted — implemented (token layer, typography and the panel
  recipe; per-component polish is a follow-up, see *Follow-ups* below).
  **Revised in place by its own 2026-08-25 amendment**: the direction below
  ("Readout" — dark-first, glow-means-live) was replaced by a second pass,
  **"Ledger"** (light-first, keyline-means-live) — same file, same ADR
  number, per-component polish delivered as part of the revision rather
  than as this ADR's own still-open follow-up. See that amendment for the
  full decision and for which statements below it supersedes. **Its own
  type pairing is superseded in turn by a 2026-08-31 amendment**: Space
  Grotesk/Martian Mono are replaced by Work Sans/JetBrains Mono as
  groundwork for a further visual-direction change; Ledger's own
  colour/geometry/component language is untouched by that amendment. **The
  website half of that further visual-direction change — geometry and
  component treatment, not colour — lands in a second, same-day 2026-08-31
  amendment**: larger radii, a plain uniform card border in place of the
  ink top-rule, and the ink-plate concept (forced-dark terminal/code
  surface plus its offset drop shadow) removed outright in favour of an
  ordinary bordered block on the theme's own surface. The two in-product
  consoles were untouched by that amendment, keeping Ledger's original
  geometry pending their own follow-up PR. See that amendment for the full
  decision. **A third, same-day 2026-08-31 amendment (below) lands that
  follow-up**: the same geometry and component treatment, applied to both
  consoles at the app skin's own, denser density.
  **This closes out the restyle series** begun by the two prior amendments:
  fonts groundwork (first 2026-08-31 amendment), the website skin (second
  2026-08-31 amendment), and now the two in-product consoles' own skin
  (third 2026-08-31 amendment, below) — all three surfaces this ADR governs
  now share the plainer, more traditional developer-tool geometry and
  component language. **A 2026-09-01 amendment (below) is a small follow-up
  polish on top of that closed-out series**, not a new leg of it: heavy
  `var(--text)`-colored structural borders left in place by the series
  above are softened to quiet 1px hairlines, and the callout/warnbox/pull
  left-accent borders are reduced 3px→2px — a maintainer visual-feedback
  fix, not a further direction change.
- **Date:** 2026-08-23
- **Amends:** [ADR 0021](0021-web-dashboard.md) (dashboard styling),
  [ADR 0052](0052-data-console-port.md) (data console styling).

## Context

Three surfaces present AnimusDB to humans: the marketing site
(`website/`), the operator dashboard (`crates/animusd/src/dashboard.*`) and the
data console (`crates/animusd/src/console.*`). All three already shared a
*family* of values — the same near-black, the same warm paper, the same blue
accent — but they shared them by **copy-paste**: three near-identical `:root`
blocks, and buttons, pills, cards and tables defined three times over. Nothing
prevented them drifting, and they had already started to.

Two further problems were named when the system was commissioned:

1. **Typography had no character.** Every surface ran a system font stack, so
   nothing distinguished AnimusDB from any other Rust infrastructure project.
2. **The two densities are real and should stay.** The site is read at leisure,
   often on a phone; the consoles are scanned all day on a desk. A single
   density serves neither. The requirement was explicitly *"two design systems
   with a common base"*.

## Decision

### The direction: Readout

Of three directions explored, and three variants of the chosen one, the
selected direction is **Readout**: the interface as an instrument with a
housing. Panels are dark glass with a lit top edge; live values glow; figures
are promoted to display type in a wide mono.

The load-bearing rule is **glow means live**. A glow is reserved for state —
a live value, the focused panel's top edge, the one primary action, an active
status dot. Static content never glows, which is what makes a glow readable as
information rather than decoration. Anything that glows therefore needs a real
"is this live" signal behind it, not a style choice.

A glow on paper reads as a printing fault, so in the light theme every
illuminated element substitutes a 2px accent keyline in the same position
(`--live-underline`), and the glow tokens resolve to `none`. The information
survives the theme switch; the mechanism does not.

### Type

**Space Grotesk** (UI, display) and **Martian Mono** (figures, identifiers,
commands, column labels). Both are OFL variable fonts, so the Latin subset is
one file per family covering the whole weight range — 22 KB and 23 KB
respectively, not per weight.

The rule that keeps the pairing meaningful: *Space Grotesk carries every word a
human wrote; Martian Mono carries everything a machine produced.* A number a
person typed into a form is Space Grotesk; the same number read back from the
cluster is Martian Mono. Figures are always `tabular-nums`.

### One base, two skins

`tokens.css` holds what is genuinely identical everywhere: the colour palette
in both themes, the two families, status colours, glow, motion. It does **not**
hold geometry. Radius, spacing rhythm, control height and type sizes are the
*skin*, and each surface sets its own on top of the shared base:

| | site skin | app skin |
|---|---|---|
| Body | 17px / 1.65 | 13px / 1.5 |
| Radius | 12px (14 on panels) | 6px (10 on panels) |
| Controls | 44px | 28px |
| Rhythm | 8 · 16 · 24 · 32 · 48 · 64 · 96 | 4 · 8 · 14 · 22 · 32 |

### Why the token file is duplicated, and what stops it drifting

The site ships static files; the consoles embed their assets in the binary with
`include_str!` and serve them from the node. There is no runtime the two can
share, and ADR 0021 rules out the build step that would generate one. So
`tokens.css` exists twice, verbatim — and `dashboard::tokens_css_matches_website_copy`
fails the test gate if the two copies differ by a single byte. Duplication
without a check is exactly the drift this ADR exists to stop; the check is the
test.

Font *delivery* legitimately differs by surface and is therefore **not** part
of that check. The site links both families from Google Fonts, which is what a
public web page should do — cached across the web, nothing in the repo.

The consoles cannot, and this is a functional constraint rather than a stylistic
one: they are served from cluster nodes, and the intended deployment target is a
Kubernetes operator keeping node traffic cluster-internal. On an air-gapped or
egress-restricted cluster a `<link>` to fonts.googleapis.com renders the admin UI
in fallback faces, and on any cluster it makes every operator's browser call a
third party when they open an admin page. So they carry the identical faces as
base64 `data:` URIs — ~61 KB per stylesheet — which also keeps **ADR 0021's rule
unchanged for the surfaces it was written about**. The `.woff2` sources live at
`crates/animusd/src/fonts/`.

Serving real `.woff2` from the consoles would mean refactoring the HTTP layer's
static-asset responses from `String` to bytes — deliberately not done here.

Each console's served stylesheet is now `concat!` of three `include_str!`
literals — fonts, tokens, skin — so it stays a single compile-time constant
with no bundler and no extra route.

## Consequences

- ADR 0021's rule is intact for the consoles it governs: they still fetch
  nothing at runtime. The rule bans external fetches, not webfaces — embedding
  a face always satisfied it, which is why the previous system-font stacks were
  a self-imposed constraint rather than a required one.
- The binary grows by ~61 KB of base64 font data per console stylesheet.
- The dim grey tier now floors at **60% alpha**. Below that, the 9.5px mono
  labels this system uses fail WCAG AA on `#07080a`, so the old 0.4-alpha
  "quiet" greys are no longer available — quietness comes from size and weight.
- Drop shadows are gone system-wide. Depth is the panel gradient plus the lit
  top edge, which is why panels can sit flush without muddying.

## Follow-ups

1. Per-component Readout polish: the glow on live values in the dashboard's
   stat tiles and the console's row states, and the light-theme keyline
   substitution wired through each of them. This ADR lands the token layer,
   the typography and the panel recipe; the per-component pass is deliberately
   separate to keep the diff reviewable.
2. The readout tile as a shared component shape across both consoles.
3. Revisit serving real `.woff2` from the consoles if the static-asset layer
   ever grows a binary response path.

## Amendment (2026-08-25) — the "Ledger" revision replaces Readout in place

Readout shipped its token layer, typography and panel recipe (Accepted,
above) with per-component polish named as this ADR's own open follow-up
(#1). Before that follow-up landed, the direction itself was replaced —
**in place, same file, same ADR number** — by a second design pass:
**Ledger**. This amendment records the new decision and closes out
follow-ups #1 and #2 as delivered by it, rather than as originally
planned.

### The direction changes: Readout → Ledger

Where Readout was "the interface as an instrument with a housing" — dark
glass, a lit top edge, glowing live values — **Ledger is the interface as
a paper record**: flat chalk-paper surfaces, ink rules, stamped marks. The
brief that produced it was explicit that the Readout mockups read as
*decorative* rather than *systemic* — glow and gradient are effects
applied to a panel, not consequences of a rule — and asked for a direction
whose emphasis mechanism survives unchanged into a medium (paper, print,
a light theme) that a glow cannot.

**Light is now the default theme — this ADR's dark-first decision is
reversed.** `tokens.css`'s bare `:root` carries the light ("chalk paper")
values directly; `[data-theme="dark"]` and the guarded
`prefers-color-scheme: dark` media query both override to the dark ("ink")
values, exactly swapped from Readout's arrangement (`:root` dark,
`[data-theme="light"]` the override). The reasoning tracks the direction
change directly: an instrument housing is naturally dark (a lit panel in a
dark chassis), a paper ledger is naturally light — a system whose own
central metaphor is print reads as backwards defaulted to dark. **Dark
remains a first-class theme, not a filter or an afterthought**: every
token gets a real, independently-chosen dark value (not a computed
inverse), and the explicit-choice / system-preference switch mechanism
ADR 0021's theme toggle already drove is completely unchanged — only which
theme sits in the unqualified `:root` moved.

**Keyline-means-live replaces glow-means-live as the system's one emphasis
device, in both themes.** Readout's rule ("Decision," above) was glow on
dark, substituted for a keyline only in the light theme so a glow on paper
wouldn't read as a printing fault. Ledger drops the glow half entirely:
there is no dark-glass panel to glow inside of, so the 2px solid accent
underline (`--live-underline`, the `.live` class) is the *only* emphasis
device in either theme, not a light-theme fallback for one. The rule it
enforces is unchanged from Readout's own: reserved for real state (a live
value, an active nav/tab), never applied to static content — what changed
is that there is now one mechanism to get right, not two kept in sync
across a theme switch. The `--glow-*` tokens stay in `tokens.css`,
resolving to `none` in both themes; they were already dead weight in
Readout's own light theme and are unused everywhere now, kept only so
their names don't dangle for whatever consumer history refers to them.

**One accent, one role.** Ledger states explicitly what Readout's palette
implied but never wrote down: `--accent` has exactly one job across the
whole system — links, the live/active keyline, and the hatch fill below —
and never appears as a filled control background. A primary/CTA button
fills with **ink** (`--text`'s color, inverted for contrast), not accent;
see `docs/engineering-lessons.md`'s 2026-08-25 entry on the token-rewrite
project for the one place this rule and the mockups' own literal markup
disagreed with a naive "just repoint the token" read of the rename.

**Component language.** Readout's panel recipe — dark glass, a lit top
edge, drop shadows for depth (Consequences, above: "Depth is the panel
gradient plus the lit top edge") — is gone entirely; Ledger has **no**
panel gradients and **no** blur/soft shadows anywhere. In its place:

- Cards carry a flat `--surface` fill, a 1px rule border, and a 2px solid
  ink top rule (`border-top: 2px solid var(--text)`); section heads get a
  2px ink bottom rule instead. Depth comes from these hairline rules, not
  from gradient or shadow.
- Pills/badges are a **stamped outline** — 1–1.5px solid current-color,
  transparent fill, mono uppercase — replacing whatever filled/glowing
  badge treatment a live status used to get.
- A bar/gauge fill is **hatched**: a 45° `repeating-linear-gradient` of
  accent and `--hatch`, not a flat or gradient fill.
- A terminal/log-shaped surface (the dashboard's Streams tail-records log)
  gets an **ink-plate** treatment: a dark interior (`--plate-bg`,
  `--plate-text`, ...) that stays the *same* dark values in both the light
  and dark theme — the one deliberate exception to "light is the default
  surface" because a terminal reads as a terminal by staying dark
  regardless of the surrounding page — bordered and given a **hard, offset
  drop shadow** (`--plate-shadow: 5px 5px 0 rgba(...)`, no blur radius) for
  the one place in the system that does use a shadow at all: it reads as a
  stamped/pasted object, not as elevation.

**The old "60% alpha grey floor" is superseded by a fixed six-stop
ramp.** Readout's Consequences noted a WCAG-driven floor on its grey tier;
Ledger replaces the whole approach with the ink-alpha ramp snapped to six
fixed stops (`--ink-85`/`72`/`55`/`50`/`30`/`16`, aliased onto the
pre-existing `--text2`/`--text3` names) rather than a single floored
value — chosen directly against the new palette's own contrast ratios,
not derived from Readout's dark-on-`#07080a` figures, which no longer
apply now that the values they were computed against are gone.

### What is unchanged

- **Token *names* are stable.** Every name introduced by this ADR's
  original "One base, two skins" design — `--bg`, `--surface`, `--text`,
  `--accent`, `--ok`/`--warn`/`--danger`, the `--glow-*` family, `--panel`,
  `--shadow*` — still exists; only values (and, for the emphasis-device
  tokens, meaning) changed. No consumer stylesheet or script needed a
  rename.
- **The two-copy `tokens.css` mechanism, and the drift test, are
  unchanged.** `crates/animusd/src/tokens.css` and
  `website/assets/tokens.css` are still byte-identical copies, still
  enforced by `dashboard::tokens_css_matches_website_copy`; the "one base,
  two skins" split (site skin vs. app skin geometry) and the reasoning for
  duplicating the base file rather than sharing a build step are unchanged
  from the original Decision.
- **Fonts are unchanged**: Space Grotesk (UI, display) and Martian Mono
  (figures, identifiers, commands) remain the pairing and the *"a human
  wrote it vs. a machine produced it"* rule for which face applies where;
  delivery still splits by surface (Google Fonts on the public site,
  base64 `data:` URIs embedded in the two consoles) for the identical
  reason this ADR originally gave (ADR 0021's no-CDN-fetch rule, and the
  Kubernetes egress-restricted deployment target). **Superseded by the
  2026-08-31 amendment, below**: the specific faces change to Work
  Sans/JetBrains Mono; the role split and the delivery-mechanics
  reasoning stated here do not.

### Statements above now superseded

Kept above as originally written, describing the system as first designed
and shipped — the following are historical as of this amendment, not
current:

- "the selected direction is **Readout**... Panels are dark glass with a
  lit top edge; live values glow" (Decision, "The direction: Readout") —
  superseded; the shipped direction is Ledger, no dark-glass panels, no
  glow.
- "The load-bearing rule is **glow means live**" and the light-theme
  keyline-substitution paragraph immediately after it — superseded by
  keyline-means-live as the sole mechanism in both themes, above.
- "Drop shadows are gone system-wide. Depth is the panel gradient plus the
  lit top edge..." (Consequences) — Ledger has no panel gradient at all;
  depth comes from ink rules, and the one shadow the system does use (the
  ink-plate's hard offset shadow) is a deliberate exception, not a
  contradiction of "no shadows."
- "The dim grey tier now floors at 60% alpha..." (Consequences) —
  superseded by the fixed six-stop ink-alpha ramp, above.
- Follow-up #1 ("Per-component Readout polish... the light-theme keyline
  substitution wired through each of them") and follow-up #2 (the readout
  tile as a shared component shape) — both **delivered**, by this
  revision rather than as separately-scoped follow-up work: the dashboard
  and console skins were reskinned wholesale onto the Ledger component
  language above (cards, pills, hatch bars, the ink-plate) rather than
  receiving Readout's own glow/keyline polish. Follow-up #3 (real
  `.woff2` from the consoles) is untouched by this revision and stays
  open.

### Related renames

The operator dashboard and data console were also rebranded as part of
this same delivery — "AnimusDB Console" → **animusd admin**, "AnimusDB
Data Console" → **animusd console** — a naming change, not a design-system
one; see [ADR 0021](0021-web-dashboard.md)'s and
[ADR 0052](0052-data-console-port.md)'s own 2026-08-25 amendments.

## Amendment (2026-08-31) — type pairing: Space Grotesk/Martian Mono → Work Sans/JetBrains Mono

A visual-canvas exploration (outside this repo) validated a further
restyle away from Ledger's own type toward a plainer, more traditional
developer-tool look. This amendment lands **only** the shared font/token
groundwork the rest of that restyle builds on — **the type pairing
changes; nothing else in Ledger does.** Colour (`--accent`'s `#2f4da8`/
dark-theme `#8fa7e6` and every other existing colour token), geometry,
radius, spacing, and shadows are all untouched here — those are a
separate, later change (see "What this groundwork is not," below).

### The pairing changes: Space Grotesk/Martian Mono → Work Sans/JetBrains Mono

`--font-ui` becomes **Work Sans** (UI, display), replacing Space Grotesk;
`--font-mono` becomes **JetBrains Mono** (figures, identifiers, commands,
column labels), replacing Martian Mono. **The role split this ADR's
original Decision established is unchanged**: *the face a human wrote
carries `--font-ui`; the face a machine produced carries `--font-mono`* —
only which two faces fill those roles changes. Both new faces are
open-source variable fonts, same as the pair they replace (Work Sans is
OFL; JetBrains Mono is Apache-2.0), so the "Latin subset, one file per
family covering the whole weight range" property (Decision, "Type",
above) carries over unchanged — the consoles still embed exactly two
`.woff2` files, not one per weight. The embedded pair's total size grows
from ~61 KB of base64 to ~118 KB (Work Sans's and JetBrains Mono's own
variable-font builds are each larger than Space Grotesk's/Martian Mono's
were) — noted as a real cost, not treated as a regression: it is still
one file per family, and the ADR 0021 no-CDN-fetch reasoning that
justifies embedding at all doesn't scale with face size.

### Font delivery is unchanged

Delivery mechanics still split by surface for the identical reason this
ADR has given twice already (original Decision's "Why the token file is
duplicated" section; the 2026-08-25 amendment's "What is unchanged"): the
website links Work Sans and JetBrains Mono from Google Fonts (`website/*.html`'s
`<head>`, weights matched to actual usage — `wght@400;500;600;700` for
Work Sans, `wght@400;500;600` for JetBrains Mono, verified against
`site.css`'s own `font:`/`font-weight:` declarations rather than carried
over from the old pair's weight list unexamined); the two consoles embed
the same two faces as base64 `data:` URIs in `crates/animusd/src/fonts.css`,
sourced from `crates/animusd/src/fonts/work-sans-latin.woff2` and
`crates/animusd/src/fonts/jetbrains-mono-latin.woff2`, for ADR 0021's
unchanged no-CDN-fetch/air-gapped-cluster reason. The `concat!`
(fonts + tokens + skin → one served stylesheet constant, `dashboard::CSS`/
`console::CSS`) is untouched.

### What this groundwork is not

This amendment is deliberately narrow — **fonts and the tokens that name
them, nothing else.** It is groundwork for a broader visual-direction
change (skin, geometry, component language) that lands in follow-up PRs:
the website's own skin (`website/assets/site.css`) in one PR, the two
consoles' skin (`dashboard.css`/`console.css`) in another. Neither is part
of this amendment; Ledger's colour palette, radius/spacing rhythm,
control heights, panel recipe (flat `--surface` fill, ink rules, stamped
pills, hatch fills, the ink-plate), and every other statement in the
2026-08-25 amendment above stay exactly as that amendment left them until
those follow-ups land.

## Amendment (2026-08-31) — website skin: larger radii, plain-bordered cards, the ink-plate removed

The 2026-08-31 type-pairing amendment above named its own follow-ups: the
website's skin in one PR, the two consoles' skin in another. This
amendment lands the first — **`website/assets/site.css` and the inline
logo mark in all 11 website pages, geometry and component treatment
only.** It was validated the same way the type-pairing change was, via a
design-canvas exploration outside this repo. **Colour is explicitly
unchanged**: every value in `website/assets/tokens.css` — light and dark
— stays exactly as the 2026-08-25 amendment left it; every rule below is
built from the existing `var(--...)` tokens, none from a literal hex or
`rgb()`/`rgba()` value. The two in-product consoles keep Ledger's
original geometry (3px radius, the card ink top-rule, the ink-plate)
until their own follow-up PR.

### Radius: 3px/4px → 6px/8px

`--radius` (buttons, inputs, small chips — the inline `code` element, the
plate/code copy button) moves from 3px to **6px**; `--radius-panel`
(cards, plates, the diagram wrapper) moves from 4px to **8px**. Functional
status pills (below) move off the `--radius` scale entirely, to a full
999px pill shape — the point is that they read as a distinct chip
category, not that they share the control radius.

### Cards: the ink top-rule is dropped for a plain, uniform border

`.lcard` and `.card` (the legacy alias) drop
`border-top: 2px solid var(--text)` — Ledger's accent-rule-on-top
treatment — for a plain `1px solid var(--border)` on all four sides, at
the new 8px panel radius. This is scoped to the two card recipes
specifically; the thick 2px ink rule survives everywhere else it was
already a section-boundary device rather than a card treatment — the
header's bottom border, the footer's top border, `.stat-row`'s top
border, and an article/doc `h2`'s bottom border are all untouched. The
decorative `.lcard .tag` label (the "MIGRATION"/"REGULATION"/
"ENVIRONMENT" tags on the homepage's "who it's for" cards, and similar
category tags elsewhere) already rendered as a plain `color: var(--accent)`
mono-uppercase text label with no pill/border — the treatment the
validated direction asked for — so it needed no change; noted here only
because the brief called it out explicitly as something to check.

### The ink-plate concept is removed, not tweaked

Ledger's `.plate`/`.code` terminal-and-diff-block recipe (2026-08-25
amendment, "Component language") is gone: a permanently dark interior
(`--plate-bg`/`--plate-text`/`--plate-comment`/`--plate-output`/
`--plate-accent`/`--plate-header`/`--plate-rule`, identical in both
themes "because a terminal reads as a terminal by staying dark") plus a
hard 5px-offset drop shadow (`--plate-shadow`) — the one shadow exception
Ledger's own Decision carved out. Both are gone, not adjusted:

- `.plate`/`.code` now sit on the ordinary themed surface
  (`var(--surface)`) with a plain `1px solid var(--border)`, at the new
  panel radius, and **no shadow at all**.
- Every span-level colour a code/terminal block used — comment (`.c`),
  output (`.o`), prompt/path/string accents (`.p`/`.s`), the base
  line/keyword colour (`.k`), and the copy button — now reads from the
  ordinary ink-alpha ramp and `--text`/`--accent` instead of the
  `--plate-*` family: comments to `--ink-55`, output to `--ink-72`,
  prompt/path/string highlights to `--accent`, base text to `--text`. The
  diff-line colours (`.dm`/`.dp`, the minus/plus rows in the "client
  configuration" diff block) move off their two literal hex values
  (`#d98a7a`/`#6fc493`, chosen to hold constant against a permanently-dark
  plate) onto `var(--danger)`/`var(--ok)`, which already carry independent
  light/dark values.
- The copy button's own literal `rgba(233, 229, 220, …)` fill/border
  (the plate-text colour at low alpha, since it had no other token to
  read from on a forced-dark surface) moves to `var(--surface2)`/
  `var(--border)`, and its "done" state to `var(--ok)`.
- `--plate-*` and `--code-bg` stay defined in `tokens.css` — untouched, in
  the same "kept only so the name doesn't dangle" spirit the surviving
  `--glow-*` tokens have had since the 2026-08-25 amendment — but nothing
  in `website/assets/site.css` reads them any more. Not deleted here
  because `tokens.css` is the byte-identical, test-enforced shared base
  with the two consoles (this ADR's original Decision, "Why the token
  file is duplicated"), and the consoles still use the ink-plate treatment
  pending their own follow-up PR.

The `.diagram-plate` wrapper (the compact two-planes strip and the fuller
write-path diagram) gets the same border/radius/no-shadow treatment as
`.plate`/`.code` — dropping its own `5px 5px 0` hard shadow and the
dark-theme override that recomputed it. This is the wrapper only: the
diagrams' internal SVG shapes and classes (`.dg-bx`, `.dg-card`, `.dg-t`,
`.dg-req`, `.dg-leader`, …) were already `var(--...)`-based and are
untouched, per the brief.

### Functional status pills stay a distinct chip; decorative tags do not

`.pill` (the compatibility table's Supported/Opt-in/Out-of-scope chips,
and the install page's port-trust column) moves off Ledger's
stamped-outline treatment (`1.5px solid currentColor`, transparent fill)
to a filled, tinted pill — `var(--ok-bg)`/`var(--warn-bg)`/
`var(--danger-bg)` (the same tokens the compatibility/ports tables'
`.pill.yes`/`.opt`/`.out` variants already had color-only access to) at a
999px radius. The point named in the brief holds: a functional status
signal keeps a distinct chip shape carrying real meaning through colour
*and* shape, where a decorative category tag (`.lcard .tag`, above) is
plain text with neither.

### The kicker "// " flourish: considered, not adopted

The validated direction's "// " comment-style prefix for `.kicker` was
evaluated against every existing use of the class across the 11 pages —
section eyebrows ("Why this exists," "The two planes"), but also a page
category label doubling as identity ("Architecture," "Compatibility"), an
"On this page" TOC heading, an inline tagline ("Open source · Rust ·
AGPL-3.0"), and a "Keep reading" article-footer label. A single `::before`
prefix reads naturally on the first group and oddly on the rest, and
`.kicker` has no markup hook to distinguish them without a rework the
brief explicitly said to skip in that case. Left as plain text; only the
radius/font changes already tracked through it (via `var(--font-mono)`,
untouched by this amendment) carry through.

### The logo mark: 24px → 36px

The header `.brand` link's inline SVG (the 19-circle hexagonal
node-cluster mark, `viewBox="-43 -43 86 86"`) moves from
`width="24" height="24"` to `width="36" height="36"` in all 11 pages
(8 top-level pages plus the 3 `articles/`). Only the two attributes
change; the mark's own circle coordinates and colours are untouched.

### Verified

Every colour value touched by this amendment traces to a `var(--...)`
token already defined (with independent light and dark values) in
`tokens.css` — `grep -rEn '#[0-9a-fA-F]{3,8}\b' website/assets/site.css`
returns nothing. `tokens.css` itself needed no new tokens and was not
touched: this amendment removes the plate's need for `--plate-*`
entirely rather than adding a replacement role, exactly as anticipated
above ("What this groundwork is not"). Checked with a real headless
browser render (Playwright against Chromium, driven through the site's
own `.theme-switch` toggle — not just `prefers-color-scheme` — since
light is the explicit default the site's own JS stamps before paint) of
the homepage, install, compatibility and architecture pages in both
themes: cards, terminal/diff blocks, the diagram plates, status pills,
decorative tags, and the header logo all render as described, with no
console errors other than the expected offline Google Fonts fetch
failures in a network-restricted sandbox.

### Statements above now superseded

For the website only — the two consoles keep the geometry and component
language described below until their own follow-up PR, so none of these
are superseded for `dashboard.css`/`console.css`:

- The "One base, two skins" table's site-skin **Radius** row (Decision,
  "One base, two skins" — "12px (14 on panels)") — already stale against
  the shipped Ledger code before this amendment (3px/4px), and superseded
  now in the other direction: the website's actual shipped values are
  6px (`--radius`) and 8px (`--radius-panel`). The table's app-skin
  column is untouched and still describes the consoles as they ship
  today.
- "Cards carry a flat `--surface` fill, a 1px rule border, and a 2px
  solid ink top rule (`border-top: 2px solid var(--text)`)" (2026-08-25
  amendment, "Component language") — superseded for the website's
  `.lcard`/`.card`: the top rule is dropped for a plain border on all
  four sides, above. Still accurate for the two consoles.
- "A terminal/log-shaped surface … gets an **ink-plate** treatment: a
  dark interior … bordered and given a **hard, offset drop shadow**"
  (2026-08-25 amendment, "Component language") — superseded for the
  website's `.plate`/`.code`/`.diagram-plate`: removed outright, above,
  not merely restyled. Still accurate for the two consoles.
- "Pills/badges are a **stamped outline**" (2026-08-25 amendment,
  "Component language") — superseded for the website's `.pill`: moved to
  a filled, tinted pill, above. Still accurate for the two consoles.

## Amendment (2026-08-31) — the two consoles' skin: proportional radii, plain-bordered cards, the ink-plate removed, filled pills

The 2026-08-31 type-pairing amendment named two follow-ups: the website's
skin in one PR, the two consoles' skin in another. The website-skin
amendment immediately above landed the first; this amendment lands the
second and **closes out the restyle series** — `crates/animusd/src/
dashboard.css`, `crates/animusd/src/console.css`, and the inline logo mark
in `dashboard.html`/`console.html`, geometry and component treatment only.
Validated the same way both prior amendments in this series were, via a
design-canvas exploration outside this repo. **Colour is explicitly
unchanged**: every rule below is built from an existing `var(--...)` token
already defined (with independent light and dark values) in `tokens.css`;
`tokens.css` itself was not touched, and
`dashboard::tokens_css_matches_website_copy` still passes.

Applying the website skin's exact pixel values here was deliberately
rejected: ADR 0056's own "One base, two skins" rule (Decision, above) —
unchanged by this whole restyle series — requires the app skin to stay
**denser** than the website skin, not to match it. Every value below scales
the app skin's own prior geometry proportionally, the same way the website
amendment scaled 3px/4px to 6px/8px, and lands strictly below the website's
now-shipped 6px/8px in both dimensions.

### Radius: uniform 3px → 5px controls / 7px panels

Both consoles shipped Ledger's original app-skin radius as a single flat
**3px** for everything — `--radius` and `--radius-panel` were defined
identically, with no control/panel distinction at all (unlike the "One
base, two skins" table's original aspirational 6px/10px app-skin row,
already stale against the shipped code before this amendment, corrected
below). This amendment introduces the same two-tier split the website's
own `--radius`/`--radius-panel` pair already has: **`--radius` moves to
5px** (buttons, inputs, the inline `code` chip, segmented controls, small
row-level chips like a replica row or a create-index row) and
**`--radius-panel` moves to 7px** (cards, the terminal-log block, the
placement grid, and every bordered form/toolbar panel — `.fact-strip`,
`.items-toolbar`, `.stream-controls`, `.index-card`, `.danger-card` in the
console; `.browser-panel`, `.placement-grid` in the dashboard). Functional
status pills (below) move off the `--radius`/`--radius-panel` scale
entirely, to a full 999px pill shape, mirroring the website's own pills
exactly.

### Cards: the ink top-rule is dropped for a plain, uniform border

Every card recipe in both stylesheets that carried Ledger's
`border-top: 2px solid var(--text)` accent rule drops it for a plain
`1px solid var(--border)` on all four sides, at the new `--radius-panel`:
the dashboard's `.card`, `.tablet-detail`, `.stream-detail`, and
`.item-detail`; the console's `.tables-card`, `.stub`, `.config-section`,
`.items-card`, and `.item-editor-card`. This is scoped to the recipes that
actually carried the top-rule-plus-`--ink-16`-sides pattern — exactly the
website amendment's own scoping rule. A handful of bordered boxes in both
files (the dashboard's `.placement-card`/`.index-card`-shaped small
containers, the console's `.fact-strip`/`.items-toolbar`/`.stream-controls`/
`.index-card`/`.danger-card`) never carried a top rule to begin with — they
keep their plain border, just at the bumped `--radius-panel`. Thick 2px ink
rules used as section-boundary devices rather than card treatments —
`header.topbar`'s bottom border, `.section-head`'s bottom border, a table
`th`'s bottom border, the dashboard's `.health-banner` top rule (a
full-width status band, not a bordered box) — are untouched, exactly the
distinction the website amendment drew for its own header/footer/`.stat-row`
rules.

### The ink-plate concept is removed, not tweaked

The dashboard's Streams tail-records log (`.stream-detail .tail-records`) —
the one place either console used Ledger's ink-plate treatment (a
permanently-dark interior via `--plate-bg`/`--plate-text`/`--plate-output`
plus a hard 5px-offset `--plate-shadow`, ADR 0056's 2026-08-25 amendment) —
now sits on the ordinary themed surface: `background: var(--surface)`,
`border: 1px solid var(--ink-16)`, `border-radius: var(--radius-panel)`,
**no shadow at all**. Its record text moves from `var(--plate-output)` to
`var(--text2)` (the same `--ink-72` stop the website's own `.o`/output
class reads), matching the "ordinary ink-alpha ramp instead of `--plate-*`"
substitution the website amendment made for its own `.plate`/`.code`. The
console never had an ink-plate surface at all (its Items/Stream tabs render
records as plain table rows, not a terminal block), so there is nothing to
remove there — grepping `--plate-` across both stylesheets after this
change returns nothing but the stray `.stream-detail .head`/`h3` selectors,
none of which read a `--plate-*` token. `--plate-*` and `--code-bg` stay
defined in `tokens.css`, unused by any consumer now (website and both
consoles alike) — kept only so the names don't dangle, exactly as the
website amendment left them.

The only `box-shadow` this amendment removes is the tail-records block's
`var(--plate-shadow)` line. Every other `box-shadow` in either file was
already either an explicit `none` (an inherited-property override, not a
plate artifact) or a legitimate `inset` accent ring marking a selected
table row or placement card (`tr.selected td:first-child`/
`.placement-card.selected` in the dashboard, `tr.sel td:first-child` in the
console) — a selection indicator, not elevation, and untouched here exactly
as the website amendment's own precedent (leaving `.lcard`'s unrelated
borders alone) suggests.

### Functional status pills move to filled/tinted; nothing decorative was found

Every pill in both consoles turned out to be a **functional** status
indicator — cluster/node health (`.health-pill`), replica/tablet/stream
state (`.pill` + its `.Active`/`.Down`/`.Leaving`/`.forming`/… modifier
classes, all reached through `dashboard_core.js`'s `pill()` helper), and
the console's GSI/LSI/table lifecycle and stream shard/event chips
(`.status-pill` + `.pill-active`/`.pill-creating`/`.pill-deleting`,
`.shard-pill-open`/`-closed`/`-ttl`) — never a static category label. All of
them move off Ledger's stamped-outline treatment (`1px`/`1.5px solid
currentColor`, transparent fill) to a filled, tinted pill at the new 999px
radius, reading the same `--ok-bg`/`--warn-bg`/`--danger-bg` tokens the
website's own compatibility pills use; the two colours those tokens don't
cover — the console's accent-toned `pill-creating` and `shard-pill-ttl` —
use `--accent-soft` (already defined for the toggle-switch's own "on" tint),
and the neutral `pill-deleting`/`shard-pill-closed`/the dashboard's
`forming` state use `--surface2`. Checked for a decorative counterpart to
the website's `.lcard .tag` (a static, un-pilled label) the way the brief
asked: the only tag-shaped element in either console is the console
topbar's `<span class="tag">console</span>` wordmark suffix next to the
`animusd` logotype — already plain `color: var(--text2)` text with no
pill/border, the same "already rendered as the treatment being asked for"
finding the website amendment made for `.lcard .tag`, so it needed no
change.

### The hatch bar: kept, unchanged

The dashboard's Overview balance bars (`.balance-bars .bar`) are the one
hatch-fill gauge either console has — already built from
`repeating-linear-gradient(45deg, var(--accent) 0 3px, var(--hatch) 3px
6px)`, both tokens carrying independent light/dark values. Kept as-is: it
was already token-built (nothing hardcoded to fix), and a 45° hatch reads
as a legible "this is a proportion, not a solid measurement" convention at
a glance in a way a flat fill would lose — simplifying it was judged to
cost more than it would gain for the one place it appears. Noted here per
the brief's own instruction to record the call either way.

### The logo mark: dashboard 20px → 30px, console 19px → 28.5px

Both consoles' header `.brand` inline SVG (the same 19-circle hexagonal
node-cluster mark the website's logo uses, `viewBox="-43 -43 86 86"`) grows
50%: the dashboard's from `width="20" height="20"` to `width="30"
height="30"`, the console's from `width="19" height="19"` to `width="28.5"
height="28.5"` — only the two attributes change, the mark's own circle
coordinates and colours are untouched, same as the website's own 24px→36px
bump.

### Verified

`grep -rEn '#[0-9a-fA-F]{3,8}\b' crates/animusd/src/dashboard.css
crates/animusd/src/console.css` returns nothing — every rule this amendment
touched reads an existing `var(--...)` token. `grep -rn "box-shadow"` over
both files shows only explicit `none` declarations and the two legitimate
selection-ring insets described above; the plate shadow is gone.
`crates/animusd/src/console.css` still carries one **pre-existing**,
untouched `rgba(...)` literal — the item-editor modal overlay's backdrop
scrim (`.item-editor-overlay`'s `rgba(33,31,26,0.45)`/`rgba(0,0,0,0.6)`) —
flagged here rather than silently left, per the validation instructions;
it predates this amendment, is not a card/pill/plate/radius concern, and
was out of scope to touch. `cargo build -p animusd` succeeds (confirms the
`concat!`/`include_str!` wiring compiles with the edited CSS/HTML) and
`dashboard::tokens_css_matches_website_copy` still passes (`tokens.css` was
not touched). A live single-node cluster (`animusd --cluster 1
--ephemeral`) was brought up in this sandbox and its served
`/admin/ui/dashboard.css`/`/console/ui/console.css` and shell HTML were
fetched directly: the served CSS carries the new `--radius`/`--radius-panel`
values, the served HTML carries the bumped logo `width`/`height`
attributes, and no `--plate-bg`/`--plate-shadow`/etc. token is referenced
outside `tokens.css`'s own definition block — confirming the edited files
are what the running binary actually serves. **This is a code-level and
served-bytes check only, not a rendered visual one**: this sandbox's egress
policy blocks the Chromium binary download Playwright needs (`playwright
.azureedge.net` and its mirrors all return a proxy-level 403 policy
denial, the same class of restriction the website amendment's own sandbox
hit for its live Google Fonts fetch, just one layer earlier in the
pipeline) — so, unlike the website amendment, this one does **not** claim a
real browser render in either theme. A maintainer with unrestricted egress
(or a `chromium-cli`-equipped sandbox) should confirm both consoles' cards,
the Streams tail-records block, the health/status pills, and both logos
render as described, in both themes, via each app's own `.theme-switch`
toggle, before treating this amendment's visual claims as verified beyond
the served-markup/CSS-bytes level.

### Statements above now superseded

- The "One base, two skins" table's app-skin **Radius** row (Decision, "One
  base, two skins" — "6px (10 on panels)") — already stale against the
  shipped code before this amendment (both consoles shipped a flat 3px for
  both), and superseded now in the other direction: the actual values as of
  this amendment are 5px (`--radius`) and 7px (`--radius-panel`).
- "The two in-product consoles keep Ledger's original geometry (3px radius,
  the card ink top-rule, the ink-plate) until their own follow-up PR"
  (2026-08-31 website-skin amendment, Context paragraph and "Statements
  above now superseded" section) — this is that follow-up PR; both
  consoles now carry the 5px/7px radius, the plain-bordered card, and no
  ink-plate, exactly mirroring the website's own treatment at app density.
- "Pills/badges are a **stamped outline**... Still accurate for the two
  consoles" and "A terminal/log-shaped surface... gets an **ink-plate**
  treatment... Still accurate for the two consoles" (2026-08-25 amendment,
  as re-confirmed by the website-skin amendment's own "Statements above now
  superseded" section) — both superseded for the two consoles by this
  amendment, above; no surface this ADR governs uses the stamped-outline
  pill or the ink-plate treatment any longer.

This completes the three-surface restyle begun by the two prior amendments
in this series: fonts (Work Sans/JetBrains Mono, first 2026-08-31
amendment), the website skin (second 2026-08-31 amendment), and now both
in-product consoles' skin, above. Every surface ADR 0056 governs shares one
plainer, more traditional developer-tool geometry and component language;
only the two-skin density difference (site vs. app radius/spacing/control
height, "One base, two skins," Decision, above) still tells them apart, by
design.

## Amendment (2026-09-01) — soften remaining heavy structural borders; reduce the callout family's left-accent to 2px

Direct maintainer visual feedback on the completed restyle series above:
*"soften the design a bit — the bold lines clash with the very techy logo
I like"* (the 19-circle hexagonal node-cluster mark). The three restyle
amendments above already removed the hard-shadow ink-plate and the
stamped-outline pills, but each one explicitly, deliberately **left in
place** a set of `2px`/`3px` `var(--text)`-colored structural rules,
calling them out by name as section-boundary devices rather than card
treatments (see the website-skin amendment's "Cards" section and the
consoles-skin amendment's own matching paragraph) — those rules are what
this amendment softens. This is a small follow-up polish on the closed-out
series, not a new leg of it: no colour token, radius, spacing, or the
ink-plate/pill/card component work from the three prior amendments changes
here — line weight only.

### Structural `var(--text)` borders: 2px/3px → a 1px `var(--border)` hairline

Every purely-structural border that used `var(--text)`/`var(--ink-*)` at
2px or more, across all three surfaces this ADR governs, drops to a plain
`1px solid var(--border)` — the same quiet hairline the already-restyled
cards/panels use elsewhere in the same files:

- `website/assets/site.css`: `header.site`'s bottom border,
  `footer.site`'s top border, `.stat-row`'s top border, the reference-page
  table `th`'s bottom border, `.article h2`'s bottom border, and
  `.doc-body h2`'s bottom border.
- `crates/animusd/src/dashboard.css`: `header.topbar`'s bottom border, the
  table `th`'s bottom border, `.section-head`'s bottom border, and
  `.health-banner`'s top border (its own `.degraded`/`.critical`
  `border-top-color: var(--danger)` override is untouched — same rule,
  now at 1px instead of 2px).
- `crates/animusd/src/console.css`: `.topbar`'s bottom border, and the
  bottom border on both `table.tables th` and `table.items-table th`.

### The callout family's left-accent: 3px → 2px, colour unchanged

`.callout`'s and `.pull`'s `border-left: 3px solid var(--accent)` and
`.warnbox`'s `border-left: 3px solid var(--danger)` (`website/assets/
site.css`) all drop to **2px**, colour untouched — these carry real
meaning (which kind of callout this is) via colour, so the border stays,
just quieter. `.warnbox`'s own `border: 1px solid var(--border-strong)`
(the box's outline, not the left accent) was already quiet and is
untouched. `.aside`'s `border-left: 2px solid var(--border-strong)` is a
third, visually similar left-rule in the same section but was already at
the target 2px width and uses neither `var(--text)`/`var(--ink-*)` nor
`var(--accent)`/`var(--danger)` — it matches neither category this
amendment addresses, so it is untouched.

### What stays exactly as it was: "keyline means live"

The 2026-08-25 Ledger revision's own load-bearing rule — a 2px solid
**accent**-colored underline/border is the system's one emphasis device,
reserved for real state ("Keyline-means-live replaces glow-means-live",
above) — is a different, meaningful device from the structural rules this
amendment softens, and is deliberately untouched: the active nav link
(`nav.main a.here`), the active reference-page subnav item (`.subnav
a.on`), the pressed theme-switch button (both consoles' and the site's own
`.theme-switch button[aria-pressed="true"]`/`.theme-switch button.active`),
the dashboard sidebar's active nav link, and the console's active tab-strip
link (`.tab-strip .tab-link.active`) all keep their exact `2px solid
var(--accent)` (or `border-bottom-color: var(--accent)` paired with a
`transparent` inactive-state sibling rule) byte-for-byte. The complaint
this amendment answers was specifically about heavy **black/ink**
dividers, not the accent live-indicator — softening that mechanism too
would have thrown away the one signal a viewer is supposed to notice.

### Verified

`grep -nE "2px solid var\(--text\)|3px solid var\(--text\)|2px solid var\(--ink"` over
`website/assets/site.css`, `crates/animusd/src/dashboard.css`, and
`crates/animusd/src/console.css` returns nothing after this amendment.
`grep -nE "2px solid var\(--accent\)|border-bottom: var\(--live-underline\)"`
over the same three files returns the identical set of matches before and
after (the callout-family accent rules that moved 3px→2px are a distinct,
intentional line — not part of this grep's `2px` pattern, since they were
3px before the change and stayed non-`2px`-literal). `cargo build -p
animusd` succeeds. **A real headless-browser check was done for both
themes, on both the website and both in-product consoles** — Playwright
against the sandbox's pre-installed Chromium, driven through each page's
own `.theme-switch` toggle (never a browser-context override): the website
homepage and the compatibility page (table-bearing); a live single-node
`animusd --cluster 1 --ephemeral` cluster's admin dashboard Overview tab
and the console's Items tab (with a real table and row created via the
DynamoDB wire, so a genuine table `th` renders). Every structural border
named above rendered as a quiet hairline in both themes on every page
checked, and every live-indicator rule (the active nav link, the active
subnav item, the pressed theme button, the active console tab) rendered
with its full, unmodified accent-colored keyline in both themes.
