# A site redesign needs a phone-width screenshot of the shared header

The PR #1343 redesign wrapped the Install button in `nav.main` and the theme
switch beside it, so below 860px both stacked left-aligned in separate rows
(~300px tall). Desktop review did not catch it. The header markup is shared by
every page, so prefer a CSS-only fix: on phones `display: contents` on the
wrappers lets brand, Install and the theme switch order as items of one flex
row (a zero-height `::after` with `flex-basis: 100%` forces the link row
break). Before merging any header/layout change, screenshot 375/390 and assert
no horizontal overflow at 320/375/390 on every page.
