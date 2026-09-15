# Read the constraint before designing around it (ADR 0056)

ADR 0021 says the dashboard ships "no external fonts, no CDN". That was read as
"no webfonts", and the surfaces ran system stacks for it. The rule actually bans
*fetching from a third party* — self-hosting an OFL face, or embedding it in the
binary, always satisfied it. A whole design constraint was self-imposed by a
misreading of one clause.

Related, and worth checking before rejecting a face on weight: both faces here
turned out to be **variable** fonts, so one 22–23 KB Latin file covers the entire
weight range. The cost was first estimated per weight, which was wrong by about
4x and nearly drove a font substitution that was not needed. Fetch the file and
look before costing it.
