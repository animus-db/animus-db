# Website: cards are items, not sections

When the maintainer said "I like cards but not blocky sections", the dislike was full-width
coloured bands and boxed page sections, not rounded item cards. The redesign had flattened both.
Restore cards for items (`.lcard`, `.card`, `.cols-2/.cols-3 > div`: `--surface`, 1px `--rule`,
8px radius, no shadow) and drop the grey `section.band` fill, keeping its spacing.
Read feedback for which of two coupled design choices was rejected before reverting either.
