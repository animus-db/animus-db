# Doc files (`CLAUDE.md`, ADRs) conflict predictably

**Doc files (`CLAUDE.md`, ADRs) conflict predictably** when parallel changes
each edit the "what remains" lists — resolve by *unioning the done-states*
(each side is usually stale only for the *other* change's feature).
