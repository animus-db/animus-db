# General form (mechanism superseded): before giving any key-ordered scan a positional resume cursor, ask what order NEW entries arrive in — if insertion order ≠ scan order, a positional cursor is a silent-loss bug.

**General form (mechanism superseded): before giving any key-ordered scan
a positional resume cursor, ask what order NEW entries arrive in — if
insertion order ≠ scan order, a positional cursor is a silent-loss bug.**
Originally learned from the now-deleted copy-based split-build driver's
own tail cursor; see `docs/engineering-lessons-archive.md`'s "The
copy-based split-build driver" section for the full incident.
