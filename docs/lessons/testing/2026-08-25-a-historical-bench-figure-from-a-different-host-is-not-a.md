# A historical bench figure from a different host is not a baseline — rerun it alongside the new number, on the same host, in the same session (ADR 0058's rung-8-vs-Train-2 bench pass, 2026-08-25).

**A historical bench figure from a different host is not a baseline —
rerun it alongside the new number, on the same host, in the same
session (ADR 0058's rung-8-vs-Train-2 bench pass, 2026-08-25).** Asked
to compare a new in-place split bench against ADR 0050 rung 8's
documented 458ms/2,000-row copy-based figure, the honest move was to
rerun the OLD bench too rather than diff a new number against an old
one measured on unknown, unrelated hardware — this session's own host
produced a copy-based blip of ~300ms (not 458ms) with a rock-steady
~108ms idle-read floor across every one of 6 runs, meaning the
absolute numbers here are simply not comparable to the ADR's own
historical figure, only to each other. Doing this also surfaced a
result worth having in hand precisely because it wasn't flattering:
the new (in-place) path's total wall clock was ~1.8x faster, as
predicted, but its own write blip was ~2.4x *worse* than the
same-host copy-based number, with zero retries needed on any run —
a single un-retried request was simply slow, a different failure
shape than the copy-based path's fast-refuse-and-retry blip, and one
the design doc's own "near-zero" framing hadn't anticipated. **General
rule**: when a task asks you to compare against a number from another
session/host/PR, treat that number as an anecdote until you've
reproduced it (or its equivalent) yourself under the same run — and
report an unflattering same-host result exactly as measured rather
than reframing it around the more flattering historical figure.
