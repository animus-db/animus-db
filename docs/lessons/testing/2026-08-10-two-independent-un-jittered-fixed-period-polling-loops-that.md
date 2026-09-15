# Two independent, un-jittered fixed-period polling loops that can each "win" a one-shot outcome are a real, silent flake source.

**Two independent, un-jittered fixed-period polling loops that can each "win" a one-shot outcome are a real, silent flake source.** (Found in `cp_reconfigure_loop`'s cadence race with `reconcile_loop`; that mechanism is superseded by ADR 0031 PR4 — the reconciler is event-driven now, no cadence to tune. Full entry archived in `docs/engineering-lessons-archive.md`.)
