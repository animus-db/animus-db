# A CAS guard closes the *concurrent* instance of a race; a *sequential* instance of the same race needs its own answer — usually cleanup, not another precondition.

**A CAS guard closes the *concurrent* instance of a race; a *sequential* instance of the same race needs its own answer — usually cleanup, not another precondition.** (Found in the pre-ADR-0028 orphan-tablet cleanup, now structurally impossible; archived in `docs/engineering-lessons-archive.md`.)
