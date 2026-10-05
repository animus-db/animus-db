# Pin simulated "previous release" binaries to literal version ranges

`BinaryProfile::B2` computed its range from `own_range()`, i.e. from the
current `MAX_SUPPORTED`. The first real cluster-version bump (1 -> 2) would have
turned the "old" binary into `[1, 2]` without any test failing loudly: the
rolling-upgrade cells would just stop exercising an old binary. Four
`version_observe_corpus` tests did fail, but only because they flipped B2 by
hand; the harness itself was silently wrong. Any profile that stands for a
released binary must spell its range (`VersionRange::new(1, 1)`), never derive it
from the constants a bump changes, and carry a unit test that fails if it moves.
