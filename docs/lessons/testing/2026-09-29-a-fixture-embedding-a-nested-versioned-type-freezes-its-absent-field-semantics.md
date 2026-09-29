# A fixture that embeds a nested versioned type freezes that type's absent-field semantics

**Lesson.** A golden fixture (ADR 0073) is immutable once merged. If it embeds
a *nested* type whose own format is versioned separately (e.g. `control-wal/v1.bin`
embeds a `Metadata` JSON), then the nested type's behavior on the fixture's
shape — here, "no `"v"` key" — is frozen with it. Adding a *required* version
field to the nested type later breaks the frozen fixture, and the fixture cannot
be edited to fix it.

**Why it matters.** The two formats look independent but only one carries a
version in the fixture's bytes. We hit this when `Metadata` gained a required
`"v"` and the already-merged `control-wal/v1.bin` stopped decoding.

**What to do.** Either (a) version the nested type from the start / embed a
minimal value in the outer fixture that you know stays valid, or (b) when the
outer envelope already proves the data is post-baseline, give the nested
field a serde default naming the only schema that ever existed untagged
(`Metadata`'s `metadata_v1`), and keep the strict named-error decoder
(`Metadata::from_json`) for standalone documents. Order stacked layers so that
removals of fields a fixture contains land before the fixture is generated.
