# A versioned serde document still gets a peek-then-match entry point

A CR/JSON format whose only "decoder" is one `Deserialize` type (the
`AnimusCluster` spec) has no byte-level version header to dispatch on, which
makes a dispatch seam look unnecessary. It is still needed: the version lives
in a field, so peek it (`value["spec"]["schemaVersion"]`) *before* choosing a
type, `match` on it, and make unknown/missing a named error. Otherwise a v2
edits the single serde type in place and v1 documents are silently misread
(serde ignores unknown fields and defaults absent ones).

- Derive the expected version in the per-version fixture test from the file
  name (`vN.json`), cross-check it against the document's own version field,
  and `panic!` on an unrecognised `N`, so adding a fixture forces adding its
  expectation.
- Do not add an empty `legacy` module ahead of need; leave the commented
  `1 => legacy::v1::decode(..)` arm where v2 will put it.
