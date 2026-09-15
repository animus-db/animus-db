# Adding a variant to *any* wire enum that flows through an exhaustive `match` needs a grep of every match site for *that enum*, not just the usual `is_relayable_command`/`ClientRequest` dispatch allowlist.

**Adding a variant to *any* wire enum that flows through an exhaustive
`match` needs a grep of every match site for *that enum*, not just the
usual `is_relayable_command`/`ClientRequest` dispatch allowlist.** Adding
`ClientResponse::NodeIdAllocated` broke `animus-cli`'s
`print_response(&ClientResponse)` — a plain, exhaustive `match` with no
wildcard arm, in a crate `is_relayable_command`'s doc comment never
mentions because it isn't a *command*-gating site at all; it's a
*response*-rendering one. The compiler caught this one (non-exhaustive
match is a hard error), but only because the match had no `_ =>` catch-all
— a wildcard arm would have silently swallowed the new variant with no
error and no runtime symptom until someone noticed the CLI printed
nothing useful for it. Lesson generalizes past this one enum: before
calling a "new variant" change done, `grep` for every `match` (and
`matches!`) over the enum's type name across the whole workspace, not just
the crate where the variant was added. (`animus-cli/src/main.rs::
print_response`; ADR 0036.)
