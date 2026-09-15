# Reading a JSON response's actual field nesting from source beats inferring it from a route's own doc-comment summary

Writing S-04 PR 3's (unverified, no-`kind`-available-sandbox) e2e leg, the
task brief said to check `GET /admin/backup-store` for `"kind": "s3"`. The
route's own doc comment says exactly that phrase ("`store` is this node's
own configured backup store: kind plus a credential-safe `location`"), and
it would have been easy to write `jq -r '.kind'` straight from that
sentence. The actual handler (`admin.rs::backup_store_view`) nests it
one level down: the top-level response is `{"store": {"kind": ...,
"location": ...}, "objects": ..., "janitor": ..., "leader": ...}`, so the
correct query is `.store.kind`, not `.kind`. **General form**: for any
script or test that asserts on a JSON wire/admin response shape, read the
actual serializer/handler function, not just its module doc comment or a
task description's paraphrase of it — a doc comment describes intent and
can (correctly) omit the wrapper object it's nested inside, and a
paraphrase one level removed from the code compounds that gap. This
matters more, not less, when the assertion can't be run in this sandbox
(no `kind`) — there is no test failure to catch the mistake before it
reaches a real CI run.
