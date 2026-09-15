# `PutOk`/`Accepted`-style replies from this codebase's relay/propose paths mean "accepted for commit," never "committed and reflected in `Metadata` yet"

**`PutOk`/`Accepted`-style replies from this codebase's relay/propose paths
mean "accepted for commit," never "committed and reflected in `Metadata`
yet"** (a corollary of the durable-before-visible discipline, but easy to
forget when writing a *test* rather than production code): a test that
proposes via `ClientRequest::ProposeSchema` and then immediately reads a
watermark/counter expecting it to have advanced will intermittently (or,
in one case while building ADR 0038 PR5's restart-fallback test,
*consistently*) observe the pre-commit value, because the apply task's
publish is a separate, asynchronous step the reply doesn't wait for by
design (`ClientRequest::ProposeSchema`'s own handler doc: "the caller
confirms the commit via replicated `Metadata`"). Every existing test that
gets this right polls (`propose_and_await`-style) for the actual effect
(a member/keyspace/tablet appearing) rather than trusting the immediate
reply — copy that idiom, don't invent a fresh assumption about what an ack
means.
