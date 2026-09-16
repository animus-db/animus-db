# Judge an *eventual* property (convergence, durability) with a converged-or-timeout poll, not a fixed-drain snapshot — then it scales to depth like a safety property.

**Judge an *eventual* property (convergence, durability) with a converged-or-timeout
poll, not a fixed-drain snapshot — then it scales to depth like a safety property.**
A fixed post-heal `run_for(N)` then a one-shot check imposes a false deadline: at
adversarial seed-depth a compound fault can leave anti-entropy still in flight when
the drain ends, so the check flakes without revealing a bug — which is why the
frontier corpus was once pinned to the bounded base set. Instead drive a *bounded*
poll (`run_for` an increment, re-read, re-check; stop early once it holds) up to a
generous budget; only budget exhaustion is a genuine failure. Keep it a pure
function of the seed (`run_for`/`run_until` only, no wall clock). This let
`frontier_corpus_converges_and_is_durable` scale to the full deep tier. (ADR 0014;
`animus-test` `support/mod.rs::run_scenario_with`.)
