# In a sequential multi-agent delivery chain, an implementation agent that ends its turn to "wait" for its own background command stalls the chain even though the harness auto-resumes it on completion

**In a sequential multi-agent delivery chain, an implementation agent
that ends its turn to "wait" for its own background command stalls the
chain even though the harness auto-resumes it on completion** — the
orchestrator cannot assume "no further message" means "still working";
it must treat every completion notification as a checkpoint to verify
the working tree/commit state directly rather than trusting the agent's
last message, and agent briefs must say explicitly that a background
command's completion re-invokes the agent and it must then continue to
the end of the task, not stop again to wait.
