# Run the real web UIs against an embedded cluster by intercepting fetch, not by forking them

The demo runs animusd's data console and dashboard unmodified. What made it cheap: the
backends were already reachable as `route(request)` / `dispatch(method, path, query, body)`
over a generic `ClientCtx`, so the embedded cluster only needed a non-blocking submit/poll
wrapper, and the browser side a `fetch` override. Gotchas worth remembering:

- A static host has no `/console/ui/*` pages, so every navigation (anchor click,
  `location.href =`, a reload) must be intercepted; `history.replaceState` makes
  pathname-based routing work but not full navigations.
- The sim reports placeholder addresses (`127.0.0.1:1`, node ids as hosts); a UI that fans
  out per node needs the shim to map those hosts to node indices.
- A request waiting on a paused virtual clock never completes; let the pump run for pending
  UI requests.
- Probe real responses (a throwaway test printing them) before designing the shim.
