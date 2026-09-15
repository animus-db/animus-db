# Adding TLS to a `hyper-util` legacy `Client` some crates use needs a hand-written connector, not a shared `MaybeTlsStream` (ADR 0064, S-01 commit 3)

`animus-operator`'s admin client (`hyper_util::client::legacy::Client`)
needed the same "plain TCP or TLS" choice `animus-env`'s `MaybeTlsStream`
and `animus-cli`'s connector already solved — but neither was reusable
here, for two independent reasons, both worth knowing before reaching for
`hyper-rustls` as the default fix: (1) this crate deliberately depends on
neither `animus-env` nor `animus-cli` (a standing architectural boundary,
not an oversight), and (2) even ignoring that, `hyper-util`'s legacy
`Client<C, B>` wants its connector shaped as a `tower_service::
Service<Uri>` returning something implementing `hyper::rt::{Read, Write}`
plus `hyper_util::client::legacy::connect::Connection` — a materially
different shape than an `AsyncRead + AsyncWrite` enum wrapped for direct
`tokio::net` use. The orphan rule bites here too: `Connection` (a
`hyper-util` trait) can't be implemented directly for `tokio_rustls::
client::TlsStream<TcpStream>` (a foreign type from a different crate) —
it needs a local newtype/enum wrapper, which is itself the same amount of
code a `MaybeTlsStream`-shaped type would have needed anyway, just with a
different trait to satisfy at the end. `hyper-rustls` would have solved
this in one dependency, but this codebase already special-cases `rustls`
usage per crate for good reasons (see this ADR's own "why `ring`, why not
a shared abstraction" decisions) — check whether the existing pattern
even applies before assuming a wrapper crate is the answer.

**General form**: "we already solved this exact problem elsewhere" is not
the same question as "can I reuse that solution here" — check the actual
trait/type shape the new call site needs (a `tower_service::Service<Uri>`
connector is not interchangeable with an `AsyncRead+AsyncWrite` wrapper,
even though both exist to answer "plain or TLS?") before writing a
duplicate, and don't be surprised when the duplicate is warranted by a
real architectural boundary (a crate that must not depend on another)
rather than an oversight.
