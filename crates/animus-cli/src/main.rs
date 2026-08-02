//! AnimusDB operator/client CLI (`animus`).
//!
//! Usage:
//!   animus status <node-addr>
//!   animus put    <node-addr> <key> <value>
//!   animus get    <node-addr> <key>
//!
//! A thin client over a node's request/reply API (see `animusd`). `<node-addr>`
//! is a node's client address as printed by `animusd`.

use std::process::ExitCode;

use animusd::{ClientRequest, ClientResponse, read_frame, write_frame};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(msg) => {
            eprintln!("animus: {msg}");
            eprintln!(
                "\nusage:\n  animus status <node-addr>\n  animus put <node-addr> <key> <value>\n  animus get <node-addr> <key>"
            );
            ExitCode::FAILURE
        }
    }
}

async fn run(args: &[String]) -> Result<(), String> {
    let cmd = args.first().map(String::as_str).ok_or("missing command")?;
    let addr = args.get(1).ok_or("missing <node-addr>")?;

    let request = match cmd {
        "status" => ClientRequest::Status,
        "put" => {
            let key = args.get(2).ok_or("put needs <key>")?;
            let value = args.get(3).ok_or("put needs <value>")?;
            ClientRequest::Put {
                key: key.clone().into_bytes(),
                value: value.clone().into_bytes(),
            }
        }
        "get" => {
            let key = args.get(2).ok_or("get needs <key>")?;
            ClientRequest::Get {
                key: key.clone().into_bytes(),
            }
        }
        other => return Err(format!("unknown command `{other}`")),
    };

    let mut stream = TcpStream::connect(addr)
        .await
        .map_err(|e| format!("cannot connect to {addr}: {e}"))?;
    write_frame(&mut stream, &request)
        .await
        .map_err(|e| format!("send failed: {e}"))?;
    let response: ClientResponse = read_frame(&mut stream)
        .await
        .map_err(|e| format!("recv failed: {e}"))?
        .ok_or("node closed the connection without replying")?;

    print_response(&response);
    if matches!(response, ClientResponse::Error(_)) {
        return Err("operation failed".into());
    }
    Ok(())
}

fn print_response(response: &ClientResponse) {
    match response {
        ClientResponse::Status(meta) => {
            println!("members: {}", meta.members.len());
            for (id, member) in &meta.members {
                println!("  node {id}: {:?}", member.status);
            }
            println!("tablets: {}", meta.tablets.len());
            for (id, t) in &meta.tablets {
                let end = t
                    .range
                    .end
                    .as_ref()
                    .map_or_else(|| "∞".to_string(), |e| show(e));
                println!(
                    "  tablet {}: [{}, {}) epoch {} replicas {:?}",
                    id.0,
                    show(&t.range.start),
                    end,
                    t.epoch.0,
                    t.replicas
                );
            }
        }
        ClientResponse::PutOk => println!("OK"),
        ClientResponse::Value(Some(v)) => println!("{}", show(v)),
        ClientResponse::Value(None) => println!("(not found)"),
        ClientResponse::Error(e) => println!("error: {e}"),
    }
}

/// Render bytes as UTF-8 if possible, else as a debug string.
fn show(bytes: &[u8]) -> String {
    String::from_utf8(bytes.to_vec()).unwrap_or_else(|_| format!("{bytes:?}"))
}
