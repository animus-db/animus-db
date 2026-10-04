//! `animus-bench` binary: parse flags, run, print the text summary, write the
//! results JSON.

use animus_bench::cli::{self, USAGE};

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    if argv.iter().any(|a| a == "--help" || a == "-h") {
        println!("{USAGE}");
        return;
    }
    let opts = match cli::parse_args(&argv) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("animus-bench: {e}\n\n{USAGE}");
            std::process::exit(2);
        }
    };
    match cli::execute(&opts, argv).await {
        Ok(report) => {
            println!("{}", report.render_text());
            println!("results written to {}", opts.out.display());
        }
        Err(e) => {
            eprintln!("animus-bench: {e}");
            std::process::exit(1);
        }
    }
}
