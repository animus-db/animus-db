//! `animus-bench` binary: parse flags, run, print the text summary, write the
//! results JSON.

use animus_bench::cli::{self, USAGE};
use animus_bench::compare;
use animus_bench::report::Report;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    if argv.first().is_some_and(|a| a == "compare") {
        std::process::exit(run_compare(&argv[1..]));
    }
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

/// `animus-bench compare ...`: read the results files, print (and optionally
/// write) the markdown table. Exit 0 for any well-formed input — reporting
/// only; 2 for usage errors, 1 for unreadable/unparseable files.
fn run_compare(args: &[String]) -> i32 {
    if args.iter().any(|a| a == "--help" || a == "-h") {
        println!("{}", compare::COMPARE_USAGE);
        return 0;
    }
    let parsed = match compare::parse_compare_args(args) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("animus-bench compare: {e}\n\n{}", compare::COMPARE_USAGE);
            return 2;
        }
    };
    let load = |files: &[String]| -> Result<Vec<Report>, String> {
        files
            .iter()
            .map(|f| {
                let text = std::fs::read_to_string(f).map_err(|e| format!("{f}: {e}"))?;
                Report::from_json(&text).map_err(|e| format!("{f}: {e}"))
            })
            .collect()
    };
    let (base, head) = match (load(&parsed.base), load(&parsed.head)) {
        (Ok(b), Ok(h)) => (b, h),
        (Err(e), _) | (_, Err(e)) => {
            eprintln!("animus-bench compare: {e}");
            return 1;
        }
    };
    let md = compare::compare(&base, &head, parsed.threshold_pct).render_markdown();
    println!("{md}");
    if let Some(out) = &parsed.out
        && let Err(e) = std::fs::write(out, &md)
    {
        eprintln!("animus-bench compare: write {out}: {e}");
        return 1;
    }
    0
}
