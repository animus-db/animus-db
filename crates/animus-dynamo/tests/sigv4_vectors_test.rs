//! AWS's official SigV4 test-vector suite (vendored under
//! `sigv4_vectors/`, see that directory's `README.md`), run against
//! `animus_dynamo::sigv4`. Each case asserts the canonical request
//! (`.creq`), the string-to-sign (`.sts`), and the full `Authorization`
//! header (`.authz`) all match AWS's own precomputed values byte-for-byte,
//! then that [`sigv4::verify`] accepts the resulting request.
//!
//! This is the compatibility oracle the ADR 0057 design calls for in place
//! of a real `aws-sdk-dynamodb` smoke test (its crypto backends carry
//! licenses `deny.toml` doesn't allow) — an independent, AWS-authored
//! source of truth, not just tests written against our own implementation.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use animus_dynamo::sigv4::{self, SigV4Request};

const ACCESS_KEY_ID: &str = "AKIDEXAMPLE";
const SECRET_ACCESS_KEY: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
const REGION: &str = "us-east-1";
const SERVICE: &str = "service";
// All vectors are fixed at this instant; used as `now_epoch_ms` so the skew
// check trivially passes.
const NOW_MS: u64 = 1_440_938_160_000; // 2015-08-30T12:36:00Z

/// One parsed `.req` fixture.
struct ParsedReq {
    method: String,
    path: String,
    query: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

/// Parse a vendored `.req` file: request line, headers (with obsolete
/// line-folding continuations and repeated header names both collapsed into
/// a single comma-joined value per name, in first-seen order), then the body
/// after a blank line. Files have **no trailing newline** (vendored as-is).
fn parse_req(bytes: &[u8]) -> ParsedReq {
    let text = std::str::from_utf8(bytes).expect("fixture is valid UTF-8");
    // Split header section from body on the first blank line (an empty
    // line, i.e. "\n\n"); a GET fixture has no body and no blank line at
    // all.
    let (head, body) = match text.find("\n\n") {
        Some(idx) => (&text[..idx], &text.as_bytes()[idx + 2..]),
        None => (text, &b""[..]),
    };

    let mut lines = head.split('\n');
    let request_line = lines.next().expect("request line present");
    // The request-target can itself contain a literal, unencoded space (the
    // `get-space` normalize-path vector deliberately does this) — split on
    // the *first* space for the method and the *last* space for the
    // HTTP-version, so an embedded space in the target survives intact.
    let method_end = request_line.find(' ').expect("method present");
    let method = request_line[..method_end].to_string();
    let after_method = &request_line[method_end + 1..];
    let version_start = after_method.rfind(' ').expect("HTTP-version present");
    let target = &after_method[..version_start];
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (target.to_string(), String::new()),
    };

    // Fold continuation lines (leading whitespace) into the preceding
    // header's occurrence list, then group by lowercased name preserving
    // first-seen order, comma-joining each occurrence's trimmed value.
    let mut order: Vec<String> = Vec::new();
    let mut occurrences: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        if line.starts_with(' ') || line.starts_with('\t') {
            let name = order
                .last()
                .cloned()
                .expect("continuation follows a header");
            occurrences
                .get_mut(&name)
                .unwrap()
                .push(line.trim().to_string());
            continue;
        }
        let (name, value) = line.split_once(':').expect("header line has a colon");
        let name = name.trim().to_ascii_lowercase();
        if !occurrences.contains_key(&name) {
            order.push(name.clone());
        }
        occurrences
            .entry(name)
            .or_default()
            .push(value.trim().to_string());
    }
    let headers: BTreeMap<String, String> = occurrences
        .into_iter()
        .map(|(name, values)| (name, values.join(",")))
        .collect();

    ParsedReq {
        method,
        path,
        query,
        headers,
        body: body.to_vec(),
    }
}

/// Run one vendored vector directory (e.g. `sigv4_vectors/get-vanilla` or
/// `sigv4_vectors/normalize-path/get-slash`), whose files are all named
/// `<case>.{req,creq,sts,authz}`.
fn run_vector(dir: &Path, case: &str) {
    let req_bytes = fs::read(dir.join(format!("{case}.req")))
        .unwrap_or_else(|e| panic!("read {case}.req: {e}"));
    let expected_creq = fs::read_to_string(dir.join(format!("{case}.creq")))
        .unwrap_or_else(|e| panic!("read {case}.creq: {e}"));
    let expected_sts = fs::read_to_string(dir.join(format!("{case}.sts")))
        .unwrap_or_else(|e| panic!("read {case}.sts: {e}"));
    let expected_authz = fs::read_to_string(dir.join(format!("{case}.authz")))
        .unwrap_or_else(|e| panic!("read {case}.authz: {e}"));

    let parsed = parse_req(&req_bytes);
    let amz_date = parsed
        .headers
        .get("x-amz-date")
        .unwrap_or_else(|| panic!("{case}: fixture has no X-Amz-Date header"));
    assert_eq!(
        amz_date, "20150830T123600Z",
        "{case}: unexpected fixture date"
    );

    // Every header the fixture carries is signed, alphabetically —
    // matching every vendored `.authz`'s SignedHeaders list.
    let signed_headers: Vec<&str> = parsed.headers.keys().map(String::as_str).collect();

    let req = SigV4Request {
        method: &parsed.method,
        path: &parsed.path,
        query: &parsed.query,
        headers: &parsed.headers,
        body: &parsed.body,
    };

    let creq = sigv4::canonical_request(&req, &signed_headers);
    assert_eq!(
        creq,
        expected_creq.trim_end_matches('\n'),
        "{case}: canonical request mismatch"
    );

    let date = &amz_date[..8];
    let scope = format!("{date}/{REGION}/{SERVICE}/aws4_request");
    let sts = sigv4::string_to_sign(amz_date, &scope, &creq);
    assert_eq!(
        sts,
        expected_sts.trim_end_matches('\n'),
        "{case}: string-to-sign mismatch"
    );

    let authz = sigv4::sign(
        &req,
        ACCESS_KEY_ID,
        SECRET_ACCESS_KEY,
        amz_date,
        REGION,
        SERVICE,
        &signed_headers,
    );
    assert_eq!(
        authz,
        expected_authz.trim_end_matches('\n'),
        "{case}: Authorization mismatch"
    );

    // The signed request must also verify end-to-end.
    let mut verify_headers = parsed.headers.clone();
    verify_headers.insert("authorization".to_string(), authz);
    let verify_req = SigV4Request {
        method: &parsed.method,
        path: &parsed.path,
        query: &parsed.query,
        headers: &verify_headers,
        body: &parsed.body,
    };
    let mut credentials = BTreeMap::new();
    credentials.insert(ACCESS_KEY_ID.to_string(), SECRET_ACCESS_KEY.to_string());
    assert_eq!(
        sigv4::verify(&verify_req, &credentials, NOW_MS),
        Ok(()),
        "{case}: verify() rejected a correctly re-signed vector request"
    );
}

macro_rules! vector_test {
    ($fn_name:ident, $case:literal) => {
        #[test]
        fn $fn_name() {
            let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/sigv4_vectors")
                .join($case);
            run_vector(&dir, $case);
        }
    };
    ($fn_name:ident, $subdir:literal, $case:literal) => {
        #[test]
        fn $fn_name() {
            let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/sigv4_vectors")
                .join($subdir)
                .join($case);
            run_vector(&dir, $case);
        }
    };
}

vector_test!(vector_get_vanilla, "get-vanilla");
vector_test!(vector_get_vanilla_query, "get-vanilla-query");
vector_test!(
    vector_get_vanilla_query_order_key_case,
    "get-vanilla-query-order-key-case"
);
vector_test!(
    vector_get_vanilla_empty_query_key,
    "get-vanilla-empty-query-key"
);
vector_test!(vector_get_vanilla_utf8_query, "get-vanilla-utf8-query");
vector_test!(vector_get_unreserved, "get-unreserved");
vector_test!(vector_get_utf8, "get-utf8");
vector_test!(vector_get_header_key_duplicate, "get-header-key-duplicate");
vector_test!(
    vector_get_header_value_multiline,
    "get-header-value-multiline"
);
vector_test!(vector_get_header_value_order, "get-header-value-order");
vector_test!(vector_get_header_value_trim, "get-header-value-trim");
vector_test!(vector_post_header_key_case, "post-header-key-case");
vector_test!(vector_post_header_key_sort, "post-header-key-sort");
vector_test!(vector_post_header_value_case, "post-header-value-case");
vector_test!(vector_post_vanilla, "post-vanilla");
vector_test!(
    vector_post_vanilla_empty_query_value,
    "post-vanilla-empty-query-value"
);
vector_test!(vector_post_vanilla_query, "post-vanilla-query");
vector_test!(
    vector_post_x_www_form_urlencoded,
    "post-x-www-form-urlencoded"
);
vector_test!(
    vector_post_x_www_form_urlencoded_parameters,
    "post-x-www-form-urlencoded-parameters"
);

// normalize-path sub-suite — `get-space` lives *only* here in the upstream
// suite (see sigv4_vectors/README.md), not as a top-level case.
vector_test!(
    vector_normalize_get_relative,
    "normalize-path",
    "get-relative"
);
vector_test!(
    vector_normalize_get_relative_relative,
    "normalize-path",
    "get-relative-relative"
);
vector_test!(vector_normalize_get_slash, "normalize-path", "get-slash");
vector_test!(
    vector_normalize_get_slash_dot_slash,
    "normalize-path",
    "get-slash-dot-slash"
);
vector_test!(
    vector_normalize_get_slash_pointless_dot,
    "normalize-path",
    "get-slash-pointless-dot"
);
vector_test!(
    vector_normalize_get_slashes,
    "normalize-path",
    "get-slashes"
);
vector_test!(vector_normalize_get_space, "normalize-path", "get-space");
