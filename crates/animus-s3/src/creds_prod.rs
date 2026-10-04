//! The process-boundary half of credential sourcing (S-08 M1), gated behind
//! the `prod` feature like [`crate::prod`]: reading `AWS_*` environment
//! variables and token files. Everything pure lives in [`crate::creds`].

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;

use crate::client::S3Error;
use crate::creds::{CredentialProvider, TokenSource};
use crate::sigv4::Credentials;

/// Credentials from `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` and the
/// optional `AWS_SESSION_TOKEN`, re-read on every call (so an externally
/// rotated environment is not possible, but a `refresh` re-reads honestly).
/// No expiry is known, so [`crate::creds::CachingProvider`] caches it
/// forever.
pub struct EnvProvider;

#[async_trait]
impl CredentialProvider for EnvProvider {
    async fn credentials(&self, _now_epoch_ms: u64) -> Result<Credentials, S3Error> {
        let get = |name: &str| {
            std::env::var(name)
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };
        let id = get("AWS_ACCESS_KEY_ID")
            .ok_or_else(|| S3Error::Credentials("AWS_ACCESS_KEY_ID is not set".to_string()))?;
        let secret = get("AWS_SECRET_ACCESS_KEY")
            .ok_or_else(|| S3Error::Credentials("AWS_SECRET_ACCESS_KEY is not set".to_string()))?;
        let creds = Credentials::new(id, secret);
        Ok(match get("AWS_SESSION_TOKEN") {
            Some(token) => creds.with_session_token(token, None),
            None => creds,
        })
    }
}

/// A [`TokenSource`] that reads (and trims) `path` on every call — projected
/// service-account tokens and Pod Identity tokens rotate on disk. The error
/// names the path, never the contents.
#[must_use]
pub fn file_token_source(path: impl Into<PathBuf>) -> TokenSource {
    let path: PathBuf = path.into();
    Arc::new(move || {
        std::fs::read_to_string(&path)
            .map(|t| t.trim().to_string())
            .map_err(|e| format!("{}: {e}", path.display()))
            .and_then(|t| {
                if t.is_empty() {
                    Err(format!("{}: token file is empty", path.display()))
                } else {
                    Ok(t)
                }
            })
    })
}

/// A [`TokenSource`] that reads environment variable `var` on every call.
#[must_use]
pub fn env_token_source(var: impl Into<String>) -> TokenSource {
    let var: String = var.into();
    Arc::new(move || {
        std::env::var(&var)
            .map(|t| t.trim().to_string())
            .map_err(|_| format!("environment variable {var} is not set"))
    })
}

/// Resolve the container-credentials endpoint from the standard AWS
/// environment: `AWS_CONTAINER_CREDENTIALS_FULL_URI` (EKS Pod Identity, ECS
/// with a full URI) or `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` (ECS, joined
/// onto the link-local `http://169.254.170.2`), plus the optional
/// authorization token from `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`
/// (preferred, rotates) or `AWS_CONTAINER_AUTHORIZATION_TOKEN`.
///
/// # Errors
/// Neither URI variable is set.
pub fn container_endpoint_from_env() -> Result<(String, Option<TokenSource>), String> {
    let get = |name: &str| std::env::var(name).ok().filter(|v| !v.trim().is_empty());
    let uri = match (
        get("AWS_CONTAINER_CREDENTIALS_FULL_URI"),
        get("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"),
    ) {
        (Some(full), _) => full,
        (None, Some(rel)) => format!("http://169.254.170.2{rel}"),
        (None, None) => {
            return Err(
                "source=container needs AWS_CONTAINER_CREDENTIALS_FULL_URI or \
                 AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"
                    .to_string(),
            );
        }
    };
    let auth = if let Some(file) = get("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE") {
        Some(file_token_source(file))
    } else if get("AWS_CONTAINER_AUTHORIZATION_TOKEN").is_some() {
        Some(env_token_source("AWS_CONTAINER_AUTHORIZATION_TOKEN"))
    } else {
        None
    };
    Ok((uri, auth))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_token_source_trims_and_rereads() {
        let dir = std::env::temp_dir().join(format!("animus-s3-tok-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("mkdir");
        let path = dir.join("token");
        std::fs::write(&path, "first\n").expect("write");
        let src = file_token_source(&path);
        assert_eq!(src().as_deref(), Ok("first"));
        std::fs::write(&path, "second\n").expect("write");
        assert_eq!(src().as_deref(), Ok("second"));
        std::fs::write(&path, "\n").expect("write");
        assert!(src().is_err());
        std::fs::remove_file(&path).ok();
        let err = src().expect_err("missing");
        assert!(err.contains("token"), "{err}");
    }
}
