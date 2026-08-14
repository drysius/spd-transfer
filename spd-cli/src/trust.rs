//! Turning the `--insecure` flag into a trust policy, loudly.
//!
//! Peer authentication does not exist yet, so there is exactly one usable policy and the
//! user has to ask for it by name. Making it the silent default would mean shipping an
//! unauthenticated transfer that looks authenticated.

use anyhow::{Result, bail};
use spd_core::transport::TrustPolicy;

/// Resolves the trust policy for this run.
///
/// # Errors
/// Fails when `--insecure` was not given, since no verified policy exists yet.
pub(crate) fn policy(insecure: bool) -> Result<TrustPolicy> {
    if !insecure {
        bail!(
            "peer authentication is not implemented yet, so every session would be \
             unauthenticated.\nRerun with --insecure if the network is one you trust; \
             pairing arrives in a later release."
        );
    }

    eprintln!("warning: --insecure accepts any peer. Traffic is encrypted, but nothing");
    eprintln!("         proves the other side is the machine you meant to reach.");

    Ok(TrustPolicy::InsecureNoVerification)
}
