//! Turning `--code` and `--insecure` into an authentication choice, loudly.
//!
//! One of the two has to be chosen out loud. A default would be wrong either way: pairing
//! by default silently fails for someone scripting a LAN copy, and `--insecure` by default
//! ships an unauthenticated transfer that looks authenticated.

use anyhow::{Context, Result, bail};
use spd_core::transport::{Authentication, PairingCode};

use crate::ui;

/// Resolves what the sending side will do.
///
/// # Errors
/// Fails if neither `--code` nor `--insecure` was given, or if the code cannot be read.
pub(crate) fn for_sender(code: Option<&str>, insecure: bool) -> Result<Authentication> {
    match (code, insecure) {
        (Some(typed), _) => {
            let code = PairingCode::parse(typed)
                .context("the pairing code was not one this program could have shown")?;
            Ok(Authentication::Code(code))
        }
        (None, true) => {
            warn_insecure("receiver");
            Ok(Authentication::Insecure)
        }
        (None, false) => bail!(
            "no way to tell who is on the other end.\nPass --code with the code the \
             receiver is showing, or --insecure if the network is one you trust."
        ),
    }
}

/// Resolves what the receiving side will do, inventing a code when none was given.
///
/// # Errors
/// Fails if neither `--code` nor `--insecure` was given, if a supplied code cannot be read,
/// or if the operating system will not provide randomness for a fresh one.
pub(crate) fn for_receiver(code: Option<&str>, insecure: bool) -> Result<Authentication> {
    if insecure {
        warn_insecure("sender");
        return Ok(Authentication::Insecure);
    }

    let code = match code {
        Some(typed) => {
            PairingCode::parse(typed).context("that pairing code cannot be typed back")?
        }
        None => PairingCode::random().context("could not generate a pairing code")?,
    };

    ui::section("pairing");
    ui::field("code", &code.to_string());
    eprintln!("         Type it on the sending machine: spd send <path> <address> --code {code}");
    eprintln!();

    Ok(Authentication::Code(code))
}

fn warn_insecure(other_side: &str) {
    eprintln!("warning: --insecure accepts any peer. Traffic is encrypted, but nothing");
    eprintln!("         proves the {other_side} is the machine you meant to reach.");
}
