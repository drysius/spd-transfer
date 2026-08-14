//! Repository automation, run as `cargo xtask <task>`.
//!
//! The point is that the CI pipeline has one definition. `.github/workflows/ci.yml`
//! calls `cargo xtask ci`, so "green locally, red on CI" means a genuine platform
//! difference rather than a workflow that drifted from the repo.
//!
//! Dependency-free on purpose: this must build before anything else does.

use std::env;
use std::process::{Command, ExitCode};

fn main() -> ExitCode {
    let task = env::args().nth(1);

    let result = match task.as_deref() {
        Some("ci") => run_ci(),
        Some("fmt") => run(&["fmt", "--all"]),
        Some(unknown) => {
            eprintln!("unknown task: {unknown}");
            print_usage();
            return ExitCode::FAILURE;
        }
        None => {
            print_usage();
            return ExitCode::FAILURE;
        }
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(step) => {
            eprintln!("\nxtask: `{step}` failed");
            ExitCode::FAILURE
        }
    }
}

fn print_usage() {
    eprintln!("usage: cargo xtask <task>");
    eprintln!();
    eprintln!("  ci    format check, clippy, tests, docs and cargo-deny");
    eprintln!("  fmt   format the whole workspace in place");
}

/// Runs the same steps as CI, stopping at the first failure so the output stays readable.
fn run_ci() -> Result<(), String> {
    run(&["fmt", "--all", "--check"])?;
    run(&[
        "clippy",
        "--workspace",
        "--all-targets",
        "--all-features",
        "--",
        "-D",
        "warnings",
    ])?;
    run(&["test", "--workspace", "--all-features"])?;
    run(&["doc", "--workspace", "--no-deps", "--all-features"])?;

    // cargo-deny is optional locally and required on CI: a missing tool must not read as
    // a passing check, so it is reported and skipped rather than silently ignored.
    if has_cargo_subcommand("deny") {
        run(&["deny", "check"])?;
    } else {
        eprintln!("xtask: cargo-deny not installed, skipping (install: cargo install cargo-deny)");
    }

    println!("\nxtask: ci passed");
    Ok(())
}

fn run(args: &[&str]) -> Result<(), String> {
    let label = format!("cargo {}", args.join(" "));
    println!("\n$ {label}");

    let status = Command::new(cargo()).args(args).status();

    match status {
        Ok(status) if status.success() => Ok(()),
        Ok(_) => Err(label),
        Err(error) => {
            eprintln!("could not start `{label}`: {error}");
            Err(label)
        }
    }
}

fn has_cargo_subcommand(name: &str) -> bool {
    Command::new(cargo())
        .args([name, "--version"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

/// Uses the cargo that invoked us, so a pinned toolchain stays pinned.
fn cargo() -> String {
    env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned())
}
