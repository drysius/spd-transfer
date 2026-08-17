//! What a hostile peer can and cannot make the receiver write.
//!
//! The unit tests in `safety::path` prove that each vector is rejected by the type. This
//! suite proves the receiver actually asks: every entry goes through `SafeRelPath` before
//! anything touches the filesystem, so a refused path leaves the destination exactly as it
//! was - no directory created on the way to the file that was never written.

mod common;

use common::{Scratch, bound_listener, dial};
use spd_core::pipeline::PipelineError;
use spd_core::pipeline::recv::{ReceiveOptions, receive_tree};
use spd_core::proto::messages::{Control, Entry, FileId};
use spd_core::safety::limits::Limits;

/// Every way a peer might try to name a file outside the destination.
///
/// Written as the components actually go on the wire, because that is the only place the
/// receiver can see them: a separator inside a component is a different attack from a
/// component that *is* a separator, and both have to be refused.
fn traversal_vectors() -> Vec<(&'static str, Vec<String>)> {
    let owned = |parts: &[&str]| parts.iter().map(|part| (*part).to_owned()).collect();

    vec![
        ("a parent directory", owned(&["..", "escaped.bin"])),
        (
            "a parent further down",
            owned(&["docs", "..", "..", "escaped.bin"]),
        ),
        ("the current directory", owned(&[".", "here.bin"])),
        ("a unix path in one component", owned(&["/etc/passwd"])),
        (
            "a windows path in one component",
            owned(&["..\\..\\escaped.bin"]),
        ),
        ("a drive letter", owned(&["C:", "windows", "system32"])),
        ("a UNC share", owned(&["\\\\server\\share", "file.bin"])),
        ("a reserved device name", owned(&["CON"])),
        ("a reserved name with an extension", owned(&["nul.txt"])),
        ("a trailing space", owned(&["report.txt "])),
        ("a trailing dot", owned(&["report.txt."])),
        ("a forbidden character", owned(&["what?.bin"])),
        ("a null byte", owned(&["na\u{0}me.bin"])),
        ("an empty component", owned(&["docs", "", "file.bin"])),
        ("no components at all", Vec::new()),
    ]
}

/// Offers one entry with `path` and reports what the receiver did about it.
async fn offer(path: Vec<String>, destination: &Scratch) -> PipelineError {
    let limits = Limits::DEFAULT;
    let (listener, address) = bound_listener(limits);
    let into = destination.path().to_path_buf();

    let receiving = tokio::spawn(async move {
        let session = listener.accept().await.unwrap();
        receive_tree(session, &into, ReceiveOptions::default(), &limits).await
    });

    let mut hostile = dial(address, limits).await;
    hostile
        .control()
        .send(&Control::Manifest {
            batch_seq: 0,
            last: true,
            entries: vec![Entry {
                file_id: FileId(1),
                path,
                size: 8,
                mtime: 1,
                mode: 0,
                hash: None,
            }],
        })
        .await
        .unwrap();

    // Whatever the receiver says next, the transfer is over: it either refused the path or
    // it did not, and the assertion is about which.
    let outcome = receiving.await.unwrap();
    hostile.close("done");

    outcome.expect_err("the receiver accepted a path it should have refused")
}

#[tokio::test]
async fn every_traversal_vector_is_refused_and_writes_nothing() {
    for (what, path) in traversal_vectors() {
        let destination = Scratch::new("traversal");
        let refused = offer(path.clone(), &destination).await;

        assert!(
            matches!(refused, PipelineError::Path(_)),
            "{what} ({path:?}) should be refused as a path, got {refused:?}"
        );

        assert!(
            std::fs::read_dir(destination.path())
                .unwrap()
                .next()
                .is_none(),
            "{what} left something behind in the destination"
        );
    }
}

#[tokio::test]
async fn a_path_deeper_than_the_limit_is_refused() {
    let destination = Scratch::new("traversal-deep");
    let too_deep = vec!["a".to_owned(); Limits::DEFAULT.max_path_depth + 1];

    let refused = offer(too_deep, &destination).await;

    assert!(matches!(refused, PipelineError::Path(_)), "got {refused:?}");
    assert!(
        std::fs::read_dir(destination.path())
            .unwrap()
            .next()
            .is_none()
    );
}

#[tokio::test]
async fn a_peer_cannot_offer_more_files_than_the_limit_allows() {
    let destination = Scratch::new("traversal-flood");
    let limits = Limits {
        max_files: 4,
        ..Limits::DEFAULT
    };

    let (listener, address) = bound_listener(limits);
    let into = destination.path().to_path_buf();

    let receiving = tokio::spawn(async move {
        let session = listener.accept().await.unwrap();
        receive_tree(session, &into, ReceiveOptions::default(), &limits).await
    });

    let mut hostile = dial(address, limits).await;

    // Every batch is well within `max_manifest_entries`; what is out of bounds is how many
    // files they add up to, which is the hole a per-message limit alone leaves open.
    for batch_seq in 0..10 {
        let entries = (0..2)
            .map(|index| Entry {
                file_id: FileId(u64::from(batch_seq) * 2 + index),
                path: vec![format!("file-{batch_seq}-{index}.bin")],
                size: 1,
                mtime: 1,
                mode: 0,
                hash: None,
            })
            .collect();

        if hostile
            .control()
            .send(&Control::Manifest {
                batch_seq,
                last: false,
                entries,
            })
            .await
            .is_err()
        {
            // The receiver hung up mid-flood, which is the point of the limit.
            break;
        }
    }

    let refused = receiving
        .await
        .unwrap()
        .expect_err("the receiver should have stopped accepting files");

    assert!(
        matches!(
            refused,
            PipelineError::TooManyFiles {
                limit: "max_files",
                ..
            }
        ),
        "got {refused:?}"
    );

    hostile.close("done");
}

/// Sends `source` to a fresh destination and hands back both, so a test can assert on what
/// arrived. The two sides carry their own limits on purpose: what they disagree about is
/// the subject of these tests.
#[cfg(unix)]
async fn transfer(source: &Scratch, sending: Limits, receiving_with: Limits) -> Scratch {
    let destination = Scratch::new("names-dest");
    let (listener, address) = bound_listener(receiving_with);
    let into = destination.path().to_path_buf();

    let receiving = tokio::spawn(async move {
        let session = listener.accept().await.unwrap();
        receive_tree(session, &into, ReceiveOptions::default(), &receiving_with)
            .await
            .unwrap()
    });

    let sender = dial(address, sending).await;
    spd_core::pipeline::send::send_tree(
        sender,
        source.path(),
        spd_core::pipeline::send::SendOptions::default(),
        &sending,
    )
    .await
    .unwrap();

    receiving.await.unwrap();
    destination
}

/// A tree with one ordinary file and one named after a wildcard, which is what a game
/// server or a plugin cache actually looks like on Linux.
#[cfg(unix)]
fn tree_with_an_unportable_name() -> Scratch {
    let source = Scratch::new("names-source");
    source.write("ordinary.txt", b"fine");
    std::fs::create_dir_all(source.path().join("?")).unwrap();
    source.write("?/README.txt", b"ordinary here, impossible on Windows");
    source
}

/// Unix only, both ways round: a name like `?` cannot be created on Windows, so there is
/// nothing there to send and nothing to receive.
#[cfg(unix)]
#[tokio::test]
async fn a_unix_only_name_crosses_when_both_sides_allow_it() {
    let posix = Limits {
        names: spd_core::safety::path::NamePolicy::Posix,
        ..Limits::DEFAULT
    };

    let source = tree_with_an_unportable_name();
    let destination = transfer(&source, posix, posix).await;

    assert_eq!(
        std::fs::read(destination.path().join("?").join("README.txt")).unwrap(),
        b"ordinary here, impossible on Windows",
        "the file the old build left behind is the one that has to arrive"
    );
    assert!(destination.path().join("ordinary.txt").exists());
}

/// The sender allows it, the receiver does not. The name must not be offered at all: it is
/// the receiver that would have to write it, and it has said it cannot.
#[cfg(unix)]
#[tokio::test]
async fn a_unix_only_name_is_held_back_when_the_receiver_cannot_write_it() {
    let posix = Limits {
        names: spd_core::safety::path::NamePolicy::Posix,
        ..Limits::DEFAULT
    };

    let source = tree_with_an_unportable_name();
    let destination = transfer(&source, posix, Limits::DEFAULT).await;

    assert!(
        !destination.path().join("?").exists(),
        "a name the receiver cannot write is never offered"
    );
    assert!(
        destination.path().join("ordinary.txt").exists(),
        "and the rest of the tree still goes"
    );
}

#[tokio::test]
async fn an_ordinary_nested_path_is_still_accepted() {
    let source = Scratch::new("traversal-ok-source");
    std::fs::create_dir_all(source.path().join("docs")).unwrap();
    source.write("docs/notes.txt", b"ordinary");

    let destination = Scratch::new("traversal-ok-dest");
    let limits = Limits::DEFAULT;
    let (listener, address) = bound_listener(limits);
    let into = destination.path().to_path_buf();

    let receiving = tokio::spawn(async move {
        let session = listener.accept().await.unwrap();
        receive_tree(session, &into, ReceiveOptions::default(), &limits)
            .await
            .unwrap()
    });

    let sender = dial(address, limits).await;
    spd_core::pipeline::send::send_tree(
        sender,
        source.path(),
        spd_core::pipeline::send::SendOptions::default(),
        &limits,
    )
    .await
    .unwrap();

    assert_eq!(receiving.await.unwrap().files, 1);
    assert_eq!(
        std::fs::read(destination.path().join("docs").join("notes.txt")).unwrap(),
        b"ordinary",
        "refusing traversal must not mean refusing ordinary directories"
    );
}
