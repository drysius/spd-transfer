//! Claims that should hold for *any* input, not just the ones somebody thought of.
//!
//! Each example-based test in this repository was written by someone imagining an attack or
//! a mistake. These are the same claims stated without the imagining: proptest generates the
//! inputs, including the ones nobody would have written down.

use proptest::prelude::*;
use spd_core::proto::codec::decode;
use spd_core::proto::messages::{Control, Decision, Entry, ErrorCode, FileId};
use spd_core::safety::limits::Limits;
use spd_core::safety::path::SafeRelPath;
use spd_core::scan::diff::{LocalFile, decide};
use spd_core::state::model::{Expected, Partial};

/// A directory that deletes itself, so the resolution property can use a real root.
struct Scratch(std::path::PathBuf);

impl Scratch {
    fn new() -> Self {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|since| since.as_nanos())
            .unwrap_or_default();
        let path = std::env::temp_dir().join(format!("spd-properties-{unique}"));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Any sequence of arbitrary strings, which is exactly what a peer can put in a manifest.
fn any_components() -> impl Strategy<Value = Vec<String>> {
    proptest::collection::vec(".*", 0..6)
}

/// The same, but weighted towards the characters that make paths dangerous.
fn nasty_components() -> impl Strategy<Value = Vec<String>> {
    let piece = prop_oneof![
        Just("..".to_owned()),
        Just(".".to_owned()),
        Just(String::new()),
        Just("/".to_owned()),
        Just("\\".to_owned()),
        Just("C:".to_owned()),
        Just("CON".to_owned()),
        Just("nul.txt".to_owned()),
        Just("trailing ".to_owned()),
        Just("trailing.".to_owned()),
        Just("\u{0}".to_owned()),
        "[a-zA-Z0-9._-]{1,12}",
    ];

    proptest::collection::vec(piece, 0..6)
}

proptest! {
    /// The claim `SafeRelPath` exists to make: it either refuses, or produces a path inside
    /// the root. There is no third outcome, whatever the peer sent.
    #[test]
    fn a_path_either_is_refused_or_stays_under_the_root(
        components in prop_oneof![any_components(), nasty_components()]
    ) {
        let scratch = Scratch::new();
        let root = scratch.0.canonicalize().unwrap();

        if let Ok(safe) = SafeRelPath::from_components(&components, &Limits::DEFAULT) {
            let resolved = safe.resolve_under(&root).unwrap();
            prop_assert!(
                resolved.starts_with(&root),
                "{components:?} resolved to {resolved:?}, outside {root:?}"
            );
        }
    }

    /// An accepted path survives the round trip it will actually make: components on the
    /// wire, rebuilt on the other side. A path that stops being acceptable after crossing
    /// would be a file the sender could offer and the receiver could never write.
    #[test]
    fn an_accepted_path_can_be_rebuilt_from_its_own_components(
        components in nasty_components()
    ) {
        if let Ok(safe) = SafeRelPath::from_components(&components, &Limits::DEFAULT) {
            let again = SafeRelPath::from_components(safe.components(), &Limits::DEFAULT);
            prop_assert!(again.is_ok(), "{components:?} did not survive a round trip");
            prop_assert_eq!(again.unwrap(), safe);
        }
    }

    /// Every control message encodes and decodes back to itself. This is what lets the
    /// protocol grow: a new variant that breaks it breaks this test first.
    #[test]
    fn a_control_message_survives_encoding(message in any_control()) {
        let encoded = postcard::to_stdvec(&message).unwrap();
        let decoded = decode(&encoded).unwrap();

        prop_assert_eq!(decoded, message);
    }

    /// Arbitrary bytes are either a message or an error, never a panic. The fuzz target
    /// says the same thing over a longer run; this one runs on every commit.
    #[test]
    fn arbitrary_bytes_never_panic_the_decoder(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
        let _ = decode(&bytes);
    }

    /// Resume arithmetic: the offset the receiver asks for is never past the end of the
    /// file being offered, whatever state its `.part` file is in. A larger offset would be
    /// bytes the file never had.
    #[test]
    fn a_resume_offset_never_points_past_the_end_of_the_file(
        offered_size in 0_u64..1_000_000,
        offered_mtime in 0_u64..1_000,
        on_disk in 0_u64..1_000_000,
        started_size in 0_u64..1_000_000,
        started_mtime in 0_u64..1_000,
        has_hash in any::<bool>(),
    ) {
        let entry = Entry {
            file_id: FileId(1),
            path: vec!["file.bin".to_owned()],
            size: offered_size,
            mtime: offered_mtime,
            mode: 0,
            hash: has_hash.then_some([7; 32]),
        };

        let partial = Partial {
            bytes_on_disk: on_disk,
            expected: Expected {
                size: started_size,
                mtime: started_mtime,
                hash: has_hash.then_some([7; 32]),
            },
        };

        if let Decision::Need { from_offset, .. } = decide(&entry, None, Some(partial)) {
            prop_assert!(
                from_offset <= entry.size,
                "asked to resume at {from_offset} of a {} byte file",
                entry.size
            );
        }
    }

    /// A file the receiver already has, byte for byte, is never asked for again - however
    /// its sizes and timestamps happen to line up. A needless resend costs bandwidth; this
    /// is the other direction, and it is the one that has to be exactly right.
    #[test]
    fn a_matching_hash_always_settles_it(
        size in 0_u64..1_000_000,
        theirs in 0_u64..1_000,
        ours in 0_u64..1_000,
    ) {
        let entry = Entry {
            file_id: FileId(1),
            path: vec!["file.bin".to_owned()],
            size,
            mtime: theirs,
            mode: 0,
            hash: Some([9; 32]),
        };

        let local = LocalFile {
            size,
            mtime: ours,
            hash: Some([9; 32]),
        };

        prop_assert_eq!(decide(&entry, Some(local), None), Decision::Skip);
    }
}

/// Any control message this build can produce.
fn any_control() -> impl Strategy<Value = Control> {
    prop_oneof![
        (any::<u16>(), any::<u64>()).prop_map(|(version, features)| Control::Hello {
            version,
            features,
            device: spd_core::proto::messages::DeviceId::from_bytes([3; 32]),
        }),
        (any::<u32>(), any::<bool>()).prop_map(|(batch_seq, last)| Control::Manifest {
            batch_seq,
            last,
            entries: vec![Entry {
                file_id: FileId(7),
                path: vec!["nested".to_owned(), "file.bin".to_owned()],
                size: 4_096,
                mtime: 1_700_000_000,
                mode: 0o644,
                hash: Some([5; 32]),
            }],
        }),
        (any::<u32>(), any::<u64>()).prop_map(|(batch_seq, file_id)| Control::SyncReply {
            batch_seq,
            decisions: vec![
                Decision::Skip,
                Decision::Need {
                    file_id: FileId(file_id),
                    from_offset: file_id,
                },
            ],
        }),
        (any::<u64>(), any::<u64>()).prop_map(|(files, bytes)| Control::Transfer { files, bytes }),
        any::<u64>().prop_map(|file_id| Control::FileDone {
            file_id: FileId(file_id),
            hash: [1; 32],
        }),
        (any::<u64>(), any::<bool>()).prop_map(|(file_id, ok)| Control::FileVerdict {
            file_id: FileId(file_id),
            ok,
        }),
        any::<[u8; 32]>().prop_map(|proof| Control::Pair { proof }),
        any::<[u8; 32]>().prop_map(|proof| Control::PairAck { proof }),
        (any::<u64>(), any::<u64>()).prop_map(|(files, bytes)| Control::Done { files, bytes }),
        ".*".prop_map(|msg| Control::Error {
            code: ErrorCode::Internal,
            msg,
        }),
    ]
}
