//! The wire format, frozen.
//!
//! Every message here is encoded and compared against bytes committed to the repository. A
//! change to a field, an order, or an enum variant changes those bytes, and this test is
//! what turns "the protocol accidentally moved" into a failure now rather than a peer on
//! the old version failing later.
//!
//! Regenerating is deliberate: run with `SPD_UPDATE_GOLDEN=1` to rewrite the files, and the
//! diff in review is then the protocol change itself, stated in bytes.

use std::path::PathBuf;

use spd_core::proto::codec::decode;
use spd_core::proto::messages::{
    Control, DataHeader, Decision, DeviceId, Entry, ErrorCode, FileId,
};
use spd_core::proto::version::{Features, PROTOCOL_VERSION};

/// Where the frozen bytes live.
fn golden(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("golden")
        .join(format!("{name}.bin"))
}

/// Compares `encoded` with the file for `name`, or writes it when asked to.
fn check(name: &str, encoded: &[u8]) {
    let path = golden(name);

    if std::env::var_os("SPD_UPDATE_GOLDEN").is_some() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, encoded).unwrap();
        return;
    }

    let expected = std::fs::read(&path).unwrap_or_else(|error| {
        panic!(
            "no frozen bytes for {name} at {}: {error}\n\
             run with SPD_UPDATE_GOLDEN=1 to create them",
            path.display()
        )
    });

    assert_eq!(
        encoded,
        expected.as_slice(),
        "the wire format of {name} changed.\n\
         If that was intended, bump PROTOCOL_VERSION and rerun with SPD_UPDATE_GOLDEN=1."
    );
}

/// A device id with no randomness in it, so the frozen bytes are stable.
fn device() -> DeviceId {
    DeviceId::from_bytes([0xAB; 32])
}

/// One of each message, with values chosen to exercise every field.
fn every_message() -> Vec<(&'static str, Control)> {
    vec![
        (
            "hello",
            Control::Hello {
                version: PROTOCOL_VERSION,
                features: Features::announced(true).bits(),
                device: device(),
            },
        ),
        (
            "hello_ack",
            Control::HelloAck {
                version: PROTOCOL_VERSION,
                features: Features::announced(false).bits(),
                device: device(),
            },
        ),
        ("pair", Control::Pair { proof: [0x11; 32] }),
        ("pair_ack", Control::PairAck { proof: [0x22; 32] }),
        (
            "manifest",
            Control::Manifest {
                batch_seq: 3,
                last: true,
                entries: vec![
                    Entry {
                        file_id: FileId(1),
                        path: vec!["nested".to_owned(), "photo.jpg".to_owned()],
                        size: 4_096,
                        mtime: 1_700_000_000,
                        mode: 0o644,
                        hash: Some([0x33; 32]),
                    },
                    Entry {
                        file_id: FileId(2),
                        path: vec!["big.bin".to_owned()],
                        size: u64::MAX,
                        mtime: 0,
                        mode: 0,
                        hash: None,
                    },
                ],
            },
        ),
        (
            "sync_reply",
            Control::SyncReply {
                batch_seq: 3,
                decisions: vec![
                    Decision::Skip,
                    Decision::Need {
                        file_id: FileId(2),
                        from_offset: 1_048_576,
                    },
                ],
            },
        ),
        (
            "transfer",
            Control::Transfer {
                files: 2,
                bytes: 1_048_576,
            },
        ),
        (
            "file_done",
            Control::FileDone {
                file_id: FileId(2),
                hash: [0x44; 32],
            },
        ),
        (
            "file_verdict",
            Control::FileVerdict {
                file_id: FileId(2),
                ok: true,
            },
        ),
        (
            "done",
            Control::Done {
                files: 2,
                bytes: 1_048_576,
            },
        ),
        (
            "error",
            Control::Error {
                code: ErrorCode::Unauthorized,
                msg: "the pairing code does not match".to_owned(),
            },
        ),
    ]
}

#[test]
fn every_control_message_still_encodes_to_the_bytes_it_did() {
    for (name, message) in every_message() {
        let encoded = postcard::to_stdvec(&message).unwrap();
        check(name, &encoded);
    }
}

#[test]
fn the_frozen_bytes_still_decode_to_what_they_meant() {
    for (name, message) in every_message() {
        let bytes = std::fs::read(golden(name)).unwrap_or_else(|error| {
            panic!("no frozen bytes for {name}: {error}");
        });

        assert_eq!(
            decode(&bytes).unwrap(),
            message,
            "{name} decoded to something other than what it was frozen from"
        );
    }
}

#[test]
fn the_data_stream_header_still_encodes_to_the_bytes_it_did() {
    let header = DataHeader {
        file_id: FileId(2),
        offset: 1_048_576,
        compressed: true,
    };

    check("data_header", &postcard::to_stdvec(&header).unwrap());

    let bytes = std::fs::read(golden("data_header")).unwrap();
    assert_eq!(postcard::from_bytes::<DataHeader>(&bytes).unwrap(), header);
}

#[test]
fn the_protocol_version_is_part_of_what_is_frozen() {
    // A version bump is how a peer knows the format changed. Freezing it here means the
    // bump cannot be forgotten quietly: the golden files change with it.
    assert_eq!(
        PROTOCOL_VERSION, 1,
        "bumping the version means regenerating the golden files in the same commit"
    );
}
