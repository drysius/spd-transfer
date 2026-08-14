//! Proving that both ends know the same short code.
//!
//! QUIC encrypts everything, but a self-signed certificate generated a second ago proves
//! nothing about who presented it. Pairing is what turns that encrypted pipe into a pipe to
//! a particular machine: the receiver shows a code, the user carries it to the sender, and
//! each side proves it knows the code without ever putting it on the wire.
//!
//! The proof is bound to the TLS session it travels in. Both sides export keying material
//! from their own session and hash it with the code; someone sitting in the middle
//! necessarily has two different TLS sessions, so the proof they could replay from one is
//! not the proof the other side expects. A relay without the code cannot produce either.
//!
//! What this is not: a PAKE. Someone who records a session can guess codes against the
//! proof offline, so the code carries [`CODE_CHARS`] × 5 bits of entropy rather than the
//! four digits a phone pairing prompt gets away with. Replacing this with a real PAKE
//! changes only this module and the two messages it defines.

use core::fmt;

use crate::proto::messages::RandomnessError;

/// Characters a code is written in.
///
/// Crockford base32 without `I`, `L`, `O` and `U`: the first three are misread as digits
/// and the fourth turns up in words nobody wants to read out.
const ALPHABET: [u8; 32] = *b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// Characters in a generated code, shown in two groups of five.
pub const CODE_CHARS: usize = 10;

/// Where the group separator goes when a code is displayed.
const GROUP: usize = 5;

/// Bytes of channel binding taken from the TLS session.
pub(crate) const BINDING_BYTES: usize = 32;

/// Label the binding is exported under.
///
/// Carries its own version: two builds that disagree about this cannot pair, which is the
/// correct outcome, and is better than agreeing on a binding that means different things.
pub(crate) const BINDING_LABEL: &[u8] = b"spd pairing v1 channel binding";

/// Context string for turning a code into a key. Fixed, unique, and never reused for
/// anything else - that is the whole contract of `derive_key`.
const KEY_CONTEXT: &str = "spd-transfer 2026-08 pairing code";

/// A pairing code: the one secret a user carries from one machine to the other.
#[derive(Clone, PartialEq, Eq)]
pub struct PairingCode(String);

impl PairingCode {
    /// Draws a fresh code from the operating system's randomness.
    ///
    /// Rejection-free: the alphabet has exactly 32 characters, so five bits map to one
    /// character with no bias to correct for.
    ///
    /// # Errors
    /// [`RandomnessError`] if the OS refuses to provide entropy. A predictable pairing code
    /// is worse than no transfer, so this is reported rather than worked around.
    pub fn random() -> Result<Self, RandomnessError> {
        let mut bytes = [0_u8; CODE_CHARS];
        getrandom::fill(&mut bytes).map_err(|source| RandomnessError {
            code: source.to_string(),
        })?;

        let code = bytes
            .iter()
            .map(|byte| char::from(ALPHABET[usize::from(byte % 32)]))
            .collect();

        Ok(Self(code))
    }

    /// Reads a code as a user typed it.
    ///
    /// Forgiving about presentation and strict about content: case, spaces and dashes are
    /// normalised away, and `O` and `I` are read as the digits they are usually mistaken
    /// for. Anything else is refused rather than quietly turned into a different code.
    ///
    /// # Errors
    /// [`PairingError::Malformed`] naming the character that could not be read, or
    /// [`PairingError::WrongLength`] if the result is not [`CODE_CHARS`] characters.
    pub fn parse(text: &str) -> Result<Self, PairingError> {
        let mut code = String::with_capacity(CODE_CHARS);

        for character in text.chars() {
            if character == '-' || character.is_whitespace() {
                continue;
            }

            let upper = character.to_ascii_uppercase();
            let normalised = match upper {
                'O' => '0',
                'I' | 'L' => '1',
                other => other,
            };

            if !ALPHABET.contains(&u8::try_from(normalised).unwrap_or(0)) {
                return Err(PairingError::Malformed { character });
            }

            code.push(normalised);
        }

        if code.chars().count() != CODE_CHARS {
            return Err(PairingError::WrongLength {
                got: code.chars().count(),
                want: CODE_CHARS,
            });
        }

        Ok(Self(code))
    }

    /// The code as it should be typed on the other machine.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The key both sides derive from the code.
    fn key(&self) -> [u8; 32] {
        blake3::derive_key(KEY_CONTEXT, self.0.as_bytes())
    }
}

/// Grouped for reading aloud: `A1B2C-D3E4F`.
impl fmt::Display for PairingCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, character) in self.0.chars().enumerate() {
            if index > 0 && index % GROUP == 0 {
                f.write_str("-")?;
            }
            write!(f, "{character}")?;
        }
        Ok(())
    }
}

/// Never prints the code.
///
/// A secret that ends up in a log because someone derived `Debug` on the struct holding it
/// is a classic, and the type is where it gets prevented.
impl fmt::Debug for PairingCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PairingCode(hidden)")
    }
}

/// How a session proves who is on the other end.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Authentication {
    /// Both sides know this code, and each proves it before anything is transferred.
    Code(PairingCode),

    /// Nothing is proved. The traffic is still encrypted - QUIC has no plaintext mode -
    /// but any peer that can reach the port is accepted.
    Insecure,
}

impl Authentication {
    /// The code, when there is one.
    pub const fn code(&self) -> Option<&PairingCode> {
        match self {
            Self::Code(code) => Some(code),
            Self::Insecure => None,
        }
    }

    /// Whether this side will refuse a peer that does not pair.
    pub const fn requires_pairing(&self) -> bool {
        matches!(self, Self::Code(_))
    }
}

/// Which end of the session a proof came from.
///
/// The two proofs differ so neither side can replay the other's: an attacker who somehow
/// obtains one still cannot answer with it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Side {
    /// The peer that dialled.
    Caller,
    /// The peer that was dialled.
    Answerer,
}

impl Side {
    const fn tag(self) -> &'static [u8] {
        match self {
            Self::Caller => b"caller",
            Self::Answerer => b"answerer",
        }
    }
}

/// One side's proof that it knows the code, for this session and no other.
pub(crate) struct Proof(blake3::Hash);

impl Proof {
    /// Computes the proof `side` should send for this session.
    pub(crate) fn compute(code: &PairingCode, side: Side, binding: &[u8; BINDING_BYTES]) -> Self {
        let mut message = Vec::with_capacity(side.tag().len() + BINDING_BYTES);
        message.extend_from_slice(side.tag());
        message.extend_from_slice(binding);

        Self(blake3::keyed_hash(&code.key(), &message))
    }

    /// The bytes to put on the wire.
    pub(crate) fn bytes(&self) -> [u8; 32] {
        *self.0.as_bytes()
    }

    /// Whether a peer's proof is the one expected here.
    ///
    /// The comparison is `blake3::Hash`'s, which is constant-time: comparing the arrays
    /// directly would leak how many leading bytes a guess got right.
    pub(crate) fn verify(&self, claimed: [u8; 32]) -> Result<(), PairingError> {
        if self.0 == blake3::Hash::from(claimed) {
            Ok(())
        } else {
            Err(PairingError::Mismatch)
        }
    }
}

/// Never prints the proof.
impl fmt::Debug for Proof {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Proof(hidden)")
    }
}

/// Why pairing failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum PairingError {
    /// The peer's proof is not the one this code produces.
    #[error("the pairing code does not match the one on the other machine")]
    Mismatch,

    /// A code contains something that is not part of the alphabet.
    #[error("a pairing code cannot contain {character:?}")]
    Malformed {
        /// The character that was refused.
        character: char,
    },

    /// A code is the wrong length.
    #[error("a pairing code is {want} characters long, not {got}")]
    WrongLength {
        /// What was given.
        got: usize,
        /// What is required.
        want: usize,
    },

    /// This side wants to pair and the peer does not, or the other way round.
    #[error(
        "this side {ours} a pairing code and the peer {theirs} one; run both with --code, or \
         both with --insecure"
    )]
    Disagreement {
        /// What this side is doing, as it appears in the message.
        ours: &'static str,
        /// What the peer is doing.
        theirs: &'static str,
    },

    /// The TLS session would not produce channel binding material.
    #[error("this session cannot be bound to a pairing proof: {reason}")]
    Unbindable {
        /// What the transport reported.
        reason: &'static str,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding(fill: u8) -> [u8; BINDING_BYTES] {
        [fill; BINDING_BYTES]
    }

    #[test]
    fn a_generated_code_is_the_right_length_and_readable() {
        let code = PairingCode::random().unwrap();

        assert_eq!(code.as_str().chars().count(), CODE_CHARS);
        assert!(
            code.as_str().bytes().all(|byte| ALPHABET.contains(&byte)),
            "a generated code must only use characters a user can type back"
        );
        assert_eq!(
            code.to_string().len(),
            CODE_CHARS + 1,
            "one group separator"
        );
    }

    #[test]
    fn two_generated_codes_differ() {
        assert_ne!(
            PairingCode::random().unwrap(),
            PairingCode::random().unwrap()
        );
    }

    #[test]
    fn a_code_reads_back_however_it_was_typed() {
        let typed = PairingCode::parse("a1b2c-d3e4f").unwrap();

        assert_eq!(typed, PairingCode::parse("A1B2C D3E4F").unwrap());
        assert_eq!(typed, PairingCode::parse("A1B2CD3E4F").unwrap());
        assert_eq!(typed.to_string(), "A1B2C-D3E4F");
    }

    #[test]
    fn the_characters_people_misread_are_read_as_the_digits_they_look_like() {
        assert_eq!(
            PairingCode::parse("O1B2C-D3E4F").unwrap(),
            PairingCode::parse("01B2C-D3E4F").unwrap()
        );
        assert_eq!(
            PairingCode::parse("AIB2C-D3E4F").unwrap(),
            PairingCode::parse("A1B2C-D3E4F").unwrap()
        );
    }

    #[test]
    fn a_code_with_a_character_that_is_not_in_the_alphabet_is_refused() {
        assert_eq!(
            PairingCode::parse("A1B2C-D3E4$").unwrap_err(),
            PairingError::Malformed { character: '$' }
        );
    }

    #[test]
    fn a_code_of_the_wrong_length_is_refused() {
        assert_eq!(
            PairingCode::parse("A1B2C").unwrap_err(),
            PairingError::WrongLength {
                got: 5,
                want: CODE_CHARS
            }
        );
    }

    #[test]
    fn a_code_never_prints_itself_by_accident() {
        let code = PairingCode::parse("A1B2C-D3E4F").unwrap();
        assert_eq!(format!("{code:?}"), "PairingCode(hidden)");
    }

    #[test]
    fn the_same_code_and_session_produce_the_same_proof() {
        let code = PairingCode::parse("A1B2C-D3E4F").unwrap();

        let mine = Proof::compute(&code, Side::Caller, &binding(7));
        let theirs = Proof::compute(&code, Side::Caller, &binding(7));

        assert!(mine.verify(theirs.bytes()).is_ok());
    }

    #[test]
    fn another_code_does_not_produce_the_proof() {
        let ours = PairingCode::parse("A1B2C-D3E4F").unwrap();
        let theirs = PairingCode::parse("Z9Y8X-W7V6T").unwrap();

        let expected = Proof::compute(&ours, Side::Caller, &binding(7));
        let offered = Proof::compute(&theirs, Side::Caller, &binding(7));

        assert_eq!(
            expected.verify(offered.bytes()).unwrap_err(),
            PairingError::Mismatch
        );
    }

    #[test]
    fn a_proof_from_another_session_does_not_transfer() {
        let code = PairingCode::parse("A1B2C-D3E4F").unwrap();

        let here = Proof::compute(&code, Side::Caller, &binding(1));
        let elsewhere = Proof::compute(&code, Side::Caller, &binding(2));

        assert_eq!(
            here.verify(elsewhere.bytes()).unwrap_err(),
            PairingError::Mismatch,
            "a proof recorded from one session must be useless in another"
        );
    }

    #[test]
    fn neither_side_can_replay_the_other_side_s_proof() {
        let code = PairingCode::parse("A1B2C-D3E4F").unwrap();

        let caller = Proof::compute(&code, Side::Caller, &binding(3));
        let answerer = Proof::compute(&code, Side::Answerer, &binding(3));

        assert_eq!(
            caller.verify(answerer.bytes()).unwrap_err(),
            PairingError::Mismatch
        );
    }
}
