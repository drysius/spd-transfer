//! TLS material: a certificate for the listener, and a client that accepts it.
//!
//! QUIC mandates TLS 1.3, so there is no plaintext mode and no second, unencrypted code
//! path to keep in sync. The certificate is self-signed and generated per run, so there is
//! nothing to verify it against and nothing for a policy to choose between. Who the peer is
//! comes from [`crate::transport::Authentication`], whose proof is bound to the session
//! this material establishes.

use std::sync::Arc;

use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, ring, verify_tls13_signature};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};

use crate::safety::limits::Limits;

/// Application-layer protocol name. Bumped with the protocol version, so a peer speaking
/// a different generation is rejected during the TLS handshake instead of later.
pub const ALPN: &[u8] = b"spd/1";

/// A certificate and its private key, for a listener.
///
/// Self-signed by design: there is no certificate authority in a peer-to-peer transfer.
/// Identity comes from pairing (F7), not from a chain.
pub struct ServerIdentity {
    /// The certificate presented to connecting peers.
    pub certificate: CertificateDer<'static>,
    /// Its private key.
    pub private_key: PrivateKeyDer<'static>,
}

impl ServerIdentity {
    /// Generates a fresh self-signed identity.
    ///
    /// # Errors
    /// [`TlsError::Generate`] if certificate generation fails.
    pub fn self_signed() -> Result<Self, TlsError> {
        let generated = rcgen::generate_simple_self_signed(vec!["spd".to_owned()])
            .map_err(|source| TlsError::Generate { source })?;

        let private_key =
            PrivateKeyDer::try_from(generated.signing_key.serialize_der()).map_err(|reason| {
                TlsError::PrivateKey {
                    reason: reason.to_string(),
                }
            })?;

        Ok(Self {
            certificate: generated.cert.der().clone(),
            private_key,
        })
    }
}

/// Builds the listener's QUIC configuration.
///
/// # Errors
/// [`TlsError::Rustls`] if the TLS configuration is rejected, [`TlsError::Quic`] if QUIC
/// cannot accept it.
pub fn server_config(
    identity: ServerIdentity,
    limits: &Limits,
) -> Result<quinn::ServerConfig, TlsError> {
    let mut tls = rustls::ServerConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|source| TlsError::Rustls { source })?
        .with_no_client_auth()
        .with_single_cert(vec![identity.certificate], identity.private_key)
        .map_err(|source| TlsError::Rustls { source })?;

    tls.alpn_protocols = vec![ALPN.to_vec()];

    let quic = QuicServerConfig::try_from(tls).map_err(|reason| TlsError::Quic {
        reason: reason.to_string(),
    })?;

    let mut config = quinn::ServerConfig::with_crypto(Arc::new(quic));
    config.transport_config(Arc::new(transport_config(limits)?));
    Ok(config)
}

/// Builds the connecting side's QUIC configuration.
///
/// The certificate is always accepted without verification, and that is not a policy knob:
/// a receiver's certificate is self-signed and generated for the run, so there is nothing
/// to verify it against. What proves who is on the other end is
/// [`crate::transport::Authentication`], whose proof is bound to the very TLS session this
/// certificate established - so a substituted certificate breaks the proof rather than
/// passing unnoticed.
///
/// # Errors
/// [`TlsError::Rustls`] if the TLS configuration is rejected, [`TlsError::Quic`] if QUIC
/// cannot accept it.
pub fn client_config(limits: &Limits) -> Result<quinn::ClientConfig, TlsError> {
    let mut tls = rustls::ClientConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|source| TlsError::Rustls { source })?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAnyPeer::new()))
        .with_no_client_auth();

    tls.alpn_protocols = vec![ALPN.to_vec()];

    let quic = QuicClientConfig::try_from(tls).map_err(|reason| TlsError::Quic {
        reason: reason.to_string(),
    })?;

    let mut config = quinn::ClientConfig::new(Arc::new(quic));
    config.transport_config(Arc::new(transport_config(limits)?));
    Ok(config)
}

/// Translates the session limits into QUIC transport parameters.
///
/// The limits a peer can push against live in one place; this is where they reach the
/// wire, so a value tightened on the command line also tightens the transport.
fn transport_config(limits: &Limits) -> Result<quinn::TransportConfig, TlsError> {
    let idle = quinn::IdleTimeout::try_from(limits.idle_timeout).map_err(|_| {
        TlsError::IdleTimeoutTooLarge {
            millis: limits.idle_timeout.as_millis(),
        }
    })?;

    let mut config = quinn::TransportConfig::default();
    config
        .max_concurrent_uni_streams(limits.max_concurrent_streams.into())
        .max_concurrent_bidi_streams(1_u8.into())
        .max_idle_timeout(Some(idle))
        .keep_alive_interval(Some(limits.idle_timeout / 3));

    Ok(config)
}

fn provider() -> Arc<CryptoProvider> {
    Arc::new(ring::default_provider())
}

/// Certificate verifier for [`TrustPolicy::InsecureNoVerification`].
///
/// Named for what it does. It still verifies the handshake *signature*, so the peer must
/// hold the key for the certificate it presented - what it does not do is prove that peer
/// is the one the user meant to reach.
#[derive(Debug)]
struct AcceptAnyPeer {
    provider: Arc<CryptoProvider>,
}

impl AcceptAnyPeer {
    fn new() -> Self {
        Self {
            provider: provider(),
        }
    }
}

impl ServerCertVerifier for AcceptAnyPeer {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        // TLS 1.2 is not offered: QUIC requires 1.3 and the configuration restricts the
        // version list, so reaching this arm would mean the peer negotiated something
        // this build never proposed.
        Err(rustls::Error::PeerIncompatible(
            rustls::PeerIncompatible::Tls12NotOffered,
        ))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Why TLS material could not be prepared.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TlsError {
    /// A self-signed certificate could not be generated.
    #[error("could not generate a self-signed certificate")]
    Generate {
        /// Underlying generator error.
        source: rcgen::Error,
    },

    /// The generated private key was not accepted.
    #[error("the generated private key was rejected: {reason}")]
    PrivateKey {
        /// What the parser reported.
        reason: String,
    },

    /// The TLS configuration was rejected.
    #[error("TLS configuration rejected")]
    Rustls {
        /// Underlying rustls error.
        source: rustls::Error,
    },

    /// The TLS configuration is not usable for QUIC.
    #[error("QUIC rejected the TLS configuration: {reason}")]
    Quic {
        /// What QUIC reported.
        reason: String,
    },

    /// `idle_timeout` exceeds what QUIC can encode.
    #[error("idle_timeout of {millis} ms is larger than QUIC can carry; use a smaller value")]
    IdleTimeoutTooLarge {
        /// Configured value in milliseconds.
        millis: u128,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fresh_identity_is_usable_as_a_server_config() {
        let identity = ServerIdentity::self_signed().unwrap();
        assert!(server_config(identity, &Limits::DEFAULT).is_ok());
    }

    #[test]
    fn two_identities_differ() {
        let first = ServerIdentity::self_signed().unwrap();
        let second = ServerIdentity::self_signed().unwrap();
        assert_ne!(first.certificate, second.certificate);
    }

    #[test]
    fn a_client_config_builds() {
        assert!(client_config(&Limits::DEFAULT).is_ok());
    }
}
