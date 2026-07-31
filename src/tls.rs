use log::warn;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, ring};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, Error as TlsError, SignatureScheme};
use std::sync::{Arc, LazyLock};
use tokio_postgres::config::SslMode;
use tokio_postgres_rustls::MakeRustlsConnect;

/// Process-wide TLS client configuration, built once and cloned per connection.
///
/// Constructing a [`ClientConfig`] (and the underlying crypto provider) is not
/// free, and `make_tls` is called for every pooled connection, so the config is
/// memoised here and shared. `ClientConfig` is cheap to clone.
static TLS_CONFIG: LazyLock<ClientConfig> = LazyLock::new(|| {
    let provider = Arc::new(ring::default_provider());

    ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        // The ring provider always supports the default protocol versions, so
        // this is infallible in practice.
        .expect("ring provider supports the default TLS protocol versions")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAnyServerCert { provider }))
        .with_no_client_auth()
});

/// Certificate verifier that validates the handshake signature against the
/// presented certificate but skips chain-of-trust and hostname verification.
///
/// This mirrors `libpq`'s `sslmode=prefer`/`require` semantics (encrypt the
/// connection without validating the server certificate), which is what
/// `pg_dump`/`pg_restore` already use. It keeps the native Rust connections
/// compatible with the same self-signed / private-CA endpoints.
#[derive(Debug)]
struct AcceptAnyServerCert {
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for AcceptAnyServerCert {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, TlsError> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        rustls::crypto::verify_tls13_signature(
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

/// Builds a `rustls`-backed TLS connector for `tokio-postgres`.
///
/// See [`AcceptAnyServerCert`] for the verification policy (encrypt without
/// validating the certificate chain or hostname, matching `libpq`).
#[must_use]
pub fn make_tls() -> MakeRustlsConnect {
    MakeRustlsConnect::new(TLS_CONFIG.clone())
}

/// Parses an `sslmode` string into a [`SslMode`].
///
/// Accepts the common `libpq` spellings. `verify-ca`/`verify-full` are treated
/// as `require` since certificate verification is not performed (see
/// [`make_tls`]). Unknown values fall back to `prefer` with a warning.
#[must_use]
pub fn parse_ssl_mode(mode: &str) -> SslMode {
    match mode.trim().to_ascii_lowercase().as_str() {
        "disable" => SslMode::Disable,
        "require" | "verify-ca" | "verify-full" => SslMode::Require,
        "prefer" | "allow" | "" => SslMode::Prefer,
        other => {
            warn!("Unknown sslmode '{other}', falling back to 'prefer'");
            SslMode::Prefer
        }
    }
}

/// Maps a [`SslMode`] back to the spelling understood by `tokio-postgres`
/// connection strings (`disable`/`prefer`/`require`).
#[must_use]
pub const fn ssl_mode_str(mode: SslMode) -> &'static str {
    match mode {
        SslMode::Disable => "disable",
        SslMode::Require => "require",
        _ => "prefer",
    }
}
