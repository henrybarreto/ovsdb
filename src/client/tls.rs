use std::path::PathBuf;

/// TLS files supplied by the caller.
///
/// # Examples
///
/// CA-only TLS configuration:
///
/// ```rust
/// use ovsdb::client::tls::Options;
/// use std::path::PathBuf;
///
/// let tls = Options {
///     ca_cert: Some(PathBuf::from("/etc/ssl/certs/ovsdb-ca.pem")),
///     client_cert: None,
///     client_key: None,
/// };
/// assert!(tls.ca_cert.is_some());
/// ```
///
/// Mutual-TLS configuration:
///
/// ```rust
/// use ovsdb::client::tls::Options;
/// use std::path::PathBuf;
///
/// let tls = Options {
///     ca_cert: Some(PathBuf::from("/etc/ssl/certs/ovsdb-ca.pem")),
///     client_cert: Some(PathBuf::from("/etc/ssl/certs/client.pem")),
///     client_key: Some(PathBuf::from("/etc/ssl/private/client-key.pem")),
/// };
/// assert!(tls.client_cert.is_some());
/// assert!(tls.client_key.is_some());
/// ```
#[derive(Debug, Clone, Default)]
pub struct Options {
    /// Optional CA bundle file used to extend the system trust store.
    pub ca_cert: Option<PathBuf>,
    /// Optional client certificate file for mutual TLS.
    pub client_cert: Option<PathBuf>,
    /// Optional private key file for mutual TLS.
    pub client_key: Option<PathBuf>,
}
