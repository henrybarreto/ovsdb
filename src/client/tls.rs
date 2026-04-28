use std::path::PathBuf;

/// TLS files supplied by the caller.
#[derive(Debug, Clone, Default)]
pub struct Options {
    /// Optional CA bundle file used to extend the system trust store.
    pub ca_cert: Option<PathBuf>,
    /// Optional client certificate file for mutual TLS.
    pub client_cert: Option<PathBuf>,
    /// Optional private key file for mutual TLS.
    pub client_key: Option<PathBuf>,
}
