use crate::model::RpcError;

/// Errors returned by the client.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("transport error: {0}")]
    /// A transport-layer I/O failure occurred.
    Transport(#[from] std::io::Error),
    #[error("json error: {0}")]
    /// JSON parsing or serialization failed.
    Json(#[from] serde_json::Error),
    #[error("server error: {0:?}")]
    /// The server returned an RPC error object.
    RpcError(RpcError),
    #[error("tls error: {0}")]
    /// TLS setup or negotiation failed.
    Tls(#[from] rustls::Error),
    #[error("connection closed")]
    /// The connection closed before a response arrived.
    ConnectionClosed,
    #[error("unexpected response")]
    /// The server returned a response with the wrong shape.
    UnexpectedResponse,
    #[error("timeout")]
    /// The operation timed out waiting for a response.
    Timeout,
    #[error("missing field {0}")]
    /// A required field was missing from the response.
    MissingField(&'static str),
    #[error("validation error: {0}")]
    /// The client rejected the payload during local validation.
    Validation(String),
    #[error("mutex poisoned")]
    /// A mutex was poisoned by a panicked thread.
    Poisoned,
}
