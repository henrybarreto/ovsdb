use crate::model::rpc;
use std::error::Error as StdError;
use std::fmt::{self, Display, Formatter};

/// Errors returned by the client.
///
/// # Examples
///
/// Match and inspect common error variants:
///
/// ```rust
/// use ovsdb::client::error::Error;
/// use std::io;
///
/// let err = Error::from(io::Error::other("connection reset"));
/// assert!(matches!(err, Error::Transport(_)));
///
/// let err = Error::Validation("bad request".to_string());
/// assert!(matches!(err, Error::Validation(message) if message == "bad request"));
/// ```
#[derive(Debug)]
pub enum Error {
    /// A transport-layer I/O failure occurred.
    Transport(std::io::Error),
    /// JSON parsing or serialization failed.
    Json(serde_json::Error),
    /// The server returned an RPC error object.
    RpcError(rpc::Error),
    /// TLS setup or negotiation failed.
    Tls(rustls::Error),
    /// The server closed the connection before a response arrived.
    ConnectionClosed,
    /// The server returned a response with the wrong shape.
    UnexpectedResponse,
    /// The operation timed out waiting for a response.
    Timeout,
    /// A required field was missing from the response.
    MissingField(&'static str),
    /// The client rejected the payload during local validation.
    Validation(String),
    /// A mutex was poisoned by a panicked thread.
    Poisoned,
    /// The background read loop terminated unexpectedly.
    BackgroundReadLoop(String),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(err) => write!(f, "transport error: {err}"),
            Self::Json(err) => write!(f, "json error: {err}"),
            Self::RpcError(err) => write!(f, "server error: {err:?}"),
            Self::Tls(err) => write!(f, "tls error: {err}"),
            Self::ConnectionClosed => f.write_str("connection closed"),
            Self::UnexpectedResponse => f.write_str("unexpected response"),
            Self::Timeout => f.write_str("timeout"),
            Self::MissingField(field) => write!(f, "missing field {field}"),
            Self::Validation(message) => write!(f, "validation error: {message}"),
            Self::Poisoned => f.write_str("mutex poisoned"),
            Self::BackgroundReadLoop(message) => {
                write!(f, "background read loop failed: {message}")
            }
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Transport(err) => Some(err),
            Self::Json(err) => Some(err),
            Self::Tls(err) => Some(err),
            Self::RpcError(_)
            | Self::ConnectionClosed
            | Self::UnexpectedResponse
            | Self::Timeout
            | Self::MissingField(_)
            | Self::Validation(_)
            | Self::Poisoned
            | Self::BackgroundReadLoop(_) => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::Transport(err)
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Self::Json(err)
    }
}

impl From<rustls::Error> for Error {
    fn from(err: rustls::Error) -> Self {
        Self::Tls(err)
    }
}

impl From<rpc::Error> for Error {
    fn from(err: rpc::Error) -> Self {
        Self::RpcError(err)
    }
}
