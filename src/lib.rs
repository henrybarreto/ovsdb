//! Shared OVSDB core types and client functionality.
//!
//! Shared schema, datum, validation, utility, and client types live here.

/// OVSDB client library and CLI support.
pub mod client;
/// OVSDB schema, datum, and validation types shared across client and future server code.
pub mod model;
/// Reserved for future server-side OVSDB support.
pub mod server;
/// Shared string helpers.
pub mod strings;
