//! Shared OVSDB core types and client functionality.
//!
//! Shared schema, datum, validation, utility, and client types live here.
//!
//! # Module Guide
//!
//! This crate is split into focused modules:
//!
//! - [`client`]: OVSDB JSON-RPC client, typed notifications, lock APIs, and
//!   operation builders.
//! - [`model`]: schema, notation, and validation types shared across APIs.
//! - [`strings`]: small shared validation helpers.
//! - [`server`]: reserved namespace for future server-side support.
//!
//! If you want to interact with a running OVSDB instance, start with
//! [`client`].
//!
//! The [`server`] module is intentionally not implemented yet; there is no
//! production server runtime exposed by this crate today.
//!
//! # Example
//!
//! Full client flow against an OVSDB server:
//!
//! ```ignore
//! use ovsdb::client::{ops::Ops, Connection, TransactionOutcome};
//! use serde_json::json;
//!
//! let client = Connection::connect("tcp:127.0.0.1:6640", None)?;
//! let dbs = client.list_dbs()?;
//! println!("databases: {dbs:?}");
//!
//! let schema = client.get_schema("OVN_Northbound")?;
//! println!("schema version: {}", schema.version);
//!
//! let tx = vec![
//!     Ops::comment("docs example"),
//!     Ops::select("Logical_Switch", &[], Some(&["name".to_string()])),
//! ];
//! let reply = client.transact("OVN_Northbound", tx)?;
//! for outcome in &reply.entries {
//!     if let TransactionOutcome::Select { rows } = outcome {
//!         println!("selected rows: {}", rows.len());
//!     }
//! }
//! # Ok::<(), ovsdb::client::error::Error>(())
//! ```

/// OVSDB client library and CLI support.
pub mod client;
/// OVSDB schema, datum, and validation types shared across client and future server code.
pub mod model;
/// Reserved for future server-side OVSDB support.
pub mod server;
/// Shared string helpers.
pub mod strings;
