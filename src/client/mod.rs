//! OVSDB client API, notifications, and transaction helpers.
//!
//! This module provides:
//!
//! - connection management and request/response handling over `tcp:`, `unix:`,
//!   `tls:`, and `ssl:` endpoints.
//! - helpers to build and validate OVSDB transaction operations before sending
//!   them.
//! - typed models for transaction outcomes and asynchronous notifications.
//! - TLS configuration for CA trust and optional mutual-auth client
//!   certificates.
//!
//! # Usage
//!
//! Connect without TLS:
//!
//! ```ignore
//! use ovsdb::client::Connection;
//!
//! let client = Connection::connect("tcp:127.0.0.1:6640", None)?;
//! let dbs = client.list_dbs()?;
//! println!("dbs: {dbs:?}");
//! # Ok::<(), ovsdb::client::error::Error>(())
//! ```
//!
//! Connect with TLS options:
//!
//! ```ignore
//! use ovsdb::client::{tls, Connection};
//! use std::path::PathBuf;
//!
//! let tls = tls::Options {
//!     ca_cert: Some(PathBuf::from("/etc/ssl/certs/ovsdb-ca.pem")),
//!     client_cert: Some(PathBuf::from("/etc/ssl/certs/client.pem")),
//!     client_key: Some(PathBuf::from("/etc/ssl/private/client-key.pem")),
//! };
//!
//! let client = Connection::connect("ssl:ovsdb.example.net:6640", Some(&tls))?;
//! let schema = client.get_schema("OVN_Northbound")?;
//! println!("schema version: {}", schema.version);
//! # Ok::<(), ovsdb::client::error::Error>(())
//! ```
//!
//! Full transaction + monitor flow:
//!
//! ```ignore
//! use ovsdb::client::{ops::Ops, Connection, TransactionOutcome, TableUpdates};
//! use serde_json::json;
//! use std::collections::HashMap;
//!
//! let client = Connection::connect("tcp:127.0.0.1:6640", None)?;
//!
//! // 1) Read available DBs and schema metadata.
//! let dbs = client.list_dbs()?;
//! assert!(dbs.iter().any(|db| db == "Open_vSwitch"));
//! let schema = client.get_schema("Open_vSwitch")?;
//! println!("schema tables: {}", schema.tables.len());
//!
//! // 2) Build and run a transaction.
//! let tx = vec![
//!     Ops::comment("create bridge"),
//!     Ops::insert("Bridge", json!({"name": "br-demo"}), Some("new_bridge")),
//!     Ops::select("Bridge", &[json!(["name", "==", "br-demo"])], Some(&["name".to_string()])),
//!     Ops::delete("Bridge", &[json!(["name", "==", "br-demo"])]),
//! ];
//!
//! let reply = client.transact("Open_vSwitch", tx)?;
//!
//! // 3) Decode typed outcomes.
//! for outcome in &reply.entries {
//!     match outcome {
//!         TransactionOutcome::Insert { uuid } => println!("inserted: {uuid}"),
//!         TransactionOutcome::Select { rows } => println!("rows: {rows:?}"),
//!         TransactionOutcome::Error(err) => println!("operation error: {err:?}"),
//!         _ => {}
//!     }
//! }
//!
//! // 4) Start monitor (example shape).
//! let mut requests = HashMap::new();
//! requests.insert(
//!     "Bridge".to_string(),
//!     json!([{
//!         "columns": ["name"],
//!         "select": {
//!             "initial": true,
//!             "insert": true,
//!             "delete": true,
//!             "modify": true
//!         }
//!     }]),
//! );
//! let monitor_id = json!("docs-monitor");
//! let initial: TableUpdates = client.monitor("Open_vSwitch", &monitor_id, &requests)?;
//! println!("initial monitor tables: {}", initial.0.len());
//! # Ok::<(), ovsdb::client::error::Error>(())
//! ```

/// Client-specific error types.
pub mod error;
/// Transaction operation builders and validators.
pub mod ops;
/// JSON-RPC request encoding and validation helpers.
pub mod rpc;
/// Client TLS configuration types.
pub mod tls;
mod transport;

use self::error::Error;
use self::rpc::Validator;
use self::tls as TLS;
use crate::model::{self, DatabaseSchema};
use crate::strings::reject_null_bytes;
use crossbeam_channel::{bounded, RecvTimeoutError, Sender};
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, RootCertStore};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use transport::BoxedIo;

/// A connected OVSDB client.
#[derive(Clone)]
pub struct Connection {
    io: Arc<Mutex<BoxedIo>>,
    read_buf: Arc<Mutex<Vec<u8>>>,
    id_counter: Arc<AtomicU64>,
    notifications: Arc<Mutex<VecDeque<Notification>>>,
    pending_requests: Arc<Mutex<HashMap<u64, Sender<Value>>>>,
    notification_cond: Arc<(Mutex<bool>, Condvar)>,
    cancelled_requests: Arc<Mutex<HashSet<u64>>>,
    lock_states: Arc<Mutex<HashMap<String, LockState>>>,
    schema_cache: Arc<Mutex<HashMap<String, DatabaseSchema>>>,
    background_error: Arc<Mutex<Option<String>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LockState {
    Idle,
    PendingUnlock,
}

/// A UUID value encoded as a string wrapper.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Uuid(pub String);

/// A row value keyed by column name.
pub type Row = serde_json::Map<String, Value>;

/// Row updates keyed by table and row UUID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct TableUpdates(pub HashMap<String, HashMap<String, RowUpdate>>);

impl TableUpdates {
    /// Return the update map for a table if it exists.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use ovsdb::client::{RowUpdate, TableUpdates};
    /// use std::collections::HashMap;
    ///
    /// let updates = TableUpdates(HashMap::from([(
    ///     "T".to_string(),
    ///     HashMap::from([("row".to_string(), RowUpdate::default())]),
    /// )]));
    /// assert!(updates.get("T").is_some());
    /// assert!(updates.get("missing").is_none());
    /// ```
    pub fn get(&self, table: &str) -> Option<&HashMap<String, RowUpdate>> {
        self.0.get(table)
    }
}

/// The old and new values for a row update.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RowUpdate {
    /// The previous row value, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old: Option<Row>,
    /// The new row value, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new: Option<Row>,
}

impl RowUpdate {
    /// Return the previous row value, if any.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use ovsdb::client::RowUpdate;
    /// use serde_json::json;
    ///
    /// let update = serde_json::from_value::<RowUpdate>(json!({
    ///     "old": {"name": "before"},
    ///     "new": {"name": "after"}
    /// }))
    /// .expect("row update json");
    /// assert!(update.old().is_some());
    /// ```
    pub const fn old(&self) -> Option<&Row> {
        self.old.as_ref()
    }

    /// Return the new row value, if any.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use ovsdb::client::RowUpdate;
    /// use serde_json::json;
    ///
    /// let update = serde_json::from_value::<RowUpdate>(json!({
    ///     "new": {"name": "after"}
    /// }))
    /// .expect("row update json");
    /// assert!(update.new_row().is_some());
    /// ```
    pub const fn new_row(&self) -> Option<&Row> {
        self.new.as_ref()
    }
}

/// A typed notification received from the server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Notification {
    /// A monitor update notification.
    Update {
        /// The RPC method name used by the notification.
        method: String,
        /// The monitor identifier.
        monitor_id: String,
        /// The table updates included in the notification.
        updates: TableUpdates,
    },
    /// A lock was granted.
    Locked(String),
    /// A lock was stolen by another client.
    Stolen(String),
}

impl Notification {
    /// Return the RPC method name associated with the notification.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use ovsdb::client::Notification;
    ///
    /// let n = Notification::Locked("l1".to_string());
    /// assert_eq!(n.method(), "locked");
    /// ```
    pub fn method(&self) -> &str {
        match self {
            Self::Update { method, .. } => method,
            Self::Locked(_) => "locked",
            Self::Stolen(_) => "stolen",
        }
    }

    /// Return the monitor identifier if the notification carries one.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use ovsdb::client::{Notification, TableUpdates};
    /// use std::collections::HashMap;
    ///
    /// let n = Notification::Update {
    ///     method: "update".to_string(),
    ///     monitor_id: "m1".to_string(),
    ///     updates: TableUpdates(HashMap::new()),
    /// };
    /// assert_eq!(n.monitor_id().map(String::as_str), Some("m1"));
    /// ```
    pub const fn monitor_id(&self) -> Option<&String> {
        match self {
            Self::Update { monitor_id, .. } => Some(monitor_id),
            Self::Locked(_) | Self::Stolen(_) => None,
        }
    }

    /// Return the lock identifier if the notification carries one.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use ovsdb::client::Notification;
    ///
    /// let n = Notification::Stolen("lock-a".to_string());
    /// assert_eq!(n.lock_id().map(String::as_str), Some("lock-a"));
    /// ```
    pub const fn lock_id(&self) -> Option<&String> {
        match self {
            Self::Locked(lock_id) | Self::Stolen(lock_id) => Some(lock_id),
            Self::Update { .. } => None,
        }
    }

    /// Return the table updates if the notification carries them.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use ovsdb::client::{Notification, TableUpdates};
    /// use std::collections::HashMap;
    ///
    /// let n = Notification::Update {
    ///     method: "update".to_string(),
    ///     monitor_id: "m1".to_string(),
    ///     updates: TableUpdates(HashMap::new()),
    /// };
    /// assert!(n.updates().is_some());
    /// ```
    pub const fn updates(&self) -> Option<&TableUpdates> {
        match self {
            Self::Update { updates, .. } => Some(updates),
            Self::Locked(_) | Self::Stolen(_) => None,
        }
    }
}

/// A typed transaction response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionResponse {
    /// The decoded transaction outcomes in response order.
    pub entries: Vec<TransactionOutcome>,
}

impl TransactionResponse {
    /// Return the number of decoded outcomes.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use ovsdb::client::{TransactionOutcome, TransactionResponse};
    ///
    /// let response = TransactionResponse {
    ///     entries: vec![TransactionOutcome::Empty],
    /// };
    /// assert_eq!(response.len(), 1);
    /// ```
    pub const fn len(&self) -> usize {
        self.entries.len()
    }

    /// Return `true` when the response contains no outcomes.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use ovsdb::client::TransactionResponse;
    ///
    /// let response = TransactionResponse { entries: vec![] };
    /// assert!(response.is_empty());
    /// ```
    pub const fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Return the outcome at the given index, if present.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use ovsdb::client::{TransactionOutcome, TransactionResponse};
    ///
    /// let response = TransactionResponse {
    ///     entries: vec![TransactionOutcome::Count { count: 2 }],
    /// };
    /// assert!(response.get(0).is_some());
    /// assert!(response.get(1).is_none());
    /// ```
    pub fn get(&self, idx: usize) -> Option<&TransactionOutcome> {
        self.entries.get(idx)
    }
}

/// A single decoded transaction outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionOutcome {
    /// An inserted row UUID.
    Insert {
        /// The inserted row UUID.
        uuid: String,
    },
    /// Rows returned by a `select` operation.
    Select {
        /// The selected rows.
        rows: Vec<Row>,
    },
    /// A count returned by a `select` operation.
    Count {
        /// The count value.
        count: u64,
    },
    /// A successful operation with no payload.
    Empty,
    /// An operation-level error returned by the server.
    Error(model::rpc::Error),
    /// A null response entry.
    Null,
}

impl TransactionOutcome {
    /// Return the inserted UUID if this outcome is an insert.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use ovsdb::client::TransactionOutcome;
    ///
    /// let outcome = TransactionOutcome::Insert {
    ///     uuid: "u".to_string(),
    /// };
    /// assert_eq!(outcome.uuid().map(String::as_str), Some("u"));
    /// ```
    pub const fn uuid(&self) -> Option<&String> {
        match self {
            Self::Insert { uuid } => Some(uuid),
            _ => None,
        }
    }

    /// Return the selected rows if this outcome is a select.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use ovsdb::client::TransactionOutcome;
    /// use serde_json::json;
    ///
    /// let rows = vec![serde_json::from_value(json!({"name":"r1"})).expect("row")];
    /// let outcome = TransactionOutcome::Select { rows };
    /// assert!(outcome.rows().is_some());
    /// ```
    pub const fn rows(&self) -> Option<&Vec<Row>> {
        match self {
            Self::Select { rows } => Some(rows),
            _ => None,
        }
    }

    /// Return the count if this outcome is a count result.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use ovsdb::client::TransactionOutcome;
    ///
    /// let outcome = TransactionOutcome::Count { count: 3 };
    /// assert_eq!(outcome.count(), Some(3));
    /// ```
    pub const fn count(&self) -> Option<u64> {
        match self {
            Self::Count { count } => Some(*count),
            _ => None,
        }
    }

    /// Return the server error if this outcome is an error.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use ovsdb::client::TransactionOutcome;
    /// use ovsdb::model::rpc;
    /// use serde_json::json;
    ///
    /// let err: rpc::Error = serde_json::from_value(json!({
    ///     "error": "not owner",
    ///     "details": "lock mismatch"
    /// }))
    /// .expect("rpc error");
    /// let outcome = TransactionOutcome::Error(err);
    /// assert!(outcome.error().is_some());
    /// ```
    pub const fn error(&self) -> Option<&model::rpc::Error> {
        match self {
            Self::Error(error) => Some(error),
            _ => None,
        }
    }

    /// Return `true` if this outcome is the empty variant.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use ovsdb::client::TransactionOutcome;
    ///
    /// assert!(TransactionOutcome::Empty.is_empty());
    /// assert!(!TransactionOutcome::Null.is_empty());
    /// ```
    pub const fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
    }
}

impl Connection {
    /// Connect to an OVSDB endpoint over TCP, Unix socket, or TLS.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use ovsdb::client::Connection;
    ///
    /// let client = Connection::connect("tcp:127.0.0.1:6640", None)?;
    /// # let _ = client;
    /// # Ok::<(), ovsdb::client::error::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns transport, TLS, timeout, validation, or response-shape errors.
    pub fn connect(address: &str, tls: Option<&TLS::Options>) -> Result<Self, Error> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        reject_null_bytes(address).map_err(|e| Error::Validation(e.to_string()))?;
        let tls = tls.cloned().unwrap_or_default();
        let io: BoxedIo = if let Some(path) = address.strip_prefix("unix:") {
            Box::new(UnixStream::connect(path)?)
        } else if let Some(addr) = address
            .strip_prefix("ssl:")
            .or_else(|| address.strip_prefix("tls:"))
        {
            let host = addr.split_once(':').map_or(addr, |(host, _)| host);
            let tcp = TcpStream::connect(addr)?;
            let mut root_cert_store = RootCertStore::empty();
            let result = rustls_native_certs::load_native_certs();
            for cert in result.certs {
                root_cert_store
                    .add(cert)
                    .map_err(|e| rustls::Error::General(e.to_string()))?;
            }

            if let Some(ca_path) = tls.ca_cert.as_ref() {
                let file = std::fs::File::open(ca_path)
                    .map_err(|e| Error::Validation(format!("failed to open CA cert: {e}")))?;
                let mut reader = std::io::BufReader::new(file);
                for cert in rustls_pemfile::certs(&mut reader) {
                    root_cert_store
                        .add(cert.map_err(|e| rustls::Error::General(e.to_string()))?)
                        .map_err(|e| rustls::Error::General(e.to_string()))?;
                }
            }

            let config_builder = ClientConfig::builder().with_root_certificates(root_cert_store);

            let config = if let (Some(cert_path), Some(key_path)) =
                (tls.client_cert.as_ref(), tls.client_key.as_ref())
            {
                let cert_file = std::fs::File::open(cert_path)
                    .map_err(|e| Error::Validation(format!("failed to open client cert: {e}")))?;
                let mut cert_reader = std::io::BufReader::new(cert_file);
                let certs = rustls_pemfile::certs(&mut cert_reader)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| rustls::Error::General(e.to_string()))?;

                let key_file = std::fs::File::open(key_path)
                    .map_err(|e| Error::Validation(format!("failed to open client key: {e}")))?;
                let mut key_reader = std::io::BufReader::new(key_file);
                let key = rustls_pemfile::private_key(&mut key_reader)
                    .map_err(|e| rustls::Error::General(e.to_string()))?
                    .ok_or_else(|| {
                        Error::Validation("no private key found in client key file".into())
                    })?;

                config_builder
                    .with_client_auth_cert(certs, key)
                    .map_err(|e| rustls::Error::General(e.to_string()))?
            } else if tls.client_cert.is_some() || tls.client_key.is_some() {
                return Err(Error::Validation(
                    "both client cert and client key are required for mutual TLS".into(),
                ));
            } else {
                config_builder.with_no_client_auth()
            };

            let domain = ServerName::try_from(host)
                .map_err(|_| Error::Validation("invalid domain".into()))?
                .to_owned();
            let conn = rustls::ClientConnection::new(Arc::new(config), domain)?;
            Box::new(rustls::StreamOwned::new(conn, tcp))
        } else {
            let addr = address.strip_prefix("tcp:").unwrap_or(address);
            let tcp = TcpStream::connect(addr)?;
            tcp.set_nodelay(true)?;
            Box::new(tcp)
        };

        let client = Self {
            io: Arc::new(Mutex::new(io)),
            read_buf: Arc::new(Mutex::new(Vec::new())),
            id_counter: Arc::new(AtomicU64::new(1)),
            notifications: Arc::new(Mutex::new(VecDeque::new())),
            pending_requests: Arc::new(Mutex::new(HashMap::new())),
            notification_cond: Arc::new((Mutex::new(false), Condvar::new())),
            cancelled_requests: Arc::new(Mutex::new(HashSet::new())),
            lock_states: Arc::new(Mutex::new(HashMap::new())),
            schema_cache: Arc::new(Mutex::new(HashMap::new())),
            background_error: Arc::new(Mutex::new(None)),
        };

        let reader = client.clone();
        std::thread::spawn(move || {
            if let Err(err) = reader.background_read_loop() {
                if let Ok(mut bg) = reader.background_error.lock() {
                    *bg = Some(err.to_string());
                }

                if let Ok(mut pending) = reader.pending_requests.lock() {
                    pending.clear();
                }

                let (lock, cvar) = &*reader.notification_cond;
                if let Ok(mut flag) = lock.lock() {
                    *flag = true;
                }
                cvar.notify_all();
            }
        });
        Ok(client)
    }

    fn fail_if_background_reader_stopped(&self) -> Result<(), Error> {
        let bg = self.background_error.lock().map_err(|_| Error::Poisoned)?;
        if let Some(message) = bg.as_ref() {
            return Err(Error::BackgroundReadLoop(message.clone()));
        }
        drop(bg);
        Ok(())
    }

    fn background_read_loop(&self) -> Result<(), Error> {
        loop {
            let msg = self.receive_message()?;

            let id = msg.get("id");
            let method = msg.get("method").and_then(Value::as_str);

            // If it has a method and id is null/missing, it's a notification
            if (id.is_none() || id == Some(&Value::Null)) && method.is_some() {
                if let Some(method) = method {
                    self.handle_notification(method, &msg)?;
                }
                continue;
            }

            // If it has method "echo", handle it (Request from server)
            if method == Some("echo") {
                let _ = self.handle_echo(&msg);
                continue;
            }

            // Otherwise it must be a response to one of our requests
            let Some(id_u64) = id.and_then(Value::as_u64) else {
                continue;
            };

            if self
                .cancelled_requests
                .lock()
                .map_err(|_| Error::Poisoned)?
                .remove(&id_u64)
            {
                self.pending_requests
                    .lock()
                    .map_err(|_| Error::Poisoned)?
                    .remove(&id_u64);
                continue;
            }

            let pending = self
                .pending_requests
                .lock()
                .map_err(|_| Error::Poisoned)?
                .remove(&id_u64);
            if let Some(tx) = pending {
                let _ = tx.send(msg);
            }
        }
    }

    fn handle_notification(&self, method: &str, msg: &Value) -> Result<(), Error> {
        if let Some(n) = Self::parse_notification(method, msg) {
            self.notifications
                .lock()
                .map_err(|_| Error::Poisoned)?
                .push_back(n);
            let (lock, cvar) = &*self.notification_cond;
            let mut pending = lock.lock().map_err(|_| Error::Poisoned)?;
            *pending = true;
            drop(pending);
            cvar.notify_all();
        }
        Ok(())
    }

    fn parse_notification(method: &str, msg: &Value) -> Option<Notification> {
        let params = msg.get("params")?.as_array()?.clone();
        Self::parse_notification_params(method, &params)
    }

    #[cfg(test)]
    fn validate_notification_payload(method: &str, params: &[Value]) -> bool {
        Self::parse_notification_params(method, params).is_some()
    }

    fn parse_notification_params(method: &str, params: &[Value]) -> Option<Notification> {
        match method {
            "update" => {
                if params.len() != 2 {
                    return None;
                }
                let obj = params.get(1).and_then(Value::as_object)?;
                rpc::Rpc::validate_table_updates(obj).ok()?;
                let monitor_id = params.first()?.as_str()?.to_string();
                let updates = serde_json::from_value(Value::Object(obj.clone())).ok()?;
                Some(Notification::Update {
                    method: method.to_string(),
                    monitor_id,
                    updates,
                })
            }
            m if m.starts_with("update") => {
                if params.len() != 2 || !params.get(1).is_some_and(Value::is_object) {
                    return None;
                }
                let monitor_id = params.first()?.as_str()?.to_string();
                let updates = serde_json::from_value(params.get(1)?.clone()).ok()?;
                Some(Notification::Update {
                    method: method.to_string(),
                    monitor_id,
                    updates,
                })
            }
            "locked" | "stolen" => {
                if params.len() != 1 || !params.first().is_some_and(Value::is_string) {
                    return None;
                }
                Some(match method {
                    "locked" => Notification::Locked(params.first()?.as_str()?.to_string()),
                    "stolen" => Notification::Stolen(params.first()?.as_str()?.to_string()),
                    _ => unreachable!(),
                })
            }
            _ => None,
        }
    }

    fn parse_table_updates(value: Value) -> Result<TableUpdates, Error> {
        let obj = value.as_object().ok_or(Error::UnexpectedResponse)?;
        rpc::Rpc::validate_table_updates(obj)?;
        serde_json::from_value(value).map_err(Error::Json)
    }

    fn parse_echo(value: &Value) -> Result<String, Error> {
        let arr = value.as_array().ok_or(Error::UnexpectedResponse)?;
        if arr.len() != 1 {
            return Err(Error::Validation(
                "echo result MUST contain exactly one string".into(),
            ));
        }
        let Some(s) = arr.first().and_then(Value::as_str) else {
            return Err(Error::Validation(
                "echo result MUST contain exactly one string".into(),
            ));
        };
        Ok(s.to_string())
    }

    fn parse_lock_state(value: &Value, method: &str) -> Result<bool, Error> {
        let obj = value.as_object().ok_or(Error::UnexpectedResponse)?;
        let locked = obj.get("locked").and_then(Value::as_bool).ok_or_else(|| {
            Error::Validation(format!("{method} result MUST contain boolean locked"))
        })?;
        if obj.len() != 1 {
            return Err(Error::Validation(format!(
                "{method} result MUST contain only locked"
            )));
        }
        Ok(locked)
    }

    fn parse_empty_result(value: &Value, method: &str) -> Result<(), Error> {
        let obj = value.as_object().ok_or(Error::UnexpectedResponse)?;
        if !obj.is_empty() {
            return Err(Error::Validation(format!(
                "{method} result MUST be an empty object"
            )));
        }
        Ok(())
    }

    fn parse_transact_response(value: &Value) -> Result<TransactionResponse, Error> {
        let arr = value.as_array().ok_or(Error::UnexpectedResponse)?;
        let mut entries = Vec::with_capacity(arr.len());
        for item in arr {
            entries.push(Self::parse_transact_entry(item)?);
        }
        Ok(TransactionResponse { entries })
    }

    fn parse_transact_entry(value: &Value) -> Result<TransactionOutcome, Error> {
        if value.is_null() {
            return Ok(TransactionOutcome::Null);
        }
        if let Some(obj) = value.as_object() {
            if let Some(error) = obj.get("error") {
                if !error.is_null() {
                    return Ok(TransactionOutcome::Error(Validator::parse_rpc_error(
                        error.clone(),
                    )?));
                }
            }
            if let Some(uuid) = obj.get("uuid") {
                return Ok(TransactionOutcome::Insert {
                    uuid: Self::parse_insert_uuid(uuid)?,
                });
            }
            if let Some(rows) = obj.get("rows") {
                let rows = Self::parse_select_rows(rows)?;
                return Ok(TransactionOutcome::Select { rows });
            }
            if let Some(count) = obj.get("count").and_then(Value::as_u64) {
                return Ok(TransactionOutcome::Count { count });
            }
            if obj.is_empty() {
                return Ok(TransactionOutcome::Empty);
            }
        }
        Err(Error::Validation(
            "transact result entries MUST be objects or null".into(),
        ))
    }

    fn parse_insert_uuid(uuid: &Value) -> Result<String, Error> {
        let uuid_arr = uuid
            .as_array()
            .ok_or_else(|| Error::Validation("insert result uuid MUST be a uuid value".into()))?;
        if uuid_arr.first() != Some(&Value::String("uuid".to_string()))
            || !uuid_arr.get(1).is_some_and(Value::is_string)
        {
            return Err(Error::Validation(
                "insert result uuid MUST be a uuid value".into(),
            ));
        }
        Ok(uuid_arr
            .get(1)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string())
    }

    fn parse_select_rows(rows: &Value) -> Result<Vec<Map<String, Value>>, Error> {
        let rows = rows
            .as_array()
            .ok_or_else(|| Error::Validation("select result MUST contain rows array".into()))?;
        rows.iter().map(Self::parse_select_row).collect()
    }

    fn parse_select_row(row: &Value) -> Result<Map<String, Value>, Error> {
        row.as_object()
            .cloned()
            .ok_or_else(|| Error::Validation("select rows MUST be objects".into()))
    }

    fn receive_message(&self) -> Result<Value, Error> {
        loop {
            let maybe_val = {
                let mut buf = self.read_buf.lock().map_err(|_| Error::Poisoned)?;
                Self::try_parse_buffer(&mut buf)?
            };
            if let Some(val) = maybe_val {
                return Ok(val);
            }
            let mut temp = [0u8; 4096];
            let n = {
                let mut io = self.io.lock().map_err(|_| Error::Poisoned)?;
                io.read(&mut temp)?
            };
            if n == 0 {
                return Err(Error::ConnectionClosed);
            }
            if let Some(bytes) = temp.get(..n) {
                self.read_buf
                    .lock()
                    .map_err(|_| Error::Poisoned)?
                    .extend_from_slice(bytes);
            }
        }
    }

    fn try_parse_buffer(buf: &mut Vec<u8>) -> Result<Option<Value>, Error> {
        if buf.is_empty() {
            return Ok(None);
        }
        let mut iter = serde_json::Deserializer::from_slice(buf).into_iter::<Value>();
        match iter.next() {
            Some(Ok(val)) => {
                let consumed = iter.byte_offset();
                let remainder = buf.split_off(consumed);
                *buf = remainder;
                Ok(Some(val))
            }
            Some(Err(e)) => {
                if !e.is_eof() {
                    return Err(Error::Json(e));
                }
                Ok(None)
            }
            None => Ok(None),
        }
    }

    fn handle_echo(&self, req: &Value) -> Result<(), Error> {
        let resp = json!({"id": req["id"], "result": req.get("params").cloned().unwrap_or(json!([])), "error": null});
        self.write_json_line(&resp)?;
        Ok(())
    }

    fn write_json_line(&self, value: &Value) -> Result<(), Error> {
        let bytes = serde_json::to_string(value)?;
        {
            let mut io = self.io.lock().map_err(|_| Error::Poisoned)?;
            io.write_all(bytes.as_bytes())?;
            io.write_all(b"\n")?;
            io.flush()?;
        }
        Ok(())
    }

    fn request_with_id(&self, method: &str, params: &Value, id: u64) -> Result<Value, Error> {
        self.fail_if_background_reader_stopped()?;
        rpc::Rpc::validate_method_params(method, params)?;
        let (tx, rx) = bounded(1);
        self.pending_requests
            .lock()
            .map_err(|_| Error::Poisoned)?
            .insert(id, tx);

        let req = rpc::Rpc::encode(method, id, params.clone());
        if let Err(err) = self.write_json_line(&req) {
            let _ = self
                .pending_requests
                .lock()
                .map_err(|_| Error::Poisoned)?
                .remove(&id);
            return Err(err);
        }

        let resp = match rx.recv_timeout(Duration::from_secs(5)) {
            Ok(resp) => resp,
            Err(RecvTimeoutError::Timeout) => {
                let _ = self
                    .pending_requests
                    .lock()
                    .map_err(|_| Error::Poisoned)?
                    .remove(&id);
                return Err(Error::Timeout);
            }
            Err(RecvTimeoutError::Disconnected) => {
                let _ = self
                    .pending_requests
                    .lock()
                    .map_err(|_| Error::Poisoned)?
                    .remove(&id);
                return Err(Error::ConnectionClosed);
            }
        };

        rpc::Rpc::decode(method, params, &resp)
    }

    /// Send a raw RPC request and return the decoded `result` payload.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use ovsdb::client::Connection;
    /// use serde_json::json;
    ///
    /// let client = Connection::connect("tcp:127.0.0.1:6640", None)?;
    /// let result = client.request("list_dbs", &json!([]))?;
    /// println!("{result}");
    /// # Ok::<(), ovsdb::client::error::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, or response-shape errors.
    pub fn request(&self, method: &str, params: &Value) -> Result<Value, Error> {
        let id = self.id_counter.fetch_add(1, Ordering::Relaxed);
        self.request_with_id(method, params, id)
    }

    /// Send a raw RPC request with a caller-chosen id.
    ///
    /// This is intended for integration tests that need to coordinate a
    /// request with an external `cancel` notification.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use ovsdb::client::Connection;
    /// use serde_json::json;
    ///
    /// let client = Connection::connect("tcp:127.0.0.1:6640", None)?;
    /// let result = client.request_with_id_for_test("echo", &json!(["hello"]), 100)?;
    /// println!("{result}");
    /// # Ok::<(), ovsdb::client::error::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, or response-shape errors.
    pub fn request_with_id_for_test(
        &self,
        method: &str,
        params: &Value,
        id: u64,
    ) -> Result<Value, Error> {
        self.request_with_id(method, params, id)
    }

    /// List the database names available on the server.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use ovsdb::client::Connection;
    ///
    /// let client = Connection::connect("tcp:127.0.0.1:6640", None)?;
    /// let dbs = client.list_dbs()?;
    /// assert!(!dbs.is_empty());
    /// # Ok::<(), ovsdb::client::error::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response is malformed.
    pub fn list_dbs(&self) -> Result<Vec<String>, Error> {
        let res = self.request("list_dbs", &json!([]))?;
        res.as_array()
            .ok_or(Error::UnexpectedResponse)?
            .iter()
            .map(|v| {
                v.as_str()
                    .ok_or(Error::UnexpectedResponse)
                    .map(std::string::ToString::to_string)
            })
            .collect::<Result<Vec<_>, _>>()
    }

    /// Fetch and validate a database schema, using an in-memory cache.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use ovsdb::client::Connection;
    ///
    /// let client = Connection::connect("tcp:127.0.0.1:6640", None)?;
    /// let schema = client.get_schema("OVN_Northbound")?;
    /// println!("version={}", schema.version);
    /// # Ok::<(), ovsdb::client::error::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails, the schema cannot be parsed,
    /// the schema fails validation, or the local cache lock is poisoned.
    pub fn get_schema(&self, database: &str) -> Result<DatabaseSchema, Error> {
        let cached = {
            let cache = self.schema_cache.lock().map_err(|_| Error::Poisoned)?;
            cache.get(database).cloned()
        };
        if let Some(schema) = cached {
            return Ok(schema);
        }

        self.refresh_schema(database)
    }

    /// Fetch and validate a database schema from the server and update cache.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use ovsdb::client::Connection;
    ///
    /// let client = Connection::connect("tcp:127.0.0.1:6640", None)?;
    /// let schema = client.refresh_schema("OVN_Northbound")?;
    /// println!("version={}", schema.version);
    /// # Ok::<(), ovsdb::client::error::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails, the schema cannot be parsed,
    /// the schema fails validation, or the local cache lock is poisoned.
    pub fn refresh_schema(&self, database: &str) -> Result<DatabaseSchema, Error> {
        let res = self.request("get_schema", &json!([database]))?;
        let schema: DatabaseSchema = serde_json::from_value(res)?;
        schema.validate().map_err(Error::Validation)?;
        self.schema_cache
            .lock()
            .map_err(|_| Error::Poisoned)?
            .insert(database.to_string(), schema.clone());
        Ok(schema)
    }

    /// Clear all cached schemas kept by this client.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use ovsdb::client::Connection;
    ///
    /// let client = Connection::connect("tcp:127.0.0.1:6640", None)?;
    /// client.clear_schema_cache()?;
    /// # Ok::<(), ovsdb::client::error::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the local cache lock is poisoned.
    pub fn clear_schema_cache(&self) -> Result<(), Error> {
        self.schema_cache
            .lock()
            .map_err(|_| Error::Poisoned)?
            .clear();
        Ok(())
    }

    /// Echo a string value through the server.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use ovsdb::client::Connection;
    ///
    /// let client = Connection::connect("tcp:127.0.0.1:6640", None)?;
    /// let echoed = client.echo("hello")?;
    /// assert_eq!(echoed, "hello");
    /// # Ok::<(), ovsdb::client::error::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response is malformed.
    pub fn echo(&self, value: &str) -> Result<String, Error> {
        let res = self.request("echo", &json!([value]))?;
        Self::parse_echo(&res)
    }

    /// Run a transaction and decode the typed outcomes.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use ovsdb::client::{ops::Ops, Connection};
    /// use serde_json::json;
    ///
    /// let client = Connection::connect("tcp:127.0.0.1:6640", None)?;
    /// let tx = vec![
    ///     Ops::comment("docs example"),
    ///     Ops::select("Logical_Switch", &[], Some(&["name".to_string()])),
    /// ];
    /// let reply = client.transact("OVN_Northbound", tx)?;
    /// println!("entries={}", reply.len());
    /// # Ok::<(), ovsdb::client::error::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the transaction response is
    /// malformed.
    pub fn transact(&self, db: &str, ops: Vec<Value>) -> Result<TransactionResponse, Error> {
        let mut p = vec![json!(db)];
        p.extend(ops);
        let res = self.request("transact", &Value::Array(p))?;
        Self::parse_transact_response(&res)
    }

    /// Allocate the next client request identifier.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use ovsdb::client::Connection;
    ///
    /// let client = Connection::connect("tcp:127.0.0.1:6640", None)?;
    /// let id = client.next_id();
    /// assert!(id > 0);
    /// # Ok::<(), ovsdb::client::error::Error>(())
    /// ```
    pub fn next_id(&self) -> u64 {
        self.id_counter.fetch_add(1, Ordering::Relaxed)
    }

    /// Start a monitor and return the initial table updates.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use ovsdb::client::Connection;
    /// use serde_json::json;
    /// use std::collections::HashMap;
    ///
    /// let client = Connection::connect("tcp:127.0.0.1:6640", None)?;
    /// let monitor_id = json!("docs-monitor");
    /// let mut requests = HashMap::new();
    /// requests.insert(
    ///     "Logical_Switch".to_string(),
    ///     json!([{"columns":["bridges"],"select":{"initial":true,"insert":true,"delete":true,"modify":true}}]),
    /// );
    /// let initial = client.monitor("OVN_Northbound", &monitor_id, &requests)?;
    /// println!("{:?}", initial.get("Logical_Switch"));
    /// # Ok::<(), ovsdb::client::error::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response is malformed.
    pub fn monitor(
        &self,
        database: &str,
        monitor_id: &Value,
        monitor_requests: &HashMap<String, Value>,
    ) -> Result<TableUpdates, Error> {
        let res = self.request("monitor", &json!([database, monitor_id, monitor_requests]))?;
        Self::parse_table_updates(res)
    }

    /// Start a conditional monitor and return the initial table updates.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use ovsdb::client::Connection;
    /// use serde_json::json;
    /// use std::collections::HashMap;
    ///
    /// let client = Connection::connect("tcp:127.0.0.1:6640", None)?;
    /// let monitor_id = json!("docs-monitor-cond");
    /// let mut requests = HashMap::new();
    /// requests.insert(
    ///     "Logical_Switch".to_string(),
    ///     json!([{"columns":["name"],"where":[["name","==","br-int"]]}]),
    /// );
    /// let _initial = client.monitor_cond("OVN_Northbound", &monitor_id, &requests)?;
    /// # Ok::<(), ovsdb::client::error::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response is malformed.
    pub fn monitor_cond(
        &self,
        database: &str,
        monitor_id: &Value,
        monitor_requests: &HashMap<String, Value>,
    ) -> Result<TableUpdates, Error> {
        let res = self.request(
            "monitor_cond",
            &json!([database, monitor_id, monitor_requests]),
        )?;
        Self::parse_table_updates(res)
    }

    /// Cancel an active monitor.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use ovsdb::client::Connection;
    /// use serde_json::json;
    /// use std::collections::HashMap;
    ///
    /// let client = Connection::connect("tcp:127.0.0.1:6640", None)?;
    /// let monitor_id = json!("docs-cancel-monitor");
    /// let requests = HashMap::from([("Logical_Switch".to_string(), json!([{}]))]);
    /// let _ = client.monitor("OVN_Northbound", &monitor_id, &requests)?;
    /// client.monitor_cancel(&monitor_id)?;
    /// # Ok::<(), ovsdb::client::error::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response is malformed.
    pub fn monitor_cancel(&self, monitor_id: &Value) -> Result<(), Error> {
        let res = self.request("monitor_cancel", &json!([monitor_id]))?;
        Self::parse_empty_result(&res, "monitor_cancel")
    }

    /// Cancel a pending request by id.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use ovsdb::client::Connection;
    ///
    /// let client = Connection::connect("tcp:127.0.0.1:6640", None)?;
    /// client.cancel(42)?;
    /// # Ok::<(), ovsdb::client::error::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if writing the cancel request fails.
    pub fn cancel(&self, id_to_cancel: u64) -> Result<(), Error> {
        let req = rpc::Rpc::encode("cancel", Value::Null, [id_to_cancel]);
        self.write_json_line(&req)?;
        self.cancelled_requests
            .lock()
            .map_err(|_| Error::Poisoned)?
            .insert(id_to_cancel);
        let pending = self
            .pending_requests
            .lock()
            .map_err(|_| Error::Poisoned)?
            .remove(&id_to_cancel);
        if let Some(tx) = pending {
            let _ = tx.send(json!({
                "id": id_to_cancel,
                "result": Value::Null,
                "error": {
                    "error": "canceled",
                    "details": "request canceled"
                }
            }));
        }
        Ok(())
    }

    /// Request a lock and return its granted state.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use ovsdb::client::Connection;
    ///
    /// let client = Connection::connect("tcp:127.0.0.1:6640", None)?;
    /// let acquired = client.lock("docs-lock")?;
    /// println!("acquired={acquired}");
    /// # Ok::<(), ovsdb::client::error::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, or response-shape errors.
    pub fn lock(&self, lock_id: &str) -> Result<bool, Error> {
        {
            let mut states = self.lock_states.lock().map_err(|_| Error::Poisoned)?;
            match states.entry(lock_id.to_string()) {
                Entry::Occupied(mut entry) => {
                    entry.insert(LockState::PendingUnlock);
                }
                Entry::Vacant(entry) => {
                    entry.insert(LockState::PendingUnlock);
                }
            }
        }
        let result = self.request("lock", &json!([lock_id]));
        if result.is_err() {
            self.lock_states
                .lock()
                .map_err(|_| Error::Poisoned)?
                .insert(lock_id.to_string(), LockState::Idle);
        }
        result.and_then(|value| Self::parse_lock_state(&value, "lock"))
    }

    /// Steal a lock and return its granted state.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use ovsdb::client::Connection;
    ///
    /// let client = Connection::connect("tcp:127.0.0.1:6640", None)?;
    /// let _ = client.steal("docs-lock")?;
    /// # Ok::<(), ovsdb::client::error::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, or response-shape errors.
    pub fn steal(&self, lock_id: &str) -> Result<bool, Error> {
        {
            let mut states = self.lock_states.lock().map_err(|_| Error::Poisoned)?;
            match states.entry(lock_id.to_string()) {
                Entry::Occupied(mut entry) => {
                    entry.insert(LockState::PendingUnlock);
                }
                Entry::Vacant(entry) => {
                    entry.insert(LockState::PendingUnlock);
                }
            }
        }
        let result = self.request("steal", &json!([lock_id]));
        if result.is_err() {
            self.lock_states
                .lock()
                .map_err(|_| Error::Poisoned)?
                .insert(lock_id.to_string(), LockState::Idle);
        }
        result.and_then(|value| Self::parse_lock_state(&value, "steal"))
    }

    /// Release a lock previously acquired with `lock` or `steal`.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use ovsdb::client::Connection;
    ///
    /// let client = Connection::connect("tcp:127.0.0.1:6640", None)?;
    /// let _ = client.lock("docs-lock")?;
    /// client.unlock("docs-lock")?;
    /// # Ok::<(), ovsdb::client::error::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns validation, transport, timeout, or response-shape errors.
    pub fn unlock(&self, lock_id: &str) -> Result<(), Error> {
        {
            let states = self.lock_states.lock().map_err(|_| Error::Poisoned)?;
            if !matches!(states.get(lock_id), Some(LockState::PendingUnlock)) {
                return Err(Error::Validation(format!(
                    "unlock {lock_id} requires a preceding lock or steal"
                )));
            }
        }
        let result = self.request("unlock", &json!([lock_id]));
        if result.is_ok() {
            self.lock_states
                .lock()
                .map_err(|_| Error::Poisoned)?
                .insert(lock_id.to_string(), LockState::Idle);
        }
        result.and_then(|value| Self::parse_empty_result(&value, "unlock"))
    }

    /// Wait for and return the next queued notification.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use ovsdb::client::Connection;
    ///
    /// let client = Connection::connect("tcp:127.0.0.1:6640", None)?;
    /// let notif = client.poll_notification()?;
    /// println!("method={}", notif.method());
    /// # Ok::<(), ovsdb::client::error::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if a mutex is poisoned.
    pub fn poll_notification(&self) -> Result<Notification, Error> {
        self.fail_if_background_reader_stopped()?;
        let (lock, cvar) = &*self.notification_cond;
        loop {
            {
                let mut notifications = self.notifications.lock().map_err(|_| Error::Poisoned)?;
                if let Some(n) = notifications.pop_front() {
                    return Ok(n);
                }
            }
            let mut pending = lock.lock().map_err(|_| Error::Poisoned)?;
            *pending = false;
            while !*pending {
                pending = cvar.wait(pending).map_err(|_| Error::Poisoned)?;
                self.fail_if_background_reader_stopped()?;
            }
            drop(pending);
        }
    }

    /// Wait for the next notification from the server, with a timeout.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use ovsdb::client::Connection;
    /// use std::time::Duration;
    ///
    /// let client = Connection::connect("tcp:127.0.0.1:6640", None)?;
    /// let maybe = client.poll_notification_timeout(Duration::from_secs(1))?;
    /// println!("got={}", maybe.is_some());
    /// # Ok::<(), ovsdb::client::error::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if a mutex is poisoned.
    pub fn poll_notification_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Option<Notification>, Error> {
        self.fail_if_background_reader_stopped()?;
        let (lock, cvar) = &*self.notification_cond;
        let start = std::time::Instant::now();
        loop {
            {
                let mut notifications = self.notifications.lock().map_err(|_| Error::Poisoned)?;
                if let Some(n) = notifications.pop_front() {
                    return Ok(Some(n));
                }
            }
            let elapsed = start.elapsed();
            if elapsed >= timeout {
                return Ok(None);
            }
            let mut pending = lock.lock().map_err(|_| Error::Poisoned)?;
            *pending = false;
            let (new_pending, result) = cvar
                .wait_timeout(pending, timeout.checked_sub(elapsed).unwrap_or_default())
                .map_err(|_| Error::Poisoned)?;
            pending = new_pending;
            self.fail_if_background_reader_stopped()?;
            if result.timed_out() {
                let res = self
                    .notifications
                    .lock()
                    .map_err(|_| Error::Poisoned)?
                    .pop_front();
                drop(pending);
                return Ok(res);
            }
            drop(pending);
        }
    }
}

#[cfg(test)]
mod client_tests {
    use super::*;
    use crate::model::{
        AtomicType, BaseType, ColumnSchema, DatabaseSchema, MaxSize, TableSchema, Type,
    };
    use crossbeam_channel::Receiver;
    use ops::{Datum, Transaction};
    use rpc::Rpc;
    use serde_json::json;

    #[test]
    fn test_validate_datum_constraints() {
        let typ = Type::Complex {
            key: BaseType::Configured {
                r#type: AtomicType::Integer,
                r#enum: None,
                min_integer: Some(0),
                max_integer: Some(10),
                min_real: None,
                max_real: None,
                min_length: None,
                max_length: None,
                ref_table: None,
                ref_type: None,
            },
            value: None,
            min: 1,
            max: MaxSize::Integer(2),
        };

        // Scalar fallback
        assert!(Datum::new(&typ, &json!(5)).validate().is_ok());
        assert!(Datum::new(&typ, &json!(11)).validate().is_err());

        // Set encoding
        assert!(Datum::new(&typ, &json!(["set", [1, 2]])).validate().is_ok());
        assert!(Datum::new(&typ, &json!(["set", [1, 2, 3]]))
            .validate()
            .is_err()); // max 2
        assert!(Datum::new(&typ, &json!(["set", []])).validate().is_err()); // min 1
    }

    #[test]
    fn test_validate_datum_rejects_invalid_map_encodings() {
        let typ = Type::Complex {
            key: BaseType::Atomic(AtomicType::String),
            value: Some(BaseType::Atomic(AtomicType::String)),
            min: 0,
            max: MaxSize::Unlimited("unlimited".to_string()),
        };

        assert!(Datum::new(&typ, &json!(["map", [["a", "b"]]]))
            .validate()
            .is_ok());
        assert!(Datum::new(&typ, &json!(["set", ["a"]])).validate().is_err());
        assert!(Datum::new(&typ, &json!("a")).validate().is_err());
    }

    #[test]
    fn test_validate_transaction_immutability() {
        let mut tables = HashMap::new();
        let mut columns = HashMap::new();
        columns.insert(
            "name".to_string(),
            ColumnSchema {
                r#type: Type::Atomic(BaseType::Atomic(AtomicType::String)),
                ephemeral: None,
                mutable: Some(false),
            },
        );
        tables.insert(
            "MyTable".to_string(),
            TableSchema {
                columns,
                max_rows: None,
                is_root: None,
                indexes: None,
            },
        );
        let schema = DatabaseSchema {
            name: "test".to_string(),
            version: "1.0.0".to_string(),
            cksum: None,
            tables,
        };

        let insert_op = json!({
            "op": "insert",
            "table": "MyTable",
            "row": {"name": "foo"},
            "uuid-name": "row1"
        });
        assert!(Transaction::new(&schema, &[insert_op]).validate().is_ok());

        let update_op = json!({
            "op": "update",
            "table": "MyTable",
            "where": [],
            "row": {"name": "bar"}
        });
        // Update on immutable column should fail
        assert!(Transaction::new(&schema, &[update_op]).validate().is_err());
    }

    #[test]
    fn test_validate_transaction_accepts_tagged_map_mutations() {
        let mut tables = HashMap::new();
        let mut columns = HashMap::new();
        columns.insert(
            "labels".to_string(),
            ColumnSchema {
                r#type: Type::Complex {
                    key: BaseType::Atomic(AtomicType::String),
                    value: Some(BaseType::Atomic(AtomicType::String)),
                    min: 0,
                    max: MaxSize::Unlimited("unlimited".to_string()),
                },
                ephemeral: None,
                mutable: Some(true),
            },
        );
        tables.insert(
            "MyTable".to_string(),
            TableSchema {
                columns,
                max_rows: None,
                is_root: None,
                indexes: None,
            },
        );
        let schema = DatabaseSchema {
            name: "test".to_string(),
            version: "1.0.0".to_string(),
            cksum: None,
            tables,
        };

        let insert_map = json!({
            "op": "mutate",
            "table": "MyTable",
            "where": [],
            "mutations": [
                ["labels", "insert", ["map", [["k1", "v1"]]]]
            ]
        });
        assert!(Transaction::new(&schema, &[insert_map]).validate().is_ok());

        let delete_map = json!({
            "op": "mutate",
            "table": "MyTable",
            "where": [],
            "mutations": [
                ["labels", "delete", ["map", [["k1", "v1"]]]]
            ]
        });
        assert!(Transaction::new(&schema, &[delete_map]).validate().is_ok());
    }

    #[test]
    fn test_validate_method_params_accepts_zero_op_transact_and_rejects_bad_monitor_arity() {
        assert!(matches!(
            Rpc::validate_method_params("transact", &json!(["Open_vSwitch"])),
            Ok(())
        ));
        assert!(matches!(
            Rpc::validate_method_params("transact", &json!(["Open_vSwitch", {"op":"insert"}])),
            Err(Error::Validation(_))
        ));
        assert!(matches!(
            Rpc::validate_method_params("monitor", &json!(["Open_vSwitch", 1])),
            Err(Error::Validation(_))
        ));
        assert!(matches!(
            Rpc::validate_method_params("monitor_cond", &json!(["Open_vSwitch", 1, {}, "extra"])),
            Err(Error::Validation(_))
        ));
        assert!(matches!(
            Rpc::validate_method_params("monitor", &json!(["Open_vSwitch", 1, {}])),
            Ok(())
        ));
    }

    #[test]
    fn test_validate_transact_op_accepts_shapes() {
        assert!(Rpc::validate_transact_op(
            &json!({"op":"insert","table":"T","row":{},"uuid-name":"row1"})
        )
        .is_ok());
        assert!(Rpc::validate_transact_op(&json!({"op":"insert","table":"T","row":{}})).is_ok());
        assert!(
            Rpc::validate_transact_op(&json!({"op":"update","table":"T","where":[],"row":{}}))
                .is_ok()
        );
        assert!(Rpc::validate_transact_op(
            &json!({"op":"mutate","table":"T","where":[],"mutations":[] })
        )
        .is_ok());
        assert!(Rpc::validate_transact_op(
            &json!({"op":"select","table":"T","where":[],"columns":["a","b"]})
        )
        .is_ok());
        assert!(Rpc::validate_transact_op(&json!({"op":"wait","table":"T","where":[],"columns":["a"],"until":"==","rows":[],"timeout":0})).is_ok());
        assert!(Rpc::validate_transact_op(&json!({"op":"abort"})).is_ok());
        assert!(Rpc::validate_transact_op(&json!({"op":"commit","durable":true})).is_ok());
    }

    #[test]
    fn test_validate_transact_op_rejects_bad_shapes() {
        assert!(Rpc::validate_transact_op(&json!({"op":"insert"})).is_err());
        assert!(Rpc::validate_transact_op(
            &json!({"op":"select","table":"T","where":[],"columns":["a",1]})
        )
        .is_err());
        assert!(Rpc::validate_transact_op(&json!({"op":"wait","table":"T","where":[],"columns":["a"],"until":"maybe","rows":[],"timeout":0})).is_err());
        assert!(Rpc::validate_transact_op(&json!({"op":"commit"})).is_err());
        assert!(Rpc::validate_transact_op(&json!({"op":"wait","table":"T","where":[],"columns":["a"],"until":"==","rows":[],"timeout":"nope"})).is_err());
        assert!(
            Rpc::validate_transact_op(&json!({"op":"insert","table":"T","row":{},"extra":1}))
                .is_err()
        );
        assert!(
            Rpc::validate_transact_op(&json!({"op":"commit","durable":true,"extra":1})).is_err()
        );
        assert!(Rpc::validate_transact_op(&json!({"op":"unknown"})).is_err());
    }

    #[test]
    fn test_validate_transact_response_rejects_extra_error_without_commit() {
        let params = json!(["db", {"op":"insert","table":"T","row":{}}]);
        let resp = json!({
            "id": 1,
            "error": null,
            "result": [
                {"uuid": ["uuid", "01234567-89ab-cdef-0123-456789abcdef"]},
                {"error": "boom"}
            ]
        });

        assert!(matches!(
            Rpc::decode("transact", &params, &resp),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn test_decode_requires_object_shape() {
        assert!(matches!(
            Rpc::decode("echo", &json!([]), json!(null)),
            Err(Error::UnexpectedResponse)
        ));
        assert!(matches!(
            Rpc::decode("echo", &json!([]), json!({"id": 1, "error": null})),
            Err(Error::MissingField("result"))
        ));
        assert!(matches!(
            Rpc::decode("echo", &json!([]), json!({"result": null, "error": null})),
            Err(Error::MissingField("id"))
        ));
        assert!(matches!(
            Rpc::decode(
                "list_dbs",
                &json!([]),
                json!({"id": 1, "result": null, "error": {"error": "x"}})
            ),
            Err(Error::RpcError(_))
        ));
        assert!(matches!(
            Rpc::decode(
                "echo",
                &json!([]),
                json!({"id": 1, "result": [1, 2], "error": null})
            ),
            Ok(v) if v == json!([1, 2])
        ));
    }

    #[test]
    fn test_schema_validation_rejects_bad_shapes() {
        let bad_schema = DatabaseSchema {
            name: "not a valid id".into(),
            version: "1.0".into(),
            cksum: None,
            tables: HashMap::new(),
        };
        assert!(bad_schema.validate().is_err());

        let mut tables = HashMap::new();
        tables.insert(
            "MyTable".to_string(),
            TableSchema {
                columns: HashMap::new(),
                max_rows: Some(0),
                is_root: None,
                indexes: Some(vec![vec![]]),
            },
        );
        let schema = DatabaseSchema {
            name: "good_name".into(),
            version: "1.0.0".into(),
            cksum: None,
            tables,
        };
        assert!(schema.validate().is_err());
    }

    #[test]
    fn test_schema_validation_rejects_enum_with_constraints() {
        let enum_type = BaseType::Configured {
            r#type: AtomicType::Integer,
            r#enum: Some(json!(["set", [1, 2]])),
            min_integer: Some(0),
            max_integer: None,
            min_real: None,
            max_real: None,
            min_length: None,
            max_length: None,
            ref_table: None,
            ref_type: None,
        };
        assert!(enum_type.validate_shape().is_err());
    }

    #[test]
    fn test_validate_method_params_rejects_overlapping_monitor_columns() {
        let params = json!([
            "Open_vSwitch",
            "monitor-id",
            {
                "Open_vSwitch": [
                    {"columns": ["external_ids"], "select": {"initial": true}},
                    {"columns": ["external_ids"], "select": {"initial": true}}
                ]
            }
        ]);
        assert!(Rpc::validate_method_params("monitor", &params).is_err());
    }

    #[test]
    fn test_validate_method_params_rejects_bad_monitor_request_shapes() {
        assert!(matches!(
            Rpc::validate_method_params(
                "monitor",
                &json!(["Open_vSwitch", "monitor-id", {"Open_vSwitch": {}}])
            ),
            Err(Error::Validation(_))
        ));
        assert!(matches!(
            Rpc::validate_method_params(
                "monitor",
                &json!([
                    "Open_vSwitch",
                    "monitor-id",
                    {"Open_vSwitch": [{"columns": ["external_ids"], "unexpected": true}]}
                ])
            ),
            Err(Error::Validation(_))
        ));
        assert!(matches!(
            Rpc::validate_method_params(
                "monitor",
                &json!([
                    "Open_vSwitch",
                    "monitor-id",
                    {"Open_vSwitch": [{"columns": ["external_ids"], "select": {"initial": "yes"}}]}
                ])
            ),
            Err(Error::Validation(_))
        ));
        assert!(matches!(
            Rpc::validate_method_params(
                "monitor",
                &json!([
                    "Open_vSwitch",
                    "monitor-id",
                    {"Open_vSwitch": [{"columns": ["external_ids", "external_ids"], "select": {"initial": true}}]}
                ])
            ),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn test_validate_transact_op_rejects_edge_shapes() {
        assert!(matches!(
            Rpc::validate_transact_op(&json!({"op":"insert","table":"T","row":{},"uuid-name":1})),
            Err(Error::Validation(_))
        ));
        assert!(matches!(
            Rpc::validate_transact_op(
                &json!({"op":"select","table":"T","where":[],"columns":["a","a"]})
            ),
            Err(Error::Validation(_))
        ));
        assert!(matches!(
            Rpc::validate_transact_op(
                &json!({"op":"mutate","table":"T","where":[],"mutations":[["c","bad",1]]})
            ),
            Err(Error::Validation(_))
        ));
        assert!(matches!(
            Rpc::validate_transact_op(
                &json!({"op":"wait","table":"T","where":[],"columns":["a"],"until":"==","rows":[],"timeout":"nope"})
            ),
            Err(Error::Validation(_))
        ));
        assert!(matches!(
            Rpc::validate_transact_op(&json!({"op":"comment","comment":1})),
            Err(Error::Validation(_))
        ));
        assert!(matches!(
            Rpc::validate_transact_op(&json!({"op":"assert","lock":1})),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn test_decode_rejects_edge_shapes() {
        assert!(matches!(
            Rpc::decode(
                "list_dbs",
                &json!([]),
                json!({"id": 1, "result": ["ok", 1], "error": null})
            ),
            Err(Error::Validation(_))
        ));
        assert!(matches!(
            Rpc::decode(
                "get_schema",
                &json!(["Open_vSwitch"]),
                json!({"id": 1, "result": [], "error": null})
            ),
            Err(Error::UnexpectedResponse)
        ));
        assert!(matches!(
            Rpc::decode(
                "monitor_cancel",
                &json!(["monitor-id"]),
                json!({"id": 1, "result": {"x": 1}, "error": null})
            ),
            Err(Error::Validation(_))
        ));
        assert!(matches!(
            Rpc::decode(
                "unlock",
                &json!(["lock-id"]),
                json!({"id": 1, "result": {"x": 1}, "error": null})
            ),
            Err(Error::Validation(_))
        ));
        assert!(matches!(
            Rpc::decode(
                "transact",
                &json!(["Open_vSwitch", {"op": "insert", "table": "T", "row": {}}]),
                json!({"id": 1, "result": [{"uuid": ["uuid", "550e8400-e29b-41d4-a716-446655440000"]}, null, null], "error": null})
            ),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn test_decode_accepts_trailing_commit_error() {
        let params = json!([
            "Open_vSwitch",
            {"op": "insert", "table": "T", "row": {}},
            {"op": "commit", "durable": true}
        ]);
        let resp = json!({
            "id": 1,
            "result": [
                {"uuid": ["uuid", "550e8400-e29b-41d4-a716-446655440000"]},
                {},
                {"error": {"error": "constraint violation", "details": null}}
            ],
            "error": null
        });
        assert!(Rpc::decode("transact", &params, resp).is_ok());
    }

    #[test]
    fn test_decode_accepts_string_error() {
        let err = Rpc::decode(
            "monitor_cancel",
            &json!(["monitor-id"]),
            json!({"id": 1, "error": "unknown monitor"}),
        );
        match err {
            Err(Error::RpcError(rpc_err)) => assert_eq!(rpc_err.error, "unknown monitor"),
            other => assert!(other.is_err(), "unexpected result: {other:?}"),
        }
    }

    #[test]
    fn test_decode_rejects_malformed_lock_and_monitor_results() {
        assert!(matches!(
            Rpc::decode(
                "lock",
                &json!(["lock-id"]),
                json!({"id": 1, "result": {"locked": "yes"}, "error": null})
            ),
            Err(Error::Validation(_))
        ));
        assert!(matches!(
            Rpc::decode(
                "steal",
                &json!(["lock-id"]),
                json!({"id": 1, "result": {"locked": false}, "error": null})
            ),
            Err(Error::Validation(_))
        ));
        assert!(matches!(
            Rpc::decode(
                "monitor",
                &json!(["Open_vSwitch", "monitor-id", {}]),
                json!({
                    "id": 1,
                    "result": {
                        "Open_vSwitch": {
                            "not-a-uuid": {
                                "old": {}
                            }
                        }
                    },
                    "error": null
                })
            ),
            Err(Error::Validation(_))
        ));
    }

    #[test]
    fn test_validate_notification_payload_rejects_malformed_update() {
        let params = vec![
            json!("monitor-id"),
            json!({
                "Open_vSwitch": {
                    "550e8400-e29b-41d4-a716-446655440000": {
                        "old": "not-an-object"
                    }
                }
            }),
        ];
        assert!(!Connection::validate_notification_payload(
            "update", &params
        ));
    }

    #[test]
    fn test_validate_notification_payload_accepts_valid_update() {
        let params = vec![
            json!("monitor-id"),
            json!({
                "Open_vSwitch": {
                    "550e8400-e29b-41d4-a716-446655440000": {
                        "old": {},
                        "new": {}
                    }
                }
            }),
        ];
        assert!(Connection::validate_notification_payload("update", &params));
    }

    #[test]
    fn test_validate_table_updates_rejects_bad_shapes() {
        let bad_table = json!({
            "not a table": {
                "550e8400-e29b-41d4-a716-446655440000": {
                    "old": {}
                }
            }
        });
        assert!(bad_table
            .as_object()
            .is_some_and(|obj| Rpc::validate_table_updates(obj).is_err()));

        let bad_uuid = json!({
            "Open_vSwitch": {
                "not-a-uuid": {
                    "old": {}
                }
            }
        });
        assert!(bad_uuid
            .as_object()
            .is_some_and(|obj| Rpc::validate_table_updates(obj).is_err()));

        let empty_row = json!({
            "Open_vSwitch": {
                "550e8400-e29b-41d4-a716-446655440000": {}
            }
        });
        assert!(empty_row
            .as_object()
            .is_some_and(|obj| Rpc::validate_table_updates(obj).is_err()));

        let unexpected_field = json!({
            "Open_vSwitch": {
                "550e8400-e29b-41d4-a716-446655440000": {
                    "old": {},
                    "extra": {}
                }
            }
        });
        assert!(unexpected_field
            .as_object()
            .is_some_and(|obj| Rpc::validate_table_updates(obj).is_err()));

        let non_object_old = json!({
            "Open_vSwitch": {
                "550e8400-e29b-41d4-a716-446655440000": {
                    "old": "bad"
                }
            }
        });
        assert!(non_object_old
            .as_object()
            .is_some_and(|obj| Rpc::validate_table_updates(obj).is_err()));

        let non_object_new = json!({
            "Open_vSwitch": {
                "550e8400-e29b-41d4-a716-446655440000": {
                    "new": "bad"
                }
            }
        });
        assert!(non_object_new
            .as_object()
            .is_some_and(|obj| Rpc::validate_table_updates(obj).is_err()));
    }

    struct MockIo {
        tx: Sender<Vec<u8>>,
        rx: Receiver<Vec<u8>>,
        read_buf: Vec<u8>,
    }
    impl Read for MockIo {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if self.read_buf.is_empty() {
                self.read_buf = self.rx.recv().map_err(|_| std::io::ErrorKind::BrokenPipe)?;
            }
            let len = std::cmp::min(buf.len(), self.read_buf.len());
            if let (Some(dest), Some(src)) = (buf.get_mut(..len), self.read_buf.get(..len)) {
                dest.copy_from_slice(src);
            }
            self.read_buf.drain(..len);
            Ok(len)
        }
    }
    impl Write for MockIo {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.tx
                .send(buf.to_vec())
                .map_err(|_| std::io::ErrorKind::BrokenPipe)?;
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn test_client() -> Connection {
        let (tx1, _rx1) = bounded(64);
        let (_tx2, rx2) = bounded(64);

        let stream: BoxedIo = Box::new(MockIo {
            tx: tx1,
            rx: rx2,
            read_buf: Vec::new(),
        });
        Connection {
            io: Arc::new(Mutex::new(stream)),
            read_buf: Arc::new(Mutex::new(Vec::new())),
            id_counter: Arc::new(AtomicU64::new(1)),
            notifications: Arc::new(Mutex::new(VecDeque::new())),
            pending_requests: Arc::new(Mutex::new(HashMap::new())),
            notification_cond: Arc::new((Mutex::new(false), Condvar::new())),
            cancelled_requests: Arc::new(Mutex::new(HashSet::new())),
            lock_states: Arc::new(Mutex::new(HashMap::new())),
            schema_cache: Arc::new(Mutex::new(HashMap::new())),
            background_error: Arc::new(Mutex::new(None)),
        }
    }

    #[test]
    fn test_parse_notification_accepts_valid_update() {
        let msg = json!({
            "params": [
                "monitor-id",
                {
                    "Open_vSwitch": {
                        "550e8400-e29b-41d4-a716-446655440000": {
                            "old": {},
                            "new": {}
                        }
                    }
                }
            ]
        });
        let parsed = Connection::parse_notification("update", &msg);
        assert!(matches!(
            parsed,
            Some(Notification::Update { method, monitor_id, updates })
                if method == "update"
                    && monitor_id == "monitor-id"
                    && updates.0.len() == 1
        ));
    }

    #[test]
    fn test_next_id_increments() {
        let client = test_client();
        assert_eq!(client.next_id(), 1);
        assert_eq!(client.next_id(), 2);
    }

    #[test]
    #[allow(clippy::unnecessary_wraps)]
    fn test_lock_and_unlock_state_gates_requests() -> Result<(), Box<dyn std::error::Error>> {
        let client = test_client();

        let unlock_err = client.unlock("missing");
        assert!(matches!(unlock_err, Err(Error::Validation(_))));
        Ok(())
    }
}
