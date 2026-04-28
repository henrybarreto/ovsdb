# ovsdb

![rust edition](https://img.shields.io/badge/rust-2021-black)
![RFC 7047](https://img.shields.io/badge/RFC-7047-2b6cb0)
![status](https://img.shields.io/badge/status-experimental-orange)

`ovsdb` is a Rust library for shared OVSDB schema and validation types. The
`ovsdb::client` module provides the Rust client library and CLI for working
with Open vSwitch Database (`OVSDB`) servers over JSON-RPC, including schema
inspection, transactions, monitoring, and lock handling.

## RFC 7047

The goal of this project is to provide an OVSDB client that is compatible with
[RFC 7047](https://www.rfc-editor.org/rfc/rfc7047.html), the Open vSwitch
Database Management Protocol.

## Features

- Connect to OVSDB servers over `tcp:`, `unix:`, `tls:` and `ssl:` endpoints
- Request database schemas and list available databases
- Build and validate transactions client-side before sending them
- Subscribe to monitor notifications and poll incoming events
- Work with OVSDB locks via `lock`, `steal`, and `unlock`
- Use TLS with native system roots or custom CA/client certificates

## Installation

Build the crate with Cargo:

```bash
cargo build
```

Run the test suite:

```bash
cargo test
```

## Quick Start

```rust
use ovsdb::client::{ops::Ops, Connection};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Connection::connect("tcp:127.0.0.1:6640", None)?;

    let databases = client.list_dbs()?;
    println!("databases: {databases:?}");

    let schema = client.get_schema("Open_vSwitch")?;
    println!("schema version: {}", schema.version);

    let txn = vec![
        Ops::comment("example transaction"),
        Ops::abort(),
    ];
    let response = client.transact("Open_vSwitch", txn)?;
    println!("response: {response}");

    Ok(())
}
```

## CLI

The binary is named `ovsdb`:

```bash
cargo run -- --help
```

### Examples

List databases available on the server:

```bash
cargo run -- client list-dbs tcp:127.0.0.1:6640
```

Inspect the schema for `Open_vSwitch`:

```bash
cargo run -- client get-schema tcp:127.0.0.1:6640 Open_vSwitch
```

Send a simple transaction payload:

```bash
cargo run -- client transact tcp:127.0.0.1:6640 '["Open_vSwitch", {"op":"comment","comment":"hello"}]'
```
