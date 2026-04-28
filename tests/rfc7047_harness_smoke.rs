#![allow(
    missing_docs,
    dead_code,
    clippy::expect_used,
    clippy::panic,
    clippy::print_stderr,
    clippy::unused_async,
    clippy::indexing_slicing
)]
mod support;

use anyhow::Result;
use serde_json::json;
use support::{read_raw_json, send_raw_json, TestOvsDBClient};

#[test]
fn harness_starts_real_ovsdb_server() -> Result<()> {
    let tc = TestOvsDBClient::start_plain()?;

    assert!(!tc.host.is_empty());
    assert!(tc.port > 0);
    assert!(tc.addr.starts_with("tcp:"));

    Ok(())
}

#[test]
fn harness_client_can_list_dbs() -> Result<()> {
    let tc = TestOvsDBClient::start_plain()?;

    let dbs = tc.client.list_dbs()?;

    assert!(
        dbs.iter().any(|db| db == "Open_vSwitch"),
        "expected Open_vSwitch database, got {dbs:?}"
    );

    Ok(())
}

#[test]
fn harness_client_can_get_open_vswitch_schema() -> Result<()> {
    let tc = TestOvsDBClient::start_plain()?;

    let schema = tc.client.get_schema("Open_vSwitch")?;

    assert!(
        schema.tables.contains_key("Open_vSwitch"),
        "expected Open_vSwitch table in schema"
    );

    Ok(())
}

#[test]
fn harness_second_client_can_connect() -> Result<()> {
    let tc = TestOvsDBClient::start_plain()?;

    let client2 = tc.second_client()?;
    let dbs = client2.list_dbs()?;

    assert!(
        dbs.iter().any(|db| db == "Open_vSwitch"),
        "expected Open_vSwitch database from second client, got {dbs:?}"
    );

    Ok(())
}

#[test]
fn harness_raw_tcp_can_send_echo() -> Result<()> {
    let tc = TestOvsDBClient::start_plain()?;

    let mut stream = tc.raw_tcp_stream()?;

    send_raw_json(
        &mut stream,
        &json!({
            "method": "echo",
            "params": ["hello"],
            "id": 1
        }),
    )?;

    let response = read_raw_json(&mut stream)?;

    assert_eq!(response.get("result"), Some(&json!(["hello"])));
    assert_eq!(response.get("error"), Some(&json!(null)));
    assert_eq!(response.get("id"), Some(&json!(1)));

    Ok(())
}
