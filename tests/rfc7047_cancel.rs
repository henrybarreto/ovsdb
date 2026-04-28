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
use ovsdb::client::error::Error as OvsdbError;
use ovsdb::client::ops::Ops as ops;
use serde_json::{json, Value};
use std::thread;
use std::time::Duration;

use support::{unique_name, RawJsonRpcStream, TestOvsDBClient};

const RFC7047_SCHEMA_PATH: &str = "tests/schemas/rfc7047_compliance.ovsschema";
const RFC7047_DB: &str = "RFC7047_Test";
const READ_TIMEOUT: Duration = Duration::from_millis(500);
const CANCEL_TIMEOUT: Duration = Duration::from_secs(5);

fn start_custom() -> Result<TestOvsDBClient> {
    TestOvsDBClient::start_with_schema(RFC7047_SCHEMA_PATH, RFC7047_DB)
}

fn scalar_row(name: &str) -> Value {
    json!({
        "name": name,
        "i": 1,
        "r": 1.0,
        "b": true,
        "s": "initial",
        "u": ["uuid", "550e8400-e29b-41d4-a716-446655440001"],
        "enum_s": "a",
        "limited_i": 1,
        "limited_r": 1.0,
        "limited_s": "ok"
    })
}

fn insert_scalar(tc: &TestOvsDBClient, name: &str) -> Result<()> {
    tc.client.transact(
        RFC7047_DB,
        vec![ops::insert(
            "ScalarTypes",
            scalar_row(name),
            Some("row_uuid"),
        )],
    )?;

    Ok(())
}

fn wait_transaction_with_id(
    client: &ovsdb::client::Connection,
    request_id: u64,
    row_name: &str,
) -> Result<Value, OvsdbError> {
    client.request_with_id_for_test(
        "transact",
        &json!([
            RFC7047_DB,
            ops::wait(
                "ScalarTypes",
                &[json!(["name", "==", row_name])],
                &["s".to_string()],
                "==",
                &[json!({"s": "value-that-will-not-appear"})],
                Some(30000)
            )
        ]),
        request_id,
    )
}

fn assert_connection_usable(stream: &mut RawJsonRpcStream, request_id: i64) -> Result<()> {
    stream.send(&json!({
        "method": "echo",
        "params": ["alive"],
        "id": request_id
    }))?;

    let response = stream.recv_responding_to_echo()?;

    assert_eq!(response.get("id"), Some(&json!(request_id)));
    assert_eq!(response.get("error"), Some(&json!(null)));
    assert_eq!(response.get("result"), Some(&json!(["alive"])));

    Ok(())
}

#[test]
fn cancel_notification_receives_no_direct_reply() -> Result<()> {
    let tc = start_custom()?;
    let mut stream = tc.raw_json_rpc_stream()?;

    stream.send(&json!({
        "method": "cancel",
        "params": [999_999],
        "id": null
    }))?;

    let maybe_response = stream.recv_responding_to_echo_timeout(READ_TIMEOUT);
    assert!(
        maybe_response.is_err(),
        "cancel notification should not receive a direct response: {maybe_response:?}"
    );

    assert_connection_usable(&mut stream, 1)?;

    Ok(())
}

#[test]
#[ignore = "CI does not support spawning threads reliable"]
fn cancel_outstanding_wait() -> Result<()> {
    let tc = start_custom()?;
    let row_name = unique_name("cancel-wait");

    insert_scalar(&tc, &row_name)?;

    let client_clone = tc.client.clone();
    let wait_thread = thread::spawn(move || wait_transaction_with_id(&client_clone, 10, &row_name));

    thread::sleep(Duration::from_secs(2));
    tc.client.cancel(10)?;

    let response = wait_thread
        .join()
        .map_err(|_| anyhow::anyhow!("wait thread panicked"))?;

    match response {
        Err(OvsdbError::RpcError(rpc_err)) => {
            assert!(
                rpc_err.error.contains("canceled"),
                "expected canceled error, got {rpc_err:?}"
            );
        }
        other => anyhow::bail!("expected canceled wait error, got {other:?}"),
    }

    Ok(())
}

#[test]
#[ignore = "CI does not support spawning threads reliable"]
fn cancel_original_request_receives_canceled() -> Result<()> {
    let tc = start_custom()?;
    let row_name = unique_name("cancel-original");

    insert_scalar(&tc, &row_name)?;

    let client_clone = tc.client.clone();
    let wait_thread = thread::spawn(move || wait_transaction_with_id(&client_clone, 20, &row_name));

    thread::sleep(Duration::from_secs(2));
    tc.client.cancel(20)?;

    let response = wait_thread
        .join()
        .map_err(|_| anyhow::anyhow!("wait thread panicked"))?;

    match response {
        Err(OvsdbError::RpcError(rpc_err)) => {
            assert!(
                rpc_err.error.contains("canceled"),
                "expected canceled error, got {rpc_err:?}"
            );
        }
        other => anyhow::bail!("expected canceled wait error, got {other:?}"),
    }

    let echoed = tc.client.echo("after-cancel")?;
    assert_eq!(echoed, "after-cancel");

    Ok(())
}

#[test]
fn cancel_unknown_request_does_not_break_connection() -> Result<()> {
    let tc = start_custom()?;
    let mut stream = tc.raw_json_rpc_stream()?;

    stream.send(&json!({
        "method": "cancel",
        "params": [123_456],
        "id": null
    }))?;

    let maybe_response = stream.recv_responding_to_echo_timeout(READ_TIMEOUT);
    assert!(
        maybe_response.is_err(),
        "cancel unknown request should not produce direct response: {maybe_response:?}"
    );

    assert_connection_usable(&mut stream, 1)?;

    Ok(())
}

#[test]
fn cancel_already_completed_request_does_not_break_connection() -> Result<()> {
    let tc = start_custom()?;
    let mut stream = tc.raw_json_rpc_stream()?;

    stream.send(&json!({
        "method": "echo",
        "params": ["done"],
        "id": 30
    }))?;

    let echo = stream.recv_responding_to_echo()?;
    assert_eq!(echo.get("id"), Some(&json!(30)));
    assert_eq!(echo.get("result"), Some(&json!(["done"])));

    stream.send(&json!({
        "method": "cancel",
        "params": [30],
        "id": null
    }))?;

    let maybe_response = stream.recv_responding_to_echo_timeout(READ_TIMEOUT);
    assert!(
        maybe_response.is_err(),
        "cancel completed request should not produce direct response: {maybe_response:?}"
    );

    assert_connection_usable(&mut stream, 31)?;

    Ok(())
}

#[test]
fn cancel_with_bad_params_does_not_break_connection() -> Result<()> {
    let tc = start_custom()?;
    let mut stream = tc.raw_json_rpc_stream()?;

    stream.send(&json!({
        "method": "cancel",
        "params": ["not-an-id"],
        "id": null
    }))?;

    let maybe_response = stream.recv_responding_to_echo_timeout(READ_TIMEOUT);
    if let Ok(response) = maybe_response {
        assert!(
            response.get("error").is_some() || response.get("method").is_some(),
            "unexpected response to bad cancel params: {response:?}"
        );
    }

    assert_connection_usable(&mut stream, 40)?;

    Ok(())
}

#[test]
fn typed_client_cancel_sends_without_hanging() -> Result<()> {
    let tc = start_custom()?;

    tc.client.cancel(999_999)?;

    let echoed = tc.client.echo("after-cancel")?;

    assert_eq!(echoed, "after-cancel");

    Ok(())
}

#[test]
#[ignore = "CI does not support spawning threads reliable"]
fn connection_reusable_after_cancel() -> Result<()> {
    let tc = start_custom()?;
    let row_name = unique_name("cancel-reusable");

    insert_scalar(&tc, &row_name)?;

    let client_clone = tc.client.clone();
    let wait_thread = thread::spawn(move || wait_transaction_with_id(&client_clone, 50, &row_name));

    thread::sleep(Duration::from_secs(2));
    tc.client.cancel(50)?;

    let canceled = wait_thread
        .join()
        .map_err(|_| anyhow::anyhow!("wait thread panicked"))?;

    match canceled {
        Err(OvsdbError::RpcError(rpc_err)) => {
            assert!(
                rpc_err.error.contains("canceled"),
                "expected canceled error, got {rpc_err:?}"
            );
        }
        other => anyhow::bail!("expected canceled wait error, got {other:?}"),
    }

    assert_eq!(tc.client.echo("alive")?, "alive");

    Ok(())
}
