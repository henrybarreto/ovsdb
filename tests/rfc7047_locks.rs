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
use ovsdb::client::error::Error;
use ovsdb::client::ops::Ops as ops;
use ovsdb::client::{Connection as Client, Notification, TransactionOutcome};
use serde_json::{json, Value};
use std::time::Duration;

use support::{unique_name, RawJsonRpcStream, TestOvsDBClient};

const RFC7047_SCHEMA_PATH: &str = "tests/schemas/rfc7047_compliance.ovsschema";
const RFC7047_DB: &str = "RFC7047_Test";
const NOTIFICATION_TIMEOUT: Duration = Duration::from_secs(5);

fn start_custom() -> Result<TestOvsDBClient> {
    TestOvsDBClient::start_with_schema(RFC7047_SCHEMA_PATH, RFC7047_DB)
}

fn lock_id(prefix: &str) -> String {
    unique_name(prefix).replace('-', "_")
}

fn poll_notification(client: &Client) -> Result<Notification> {
    client
        .poll_notification_timeout(NOTIFICATION_TIMEOUT)?
        .ok_or_else(|| anyhow::anyhow!("timeout waiting for lock notification"))
}

fn expect_locked_notification(client: &Client, expected_lock: &str) -> Result<()> {
    let notif = poll_notification(client)?;

    assert_eq!(notif.method(), "locked");
    assert_eq!(
        notif.lock_id().map(String::as_str),
        Some(expected_lock),
        "locked notification should contain lock id {expected_lock}, got {notif:?}"
    );

    Ok(())
}

fn expect_stolen_notification(client: &Client, expected_lock: &str) -> Result<()> {
    let notif = poll_notification(client)?;

    assert_eq!(notif.method(), "stolen");
    assert_eq!(
        notif.lock_id().map(String::as_str),
        Some(expected_lock),
        "stolen notification should contain lock id {expected_lock}, got {notif:?}"
    );

    Ok(())
}

fn assert_no_notification(client: &Client) -> Result<()> {
    let result = client.poll_notification_timeout(Duration::from_millis(500))?;

    assert!(
        result.is_none(),
        "unexpected notification arrived: {result:?}"
    );

    Ok(())
}

fn assert_current_owner_can_assert(client: &Client, lock: &str) -> Result<()> {
    let result = client.transact(RFC7047_DB, vec![ops::assert(lock)])?;

    assert!(
        matches!(result.get(0), Some(TransactionOutcome::Empty)),
        "assert should succeed for lock owner, got {result:?}"
    );

    Ok(())
}

fn assert_not_owner_fails_assert(client: &Client, lock: &str) -> Result<()> {
    let result = client.transact(RFC7047_DB, vec![ops::assert(lock)])?;

    match result.get(0) {
        Some(TransactionOutcome::Error(err)) => {
            assert!(
                !err.error.is_empty(),
                "assert error string should not be empty"
            );
        }
        other => anyhow::bail!("expected assert operation error, got {other:?}"),
    }

    Ok(())
}

fn raw_request(
    stream: &mut RawJsonRpcStream,
    method: &str,
    params: &Value,
    id: u64,
) -> Result<Value> {
    stream.send(&json!({
        "method": method,
        "params": params,
        "id": id
    }))?;

    stream.recv_responding_to_echo_timeout(NOTIFICATION_TIMEOUT)
}

#[test]
fn lock_free_returns_true() -> Result<()> {
    let tc = start_custom()?;
    let lock = lock_id("lock_free");

    let locked = tc.client.lock(&lock)?;

    assert!(locked, "expected free lock to be acquired");

    tc.client.unlock(&lock)?;

    Ok(())
}

#[test]
fn lock_held_returns_false_and_queues() -> Result<()> {
    let tc = start_custom()?;
    let client2 = tc.second_client()?;
    let lock = lock_id("lock_held");

    assert!(tc.client.lock(&lock)?);

    let acquired = client2.lock(&lock)?;

    assert!(
        !acquired,
        "second client should not acquire lock immediately"
    );

    tc.client.unlock(&lock)?;

    expect_locked_notification(&client2, &lock)?;

    client2.unlock(&lock)?;

    Ok(())
}

#[test]
fn queued_waiter_receives_locked_notification() -> Result<()> {
    let tc = start_custom()?;
    let client2 = tc.second_client()?;
    let lock = lock_id("lock_notify");

    assert!(tc.client.lock(&lock)?);
    assert!(!client2.lock(&lock)?);

    tc.client.unlock(&lock)?;

    expect_locked_notification(&client2, &lock)?;

    client2.unlock(&lock)?;

    Ok(())
}

#[test]
fn lock_waiters_are_fifo() -> Result<()> {
    let tc = start_custom()?;
    let client2 = tc.second_client()?;
    let client3 = tc.second_client()?;
    let lock = lock_id("lock_fifo");

    assert!(tc.client.lock(&lock)?);
    assert!(!client2.lock(&lock)?);
    assert!(!client3.lock(&lock)?);

    tc.client.unlock(&lock)?;

    expect_locked_notification(&client2, &lock)?;
    assert_no_notification(&client3)?;

    client2.unlock(&lock)?;

    expect_locked_notification(&client3, &lock)?;

    client3.unlock(&lock)?;

    Ok(())
}

#[test]
fn steal_succeeds() -> Result<()> {
    let tc = start_custom()?;
    let client2 = tc.second_client()?;
    let lock = lock_id("steal_success");

    assert!(tc.client.lock(&lock)?);

    let stolen = client2.steal(&lock)?;

    assert!(stolen, "steal should return locked:true");

    expect_stolen_notification(&tc.client, &lock)?;

    client2.unlock(&lock)?;

    Ok(())
}

#[test]
fn stolen_owner_receives_stolen_notification() -> Result<()> {
    let tc = start_custom()?;
    let client2 = tc.second_client()?;
    let lock = lock_id("stolen_notify");

    assert!(tc.client.lock(&lock)?);
    assert!(client2.steal(&lock)?);

    expect_stolen_notification(&tc.client, &lock)?;

    client2.unlock(&lock)?;

    Ok(())
}

#[test]
fn unlock_owner_releases_lock() -> Result<()> {
    let tc = start_custom()?;
    let client2 = tc.second_client()?;
    let lock = lock_id("unlock_release");

    assert!(tc.client.lock(&lock)?);
    tc.client.unlock(&lock)?;

    let acquired_by_second = client2.lock(&lock)?;
    assert!(
        acquired_by_second,
        "second client should acquire lock after unlock"
    );

    client2.unlock(&lock)?;

    Ok(())
}

#[test]
fn unlock_queued_waiter_cancels_wait() -> Result<()> {
    let tc = start_custom()?;
    let client2 = tc.second_client()?;
    let client3 = tc.second_client()?;
    let lock = lock_id("unlock_queued");

    assert!(tc.client.lock(&lock)?);
    assert!(!client2.lock(&lock)?);

    client2.unlock(&lock)?;

    assert!(!client3.lock(&lock)?);

    tc.client.unlock(&lock)?;

    expect_locked_notification(&client3, &lock)?;
    assert_no_notification(&client2)?;

    client3.unlock(&lock)?;

    Ok(())
}

#[test]
fn disconnect_owner_releases_lock() -> Result<()> {
    let tc = start_custom()?;
    let waiter = tc.second_client()?;
    let lock = lock_id("disconnect_owner");

    let stream = tc.raw_tcp_stream()?;
    let read_half = stream.try_clone()?;
    let write_half = stream;
    let mut raw = RawJsonRpcStream::new(read_half, write_half);

    let response = raw_request(&mut raw, "lock", &json!([lock]), 1)?;

    assert_eq!(response.get("id"), Some(&json!(1)));
    assert_eq!(response.get("error"), Some(&json!(null)));
    assert_eq!(
        response
            .get("result")
            .and_then(|r| r.get("locked"))
            .and_then(Value::as_bool),
        Some(true)
    );

    assert!(!waiter.lock(&lock)?);

    drop(raw);

    expect_locked_notification(&waiter, &lock)?;

    waiter.unlock(&lock)?;

    Ok(())
}

#[test]
fn disconnect_queued_waiter_removes_wait() -> Result<()> {
    let tc = start_custom()?;
    let client3 = tc.second_client()?;
    let lock = lock_id("disconnect_queued");

    assert!(tc.client.lock(&lock)?);

    let stream = tc.raw_tcp_stream()?;
    let read_half = stream.try_clone()?;
    let write_half = stream;
    let mut queued = RawJsonRpcStream::new(read_half, write_half);

    let response = raw_request(&mut queued, "lock", &json!([lock]), 1)?;

    assert_eq!(response.get("id"), Some(&json!(1)));
    assert_eq!(
        response
            .get("result")
            .and_then(|r| r.get("locked"))
            .and_then(Value::as_bool),
        Some(false)
    );

    drop(queued);

    assert!(!client3.lock(&lock)?);

    tc.client.unlock(&lock)?;

    expect_locked_notification(&client3, &lock)?;

    client3.unlock(&lock)?;

    Ok(())
}

#[test]
fn lock_scope_is_server_wide() -> Result<()> {
    let tc = start_custom()?;
    let client2 = tc.second_client()?;
    let lock = lock_id("server_wide");

    assert!(tc.client.lock(&lock)?);
    assert!(
        !client2.lock(&lock)?,
        "same lock id should be held server-wide"
    );

    tc.client.unlock(&lock)?;
    expect_locked_notification(&client2, &lock)?;

    client2.unlock(&lock)?;

    Ok(())
}

#[test]
fn lock_same_lock_twice_without_unlock_fails_or_client_rejects() -> Result<()> {
    let tc = start_custom()?;
    let lock = lock_id("double_lock");

    assert!(tc.client.lock(&lock)?);

    let second = tc.client.lock(&lock);

    if let Ok(value) = second {
        assert!(
            !value,
            "second lock without unlock must not report acquired:true"
        );
    }

    tc.client.unlock(&lock).ok();

    Ok(())
}

#[test]
fn steal_same_lock_twice_without_unlock_fails_or_client_rejects() -> Result<()> {
    let tc = start_custom()?;
    let lock = lock_id("double_steal");

    assert!(tc.client.steal(&lock)?);

    let second = tc.client.steal(&lock);

    if let Ok(value) = second {
        assert!(
            !value,
            "second steal without unlock must not report acquired:true"
        );
    }

    tc.client.unlock(&lock).ok();

    Ok(())
}

#[test]
fn unlock_without_prior_lock_or_steal_fails_or_client_rejects() -> Result<()> {
    let tc = start_custom()?;
    let lock = lock_id("unlock_without_lock");

    let result = tc.client.unlock(&lock);

    match result {
        Ok(()) => anyhow::bail!("unlock without prior lock/steal should not succeed"),
        Err(Error::Validation(_) | Error::RpcError(_)) => Ok(()),
        Err(other) => anyhow::bail!("unexpected error: {other:?}"),
    }
}

#[test]
fn assert_succeeds_only_for_current_lock_owner() -> Result<()> {
    let tc = start_custom()?;
    let client2 = tc.second_client()?;
    let lock = lock_id("assert_owner");

    assert!(tc.client.lock(&lock)?);

    assert_current_owner_can_assert(&tc.client, &lock)?;
    assert_not_owner_fails_assert(&client2, &lock)?;

    tc.client.unlock(&lock)?;

    Ok(())
}

#[test]
fn assert_fails_after_stolen_notification() -> Result<()> {
    let tc = start_custom()?;
    let stealer = tc.second_client()?;
    let lock = lock_id("assert_after_stolen");

    assert!(tc.client.lock(&lock)?);
    assert_current_owner_can_assert(&tc.client, &lock)?;

    assert!(stealer.steal(&lock)?);
    expect_stolen_notification(&tc.client, &lock)?;

    assert_not_owner_fails_assert(&tc.client, &lock)?;
    assert_current_owner_can_assert(&stealer, &lock)?;

    stealer.unlock(&lock)?;

    Ok(())
}

#[test]
fn raw_locked_notification_has_null_id() -> Result<()> {
    let tc = start_custom()?;
    let lock = lock_id("raw_locked");

    assert!(tc.client.lock(&lock)?);

    let stream = tc.raw_tcp_stream()?;
    let (read_half, write_half) = (stream.try_clone()?, stream);
    let mut raw = RawJsonRpcStream::new(read_half, write_half);

    let lock_response = raw_request(&mut raw, "lock", &json!([lock]), 1)?;
    assert_eq!(lock_response.get("id"), Some(&json!(1)));
    assert_eq!(lock_response.get("error"), Some(&json!(null)));
    assert_eq!(
        lock_response
            .get("result")
            .and_then(|r| r.get("locked"))
            .and_then(Value::as_bool),
        Some(false)
    );

    tc.client.unlock(&lock)?;

    let notification = raw.recv_responding_to_echo_timeout(NOTIFICATION_TIMEOUT)?;

    assert_eq!(notification.get("method"), Some(&json!("locked")));
    assert_eq!(notification.get("params"), Some(&json!([lock])));
    assert_eq!(notification.get("id"), Some(&json!(null)));

    let unlock_response = raw_request(&mut raw, "unlock", &json!([lock]), 2)?;
    assert_eq!(unlock_response.get("id"), Some(&json!(2)));
    assert_eq!(unlock_response.get("error"), Some(&json!(null)));
    assert_eq!(unlock_response.get("result"), Some(&json!({})));

    Ok(())
}

#[test]
fn raw_stolen_notification_has_null_id() -> Result<()> {
    let tc = start_custom()?;
    let lock = lock_id("raw_stolen");

    let stream = tc.raw_tcp_stream()?;
    let (read_half, write_half) = (stream.try_clone()?, stream);
    let mut raw = RawJsonRpcStream::new(read_half, write_half);

    let lock_response = raw_request(&mut raw, "lock", &json!([lock]), 1)?;
    assert_eq!(lock_response.get("id"), Some(&json!(1)));
    assert_eq!(lock_response.get("error"), Some(&json!(null)));
    assert_eq!(
        lock_response
            .get("result")
            .and_then(|r| r.get("locked"))
            .and_then(Value::as_bool),
        Some(true)
    );

    let stealer = tc.second_client()?;
    assert!(stealer.steal(&lock)?);

    let notification = raw.recv_responding_to_echo_timeout(NOTIFICATION_TIMEOUT)?;

    assert_eq!(notification.get("method"), Some(&json!("stolen")));
    assert_eq!(notification.get("params"), Some(&json!([lock])));
    assert_eq!(notification.get("id"), Some(&json!(null)));

    stealer.unlock(&lock)?;

    Ok(())
}
