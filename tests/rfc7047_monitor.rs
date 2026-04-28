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

use anyhow::{Context, Result};
use ovsdb::client::error::Error;
use ovsdb::client::ops::Ops as ops;
use ovsdb::client::{Notification, RowUpdate, TableUpdates};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::Duration;

use support::{exec_text, unique_name, RawJsonRpcStream, TestOvsDBClient};

const RFC7047_SCHEMA_PATH: &str = "tests/schemas/rfc7047_compliance.ovsschema";
const RFC7047_DB: &str = "RFC7047_Test";
const NOTIFICATION_TIMEOUT: Duration = Duration::from_secs(5);

fn start_custom() -> Result<TestOvsDBClient> {
    TestOvsDBClient::start_with_schema(RFC7047_SCHEMA_PATH, RFC7047_DB)
}

fn scalar_row(name: &str) -> Value {
    json!({
        "name": name,
        "i": 1,
        "r": 1.5,
        "b": true,
        "s": "initial",
        "u": ["uuid", "550e8400-e29b-41d4-a716-446655440001"],
        "enum_s": "a",
        "limited_i": 1,
        "limited_r": 1.5,
        "limited_s": "ok"
    })
}

#[allow(clippy::fn_params_excessive_bools)]
fn monitor_request(
    columns: Option<Vec<&str>>,
    initial: bool,
    insert: bool,
    delete: bool,
    modify: bool,
) -> HashMap<String, Value> {
    let mut request = serde_json::Map::new();

    if let Some(columns) = columns {
        request.insert(
            "columns".to_string(),
            json!(columns.into_iter().map(str::to_string).collect::<Vec<_>>()),
        );
    }

    request.insert(
        "select".to_string(),
        json!({
            "initial": initial,
            "insert": insert,
            "delete": delete,
            "modify": modify
        }),
    );

    let mut map = HashMap::new();
    map.insert("ScalarTypes".to_string(), json!([Value::Object(request)]));
    map
}

fn monitor_all_events(columns: Vec<&str>) -> HashMap<String, Value> {
    monitor_request(Some(columns), true, true, true, true)
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

fn update_scalar_s(tc: &TestOvsDBClient, name: &str, new_value: &str) -> Result<()> {
    tc.client.transact(
        RFC7047_DB,
        vec![ops::update(
            "ScalarTypes",
            &[json!(["name", "==", name])],
            json!({"s": new_value}),
        )],
    )?;

    Ok(())
}

fn delete_scalar(tc: &TestOvsDBClient, name: &str) -> Result<()> {
    tc.client.transact(
        RFC7047_DB,
        vec![ops::delete("ScalarTypes", &[json!(["name", "==", name])])],
    )?;

    Ok(())
}

fn poll_update(tc: &TestOvsDBClient) -> Result<Notification> {
    let notif = tc
        .client
        .poll_notification_timeout(NOTIFICATION_TIMEOUT)?
        .ok_or_else(|| anyhow::anyhow!("timeout waiting for monitor update"))?;

    assert_eq!(notif.method(), "update");

    Ok(notif)
}

fn assert_no_notification(tc: &TestOvsDBClient) -> Result<()> {
    let result = tc
        .client
        .poll_notification_timeout(Duration::from_millis(500))?;

    assert!(
        result.is_none(),
        "unexpected monitor notification arrived: {result:?}"
    );

    Ok(())
}

const fn updates(notif: &Notification) -> &TableUpdates {
    notif
        .updates()
        .expect("update notification must carry table updates")
}

fn table_updates<'a>(
    updates: &'a TableUpdates,
    table: &str,
) -> Option<&'a HashMap<String, RowUpdate>> {
    updates.get(table)
}

fn any_new_row<'a>(
    updates: &'a TableUpdates,
    table: &str,
) -> Option<&'a serde_json::Map<String, Value>> {
    table_updates(updates, table)?
        .values()
        .find_map(RowUpdate::new_row)
}

fn row_has_only_columns(row: &serde_json::Map<String, Value>, columns: &[&str]) -> bool {
    row.len() == columns.len() && columns.iter().all(|column| row.contains_key(*column))
}

fn contains_insert_update(notif: &Notification, table: &str) -> bool {
    updates(notif).get(table).is_some_and(|table_updates| {
        table_updates
            .values()
            .any(|row_update| row_update.old().is_none() && row_update.new_row().is_some())
    })
}

fn contains_modify_update(notif: &Notification, table: &str) -> bool {
    updates(notif).get(table).is_some_and(|table_updates| {
        table_updates
            .values()
            .any(|row_update| row_update.old().is_some() && row_update.new_row().is_some())
    })
}

fn contains_delete_update(notif: &Notification, table: &str) -> bool {
    updates(notif).get(table).is_some_and(|table_updates| {
        table_updates
            .values()
            .any(|row_update| row_update.old().is_some() && row_update.new_row().is_none())
    })
}

#[test]
fn monitor_initial_snapshot() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("monitor-initial");

    insert_scalar(&tc, &name)?;

    let monitor_id = json!(unique_name("monitor"));
    let requests = monitor_all_events(vec!["name", "s"]);

    let initial = tc.client.monitor(RFC7047_DB, &monitor_id, &requests)?;

    let row = any_new_row(&initial, "ScalarTypes")
        .context("expected initial monitor snapshot for ScalarTypes")?;
    assert_eq!(row.get("name").and_then(Value::as_str), Some(name.as_str()));
    assert_eq!(row.get("s").and_then(Value::as_str), Some("initial"));

    Ok(())
}

#[test]
fn monitor_initial_false_returns_empty_result() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("monitor-no-initial");

    insert_scalar(&tc, &name)?;

    let monitor_id = json!(unique_name("monitor"));
    let requests = monitor_request(Some(vec!["name", "s"]), false, true, true, true);

    let initial = tc.client.monitor(RFC7047_DB, &monitor_id, &requests)?;

    assert!(
        table_updates(&initial, "ScalarTypes").is_none_or(std::collections::HashMap::is_empty),
        "initial:false should not include existing row {name}, got {initial:?}"
    );

    Ok(())
}

#[test]
fn monitor_selected_columns() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("monitor-columns");

    insert_scalar(&tc, &name)?;

    let monitor_id = json!(unique_name("monitor"));
    let requests = monitor_all_events(vec!["name"]);

    let initial = tc.client.monitor(RFC7047_DB, &monitor_id, &requests)?;

    let row = any_new_row(&initial, "ScalarTypes")
        .context("expected initial monitor snapshot for ScalarTypes")?;
    assert_eq!(row.get("name").and_then(Value::as_str), Some(name.as_str()));
    assert!(
        row_has_only_columns(row, &["name"]),
        "monitor selected only name, got row {row:?}"
    );

    Ok(())
}

#[test]
fn monitor_default_select_flags() -> Result<()> {
    let tc = start_custom()?;

    let monitor_id = json!(unique_name("monitor-default"));
    let mut table_request = serde_json::Map::new();
    table_request.insert("columns".to_string(), json!(["name", "s"]));

    let mut requests = HashMap::new();
    requests.insert(
        "ScalarTypes".to_string(),
        json!([Value::Object(table_request)]),
    );

    let _initial = tc.client.monitor(RFC7047_DB, &monitor_id, &requests)?;

    let name = unique_name("monitor-default-row");

    insert_scalar(&tc, &name)?;

    let notif = poll_update(&tc)?;

    assert!(
        contains_insert_update(&notif, "ScalarTypes"),
        "default select should include insert updates, got {notif:?}"
    );

    Ok(())
}

#[test]
fn monitor_insert_update_has_new_only() -> Result<()> {
    let tc = start_custom()?;

    let monitor_id = json!(unique_name("monitor-insert"));
    let requests = monitor_request(Some(vec!["name", "s"]), false, true, true, true);

    let _initial = tc.client.monitor(RFC7047_DB, &monitor_id, &requests)?;

    let name = unique_name("monitor-insert-row");
    insert_scalar(&tc, &name)?;

    let notif = poll_update(&tc)?;

    assert!(
        contains_insert_update(&notif, "ScalarTypes"),
        "insert update should have new only, got {notif:?}"
    );

    Ok(())
}

#[test]
fn monitor_modify_update_has_old_and_new() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("monitor-modify");

    insert_scalar(&tc, &name)?;

    let monitor_id = json!(unique_name("monitor-modify"));
    let requests = monitor_request(Some(vec!["name", "s"]), false, true, true, true);

    let _initial = tc.client.monitor(RFC7047_DB, &monitor_id, &requests)?;

    update_scalar_s(&tc, &name, "modified")?;

    let notif = poll_update(&tc)?;

    assert!(
        contains_modify_update(&notif, "ScalarTypes"),
        "modify update should have old and new, got {notif:?}"
    );

    Ok(())
}

#[test]
fn monitor_delete_update_has_old_only() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("monitor-delete");

    insert_scalar(&tc, &name)?;

    let monitor_id = json!(unique_name("monitor-delete"));
    let requests = monitor_request(Some(vec!["name", "s"]), false, true, true, true);

    let _initial = tc.client.monitor(RFC7047_DB, &monitor_id, &requests)?;

    delete_scalar(&tc, &name)?;

    let notif = poll_update(&tc)?;

    assert!(
        contains_delete_update(&notif, "ScalarTypes"),
        "delete update should have old only, got {notif:?}"
    );

    Ok(())
}

#[test]
fn monitor_insert_false_suppresses_insert() -> Result<()> {
    let tc = start_custom()?;

    let monitor_id = json!(unique_name("monitor-no-insert"));
    let requests = monitor_request(Some(vec!["name", "s"]), false, false, true, true);

    let _initial = tc.client.monitor(RFC7047_DB, &monitor_id, &requests)?;

    let name = unique_name("monitor-no-insert-row");
    insert_scalar(&tc, &name)?;

    assert_no_notification(&tc)?;

    Ok(())
}

#[test]
fn monitor_modify_false_suppresses_modify() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("monitor-no-modify");

    insert_scalar(&tc, &name)?;

    let monitor_id = json!(unique_name("monitor-no-modify"));
    let requests = monitor_request(Some(vec!["name", "s"]), false, true, true, false);

    let _initial = tc.client.monitor(RFC7047_DB, &monitor_id, &requests)?;

    update_scalar_s(&tc, &name, "modified")?;

    assert_no_notification(&tc)?;

    Ok(())
}

#[test]
fn monitor_delete_false_suppresses_delete() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("monitor-no-delete");

    insert_scalar(&tc, &name)?;

    let monitor_id = json!(unique_name("monitor-no-delete"));
    let requests = monitor_request(Some(vec!["name", "s"]), false, true, false, true);

    let _initial = tc.client.monitor(RFC7047_DB, &monitor_id, &requests)?;

    delete_scalar(&tc, &name)?;

    assert_no_notification(&tc)?;

    Ok(())
}

#[test]
fn monitor_unmonitored_column_change_sends_no_useful_update() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("monitor-unmonitored");

    insert_scalar(&tc, &name)?;

    let monitor_id = json!(unique_name("monitor-unmonitored"));
    let requests = monitor_request(Some(vec!["name"]), false, true, true, true);

    let _initial = tc.client.monitor(RFC7047_DB, &monitor_id, &requests)?;

    update_scalar_s(&tc, &name, "changed-but-unmonitored")?;

    assert_no_notification(&tc)?;

    Ok(())
}

#[test]
fn monitor_multiple_requests_same_table_non_overlapping_columns_success() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("monitor-multi");

    insert_scalar(&tc, &name)?;

    let monitor_id = json!(unique_name("monitor-multi"));

    let mut requests = HashMap::new();
    requests.insert(
        "ScalarTypes".to_string(),
        json!([
            {
                "columns": ["name"],
                "select": {"initial": true, "insert": true, "delete": true, "modify": true}
            },
            {
                "columns": ["s"],
                "select": {"initial": true, "insert": true, "delete": true, "modify": true}
            }
        ]),
    );

    let initial = tc.client.monitor(RFC7047_DB, &monitor_id, &requests)?;
    let row = any_new_row(&initial, "ScalarTypes")
        .context("expected initial monitor snapshot for ScalarTypes")?;
    assert_eq!(row.get("name").and_then(Value::as_str), Some(name.as_str()));
    assert_eq!(row.get("s").and_then(Value::as_str), Some("initial"));

    Ok(())
}

#[test]
fn monitor_duplicate_columns_fails() -> Result<()> {
    let tc = start_custom()?;

    let monitor_id = json!(unique_name("monitor-dup-columns"));

    let mut requests = HashMap::new();
    requests.insert(
        "ScalarTypes".to_string(),
        json!([
            {
                "columns": ["name", "name"],
                "select": {"initial": true, "insert": true, "delete": true, "modify": true}
            }
        ]),
    );

    let result = tc.client.monitor(RFC7047_DB, &monitor_id, &requests);

    match result {
        Ok(value) => anyhow::bail!("expected duplicate columns to fail, got {value:?}"),
        Err(Error::RpcError(_) | Error::Validation(_)) => Ok(()),
        Err(other) => anyhow::bail!("unexpected error: {other:?}"),
    }
}

#[test]
fn monitor_overlapping_requests_same_table_fail() -> Result<()> {
    let tc = start_custom()?;

    let monitor_id = json!(unique_name("monitor-overlap"));

    let mut requests = HashMap::new();
    requests.insert(
        "ScalarTypes".to_string(),
        json!([
            {
                "columns": ["name", "s"],
                "select": {"initial": true, "insert": true, "delete": true, "modify": true}
            },
            {
                "columns": ["s", "i"],
                "select": {"initial": true, "insert": true, "delete": true, "modify": true}
            }
        ]),
    );

    let result = tc.client.monitor(RFC7047_DB, &monitor_id, &requests);

    match result {
        Ok(value) => anyhow::bail!("expected overlapping monitor columns to fail, got {value:?}"),
        Err(Error::RpcError(_) | Error::Validation(_)) => Ok(()),
        Err(other) => anyhow::bail!("unexpected error: {other:?}"),
    }
}

#[test]
fn monitor_cancel_success() -> Result<()> {
    let tc = start_custom()?;

    let monitor_id = json!(unique_name("monitor-cancel"));
    let requests = monitor_all_events(vec!["name", "s"]);

    let _initial = tc.client.monitor(RFC7047_DB, &monitor_id, &requests)?;

    tc.client.monitor_cancel(&monitor_id)?;

    Ok(())
}

#[test]
fn monitor_cancel_stops_updates() -> Result<()> {
    let tc = start_custom()?;

    let monitor_id = json!(unique_name("monitor-cancel-stop"));
    let requests = monitor_all_events(vec!["name", "s"]);

    let _initial = tc.client.monitor(RFC7047_DB, &monitor_id, &requests)?;

    tc.client.monitor_cancel(&monitor_id)?;

    let name = unique_name("monitor-after-cancel");
    insert_scalar(&tc, &name)?;

    assert_no_notification(&tc)?;

    Ok(())
}

#[test]
fn monitor_cancel_unknown_monitor_fails() -> Result<()> {
    let tc = start_custom()?;

    let result = tc.client.monitor_cancel(&json!("does-not-exist"));

    match result {
        Err(Error::RpcError(rpc_err)) => {
            assert_eq!(rpc_err.error, "unknown monitor");
            Ok(())
        }
        Err(Error::Validation(_)) => Ok(()),
        other => anyhow::bail!("expected unknown monitor error, got {other:?}"),
    }
}

#[test]
fn raw_update_notification_has_null_id_and_method_update() -> Result<()> {
    let tc = start_custom()?;
    let stream = tc.raw_tcp_stream()?;
    let read_half = stream.try_clone()?;
    let write_half = stream;
    let mut rpc = RawJsonRpcStream::new(read_half, write_half);
    let monitor_id = "raw-monitor";
    let requests = monitor_request(Some(vec!["name", "s"]), true, true, true, true);
    let raw_timeout = Duration::from_secs(20);

    rpc.send(&json!({
        "method": "monitor",
        "params": [RFC7047_DB, monitor_id, requests],
        "id": 1
    }))?;

    let monitor_response = match rpc.recv_responding_to_echo_timeout(raw_timeout) {
        Ok(value) => value,
        Err(err) => {
            let logs = exec_text(
                &tc.container,
                vec!["sh", "-c", "cat /tmp/ovsdb-custom.log || true"],
            )
            .unwrap_or_default();
            panic!("raw monitor response read failed: {err:?}\nlogs:\n{logs}");
        }
    };
    assert_eq!(monitor_response.get("id"), Some(&json!(1)));
    assert_eq!(monitor_response.get("error"), Some(&json!(null)));

    let name = unique_name("raw-monitor-insert");
    let client = tc.client.clone();
    let insert_handle = std::thread::spawn(move || {
        client.transact(
            RFC7047_DB,
            vec![ops::insert(
                "ScalarTypes",
                scalar_row(&name),
                Some("row_uuid"),
            )],
        )
    });

    let update = match rpc.recv_responding_to_echo_timeout(raw_timeout) {
        Ok(value) => value,
        Err(err) => {
            let logs = exec_text(
                &tc.container,
                vec!["sh", "-c", "cat /tmp/ovsdb-custom.log || true"],
            )
            .unwrap_or_default();
            panic!("raw update read failed: {err:?}\nlogs:\n{logs}");
        }
    };

    assert_eq!(update.get("method"), Some(&json!("update")));
    assert_eq!(update.get("id"), Some(&json!(null)));

    let params = update
        .get("params")
        .and_then(Value::as_array)
        .context("update notification params should be array")?;

    assert_eq!(
        params.first(),
        Some(&json!(monitor_id)),
        "first update param should be monitor id: {update:?}"
    );

    insert_handle
        .join()
        .map_err(|_| anyhow::anyhow!("join raw insert thread failed"))?
        .context("raw insert transaction failed")?;

    Ok(())
}
