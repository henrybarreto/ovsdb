#![allow(
    missing_docs,
    dead_code,
    clippy::cast_possible_wrap,
    clippy::expect_used,
    clippy::fn_params_excessive_bools,
    clippy::indexing_slicing,
    clippy::map_unwrap_or,
    clippy::match_same_arms,
    clippy::needless_lifetimes,
    clippy::panic,
    clippy::print_stderr,
    clippy::redundant_closure,
    clippy::unused_async,
    clippy::unnecessary_map_or,
    clippy::unnecessary_sort_by
)]

mod support;

use anyhow::{Context, Result};
use ovsdb::client::ops::Ops as ops;
use ovsdb::client::{Row, TransactionOutcome, TransactionResponse};
use serde_json::{json, Value};

use support::{read_raw_json, send_raw_json, unique_name, TestOvsDBClient};

const RFC7047_SCHEMA_PATH: &str = "tests/schemas/rfc7047_compliance.ovsschema";
const RFC7047_DB: &str = "RFC7047_Test";

const UUID_1: &str = "550e8400-e29b-41d4-a716-446655440001";
const MISSING_UUID: &str = "550e8400-e29b-41d4-a716-446655449999";

fn start_custom() -> Result<TestOvsDBClient> {
    TestOvsDBClient::start_with_schema(RFC7047_SCHEMA_PATH, RFC7047_DB)
}

fn raw_transact(tc: &TestOvsDBClient, operations: Vec<Value>) -> Result<Value> {
    let mut stream = tc.raw_tcp_stream()?;

    let mut params = Vec::with_capacity(operations.len() + 1);
    params.push(json!(RFC7047_DB));
    params.extend(operations);

    send_raw_json(
        &mut stream,
        &json!({
            "method": "transact",
            "params": params,
            "id": 1
        }),
    )?;

    read_raw_json(&mut stream)
}

fn raw_result_array(response: &Value) -> Result<&Vec<Value>> {
    assert_eq!(
        response.get("error"),
        Some(&json!(null)),
        "transaction operation/deferred errors should not be JSON-RPC envelope errors: {response:?}"
    );

    response
        .get("result")
        .and_then(Value::as_array)
        .context("expected JSON-RPC result array")
}

fn assert_commit_time_extra_error(response: &Value, operation_count: usize) -> Result<()> {
    let result = raw_result_array(response)?;

    assert_eq!(
        result.len(),
        operation_count + 1,
        "deferred commit error should append one extra result entry: {response:?}"
    );

    let last = result.last().context("missing last result entry")?;
    assert!(
        last.get("error").and_then(Value::as_str).is_some(),
        "last entry should be an error object, got {last:?}"
    );

    Ok(())
}

fn select_rows(result: &TransactionResponse, index: usize) -> Result<&Vec<Row>> {
    result
        .get(index)
        .and_then(TransactionOutcome::rows)
        .ok_or_else(|| anyhow::anyhow!("missing select rows at index {index}: {result:?}"))
}

fn assert_table_empty(tc: &TestOvsDBClient, table: &str, where_clause: &[Value]) -> Result<()> {
    let result = tc
        .client
        .transact(RFC7047_DB, vec![ops::select(table, where_clause, None)])?;

    let rows = select_rows(&result, 0)?;
    assert!(rows.is_empty(), "expected no rows in {table}, got {rows:?}");

    Ok(())
}

fn assert_table_has_one(
    tc: &TestOvsDBClient,
    table: &str,
    where_clause: &[Value],
) -> Result<serde_json::Map<String, Value>> {
    let result = tc
        .client
        .transact(RFC7047_DB, vec![ops::select(table, where_clause, None)])?;

    let rows = select_rows(&result, 0)?;
    assert_eq!(rows.len(), 1, "expected one row in {table}, got {rows:?}");

    Ok(rows[0].clone())
}

#[test]
fn max_rows_violation_appends_extra_error_result() -> Result<()> {
    let tc = start_custom()?;

    let a = unique_name("maxrows-a");
    let b = unique_name("maxrows-b");

    let response = raw_transact(
        &tc,
        vec![
            json!({
                "op": "insert",
                "table": "MaxRows",
                "row": {"name": a},
                "uuid-name": "row_a"
            }),
            json!({
                "op": "insert",
                "table": "MaxRows",
                "row": {"name": b},
                "uuid-name": "row_b"
            }),
        ],
    )?;

    assert_commit_time_extra_error(&response, 2)?;

    let result = raw_result_array(&response)?;
    assert!(
        result[0].get("uuid").is_some(),
        "first insert should have operation result before deferred failure: {response:?}"
    );
    assert!(
        result[1].get("uuid").is_some(),
        "second insert should have operation result before deferred failure: {response:?}"
    );

    assert_table_empty(&tc, "MaxRows", &[])?;

    Ok(())
}

#[test]
fn index_violation_appends_extra_error_result() -> Result<()> {
    let tc = start_custom()?;

    let tenant = unique_name("tenant");
    let key = "same-key";

    let response = raw_transact(
        &tc,
        vec![
            json!({
                "op": "insert",
                "table": "Indexed",
                "row": {
                    "name": unique_name("idx-a"),
                    "tenant": tenant,
                    "key": key
                },
                "uuid-name": "idx_a"
            }),
            json!({
                "op": "insert",
                "table": "Indexed",
                "row": {
                    "name": unique_name("idx-b"),
                    "tenant": tenant,
                    "key": key
                },
                "uuid-name": "idx_b"
            }),
        ],
    )?;

    assert_commit_time_extra_error(&response, 2)?;

    assert_table_empty(&tc, "Indexed", &[json!(["tenant", "==", tenant])])?;

    Ok(())
}

#[test]
fn strong_ref_violation_appends_extra_error_result() -> Result<()> {
    let tc = start_custom()?;

    let child = unique_name("strong-child-missing");

    let response = raw_transact(
        &tc,
        vec![json!({
            "op": "insert",
            "table": "StrongChild",
            "row": {
                "name": child,
                "parent": ["uuid", MISSING_UUID]
            },
            "uuid-name": "child"
        })],
    )?;

    assert_commit_time_extra_error(&response, 1)?;

    assert_table_empty(&tc, "StrongChild", &[json!(["name", "==", child])])?;

    Ok(())
}

#[test]
fn deferred_error_rolls_back_all_operations() -> Result<()> {
    let tc = start_custom()?;

    let scalar = unique_name("rollback-scalar");
    let a = unique_name("maxrows-a");
    let b = unique_name("maxrows-b");

    let response = raw_transact(
        &tc,
        vec![
            json!({
                "op": "insert",
                "table": "ScalarTypes",
                "row": {
                    "name": scalar,
                    "i": 1,
                    "r": 1.0,
                    "b": true,
                    "s": "ok",
                    "u": ["uuid", UUID_1],
                    "enum_s": "a",
                    "limited_i": 1,
                    "limited_r": 1.0,
                    "limited_s": "ok"
                },
                "uuid-name": "scalar"
            }),
            json!({
                "op": "insert",
                "table": "MaxRows",
                "row": {"name": a},
                "uuid-name": "max_a"
            }),
            json!({
                "op": "insert",
                "table": "MaxRows",
                "row": {"name": b},
                "uuid-name": "max_b"
            }),
        ],
    )?;

    assert_commit_time_extra_error(&response, 3)?;

    assert_table_empty(&tc, "ScalarTypes", &[json!(["name", "==", scalar])])?;
    assert_table_empty(&tc, "MaxRows", &[])?;

    Ok(())
}

#[test]
fn weak_ref_to_deleted_row_is_removed() -> Result<()> {
    let tc = start_custom()?;

    let parent = unique_name("weak-parent");
    let child = unique_name("weak-child");

    let insert = tc.client.transact(
        RFC7047_DB,
        vec![
            ops::insert(
                "WeakParent",
                json!({"name": parent.as_str()}),
                Some("parent_uuid"),
            ),
            ops::insert(
                "WeakChild",
                json!({
                    "name": child.as_str(),
                    "parent": ["named-uuid", "parent_uuid"]
                }),
                Some("child_uuid"),
            ),
        ],
    )?;

    assert!(
        matches!(insert.get(0), Some(TransactionOutcome::Insert { .. })),
        "parent insert failed: {insert:?}"
    );

    tc.client.transact(
        RFC7047_DB,
        vec![ops::delete("WeakParent", &[json!(["name", "==", parent])])],
    )?;

    let row = assert_table_has_one(&tc, "WeakChild", &[json!(["name", "==", child])])?;
    assert_eq!(
        row.get("parent"),
        Some(&json!(["set", []])),
        "weak ref should be removed after target delete, got {row:?}"
    );

    Ok(())
}

#[test]
fn weak_ref_inside_map_removes_pair() -> Result<()> {
    let tc = start_custom()?;

    let parent = unique_name("weak-map-parent");
    let child = unique_name("weak-map-child");

    tc.client.transact(
        RFC7047_DB,
        vec![
            ops::insert(
                "WeakParent",
                json!({"name": parent.as_str()}),
                Some("parent_uuid"),
            ),
            ops::insert(
                "WeakMapChild",
                json!({
                    "name": child.as_str(),
                    "parents": ["map", [["p", ["named-uuid", "parent_uuid"]]]]
                }),
                Some("child_uuid"),
            ),
        ],
    )?;

    tc.client.transact(
        RFC7047_DB,
        vec![ops::delete("WeakParent", &[json!(["name", "==", parent])])],
    )?;

    let row = assert_table_has_one(&tc, "WeakMapChild", &[json!(["name", "==", child])])?;

    assert_eq!(
        row.get("parents"),
        Some(&json!(["map", []])),
        "weak map pair should be removed after target delete, got {row:?}"
    );

    Ok(())
}

#[test]
fn non_root_unreferenced_row_is_garbage_collected() -> Result<()> {
    let tc = start_custom()?;

    let child = unique_name("nonroot-unreferenced");

    tc.client.transact(
        RFC7047_DB,
        vec![ops::insert(
            "NonRoot",
            json!({"name": child.as_str()}),
            Some("child_uuid"),
        )],
    )?;

    assert_table_empty(&tc, "NonRoot", &[json!(["name", "==", child])])?;

    Ok(())
}

#[test]
fn referenced_non_root_row_persists() -> Result<()> {
    let tc = start_custom()?;

    let root = unique_name("root");
    let child = unique_name("nonroot-referenced");

    tc.client.transact(
        RFC7047_DB,
        vec![
            ops::insert(
                "NonRoot",
                json!({"name": child.as_str()}),
                Some("child_uuid"),
            ),
            ops::insert(
                "Root",
                json!({
                    "name": root,
                    "child": ["named-uuid", "child_uuid"]
                }),
                Some("root_uuid"),
            ),
        ],
    )?;

    let row = assert_table_has_one(&tc, "NonRoot", &[json!(["name", "==", child])])?;
    assert_eq!(
        row.get("name").and_then(Value::as_str),
        Some(child.as_str())
    );

    Ok(())
}

#[test]
fn max_rows_checked_after_gc() -> Result<()> {
    let tc = start_custom()?;

    let a = unique_name("nonroot-max-a");
    let b = unique_name("nonroot-max-b");

    let response = raw_transact(
        &tc,
        vec![
            json!({
                "op": "insert",
                "table": "NonRootMaxRows",
                "row": {"name": a},
                "uuid-name": "a"
            }),
            json!({
                "op": "insert",
                "table": "NonRootMaxRows",
                "row": {"name": b},
                "uuid-name": "b"
            }),
        ],
    )?;

    let result = raw_result_array(&response)?;
    assert_eq!(
        result.len(),
        2,
        "transaction should succeed without extra deferred error: {response:?}"
    );

    assert_table_empty(&tc, "NonRootMaxRows", &[])?;

    Ok(())
}

#[test]
fn index_checked_after_gc() -> Result<()> {
    let tc = start_custom()?;

    let duplicate = unique_name("nonroot-idx");

    let response = raw_transact(
        &tc,
        vec![
            json!({
                "op": "insert",
                "table": "NonRootIndexed",
                "row": {"name": duplicate},
                "uuid-name": "a"
            }),
            json!({
                "op": "insert",
                "table": "NonRootIndexed",
                "row": {"name": duplicate},
                "uuid-name": "b"
            }),
        ],
    )?;

    let result = raw_result_array(&response)?;
    assert_eq!(
        result.len(),
        2,
        "duplicate non-root rows should be GC'd before index check: {response:?}"
    );

    assert_table_empty(&tc, "NonRootIndexed", &[])?;

    Ok(())
}
