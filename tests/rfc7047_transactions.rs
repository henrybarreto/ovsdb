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

use anyhow::Result;
use ovsdb::client::error::Error;
use ovsdb::client::ops::Ops as ops;
use ovsdb::client::{TransactionOutcome, TransactionResponse};
use serde_json::{json, Value};

use support::{unique_name, TestOvsDBClient};

const RFC7047_SCHEMA_PATH: &str = "tests/schemas/rfc7047_compliance.ovsschema";
const RFC7047_DB: &str = "RFC7047_Test";

const UUID_1: &str = "550e8400-e29b-41d4-a716-446655440001";

fn start_custom() -> Result<TestOvsDBClient> {
    TestOvsDBClient::start_with_schema(RFC7047_SCHEMA_PATH, RFC7047_DB)
}

fn scalar_row(name: &str) -> Value {
    json!({
        "name": name,
        "i": 1,
        "r": 1.5,
        "b": true,
        "s": "hello",
        "u": ["uuid", UUID_1],
        "enum_s": "a",
        "limited_i": 1,
        "limited_r": 1.5,
        "limited_s": "ok"
    })
}

fn set_row(name: &str) -> Value {
    json!({
        "name": name,
        "ints": ["set", []],
        "strings": ["set", []],
        "uuids": ["set", []],
        "small_set": "required"
    })
}

fn map_row(name: &str) -> Value {
    json!({
        "name": name,
        "ss": ["map", []],
        "si": ["map", []],
        "su": ["map", []],
        "small_map": ["map", [["required", "value"]]]
    })
}

fn select_rows(result: &TransactionResponse, index: usize) -> Result<&Vec<ovsdb::client::Row>> {
    result
        .get(index)
        .and_then(TransactionOutcome::rows)
        .ok_or_else(|| anyhow::anyhow!("missing select rows at index {index}: {result:?}"))
}

fn expect_count(result: &TransactionResponse, index: usize, expected: i64) -> Result<()> {
    match result.get(index) {
        Some(TransactionOutcome::Count { count }) => {
            assert_eq!(*count as i64, expected);
            Ok(())
        }
        other => anyhow::bail!("expected count {expected} at index {index}, got {other:?}"),
    }
}

fn expect_operation_error(result: &TransactionResponse, index: usize) -> Result<()> {
    match result.get(index) {
        Some(TransactionOutcome::Error(err)) => {
            assert!(
                !err.error.is_empty(),
                "operation error string should not be empty"
            );
            Ok(())
        }
        other => anyhow::bail!("expected operation error at index {index}, got {other:?}"),
    }
}

#[test]
fn insert_all_scalar_types() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("insert-scalar");

    let result = tc.client.transact(
        RFC7047_DB,
        vec![ops::insert(
            "ScalarTypes",
            scalar_row(&name),
            Some("row_uuid"),
        )],
    )?;

    assert!(
        matches!(
            result.get(0),
            Some(TransactionOutcome::Insert { uuid }) if !uuid.is_empty()
        ),
        "expected insert uuid result, got {result:?}"
    );

    Ok(())
}

#[test]
fn insert_wrong_type_fails() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("insert-wrong-type");

    let result = tc.client.transact(
        RFC7047_DB,
        vec![ops::insert(
            "ScalarTypes",
            json!({
                "name": name,
                "i": "not-an-integer",
                "r": 1.0,
                "b": true,
                "s": "x",
                "u": ["uuid", UUID_1],
                "enum_s": "a",
                "limited_i": 1,
                "limited_r": 1.0,
                "limited_s": "x"
            }),
            Some("row_uuid"),
        )],
    );

    match result {
        Ok(result) => expect_operation_error(&result, 0)?,
        Err(Error::Validation(_)) => {}
        Err(other) => anyhow::bail!("unexpected error: {other:?}"),
    }

    Ok(())
}

#[test]
fn insert_enum_violation_fails() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("insert-enum-bad");

    let result = tc.client.transact(
        RFC7047_DB,
        vec![ops::insert(
            "ScalarTypes",
            json!({
                "name": name,
                "i": 1,
                "r": 1.0,
                "b": true,
                "s": "x",
                "u": ["uuid", UUID_1],
                "enum_s": "not-allowed",
                "limited_i": 1,
                "limited_r": 1.0,
                "limited_s": "x"
            }),
            Some("row_uuid"),
        )],
    );

    match result {
        Ok(result) => expect_operation_error(&result, 0)?,
        Err(Error::Validation(_)) => {}
        Err(other) => anyhow::bail!("unexpected error: {other:?}"),
    }

    Ok(())
}

#[test]
fn insert_constraint_violation_fails() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("insert-range-bad");

    let result = tc.client.transact(
        RFC7047_DB,
        vec![ops::insert(
            "ScalarTypes",
            json!({
                "name": name,
                "i": 1,
                "r": 1.0,
                "b": true,
                "s": "x",
                "u": ["uuid", UUID_1],
                "enum_s": "a",
                "limited_i": 999,
                "limited_r": 1.0,
                "limited_s": "x"
            }),
            Some("row_uuid"),
        )],
    );

    match result {
        Ok(result) => expect_operation_error(&result, 0)?,
        Err(Error::Validation(_)) => {}
        Err(other) => anyhow::bail!("unexpected error: {other:?}"),
    }

    Ok(())
}

#[test]
fn select_projected_columns() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("select-projection");

    let result = tc.client.transact(
        RFC7047_DB,
        vec![
            ops::insert("ScalarTypes", scalar_row(&name), Some("row_uuid")),
            ops::select(
                "ScalarTypes",
                &[json!(["name", "==", name])],
                Some(&["name".to_string(), "i".to_string()]),
            ),
        ],
    )?;

    let rows = select_rows(&result, 1)?;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("name").and_then(Value::as_str),
        Some(name.as_str())
    );
    assert_eq!(rows[0].get("i").and_then(Value::as_i64), Some(1));
    assert!(
        !rows[0].contains_key("s"),
        "projection should not include unrequested column: {:?}",
        rows[0]
    );

    Ok(())
}

#[test]
fn select_unknown_column_fails() -> Result<()> {
    let tc = start_custom()?;

    let result = tc.client.transact(
        RFC7047_DB,
        vec![ops::select(
            "ScalarTypes",
            &[],
            Some(&["does_not_exist".to_string()]),
        )],
    );

    match result {
        Ok(result) => expect_operation_error(&result, 0)?,
        Err(Error::Validation(_)) => {}
        Err(other) => anyhow::bail!("unexpected error: {other:?}"),
    }

    Ok(())
}

#[test]
fn update_one_row_returns_count() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("update-one");

    let result = tc.client.transact(
        RFC7047_DB,
        vec![
            ops::insert("ScalarTypes", scalar_row(&name), Some("row_uuid")),
            ops::update(
                "ScalarTypes",
                &[json!(["name", "==", name])],
                json!({"s": "updated"}),
            ),
            ops::select(
                "ScalarTypes",
                &[json!(["name", "==", name])],
                Some(&["s".to_string()]),
            ),
        ],
    )?;

    expect_count(&result, 1, 1)?;

    let rows = select_rows(&result, 2)?;
    assert_eq!(rows[0].get("s").and_then(Value::as_str), Some("updated"));

    Ok(())
}

#[test]
fn update_zero_rows_returns_count_zero() -> Result<()> {
    let tc = start_custom()?;

    let result = tc.client.transact(
        RFC7047_DB,
        vec![ops::update(
            "ScalarTypes",
            &[json!(["name", "==", "missing-row"])],
            json!({"s": "updated"}),
        )],
    )?;

    expect_count(&result, 0, 0)?;

    Ok(())
}

#[test]
fn update_wrong_type_fails() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("update-wrong-type");

    let result = tc.client.transact(
        RFC7047_DB,
        vec![
            ops::insert("ScalarTypes", scalar_row(&name), Some("row_uuid")),
            ops::update(
                "ScalarTypes",
                &[json!(["name", "==", name])],
                json!({"i": "not-an-integer"}),
            ),
        ],
    );

    match result {
        Ok(result) => expect_operation_error(&result, 1)?,
        Err(Error::Validation(_)) => {}
        Err(other) => anyhow::bail!("unexpected error: {other:?}"),
    }

    Ok(())
}

#[test]
fn update_mutable_false_column_fails() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("readonly");

    let result = tc.client.transact(
        RFC7047_DB,
        vec![
            ops::insert(
                "Readonly",
                json!({
                    "name": name.as_str(),
                    "immutable": "initial"
                }),
                Some("row_uuid"),
            ),
            ops::update(
                "Readonly",
                &[json!(["name", "==", name])],
                json!({"immutable": "changed"}),
            ),
        ],
    );

    match result {
        Ok(result) => expect_operation_error(&result, 1)?,
        Err(Error::Validation(_)) => {}
        Err(other) => anyhow::bail!("unexpected error: {other:?}"),
    }

    Ok(())
}

#[test]
fn mutate_integer_add() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("mutate-add");

    let result = tc.client.transact(
        RFC7047_DB,
        vec![
            ops::insert("ScalarTypes", scalar_row(&name), Some("row_uuid")),
            ops::mutate(
                "ScalarTypes",
                &[json!(["name", "==", name])],
                &[json!(["i", "+=", 5])],
            ),
            ops::select(
                "ScalarTypes",
                &[json!(["name", "==", name])],
                Some(&["i".to_string()]),
            ),
        ],
    )?;

    expect_count(&result, 1, 1)?;
    let rows = select_rows(&result, 2)?;
    assert_eq!(rows[0].get("i").and_then(Value::as_i64), Some(6));

    Ok(())
}

#[test]
fn mutate_integer_subtract() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("mutate-sub");

    let mut row = scalar_row(&name);
    row["i"] = json!(10);

    let result = tc.client.transact(
        RFC7047_DB,
        vec![
            ops::insert("ScalarTypes", row, Some("row_uuid")),
            ops::mutate(
                "ScalarTypes",
                &[json!(["name", "==", name])],
                &[json!(["i", "-=", 3])],
            ),
            ops::select(
                "ScalarTypes",
                &[json!(["name", "==", name])],
                Some(&["i".to_string()]),
            ),
        ],
    )?;

    let rows = select_rows(&result, 2)?;
    assert_eq!(rows[0].get("i").and_then(Value::as_i64), Some(7));

    Ok(())
}

#[test]
fn mutate_integer_multiply() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("mutate-mul");

    let mut row = scalar_row(&name);
    row["i"] = json!(4);

    let result = tc.client.transact(
        RFC7047_DB,
        vec![
            ops::insert("ScalarTypes", row, Some("row_uuid")),
            ops::mutate(
                "ScalarTypes",
                &[json!(["name", "==", name])],
                &[json!(["i", "*=", 3])],
            ),
            ops::select(
                "ScalarTypes",
                &[json!(["name", "==", name])],
                Some(&["i".to_string()]),
            ),
        ],
    )?;

    let rows = select_rows(&result, 2)?;
    assert_eq!(rows[0].get("i").and_then(Value::as_i64), Some(12));

    Ok(())
}

#[test]
fn mutate_integer_divide() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("mutate-div");

    let mut row = scalar_row(&name);
    row["i"] = json!(12);

    let result = tc.client.transact(
        RFC7047_DB,
        vec![
            ops::insert("ScalarTypes", row, Some("row_uuid")),
            ops::mutate(
                "ScalarTypes",
                &[json!(["name", "==", name])],
                &[json!(["i", "/=", 3])],
            ),
            ops::select(
                "ScalarTypes",
                &[json!(["name", "==", name])],
                Some(&["i".to_string()]),
            ),
        ],
    )?;

    let rows = select_rows(&result, 2)?;
    assert_eq!(rows[0].get("i").and_then(Value::as_i64), Some(4));

    Ok(())
}

#[test]
fn mutate_integer_modulo() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("mutate-mod");

    let mut row = scalar_row(&name);
    row["i"] = json!(14);

    let result = tc.client.transact(
        RFC7047_DB,
        vec![
            ops::insert("ScalarTypes", row, Some("row_uuid")),
            ops::mutate(
                "ScalarTypes",
                &[json!(["name", "==", name])],
                &[json!(["i", "%=", 5])],
            ),
            ops::select(
                "ScalarTypes",
                &[json!(["name", "==", name])],
                Some(&["i".to_string()]),
            ),
        ],
    )?;

    let rows = select_rows(&result, 2)?;
    assert_eq!(rows[0].get("i").and_then(Value::as_i64), Some(4));

    Ok(())
}

#[test]
fn mutate_set_insert_delete() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("mutate-set");

    let result = tc.client.transact(
        RFC7047_DB,
        vec![
            ops::insert("SetTypes", set_row(&name), Some("row_uuid")),
            ops::mutate(
                "SetTypes",
                &[json!(["name", "==", name])],
                &[json!(["strings", "insert", ["set", ["a", "b"]]])],
            ),
            ops::mutate(
                "SetTypes",
                &[json!(["name", "==", name])],
                &[json!(["strings", "delete", "a"])],
            ),
            ops::select(
                "SetTypes",
                &[json!(["name", "==", name])],
                Some(&["strings".to_string()]),
            ),
        ],
    )?;

    expect_count(&result, 1, 1)?;
    expect_count(&result, 2, 1)?;

    let rows = select_rows(&result, 3)?;
    assert_eq!(rows[0].get("strings"), Some(&json!("b")));

    Ok(())
}

#[test]
fn mutate_map_insert_delete() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("mutate-map");

    let result = tc.client.transact(
        RFC7047_DB,
        vec![
            ops::insert("MapTypes", map_row(&name), Some("row_uuid")),
            ops::mutate(
                "MapTypes",
                &[json!(["name", "==", name])],
                &[json!(["ss", "insert", ["map", [["a", "1"], ["b", "2"]]]])],
            ),
            ops::mutate(
                "MapTypes",
                &[json!(["name", "==", name])],
                &[json!(["ss", "delete", ["set", ["a"]]])],
            ),
            ops::select(
                "MapTypes",
                &[json!(["name", "==", name])],
                Some(&["ss".to_string()]),
            ),
        ],
    )?;

    expect_count(&result, 1, 1)?;
    expect_count(&result, 2, 1)?;

    let rows = select_rows(&result, 3)?;
    assert_eq!(rows[0].get("ss"), Some(&json!(["map", [["b", "2"]]])));

    Ok(())
}

#[test]
fn mutate_divide_by_zero_domain_error() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("mutate-div-zero");

    let result = tc.client.transact(
        RFC7047_DB,
        vec![
            ops::insert("ScalarTypes", scalar_row(&name), Some("row_uuid")),
            ops::mutate(
                "ScalarTypes",
                &[json!(["name", "==", name])],
                &[json!(["i", "/=", 0])],
            ),
        ],
    );

    match result {
        Ok(result) => expect_operation_error(&result, 1)?,
        Err(Error::Validation(_)) => {}
        Err(other) => anyhow::bail!("unexpected error: {other:?}"),
    }

    Ok(())
}

#[test]
fn delete_one_row_returns_count() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("delete-one");

    let result = tc.client.transact(
        RFC7047_DB,
        vec![
            ops::insert("ScalarTypes", scalar_row(&name), Some("row_uuid")),
            ops::delete("ScalarTypes", &[json!(["name", "==", name])]),
            ops::select("ScalarTypes", &[json!(["name", "==", name])], None),
        ],
    )?;

    expect_count(&result, 1, 1)?;

    let rows = select_rows(&result, 2)?;
    assert!(rows.is_empty(), "row should have been deleted: {rows:?}");

    Ok(())
}

#[test]
fn delete_zero_rows_returns_count_zero() -> Result<()> {
    let tc = start_custom()?;

    let result = tc.client.transact(
        RFC7047_DB,
        vec![ops::delete(
            "ScalarTypes",
            &[json!(["name", "==", "missing-row"])],
        )],
    )?;

    expect_count(&result, 0, 0)?;

    Ok(())
}

#[test]
fn wait_equal_already_true() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("wait-eq");

    let result = tc.client.transact(
        RFC7047_DB,
        vec![
            ops::insert("ScalarTypes", scalar_row(&name), Some("row_uuid")),
            ops::wait(
                "ScalarTypes",
                &[json!(["name", "==", name])],
                &["name".to_string()],
                "==",
                &[json!({"name": name})],
                Some(0),
            ),
        ],
    )?;

    assert!(
        matches!(result.get(1), Some(TransactionOutcome::Empty)),
        "wait should succeed with empty result, got {result:?}"
    );

    Ok(())
}

#[test]
fn wait_not_equal_already_true() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("wait-ne");

    let result = tc.client.transact(
        RFC7047_DB,
        vec![
            ops::insert("ScalarTypes", scalar_row(&name), Some("row_uuid")),
            ops::wait(
                "ScalarTypes",
                &[json!(["name", "==", name])],
                &["s".to_string()],
                "!=",
                &[json!({"s": "different"})],
                Some(0),
            ),
        ],
    )?;

    assert!(
        matches!(result.get(1), Some(TransactionOutcome::Empty)),
        "wait != should succeed with empty result, got {result:?}"
    );

    Ok(())
}

#[test]
fn wait_timeout_zero_fails() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("wait-timeout");

    let result = tc.client.transact(
        RFC7047_DB,
        vec![
            ops::insert("ScalarTypes", scalar_row(&name), Some("row_uuid")),
            ops::wait(
                "ScalarTypes",
                &[json!(["name", "==", name])],
                &["s".to_string()],
                "==",
                &[json!({"s": "not-current-value"})],
                Some(0),
            ),
        ],
    )?;

    expect_operation_error(&result, 1)?;

    Ok(())
}

#[test]
fn commit_false_success() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("commit-false");

    let result = tc.client.transact(
        RFC7047_DB,
        vec![
            ops::insert("ScalarTypes", scalar_row(&name), Some("row_uuid")),
            ops::commit(false),
        ],
    )?;

    assert!(
        matches!(result.get(1), Some(TransactionOutcome::Empty)),
        "commit false should return empty result, got {result:?}"
    );

    Ok(())
}

#[test]
fn abort_rolls_back_prior_insert() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("abort");

    let result = tc.client.transact(
        RFC7047_DB,
        vec![
            ops::insert("ScalarTypes", scalar_row(&name), Some("row_uuid")),
            ops::abort(),
        ],
    )?;

    expect_operation_error(&result, 1)?;

    let check = tc.client.transact(
        RFC7047_DB,
        vec![ops::select(
            "ScalarTypes",
            &[json!(["name", "==", name])],
            Some(&["name".to_string()]),
        )],
    )?;

    let rows = select_rows(&check, 0)?;
    assert!(
        rows.is_empty(),
        "abort must roll back prior insert, but row still exists: {rows:?}"
    );

    Ok(())
}

#[test]
fn comment_success() -> Result<()> {
    let tc = start_custom()?;

    let result = tc.client.transact(
        RFC7047_DB,
        vec![ops::comment("rfc7047 transaction comment test")],
    )?;

    assert!(
        matches!(result.get(0), Some(TransactionOutcome::Empty)),
        "comment should return empty result, got {result:?}"
    );

    Ok(())
}

#[test]
fn assert_without_lock_fails() -> Result<()> {
    let tc = start_custom()?;
    let lock_id = unique_name("assert-lock").replace('-', "_");

    let result = tc
        .client
        .transact(RFC7047_DB, vec![ops::assert(&lock_id)])?;

    expect_operation_error(&result, 0)?;

    Ok(())
}

#[test]
fn assert_with_owned_lock_succeeds() -> Result<()> {
    let tc = start_custom()?;
    let lock_id = unique_name("assert-owned").replace('-', "_");

    let locked = tc.client.lock(&lock_id)?;
    assert!(locked, "expected to acquire lock {lock_id}");

    let result = tc
        .client
        .transact(RFC7047_DB, vec![ops::assert(&lock_id)])?;

    assert!(
        matches!(result.get(0), Some(TransactionOutcome::Empty)),
        "assert should succeed when lock is owned, got {result:?}"
    );

    tc.client.unlock(&lock_id)?;

    Ok(())
}
