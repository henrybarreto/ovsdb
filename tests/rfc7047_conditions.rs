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
const UUID_2: &str = "550e8400-e29b-41d4-a716-446655440002";
const UUID_3: &str = "550e8400-e29b-41d4-a716-446655440003";

fn start_custom() -> Result<TestOvsDBClient> {
    TestOvsDBClient::start_with_schema(RFC7047_SCHEMA_PATH, RFC7047_DB)
}

fn scalar_row(
    name: &str,
    integer_value: i64,
    real_value: f64,
    bool_value: bool,
    string_value: &str,
    uuid_value: &str,
) -> Value {
    json!({
        "name": name,
        "i": integer_value,
        "r": real_value,
        "b": bool_value,
        "s": string_value,
        "u": ["uuid", uuid_value],
        "enum_s": "a",
        "limited_i": 1,
        "limited_r": 1.0,
        "limited_s": "ok"
    })
}

fn map_row(name: &str) -> Value {
    json!({
        "name": name,
        "ss": ["map", [["a", "1"], ["b", "2"]]],
        "si": ["map", [["one", 1], ["two", 2]]],
        "su": ["map", [["u1", ["uuid", UUID_1]], ["u2", ["uuid", UUID_2]]]],
        "small_map": ["map", [["required", "value"]]]
    })
}

fn assert_names(actual: Vec<String>, expected: &[&str]) {
    let mut expected = expected
        .iter()
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>();
    let mut actual = actual;
    expected.sort();
    actual.sort();
    assert_eq!(actual, expected);
}

fn insert_scalar_rows(tc: &TestOvsDBClient) -> Result<(String, String, String)> {
    let low = unique_name("cond-scalar-low");
    let mid = unique_name("cond-scalar-mid");
    let high = unique_name("cond-scalar-high");

    tc.client.transact(
        RFC7047_DB,
        vec![
            ops::insert(
                "ScalarTypes",
                scalar_row(&low, 1, 1.5, false, "alpha", UUID_1),
                Some("low_uuid"),
            ),
            ops::insert(
                "ScalarTypes",
                scalar_row(&mid, 5, 5.5, true, "beta", UUID_2),
                Some("mid_uuid"),
            ),
            ops::insert(
                "ScalarTypes",
                scalar_row(&high, 9, 9.5, true, "gamma", UUID_3),
                Some("high_uuid"),
            ),
        ],
    )?;

    Ok((low, mid, high))
}

fn select_names(tc: &TestOvsDBClient, table: &str, conditions: &[Value]) -> Result<Vec<String>> {
    let result = tc.client.transact(
        RFC7047_DB,
        vec![ops::select(table, conditions, Some(&["name".to_string()]))],
    )?;

    let rows = result
        .get(0)
        .and_then(TransactionOutcome::rows)
        .ok_or_else(|| anyhow::anyhow!("missing select rows: {result:?}"))?;

    let mut names = rows
        .iter()
        .filter_map(|row| {
            row.get("name")
                .and_then(Value::as_str)
                .map(ToString::to_string)
        })
        .collect::<Vec<_>>();
    names.sort();
    Ok(names)
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

fn select_with_bad_condition(
    tc: &TestOvsDBClient,
    table: &str,
    conditions: &[Value],
) -> Result<()> {
    let result = tc.client.transact(
        RFC7047_DB,
        vec![ops::select(table, conditions, Some(&["name".to_string()]))],
    );

    match result {
        Ok(response) => expect_operation_error(&response, 0),
        Err(Error::Validation(_)) => Ok(()),
        Err(Error::RpcError(_)) => Ok(()),
        Err(other) => anyhow::bail!("unexpected error type: {other:?}"),
    }
}

#[test]
fn condition_integer_all_comparisons() -> Result<()> {
    let tc = start_custom()?;
    let (low, mid, high) = insert_scalar_rows(&tc)?;

    assert_names(
        select_names(&tc, "ScalarTypes", &[json!(["i", "<", 5])])?,
        &[low.as_str()],
    );
    assert_names(
        select_names(&tc, "ScalarTypes", &[json!(["i", "<=", 5])])?,
        &[low.as_str(), mid.as_str()],
    );
    assert_names(
        select_names(&tc, "ScalarTypes", &[json!(["i", "==", 5])])?,
        &[mid.as_str()],
    );
    assert_names(
        select_names(&tc, "ScalarTypes", &[json!(["i", "!=", 5])])?,
        &[low.as_str(), high.as_str()],
    );
    assert_names(
        select_names(&tc, "ScalarTypes", &[json!(["i", ">=", 5])])?,
        &[mid.as_str(), high.as_str()],
    );
    assert_names(
        select_names(&tc, "ScalarTypes", &[json!(["i", ">", 5])])?,
        &[high.as_str()],
    );
    assert_names(
        select_names(&tc, "ScalarTypes", &[json!(["i", "includes", 5])])?,
        &[mid.as_str()],
    );
    assert_names(
        select_names(&tc, "ScalarTypes", &[json!(["i", "excludes", 5])])?,
        &[low.as_str(), high.as_str()],
    );

    Ok(())
}

#[test]
fn condition_real_all_comparisons() -> Result<()> {
    let tc = start_custom()?;
    let (low, mid, high) = insert_scalar_rows(&tc)?;

    assert_names(
        select_names(&tc, "ScalarTypes", &[json!(["r", "<", 5.5])])?,
        &[low.as_str()],
    );
    assert_names(
        select_names(&tc, "ScalarTypes", &[json!(["r", "<=", 5.5])])?,
        &[low.as_str(), mid.as_str()],
    );
    assert_names(
        select_names(&tc, "ScalarTypes", &[json!(["r", "==", 5.5])])?,
        &[mid.as_str()],
    );
    assert_names(
        select_names(&tc, "ScalarTypes", &[json!(["r", "!=", 5.5])])?,
        &[low.as_str(), high.as_str()],
    );
    assert_names(
        select_names(&tc, "ScalarTypes", &[json!(["r", ">=", 5.5])])?,
        &[mid.as_str(), high.as_str()],
    );
    assert_names(
        select_names(&tc, "ScalarTypes", &[json!(["r", ">", 5.5])])?,
        &[high.as_str()],
    );
    assert_names(
        select_names(&tc, "ScalarTypes", &[json!(["r", "includes", 5.5])])?,
        &[mid.as_str()],
    );
    assert_names(
        select_names(&tc, "ScalarTypes", &[json!(["r", "excludes", 5.5])])?,
        &[low.as_str(), high.as_str()],
    );

    Ok(())
}

#[test]
fn condition_boolean_eq_ne_includes_excludes() -> Result<()> {
    let tc = start_custom()?;
    let (low, mid, high) = insert_scalar_rows(&tc)?;

    assert_names(
        select_names(&tc, "ScalarTypes", &[json!(["b", "==", true])])?,
        &[mid.as_str(), high.as_str()],
    );
    assert_names(
        select_names(&tc, "ScalarTypes", &[json!(["b", "!=", true])])?,
        &[low.as_str()],
    );
    assert_names(
        select_names(&tc, "ScalarTypes", &[json!(["b", "includes", true])])?,
        &[mid.as_str(), high.as_str()],
    );
    assert_names(
        select_names(&tc, "ScalarTypes", &[json!(["b", "excludes", true])])?,
        &[low.as_str()],
    );

    Ok(())
}

#[test]
fn condition_string_eq_ne_includes_excludes() -> Result<()> {
    let tc = start_custom()?;
    let (low, mid, high) = insert_scalar_rows(&tc)?;

    assert_names(
        select_names(&tc, "ScalarTypes", &[json!(["s", "==", "beta"])])?,
        &[mid.as_str()],
    );
    assert_names(
        select_names(&tc, "ScalarTypes", &[json!(["s", "!=", "beta"])])?,
        &[low.as_str(), high.as_str()],
    );
    assert_names(
        select_names(&tc, "ScalarTypes", &[json!(["s", "includes", "beta"])])?,
        &[mid.as_str()],
    );
    assert_names(
        select_names(&tc, "ScalarTypes", &[json!(["s", "excludes", "beta"])])?,
        &[low.as_str(), high.as_str()],
    );

    Ok(())
}

#[test]
fn condition_uuid_eq_ne_includes_excludes() -> Result<()> {
    let tc = start_custom()?;
    let (low, mid, high) = insert_scalar_rows(&tc)?;

    assert_names(
        select_names(&tc, "ScalarTypes", &[json!(["u", "==", ["uuid", UUID_2]])])?,
        &[mid.as_str()],
    );
    assert_names(
        select_names(&tc, "ScalarTypes", &[json!(["u", "!=", ["uuid", UUID_2]])])?,
        &[low.as_str(), high.as_str()],
    );
    assert_names(
        select_names(
            &tc,
            "ScalarTypes",
            &[json!(["u", "includes", ["uuid", UUID_2]])],
        )?,
        &[mid.as_str()],
    );
    assert_names(
        select_names(
            &tc,
            "ScalarTypes",
            &[json!(["u", "excludes", ["uuid", UUID_2]])],
        )?,
        &[low.as_str(), high.as_str()],
    );

    Ok(())
}

#[test]
fn condition_set_eq_ne_includes_excludes() -> Result<()> {
    let tc = start_custom()?;

    let a = unique_name("cond-set-a");
    let b = unique_name("cond-set-b");

    tc.client.transact(
        RFC7047_DB,
        vec![
            ops::insert(
                "SetTypes",
                json!({
                    "name": a.as_str(),
                    "ints": ["set", [1, 2, 3]],
                    "strings": ["set", ["alpha", "beta"]],
                    "uuids": ["set", [["uuid", UUID_1], ["uuid", UUID_2]]],
                    "small_set": ["set", ["x", "y"]]
                }),
                Some("set_a"),
            ),
            ops::insert(
                "SetTypes",
                json!({
                    "name": b.as_str(),
                    "ints": ["set", [9]],
                    "strings": "gamma",
                    "uuids": ["uuid", UUID_3],
                    "small_set": "z"
                }),
                Some("set_b"),
            ),
        ],
    )?;

    assert_names(
        select_names(
            &tc,
            "SetTypes",
            &[json!(["strings", "==", ["set", ["alpha", "beta"]]])],
        )?,
        &[a.as_str()],
    );
    assert_names(
        select_names(
            &tc,
            "SetTypes",
            &[json!(["strings", "!=", ["set", ["alpha", "beta"]]])],
        )?,
        &[b.as_str()],
    );
    assert_names(
        select_names(&tc, "SetTypes", &[json!(["strings", "includes", "alpha"])])?,
        &[a.as_str()],
    );
    assert_names(
        select_names(
            &tc,
            "SetTypes",
            &[json!(["strings", "includes", ["set", ["alpha", "beta"]]])],
        )?,
        &[a.as_str()],
    );
    assert_names(
        select_names(&tc, "SetTypes", &[json!(["strings", "excludes", "alpha"])])?,
        &[b.as_str()],
    );
    assert_names(
        select_names(
            &tc,
            "SetTypes",
            &[json!(["strings", "excludes", ["set", ["not-present"]]])],
        )?,
        &[a.as_str(), b.as_str()],
    );

    Ok(())
}

#[test]
fn condition_map_eq_ne_includes_excludes() -> Result<()> {
    let tc = start_custom()?;

    let a = unique_name("cond-map-a");
    let b = unique_name("cond-map-b");

    tc.client.transact(
        RFC7047_DB,
        vec![
            ops::insert("MapTypes", map_row(&a), Some("map_a")),
            ops::insert(
                "MapTypes",
                json!({
                    "name": b.as_str(),
                    "ss": ["map", [["c", "3"]]],
                    "si": ["map", [["three", 3]]],
                    "su": ["map", [["u3", ["uuid", UUID_3]]]],
                    "small_map": ["map", [["required", "value"]]]
                }),
                Some("map_b"),
            ),
        ],
    )?;

    assert_names(
        select_names(
            &tc,
            "MapTypes",
            &[json!(["ss", "==", ["map", [["a", "1"], ["b", "2"]]]])],
        )?,
        &[a.as_str()],
    );
    assert_names(
        select_names(
            &tc,
            "MapTypes",
            &[json!(["ss", "!=", ["map", [["a", "1"], ["b", "2"]]]])],
        )?,
        &[b.as_str()],
    );
    assert_names(
        select_names(
            &tc,
            "MapTypes",
            &[json!(["ss", "includes", ["map", [["a", "1"]]]])],
        )?,
        &[a.as_str()],
    );
    assert_names(
        select_names(
            &tc,
            "MapTypes",
            &[json!(["ss", "excludes", ["map", [["a", "1"]]]])],
        )?,
        &[b.as_str()],
    );
    assert_names(
        select_names(
            &tc,
            "MapTypes",
            &[json!(["ss", "excludes", ["map", [["not", "present"]]]])],
        )?,
        &[a.as_str(), b.as_str()],
    );

    Ok(())
}

#[test]
fn condition_multiple_conditions_are_anded() -> Result<()> {
    let tc = start_custom()?;
    let (_low, mid, _high) = insert_scalar_rows(&tc)?;

    let names = select_names(
        &tc,
        "ScalarTypes",
        &[
            json!(["i", ">=", 5]),
            json!(["b", "==", true]),
            json!(["s", "!=", "gamma"]),
        ],
    )?;

    assert_names(names, &[mid.as_str()]);

    let no_names = select_names(
        &tc,
        "ScalarTypes",
        &[
            json!(["i", ">=", 5]),
            json!(["b", "==", true]),
            json!(["s", "==", "does-not-exist"]),
        ],
    )?;

    assert!(no_names.is_empty(), "expected no rows, got {no_names:?}");

    Ok(())
}

#[test]
fn condition_invalid_operator_fails() -> Result<()> {
    let tc = start_custom()?;
    select_with_bad_condition(&tc, "ScalarTypes", &[json!(["i", "not-an-operator", 1])])
}

#[test]
fn condition_wrong_length_fails() -> Result<()> {
    let tc = start_custom()?;

    select_with_bad_condition(&tc, "ScalarTypes", &[json!(["i", "=="])])?;
    select_with_bad_condition(&tc, "ScalarTypes", &[json!(["i", "==", 1, "extra"])])
}

#[test]
fn condition_unknown_column_fails() -> Result<()> {
    let tc = start_custom()?;
    select_with_bad_condition(&tc, "ScalarTypes", &[json!(["does_not_exist", "==", 1])])
}

#[test]
fn condition_wrong_type_fails() -> Result<()> {
    let tc = start_custom()?;

    select_with_bad_condition(&tc, "ScalarTypes", &[json!(["i", "==", "not-an-integer"])])?;
    select_with_bad_condition(&tc, "MapTypes", &[json!(["ss", "includes", "not-a-map"])])
}

#[test]
fn condition_lt_on_boolean_fails() -> Result<()> {
    let tc = start_custom()?;
    select_with_bad_condition(&tc, "ScalarTypes", &[json!(["b", "<", true])])
}

#[test]
fn condition_lt_on_string_fails() -> Result<()> {
    let tc = start_custom()?;
    select_with_bad_condition(&tc, "ScalarTypes", &[json!(["s", "<", "abc"])])
}

#[test]
fn condition_lt_on_uuid_fails() -> Result<()> {
    let tc = start_custom()?;
    select_with_bad_condition(&tc, "ScalarTypes", &[json!(["u", "<", ["uuid", UUID_1]])])
}
