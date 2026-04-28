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
use ovsdb::client::error::Error;
use ovsdb::client::ops::Ops as ops;
use ovsdb::client::{Row, TransactionOutcome, TransactionResponse};
use serde_json::{json, Value};

use support::{unique_name, TestOvsDBClient};

const RFC7047_SCHEMA_PATH: &str = "tests/schemas/rfc7047_compliance.ovsschema";
const RFC7047_DB: &str = "RFC7047_Test";

const UUID_1: &str = "550e8400-e29b-41d4-a716-446655440001";
const UUID_2: &str = "550e8400-e29b-41d4-a716-446655440002";

fn start_custom() -> Result<TestOvsDBClient> {
    TestOvsDBClient::start_with_schema(RFC7047_SCHEMA_PATH, RFC7047_DB)
}

fn select_rows(result: &TransactionResponse, index: usize) -> Result<&Vec<Row>> {
    result
        .get(index)
        .and_then(TransactionOutcome::rows)
        .ok_or_else(|| anyhow::anyhow!("missing select rows at index {index}: {result:?}"))
}

fn assert_transact_error_or_validation(
    result: std::result::Result<TransactionResponse, Error>,
) -> Result<()> {
    match result {
        Ok(response) => {
            let outcome = response
                .get(0)
                .context("expected transact result entry for bad notation")?;
            if outcome.error().is_some() {
                Ok(())
            } else {
                anyhow::bail!("expected transaction operation error, got {response:?}");
            }
        }
        Err(Error::Validation(_)) => Ok(()),
        Err(Error::RpcError(_)) => Ok(()),
        Err(other) => anyhow::bail!("expected validation or RPC error, got {other:?}"),
    }
}

fn json_key(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_default()
}

fn set_members(value: &Value) -> Result<Vec<Value>> {
    if let Some(arr) = value.as_array() {
        if arr.first() == Some(&json!("set")) {
            let inner = arr
                .get(1)
                .and_then(Value::as_array)
                .context("set payload must be array")?;
            return Ok(inner.clone());
        }
    }

    Ok(vec![value.clone()])
}

fn assert_ovsdb_set_eq(actual: &Value, expected: &[Value]) -> Result<()> {
    let mut actual = set_members(actual)?;
    let mut expected = expected.to_vec();
    actual.sort_by_key(|a| json_key(a));
    expected.sort_by_key(|a| json_key(a));
    assert_eq!(actual, expected);
    Ok(())
}

fn map_pairs(value: &Value) -> Result<Vec<(Value, Value)>> {
    let arr = value.as_array().context("map must be JSON array")?;
    if arr.len() != 2 || arr.first() != Some(&json!("map")) {
        anyhow::bail!("expected OVSDB map, got {value:?}");
    }

    let pairs = arr
        .get(1)
        .and_then(Value::as_array)
        .context("map payload must be array")?;

    pairs
        .iter()
        .map(|pair| {
            let p = pair.as_array().context("map pair must be array")?;
            if p.len() != 2 {
                anyhow::bail!("map pair must have length 2: {pair:?}");
            }
            Ok((p[0].clone(), p[1].clone()))
        })
        .collect()
}

fn assert_ovsdb_map_eq(actual: &Value, expected: &[(Value, Value)]) -> Result<()> {
    let mut actual = map_pairs(actual)?;
    let mut expected = expected.to_vec();
    actual.sort_by(|a, b| {
        let left = format!("{}={}", json_key(&a.0), json_key(&a.1));
        let right = format!("{}={}", json_key(&b.0), json_key(&b.1));
        left.cmp(&right)
    });
    expected.sort_by(|a, b| {
        let left = format!("{}={}", json_key(&a.0), json_key(&a.1));
        let right = format!("{}={}", json_key(&b.0), json_key(&b.1));
        left.cmp(&right)
    });
    assert_eq!(actual, expected);
    Ok(())
}

#[test]
fn atom_string_round_trip() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("atom-string");

    let result = tc.client.transact(
        RFC7047_DB,
        vec![
            ops::insert(
                "ScalarTypes",
                json!({
                    "name": name.as_str(),
                    "i": 1,
                    "r": 1.5,
                    "b": true,
                    "s": "hello",
                    "u": ["uuid", UUID_1],
                    "enum_s": "a",
                    "limited_i": 1,
                    "limited_r": 1.5,
                    "limited_s": "ok"
                }),
                Some("row_uuid"),
            ),
            ops::select(
                "ScalarTypes",
                &[json!(["name", "==", name])],
                Some(&["s".to_string()]),
            ),
        ],
    )?;

    let rows = select_rows(&result, 1)?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("s").and_then(Value::as_str), Some("hello"));

    Ok(())
}

#[test]
fn atom_integer_round_trip() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("atom-int");

    let result = tc.client.transact(
        RFC7047_DB,
        vec![
            ops::insert(
                "ScalarTypes",
                json!({
                    "name": name.as_str(),
                    "i": 42,
                    "r": 1.0,
                    "b": true,
                    "s": "x",
                    "u": ["uuid", UUID_1],
                    "enum_s": "a",
                    "limited_i": 10,
                    "limited_r": 1.0,
                    "limited_s": "x"
                }),
                Some("row_uuid"),
            ),
            ops::select(
                "ScalarTypes",
                &[json!(["name", "==", name])],
                Some(&["i".to_string(), "limited_i".to_string()]),
            ),
        ],
    )?;

    let rows = select_rows(&result, 1)?;
    assert_eq!(rows[0].get("i").and_then(Value::as_i64), Some(42));
    assert_eq!(rows[0].get("limited_i").and_then(Value::as_i64), Some(10));

    Ok(())
}

#[test]
fn atom_real_round_trip() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("atom-real");

    let result = tc.client.transact(
        RFC7047_DB,
        vec![
            ops::insert(
                "ScalarTypes",
                json!({
                    "name": name.as_str(),
                    "i": 1,
                    "r": 3.25,
                    "b": true,
                    "s": "x",
                    "u": ["uuid", UUID_1],
                    "enum_s": "a",
                    "limited_i": 1,
                    "limited_r": 3.25,
                    "limited_s": "x"
                }),
                Some("row_uuid"),
            ),
            ops::select(
                "ScalarTypes",
                &[json!(["name", "==", name])],
                Some(&["r".to_string(), "limited_r".to_string()]),
            ),
        ],
    )?;

    let rows = select_rows(&result, 1)?;
    assert_eq!(rows[0].get("r").and_then(Value::as_f64), Some(3.25));
    assert_eq!(rows[0].get("limited_r").and_then(Value::as_f64), Some(3.25));

    Ok(())
}

#[test]
fn atom_boolean_round_trip() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("atom-bool");

    let result = tc.client.transact(
        RFC7047_DB,
        vec![
            ops::insert(
                "ScalarTypes",
                json!({
                    "name": name.as_str(),
                    "i": 1,
                    "r": 1.0,
                    "b": false,
                    "s": "x",
                    "u": ["uuid", UUID_1],
                    "enum_s": "a",
                    "limited_i": 1,
                    "limited_r": 1.0,
                    "limited_s": "x"
                }),
                Some("row_uuid"),
            ),
            ops::select(
                "ScalarTypes",
                &[json!(["name", "==", name])],
                Some(&["b".to_string()]),
            ),
        ],
    )?;

    let rows = select_rows(&result, 1)?;
    assert_eq!(rows[0].get("b").and_then(Value::as_bool), Some(false));

    Ok(())
}

#[test]
fn uuid_round_trip() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("uuid");

    let result = tc.client.transact(
        RFC7047_DB,
        vec![
            ops::insert(
                "ScalarTypes",
                json!({
                    "name": name.as_str(),
                    "i": 1,
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
            ),
            ops::select(
                "ScalarTypes",
                &[json!(["name", "==", name])],
                Some(&["u".to_string()]),
            ),
        ],
    )?;

    let rows = select_rows(&result, 1)?;
    assert_eq!(rows[0].get("u"), Some(&json!(["uuid", UUID_1])));

    Ok(())
}

#[test]
fn empty_set_round_trip() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("empty-set");

    let result = tc.client.transact(
        RFC7047_DB,
        vec![
            ops::insert(
                "SetTypes",
                json!({
                    "name": name.as_str(),
                    "ints": ["set", []],
                    "strings": ["set", []],
                    "uuids": ["set", []],
                    "small_set": "required"
                }),
                Some("row_uuid"),
            ),
            ops::select(
                "SetTypes",
                &[json!(["name", "==", name])],
                Some(&[
                    "ints".to_string(),
                    "strings".to_string(),
                    "uuids".to_string(),
                ]),
            ),
        ],
    )?;

    let rows = select_rows(&result, 1)?;
    assert_ovsdb_set_eq(rows[0].get("ints").context("missing ints")?, &[])?;
    assert_ovsdb_set_eq(rows[0].get("strings").context("missing strings")?, &[])?;
    assert_ovsdb_set_eq(rows[0].get("uuids").context("missing uuids")?, &[])?;

    Ok(())
}

#[test]
fn singleton_set_as_atom_round_trip() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("singleton-set");

    let result = tc.client.transact(
        RFC7047_DB,
        vec![
            ops::insert(
                "SetTypes",
                json!({
                    "name": name.as_str(),
                    "ints": 7,
                    "strings": "one",
                    "uuids": ["uuid", UUID_1],
                    "small_set": "required"
                }),
                Some("row_uuid"),
            ),
            ops::select(
                "SetTypes",
                &[json!(["name", "==", name])],
                Some(&[
                    "ints".to_string(),
                    "strings".to_string(),
                    "uuids".to_string(),
                ]),
            ),
        ],
    )?;

    let rows = select_rows(&result, 1)?;
    assert_ovsdb_set_eq(rows[0].get("ints").context("missing ints")?, &[json!(7)])?;
    assert_ovsdb_set_eq(
        rows[0].get("strings").context("missing strings")?,
        &[json!("one")],
    )?;
    assert_ovsdb_set_eq(
        rows[0].get("uuids").context("missing uuids")?,
        &[json!(["uuid", UUID_1])],
    )?;

    Ok(())
}

#[test]
fn explicit_set_round_trip() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("explicit-set");

    let result = tc.client.transact(
        RFC7047_DB,
        vec![
            ops::insert(
                "SetTypes",
                json!({
                    "name": name.as_str(),
                    "ints": ["set", [1, 2, 3]],
                    "strings": ["set", ["a", "b"]],
                    "uuids": ["set", [["uuid", UUID_1], ["uuid", UUID_2]]],
                    "small_set": ["set", ["x", "y"]]
                }),
                Some("row_uuid"),
            ),
            ops::select(
                "SetTypes",
                &[json!(["name", "==", name])],
                Some(&[
                    "ints".to_string(),
                    "strings".to_string(),
                    "uuids".to_string(),
                    "small_set".to_string(),
                ]),
            ),
        ],
    )?;

    let rows = select_rows(&result, 1)?;
    assert_ovsdb_set_eq(
        rows[0].get("ints").context("missing ints")?,
        &[json!(1), json!(2), json!(3)],
    )?;
    assert_ovsdb_set_eq(
        rows[0].get("strings").context("missing strings")?,
        &[json!("a"), json!("b")],
    )?;
    assert_ovsdb_set_eq(
        rows[0].get("uuids").context("missing uuids")?,
        &[json!(["uuid", UUID_1]), json!(["uuid", UUID_2])],
    )?;
    assert_ovsdb_set_eq(
        rows[0].get("small_set").context("missing small_set")?,
        &[json!("x"), json!("y")],
    )?;

    Ok(())
}

#[test]
fn empty_map_round_trip() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("empty-map");

    let result = tc.client.transact(
        RFC7047_DB,
        vec![
            ops::insert(
                "MapTypes",
                json!({
                    "name": name.as_str(),
                    "ss": ["map", []],
                    "si": ["map", []],
                    "su": ["map", []],
                    "small_map": ["map", [["required", "value"]]]
                }),
                Some("row_uuid"),
            ),
            ops::select(
                "MapTypes",
                &[json!(["name", "==", name])],
                Some(&["ss".to_string(), "si".to_string(), "su".to_string()]),
            ),
        ],
    )?;

    let rows = select_rows(&result, 1)?;
    assert_ovsdb_map_eq(rows[0].get("ss").context("missing ss")?, &[])?;
    assert_ovsdb_map_eq(rows[0].get("si").context("missing si")?, &[])?;
    assert_ovsdb_map_eq(rows[0].get("su").context("missing su")?, &[])?;

    Ok(())
}

#[test]
fn map_round_trip() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("map");

    let result = tc.client.transact(
        RFC7047_DB,
        vec![
            ops::insert(
                "MapTypes",
                json!({
                    "name": name.as_str(),
                    "ss": ["map", [["k1", "v1"], ["k2", "v2"]]],
                    "si": ["map", [["one", 1], ["two", 2]]],
                    "su": ["map", [["u1", ["uuid", UUID_1]], ["u2", ["uuid", UUID_2]]]],
                    "small_map": ["map", [["required", "value"]]]
                }),
                Some("row_uuid"),
            ),
            ops::select(
                "MapTypes",
                &[json!(["name", "==", name])],
                Some(&["ss".to_string(), "si".to_string(), "su".to_string()]),
            ),
        ],
    )?;

    let rows = select_rows(&result, 1)?;
    assert_ovsdb_map_eq(
        rows[0].get("ss").context("missing ss")?,
        &[(json!("k1"), json!("v1")), (json!("k2"), json!("v2"))],
    )?;
    assert_ovsdb_map_eq(
        rows[0].get("si").context("missing si")?,
        &[(json!("one"), json!(1)), (json!("two"), json!(2))],
    )?;
    assert_ovsdb_map_eq(
        rows[0].get("su").context("missing su")?,
        &[
            (json!("u1"), json!(["uuid", UUID_1])),
            (json!("u2"), json!(["uuid", UUID_2])),
        ],
    )?;

    Ok(())
}

#[test]
fn named_uuid_reference_round_trip() -> Result<()> {
    let tc = start_custom()?;
    let parent_name = unique_name("parent");
    let child_name = unique_name("child");

    let result = tc.client.transact(
        RFC7047_DB,
        vec![
            ops::insert(
                "StrongParent",
                json!({
                    "name": parent_name.as_str()
                }),
                Some("new_parent"),
            ),
            ops::insert(
                "StrongChild",
                json!({
                    "name": child_name.as_str(),
                    "parent": ["named-uuid", "new_parent"]
                }),
                Some("new_child"),
            ),
            ops::select(
                "StrongChild",
                &[json!(["name", "==", child_name])],
                Some(&["name".to_string(), "parent".to_string()]),
            ),
        ],
    )?;

    let parent_uuid = result
        .get(0)
        .and_then(TransactionOutcome::uuid)
        .cloned()
        .context("expected parent insert uuid")?;

    let rows = select_rows(&result, 2)?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("parent"), Some(&json!(["uuid", parent_uuid])));

    Ok(())
}

#[test]
fn named_uuid_reference_inside_set() -> Result<()> {
    let tc = start_custom()?;
    let parent_name = unique_name("set-parent");
    let set_name = unique_name("set-named-uuid");

    let result = tc.client.transact(
        RFC7047_DB,
        vec![
            ops::insert(
                "WeakParent",
                json!({
                    "name": parent_name
                }),
                Some("new_uuid_for_set"),
            ),
            ops::insert(
                "SetTypes",
                json!({
                    "name": set_name.as_str(),
                    "ints": ["set", []],
                    "strings": ["set", []],
                    "uuids": ["set", [["named-uuid", "new_uuid_for_set"]]],
                    "small_set": "required"
                }),
                Some("set_row"),
            ),
            ops::select(
                "SetTypes",
                &[json!(["name", "==", set_name])],
                Some(&["uuids".to_string()]),
            ),
        ],
    )?;

    let inserted_uuid = result
        .get(0)
        .and_then(TransactionOutcome::uuid)
        .cloned()
        .context("expected inserted uuid")?;

    let rows = select_rows(&result, 2)?;
    assert_ovsdb_set_eq(
        rows[0].get("uuids").context("missing uuids")?,
        &[json!(["uuid", inserted_uuid])],
    )?;

    Ok(())
}

#[test]
fn named_uuid_reference_inside_map() -> Result<()> {
    let tc = start_custom()?;
    let parent_name = unique_name("map-parent");
    let map_name = unique_name("map-named-uuid");

    let result = tc.client.transact(
        RFC7047_DB,
        vec![
            ops::insert(
                "WeakParent",
                json!({
                    "name": parent_name
                }),
                Some("new_uuid_for_map"),
            ),
            ops::insert(
                "MapTypes",
                json!({
                    "name": map_name.as_str(),
                    "ss": ["map", []],
                    "si": ["map", []],
                    "su": ["map", [["ref", ["named-uuid", "new_uuid_for_map"]]]],
                    "small_map": ["map", [["required", "value"]]]
                }),
                Some("map_row"),
            ),
            ops::select(
                "MapTypes",
                &[json!(["name", "==", map_name])],
                Some(&["su".to_string()]),
            ),
        ],
    )?;

    let inserted_uuid = result
        .get(0)
        .and_then(TransactionOutcome::uuid)
        .cloned()
        .context("expected inserted uuid")?;

    let rows = select_rows(&result, 2)?;
    assert_ovsdb_map_eq(
        rows[0].get("su").context("missing su")?,
        &[(json!("ref"), json!(["uuid", inserted_uuid]))],
    )?;

    Ok(())
}

#[test]
fn bad_uuid_rejected() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("bad-uuid");

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
                "u": ["uuid", 1],
                "enum_s": "a",
                "limited_i": 1,
                "limited_r": 1.0,
                "limited_s": "x"
            }),
            Some("row_uuid"),
        )],
    );

    assert_transact_error_or_validation(result)?;

    Ok(())
}

#[test]
fn bad_set_rejected() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("bad-set");

    let result = tc.client.transact(
        RFC7047_DB,
        vec![ops::insert(
            "SetTypes",
            json!({
                "name": name,
                "ints": ["set", 1],
                "strings": ["set", []],
                "uuids": ["set", []],
                "small_set": "required"
            }),
            Some("row_uuid"),
        )],
    );

    assert_transact_error_or_validation(result)?;

    Ok(())
}

#[test]
fn bad_map_rejected() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("bad-map");

    let result = tc.client.transact(
        RFC7047_DB,
        vec![ops::insert(
            "MapTypes",
            json!({
                "name": name,
                "ss": ["map", [["k"]]],
                "si": ["map", []],
                "su": ["map", []],
                "small_map": ["map", [["required", "value"]]]
            }),
            Some("row_uuid"),
        )],
    );

    assert_transact_error_or_validation(result)?;

    Ok(())
}

#[test]
fn unknown_named_uuid_rejected() -> Result<()> {
    let tc = start_custom()?;
    let child_name = unique_name("unknown-named-uuid");

    let result = tc.client.transact(
        RFC7047_DB,
        vec![ops::insert(
            "StrongChild",
            json!({
                "name": child_name,
                "parent": ["named-uuid", "does_not_exist"]
            }),
            Some("child_uuid"),
        )],
    );

    assert_transact_error_or_validation(result)?;

    Ok(())
}

#[test]
fn map_encoded_as_json_object_rejected() -> Result<()> {
    let tc = start_custom()?;
    let name = unique_name("json-object-map");

    let result = tc.client.transact(
        RFC7047_DB,
        vec![ops::insert(
            "MapTypes",
            json!({
                "name": name,
                "ss": {"k": "v"},
                "si": ["map", []],
                "su": ["map", []],
                "small_map": ["map", [["required", "value"]]]
            }),
            Some("row_uuid"),
        )],
    );

    assert_transact_error_or_validation(result)?;

    Ok(())
}
