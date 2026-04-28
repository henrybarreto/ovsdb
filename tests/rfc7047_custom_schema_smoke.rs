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
use ovsdb::client::ops::Ops;
use serde_json::{json, Value};
use support::{unique_name, TestOvsDBClient};

const RFC7047_SCHEMA_PATH: &str = "tests/schemas/rfc7047_compliance.ovsschema";
const RFC7047_DB: &str = "RFC7047_Test";

#[test]
fn custom_schema_server_starts() -> Result<()> {
    let tc = TestOvsDBClient::start_with_schema(RFC7047_SCHEMA_PATH, RFC7047_DB)?;

    assert!(!tc.host.is_empty());
    assert!(tc.port > 0);
    assert!(tc.addr.starts_with("tcp:"));

    Ok(())
}

#[test]
fn custom_schema_list_dbs_contains_rfc7047_test() -> Result<()> {
    let tc = TestOvsDBClient::start_with_schema(RFC7047_SCHEMA_PATH, RFC7047_DB)?;

    let dbs = tc.client.list_dbs()?;

    assert!(
        dbs.iter().any(|db| db == RFC7047_DB),
        "expected {RFC7047_DB} in list_dbs, got {dbs:?}"
    );

    Ok(())
}

#[test]
fn custom_schema_get_schema_parses_tables() -> Result<()> {
    let tc = TestOvsDBClient::start_with_schema(RFC7047_SCHEMA_PATH, RFC7047_DB)?;

    let schema = tc.client.get_schema(RFC7047_DB)?;

    for table in [
        "ScalarTypes",
        "SetTypes",
        "MapTypes",
        "Root",
        "NonRoot",
        "StrongParent",
        "StrongChild",
        "WeakParent",
        "WeakChild",
        "Indexed",
        "MaxRows",
        "Readonly",
        "Ephemeral",
    ] {
        assert!(
            schema.tables.contains_key(table),
            "expected table {table} in schema"
        );
    }

    let scalar = schema
        .tables
        .get("ScalarTypes")
        .expect("ScalarTypes table missing");

    for column in [
        "name",
        "i",
        "r",
        "b",
        "s",
        "u",
        "enum_s",
        "limited_i",
        "limited_r",
        "limited_s",
    ] {
        assert!(
            scalar.columns.contains_key(column),
            "expected ScalarTypes column {column}"
        );
    }

    Ok(())
}

#[test]
fn custom_schema_typed_insert_then_select_scalar_row() -> Result<()> {
    let tc = TestOvsDBClient::start_with_schema(RFC7047_SCHEMA_PATH, RFC7047_DB)?;

    let row_name = unique_name("scalar-select");

    let insert = tc.client.transact(
        RFC7047_DB,
        vec![Ops::insert(
            "ScalarTypes",
            json!({
                "name": row_name.as_str(),
                "i": 1,
                "r": 1.25,
                "b": false,
                "s": "abc",
                "u": ["uuid", "550e8400-e29b-41d4-a716-446655440001"],
                "enum_s": "b",
                "limited_i": 3,
                "limited_r": 4.5,
                "limited_s": "value"
            }),
            Some("row_uuid"),
        )],
    )?;

    assert!(
        insert.get(0).and_then(|outcome| outcome.uuid()).is_some(),
        "unexpected insert result: {insert:?}"
    );

    let select = tc.client.transact(
        RFC7047_DB,
        vec![Ops::select(
            "ScalarTypes",
            &[json!(["name", "==", row_name.as_str()])],
            Some(&["name".to_string(), "i".to_string(), "enum_s".to_string()]),
        )],
    )?;

    let rows = select
        .get(0)
        .and_then(|outcome| outcome.rows())
        .ok_or_else(|| anyhow::anyhow!("missing select rows in result: {select:?}"))?;

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("name").and_then(Value::as_str),
        Some(row_name.as_str())
    );
    assert_eq!(rows[0].get("i").and_then(Value::as_i64), Some(1));
    assert_eq!(rows[0].get("enum_s").and_then(Value::as_str), Some("b"));

    Ok(())
}

#[test]
fn custom_schema_raw_transact_insert_scalar_row() -> Result<()> {
    let tc = TestOvsDBClient::start_with_schema(RFC7047_SCHEMA_PATH, RFC7047_DB)?;

    let row_name = unique_name("raw-scalar");
    let mut stream = tc.raw_tcp_stream()?;

    support::send_raw_json(
        &mut stream,
        &json!({
            "method": "transact",
            "params": [
                RFC7047_DB,
                {
                    "op": "insert",
                    "table": "ScalarTypes",
                    "row": {
                        "name": row_name,
                        "i": 2,
                        "r": 2.5,
                        "b": true,
                        "s": "raw",
                        "u": ["uuid", "550e8400-e29b-41d4-a716-446655440002"],
                        "enum_s": "c",
                        "limited_i": 4,
                        "limited_r": 4.5,
                        "limited_s": "raw"
                    },
                    "uuid-name": "raw_uuid"
                }
            ],
            "id": 1
        }),
    )?;

    let response = support::read_raw_json(&mut stream)?;

    assert_eq!(response.get("error"), Some(&json!(null)));
    assert_eq!(response.get("id"), Some(&json!(1)));

    let result = response
        .get("result")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("expected result array, got {response:?}"))?;

    assert_eq!(result.len(), 1);
    assert!(
        result[0].get("uuid").is_some(),
        "expected insert uuid result, got {response:?}"
    );

    Ok(())
}
