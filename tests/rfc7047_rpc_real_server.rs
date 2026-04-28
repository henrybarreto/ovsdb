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
use serde_json::json;

use support::TestOvsDBClient;

const OPEN_VSWITCH_DB: &str = "Open_vSwitch";
const UNKNOWN_DB: &str = "does_not_exist";

#[test]
fn list_dbs_success() -> Result<()> {
    let tc = TestOvsDBClient::start_plain()?;

    let dbs = tc.client.list_dbs()?;

    assert!(
        dbs.iter().any(|db| db == OPEN_VSWITCH_DB),
        "expected {OPEN_VSWITCH_DB} in list_dbs result, got {dbs:?}"
    );

    Ok(())
}

#[test]
fn list_dbs_returns_non_empty_strings() -> Result<()> {
    let tc = TestOvsDBClient::start_plain()?;

    let dbs = tc.client.list_dbs()?;

    assert!(!dbs.is_empty(), "expected at least one database");
    assert!(
        dbs.iter().all(|db| !db.trim().is_empty()),
        "database names must be non-empty strings, got {dbs:?}"
    );

    Ok(())
}

#[test]
fn get_schema_success() -> Result<()> {
    let tc = TestOvsDBClient::start_plain()?;

    let schema = tc.client.get_schema(OPEN_VSWITCH_DB)?;

    assert_eq!(schema.name, OPEN_VSWITCH_DB);
    assert!(
        !schema.version.is_empty(),
        "schema version should not be empty"
    );
    assert!(
        schema.tables.contains_key("Open_vSwitch"),
        "expected Open_vSwitch table, got tables: {:?}",
        schema.tables.keys().collect::<Vec<_>>()
    );

    Ok(())
}

#[test]
fn get_schema_unknown_database_returns_rpc_error() -> Result<()> {
    let tc = TestOvsDBClient::start_plain()?;

    let err = tc.client.get_schema(UNKNOWN_DB);

    match err {
        Err(Error::RpcError(rpc_err)) => {
            assert_eq!(rpc_err.error, "unknown database");
        }
        other => {
            anyhow::bail!("expected unknown database RpcError, got {other:?}");
        }
    }

    Ok(())
}

#[test]
fn echo_round_trip_string() -> Result<()> {
    let tc = TestOvsDBClient::start_plain()?;

    let result = tc.client.echo("hello-rfc7047")?;

    assert_eq!(result, "hello-rfc7047");

    Ok(())
}

#[test]
fn echo_arbitrary_json_params() -> Result<()> {
    let tc = TestOvsDBClient::start_plain()?;

    let payload = json!([
        {
            "name": "ovsdb",
            "enabled": true,
            "count": 3,
            "nested": [1, 2, {"x": null}]
        }
    ]);

    let result = tc.client.request("echo", &payload)?;

    assert_eq!(result, payload);

    Ok(())
}

#[test]
fn transact_empty_transaction_success() -> Result<()> {
    let tc = TestOvsDBClient::start_plain()?;

    let result = tc.client.transact(OPEN_VSWITCH_DB, vec![])?;

    assert!(
        result.entries.is_empty(),
        "empty transact should return an empty result array"
    );

    Ok(())
}

#[test]
fn transact_unknown_database_returns_rpc_error() -> Result<()> {
    let tc = TestOvsDBClient::start_plain()?;

    let err = tc.client.request("transact", &json!([UNKNOWN_DB]));

    match err {
        Err(Error::RpcError(rpc_err)) => {
            assert_eq!(rpc_err.error, "unknown database");
        }
        other => {
            anyhow::bail!("expected unknown database RpcError, got {other:?}");
        }
    }

    Ok(())
}

#[test]
fn transact_unknown_operation_is_operation_error() -> Result<()> {
    let tc = TestOvsDBClient::start_plain()?;

    let result = tc.client.request(
        "transact",
        &json!([
            OPEN_VSWITCH_DB,
            {
                "op": "not-a-real-operation"
            }
        ]),
    );

    match result {
        Err(Error::Validation(_)) => {}
        Ok(value) => {
            let entries = value
                .as_array()
                .context("transact result should be an array")?;
            assert_eq!(entries.len(), 1);
            assert!(
                entries[0].get("error").is_some(),
                "unknown operation should produce operation error, got {value:?}"
            );
        }
        Err(other) => {
            anyhow::bail!("unexpected error for unknown operation: {other:?}");
        }
    }

    Ok(())
}

#[test]
fn transact_malformed_insert_rejected_or_operation_error() -> Result<()> {
    let tc = TestOvsDBClient::start_plain()?;

    let result = tc.client.request(
        "transact",
        &json!([
            OPEN_VSWITCH_DB,
            {
                "op": "insert"
            }
        ]),
    );

    match result {
        Ok(value) => {
            let entries = value
                .as_array()
                .context("transact result should be an array")?;

            assert_eq!(entries.len(), 1);
            assert!(
                entries[0].get("error").is_some(),
                "malformed insert should produce operation error, got {value:?}"
            );
        }
        Err(Error::Validation(_)) => {}
        Err(other) => {
            anyhow::bail!("unexpected error for malformed insert: {other:?}");
        }
    }

    Ok(())
}

#[test]
fn client_survives_rpc_error_and_can_reuse_connection() -> Result<()> {
    let tc = TestOvsDBClient::start_plain()?;

    let _ = tc.client.get_schema(UNKNOWN_DB);

    let dbs = tc.client.list_dbs()?;

    assert!(
        dbs.iter().any(|db| db == OPEN_VSWITCH_DB),
        "connection should remain usable after RPC error, got {dbs:?}"
    );

    Ok(())
}

#[test]
fn client_survives_operation_error_and_can_reuse_connection() -> Result<()> {
    let tc = TestOvsDBClient::start_plain()?;

    let _ = tc.client.request(
        "transact",
        &json!([
            OPEN_VSWITCH_DB,
            {
                "op": "not-a-real-operation"
            }
        ]),
    );

    let echoed = tc.client.echo("still-alive")?;

    assert_eq!(echoed, "still-alive");

    Ok(())
}
