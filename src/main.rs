//! Command-line client for interacting with OVSDB servers.

mod cli;

use cli::{parse, Command, Handler};
use ovsdb::client::error as client_error;
use ovsdb::client::Connection as Client;
use ovsdb::model::DatabaseSchema;
use serde_json::{json, Value};
use std::error::Error as StdError;
use std::fmt::{self, Display, Formatter};
use std::{collections::HashMap, io::Write, process::ExitCode};

#[derive(Debug)]
pub(crate) enum RunError {
    ConvertRequiresMatchingSchema,
    QueryTransactionMustBeArray,
    MonitorCondSinceResultShape,
    InvalidWaitState,
    WaitTimedOut,
    Cli(cli::CliError),
    Client(client_error::Error),
    Io(std::io::Error),
    Json(serde_json::Error),
    MissingTable { table: String },
}

impl Display for RunError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConvertRequiresMatchingSchema => {
                f.write_str("convert requires matching schema version and checksum")
            }
            Self::QueryTransactionMustBeArray => {
                f.write_str("query transaction must be a JSON array")
            }
            Self::MonitorCondSinceResultShape => {
                f.write_str("monitor_cond_since result must be [found, last_id, table_updates]")
            }
            Self::InvalidWaitState => f.write_str("state must be added, connected, or removed"),
            Self::WaitTimedOut => f.write_str("timed out waiting for database state"),
            Self::Cli(err) => Display::fmt(err, f),
            Self::Client(err) => Display::fmt(err, f),
            Self::Io(err) => Display::fmt(err, f),
            Self::Json(err) => Display::fmt(err, f),
            Self::MissingTable { table } => write!(f, "table {table} not found"),
        }
    }
}

impl StdError for RunError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Cli(err) => Some(err),
            Self::Client(err) => Some(err),
            Self::Io(err) => Some(err),
            Self::Json(err) => Some(err),
            Self::ConvertRequiresMatchingSchema
            | Self::QueryTransactionMustBeArray
            | Self::MonitorCondSinceResultShape
            | Self::InvalidWaitState
            | Self::WaitTimedOut
            | Self::MissingTable { .. } => None,
        }
    }
}

impl From<cli::CliError> for RunError {
    fn from(err: cli::CliError) -> Self {
        Self::Cli(err)
    }
}

impl From<client_error::Error> for RunError {
    fn from(err: client_error::Error) -> Self {
        Self::Client(err)
    }
}

impl From<std::io::Error> for RunError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<serde_json::Error> for RunError {
    fn from(err: serde_json::Error) -> Self {
        Self::Json(err)
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            let mut stderr = std::io::stderr().lock();
            writeln!(stderr, "{err}").ok();
            ExitCode::from(1)
        }
    }
}

fn run() -> Result<(), RunError> {
    let (command, pretty) = parse()?;
    match command {
        Command::Client {
            server,
            handler,
            tls,
        } => {
            let client = Client::connect(&server, tls.as_ref())?;
            run_handler(&client, *handler, pretty)?;
        }
    }

    Ok(())
}

fn run_handler(client: &Client, handler: Handler, pretty: bool) -> Result<(), RunError> {
    match handler {
        Handler::ListDbs
        | Handler::GetSchema { .. }
        | Handler::GetSchemaVersion { .. }
        | Handler::GetSchemaCksum { .. }
        | Handler::ListTables { .. }
        | Handler::ListColumns { .. } => {
            run_read(client, handler, pretty)?;
        }
        Handler::Convert { .. }
        | Handler::NeedsConversion { .. }
        | Handler::Transact { .. }
        | Handler::Query { .. } => run_data(client, handler, pretty)?,
        Handler::Monitor { .. }
        | Handler::MonitorCond { .. }
        | Handler::MonitorCondSince { .. } => run_monitor(client, handler, pretty)?,
        Handler::Wait { .. }
        | Handler::Lock { .. }
        | Handler::Steal { .. }
        | Handler::Unlock { .. } => run_control(client, handler, pretty)?,
    }
    Ok(())
}

fn run_read(client: &Client, handler: Handler, pretty: bool) -> Result<(), RunError> {
    match handler {
        Handler::ListDbs => print_lines(&client.list_dbs()?),
        Handler::GetSchema { database } => print_json(&client.get_schema(&database)?, pretty)?,
        Handler::GetSchemaVersion { database } => {
            let schema = client.get_schema(&database)?;
            let mut stdout = std::io::stdout().lock();
            writeln!(stdout, "{}", schema.version)?;
        }
        Handler::GetSchemaCksum { database } => {
            let schema = client.get_schema(&database)?;
            let mut stdout = std::io::stdout().lock();
            if let Some(cksum) = schema.cksum {
                writeln!(stdout, "{cksum}")?;
            } else {
                writeln!(stdout)?;
            }
        }
        Handler::ListTables { database } => {
            let schema = client.get_schema(&database)?;
            let mut stdout = std::io::stdout().lock();
            let mut names: Vec<&String> = schema.tables.keys().collect();
            names.sort();
            for name in names {
                writeln!(stdout, "{name}")?;
            }
        }
        Handler::ListColumns { database, table } => {
            let schema = client.get_schema(&database)?;
            if let Some(table) = table {
                print_columns_for_table(schema.tables.get(&table), &table)?;
            } else {
                let mut names: Vec<&String> = schema.tables.keys().collect();
                names.sort();
                for name in names {
                    print_columns_for_table(schema.tables.get(name), name)?;
                }
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}

fn run_data(client: &Client, handler: Handler, pretty: bool) -> Result<(), RunError> {
    match handler {
        Handler::Convert { schema } => handle_convert(client, &schema)?,
        Handler::NeedsConversion { schema } => handle_needs_conversion(client, &schema)?,
        Handler::Transact { transaction } => {
            let transaction: Value = serde_json::from_str(&transaction)?;
            print_json(&client.request("transact", &transaction)?, pretty)?;
        }
        Handler::Query { transaction } => handle_query(client, &transaction, pretty)?,
        _ => unreachable!(),
    }
    Ok(())
}

fn run_monitor(client: &Client, handler: Handler, pretty: bool) -> Result<(), RunError> {
    match handler {
        Handler::Monitor {
            database,
            table,
            columns,
        } => {
            let initial = monitor_table(client, &database, &table, &columns)?;
            print_json(&initial, pretty)?;
            stream_monitor(client, pretty)?;
        }
        Handler::MonitorCond {
            database,
            condition,
            table,
            columns,
        } => {
            let initial =
                monitor_table_conditional(client, &database, &table, &columns, &condition, None)?;
            print_json(&initial, pretty)?;
            stream_monitor(client, pretty)?;
        }
        Handler::MonitorCondSince {
            database,
            last_id,
            condition,
            table,
            columns,
        } => {
            let initial = monitor_table_conditional(
                client,
                &database,
                &table,
                &columns,
                &condition,
                last_id.as_deref(),
            )?;
            print_json(&initial, pretty)?;
            stream_monitor(client, pretty)?;
        }
        _ => unreachable!(),
    }
    Ok(())
}

fn run_control(client: &Client, handler: Handler, pretty: bool) -> Result<(), RunError> {
    match handler {
        Handler::Wait { database, state } => {
            wait_for_database(client, &database, &state)?;
            let mut stdout = std::io::stdout().lock();
            writeln!(stdout)?;
        }
        Handler::Lock { lock_id } => print_json(&client.lock(&lock_id)?, pretty)?,
        Handler::Steal { lock_id } => print_json(&client.steal(&lock_id)?, pretty)?,
        Handler::Unlock { lock_id } => {
            client.unlock(&lock_id)?;
            let mut stdout = std::io::stdout().lock();
            writeln!(stdout)?;
        }
        _ => unreachable!(),
    }
    Ok(())
}

fn handle_convert(client: &Client, schema: &str) -> Result<(), RunError> {
    let local_schema: DatabaseSchema = serde_json::from_value(load_schema_file(schema)?)?;
    let remote_schema = client.get_schema(&local_schema.name)?;
    if remote_schema.version == local_schema.version && remote_schema.cksum == local_schema.cksum {
        let mut stdout = std::io::stdout().lock();
        writeln!(stdout)?;
        Ok(())
    } else {
        Err(RunError::ConvertRequiresMatchingSchema)
    }
}

fn handle_needs_conversion(client: &Client, schema: &str) -> Result<(), RunError> {
    let local_schema: DatabaseSchema = serde_json::from_value(load_schema_file(schema)?)?;
    let remote_schema = client.get_schema(&local_schema.name)?;
    let mut stdout = std::io::stdout().lock();
    if remote_schema.version == local_schema.version && remote_schema.cksum == local_schema.cksum {
        writeln!(stdout, "no")?;
    } else {
        writeln!(stdout, "yes")?;
    }
    Ok(())
}

fn handle_query(client: &Client, transaction: &str, pretty: bool) -> Result<(), RunError> {
    let mut transaction: Value = serde_json::from_str(transaction)?;
    let arr = transaction
        .as_array_mut()
        .ok_or(RunError::QueryTransactionMustBeArray)?;
    arr.push(json!({"op":"abort"}));
    let reply = client.request("transact", &transaction)?;
    if let Some(arr) = reply.as_array() {
        let trimmed = match arr.split_last() {
            Some((_last, prefix)) => Value::Array(prefix.to_vec()),
            None => reply,
        };
        print_json(&trimmed, pretty)?;
    } else {
        print_json(&reply, pretty)?;
    }
    Ok(())
}

fn wait_for_database(client: &Client, database: &str, state: &str) -> Result<(), RunError> {
    let want_present = matches!(state, "added" | "connected");
    let want_absent = state == "removed";
    if !want_present && !want_absent {
        return Err(RunError::InvalidWaitState);
    }

    for _ in 0..60 {
        let dbs = client.list_dbs()?;
        let present = dbs.iter().any(|db| db == database);
        if (want_present && present) || (want_absent && !present) {
            return Ok(());
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    Err(RunError::WaitTimedOut)
}

fn monitor_table(
    client: &Client,
    database: &str,
    table: &str,
    columns: &[String],
) -> Result<ovsdb::client::TableUpdates, RunError> {
    let monitor_id = format!("monitor-{}", client.next_id());
    let mut requests = HashMap::new();
    requests.insert(
        table.to_string(),
        json!([{
            "columns": columns,
            "select": {
                "initial": true,
                "insert": true,
                "delete": true,
                "modify": true
            }
        }]),
    );
    Ok(client.monitor(database, &json!(monitor_id), &requests)?)
}

fn monitor_table_conditional(
    client: &Client,
    database: &str,
    table: &str,
    columns: &[String],
    condition: &str,
    last_id: Option<&str>,
) -> Result<ovsdb::client::TableUpdates, RunError> {
    let monitor_id = format!("monitor-{}", client.next_id());
    let where_clause: Value = serde_json::from_str(condition)?;
    let mut requests = HashMap::new();
    requests.insert(
        table.to_string(),
        json!([{
            "columns": columns,
            "where": where_clause,
            "select": {
                "initial": true,
                "insert": true,
                "delete": true,
                "modify": true
            }
        }]),
    );

    let result = if let Some(last_id) = last_id {
        client.request(
            "monitor_cond_since",
            &json!([database, monitor_id, requests, last_id]),
        )?
    } else {
        client.request("monitor_cond", &json!([database, monitor_id, requests]))?
    };
    decode_conditional_monitor_result(result)
}

fn decode_conditional_monitor_result(
    value: Value,
) -> Result<ovsdb::client::TableUpdates, RunError> {
    let updates_value = if let Some(arr) = value.as_array() {
        arr.get(2)
            .cloned()
            .ok_or(RunError::MonitorCondSinceResultShape)?
    } else {
        value
    };
    Ok(serde_json::from_value(updates_value)?)
}

fn stream_monitor(client: &Client, pretty: bool) -> Result<(), RunError> {
    loop {
        let msg = client.poll_notification()?;
        print_json(&msg, pretty)?;
    }
}

fn load_schema_file(path: &str) -> Result<Value, RunError> {
    let data = std::fs::read_to_string(path)?;
    Ok(serde_json::from_str(&data)?)
}

fn print_lines(lines: &[String]) {
    let mut stdout = std::io::stdout().lock();
    for line in lines {
        writeln!(stdout, "{line}").ok();
    }
}

fn print_columns_for_table(
    table: Option<&ovsdb::model::TableSchema>,
    table_name: &str,
) -> Result<(), RunError> {
    let table = table.ok_or_else(|| RunError::MissingTable {
        table: table_name.to_string(),
    })?;
    let mut names: Vec<&String> = table.columns.keys().collect();
    names.sort();
    let mut stdout = std::io::stdout().lock();
    for name in names {
        writeln!(stdout, "{table_name}\t{name}")?;
    }
    Ok(())
}

fn print_json<T: serde::Serialize>(value: &T, pretty: bool) -> Result<(), RunError> {
    let mut stdout = std::io::stdout().lock();
    if pretty {
        writeln!(stdout, "{}", serde_json::to_string_pretty(value)?)?;
    } else {
        writeln!(stdout, "{}", serde_json::to_string(value)?)?;
    }
    Ok(())
}
