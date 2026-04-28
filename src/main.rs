//! Command-line client for interacting with OVSDB servers.

mod cli;

use cli::{parse, Command, Handler};
use ovsdb::client::Connection as Client;
use ovsdb::model::DatabaseSchema;
use serde_json::{json, Value};
use std::{collections::HashMap, io::Write, process::ExitCode};

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

fn run() -> anyhow::Result<()> {
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
        Command::Server => {
            anyhow::bail!("server subcommand is not implemented yet");
        }
    }

    Ok(())
}

fn run_handler(client: &Client, handler: Handler, pretty: bool) -> anyhow::Result<()> {
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
        Handler::Dump {
            database,
            table,
            columns,
        } => {
            let _ = (&database, &table, &columns);
            anyhow::bail!("dump is not implemented yet")
        }
        Handler::Backup { database } => {
            let _ = &database;
            anyhow::bail!("backup is not implemented yet")
        }
        Handler::Restore {
            force,
            database,
            snapshot,
        } => {
            let _ = (&force, &database, &snapshot);
            anyhow::bail!("restore is not implemented yet")
        }
    }
    Ok(())
}

fn run_read(client: &Client, handler: Handler, pretty: bool) -> anyhow::Result<()> {
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

fn run_data(client: &Client, handler: Handler, pretty: bool) -> anyhow::Result<()> {
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

fn run_monitor(client: &Client, handler: Handler, pretty: bool) -> anyhow::Result<()> {
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
            let _ = &condition;
            let initial = monitor_table(client, &database, &table, &columns)?;
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
            let _ = (&condition, &last_id);
            let initial = monitor_table(client, &database, &table, &columns)?;
            print_json(&initial, pretty)?;
            stream_monitor(client, pretty)?;
        }
        _ => unreachable!(),
    }
    Ok(())
}

fn run_control(client: &Client, handler: Handler, pretty: bool) -> anyhow::Result<()> {
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

fn handle_convert(client: &Client, schema: &str) -> anyhow::Result<()> {
    let local_schema: DatabaseSchema = serde_json::from_value(load_schema_file(schema)?)?;
    let remote_schema = client.get_schema(&local_schema.name)?;
    if remote_schema.version == local_schema.version && remote_schema.cksum == local_schema.cksum {
        let mut stdout = std::io::stdout().lock();
        writeln!(stdout)?;
        Ok(())
    } else {
        anyhow::bail!("convert is not implemented yet");
    }
}

fn handle_needs_conversion(client: &Client, schema: &str) -> anyhow::Result<()> {
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

fn handle_query(client: &Client, transaction: &str, pretty: bool) -> anyhow::Result<()> {
    let mut transaction: Value = serde_json::from_str(transaction)?;
    let arr = transaction
        .as_array_mut()
        .ok_or_else(|| anyhow::anyhow!("query transaction must be a JSON array"))?;
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

fn wait_for_database(client: &Client, database: &str, state: &str) -> anyhow::Result<()> {
    let want_present = matches!(state, "added" | "connected");
    let want_absent = state == "removed";
    if !want_present && !want_absent {
        anyhow::bail!("state must be added, connected, or removed");
    }

    for _ in 0..60 {
        let dbs = client.list_dbs()?;
        let present = dbs.iter().any(|db| db == database);
        if (want_present && present) || (want_absent && !present) {
            return Ok(());
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    anyhow::bail!("timed out waiting for database state");
}

fn monitor_table(
    client: &Client,
    database: &str,
    table: &str,
    columns: &[String],
) -> anyhow::Result<ovsdb::client::TableUpdates> {
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

fn stream_monitor(client: &Client, pretty: bool) -> anyhow::Result<()> {
    loop {
        let msg = client.poll_notification()?;
        print_json(&msg, pretty)?;
    }
}

fn load_schema_file(path: &str) -> anyhow::Result<Value> {
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
) -> anyhow::Result<()> {
    let table = table.ok_or_else(|| anyhow::anyhow!("table {table_name} not found"))?;
    let mut names: Vec<&String> = table.columns.keys().collect();
    names.sort();
    let mut stdout = std::io::stdout().lock();
    for name in names {
        writeln!(stdout, "{table_name}\t{name}")?;
    }
    Ok(())
}

fn print_json<T: serde::Serialize>(value: &T, pretty: bool) -> anyhow::Result<()> {
    let mut stdout = std::io::stdout().lock();
    if pretty {
        writeln!(stdout, "{}", serde_json::to_string_pretty(value)?)?;
    } else {
        writeln!(stdout, "{}", serde_json::to_string(value)?)?;
    }
    Ok(())
}
