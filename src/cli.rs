use clap::{Arg, ArgAction, Command as ClapCommand};
use ovsdb::client::tls as TLS;
use std::path::PathBuf;

const DEFAULT_SERVER: &str = "unix:/run/openvswitch/db.sock";
const DEFAULT_DATABASE: &str = "Open_vSwitch";

pub enum Handler {
    ListDbs,
    GetSchema {
        database: String,
    },
    GetSchemaVersion {
        database: String,
    },
    GetSchemaCksum {
        database: String,
    },
    ListTables {
        database: String,
    },
    ListColumns {
        database: String,
        table: Option<String>,
    },
    Convert {
        schema: String,
    },
    NeedsConversion {
        schema: String,
    },
    Transact {
        transaction: String,
    },
    Query {
        transaction: String,
    },
    Dump {
        database: String,
        table: Option<String>,
        columns: Vec<String>,
    },
    Backup {
        database: String,
    },
    Restore {
        force: bool,
        database: String,
        snapshot: String,
    },
    Monitor {
        database: String,
        table: String,
        columns: Vec<String>,
    },
    MonitorCond {
        database: String,
        condition: String,
        table: String,
        columns: Vec<String>,
    },
    MonitorCondSince {
        database: String,
        last_id: Option<String>,
        condition: String,
        table: String,
        columns: Vec<String>,
    },
    Wait {
        database: String,
        state: String,
    },
    Lock {
        lock_id: String,
    },
    Steal {
        lock_id: String,
    },
    Unlock {
        lock_id: String,
    },
}

pub enum Command {
    Client {
        server: String,
        handler: Box<Handler>,
        tls: Option<TLS::Options>,
    },
    Server,
}

#[allow(clippy::too_many_lines)]
pub fn parse() -> anyhow::Result<(Command, bool)> {
    let matches = ClapCommand::new("ovsdb")
        .version(env!("CARGO_PKG_VERSION"))
        .about("Open vSwitch database JSON-RPC client")
        .arg(
            Arg::new("pretty")
                .short('p')
                .long("pretty")
                .help("Pretty-print JSON output")
                .global(true)
                .action(ArgAction::SetTrue),
        )
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(client())
        .subcommand(server())
        .get_matches();

    let Some((root_name, root_matches)) = matches.subcommand() else {
        unreachable!("subcommand required");
    };

    let command = match root_name {
        "client" => {
            let ca_cert = matches.get_one::<PathBuf>("ca-cert").cloned();
            let client_cert = matches.get_one::<PathBuf>("client-cert").cloned();
            let client_key = matches.get_one::<PathBuf>("client-key").cloned();
            if client_cert.is_some() ^ client_key.is_some() {
                anyhow::bail!("both --client-cert and --client-key are required together");
            }
            let tls = match (ca_cert, client_cert, client_key) {
                (None, None, None) => None,
                (ca_cert, client_cert, client_key) => Some(TLS::Options {
                    ca_cert,
                    client_cert,
                    client_key,
                }),
            };

            let Some((name, sub_matches)) = root_matches.subcommand() else {
                unreachable!("subcommand required");
            };
            let args: Vec<String> = sub_matches
                .get_many::<String>("args")
                .map(|values| values.cloned().collect())
                .unwrap_or_default();

            let looks_like_server = |value: &str| {
                matches!(
                    value,
                    s if s.starts_with("tcp:")
                        || s.starts_with("ssl:")
                        || s.starts_with("unix:")
                        || s.starts_with("ptcp:")
                        || s.starts_with("pssl:")
                        || s.starts_with("punix:")
                ) || (value.contains(':')
                    && !value.starts_with('[')
                    && !value.starts_with('{')
                    && !value.starts_with('"')
                    && !value.contains(' '))
            };

            let (server, rest) = args
                .first()
                .filter(|first| looks_like_server(first))
                .map_or_else(
                    || (DEFAULT_SERVER.to_string(), args.as_slice()),
                    |first| (first.clone(), args.get(1..).unwrap_or(&[])),
                );
            let first_or_default = |slice: &[String], default: &str| {
                slice
                    .first()
                    .cloned()
                    .unwrap_or_else(|| default.to_string())
            };
            let join_remaining = |slice: &[String], what: &str| -> anyhow::Result<String> {
                match slice {
                    [] => anyhow::bail!("missing {what}"),
                    [single] => Ok(single.clone()),
                    many => Ok(many.join(" ")),
                }
            };
            let split_columns = |slice: &[String]| -> Vec<String> {
                slice
                    .iter()
                    .flat_map(|part| part.split(','))
                    .filter(|s| !s.is_empty())
                    .map(std::string::ToString::to_string)
                    .collect()
            };

            let handler = match name {
                "list-dbs" => Handler::ListDbs,
                "get-schema" => Handler::GetSchema {
                    database: first_or_default(rest, DEFAULT_DATABASE),
                },
                "get-schema-version" => Handler::GetSchemaVersion {
                    database: first_or_default(rest, DEFAULT_DATABASE),
                },
                "get-schema-cksum" => Handler::GetSchemaCksum {
                    database: first_or_default(rest, DEFAULT_DATABASE),
                },
                "list-tables" => Handler::ListTables {
                    database: first_or_default(rest, DEFAULT_DATABASE),
                },
                "list-columns" => Handler::ListColumns {
                    database: first_or_default(rest, DEFAULT_DATABASE),
                    table: rest.get(1).cloned(),
                },
                "convert" => Handler::Convert {
                    schema: join_remaining(rest, "schema")?,
                },
                "needs-conversion" => Handler::NeedsConversion {
                    schema: join_remaining(rest, "schema")?,
                },
                "transact" => Handler::Transact {
                    transaction: join_remaining(rest, "transaction")?,
                },
                "query" => Handler::Query {
                    transaction: join_remaining(rest, "transaction")?,
                },
                "dump" => Handler::Dump {
                    database: first_or_default(rest, DEFAULT_DATABASE),
                    table: rest.get(1).cloned(),
                    columns: rest.iter().skip(2).cloned().collect(),
                },
                "backup" => Handler::Backup {
                    database: first_or_default(rest, DEFAULT_DATABASE),
                },
                "restore" => Handler::Restore {
                    force: sub_matches.get_flag("force"),
                    database: first_or_default(rest, DEFAULT_DATABASE),
                    snapshot: rest.get(1).cloned().unwrap_or_default(),
                },
                "monitor" => {
                    let (database, table, columns) = match rest {
                        [] => anyhow::bail!("monitor requires a table name"),
                        [table] => (DEFAULT_DATABASE.to_string(), table.clone(), Vec::new()),
                        [database, table, tail @ ..] => {
                            (database.clone(), table.clone(), split_columns(tail))
                        }
                    };
                    Handler::Monitor {
                        database,
                        table,
                        columns,
                    }
                }
                "monitor-cond" => {
                    let (database, condition, table, columns) = match rest {
                        [] | [_] => anyhow::bail!("monitor-cond requires a condition and table"),
                        [condition, table] => (
                            DEFAULT_DATABASE.to_string(),
                            condition.clone(),
                            table.clone(),
                            Vec::new(),
                        ),
                        [database, condition, table, tail @ ..] => (
                            database.clone(),
                            condition.clone(),
                            table.clone(),
                            split_columns(tail),
                        ),
                    };
                    Handler::MonitorCond {
                        database,
                        condition,
                        table,
                        columns,
                    }
                }
                "monitor-cond-since" => {
                    let (database, last_id, condition, table, columns) = match rest {
                        [] | [_] => {
                            anyhow::bail!("monitor-cond-since requires a condition and table")
                        }
                        [condition, table] => (
                            DEFAULT_DATABASE.to_string(),
                            None,
                            condition.clone(),
                            table.clone(),
                            Vec::new(),
                        ),
                        [last_id, condition, table] => (
                            DEFAULT_DATABASE.to_string(),
                            Some(last_id.clone()),
                            condition.clone(),
                            table.clone(),
                            Vec::new(),
                        ),
                        [database, condition, table, tail @ ..] if tail.len() == 1 => (
                            database.clone(),
                            None,
                            condition.clone(),
                            table.clone(),
                            split_columns(tail),
                        ),
                        [database, last_id, condition, table, tail @ ..] => (
                            database.clone(),
                            Some(last_id.clone()),
                            condition.clone(),
                            table.clone(),
                            split_columns(tail),
                        ),
                    };
                    Handler::MonitorCondSince {
                        database,
                        last_id,
                        condition,
                        table,
                        columns,
                    }
                }
                "wait" => Handler::Wait {
                    database: first_or_default(rest, DEFAULT_DATABASE),
                    state: rest.get(1).cloned().unwrap_or_default(),
                },
                "lock" => Handler::Lock {
                    lock_id: join_remaining(rest, "lock")?,
                },
                "steal" => Handler::Steal {
                    lock_id: join_remaining(rest, "lock")?,
                },
                "unlock" => Handler::Unlock {
                    lock_id: join_remaining(rest, "lock")?,
                },
                _ => anyhow::bail!("unknown command: {name}"),
            };

            Command::Client {
                server,
                handler: Box::new(handler),
                tls,
            }
        }
        "server" => Command::Server,
        _ => anyhow::bail!("unknown command: {root_name}"),
    };

    let pretty = matches.get_flag("pretty");
    Ok((command, pretty))
}

#[allow(clippy::too_many_lines)]
fn client() -> ClapCommand {
    ClapCommand::new("client")
        .about("client-side JSON-RPC commands")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .arg(
            Arg::new("ca-cert")
                .long("ca-cert")
                .value_name("PATH")
                .help("PEM file containing additional CA certificates")
                .global(true)
                .value_parser(clap::value_parser!(PathBuf)),
        )
        .arg(
            Arg::new("client-cert")
                .long("client-cert")
                .value_name("PATH")
                .help("PEM file containing the client certificate")
                .global(true)
                .value_parser(clap::value_parser!(PathBuf)),
        )
        .arg(
            Arg::new("client-key")
                .long("client-key")
                .value_name("PATH")
                .help("PEM file containing the client private key")
                .global(true)
                .value_parser(clap::value_parser!(PathBuf)),
        )
        .subcommands([
            ClapCommand::new("list-dbs")
                .about("list databases available on SERVER")
                .arg(
                    Arg::new("args")
                        .value_name("SERVER")
                        .num_args(0..)
                        .allow_hyphen_values(true)
                        .trailing_var_arg(true),
                ),
            ClapCommand::new("get-schema")
                .about("retrieve schema for DATABASE from SERVER")
                .arg(
                    Arg::new("args")
                        .value_name("SERVER [DATABASE]")
                        .num_args(0..)
                        .allow_hyphen_values(true)
                        .trailing_var_arg(true),
                ),
            ClapCommand::new("get-schema-version")
                .about("retrieve schema version for DATABASE from SERVER")
                .arg(
                    Arg::new("args")
                        .value_name("SERVER [DATABASE]")
                        .num_args(0..)
                        .allow_hyphen_values(true)
                        .trailing_var_arg(true),
                ),
            ClapCommand::new("get-schema-cksum")
                .about("retrieve schema checksum for DATABASE from SERVER")
                .arg(
                    Arg::new("args")
                        .value_name("SERVER [DATABASE]")
                        .num_args(0..)
                        .allow_hyphen_values(true)
                        .trailing_var_arg(true),
                ),
            ClapCommand::new("list-tables")
                .about("list table for DATABASE on SERVER")
                .arg(
                    Arg::new("args")
                        .value_name("SERVER [DATABASE]")
                        .num_args(0..)
                        .allow_hyphen_values(true)
                        .trailing_var_arg(true),
                ),
            ClapCommand::new("list-columns")
                .about("list columns in TABLE, or all tables, in DATABASE on SERVER")
                .arg(
                    Arg::new("args")
                        .value_name("SERVER [DATABASE] [TABLE]")
                        .num_args(0..)
                        .allow_hyphen_values(true)
                        .trailing_var_arg(true),
                ),
            ClapCommand::new("convert")
                .about("convert database on SERVER named in SCHEMA to SCHEMA")
                .arg(
                    Arg::new("args")
                        .value_name("SERVER SCHEMA")
                        .num_args(0..)
                        .allow_hyphen_values(true)
                        .trailing_var_arg(true),
                ),
            ClapCommand::new("needs-conversion")
                .about("tests whether SCHEMA's db on SERVER needs conversion")
                .arg(
                    Arg::new("args")
                        .value_name("SERVER SCHEMA")
                        .num_args(0..)
                        .allow_hyphen_values(true)
                        .trailing_var_arg(true),
                ),
            ClapCommand::new("transact")
                .about("run TRANSACTION (params for \"transact\" request) on SERVER")
                .arg(
                    Arg::new("args")
                        .value_name("SERVER TRANSACTION")
                        .num_args(0..)
                        .allow_hyphen_values(true)
                        .trailing_var_arg(true),
                ),
            ClapCommand::new("query")
                .about("run TRANSACTION (params for \"transact\" request) on SERVER, as read-only")
                .arg(
                    Arg::new("args")
                        .value_name("SERVER TRANSACTION")
                        .num_args(0..)
                        .allow_hyphen_values(true)
                        .trailing_var_arg(true),
                ),
            ClapCommand::new("dump")
                .about("dump contents of TABLEs in DATABASE on SERVER to stdout")
                .arg(
                    Arg::new("args")
                        .value_name("SERVER [DATABASE] [TABLE [COLUMN]...]")
                        .num_args(0..)
                        .allow_hyphen_values(true)
                        .trailing_var_arg(true),
                ),
            ClapCommand::new("backup")
                .about("dump database contents in the form of a database file")
                .arg(
                    Arg::new("args")
                        .value_name("SERVER [DATABASE]")
                        .num_args(0..)
                        .allow_hyphen_values(true)
                        .trailing_var_arg(true),
                ),
            ClapCommand::new("monitor")
                .about("monitor table contents and print updates as they arrive")
                .arg(
                    Arg::new("args")
                        .value_name("SERVER [DATABASE] TABLE [COLUMN,...]...")
                        .num_args(0..)
                        .allow_hyphen_values(true)
                        .trailing_var_arg(true),
                ),
            ClapCommand::new("monitor-cond")
                .about("monitor contents that match CONDITION")
                .arg(
                    Arg::new("args")
                        .value_name("SERVER [DATABASE] CONDITION TABLE [COLUMN,...]...")
                        .num_args(0..)
                        .allow_hyphen_values(true)
                        .trailing_var_arg(true),
                ),
            ClapCommand::new("monitor-cond-since")
                .about("monitor contents that match CONDITION since LASTID")
                .arg(
                    Arg::new("args")
                        .value_name("SERVER [DATABASE] [LASTID] CONDITION TABLE [COLUMN,...]...")
                        .num_args(0..)
                        .allow_hyphen_values(true)
                        .trailing_var_arg(true),
                ),
            ClapCommand::new("wait")
                .about("wait until DATABASE reaches STATE")
                .arg(
                    Arg::new("args")
                        .value_name("SERVER DATABASE STATE")
                        .num_args(0..)
                        .allow_hyphen_values(true)
                        .trailing_var_arg(true),
                ),
            ClapCommand::new("lock")
                .about("create or wait for LOCK in SERVER")
                .arg(
                    Arg::new("args")
                        .value_name("SERVER LOCK")
                        .num_args(0..)
                        .allow_hyphen_values(true)
                        .trailing_var_arg(true),
                ),
            ClapCommand::new("steal")
                .about("steal LOCK from SERVER")
                .arg(
                    Arg::new("args")
                        .value_name("SERVER LOCK")
                        .num_args(0..)
                        .allow_hyphen_values(true)
                        .trailing_var_arg(true),
                ),
            ClapCommand::new("unlock")
                .about("unlock LOCK in SERVER")
                .arg(
                    Arg::new("args")
                        .value_name("SERVER LOCK")
                        .num_args(0..)
                        .allow_hyphen_values(true)
                        .trailing_var_arg(true),
                ),
            ClapCommand::new("restore")
                .about("restore database contents from a database file")
                .arg(
                    Arg::new("force")
                        .long("force")
                        .action(ArgAction::SetTrue)
                        .help("proceed despite schema differences"),
                )
                .arg(
                    Arg::new("args")
                        .value_name("SERVER [DATABASE] [SNAPSHOT]")
                        .num_args(0..)
                        .allow_hyphen_values(true)
                        .trailing_var_arg(true),
                ),
        ])
}

fn server() -> ClapCommand {
    ClapCommand::new("server")
        .about("server-side commands")
        .arg_required_else_help(false)
}
