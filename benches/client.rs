#![allow(missing_docs)]

use anyhow::{Context, Result};
use ovsdb::client::ops::Ops as ops;
use ovsdb::model::DatabaseSchema;
use serde_json::{json, Value};
use std::{
    env,
    hint::black_box,
    io::Write,
    path::PathBuf,
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

static UNIQUE_SUFFIX: AtomicU64 = AtomicU64::new(1);
const BENCH_ITERATIONS: u64 = 1000;
const IMAGE_NAME: &str = "ovsdb-server:test";
const CONTAINERFILE: &str = "Containerfile.ovsdb-server";
const NATIVE_OVSDB_CLIENT: &str = "/usr/bin/ovsdb-client";

struct RustBenchClient {
    binary: PathBuf,
    server: String,
}

struct NativeBenchClient {
    binary: PathBuf,
    server: String,
}

struct CompareBench {
    _container: Option<ManagedContainer>,
    rust: RustBenchClient,
    native: NativeBenchClient,
    table: String,
    id_col: String,
    map_col: String,
}

struct ManagedContainer {
    id: String,
    host: String,
    port: String,
}

impl RustBenchClient {
    fn new(server: &str) -> Result<Self> {
        Ok(Self {
            binary: resolve_rust_client_binary()?,
            server: server.to_string(),
        })
    }

    fn list_dbs(&self) -> Result<Vec<String>> {
        let output = run_client_output(&self.binary, &["client", "list-dbs", &self.server])?;
        Ok(output
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(ToString::to_string)
            .collect())
    }

    fn get_schema(&self) -> Result<DatabaseSchema> {
        let output = run_client_output(
            &self.binary,
            &["client", "get-schema", &self.server, "Open_vSwitch"],
        )?;
        Ok(serde_json::from_str(&output)?)
    }

    fn select_table(&self, table: &str, id_col: &str, map_col: &str) -> Result<Value> {
        let txn = serde_json::to_string(&json!([
            "Open_vSwitch",
            ops::select(table, &[], Some(&[id_col.to_string(), map_col.to_string()])),
        ]))?;
        run_client_json(&self.binary, &["client", "query", &self.server, &txn])
    }

    fn insert_row(
        &self,
        table: &str,
        id_col: &str,
        map_col: &str,
        row_name: &str,
    ) -> Result<Value> {
        let txn = serde_json::to_string(&json!([
            "Open_vSwitch",
            ops::insert(table, live_row(id_col, map_col, row_name), Some("row_uuid")),
        ]))?;
        run_client_json(&self.binary, &["client", "transact", &self.server, &txn])
    }

    fn update_row(
        &self,
        table: &str,
        id_col: &str,
        map_col: &str,
        row_name: &str,
    ) -> Result<Value> {
        let txn = serde_json::to_string(&json!([
            "Open_vSwitch",
            ops::insert(table, live_row(id_col, map_col, row_name), Some("row_uuid")),
            ops::update(
                table,
                &live_where(id_col, row_name),
                json!({map_col: ["map", [["suite", "ok"]]]}),
            ),
        ]))?;
        run_client_json(&self.binary, &["client", "transact", &self.server, &txn])
    }

    fn mutate_row(
        &self,
        table: &str,
        id_col: &str,
        map_col: &str,
        row_name: &str,
    ) -> Result<Value> {
        let txn = serde_json::to_string(&json!([
            "Open_vSwitch",
            ops::insert(table, live_row(id_col, map_col, row_name), Some("row_uuid")),
            ops::mutate(
                table,
                &live_where(id_col, row_name),
                &[json!([map_col, "insert", ["map", [["extra", "value"]]]])],
            ),
        ]))?;
        run_client_json(&self.binary, &["client", "transact", &self.server, &txn])
    }

    fn delete_row(
        &self,
        table: &str,
        id_col: &str,
        map_col: &str,
        row_name: &str,
    ) -> Result<Value> {
        let txn = serde_json::to_string(&json!([
            "Open_vSwitch",
            ops::insert(table, live_row(id_col, map_col, row_name), Some("row_uuid")),
            ops::delete(table, &live_where(id_col, row_name)),
        ]))?;
        run_client_json(&self.binary, &["client", "transact", &self.server, &txn])
    }

    fn cleanup_rows(&self, table: &str, id_col: &str, row_names: &[String]) -> Result<()> {
        for chunk in row_names.chunks(100) {
            let deletes: Vec<Value> = chunk
                .iter()
                .map(|row_name| ops::delete(table, &[json!([id_col, "==", row_name])]))
                .collect();
            let mut txn = vec![json!("Open_vSwitch")];
            txn.extend(deletes);
            let txn = serde_json::to_string(&Value::Array(txn))?;
            let _ = run_client_json(&self.binary, &["client", "transact", &self.server, &txn])?;
        }
        Ok(())
    }
}

impl NativeBenchClient {
    fn new(server: &str) -> Self {
        Self {
            binary: PathBuf::from(NATIVE_OVSDB_CLIENT),
            server: server.to_string(),
        }
    }

    fn list_dbs(&self) -> Result<Vec<String>> {
        let output = run_client_output(&self.binary, &["list-dbs", &self.server])?;
        Ok(output
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(ToString::to_string)
            .collect())
    }

    fn get_schema(&self) -> Result<String> {
        run_client_output(&self.binary, &["get-schema", &self.server, "Open_vSwitch"])
    }

    fn select_table(&self, table: &str, id_col: &str, map_col: &str) -> Result<Value> {
        let txn = serde_json::to_string(&json!([
            "Open_vSwitch",
            ops::select(table, &[], Some(&[id_col.to_string(), map_col.to_string()])),
        ]))?;
        run_client_json(&self.binary, &["-f", "json", "query", &self.server, &txn])
    }

    fn insert_row(
        &self,
        table: &str,
        id_col: &str,
        map_col: &str,
        row_name: &str,
    ) -> Result<Value> {
        let txn = serde_json::to_string(&json!([
            "Open_vSwitch",
            ops::insert(table, live_row(id_col, map_col, row_name), Some("row_uuid")),
        ]))?;
        run_client_json(
            &self.binary,
            &["-f", "json", "transact", &self.server, &txn],
        )
    }

    fn update_row(
        &self,
        table: &str,
        id_col: &str,
        map_col: &str,
        row_name: &str,
    ) -> Result<Value> {
        let txn = serde_json::to_string(&json!([
            "Open_vSwitch",
            ops::insert(table, live_row(id_col, map_col, row_name), Some("row_uuid")),
            ops::update(
                table,
                &live_where(id_col, row_name),
                json!({map_col: ["map", [["suite", "ok"]]]}),
            ),
        ]))?;
        run_client_json(
            &self.binary,
            &["-f", "json", "transact", &self.server, &txn],
        )
    }

    fn mutate_row(
        &self,
        table: &str,
        id_col: &str,
        map_col: &str,
        row_name: &str,
    ) -> Result<Value> {
        let txn = serde_json::to_string(&json!([
            "Open_vSwitch",
            ops::insert(table, live_row(id_col, map_col, row_name), Some("row_uuid")),
            ops::mutate(
                table,
                &live_where(id_col, row_name),
                &[json!([map_col, "insert", ["map", [["extra", "value"]]]])],
            ),
        ]))?;
        run_client_json(
            &self.binary,
            &["-f", "json", "transact", &self.server, &txn],
        )
    }

    fn delete_row(
        &self,
        table: &str,
        id_col: &str,
        map_col: &str,
        row_name: &str,
    ) -> Result<Value> {
        let txn = serde_json::to_string(&json!([
            "Open_vSwitch",
            ops::insert(table, live_row(id_col, map_col, row_name), Some("row_uuid")),
            ops::delete(table, &live_where(id_col, row_name)),
        ]))?;
        run_client_json(
            &self.binary,
            &["-f", "json", "transact", &self.server, &txn],
        )
    }

    fn cleanup_rows(&self, table: &str, id_col: &str, row_names: &[String]) -> Result<()> {
        for chunk in row_names.chunks(100) {
            let deletes: Vec<Value> = chunk
                .iter()
                .map(|row_name| ops::delete(table, &[json!([id_col, "==", row_name])]))
                .collect();
            let mut txn = vec![json!("Open_vSwitch")];
            txn.extend(deletes);
            let txn = serde_json::to_string(&Value::Array(txn))?;
            let _ = run_client_json(
                &self.binary,
                &["-f", "json", "transact", &self.server, &txn],
            )?;
        }
        Ok(())
    }
}

impl CompareBench {
    fn start() -> Result<Self> {
        let (container, server) = if let Ok(server) = env::var("OVSDB_BENCH_SERVER") {
            (None, server)
        } else {
            ensure_image()?;
            let container = start_container()?;
            let server = container.server_uri();
            (Some(container), server)
        };

        let rust = RustBenchClient::new(&server)?;
        let native = NativeBenchClient::new(&server);

        let schema = rust.get_schema().context("load Open_vSwitch schema")?;
        let (table, id_col, map_col) = choose_writable_table(&schema)?;

        Ok(Self {
            _container: container,
            rust,
            native,
            table,
            id_col,
            map_col,
        })
    }
}

fn ensure_image() -> Result<()> {
    let inspect = Command::new("docker")
        .args(["image", "inspect", IMAGE_NAME])
        .output()
        .context("inspect ovsdb-server image")?;
    if inspect.status.success() {
        return Ok(());
    }

    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    run_docker(&[
        "build",
        "-t",
        IMAGE_NAME,
        "-f",
        root.join(CONTAINERFILE)
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("invalid containerfile path"))?,
        root.to_str()
            .ok_or_else(|| anyhow::anyhow!("invalid project root path"))?,
    ])
    .context("build ovsdb-server image")
}

fn start_container() -> Result<ManagedContainer> {
    let name = format!(
        "ovsdb-bench-{}-{}",
        std::process::id(),
        UNIQUE_SUFFIX.fetch_add(1, Ordering::Relaxed)
    );
    let output = run_docker_output(&[
        "run",
        "--detach",
        "--publish-all",
        "--name",
        &name,
        IMAGE_NAME,
        "ovsdb-server",
        "--remote=ptcp:6640:0.0.0.0",
        "--no-chdir",
        "Open_vSwitch.db",
    ])?;
    let id = String::from_utf8(output.stdout).context("decode docker run output")?;
    let id = id.trim().to_string();

    let port_output = run_docker_output(&["port", &id, "6640/tcp"])?;
    let port_text = String::from_utf8(port_output.stdout).context("decode docker port output")?;
    let port = port_text
        .trim()
        .rsplit(':')
        .next()
        .context("parse docker published port")?;

    Ok(ManagedContainer {
        id,
        host: "127.0.0.1".to_string(),
        port: port.to_string(),
    })
}

fn run_docker(args: &[&str]) -> Result<()> {
    let output = run_docker_output(args)?;
    if output.status.success() {
        return Ok(());
    }

    Err(anyhow::anyhow!(
        "docker {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr).trim()
    ))
}

fn run_docker_output(args: &[&str]) -> Result<std::process::Output> {
    Command::new("docker")
        .args(args)
        .output()
        .with_context(|| format!("run docker {}", args.join(" ")))
}

fn live_row(id_col: &str, map_col: &str, row_name: &str) -> Value {
    json!({
        id_col: row_name,
        map_col: ["map", []]
    })
}

fn live_where(id_col: &str, row_name: &str) -> Vec<Value> {
    vec![json!([id_col, "==", row_name])]
}

fn run_client_json(binary: &PathBuf, args: &[&str]) -> Result<Value> {
    let output = run_client_output(binary, args)?;
    Ok(serde_json::from_str(&output)?)
}

fn run_client_output(binary: &PathBuf, args: &[&str]) -> Result<String> {
    let output = Command::new(binary)
        .args(args)
        .output()
        .with_context(|| format!("run {} {}", binary.display(), args.join(" ")))?;
    if output.status.success() {
        return String::from_utf8(output.stdout).context("decode client output");
    }

    Err(anyhow::anyhow!(
        "{} {} failed: {}",
        binary.display(),
        args.join(" "),
        String::from_utf8_lossy(&output.stderr).trim()
    ))
}

fn resolve_rust_client_binary() -> Result<PathBuf> {
    if let Some(path) = env::var_os("CARGO_BIN_EXE_ovsdb") {
        let path = PathBuf::from(path);
        if path
            .components()
            .any(|component| component.as_os_str() == "release")
        {
            return Ok(path);
        }
    }

    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let release = root.join("target/release/ovsdb");
    if release.exists() {
        return Ok(release);
    }

    let status = Command::new("cargo")
        .args(["build", "--release", "--bin", "ovsdb"])
        .current_dir(&root)
        .status()
        .context("build release ovsdb binary")?;
    if !status.success() {
        anyhow::bail!("failed to build release ovsdb binary");
    }

    if release.exists() {
        return Ok(release);
    }

    anyhow::bail!("could not find release ovsdb binary")
}

impl ManagedContainer {
    fn server_uri(&self) -> String {
        format!("tcp:{}:{}", self.host, self.port)
    }
}

impl Drop for ManagedContainer {
    fn drop(&mut self) {
        let _ = Command::new("docker").args(["rm", "-f", &self.id]).output();
    }
}

fn choose_writable_table(schema: &DatabaseSchema) -> Result<(String, String, String)> {
    let preferred_tables = ["Manager", "Controller"];
    let identity_columns = ["target", "name", "address", "remote"];
    let map_columns = ["external_ids"];

    for table_name in preferred_tables {
        let Some(table) = schema.tables.get(table_name) else {
            continue;
        };

        let Some(identity) = identity_columns
            .iter()
            .find(|column| table.columns.contains_key(**column))
            .map(|column| (*column).to_string())
        else {
            continue;
        };

        let Some(map_column) = map_columns
            .iter()
            .find(|column| table.columns.contains_key(**column))
            .map(|column| (*column).to_string())
        else {
            continue;
        };

        return Ok((table_name.to_string(), identity, map_column));
    }

    anyhow::bail!("no writable table found in schema")
}

fn unique_name(prefix: &str) -> String {
    format!("{prefix}-{}", UNIQUE_SUFFIX.fetch_add(1, Ordering::Relaxed))
}

fn time_case<F>(iterations: u64, mut step: F) -> Result<Duration>
where
    F: FnMut() -> Result<()>,
{
    let started = Instant::now();
    for _ in 0..iterations {
        step()?;
    }
    Ok(started.elapsed())
}

fn time_case_collect<F>(iterations: u64, mut step: F) -> Result<(Duration, Vec<String>)>
where
    F: FnMut() -> Result<Option<String>>,
{
    let started = Instant::now();
    let mut row_names = Vec::new();
    for _ in 0..iterations {
        if let Some(row_name) = step()? {
            row_names.push(row_name);
        }
    }
    Ok((started.elapsed(), row_names))
}

fn print_compare_result(
    name: &str,
    iterations: u64,
    rust_elapsed: Duration,
    native_elapsed: Duration,
) {
    let rust_micros = rust_elapsed.as_secs_f64() * 1_000_000.0;
    let native_micros = native_elapsed.as_secs_f64() * 1_000_000.0;
    let iterations = iterations
        .to_string()
        .parse::<f64>()
        .unwrap_or(f64::INFINITY);
    let rust_avg = rust_micros / iterations;
    let native_avg = native_micros / iterations;
    let ratio = if rust_micros > 0.0 {
        native_micros / rust_micros
    } else {
        f64::INFINITY
    };
    let mut stdout = std::io::stdout().lock();
    writeln!(
        stdout,
        "{name}: {iterations} iterations | rust {rust_elapsed:?} (avg {rust_avg:.2} us/iter) | native {native_elapsed:?} (avg {native_avg:.2} us/iter) | native/rust {ratio:.2}x",
    )
    .ok();
}

fn bench_list_dbs(rust: &RustBenchClient, native: &NativeBenchClient) -> Result<()> {
    let rust_elapsed = time_case(BENCH_ITERATIONS, || {
        black_box(rust.list_dbs()?);
        Ok(())
    })?;
    let native_elapsed = time_case(BENCH_ITERATIONS, || {
        black_box(native.list_dbs()?);
        Ok(())
    })?;
    print_compare_result(
        "client_list_dbs",
        BENCH_ITERATIONS,
        rust_elapsed,
        native_elapsed,
    );
    Ok(())
}

fn bench_get_schema(rust: &RustBenchClient, native: &NativeBenchClient) -> Result<()> {
    let rust_elapsed = time_case(BENCH_ITERATIONS, || {
        black_box(rust.get_schema()?);
        Ok(())
    })?;
    let native_elapsed = time_case(BENCH_ITERATIONS, || {
        black_box(native.get_schema()?);
        Ok(())
    })?;
    print_compare_result(
        "client_get_schema",
        BENCH_ITERATIONS,
        rust_elapsed,
        native_elapsed,
    );
    Ok(())
}

fn bench_select(
    rust: &RustBenchClient,
    native: &NativeBenchClient,
    table: &str,
    id_col: &str,
    map_col: &str,
) -> Result<()> {
    let rust_elapsed = time_case(BENCH_ITERATIONS, || {
        black_box(rust.select_table(table, id_col, map_col)?);
        Ok(())
    })?;
    let native_elapsed = time_case(BENCH_ITERATIONS, || {
        black_box(native.select_table(table, id_col, map_col)?);
        Ok(())
    })?;
    print_compare_result(
        "client_select",
        BENCH_ITERATIONS,
        rust_elapsed,
        native_elapsed,
    );
    Ok(())
}

fn bench_insert(
    rust: &RustBenchClient,
    native: &NativeBenchClient,
    table: &str,
    id_col: &str,
    map_col: &str,
) -> Result<()> {
    let (rust_elapsed, rust_rows) = time_case_collect(BENCH_ITERATIONS, || {
        let row_name = unique_name("bench-insert");
        black_box(rust.insert_row(table, id_col, map_col, &row_name)?);
        Ok(Some(row_name))
    })?;
    rust.cleanup_rows(table, id_col, &rust_rows)?;
    let (native_elapsed, native_rows) = time_case_collect(BENCH_ITERATIONS, || {
        let row_name = unique_name("bench-insert");
        black_box(native.insert_row(table, id_col, map_col, &row_name)?);
        Ok(Some(row_name))
    })?;
    native.cleanup_rows(table, id_col, &native_rows)?;
    print_compare_result(
        "client_insert",
        BENCH_ITERATIONS,
        rust_elapsed,
        native_elapsed,
    );
    Ok(())
}

fn bench_update(
    rust: &RustBenchClient,
    native: &NativeBenchClient,
    table: &str,
    id_col: &str,
    map_col: &str,
) -> Result<()> {
    let (rust_elapsed, rust_rows) = time_case_collect(BENCH_ITERATIONS, || {
        let row_name = unique_name("bench-update");
        black_box(rust.update_row(table, id_col, map_col, &row_name)?);
        Ok(Some(row_name))
    })?;
    rust.cleanup_rows(table, id_col, &rust_rows)?;
    let (native_elapsed, native_rows) = time_case_collect(BENCH_ITERATIONS, || {
        let row_name = unique_name("bench-update");
        black_box(native.update_row(table, id_col, map_col, &row_name)?);
        Ok(Some(row_name))
    })?;
    native.cleanup_rows(table, id_col, &native_rows)?;
    print_compare_result(
        "client_insert_then_update",
        BENCH_ITERATIONS,
        rust_elapsed,
        native_elapsed,
    );
    Ok(())
}

fn bench_mutate(
    rust: &RustBenchClient,
    native: &NativeBenchClient,
    table: &str,
    id_col: &str,
    map_col: &str,
) -> Result<()> {
    let (rust_elapsed, rust_rows) = time_case_collect(BENCH_ITERATIONS, || {
        let row_name = unique_name("bench-mutate");
        black_box(rust.mutate_row(table, id_col, map_col, &row_name)?);
        Ok(Some(row_name))
    })?;
    rust.cleanup_rows(table, id_col, &rust_rows)?;
    let (native_elapsed, native_rows) = time_case_collect(BENCH_ITERATIONS, || {
        let row_name = unique_name("bench-mutate");
        black_box(native.mutate_row(table, id_col, map_col, &row_name)?);
        Ok(Some(row_name))
    })?;
    native.cleanup_rows(table, id_col, &native_rows)?;
    print_compare_result(
        "client_insert_then_mutate",
        BENCH_ITERATIONS,
        rust_elapsed,
        native_elapsed,
    );
    Ok(())
}

fn bench_delete(
    rust: &RustBenchClient,
    native: &NativeBenchClient,
    table: &str,
    id_col: &str,
    map_col: &str,
) -> Result<()> {
    let rust_elapsed = time_case(BENCH_ITERATIONS, || {
        let row_name = unique_name("bench-delete");
        black_box(rust.delete_row(table, id_col, map_col, &row_name)?);
        Ok(())
    })?;
    let native_elapsed = time_case(BENCH_ITERATIONS, || {
        let row_name = unique_name("bench-delete");
        black_box(native.delete_row(table, id_col, map_col, &row_name)?);
        Ok(())
    })?;
    print_compare_result(
        "client_insert_then_delete",
        BENCH_ITERATIONS,
        rust_elapsed,
        native_elapsed,
    );
    Ok(())
}

fn main() -> Result<()> {
    let bench = CompareBench::start()?;

    bench_list_dbs(&bench.rust, &bench.native)?;
    bench_get_schema(&bench.rust, &bench.native)?;
    bench_select(
        &bench.rust,
        &bench.native,
        &bench.table,
        &bench.id_col,
        &bench.map_col,
    )?;
    bench_insert(
        &bench.rust,
        &bench.native,
        &bench.table,
        &bench.id_col,
        &bench.map_col,
    )?;
    bench_update(
        &bench.rust,
        &bench.native,
        &bench.table,
        &bench.id_col,
        &bench.map_col,
    )?;
    bench_mutate(
        &bench.rust,
        &bench.native,
        &bench.table,
        &bench.id_col,
        &bench.map_col,
    )?;
    bench_delete(
        &bench.rust,
        &bench.native,
        &bench.table,
        &bench.id_col,
        &bench.map_col,
    )?;

    Ok(())
}
