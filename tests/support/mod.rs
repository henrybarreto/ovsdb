#![allow(dead_code)]
use anyhow::{Context, Result};
use ovsdb::client::Connection as Client;
use serde_json::Value;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Duration;
use testcontainers::{
    core::{AccessMode, ExecCommand, IntoContainerPort, Mount, WaitFor},
    runners::{SyncBuilder, SyncRunner},
    Container, GenericBuildableImage, GenericImage, ImageExt,
};

static OVSDB_IMAGE: OnceLock<GenericImage> = OnceLock::new();
static UNIQUE_SUFFIX: AtomicU64 = AtomicU64::new(1);

/// Reusable integration-test harness for a real `ovsdb-server`.
pub struct TestOvsDBClient {
    pub container: Container<GenericImage>,
    pub client: ovsdb::client::Connection,
    pub host: String,
    pub port: u16,
    pub addr: String,
}

fn build_ovsdb_image() -> GenericImage {
    let image = OVSDB_IMAGE.get_or_init(|| {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

        GenericBuildableImage::new("ovsdb-server", "test")
            .with_dockerfile(root.join("Containerfile.ovsdb-server"))
            .build_image()
            .expect("failed to build ovsdb-server image")
    });

    image.clone()
}

pub fn exec_text(container: &Container<GenericImage>, cmd: Vec<&str>) -> Result<String> {
    let mut res = container
        .exec(ExecCommand::new(cmd))
        .context("exec command in ovsdb container")?;

    let mut out = String::new();
    res.stdout()
        .read_to_string(&mut out)
        .context("read exec stdout")?;
    res.stderr()
        .read_to_string(&mut out)
        .context("read exec stderr")?;

    Ok(out)
}

pub fn kill_ovsdb_server(container: &Container<GenericImage>) -> Result<()> {
    exec_text(
        container,
        vec![
            "sh",
            "-c",
            "if [ -f /tmp/ovsdb.pid ]; then kill -9 $(cat /tmp/ovsdb.pid) || true; fi; \
             if [ -f /tmp/ovsdb-custom.pid ]; then kill -9 $(cat /tmp/ovsdb-custom.pid) || true; fi; \
             pkill -9 ovsdb-server || true",
        ],
    )
    .map(|_| ())
}

fn connect_with_retry(addr: &str) -> Result<Client> {
    let mut last_err = None;

    for _ in 0..20 {
        match Client::connect(addr, None) {
            Ok(client) => return Ok(client),
            Err(err) => {
                last_err = Some(err);
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }

    Err(anyhow::anyhow!(
        "failed to connect to ovsdb-server at {addr}: {last_err:?}"
    ))
}

impl TestOvsDBClient {
    pub fn start_plain() -> Result<Self> {
        let image = build_ovsdb_image();

        let container = image
            .with_exposed_port(6640.tcp())
            .with_wait_for(WaitFor::seconds(2))
            .start()
            .context("start ovsdb-server container")?;

        let host = container
            .get_host()
            .context("get ovsdb-server host")?
            .to_string();

        let port = container
            .get_host_port_ipv4(6640.tcp())
            .context("get ovsdb-server mapped port")?;

        let addr = format!("tcp:{host}:{port}");
        let client = Client::connect(&addr, None)
            .with_context(|| format!("connect to ovsdb-server at {addr}"))?;

        Ok(Self {
            container,
            client,
            host,
            port,
            addr,
        })
    }

    pub fn start_tls() -> Result<Self> {
        anyhow::bail!("TLS harness not implemented yet")
    }

    pub fn start_with_schema(schema_path: &str, db_name: &str) -> Result<Self> {
        let image = build_ovsdb_image();
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let schema_abs = root.join(schema_path);

        if !schema_abs.exists() {
            anyhow::bail!("schema file does not exist: {}", schema_abs.display());
        }

        let container = image
            .with_exposed_port(6640.tcp())
            .with_cmd(["sleep", "infinity"])
            .with_mount(
                Mount::bind_mount(
                    schema_abs.to_string_lossy().into_owned(),
                    "/tmp/schema.ovsschema",
                )
                .with_access_mode(AccessMode::ReadOnly),
            )
            .start()
            .context("start custom-schema ovsdb container")?;

        exec_text(
            &container,
            vec![
                "sh",
                "-c",
                "rm -f /tmp/custom.db && ovsdb-tool create /tmp/custom.db /tmp/schema.ovsschema",
            ],
        )
        .context("create custom ovsdb database")?;

        exec_text(
            &container,
            vec![
                "sh",
                "-c",
                "ovsdb-server --remote=ptcp:6640:0.0.0.0 --no-chdir --pidfile=/tmp/ovsdb-custom.pid --log-file=/tmp/ovsdb-custom.log /tmp/custom.db --detach",
            ],
        )
        .context("start custom-schema ovsdb-server")?;

        let host = container
            .get_host()
            .context("get custom-schema ovsdb host")?
            .to_string();

        let port = container
            .get_host_port_ipv4(6640.tcp())
            .context("get custom-schema ovsdb mapped port")?;
        let addr = format!("tcp:{host}:{port}");

        let client = connect_with_retry(&addr)
            .with_context(|| format!("connect to custom-schema ovsdb-server at {addr}"))?;

        let dbs = client
            .list_dbs()
            .with_context(|| format!("list databases on custom-schema ovsdb-server at {addr}"))?;
        if !dbs.iter().any(|db| db == db_name) {
            let logs = exec_text(
                &container,
                vec!["sh", "-c", "cat /tmp/ovsdb-custom.log || true"],
            )
            .unwrap_or_default();
            anyhow::bail!(
                "custom schema database {db_name:?} not found in list_dbs: {dbs:?}\nlogs:\n{logs}"
            );
        }

        Ok(Self {
            container,
            client,
            host,
            port,
            addr,
        })
    }

    pub fn second_client(&self) -> Result<Client> {
        Client::connect(&self.addr, None)
            .with_context(|| format!("connect second client to {}", self.addr))
    }

    pub fn raw_tcp_stream(&self) -> Result<TcpStream> {
        let addr = format!("{}:{}", self.host, self.port);

        TcpStream::connect(&addr).with_context(|| format!("open raw TCP stream to {addr}"))
    }

    pub fn raw_json_rpc_stream(&self) -> Result<RawJsonRpcStream> {
        let stream = self.raw_tcp_stream()?;
        Ok(RawJsonRpcStream::new(stream.try_clone()?, stream))
    }

    pub fn restart_custom_server(&self) -> Result<()> {
        exec_text(
            &self.container,
            vec![
                "sh",
                "-c",
                "ovsdb-server --remote=ptcp:6640:0.0.0.0 --no-chdir --pidfile=/tmp/ovsdb-custom.pid --log-file=/tmp/ovsdb-custom.log /tmp/custom.db --detach",
            ],
        )
        .map(|_| ())
    }
}

pub fn send_raw_json(stream: &mut TcpStream, value: &Value) -> Result<()> {
    let mut bytes = serde_json::to_vec(value).context("serialize raw JSON-RPC value")?;
    bytes.push(b'\n');

    stream
        .write_all(&bytes)
        .context("write raw JSON-RPC message")?;
    stream.flush().context("flush raw JSON-RPC message")?;

    Ok(())
}

pub fn read_raw_json(stream: &mut TcpStream) -> Result<Value> {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();

    reader
        .read_line(&mut line)
        .context("read raw JSON-RPC response line")?;

    if line.is_empty() {
        anyhow::bail!("connection closed before JSON-RPC response");
    }

    let mut values = serde_json::Deserializer::from_str(&line).into_iter::<Value>();
    values
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing raw JSON-RPC response"))?
        .context("parse raw JSON-RPC response")
}

/// Buffered raw JSON-RPC stream helper that can respond to server-initiated echo probes.
pub struct RawJsonRpcStream {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    buf: Vec<u8>,
}

impl RawJsonRpcStream {
    pub fn new(reader: TcpStream, writer: TcpStream) -> Self {
        Self {
            reader: BufReader::new(reader),
            writer,
            buf: Vec::new(),
        }
    }

    pub fn send(&mut self, value: &Value) -> Result<()> {
        let mut bytes = serde_json::to_vec(value).context("serialize raw JSON-RPC value")?;
        bytes.push(b'\n');
        self.writer
            .write_all(&bytes)
            .context("write raw JSON-RPC message")?;
        self.writer.flush().context("flush raw JSON-RPC message")?;
        Ok(())
    }

    fn try_parse_buffer(&mut self) -> Result<Option<Value>> {
        if self.buf.is_empty() {
            return Ok(None);
        }

        let mut iter = serde_json::Deserializer::from_slice(&self.buf).into_iter::<Value>();
        match iter.next() {
            Some(Ok(value)) => {
                let consumed = iter.byte_offset();
                let remainder = self.buf.split_off(consumed);
                self.buf = remainder;
                Ok(Some(value))
            }
            Some(Err(err)) => {
                if err.is_eof() {
                    Ok(None)
                } else {
                    Err(anyhow::Error::new(err).context("parse raw JSON-RPC message"))
                }
            }
            None => Ok(None),
        }
    }

    pub fn recv(&mut self) -> Result<Value> {
        loop {
            if let Some(value) = self.try_parse_buffer()? {
                return Ok(value);
            }

            let mut temp = [0u8; 4096];
            let n = self
                .reader
                .read(&mut temp)
                .context("read raw JSON-RPC response bytes")?;
            if n == 0 {
                anyhow::bail!("connection closed before JSON-RPC response");
            }
            self.buf.extend_from_slice(&temp[..n]);
        }
    }

    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<Value> {
        self.reader.get_mut().set_read_timeout(Some(timeout))?;
        let result = self.recv();
        self.reader.get_mut().set_read_timeout(None).ok();
        result
    }

    pub fn recv_responding_to_echo(&mut self) -> Result<Value> {
        loop {
            let value = self.recv()?;
            if value.get("method").and_then(Value::as_str) == Some("echo") {
                let id = value.get("id").cloned().unwrap_or(Value::Null);
                let params = value
                    .get("params")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!([]));
                self.send(&serde_json::json!({
                    "id": id,
                    "result": params,
                    "error": null
                }))?;
                continue;
            }

            return Ok(value);
        }
    }

    pub fn recv_responding_to_echo_timeout(&mut self, timeout: Duration) -> Result<Value> {
        let start = std::time::Instant::now();
        loop {
            let elapsed = start.elapsed();
            if elapsed >= timeout {
                anyhow::bail!("timeout waiting for raw JSON-RPC response");
            }
            let value = self.recv_timeout(timeout.saturating_sub(elapsed))?;
            if value.get("method").and_then(Value::as_str) == Some("echo") {
                let id = value.get("id").cloned().unwrap_or(Value::Null);
                let params = value
                    .get("params")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!([]));
                self.send(&serde_json::json!({
                    "id": id,
                    "result": params,
                    "error": null
                }))?;
                continue;
            }

            return Ok(value);
        }
    }
}

pub fn unique_name(prefix: &str) -> String {
    let n = UNIQUE_SUFFIX.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{n}")
}
