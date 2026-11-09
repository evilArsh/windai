use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};

use tempfile::TempDir;
use wind_mcp::connector::Connection;
use wind_mcp::options::McpOptions;
use wind_mcp::roots::Roots;
use wind_mcp::runner::plan;
use wind_mcp::spec::{ServiceSpec, StdioSpec, Transport};

pub async fn open_test_server() -> Arc<Connection> {
    open_test_server_with(McpOptions::default()).await
}

pub async fn open_test_server_with(options: McpOptions) -> Arc<Connection> {
    let command = env!("CARGO_BIN_EXE_mcp_test_server").to_owned();
    let cwd = temporary_directory();
    let spec = ServiceSpec {
        name: "test".to_owned(),
        description: None,
        transport: Transport::Stdio(StdioSpec {
            command: command.clone(),
            args: Vec::new(),
            env: BTreeMap::new(),
            cwd: cwd.clone(),
        }),
    };
    let roots = Roots::new(
        PathBuf::from(&command)
            .parent()
            .expect("the test server binary must have a parent")
            .to_path_buf(),
    );
    let Transport::Stdio(stdio) = &spec.transport else {
        panic!("the test server must use the stdio transport");
    };
    let plan = plan(stdio, &roots).expect("the test server must plan");
    Connection::open(&spec, &plan, &options)
        .await
        .expect("the test server must connect")
}

fn temporary_directory() -> PathBuf {
    static DIRECTORIES: OnceLock<Mutex<Vec<TempDir>>> = OnceLock::new();
    let directory = TempDir::new().expect("create a temporary directory");
    let path = directory.path().to_path_buf();
    DIRECTORIES
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .expect("lock the temporary directories")
        .push(directory);
    path
}
