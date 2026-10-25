use reqwest::{Client, ClientBuilder};
use std::{sync::OnceLock, time::Duration};

/// 单次读取的空闲上限，不限整段生成的时长
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(120);

static HTTP_CLIENT: OnceLock<Result<Client, String>> = OnceLock::new();

pub fn create_new() -> Result<Client, String> {
    ClientBuilder::new()
        .read_timeout(DEFAULT_TIMEOUT)
        .build()
        .map_err(|error| error.to_string())
}

pub fn get() -> Result<Client, String> {
    HTTP_CLIENT.get_or_init(create_new).clone()
}
