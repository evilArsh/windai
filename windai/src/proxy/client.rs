use reqwest::{Client, ClientBuilder};
use std::{sync::OnceLock, time::Duration};

static HTTP_CLIENT: OnceLock<Client> = OnceLock::new();

/// create a new http client
/// # Panics
/// This function will panic if the client creation fails
pub fn create_new() -> Client {
    ClientBuilder::new()
        .timeout(Duration::from_secs(600))
        .build()
        .expect("create request client failed")
}

/// get a clone of a global http client
/// # Panics
/// This function will panic if the client creation fails
pub fn get() -> Client {
    HTTP_CLIENT.get_or_init(create_new).clone()
}
