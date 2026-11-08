#[path = "../common/lib.rs"]
mod common;

#[tokio::main]
async fn main() {
    common::run_stdio().await;
}
