use std::time::{Duration, Instant};
use tempfile::TempDir;
use wind_runtime::{Runtime, RuntimeOptions};
use wind_wire::{AppDirs, Content};

#[tokio::test]
async fn a_model_less_submit_reaches_the_failed_phase() {
    let dir = TempDir::new().expect("create a temp dir");
    let runtime = Runtime::open(RuntimeOptions::new(AppDirs::under(dir.path())))
        .await
        .expect("open the runtime");
    let mut opening = runtime.subscribe().await.expect("subscribe to the feed");
    let topic_id = runtime
        .storage()
        .create_topic(None, "smoke", None, None, None)
        .await
        .expect("create the topic");
    runtime
        .submit(topic_id, vec![Content::new_text("hello".into())])
        .await
        .expect("submit the input");

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut received = Vec::new();
    let mut failed = false;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, opening.subscription.recv()).await {
            Ok(Ok(Some(event))) => {
                received.push(event.envelope.name.clone());
                let phase = event
                    .envelope
                    .payload
                    .as_ref()
                    .and_then(|payload| payload.get("phase"))
                    .and_then(|phase| phase.as_str());
                if event.envelope.name == "core/agent.status" && phase == Some("failed") {
                    failed = true;
                    break;
                }
            }
            Ok(Ok(None)) => break,
            Ok(Err(error)) => panic!("feed error: {error}; received {received:?}"),
            Err(_) => break,
        }
    }
    assert!(
        failed,
        "expected a failed core/agent.status; received {received:?}"
    );
    runtime.shutdown().await.expect("shut down the runtime");
    drop(dir);
}
