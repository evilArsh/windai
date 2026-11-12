use tempfile::TempDir;
use wind_runtime::{Runtime, RuntimeOptions};
use wind_wire::AppDirs;

#[tokio::test]
async fn opens_nested_directories_and_closes_all_subscriptions_idempotently() {
    let tmp = TempDir::new().unwrap();
    let dirs = AppDirs::under(tmp.path().join("nested").join("runtime"));
    let runtime = Runtime::open(RuntimeOptions::new(dirs.clone()))
        .await
        .unwrap();
    for dir in [
        dirs.mods_root(),
        dirs.topics_root(),
        dirs.public_skills(),
        dirs.bytecode_root(),
    ] {
        assert!(dir.is_dir());
    }
    let mut a = runtime.subscribe().await.unwrap();
    let mut b = runtime.subscribe().await.unwrap();
    assert_eq!(a.cut, b.cut);
    let (first, second) = tokio::join!(runtime.shutdown(), runtime.shutdown());
    first.unwrap();
    second.unwrap();
    assert!(a.subscription.recv().await.unwrap().is_none());
    assert!(b.subscription.recv().await.unwrap().is_none());
    runtime.shutdown().await.unwrap();
    assert!(runtime.subscribe().await.is_err());
}

#[tokio::test]
async fn invalid_feed_options_can_be_corrected_and_reopened() {
    let tmp = TempDir::new().unwrap();
    let dirs = AppDirs::under(tmp.path());
    let mut options = RuntimeOptions::new(dirs.clone());
    options.feed.batch_size = 0;
    assert!(Runtime::open(options).await.is_err());
    let runtime = Runtime::open(RuntimeOptions::new(dirs)).await.unwrap();
    runtime.shutdown().await.unwrap();
}
