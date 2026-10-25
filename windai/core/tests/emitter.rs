//! 验证取消令牌对分片发射的门控。

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use wind_core::agent::emitter::BlockEmitter;

/// 构造分片写入内存通道的发射器
fn emitter(cancel: CancellationToken) -> (BlockEmitter, mpsc::Receiver<String>) {
    let (tx, rx) = mpsc::channel(16);
    let emitter = BlockEmitter::new(
        1,
        1,
        cancel,
        Box::new(move |fragment| {
            let _ = tx.try_send(fragment);
        }),
    );
    (emitter, rx)
}

/// 未取消的运行照常推送分片
#[tokio::test]
async fn a_live_job_publishes_its_fragments() {
    let cancel = CancellationToken::new();
    let (emitter, mut rx) = emitter(cancel);
    emitter.publish("hel");
    emitter.publish("lo");
    assert_eq!(rx.recv().await, Some("hel".to_string()));
    assert_eq!(rx.recv().await, Some("lo".to_string()));
}

/// 取消前发布的分片照常到达，取消后发布的分片不再到达
#[tokio::test]
async fn a_cancelled_job_publishes_nothing() {
    let cancel = CancellationToken::new();
    let (emitter, mut rx) = emitter(cancel.clone());
    emitter.publish("hel");
    cancel.cancel();
    assert!(emitter.is_cancelled());
    emitter.publish("lo");
    assert_eq!(rx.recv().await, Some("hel".to_string()));
    assert!(
        rx.try_recv().is_err(),
        "fragments published after cancellation must not be pushed to the frontend, otherwise the UI keeps growing text after a stop"
    );
}

/// 发射器携带所服务节点与本次运行的标识
#[tokio::test]
async fn an_emitter_carries_the_identity_of_its_job() {
    let cancel = CancellationToken::new();
    let (emitter, _rx) = emitter(cancel);
    assert_eq!(emitter.node(), 1);
    assert_eq!(emitter.job(), 1);
    assert!(!emitter.is_cancelled());
}
