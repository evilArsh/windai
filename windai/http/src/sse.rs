use std::convert::Infallible;
use std::task::Poll;

use axum::response::sse::Event;
use futures::{Stream, StreamExt};
use serde::Serialize;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_util::sync::CancellationToken;

pub fn event_stream<T>(
    rx: broadcast::Receiver<T>,
    cancel: CancellationToken,
) -> impl Stream<Item = Result<Event, Infallible>>
where
    T: Serialize + AsRef<str> + Clone + Send + 'static,
{
    let mut seq: u64 = 0;
    let mut cancel = Box::pin(cancel.cancelled_owned());
    let mut events = Box::pin(BroadcastStream::new(rx).filter_map(move |item| {
        seq += 1;
        let event = match item {
            Ok(ev) => ev,
            Err(_) => return futures::future::ready(None),
        };
        let ev = Event::default()
            .id(seq.to_string())
            .event(event.as_ref())
            .json_data(&event)
            .ok()
            .map(Ok::<_, Infallible>);
        futures::future::ready(ev)
    }));
    futures::stream::poll_fn(move |cx| {
        if cancel.as_mut().poll(cx).is_ready() {
            return Poll::Ready(None);
        }
        events.as_mut().poll_next(cx)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ends_immediately_when_already_cancelled() {
        let (tx, rx) = broadcast::channel::<String>(16);
        drop(tx);
        let token = CancellationToken::new();
        token.cancel();
        let mut stream = event_stream(rx, token);
        assert!(
            stream.next().await.is_none(),
            "已取消的 token 必须立即终止流"
        );
    }

    #[tokio::test]
    async fn yields_frames_then_ends_on_cancel() {
        let (tx, rx) = broadcast::channel::<String>(16);
        let token = CancellationToken::new();
        let mut stream = event_stream(rx, token.clone());

        tx.send("ping".to_string()).unwrap();
        assert!(stream.next().await.is_some(), "应能读到事件帧");

        token.cancel();
        assert!(stream.next().await.is_none(), "cancel 后流必须终止");
    }
}
