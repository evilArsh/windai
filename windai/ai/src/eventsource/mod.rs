//! 从字节流构造事件流，并解析服务端发送事件（SSE）

mod event;
mod event_stream;
mod parser;
mod traits;
mod utf8_stream;

pub use event::Event;
pub use event_stream::{EventStream, EventStreamError};
pub use traits::Eventsource;

#[cfg(test)]
mod tests {
    use super::{Event, EventStream};
    use futures::prelude::*;

    /// 把一段字节送进解析器，收下全部事件
    async fn events_of(chunks: Vec<&'static [u8]>) -> Vec<Event> {
        EventStream::new(futures::stream::iter(
            chunks.into_iter().map(Ok::<_, std::io::Error>),
        ))
        .try_collect::<Vec<_>>()
        .await
        .expect("parse every event")
    }

    #[tokio::test]
    async fn fields_become_one_event() {
        let events = events_of(vec![b"event: message\ndata: {\"a\":1}\nid: 7\n\n"]).await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event, "message");
        assert_eq!(events[0].data, "{\"a\":1}");
        assert_eq!(events[0].id, "7");
    }

    #[tokio::test]
    async fn a_data_spanning_two_chunks_is_reassembled() {
        // 分块边界落在字段中间是常态，不是异常
        let events = events_of(vec![b"data: {\"a\"", b":1}\n\n"]).await;
        assert_eq!(events[0].data, "{\"a\":1}");
    }

    #[tokio::test]
    async fn a_multibyte_character_split_across_chunks_survives() {
        let text = "你好".as_bytes();
        let first = &text[..1];
        let second = &text[1..];
        let mut chunks = vec![b"data: ".as_slice()];
        chunks.push(first);
        chunks.push(second);
        chunks.push(b"\n\n");
        let events = events_of(chunks).await;
        assert_eq!(
            events[0].data, "你好",
            "a multibyte character split across chunks must not be truncated"
        );
    }

    #[tokio::test]
    async fn a_comment_line_is_not_data() {
        let events = events_of(vec![b": keep-alive\ndata: x\n\n"]).await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "x");
    }

    #[tokio::test]
    async fn several_events_in_one_chunk_are_all_delivered() {
        let events = events_of(vec![b"data: a\n\ndata: b\n\n"]).await;
        let data: Vec<&str> = events.iter().map(|event| event.data.as_str()).collect();
        assert_eq!(data, vec!["a", "b"]);
    }
}
