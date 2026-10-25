use super::event_stream::EventStream;
use futures::Stream;

pub trait Eventsource: Sized {
    fn eventsource(self) -> EventStream<Self>;
}

impl<S, B, E> Eventsource for S
where
    S: Stream<Item = Result<B, E>>,
    B: AsRef<[u8]>,
{
    fn eventsource(self) -> EventStream<Self> {
        EventStream::new(self)
    }
}
