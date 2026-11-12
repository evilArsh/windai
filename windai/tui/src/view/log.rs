use crate::text::{concise, format_time, summarize};
use crate::theme;
use chrono::Local;
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::Line;
use ratatui::widgets::{Block, Paragraph};
use std::collections::VecDeque;
use wind_wire::event::{FeedEvent, Scope};

const LOG_CAPACITY: usize = 2000;

#[derive(Debug, Clone, PartialEq)]
pub struct LogEntry {
    pub time: String,
    pub name: String,
    pub seq: Option<i64>,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogRing {
    entries: VecDeque<LogEntry>,
}

impl LogRing {
    pub fn new() -> Self {
        Self {
            entries: VecDeque::new(),
        }
    }

    pub fn entries(&self) -> impl Iterator<Item = &LogEntry> {
        self.entries.iter()
    }

    pub fn record(&mut self, topic_id: i64, event: &FeedEvent) -> bool {
        let relevant = match &event.envelope.scope {
            Scope::Global => true,
            Scope::Topic {
                topic_id: event_topic,
                ..
            } => *event_topic == topic_id,
        };
        if !relevant {
            return false;
        }
        let summary = match event.envelope.payload.as_ref() {
            Some(payload) => summarize(&event.envelope.name, payload),
            None => "-".to_owned(),
        };
        self.push_entry(LogEntry {
            time: format_time(event.envelope.time),
            name: event.envelope.name.clone(),
            seq: Some(event.key.seq),
            summary,
        });
        true
    }

    pub fn push_local(&mut self, message: &str) {
        self.push_entry(LogEntry {
            time: format_time(Local::now().timestamp_millis()),
            name: "wind-tui".to_owned(),
            seq: None,
            summary: message.to_owned(),
        });
    }

    fn push_entry(&mut self, entry: LogEntry) {
        if self.entries.len() == LOG_CAPACITY {
            self.entries.pop_front();
        }
        self.entries.push_back(entry);
    }
}

impl Default for LogRing {
    fn default() -> Self {
        Self::new()
    }
}

pub fn render(frame: &mut Frame, area: Rect, log: &LogRing) {
    if area.is_empty() {
        return;
    }
    let block = Block::bordered()
        .border_style(theme::BORDER)
        .title("Layer 3 log")
        .title_style(theme::TITLE);
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if inner.is_empty() {
        return;
    }
    let capacity = usize::from(inner.height) / 2;
    let skipped = log.entries.len().saturating_sub(capacity);
    let width = usize::from(inner.width);
    let mut lines = Vec::with_capacity(capacity * 2);
    for entry in log.entries.iter().skip(skipped) {
        let headline = match entry.seq {
            Some(seq) => format!("{} {} #{seq}", entry.time, entry.name),
            None => format!("{} {}", entry.time, entry.name),
        };
        lines.push(Line::from(headline));
        lines.push(Line::from(concise(&entry.summary, width)));
    }
    frame.render_widget(Paragraph::new(lines), inner);
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wind_wire::event::{Envelope, EventKey, Origin, Shape, Source, Target};

    fn event(scope: Scope, seq: i64) -> FeedEvent {
        FeedEvent {
            key: EventKey::new(1, seq).expect("build an event key"),
            envelope: Envelope {
                time: 0,
                name: "core/agent.sealed".to_owned(),
                scope,
                origin: Origin::Internal,
                source: Source::core("test"),
                payload: Some(json!({"instance_id": 3})),
                shape: Shape::Observe,
            },
            target: Target::Core,
        }
    }

    #[test]
    fn record_keeps_current_topic_and_global_events() {
        let mut log = LogRing::new();
        let current = Scope::Topic {
            topic_id: 7,
            instance_id: 1,
        };
        let other = Scope::Topic {
            topic_id: 8,
            instance_id: 1,
        };
        assert!(log.record(7, &event(current, 1)));
        assert!(log.record(7, &event(Scope::Global, 2)));
        assert!(!log.record(7, &event(other, 3)));
        assert_eq!(log.entries.len(), 2);
        assert_eq!(log.entries.front().map(|entry| entry.seq), Some(Some(1)));
        assert_eq!(log.entries.back().map(|entry| entry.seq), Some(Some(2)));
    }

    #[test]
    fn record_drops_the_oldest_entry_past_capacity() {
        let mut log = LogRing::new();
        for seq in 1..=LOG_CAPACITY as i64 + 1 {
            assert!(log.record(7, &event(Scope::Global, seq)));
        }
        assert_eq!(log.entries.len(), LOG_CAPACITY);
        assert_eq!(log.entries.front().map(|entry| entry.seq), Some(Some(2)));
        assert_eq!(
            log.entries.back().map(|entry| entry.seq),
            Some(Some(LOG_CAPACITY as i64 + 1))
        );
    }
}
