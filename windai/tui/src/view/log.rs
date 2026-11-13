use super::pane_offset;
use crate::text::{concise, format_time, summarize};
use crate::theme;
use chrono::Local;
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Paragraph};
use std::collections::VecDeque;
use std::sync::Arc;
use unicode_width::UnicodeWidthStr;
use wind_wire::event::{FeedEvent, Scope};

const LOG_CAPACITY: usize = 2000;
/// 每条日志占的行数：标题、摘要、一条空白
const ENTRY_ROWS: usize = 3;
const SUMMARY_INDENT: &str = "  ";
const SUMMARY_BRANCH: &str = "└ ";

#[derive(Debug, Clone, PartialEq)]
pub struct LogEntry {
    pub time: String,
    pub name: String,
    pub seq: Option<i64>,
    pub summary: String,
    /// 事件原文，本地消息为空
    pub event: Option<Arc<FeedEvent>>,
}

/// 日志标题行的可点击区域
#[derive(Debug, Clone, PartialEq)]
pub struct LogHit {
    pub rect: Rect,
    pub event: Arc<FeedEvent>,
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
            event: Some(Arc::new(event.clone())),
        });
        true
    }

    pub fn push_local(&mut self, message: &str) {
        self.push_entry(LogEntry {
            time: format_time(Local::now().timestamp_millis()),
            name: "wind-tui".to_owned(),
            seq: None,
            summary: message.to_owned(),
            event: None,
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

pub fn render(
    frame: &mut Frame,
    area: Rect,
    log: &LogRing,
    scroll: &mut u16,
    focused: bool,
    hits: &mut Vec<LogHit>,
) -> u16 {
    hits.clear();
    if area.is_empty() {
        return 0;
    }
    let border = if focused { theme::FOCUS } else { theme::BORDER };
    let block = Block::bordered()
        .border_style(border)
        .title("Layer 3 log")
        .title_style(theme::TITLE);
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if inner.is_empty() {
        return 0;
    }
    let total = u16::try_from(log.entries.len().saturating_mul(ENTRY_ROWS)).unwrap_or(u16::MAX);
    let (offset, max) = pane_offset(total, inner.height, *scroll);
    if *scroll != u16::MAX {
        *scroll = offset;
    }
    let first_entry = usize::from(offset) / ENTRY_ROWS;
    let skip_line = usize::from(offset) % ENTRY_ROWS;
    let width = usize::from(inner.width);
    let capacity = usize::from(inner.height).saturating_add(skip_line);
    let mut rows: Vec<(Line<'static>, Option<Arc<FeedEvent>>)> = Vec::with_capacity(capacity);
    for entry in log.entries.iter().skip(first_entry) {
        if rows.len() >= capacity {
            break;
        }
        let [headline, summary, blank] = entry_lines(entry, width);
        rows.push((headline, entry.event.clone()));
        rows.push((summary, None));
        rows.push((blank, None));
    }
    let visible = rows
        .into_iter()
        .skip(skip_line)
        .take(usize::from(inner.height))
        .collect::<Vec<_>>();
    for (row, (_, event)) in visible.iter().enumerate() {
        if let Some(event) = event {
            hits.push(LogHit {
                rect: Rect::new(
                    inner.x,
                    inner
                        .y
                        .saturating_add(u16::try_from(row).unwrap_or(u16::MAX)),
                    inner.width,
                    1,
                ),
                event: Arc::clone(event),
            });
        }
    }
    let lines = visible
        .into_iter()
        .map(|(line, _)| line)
        .collect::<Vec<_>>();
    frame.render_widget(Paragraph::new(lines), inner);
    max
}

fn entry_lines(entry: &LogEntry, width: usize) -> [Line<'static>; ENTRY_ROWS] {
    [
        headline(entry, width),
        summary(entry, width),
        Line::default(),
    ]
}

fn headline(entry: &LogEntry, width: usize) -> Line<'static> {
    let mut spans = Vec::new();
    if let Some(seq) = entry.seq {
        spans.push(Span::styled(format!("#{seq}"), theme::LOG_INDEX));
        spans.push(Span::raw(" "));
    }
    let used = span_width(&spans).saturating_add(entry.name.width());
    spans.push(Span::styled(entry.name.clone(), event_style(&entry.name)));
    let time_width = entry.time.width();
    if used + 1 + time_width <= width {
        spans.push(Span::raw(" ".repeat(width - used - time_width)));
        spans.push(Span::styled(entry.time.clone(), theme::LOG_INDEX));
    }
    Line::from(spans)
}

fn summary(entry: &LogEntry, width: usize) -> Line<'static> {
    let indent = SUMMARY_INDENT.width() + SUMMARY_BRANCH.width();
    let text = concise(&entry.summary, width.saturating_sub(indent));
    Line::from(vec![
        Span::raw(SUMMARY_INDENT),
        Span::styled(SUMMARY_BRANCH.to_owned(), theme::LOG_INDEX),
        Span::styled(text, theme::LOG_BODY),
    ])
}

fn event_style(name: &str) -> Style {
    let Some((family, event)) = name.split_once('/') else {
        return theme::LOG_LOCAL;
    };
    if family != "core" {
        return theme::LOG_LOCAL;
    }
    if event.starts_with("agent") {
        theme::LOG_AGENT
    } else if event.starts_with("mod") {
        theme::LOG_MOD
    } else if event.starts_with("mcp") {
        theme::LOG_MCP
    } else if event.starts_with("block")
        || event.starts_with("message")
        || event.starts_with("input")
    {
        theme::LOG_LLM
    } else {
        theme::LOG_CORE
    }
}

fn span_width(spans: &[Span<'static>]) -> usize {
    spans.iter().map(|span| span.content.width()).sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use ratatui::style::Modifier;
    use serde_json::json;
    use wind_wire::event::{Envelope, EventKey, Origin, Shape, Source, Target};

    fn entry(seq: Option<i64>, name: &str, summary: &str) -> LogEntry {
        LogEntry {
            time: "12:04:31".to_owned(),
            name: name.to_owned(),
            seq,
            summary: summary.to_owned(),
            event: None,
        }
    }

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

    #[test]
    fn headline_puts_the_sequence_and_the_colored_name_first() {
        let entry = entry(Some(1842), "core/block.delta", "msg 92 seq 1");
        let line = headline(&entry, 40);
        let rendered = line.to_string();
        assert!(rendered.starts_with("#1842 core/block.delta"), "{rendered}");
        assert!(rendered.ends_with("12:04:31"), "{rendered}");
        assert_eq!(rendered.width(), 40);
        assert_eq!(line.spans[0].style, theme::LOG_INDEX);
        assert_eq!(line.spans[2].style, theme::LOG_LLM);
    }

    #[test]
    fn narrow_headlines_leave_out_the_time() {
        let entry = entry(Some(7), "core/agent.spawn", "agent 3 parent -");
        let line = headline(&entry, 20);
        assert_eq!(line.to_string(), "#7 core/agent.spawn");
    }

    #[test]
    fn local_entries_have_no_sequence_number() {
        let entry = entry(None, "wind-tui", "resubscribing");
        let line = headline(&entry, 40);
        assert!(line.to_string().starts_with("wind-tui"));
        assert_eq!(line.spans[0].style, theme::LOG_LOCAL);
    }

    #[test]
    fn summaries_are_indented_and_dim() {
        let entry = entry(Some(7), "core/agent.spawn", "agent 3 parent -");
        let line = summary(&entry, 40);
        assert_eq!(line.to_string(), "  └ agent 3 parent -");
        assert!(line.spans[2].style.add_modifier.contains(Modifier::DIM));
    }

    #[test]
    fn summaries_are_truncated_to_the_pane_width() {
        let entry = entry(Some(7), "core/agent.spawn", "0123456789abcdef");
        let line = summary(&entry, 10);
        assert!(line.width() <= 10);
        assert!(line.to_string().ends_with('…'));
    }

    #[test]
    fn event_names_are_colored_by_family() {
        assert_eq!(event_style("core/agent.spawn"), theme::LOG_AGENT);
        assert_eq!(event_style("core/mod.installed"), theme::LOG_MOD);
        assert_eq!(event_style("core/mcp.started"), theme::LOG_MCP);
        assert_eq!(event_style("core/block.delta"), theme::LOG_LLM);
        assert_eq!(event_style("core/message.seal"), theme::LOG_LLM);
        assert_eq!(event_style("core/input.accepted"), theme::LOG_LLM);
        assert_eq!(event_style("core/other.event"), theme::LOG_CORE);
        assert_eq!(event_style("wind-tui"), theme::LOG_LOCAL);
    }

    #[test]
    fn entries_gain_a_blank_row_between_them() {
        let first = entry(Some(1), "core/agent.spawn", "agent 1");
        let lines = entry_lines(&first, 40);
        assert_eq!(lines.len(), ENTRY_ROWS);
        assert!(lines[ENTRY_ROWS - 1].to_string().is_empty());
    }

    #[test]
    fn rendering_records_a_hit_for_each_headline_with_an_event() {
        let mut log = LogRing::new();
        assert!(log.record(7, &event(Scope::Global, 1)));
        log.push_local("local");
        assert!(log.record(7, &event(Scope::Global, 2)));

        let mut terminal = Terminal::new(TestBackend::new(40, 12)).expect("create a test terminal");
        let mut scroll = u16::MAX;
        let mut hits = Vec::new();
        terminal
            .draw(|frame| {
                render(frame, frame.area(), &log, &mut scroll, false, &mut hits);
            })
            .expect("draw the frame");
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].rect, Rect::new(1, 1, 38, 1));
        assert_eq!(hits[1].rect, Rect::new(1, 7, 38, 1));
        assert_eq!(hits[1].event.key.seq, 2);
    }

    #[test]
    fn hits_follow_the_scrolled_rows() {
        let mut log = LogRing::new();
        for seq in 1..=4 {
            assert!(log.record(7, &event(Scope::Global, seq)));
        }

        let mut terminal = Terminal::new(TestBackend::new(40, 8)).expect("create a test terminal");
        let mut scroll = 5;
        let mut hits = Vec::new();
        terminal
            .draw(|frame| {
                render(frame, frame.area(), &log, &mut scroll, false, &mut hits);
            })
            .expect("draw the frame");
        assert_eq!(scroll, 5);
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].rect.y, 2);
        assert_eq!(hits[0].event.key.seq, 3);
        assert_eq!(hits[1].rect.y, 5);
        assert_eq!(hits[1].event.key.seq, 4);
    }
}
