use crate::text::concise;
use crate::theme;
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use std::env;
use std::fs;
use unicode_width::UnicodeWidthStr;
use wind_wire::Usage;

const SEPARATOR: &str = " | ";
const SEPARATOR_WIDTH: usize = 3;
const TWO_LINE_MIN_HEIGHT: u16 = 22;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BarKind {
    Provider,
    Model,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BarData {
    pub provider: Option<String>,
    pub model: Option<String>,
    pub thinking: Option<String>,
    pub directory: Option<String>,
    pub branch: Option<String>,
    pub usage: Usage,
    pub phase: Option<String>,
    pub phase_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BarSegment {
    pub kind: Option<BarKind>,
    pub text: String,
    pub rect: Rect,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BarLayout {
    pub lines: u16,
    pub segments: Vec<BarSegment>,
    pub rows: Vec<Line<'static>>,
}

impl Default for BarLayout {
    fn default() -> Self {
        Self {
            lines: 1,
            segments: Vec::new(),
            rows: Vec::new(),
        }
    }
}

struct Pending {
    kind: Option<BarKind>,
    text: String,
}

pub fn compose(data: &BarData, width: u16, height: u16) -> BarLayout {
    if width == 0 {
        return BarLayout::default();
    }
    let limit = usize::from(width);
    let mut pending = pending_segments(data);
    let one_line_fits = joined_width(&pending) <= limit;
    let two_lines = !one_line_fits
        && height >= TWO_LINE_MIN_HEIGHT
        && pending.iter().any(|segment| segment.kind.is_some());

    if !one_line_fits {
        if two_lines {
            // 第一行固定为提供商与模型，只有第二行参与舍弃。
            while joined_width(split(&pending).1) > limit {
                if !drop_tail(&mut pending) {
                    break;
                }
            }
            truncate_first_line(&mut pending, limit);
        } else {
            while joined_width(&pending) > limit {
                if !drop_tail(&mut pending) {
                    break;
                }
            }
            truncate_first_line(&mut pending, limit);
        }
    }

    let first_line = if two_lines {
        split(&pending).0
    } else {
        &pending[..]
    };
    let second_line = if two_lines {
        split(&pending).1
    } else {
        &[][..]
    };
    build(first_line, second_line)
}

fn pending_segments(data: &BarData) -> Vec<Pending> {
    let mut segments = Vec::new();
    if let Some(provider) = &data.provider {
        segments.push(Pending {
            kind: Some(BarKind::Provider),
            text: provider.clone(),
        });
    }
    if let Some(model) = &data.model {
        segments.push(Pending {
            kind: Some(BarKind::Model),
            text: model.clone(),
        });
    }
    if let Some(thinking) = &data.thinking {
        segments.push(Pending {
            kind: None,
            text: format!("think:{thinking}"),
        });
    }
    if let Some(directory) = &data.directory {
        segments.push(Pending {
            kind: None,
            text: directory.clone(),
        });
    }
    if let Some(branch) = &data.branch {
        segments.push(Pending {
            kind: None,
            text: format!("⎇ {branch}"),
        });
    }
    segments.push(Pending {
        kind: None,
        text: format!(
            "◫ {}/{} tok",
            abbreviate(data.usage.input_tokens),
            abbreviate(data.usage.output_tokens)
        ),
    });
    if let Some(phase) = &data.phase {
        let text = match data.phase_reason.as_deref() {
            Some(reason) if !reason.is_empty() && reason != phase => format!("{phase}: {reason}"),
            _ => phase.clone(),
        };
        segments.push(Pending { kind: None, text });
    }
    segments
}

fn build(first: &[Pending], second: &[Pending]) -> BarLayout {
    let mut rows = Vec::new();
    let mut segments = Vec::new();
    for (row_index, row) in [first, second].into_iter().enumerate() {
        if row.is_empty() {
            continue;
        }
        let mut spans = Vec::new();
        let mut x = 0u16;
        for (index, segment) in row.iter().enumerate() {
            if index > 0 {
                spans.push(Span::styled(SEPARATOR, theme::BAR));
                x = x.saturating_add(SEPARATOR_WIDTH as u16);
            }
            let width = segment.text.width().min(usize::from(u16::MAX)) as u16;
            segments.push(BarSegment {
                kind: segment.kind,
                text: segment.text.clone(),
                rect: Rect::new(x, row_index as u16, width, 1),
            });
            spans.push(Span::styled(segment.text.clone(), style(segment.kind)));
            x = x.saturating_add(width);
        }
        rows.push(Line::from(spans));
    }
    let lines = u16::try_from(rows.len()).unwrap_or(u16::MAX).max(1);
    BarLayout {
        lines,
        segments,
        rows,
    }
}

fn style(kind: Option<BarKind>) -> Style {
    match kind {
        Some(_) => theme::BAR_CLICK,
        None => theme::BAR,
    }
}

fn split(segments: &[Pending]) -> (&[Pending], &[Pending]) {
    let index = segments
        .iter()
        .position(|segment| segment.kind.is_none())
        .unwrap_or(segments.len());
    segments.split_at(index)
}

fn drop_tail(segments: &mut Vec<Pending>) -> bool {
    let protected = segments
        .iter()
        .filter(|segment| segment.kind.is_some())
        .count();
    if segments.len() <= protected {
        return false;
    }
    segments.pop();
    true
}

fn truncate_first_line(segments: &mut Vec<Pending>, limit: usize) {
    if segments.is_empty() {
        return;
    }
    let Some(index) = segments
        .iter()
        .position(|segment| segment.kind == Some(BarKind::Model))
    else {
        truncate_segment(segments, 0, limit);
        return;
    };
    let prefix: usize = segments[..index]
        .iter()
        .map(|segment| segment.text.width() + SEPARATOR_WIDTH)
        .sum();
    let available = limit.saturating_sub(prefix);
    let text = concise(&segments[index].text, available);
    if text.is_empty() {
        segments.remove(index);
    } else {
        segments[index].text = text;
    }
    if joined_width(segments) > limit {
        truncate_segment(segments, 0, limit);
    }
}

fn truncate_segment(segments: &mut [Pending], index: usize, limit: usize) {
    if let Some(segment) = segments.get_mut(index) {
        segment.text = concise(&segment.text, limit);
    }
}

fn joined_width(segments: &[Pending]) -> usize {
    let content: usize = segments.iter().map(|segment| segment.text.width()).sum();
    content + SEPARATOR_WIDTH * segments.len().saturating_sub(1)
}

fn abbreviate(value: i32) -> String {
    let value = u64::try_from(value).unwrap_or(0);
    if value < 1000 {
        value.to_string()
    } else {
        format!("{}k", value / 1000)
    }
}

pub fn branch_from_head(contents: &str) -> Option<String> {
    let trimmed = contents.trim();
    if let Some(branch) = trimmed.strip_prefix("ref: refs/heads/") {
        return (!branch.is_empty()).then(|| branch.to_owned());
    }
    if trimmed.len() == 40 && trimmed.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Some(trimmed[..7].to_owned());
    }
    None
}

pub fn read_branch() -> Option<String> {
    let head = env::current_dir().ok()?.join(".git").join("HEAD");
    let contents = fs::read_to_string(head).ok()?;
    branch_from_head(&contents)
}

pub fn render(frame: &mut Frame, area: Rect, layout: &BarLayout) {
    if area.is_empty() || layout.rows.is_empty() {
        return;
    }
    frame.render_widget(Paragraph::new(layout.rows.clone()), area);
}
