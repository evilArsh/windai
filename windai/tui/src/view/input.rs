use crate::app::{COMPLETION_ROWS, Completion, Focus};
use crate::text::concise;
use crate::theme;
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Clear, Paragraph};
use ratatui_textarea::TextArea;
use unicode_width::UnicodeWidthStr;

pub fn render(
    frame: &mut Frame,
    area: Rect,
    input: &TextArea<'_>,
    focus: Focus,
    completion: Option<&Completion>,
    notice: Option<&str>,
) {
    if area.is_empty() {
        return;
    }
    let border = if focus == Focus::Input {
        theme::FOCUS
    } else {
        theme::BORDER
    };
    let block = match notice {
        Some(notice) => Block::bordered()
            .border_style(border)
            .title(Span::styled(format!(" {notice} "), theme::NOTICE)),
        None => Block::bordered().border_style(border),
    };
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if !inner.is_empty() {
        frame.render_widget(input, inner);
    }
    if let Some(completion) = completion {
        render_completion(frame, area, input, completion);
    }
}

fn render_completion(
    frame: &mut Frame,
    input_area: Rect,
    input: &TextArea<'_>,
    completion: &Completion,
) {
    let text = input.lines().join("\n");
    let prefix = text
        .strip_prefix('/')
        .filter(|rest| !rest.chars().any(char::is_whitespace))
        .unwrap_or_default();
    let hit = 1 + prefix.chars().count();
    let start = completion
        .selected
        .saturating_sub(COMPLETION_ROWS.saturating_sub(1));
    let available_height = usize::from(input_area.y).min(COMPLETION_ROWS);
    let rows = completion
        .items
        .iter()
        .skip(start)
        .take(available_height)
        .enumerate()
        .map(|(offset, spec)| {
            let selected = start + offset == completion.selected;
            completion_line(spec.name, spec.description, hit, input_area.width, selected)
        })
        .collect::<Vec<_>>();
    if rows.is_empty() || input_area.width == 0 {
        return;
    }
    let height = rows.len() as u16;
    let area = Rect::new(
        input_area.x,
        input_area.y.saturating_sub(height),
        input_area.width,
        height,
    );
    frame.render_widget(Clear, area);
    frame.render_widget(Paragraph::new(rows), area);
}

fn completion_line(
    name: &'static str,
    description: &'static str,
    hit: usize,
    width: u16,
    selected: bool,
) -> Line<'static> {
    let hit_bytes = name
        .char_indices()
        .nth(hit)
        .map(|(index, _)| index)
        .unwrap_or(name.len());
    let (matched, rest) = name.split_at(hit_bytes);
    let mut spans = vec![
        Span::styled(matched.to_owned(), theme::TITLE),
        Span::raw(rest.to_owned()),
    ];
    let available = usize::from(width).saturating_sub(name.width() + 2);
    if available > 0 {
        spans.push(Span::styled(
            format!("  {}", concise(description, available)),
            theme::BAR,
        ));
    }
    let line = Line::from(spans);
    if selected {
        line.style(theme::SELECTED)
    } else {
        line
    }
}
