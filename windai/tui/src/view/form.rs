use crate::form::{ADAPTERS, Form, ModelForm, ProviderForm};
use crate::theme;
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Clear, Paragraph};
use wind_storage::Provider;

pub fn render(frame: &mut Frame, area: Rect, form: &Form, providers: &[Provider]) {
    if area.is_empty() {
        return;
    }
    let rows = match form {
        Form::Provider(form) => provider_rows(form),
        Form::Model(form) => model_rows(form, providers),
    };
    let popup = popup(area, rows.len());
    if popup.is_empty() {
        return;
    }
    frame.render_widget(Clear, popup);
    let block = Block::bordered()
        .border_style(theme::FOCUS)
        .title(form.title())
        .title_style(theme::TITLE);
    let inner = block.inner(popup);
    frame.render_widget(block, popup);
    if !inner.is_empty() {
        frame.render_widget(Paragraph::new(rows), inner);
    }
}

fn popup(area: Rect, rows: usize) -> Rect {
    let width = ((u32::from(area.width) * 3 / 4) as u16)
        .min(72)
        .min(area.width);
    let requested = u16::try_from(rows.saturating_add(2)).unwrap_or(u16::MAX);
    let height = ((u32::from(area.height) * 3 / 4) as u16)
        .min(requested)
        .min(area.height);
    let x = area.x + area.width.saturating_sub(width) / 2;
    let y = area.y + area.height.saturating_sub(height) / 2;
    Rect::new(x, y, width, height)
}

fn provider_rows(form: &ProviderForm) -> Vec<Line<'static>> {
    let mut rows = vec![
        text_line("name", &form.name, form.focus == 0, form.cursor),
        text_line("base_url", &form.base_url, form.focus == 1, form.cursor),
        text_line("api_key", &form.api_key, form.focus == 2, form.cursor),
    ];
    if form.editing.is_some() {
        push_hint(&mut rows, "blank api_key keeps the current key");
    }
    push_error(&mut rows, form.error.as_deref());
    rows
}

fn model_rows(form: &ModelForm, providers: &[Provider]) -> Vec<Line<'static>> {
    let provider = providers
        .get(form.provider_index)
        .map(|provider| provider.name.as_str())
        .unwrap_or("-");
    let adapter = ADAPTERS
        .get(form.adapter_index)
        .map(ToString::to_string)
        .unwrap_or_else(|| "-".to_owned());
    let mut rows = vec![
        enum_line(
            "provider",
            provider,
            form.focus == 0,
            form.editing.is_none(),
        ),
        text_line("name", &form.name, form.focus == 1, form.cursor),
        enum_line("adapter", &adapter, form.focus == 2, true),
        text_line("endpoint", &form.endpoint, form.focus == 3, form.cursor),
        text_line("config", &form.config, form.focus == 4, form.cursor),
    ];
    push_error(&mut rows, form.error.as_deref());
    rows
}

fn push_hint(rows: &mut Vec<Line<'static>>, hint: &str) {
    rows.push(Line::from(Span::styled(
        hint.to_owned(),
        theme::PLACEHOLDER,
    )));
}

fn push_error(rows: &mut Vec<Line<'static>>, error: Option<&str>) {
    if let Some(error) = error {
        rows.push(Line::from(Span::styled(error.to_owned(), theme::ERROR)));
    }
}

fn label_style(active: bool) -> ratatui::style::Style {
    if active { theme::FOCUS } else { theme::BORDER }
}

fn text_line(label: &str, value: &str, active: bool, cursor: usize) -> Line<'static> {
    let mut spans = vec![Span::styled(format!("{label:<12} "), label_style(active))];
    let length = value.chars().count();
    for (index, ch) in value.chars().enumerate() {
        if active && index == cursor {
            spans.push(Span::styled(ch.to_string(), theme::CURSOR));
        } else {
            spans.push(Span::raw(ch.to_string()));
        }
    }
    if active && cursor >= length {
        spans.push(Span::styled(" ".to_owned(), theme::CURSOR));
    }
    Line::from(spans)
}

fn enum_line(label: &str, value: &str, active: bool, cycle: bool) -> Line<'static> {
    let mut spans = vec![
        Span::styled(format!("{label:<12} "), label_style(active)),
        Span::raw(value.to_owned()),
    ];
    if cycle {
        spans.push(Span::styled("  >".to_owned(), theme::BAR));
    }
    Line::from(spans)
}
