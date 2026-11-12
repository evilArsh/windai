use super::form;
use crate::app::{
    App, CONFIG_DIVIDER_ACTIONS, CONFIG_DIVIDER_CREATE, CONFIG_EDIT_MODEL, CONFIG_EDIT_PROVIDER,
    CONFIG_MODEL, CONFIG_NEW_MODEL, CONFIG_NEW_PROVIDER, CONFIG_PROVIDER, CONFIG_REASONING,
    CONFIG_SIDE_PANEL, CONFIG_STREAM, Overlay,
};
use crate::command::COMMANDS;
use crate::text::{concise, format_time};
use crate::theme;
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::Line;
use ratatui::widgets::{Block, Clear, Paragraph};

pub fn render(frame: &mut Frame, area: Rect, app: &App) {
    if area.is_empty() {
        return;
    }
    if let Some(form) = app.form() {
        form::render(frame, area, form, app.providers());
        return;
    }
    let Some(overlay) = app.overlay() else {
        return;
    };
    let (title, rows) = match overlay {
        Overlay::Resume { items, selected } => {
            let current = app.current_topic_id();
            (
                "resume",
                items
                    .iter()
                    .enumerate()
                    .map(|(index, item)| {
                        let time = item
                            .created_at
                            .map(format_time)
                            .unwrap_or_else(|| "-".to_owned());
                        let marker = if Some(item.topic.id) == current {
                            "*"
                        } else {
                            " "
                        };
                        (
                            format!(
                                "{marker} #{} {} {} {}",
                                item.topic.id, item.topic.label, item.phase, time
                            ),
                            index == *selected,
                        )
                    })
                    .collect(),
            )
        }
        Overlay::Providers { selected } => {
            let current = app
                .selection()
                .provider
                .as_ref()
                .map(|provider| provider.id);
            (
                "providers",
                app.providers()
                    .iter()
                    .enumerate()
                    .map(|(index, provider)| {
                        let marker = if Some(provider.id) == current {
                            "*"
                        } else {
                            " "
                        };
                        (
                            format!("{marker} {} · {}", provider.name, provider.base_url),
                            index == *selected,
                        )
                    })
                    .collect(),
            )
        }
        Overlay::Models { selected } => {
            let current = app.selection().model.as_ref().map(|model| model.id);
            (
                "models",
                app.models()
                    .iter()
                    .enumerate()
                    .map(|(index, model)| {
                        let marker = if Some(model.id) == current { "*" } else { " " };
                        (format!("{marker} {}", model.name), index == *selected)
                    })
                    .collect(),
            )
        }
        Overlay::Config { selected } => ("config", config_rows(app, *selected)),
        Overlay::Help => ("help", help_rows()),
    };
    let popup = popup(area, rows.len());
    if popup.is_empty() {
        return;
    }
    frame.render_widget(Clear, popup);
    let block = Block::bordered()
        .border_style(theme::FOCUS)
        .title(title)
        .title_style(theme::TITLE);
    let inner = block.inner(popup);
    frame.render_widget(block, popup);
    if inner.is_empty() {
        return;
    }
    let width = usize::from(inner.width);
    let lines: Vec<Line<'static>> = rows
        .into_iter()
        .map(|(text, selected)| {
            let line = Line::from(concise(&text, width));
            if selected {
                line.style(theme::SELECTED)
            } else {
                line
            }
        })
        .collect();
    frame.render_widget(Paragraph::new(lines), inner);
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

fn config_rows(app: &App, selected: usize) -> Vec<(String, bool)> {
    let provider = app
        .selection()
        .provider
        .as_ref()
        .map(|provider| provider.name.as_str())
        .unwrap_or("-");
    let model = app
        .selection()
        .model
        .as_ref()
        .map(|model| model.name.as_str())
        .unwrap_or("-");
    let reasoning = app.selection().reasoning_effort.as_deref().unwrap_or("-");
    let stream = app.selection().stream_label();
    let side_panel = if app.show_side() { "on" } else { "off" };
    let values = [
        (CONFIG_PROVIDER, "provider", provider),
        (CONFIG_MODEL, "model", model),
        (CONFIG_REASONING, "reasoning", reasoning),
        (CONFIG_STREAM, "stream", stream),
        (CONFIG_SIDE_PANEL, "side panel", side_panel),
    ];
    let mut rows: Vec<(String, bool)> = values
        .into_iter()
        .map(|(index, label, value)| (format!("{label:<14} {value}  >"), index == selected))
        .collect();
    rows.insert(CONFIG_DIVIDER_ACTIONS, ("────────────".to_owned(), false));
    rows.push(("edit provider".to_owned(), CONFIG_EDIT_PROVIDER == selected));
    rows.push(("edit model".to_owned(), CONFIG_EDIT_MODEL == selected));
    rows.insert(CONFIG_DIVIDER_CREATE, ("────────────".to_owned(), false));
    rows.push(("new provider".to_owned(), CONFIG_NEW_PROVIDER == selected));
    rows.push(("new model".to_owned(), CONFIG_NEW_MODEL == selected));
    rows
}

fn help_rows() -> Vec<(String, bool)> {
    let mut rows: Vec<(String, bool)> = COMMANDS
        .iter()
        .map(|spec| (format!("{:<12} {}", spec.name, spec.description), false))
        .collect();
    rows.push((String::new(), false));
    for (key, action) in [
        ("Tab", "complete a command / switch focus"),
        ("Enter", "submit"),
        ("Alt+Enter", "insert a newline"),
        ("PgUp/PgDn", "scroll the focused pane by a page"),
        ("Wheel", "scroll the pane under the pointer"),
        ("Esc", "close"),
        ("Ctrl+C", "quit"),
    ] {
        rows.push((format!("{key:<12} {action}"), false));
    }
    rows
}
