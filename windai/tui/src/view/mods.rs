use crate::app::ModView;
use crate::text::concise;
use crate::theme;
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::Line;
use ratatui::widgets::{Block, Paragraph};

pub fn render(frame: &mut Frame, area: Rect, mods: &[ModView]) {
    if area.is_empty() {
        return;
    }
    let block = Block::bordered()
        .border_style(theme::BORDER)
        .title("Layer 1 mods")
        .title_style(theme::TITLE);
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if inner.is_empty() {
        return;
    }
    let width = usize::from(inner.width);
    let lines: Vec<Line<'static>> = if mods.is_empty() {
        vec![Line::from("—").style(theme::PLACEHOLDER)]
    } else {
        mods.iter()
            .map(|module| {
                Line::from(concise(
                    &format!("{} v{}", module.name, module.version),
                    width,
                ))
            })
            .collect()
    };
    frame.render_widget(Paragraph::new(lines), inner);
}
