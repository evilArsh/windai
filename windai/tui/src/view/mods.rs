use super::pane_offset;
use crate::app::ModView;
use crate::text::concise;
use crate::theme;
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::Line;
use ratatui::widgets::{Block, Paragraph};

pub fn render(
    frame: &mut Frame,
    area: Rect,
    mods: &[ModView],
    scroll: &mut u16,
    focused: bool,
) -> u16 {
    if area.is_empty() {
        return 0;
    }
    let border = if focused { theme::FOCUS } else { theme::BORDER };
    let block = Block::bordered()
        .border_style(border)
        .title("Layer 1 mods")
        .title_style(theme::TITLE);
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if inner.is_empty() {
        return 0;
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
    let total = u16::try_from(lines.len()).unwrap_or(u16::MAX);
    let (offset, max) = pane_offset(total, inner.height, *scroll);
    *scroll = offset;
    frame.render_widget(Paragraph::new(lines).scroll((offset, 0)), inner);
    max
}
