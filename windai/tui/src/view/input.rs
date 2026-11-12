use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::widgets::Block;
use ratatui_textarea::TextArea;

use crate::app::Focus;
use crate::theme;

pub fn render(frame: &mut Frame, area: Rect, input: &TextArea<'_>, focus: Focus) {
    if area.is_empty() {
        return;
    }
    let border = if focus == Focus::Input {
        theme::FOCUS
    } else {
        theme::BORDER
    };
    let block = Block::bordered().border_style(border);
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if !inner.is_empty() {
        frame.render_widget(input, inner);
    }
}
