pub mod bar;
pub mod content;
pub mod form;
pub mod input;
pub mod log;
pub mod mods;
pub mod overlay;
pub mod tree;

use crate::app::{App, Focus, Screen};
use crate::theme;
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::widgets::{Block, Paragraph};

pub struct SideLayout {
    pub mods: Rect,
    pub tree: Rect,
    pub log: Rect,
}

pub struct Layout {
    pub content: Rect,
    pub bar: Rect,
    pub input: Rect,
    pub side: Option<SideLayout>,
}

pub fn left_width(area: Rect, show_side: bool) -> u16 {
    if show_side && area.width >= 60 && area.height >= 20 {
        let side = (u32::from(area.width) / 3).clamp(28, 48) as u16;
        area.width - side
    } else {
        area.width
    }
}

pub fn pane_offset(total: u16, height: u16, scroll: u16) -> (u16, u16) {
    let max = total.saturating_sub(height);
    (scroll.min(max), max)
}

pub fn layout(area: Rect, bar_height: u16, show_side: bool) -> Layout {
    let hidden = Rect::new(area.x, area.y, 0, 0);
    let input_height = ((u32::from(area.height) * 40 / 100).clamp(5, 15) as u16).min(area.height);

    if area.height < 10 {
        let y = area.y.saturating_add(area.height - input_height);
        return Layout {
            content: hidden,
            bar: hidden,
            input: Rect::new(area.x, y, area.width, input_height),
            side: None,
        };
    }

    let bar_height = if area.height < 22 {
        1
    } else {
        bar_height.clamp(1, 2)
    };
    let side_visible = show_side && area.width >= 60 && area.height >= 20;
    let left = left_width(area, show_side);
    let side_width = side_visible.then(|| area.width - left);

    let content_height = area
        .height
        .saturating_sub(input_height)
        .saturating_sub(bar_height);
    let bar_y = area.y.saturating_add(content_height);
    let input_y = bar_y.saturating_add(bar_height);
    let content = Rect::new(area.x, area.y, left, content_height);
    let bar = Rect::new(area.x, bar_y, left, bar_height);
    let input = Rect::new(area.x, input_y, left, input_height);

    let side = side_width.map(|width| {
        let x = area.x.saturating_add(left);
        let mods_height = (u32::from(area.height) / 4).clamp(3, 10) as u16;
        let tree_height = (u32::from(area.height) / 3).clamp(4, 14) as u16;
        let log_height = area
            .height
            .saturating_sub(mods_height)
            .saturating_sub(tree_height);
        let tree_y = area.y.saturating_add(mods_height);
        let log_y = tree_y.saturating_add(tree_height);
        SideLayout {
            mods: Rect::new(x, area.y, width, mods_height),
            tree: Rect::new(x, tree_y, width, tree_height),
            log: Rect::new(x, log_y, width, log_height),
        }
    });

    Layout {
        content,
        bar,
        input,
        side,
    }
}

pub fn render(frame: &mut Frame, app: &mut App) {
    let area = frame.area();
    let data = app.bar_data();
    let storage_error = app.storage_error().map(str::to_owned);
    let show_side = app.show_side() && storage_error.is_none();
    let width = left_width(area, show_side);
    let bar = bar::compose(&data, width, area.height);
    let bar_height = bar.lines;
    app.set_bar(bar);
    let layout = layout(area, bar_height, show_side);
    app.set_bar_area(layout.bar);
    let focus = app.focus();
    match app.screen_mut() {
        Screen::Empty => {
            match &storage_error {
                Some(error) => render_panel(
                    frame,
                    layout.content,
                    "Content Area",
                    Some(error),
                    theme::ERROR,
                    focus == Focus::Content,
                ),
                None => render_panel(
                    frame,
                    layout.content,
                    "Content Area",
                    Some("No active session"),
                    theme::PLACEHOLDER,
                    focus == Focus::Content,
                ),
            }
            if let Some(side) = layout.side {
                let mut scroll = 0;
                mods::render(frame, side.mods, &[], &mut scroll, focus == Focus::Mods);
                tree::render(frame, side.tree, &[], &mut scroll, focus == Focus::Tree);
                log::render(
                    frame,
                    side.log,
                    &log::LogRing::new(),
                    &mut scroll,
                    focus == Focus::Log,
                );
            }
        }
        Screen::Session(session) => {
            match &storage_error {
                Some(error) => render_panel(
                    frame,
                    layout.content,
                    "Content Area",
                    Some(error),
                    theme::ERROR,
                    focus == Focus::Content,
                ),
                None => content::render(frame, layout.content, session, focus == Focus::Content),
            }
            if let Some(side) = layout.side {
                let mods_max = mods::render(
                    frame,
                    side.mods,
                    &session.mods,
                    &mut session.mods_scroll,
                    focus == Focus::Mods,
                );
                session.mods_max = mods_max;
                let tree_max = tree::render(
                    frame,
                    side.tree,
                    &session.nodes,
                    &mut session.tree_scroll,
                    focus == Focus::Tree,
                );
                session.tree_max = tree_max;
                let log_max = log::render(
                    frame,
                    side.log,
                    &session.log,
                    &mut session.log_scroll,
                    focus == Focus::Log,
                );
                session.log_max = log_max;
            }
        }
    }
    bar::render(frame, layout.bar, app.bar_layout());
    render_notice(frame, layout.bar, app.notice());
    input::render(
        frame,
        layout.input,
        app.input(),
        app.focus(),
        app.completion(),
    );
    overlay::render(frame, frame.area(), app);
}

fn render_panel(
    frame: &mut Frame,
    area: Rect,
    title: &str,
    message: Option<&str>,
    style: Style,
    focused: bool,
) {
    if area.is_empty() {
        return;
    }
    let border = if focused { theme::FOCUS } else { theme::BORDER };
    let block = Block::bordered()
        .border_style(border)
        .title(title)
        .title_style(theme::TITLE);
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if let Some(message) = message
        && !inner.is_empty()
    {
        frame.render_widget(Paragraph::new(message).style(style), inner);
    }
}

fn render_notice(frame: &mut Frame, area: Rect, notice: Option<&str>) {
    let Some(notice) = notice else {
        return;
    };
    if area.is_empty() {
        return;
    }
    let line = Rect::new(area.x, area.bottom().saturating_sub(1), area.width, 1);
    frame.render_widget(Paragraph::new(notice).style(theme::BAR), line);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pane_offsets_clamp_to_the_content() {
        assert_eq!(pane_offset(3, 5, 0), (0, 0));
        assert_eq!(pane_offset(9, 5, 7), (4, 4));
        assert_eq!(pane_offset(9, 5, 2), (2, 4));
        assert_eq!(pane_offset(9, 5, u16::MAX), (4, 4));
    }
}
