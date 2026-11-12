pub mod content;
pub mod form;
pub mod input;
pub mod log;
pub mod mods;
pub mod overlay;
pub mod tree;

use crate::app::{App, Screen};
use crate::theme;
use ratatui::Frame;
use ratatui::layout::Rect;
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
    let side_width = if show_side && area.width >= 60 && area.height >= 20 {
        Some((u32::from(area.width) / 3).clamp(28, 48) as u16)
    } else {
        None
    };
    let left_width = side_width.map_or(area.width, |width| area.width - width);

    let content_height = area
        .height
        .saturating_sub(input_height)
        .saturating_sub(bar_height);
    let bar_y = area.y.saturating_add(content_height);
    let input_y = bar_y.saturating_add(bar_height);
    let content = Rect::new(area.x, area.y, left_width, content_height);
    let bar = Rect::new(area.x, bar_y, left_width, bar_height);
    let input = Rect::new(area.x, input_y, left_width, input_height);

    let side = side_width.map(|width| {
        let x = area.x.saturating_add(left_width);
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
    let layout = layout(frame.area(), 1, app.show_side());
    match app.screen_mut() {
        Screen::Empty => {
            render_panel(
                frame,
                layout.content,
                "Content Area",
                Some("No active session"),
            );
            if let Some(side) = layout.side {
                mods::render(frame, side.mods, &[]);
                tree::render(frame, side.tree, &[]);
                log::render(frame, side.log, &log::LogRing::new());
            }
        }
        Screen::Session(session) => {
            content::render(frame, layout.content, session);
            if let Some(side) = layout.side {
                mods::render(frame, side.mods, &session.mods);
                tree::render(frame, side.tree, &session.nodes);
                log::render(frame, side.log, &session.log);
            }
        }
    }
    render_bar(frame, layout.bar, app.notice());
    input::render(
        frame,
        layout.input,
        app.input(),
        app.focus(),
        app.completion(),
    );
    overlay::render(frame, frame.area(), app);
}

fn render_panel(frame: &mut Frame, area: Rect, title: &str, message: Option<&str>) {
    if area.is_empty() {
        return;
    }
    let block = Block::bordered()
        .border_style(theme::BORDER)
        .title(title)
        .title_style(theme::TITLE);
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if let Some(message) = message
        && !inner.is_empty()
    {
        frame.render_widget(Paragraph::new(message).style(theme::PLACEHOLDER), inner);
    }
}

fn render_bar(frame: &mut Frame, area: Rect, notice: Option<&str>) {
    if area.is_empty() {
        return;
    }
    frame.render_widget(
        Paragraph::new("No provider selected").style(theme::BAR),
        area,
    );
    if let Some(notice) = notice {
        let line = Rect::new(area.x, area.bottom().saturating_sub(1), area.width, 1);
        frame.render_widget(Paragraph::new(notice).style(theme::BAR), line);
    }
}
