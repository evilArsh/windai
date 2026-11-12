use ratatui::style::{Color, Modifier, Style};

pub const BORDER: Style = Style::new().fg(Color::DarkGray);
pub const FOCUS: Style = Style::new().fg(Color::Cyan);
pub const TITLE: Style = Style::new().fg(Color::Gray).add_modifier(Modifier::BOLD);
pub const ROLE: Style = Style::new().fg(Color::Cyan).add_modifier(Modifier::BOLD);
pub const REASONING: Style = Style::new().add_modifier(Modifier::DIM);
pub const ERROR: Style = Style::new().fg(Color::Red);
pub const BAR: Style = Style::new().fg(Color::DarkGray);
pub const BAR_CLICK: Style = Style::new()
    .fg(Color::Cyan)
    .add_modifier(Modifier::UNDERLINED);
pub const PLACEHOLDER: Style = Style::new().fg(Color::DarkGray);
pub const CURSOR: Style = Style::new().add_modifier(Modifier::REVERSED);
pub const SELECTED: Style = Style::new().add_modifier(Modifier::REVERSED);
