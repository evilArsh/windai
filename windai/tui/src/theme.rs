use ratatui::style::{Color, Modifier, Style};

pub const BORDER: Style = Style::new().fg(Color::DarkGray);
pub const FOCUS: Style = Style::new().fg(Color::Cyan);
pub const TITLE: Style = Style::new().fg(Color::Gray).add_modifier(Modifier::BOLD);
pub const BAR: Style = Style::new().fg(Color::DarkGray);
pub const PLACEHOLDER: Style = Style::new().fg(Color::DarkGray);
pub const CURSOR: Style = Style::new().add_modifier(Modifier::REVERSED);
