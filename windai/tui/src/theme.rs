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
pub const NOTICE: Style = Style::new().fg(Color::Yellow);
pub const PLACEHOLDER: Style = Style::new().fg(Color::DarkGray);
pub const CURSOR: Style = Style::new().add_modifier(Modifier::REVERSED);
pub const SELECTED: Style = Style::new().add_modifier(Modifier::REVERSED);

/// 日志序号与本地消息
pub const LOG_INDEX: Style = Style::new().fg(Color::DarkGray);
/// 日志摘要行
pub const LOG_BODY: Style = Style::new().fg(Color::Gray).add_modifier(Modifier::DIM);
pub const LOG_LLM: Style = Style::new().fg(Color::Cyan);
pub const LOG_AGENT: Style = Style::new().fg(Color::Magenta);
pub const LOG_MOD: Style = Style::new().fg(Color::Green);
pub const LOG_MCP: Style = Style::new().fg(Color::Yellow);
pub const LOG_CORE: Style = Style::new().fg(Color::Blue);
pub const LOG_LOCAL: Style = Style::new().fg(Color::DarkGray);

pub const TOOL_CALL: Style = Style::new().fg(Color::Yellow);
pub const TOOL_RESULT: Style = Style::new().fg(Color::Cyan);
pub const JSON_KEY: Style = Style::new().fg(Color::Cyan);
pub const JSON_STRING: Style = Style::new().fg(Color::Green);
pub const JSON_NUMBER: Style = Style::new().fg(Color::Yellow);
pub const JSON_LITERAL: Style = Style::new().fg(Color::Magenta);
pub const JSON_PUNCT: Style = Style::new().fg(Color::DarkGray);
