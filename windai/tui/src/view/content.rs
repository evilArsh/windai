use crate::app::{BlockView, MessageView, Session};
use crate::text::concise;
use crate::theme;
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, Paragraph, Wrap};
use serde_json::Value;
use std::borrow::Cow;
use unicode_width::UnicodeWidthStr;
use wind_wire::Body;

pub fn render(frame: &mut Frame, area: Rect, session: &mut Session) {
    if area.is_empty() {
        return;
    }
    let block = Block::bordered()
        .border_style(theme::BORDER)
        .title("Content Area")
        .title_style(theme::TITLE);
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if inner.is_empty() {
        return;
    }
    let width = inner.width;
    let mut lines: Vec<Line<'static>> = Vec::new();
    for message in &mut session.messages {
        lines.push(message_title(message));
        for block in &mut message.blocks {
            lines.extend(block_text(block, width).lines);
        }
        lines.push(Line::default());
    }
    let paragraph = Paragraph::new(lines).wrap(Wrap { trim: false });
    frame.render_widget(paragraph, inner);
}

fn message_title(message: &MessageView) -> Line<'static> {
    let mut spans = vec![Span::styled(message.role.as_ref().to_owned(), theme::ROLE)];
    if let Some(reason) = &message.sealed {
        spans.push(Span::styled(format!(" {reason}"), theme::BAR));
    }
    if let Some(reason) = &message.interrupted {
        spans.push(Span::styled(format!(" {reason}"), theme::ERROR));
    }
    Line::from(spans)
}

fn block_text(block: &mut BlockView, width: u16) -> Text<'static> {
    match &block.body {
        Body::Text { content } => markdown_cache(
            &mut block.rendered,
            &mut block.rendered_width,
            block.revision,
            content,
            width,
        ),
        Body::Reasoning { content, .. } => plain(format!("reasoning {content}"), theme::REASONING),
        Body::ToolCall {
            call_name,
            call_arguments,
            ..
        } => Text::from(tool_call_line(call_name, call_arguments, width)),
        Body::ToolResult { call_res, .. } => tool_result_text(call_res, width),
        Body::Image { content } => Text::from(placeholder("image", None, content)),
        Body::File { content } => Text::from(placeholder("file", None, content)),
        Body::Audio {
            format, content, ..
        } => Text::from(placeholder("audio", Some(format), content)),
    }
}

fn markdown_cache(
    rendered: &mut Option<(i64, Text<'static>)>,
    rendered_width: &mut Option<u16>,
    revision: i64,
    content: &str,
    width: u16,
) -> Text<'static> {
    let stale = rendered
        .as_ref()
        .is_none_or(|(cached, _)| *cached != revision)
        || *rendered_width != Some(width);
    if stale {
        let options = tui_markdown::Options::default().table_width(width);
        let text = tui_markdown::from_str_with_options(content, &options);
        *rendered = Some((revision, to_static(text)));
        *rendered_width = Some(width);
    }
    rendered
        .as_ref()
        .map(|(_, text)| text.clone())
        .unwrap_or_default()
}

fn to_static(text: Text<'_>) -> Text<'static> {
    Text {
        alignment: text.alignment,
        style: text.style,
        lines: text
            .lines
            .into_iter()
            .map(|line| Line {
                style: line.style,
                alignment: line.alignment,
                spans: line
                    .spans
                    .into_iter()
                    .map(|span| Span {
                        style: span.style,
                        content: Cow::Owned(span.content.into_owned()),
                    })
                    .collect(),
            })
            .collect(),
    }
}

fn plain(content: String, style: Style) -> Text<'static> {
    let mut text = Text::from(content);
    for line in &mut text.lines {
        line.style = style;
    }
    text
}

fn tool_call_line(call_name: &str, call_arguments: &str, width: u16) -> String {
    let width = usize::from(width);
    let first = call_arguments.lines().next().unwrap_or_default();
    let available = width.saturating_sub(call_name.width() + 1);
    if first.trim().is_empty() || available == 0 {
        return concise(call_name, width);
    }
    format!("{call_name} {}", concise(first, available))
}

fn tool_result_text(call_res: &Value, width: u16) -> Text<'static> {
    let style = if tool_result_failed(call_res) {
        theme::ERROR
    } else {
        Style::default()
    };
    let summary = serde_json::to_string(call_res).unwrap_or_else(|_| call_res.to_string());
    Text::from(vec![
        Line::from(Span::styled("result".to_owned(), style)),
        Line::from(Span::styled(concise(&summary, usize::from(width)), style)),
    ])
}

fn tool_result_failed(call_res: &Value) -> bool {
    let Some(fields) = call_res.as_object() else {
        return false;
    };
    fields.contains_key("error") || fields.get("refused").and_then(Value::as_bool) == Some(true)
}

fn placeholder(kind: &str, format: Option<&str>, content: &str) -> String {
    match format.filter(|format_name| !format_name.is_empty()) {
        Some(format_name) => format!("{kind} {format_name} {}B", content.len()),
        None => format!("{kind} {}B", content.len()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::style::{Color, Modifier};
    use serde_json::json;
    use wind_wire::Role;

    fn block(body: Body) -> BlockView {
        BlockView::new(0, 1, body)
    }

    fn text(body: Body, width: u16) -> Text<'static> {
        let mut block = block(body);
        block_text(&mut block, width)
    }

    fn red_lines(text: &Text<'_>) -> usize {
        text.lines
            .iter()
            .filter(|line| {
                line.spans
                    .iter()
                    .any(|span| span.style.fg == Some(Color::Red))
            })
            .count()
    }

    #[test]
    fn text_renders_markdown_and_reuses_the_cache() {
        let mut block = block(Body::Text {
            content: "**bold**".to_owned(),
        });
        let first = block_text(&mut block, 40);
        assert_eq!(first.to_string(), "bold");
        block.body = Body::Text {
            content: "changed".to_owned(),
        };
        let cached = block_text(&mut block, 40);
        assert_eq!(cached.to_string(), "bold");
        block.revision = 2;
        let refreshed = block_text(&mut block, 40);
        assert_eq!(refreshed.to_string(), "changed");
    }

    #[test]
    fn text_reparses_when_the_width_changes() {
        let mut block = block(Body::Text {
            content: "first".to_owned(),
        });
        assert_eq!(block_text(&mut block, 40).to_string(), "first");
        block.body = Body::Text {
            content: "second".to_owned(),
        };
        assert_eq!(block_text(&mut block, 20).to_string(), "second");
    }

    #[test]
    fn reasoning_renders_with_prefix_and_dim_style() {
        let text = text(
            Body::Reasoning {
                item_id: None,
                content: "thinking it through".to_owned(),
            },
            40,
        );
        assert_eq!(text.to_string(), "reasoning thinking it through");
        assert!(text.lines[0].style.add_modifier.contains(Modifier::DIM));
    }

    #[test]
    fn tool_call_renders_the_name_and_the_first_argument_line() {
        let text = text(
            Body::ToolCall {
                item_id: None,
                call_id: "call-1".to_owned(),
                call_name: "read".to_owned(),
                call_arguments: "{\"path\":\"a\"}\nignored".to_owned(),
            },
            80,
        );
        assert_eq!(text.lines.len(), 1);
        assert_eq!(text.to_string(), "read {\"path\":\"a\"}");
    }

    #[test]
    fn tool_call_without_arguments_renders_the_name_only() {
        let text = text(
            Body::ToolCall {
                item_id: None,
                call_id: "call-1".to_owned(),
                call_name: "read".to_owned(),
                call_arguments: String::new(),
            },
            80,
        );
        assert_eq!(text.to_string(), "read");
    }

    #[test]
    fn tool_result_renders_two_lines_and_marks_errors() {
        let ok = text(
            Body::ToolResult {
                call_id: "call-1".to_owned(),
                call_res: json!({"ok": true}),
            },
            80,
        );
        assert_eq!(ok.lines.len(), 2);
        assert_eq!(ok.lines[0].to_string(), "result");
        assert!(ok.to_string().contains("\"ok\":true"));
        assert_eq!(red_lines(&ok), 0);

        let error = text(
            Body::ToolResult {
                call_id: "call-1".to_owned(),
                call_res: json!({"error": "boom"}),
            },
            80,
        );
        assert_eq!(red_lines(&error), 2);

        let refused = text(
            Body::ToolResult {
                call_id: "call-1".to_owned(),
                call_res: json!({"refused": true, "reason": "no"}),
            },
            80,
        );
        assert_eq!(red_lines(&refused), 2);

        let allowed = text(
            Body::ToolResult {
                call_id: "call-1".to_owned(),
                call_res: json!({"refused": false}),
            },
            80,
        );
        assert_eq!(red_lines(&allowed), 0);
    }

    #[test]
    fn media_blocks_render_placeholders() {
        assert_eq!(
            text(
                Body::Image {
                    content: "abcd".to_owned(),
                },
                40
            )
            .to_string(),
            "image 4B"
        );
        assert_eq!(
            text(
                Body::File {
                    content: "abcde".to_owned(),
                },
                40
            )
            .to_string(),
            "file 5B"
        );
        assert_eq!(
            text(
                Body::Audio {
                    item_id: None,
                    format: "mp3".to_owned(),
                    content: "abc".to_owned(),
                },
                40
            )
            .to_string(),
            "audio mp3 3B"
        );
        assert_eq!(
            text(
                Body::Audio {
                    item_id: None,
                    format: String::new(),
                    content: "abc".to_owned(),
                },
                40
            )
            .to_string(),
            "audio 3B"
        );
    }

    #[test]
    fn wide_tool_arguments_are_truncated() {
        let text = text(
            Body::ToolCall {
                item_id: None,
                call_id: "call-1".to_owned(),
                call_name: "read".to_owned(),
                call_arguments: "abcdefghijklmnop".to_owned(),
            },
            10,
        );
        assert_eq!(text.lines.len(), 1);
        assert!(text.to_string().ends_with('…'));
        assert!(text.width() <= 10);
    }

    #[test]
    fn narrow_tool_names_are_truncated() {
        let text = text(
            Body::ToolCall {
                item_id: None,
                call_id: "call-1".to_owned(),
                call_name: "a_very_long_tool_name".to_owned(),
                call_arguments: "{\"path\":\"a\"}".to_owned(),
            },
            8,
        );
        assert_eq!(text.lines.len(), 1);
        assert_eq!(text.to_string(), "a_very_…");
        assert!(text.width() <= 8);
    }

    #[test]
    fn message_title_carries_the_role_and_terminal_state() {
        let mut message = MessageView::new(1, Role::Assistant);
        message.sealed = Some("stop".to_owned());
        assert_eq!(message_title(&message).to_string(), "assistant stop");
        message.interrupted = Some("failed".to_owned());
        let title = message_title(&message);
        assert_eq!(title.to_string(), "assistant stop failed");
        assert!(
            title
                .spans
                .iter()
                .any(|span| span.style.fg == Some(Color::Red))
        );
    }
}
