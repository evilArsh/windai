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

pub fn render(frame: &mut Frame, area: Rect, session: &mut Session, focused: bool) {
    if area.is_empty() {
        return;
    }
    let border = if focused { theme::FOCUS } else { theme::BORDER };
    let block = Block::bordered()
        .border_style(border)
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
        if message.blocks.is_empty() {
            lines.push(Line::default());
            continue;
        }
        for block in &mut message.blocks {
            lines.extend(block_text(block, width).lines);
            lines.push(Line::default());
        }
    }
    let paragraph = Paragraph::new(lines).wrap(Wrap { trim: false });
    let total = u16::try_from(paragraph.line_count(inner.width)).unwrap_or(u16::MAX);
    session.content_max = max_scroll(total, inner.height);
    if session.content_max == 0 {
        session.follow = true;
    }
    session.content_scroll =
        scroll_offset(session.content_scroll, session.follow, total, inner.height);
    frame.render_widget(paragraph.scroll((session.content_scroll, 0)), inner);
}

pub fn max_scroll(total: u16, height: u16) -> u16 {
    total.saturating_sub(height)
}

pub fn scroll_offset(scroll: u16, follow: bool, total: u16, height: u16) -> u16 {
    let max = max_scroll(total, height);
    if follow { max } else { scroll.min(max) }
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
        Body::Reasoning { content, .. } => plain(content.clone(), theme::REASONING),
        Body::ToolCall {
            call_name,
            call_arguments,
            ..
        } => Text::from(Line::from(Span::styled(
            tool_call_line(call_name, call_arguments, width),
            theme::TOOL_CALL,
        ))),
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

/// 结果体最多渲染的行数，超出部分以省略号收尾
const RESULT_BODY_LINES: usize = 60;

fn tool_result_text(call_res: &Value, width: u16) -> Text<'static> {
    let failed = tool_result_failed(call_res);
    let mut body = Vec::new();
    match call_res {
        Value::Object(map) if !map.is_empty() => {
            for (key, value) in map {
                emit_json(
                    value,
                    1,
                    vec![indent(1), key_span(key), punct_span(": ")],
                    width,
                    &mut body,
                );
            }
        }
        value => emit_json(value, 1, vec![indent(1)], width, &mut body),
    }
    let clipped = body.len() > RESULT_BODY_LINES;
    body.truncate(RESULT_BODY_LINES);
    if clipped {
        body.push(Line::from(Span::styled("…".to_owned(), theme::JSON_PUNCT)));
    }
    if failed {
        for line in &mut body {
            line.style = theme::ERROR;
            for span in &mut line.spans {
                span.style = theme::ERROR;
            }
        }
    }
    Text::from(body)
}

fn emit_json(
    value: &Value,
    depth: usize,
    mut prefix: Vec<Span<'static>>,
    width: u16,
    out: &mut Vec<Line<'static>>,
) {
    if out.len() > RESULT_BODY_LINES {
        return;
    }
    if let Some(map) = value.as_object().filter(|map| !map.is_empty()) {
        prefix.push(punct_span("{"));
        out.push(Line::from(prefix));
        for (key, child) in map {
            emit_json(
                child,
                depth + 1,
                vec![indent(depth + 1), key_span(key), punct_span(": ")],
                width,
                out,
            );
        }
        out.push(Line::from(vec![indent(depth), punct_span("}")]));
        return;
    }
    if let Some(items) = value.as_array().filter(|items| !items.is_empty()) {
        prefix.push(punct_span("["));
        out.push(Line::from(prefix));
        for item in items {
            emit_json(item, depth + 1, vec![indent(depth + 1)], width, out);
        }
        out.push(Line::from(vec![indent(depth), punct_span("]")]));
        return;
    }
    let available = usize::from(width).saturating_sub(span_width(&prefix));
    prefix.push(scalar_span(value, available));
    out.push(Line::from(prefix));
}

fn indent(depth: usize) -> Span<'static> {
    Span::raw("  ".repeat(depth))
}

fn key_span(key: &str) -> Span<'static> {
    Span::styled(format!("\"{key}\""), theme::JSON_KEY)
}

fn punct_span(text: &str) -> Span<'static> {
    Span::styled(text.to_owned(), theme::JSON_PUNCT)
}

fn scalar_span(value: &Value, available: usize) -> Span<'static> {
    match value {
        Value::String(text) => Span::styled(
            concise(&format!("\"{text}\""), available),
            theme::JSON_STRING,
        ),
        Value::Number(number) => {
            Span::styled(concise(&number.to_string(), available), theme::JSON_NUMBER)
        }
        Value::Bool(true) => Span::styled("true".to_owned(), theme::JSON_LITERAL),
        Value::Bool(false) => Span::styled("false".to_owned(), theme::JSON_LITERAL),
        Value::Null => Span::styled("null".to_owned(), theme::JSON_LITERAL),
        Value::Object(_) => punct_span("{}"),
        Value::Array(_) => punct_span("[]"),
    }
}

fn span_width(spans: &[Span<'static>]) -> usize {
    spans.iter().map(|span| span.content.width()).sum()
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
    fn reasoning_renders_dim_without_a_title() {
        let text = text(
            Body::Reasoning {
                item_id: None,
                content: "thinking it through".to_owned(),
            },
            40,
        );
        assert_eq!(text.to_string(), "thinking it through");
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
    fn tool_result_renders_pretty_json_and_marks_errors() {
        let ok = text(
            Body::ToolResult {
                call_id: "call-1".to_owned(),
                call_res: json!({"ok": true}),
            },
            80,
        );
        assert_eq!(ok.to_string(), "  \"ok\": true");
        assert_eq!(red_lines(&ok), 0);

        let error = text(
            Body::ToolResult {
                call_id: "call-1".to_owned(),
                call_res: json!({"error": "boom"}),
            },
            80,
        );
        assert_eq!(red_lines(&error), 1);
        assert_eq!(error.lines[0].to_string(), "  \"error\": \"boom\"");

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
    fn nested_tool_results_are_indented() {
        let nested = text(
            Body::ToolResult {
                call_id: "call-1".to_owned(),
                call_res: json!({"user": {"id": 1, "tags": ["a", null]}, "ok": true}),
            },
            80,
        );
        assert_eq!(
            nested.to_string(),
            concat!(
                "  \"user\": {\n",
                "    \"id\": 1\n",
                "    \"tags\": [\n",
                "      \"a\"\n",
                "      null\n",
                "    ]\n",
                "  }\n",
                "  \"ok\": true"
            )
        );
    }

    #[test]
    fn empty_containers_and_scalars_stay_on_one_line() {
        let scalar = text(
            Body::ToolResult {
                call_id: "call-1".to_owned(),
                call_res: json!("done"),
            },
            80,
        );
        assert_eq!(scalar.to_string(), "  \"done\"");

        let nested = text(
            Body::ToolResult {
                call_id: "call-1".to_owned(),
                call_res: json!({"empty": {}, "list": []}),
            },
            80,
        );
        assert_eq!(nested.to_string(), "  \"empty\": {}\n  \"list\": []");
    }

    #[test]
    fn long_tool_results_stop_at_the_line_budget() {
        let items: Vec<i32> = (0..200).collect();
        let text = text(
            Body::ToolResult {
                call_id: "call-1".to_owned(),
                call_res: json!({"items": items}),
            },
            80,
        );
        assert_eq!(text.lines.len(), RESULT_BODY_LINES + 1);
        assert_eq!(
            text.lines.last().map(ToString::to_string).as_deref(),
            Some("…")
        );
    }

    #[test]
    fn long_tool_result_lines_are_truncated() {
        let text = text(
            Body::ToolResult {
                call_id: "call-1".to_owned(),
                call_res: json!({"path": "a-very-long-value-that-does-not-fit"}),
            },
            16,
        );
        assert_eq!(text.lines.len(), 1);
        assert!(text.lines[0].to_string().ends_with('…'));
        assert!(text.lines[0].width() <= 16);
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

    fn session_with_lines(count: usize) -> Session {
        let topic = wind_storage::Topic {
            id: 1,
            parent_id: None,
            label: "topic".to_owned(),
            model_id: None,
            icon: None,
            workspace: None,
        };
        let mut session = Session::new(topic, None);
        let mut message = MessageView::new(1, Role::Assistant);
        let content = (0..count)
            .map(|index| format!("line {index}"))
            .collect::<Vec<_>>()
            .join("\n");
        message
            .blocks
            .push(BlockView::new(0, 1, Body::Text { content }));
        session.messages.push(message);
        session
    }

    #[test]
    fn following_scrolls_to_the_last_line() {
        let mut terminal = ratatui::Terminal::new(ratatui::backend::TestBackend::new(40, 12))
            .expect("build a test terminal");
        let mut session = session_with_lines(100);
        terminal
            .draw(|frame| render(frame, frame.area(), &mut session, false))
            .expect("draw the content");
        assert!(session.follow);
        let rendered = terminal.backend().buffer().content().to_vec();
        let text: String = rendered.iter().map(|cell| cell.symbol()).collect();
        assert!(text.contains("line 99"), "{text}");
    }

    #[test]
    fn short_content_has_no_scroll() {
        assert_eq!(max_scroll(5, 10), 0);
        assert_eq!(scroll_offset(4, false, 5, 10), 0);
        assert_eq!(scroll_offset(0, true, 5, 10), 0);
    }

    #[test]
    fn following_stays_at_the_bottom_after_an_append() {
        assert_eq!(scroll_offset(0, true, 100, 10), 90);
        assert_eq!(scroll_offset(0, true, 120, 10), 110);
    }

    #[test]
    fn a_stopped_view_keeps_its_offset_and_clamps_on_shrink() {
        assert_eq!(scroll_offset(40, false, 100, 10), 40);
        assert_eq!(scroll_offset(40, false, 30, 10), 20);
        assert_eq!(scroll_offset(40, false, 5, 10), 0);
    }
}
