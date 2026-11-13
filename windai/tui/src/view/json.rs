use crate::text::concise;
use crate::theme;
use ratatui::text::{Line, Span};
use serde_json::Value;
use unicode_width::UnicodeWidthStr;

pub fn indent(depth: usize) -> Span<'static> {
    Span::raw("  ".repeat(depth))
}

pub fn key(name: &str) -> Span<'static> {
    Span::styled(format!("\"{name}\""), theme::JSON_KEY)
}

pub fn punct(text: &str) -> Span<'static> {
    Span::styled(text.to_owned(), theme::JSON_PUNCT)
}

/// 展开一个值；width 为标量可用的列数，limit 为最多写入的行数
pub fn emit(
    value: &Value,
    depth: usize,
    mut prefix: Vec<Span<'static>>,
    width: u16,
    limit: usize,
    out: &mut Vec<Line<'static>>,
) {
    if out.len() > limit {
        return;
    }
    if let Some(map) = value.as_object().filter(|map| !map.is_empty()) {
        prefix.push(punct("{"));
        out.push(Line::from(prefix));
        for (index, (name, child)) in map.iter().enumerate() {
            emit(
                child,
                depth + 1,
                vec![indent(depth + 1), key(name), punct(": ")],
                width,
                limit,
                out,
            );
            if index + 1 < map.len() {
                append_comma(out);
            }
        }
        out.push(Line::from(vec![indent(depth), punct("}")]));
        return;
    }
    if let Some(items) = value.as_array().filter(|items| !items.is_empty()) {
        prefix.push(punct("["));
        out.push(Line::from(prefix));
        for (index, item) in items.iter().enumerate() {
            emit(item, depth + 1, vec![indent(depth + 1)], width, limit, out);
            if index + 1 < items.len() {
                append_comma(out);
            }
        }
        out.push(Line::from(vec![indent(depth), punct("]")]));
        return;
    }
    let available = usize::from(width).saturating_sub(span_width(&prefix));
    prefix.push(scalar(value, available));
    out.push(Line::from(prefix));
}

pub fn append_comma(out: &mut [Line<'static>]) {
    if let Some(line) = out.last_mut() {
        line.spans.push(punct(","));
    }
}

fn scalar(value: &Value, available: usize) -> Span<'static> {
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
        Value::Object(_) => punct("{}"),
        Value::Array(_) => punct("[]"),
    }
}

fn span_width(spans: &[Span<'static>]) -> usize {
    spans.iter().map(|span| span.content.width()).sum()
}
