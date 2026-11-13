use super::json;
use crate::text::format_time;
use crate::theme;
use ratatui::text::{Line, Span, Text};
use wind_wire::event::FeedEvent;

pub fn title(event: &FeedEvent) -> String {
    format!(
        "#{} {} {}",
        event.key.seq,
        event.envelope.name,
        format_time(event.envelope.time)
    )
}

pub fn body(event: &FeedEvent) -> Text<'static> {
    let mut lines = Vec::new();
    match serde_json::to_value(event) {
        Ok(value) => json::emit(&value, 0, Vec::new(), u16::MAX, usize::MAX, &mut lines),
        Err(error) => lines.push(Line::from(Span::styled(
            format!("cannot serialize the event: {error}"),
            theme::ERROR,
        ))),
    }
    Text::from(lines)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wind_wire::event::{Envelope, EventKey, Origin, Scope, Shape, Source, Target};

    fn event(payload: serde_json::Value) -> FeedEvent {
        FeedEvent {
            key: EventKey::new(7, 42).expect("build an event key"),
            envelope: Envelope {
                time: 0,
                name: "core/block.committed".to_owned(),
                scope: Scope::Topic {
                    topic_id: 1,
                    instance_id: 2,
                },
                origin: Origin::Internal,
                source: Source::core("test"),
                payload: Some(payload),
                shape: Shape::Observe,
            },
            target: Target::Core,
        }
    }

    #[test]
    fn title_carries_the_sequence_and_the_name() {
        let event = event(json!({}));
        let title = title(&event);
        assert!(title.starts_with("#42 core/block.committed "), "{title}");
    }

    #[test]
    fn body_keeps_every_field_without_clipping() {
        let long = "x".repeat(400);
        let event = event(json!({"block": {"content": long}}));
        let lines: Vec<String> = body(&event).lines.iter().map(ToString::to_string).collect();
        assert!(lines[0].trim() == "{", "{:?}", lines[0]);
        assert!(lines.iter().any(|line| line.contains("\"key\"")));
        assert!(lines.iter().any(|line| line.contains("\"envelope\"")));
        assert!(lines.iter().any(|line| line.contains("\"seq\": \"42\"")));
        assert!(lines.iter().any(|line| line.contains(&"x".repeat(400))));
    }
}
