use unicode_width::UnicodeWidthStr;
use wind_tui::view::bar::{BarData, BarKind, BarLayout, BarSegment, branch_from_head, compose};
use wind_wire::Usage;

fn full_data() -> BarData {
    BarData {
        provider: Some("deepseek".to_owned()),
        model: Some("deepseek-chat".to_owned()),
        thinking: Some("max".to_owned()),
        directory: Some("/work".to_owned()),
        branch: Some("agent".to_owned()),
        usage: Usage {
            input_tokens: 114_000,
            output_tokens: 52_000,
        },
        phase: Some("running".to_owned()),
        phase_reason: None,
    }
}

fn segment(layout: &BarLayout, kind: BarKind) -> Option<&BarSegment> {
    layout
        .segments
        .iter()
        .find(|segment| segment.kind == Some(kind))
}

fn texts(layout: &BarLayout) -> Vec<&str> {
    layout
        .segments
        .iter()
        .map(|segment| segment.text.as_str())
        .collect()
}

fn joined_width(parts: &[&str]) -> u16 {
    let content: usize = parts.iter().map(|part| part.width()).sum();
    (content + 3 * parts.len().saturating_sub(1)) as u16
}

fn contains(rect: ratatui::layout::Rect, x: u16, y: u16) -> bool {
    x >= rect.x && x < rect.right() && y >= rect.y && y < rect.bottom()
}

#[test]
fn compose_emits_every_segment_in_order() {
    let layout = compose(&full_data(), 200, 30);
    assert_eq!(layout.lines, 1);
    assert_eq!(layout.rows.len(), 1);
    assert_eq!(
        layout.rows[0].to_string(),
        "deepseek | deepseek-chat | think:max | /work | ⎇ agent | ◫ 114k/52k tok | running"
    );
    assert_eq!(
        texts(&layout),
        [
            "deepseek",
            "deepseek-chat",
            "think:max",
            "/work",
            "⎇ agent",
            "◫ 114k/52k tok",
            "running"
        ]
    );
    assert_eq!(layout.segments[0].kind, Some(BarKind::Provider));
    assert_eq!(layout.segments[1].kind, Some(BarKind::Model));
    assert!(
        layout.segments[2..]
            .iter()
            .all(|segment| segment.kind.is_none())
    );
}

#[test]
fn compose_omits_missing_segments() {
    let data = BarData {
        provider: Some("deepseek".to_owned()),
        model: Some("deepseek-chat".to_owned()),
        thinking: None,
        directory: Some("/work".to_owned()),
        branch: None,
        usage: Usage {
            input_tokens: 0,
            output_tokens: 0,
        },
        phase: None,
        phase_reason: None,
    };
    let layout = compose(&data, 200, 30);
    assert_eq!(
        layout.rows[0].to_string(),
        "deepseek | deepseek-chat | /work | ◫ 0/0 tok"
    );
    assert!(segment(&layout, BarKind::Provider).is_some());
    assert!(segment(&layout, BarKind::Model).is_some());
}

#[test]
fn compose_renders_without_a_provider_or_model() {
    let data = BarData {
        provider: None,
        model: None,
        thinking: Some("high".to_owned()),
        directory: None,
        branch: None,
        usage: Usage {
            input_tokens: 0,
            output_tokens: 0,
        },
        phase: None,
        phase_reason: None,
    };
    let layout = compose(&data, 80, 30);
    assert_eq!(layout.lines, 1);
    assert_eq!(layout.rows[0].to_string(), "think:high | ◫ 0/0 tok");
    assert!(layout.segments.iter().all(|segment| segment.kind.is_none()));
}

#[test]
fn compose_appends_the_phase_reason() {
    let mut data = full_data();
    data.phase = Some("failed".to_owned());
    data.phase_reason = Some("boom".to_owned());
    let layout = compose(&data, 200, 30);
    assert_eq!(
        layout.segments.last().map(|segment| segment.text.as_str()),
        Some("failed: boom")
    );

    data.phase = Some("cancelled".to_owned());
    data.phase_reason = Some("cancelled".to_owned());
    let layout = compose(&data, 200, 30);
    assert_eq!(
        layout.segments.last().map(|segment| segment.text.as_str()),
        Some("cancelled")
    );
}

#[test]
fn compose_splits_into_two_lines_when_allowed() {
    let layout = compose(&full_data(), 50, 30);
    assert_eq!(layout.lines, 2);
    assert_eq!(layout.rows.len(), 2);
    assert_eq!(layout.rows[0].to_string(), "deepseek | deepseek-chat");
    assert_eq!(
        segment(&layout, BarKind::Provider).map(|segment| segment.rect),
        Some(ratatui::layout::Rect::new(0, 0, 8, 1))
    );
    assert_eq!(
        segment(&layout, BarKind::Model).map(|segment| segment.rect),
        Some(ratatui::layout::Rect::new(11, 0, 13, 1))
    );
    let thinking = layout
        .segments
        .iter()
        .find(|segment| segment.text == "think:max")
        .expect("the thinking segment is visible");
    assert_eq!(thinking.rect.y, 1);
}

#[test]
fn compose_keeps_one_line_below_the_height_breakpoint() {
    let layout = compose(&full_data(), 50, 21);
    assert_eq!(layout.lines, 1);
    assert_eq!(layout.rows.len(), 1);
    assert_eq!(
        layout.rows[0].to_string(),
        "deepseek | deepseek-chat | think:max | /work"
    );
}

#[test]
fn compose_discards_segments_from_the_tail() {
    let expected = [
        "deepseek",
        "deepseek-chat",
        "think:max",
        "/work",
        "⎇ agent",
        "◫ 114k/52k tok",
        "running",
    ];
    for keep in (2..=expected.len()).rev() {
        let width = joined_width(&expected[..keep]);
        let layout = compose(&full_data(), width, 20);
        assert_eq!(layout.lines, 1, "keep {keep}");
        assert_eq!(texts(&layout), expected[..keep], "keep {keep}");
    }
}

#[test]
fn compose_truncates_the_model_name_when_only_provider_and_model_remain() {
    let data = BarData {
        provider: Some("p".to_owned()),
        model: Some("long-model-name".to_owned()),
        thinking: None,
        directory: None,
        branch: None,
        usage: Usage {
            input_tokens: 0,
            output_tokens: 0,
        },
        phase: None,
        phase_reason: None,
    };
    let layout = compose(&data, 8, 20);
    assert_eq!(layout.lines, 1);
    assert_eq!(
        segment(&layout, BarKind::Provider).map(|segment| segment.text.as_str()),
        Some("p")
    );
    let model = segment(&layout, BarKind::Model).expect("the model is present");
    assert!(model.text.ends_with('…'), "model text is {}", model.text);
    assert_eq!(layout.rows[0].to_string(), "p | lon…");
    assert!(layout.rows[0].to_string().width() <= 8);
    assert!(
        layout
            .segments
            .iter()
            .all(|segment| segment.text != "◫ 0/0 tok")
    );
}

#[test]
fn usage_numbers_are_abbreviated_and_saturated() {
    let usage = |input_tokens, output_tokens| BarData {
        provider: None,
        model: None,
        thinking: None,
        directory: None,
        branch: None,
        usage: Usage {
            input_tokens,
            output_tokens,
        },
        phase: None,
        phase_reason: None,
    };
    assert_eq!(
        compose(&usage(0, 999), 80, 30).segments[0].text,
        "◫ 0/999 tok"
    );
    assert_eq!(
        compose(&usage(1000, 114_000), 80, 30).segments[0].text,
        "◫ 1k/114k tok"
    );
    assert_eq!(
        compose(&usage(-5, -1999), 80, 30).segments[0].text,
        "◫ 0/0 tok"
    );
}

#[test]
fn branch_from_head_reads_branch_names_and_detached_heads() {
    assert_eq!(
        branch_from_head("ref: refs/heads/agent\n"),
        Some("agent".to_owned())
    );
    assert_eq!(
        branch_from_head("ref: refs/heads/feature/nested"),
        Some("feature/nested".to_owned())
    );
    assert_eq!(
        branch_from_head("0123456789abcdef0123456789abcdef01234567\n"),
        Some("0123456".to_owned())
    );
    assert_eq!(branch_from_head("ref: refs/tags/v1"), None);
    assert_eq!(branch_from_head("not a head"), None);
    assert_eq!(branch_from_head(""), None);
    assert_eq!(branch_from_head("   \n"), None);
    assert_eq!(branch_from_head("0123456789abcdef"), None);
}

#[test]
fn clickable_segments_expose_their_drawn_rectangles() {
    let layout = compose(&full_data(), 200, 30);
    let provider = segment(&layout, BarKind::Provider).expect("the provider is visible");
    let model = segment(&layout, BarKind::Model).expect("the model is visible");
    assert_eq!(provider.rect, ratatui::layout::Rect::new(0, 0, 8, 1));
    assert_eq!(model.rect, ratatui::layout::Rect::new(11, 0, 13, 1));
    assert_eq!(
        layout
            .segments
            .iter()
            .find(|segment| contains(segment.rect, 0, 0))
            .and_then(|segment| segment.kind),
        Some(BarKind::Provider)
    );
    assert_eq!(
        layout
            .segments
            .iter()
            .find(|segment| contains(segment.rect, 11, 0))
            .and_then(|segment| segment.kind),
        Some(BarKind::Model)
    );
    assert!(
        layout
            .segments
            .iter()
            .all(|segment| !contains(segment.rect, 8, 0))
    );
    let thinking = layout
        .segments
        .iter()
        .find(|segment| segment.text == "think:max")
        .expect("the thinking segment is visible");
    assert_eq!(thinking.kind, None);
}

#[test]
fn segment_rectangles_stay_inside_the_requested_width() {
    for width in [1u16, 2, 5, 10, 20, 40, 80, 200] {
        let layout = compose(&full_data(), width, 30);
        for segment in &layout.segments {
            assert!(
                segment.rect.right() <= width,
                "segment {:?} exceeds width {width}",
                segment.rect
            );
        }
        for row in &layout.rows {
            assert!(row.to_string().width() <= usize::from(width));
        }
    }
}
