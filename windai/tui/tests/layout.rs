use ratatui::layout::Rect;
use wind_tui::view::{Layout, layout};

const SIZES: &[(u16, u16)] = &[
    (0, 0),
    (1, 1),
    (5, 3),
    (10, 5),
    (20, 8),
    (40, 9),
    (20, 10),
    (40, 12),
    (59, 15),
    (60, 18),
    (60, 19),
    (59, 20),
    (60, 20),
    (71, 20),
    (72, 20),
    (73, 24),
    (79, 21),
    (80, 22),
    (84, 24),
    (100, 30),
    (120, 40),
    (160, 50),
    (200, 60),
    (300, 100),
    (u16::MAX, u16::MAX),
];

const BASE_BAR_HEIGHT: u16 = 1;
const BAR_HEIGHT_REQUESTS: [u16; 5] = [0, 1, 2, 3, u16::MAX];

fn expected_input_height(height: u16) -> u16 {
    let clamped = (u32::from(height) * 20 / 100).clamp(3, 6) as u16;
    clamped.min(height)
}

fn expected_bar_height(height: u16, requested: u16) -> u16 {
    if height < 10 {
        0
    } else if height < 22 {
        1
    } else {
        requested.clamp(1, 2)
    }
}

fn expected_side_width(width: u16) -> u16 {
    let cap = width.saturating_sub(40);
    ((u32::from(width) * 2 / 5) as u16).clamp(30, 64).min(cap)
}

fn side_visible(width: u16, height: u16) -> bool {
    width >= 72 && height >= 20
}

fn all_rects(layout: &Layout) -> Vec<Rect> {
    let mut rects = vec![layout.content, layout.bar, layout.input];
    if let Some(side) = &layout.side {
        rects.push(side.mods);
        rects.push(side.tree);
        rects.push(side.log);
    }
    rects
}

fn assert_inside(area: Rect, rect: Rect, context: &str) {
    assert!(
        rect.x >= area.x && rect.y >= area.y,
        "{context}: {rect:?} starts outside {area:?}"
    );
    assert!(
        rect.right() <= area.right() && rect.bottom() <= area.bottom(),
        "{context}: {rect:?} ends outside {area:?}"
    );
}

fn assert_no_overlap(rects: &[Rect], context: &str) {
    for (index, first) in rects.iter().enumerate() {
        if first.is_empty() {
            continue;
        }
        for second in rects.iter().skip(index + 1) {
            if second.is_empty() {
                continue;
            }
            assert!(
                !first.intersects(*second),
                "{context}: {first:?} overlaps {second:?}"
            );
        }
    }
}

#[test]
fn layout_keeps_every_rect_inside_and_disjoint() {
    for &(width, height) in SIZES {
        let area = Rect::new(0, 0, width, height);
        for bar_height in BAR_HEIGHT_REQUESTS {
            for show_side in [true, false] {
                let layout = layout(area, bar_height, show_side);
                let context = format!("{width}x{height} bar={bar_height} side={show_side}");
                let rects = all_rects(&layout);
                for rect in &rects {
                    assert_inside(area, *rect, &context);
                }
                assert_no_overlap(&rects, &context);
            }
        }
    }
}

#[test]
fn layout_accounts_for_offset_area() {
    let area = Rect::new(7, 3, 100, 30);
    let layout = layout(area, BASE_BAR_HEIGHT, true);
    let rects = all_rects(&layout);
    for rect in &rects {
        assert_inside(area, *rect, "offset area");
    }
    assert_no_overlap(&rects, "offset area");
    assert_eq!(layout.content.x, area.x);
    assert_eq!(layout.content.y, area.y);
    assert_eq!(layout.input.bottom(), area.bottom());
    let side = layout.side.as_ref().expect("side layout is visible");
    assert_eq!(side.mods.x, area.x + layout.content.width);
}

#[test]
fn hidden_side_thresholds() {
    for &(width, height) in SIZES {
        let with_side = layout(Rect::new(0, 0, width, height), BASE_BAR_HEIGHT, true);
        assert_eq!(
            with_side.side.is_some(),
            side_visible(width, height),
            "side visibility at {width}x{height}"
        );
        let without_side = layout(Rect::new(0, 0, width, height), BASE_BAR_HEIGHT, false);
        assert!(
            without_side.side.is_none(),
            "side is hidden when show_side is false at {width}x{height}"
        );
    }
}

#[test]
fn hiding_the_side_panel_lets_the_left_column_fill_the_width() {
    for &(width, height) in SIZES.iter().filter(|(_, height)| *height >= 10) {
        let layout = layout(Rect::new(0, 0, width, height), BASE_BAR_HEIGHT, false);
        assert_eq!(
            layout.content.width, width,
            "content width at {width}x{height}"
        );
        assert_eq!(layout.bar.width, width, "bar width at {width}x{height}");
        assert_eq!(layout.input.width, width, "input width at {width}x{height}");
    }
}

#[test]
fn input_height_is_clamped() {
    for &(width, height) in SIZES {
        let layout = layout(Rect::new(0, 0, width, height), BASE_BAR_HEIGHT, true);
        assert_eq!(
            layout.input.height,
            expected_input_height(height),
            "input height at {width}x{height}"
        );
        if height < 10 {
            assert_eq!(layout.input.width, width);
        }
    }
}

#[test]
fn bar_height_follows_breakpoint_and_request() {
    for &(width, height) in SIZES {
        let area = Rect::new(0, 0, width, height);
        for bar_height in BAR_HEIGHT_REQUESTS {
            let layout = layout(area, bar_height, true);
            assert_eq!(
                layout.bar.height,
                expected_bar_height(height, bar_height),
                "bar height at {width}x{height} requested {bar_height}"
            );
        }
    }
}

#[test]
fn tiny_height_draws_only_input() {
    for &(width, height) in SIZES.iter().filter(|(_, height)| *height < 10) {
        let layout = layout(Rect::new(0, 0, width, height), BASE_BAR_HEIGHT, true);
        assert!(layout.content.is_empty(), "content at {width}x{height}");
        assert!(layout.bar.is_empty(), "bar at {width}x{height}");
        assert_eq!(layout.input.height, expected_input_height(height));
        assert_eq!(layout.input.width, width);
        assert_eq!(layout.input.bottom(), height);
    }
}

#[test]
fn left_column_fills_height() {
    for &(width, height) in SIZES.iter().filter(|(_, height)| *height >= 10) {
        let layout = layout(Rect::new(0, 0, width, height), BASE_BAR_HEIGHT, true);
        assert_eq!(layout.content.y, 0, "content at {width}x{height}");
        assert_eq!(layout.bar.y, layout.content.bottom());
        assert_eq!(layout.input.y, layout.bar.bottom());
        assert_eq!(layout.input.bottom(), height);
        let left_width = if side_visible(width, height) {
            width - expected_side_width(width)
        } else {
            width
        };
        assert_eq!(layout.content.width, left_width);
        assert_eq!(layout.bar.width, left_width);
        assert_eq!(layout.input.width, left_width);
    }
}

#[test]
fn side_layers_partition_the_side_column() {
    for &(width, height) in SIZES.iter().filter(|(w, h)| side_visible(*w, *h)) {
        let area = Rect::new(0, 0, width, height);
        let layout = layout(area, BASE_BAR_HEIGHT, true);
        let side = layout.side.as_ref().expect("side layout is visible");
        let side_width = expected_side_width(width);
        assert_eq!(side.mods.x, width - side_width);
        assert_eq!(side.mods.width, side_width);
        assert_eq!(side.mods.y, 0);
        assert_eq!(side.mods.height, (height / 4).clamp(3, 10));
        assert_eq!(side.tree.y, side.mods.bottom());
        assert_eq!(side.tree.height, (height / 3).clamp(4, 14));
        assert_eq!(side.log.y, side.tree.bottom());
        assert_eq!(side.log.bottom(), height);
        assert_eq!(
            side.log.height,
            height - side.mods.height - side.tree.height
        );
        assert_inside(area, side.mods, "side mods");
        assert_inside(area, side.tree, "side tree");
        assert_inside(area, side.log, "side log");
    }
}
