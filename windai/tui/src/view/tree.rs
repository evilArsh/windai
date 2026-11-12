use super::pane_offset;
use crate::app::{NodeView, phase_is_terminal};
use crate::text::concise;
use crate::theme;
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::Line;
use ratatui::widgets::{Block, Paragraph};
use std::collections::HashMap;
use wind_wire::AgentRole;

pub fn render(
    frame: &mut Frame,
    area: Rect,
    nodes: &[NodeView],
    scroll: &mut u16,
    focused: bool,
) -> u16 {
    if area.is_empty() {
        return 0;
    }
    let border = if focused { theme::FOCUS } else { theme::BORDER };
    let block = Block::bordered()
        .border_style(border)
        .title("Layer 2 tree")
        .title_style(theme::TITLE);
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if inner.is_empty() {
        return 0;
    }
    let width = usize::from(inner.width);
    let lines: Vec<Line<'static>> = ordered(nodes)
        .into_iter()
        .map(|(depth, node)| {
            let mut text = format!(
                "{}#{} {} {}",
                "  ".repeat(depth),
                node.id,
                node.role.as_ref(),
                node.phase
            );
            if let Some(wait) = node.suspended_wait {
                text.push_str(&format!(" wait #{wait}"));
            }
            let style = if phase_is_terminal(&node.phase) {
                theme::BAR
            } else {
                Style::default()
            };
            Line::from(concise(&text, width)).style(style)
        })
        .collect();
    let total = u16::try_from(lines.len()).unwrap_or(u16::MAX);
    let (offset, max) = pane_offset(total, inner.height, *scroll);
    *scroll = offset;
    frame.render_widget(Paragraph::new(lines).scroll((offset, 0)), inner);
    max
}

fn ordered(nodes: &[NodeView]) -> Vec<(usize, &NodeView)> {
    let known: HashMap<i64, ()> = nodes.iter().map(|node| (node.id, ())).collect();
    let mut children: HashMap<Option<i64>, Vec<&NodeView>> = HashMap::new();
    for node in nodes {
        let parent = node.parent_id.filter(|parent| known.contains_key(parent));
        children.entry(parent).or_default().push(node);
    }
    for group in children.values_mut() {
        group.sort_by_key(|node| node.id);
    }
    let Some(mut roots) = children.remove(&None) else {
        return Vec::new();
    };
    roots.sort_by_key(|node| (node.role != AgentRole::Main, node.id));
    let mut stack: Vec<(usize, &NodeView)> =
        roots.into_iter().rev().map(|node| (0, node)).collect();
    let mut ordered = Vec::with_capacity(nodes.len());
    while let Some((depth, node)) = stack.pop() {
        ordered.push((depth, node));
        if let Some(group) = children.get(&Some(node.id)) {
            for child in group.iter().rev() {
                stack.push((depth + 1, child));
            }
        }
    }
    ordered
}
