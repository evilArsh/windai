use crate::theme;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use unicode_width::UnicodeWidthStr;

/// 屏幕格坐标，列与行都从终端左上角算起
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pos {
    pub column: u16,
    pub row: u16,
}

impl Pos {
    pub fn new(column: u16, row: u16) -> Self {
        Self { column, row }
    }

    fn before(self, other: Self) -> bool {
        (self.row, self.column) < (other.row, other.column)
    }

    fn clamp(self, area: Rect) -> Self {
        let right = area.right().saturating_sub(1).max(area.left());
        let bottom = area.bottom().saturating_sub(1).max(area.top());
        Self {
            column: self.column.clamp(area.left(), right),
            row: self.row.clamp(area.top(), bottom),
        }
    }
}

/// 拖选区域
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Select {
    /// 面板内容区
    pub area: Rect,
    pub anchor: Pos,
    pub head: Pos,
}

impl Select {
    pub fn new(area: Rect, anchor: Pos) -> Self {
        Self {
            area,
            anchor: anchor.clamp(area),
            head: anchor.clamp(area),
        }
    }

    pub fn drag_to(&mut self, head: Pos) {
        self.head = head.clamp(self.area);
    }

    pub fn is_drag(&self) -> bool {
        self.anchor != self.head
    }

    fn span(&self) -> (Pos, Pos) {
        if self.head.before(self.anchor) {
            (self.head, self.anchor)
        } else {
            (self.anchor, self.head)
        }
    }

    pub fn text(&self, rows: &[Row]) -> String {
        let (start, end) = self.span();
        let left = self.area.left();
        let first = usize::from(start.row.saturating_sub(self.area.top()));
        let last = usize::from(end.row.saturating_sub(self.area.top()));
        let mut lines = Vec::new();
        for (index, row) in rows.iter().enumerate().take(last + 1).skip(first) {
            let from = if index == first {
                start.column.saturating_sub(left)
            } else {
                0
            };
            let to = if index == last {
                end.column.saturating_sub(left)
            } else {
                u16::MAX
            };
            lines.push(row.slice(from, to));
        }
        lines.join("\n").trim_end().to_owned()
    }

    pub fn paint(&self, buffer: &mut Buffer) {
        let (start, end) = self.span();
        for row in start.row..=end.row {
            let from = if row == start.row {
                start.column
            } else {
                self.area.left()
            };
            let to = if row == end.row {
                end.column
            } else {
                self.area.right()
            };
            let mut column = self.area.left();
            while column < self.area.right() {
                let width = u16::try_from(buffer[(column, row)].symbol().width())
                    .unwrap_or(1)
                    .max(1);
                if column < to && column.saturating_add(width) > from {
                    buffer[(column, row)].set_style(theme::SELECTED);
                }
                column = column.saturating_add(width);
            }
        }
    }
}

/// 面板的一行，按屏幕格切分
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    cells: Vec<(String, u16)>,
}

impl Row {
    pub fn text(&self) -> String {
        self.slice(0, u16::MAX)
    }

    pub fn slice(&self, from: u16, to: u16) -> String {
        let mut text = String::new();
        let mut column = 0u16;
        for (symbol, width) in &self.cells {
            let start = column;
            column = column.saturating_add(*width);
            if column > from && start < to {
                text.push_str(symbol);
            }
        }
        text.trim_end().to_owned()
    }
}

pub fn rows(buffer: &Buffer, area: Rect) -> Vec<Row> {
    if area.is_empty() {
        return Vec::new();
    }
    (area.top()..area.bottom())
        .map(|row| cells(buffer, area, row))
        .collect()
}

fn cells(buffer: &Buffer, area: Rect, row: u16) -> Row {
    let mut cells = Vec::new();
    let mut column = area.left();
    while column < area.right() {
        let symbol = buffer[(column, row)].symbol();
        let width = u16::try_from(symbol.width()).unwrap_or(1).max(1);
        cells.push((symbol.to_owned(), width));
        column = column.saturating_add(width);
    }
    Row { cells }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::style::{Modifier, Style};

    fn buffer(lines: &[&str]) -> Buffer {
        let width = lines.iter().map(|line| line.width()).max().unwrap_or(0) as u16;
        let mut buffer = Buffer::empty(Rect::new(0, 0, width, lines.len() as u16));
        for (y, line) in lines.iter().enumerate() {
            buffer.set_string(0, y as u16, line, Style::default());
        }
        buffer
    }

    #[test]
    fn rows_split_wide_graphemes_into_one_cell() {
        let buffer = buffer(&["ab漢c"]);
        let extracted = rows(&buffer, buffer.area);
        assert_eq!(extracted.len(), 1);
        assert_eq!(extracted[0].text(), "ab漢c");
        assert_eq!(extracted[0].slice(0, 2), "ab");
        assert_eq!(extracted[0].slice(1, 3), "b漢");
        assert_eq!(extracted[0].slice(2, 4), "漢");
        assert_eq!(extracted[0].slice(3, 4), "漢");
        assert_eq!(extracted[0].slice(4, 5), "c");
    }

    #[test]
    fn selection_joins_rows_and_trims_the_tail() {
        let buffer = buffer(&["alpha", "beta", "gamma"]);
        let area = buffer.area;
        let mut select = Select::new(area, Pos::new(3, 0));
        select.drag_to(Pos::new(2, 2));
        assert_eq!(select.text(&rows(&buffer, area)), "ha\nbeta\nga");

        let mut reverse = Select::new(area, Pos::new(2, 2));
        reverse.drag_to(Pos::new(3, 0));
        assert_eq!(reverse.text(&rows(&buffer, area)), "ha\nbeta\nga");
    }

    #[test]
    fn selection_clamps_the_head_to_the_area() {
        let area = Rect::new(2, 3, 6, 2);
        let mut select = Select::new(area, Pos::new(4, 4));
        select.drag_to(Pos::new(0, 99));
        assert_eq!(select.head, Pos::new(2, 4));
    }

    #[test]
    fn painting_marks_the_dragged_cells_only() {
        let mut buffer = buffer(&["alphabet"]);
        let area = buffer.area;
        let mut select = Select::new(area, Pos::new(2, 0));
        select.drag_to(Pos::new(5, 0));
        select.paint(&mut buffer);
        let reversed = |column: u16| buffer[(column, 0)].modifier.contains(Modifier::REVERSED);
        assert!(!reversed(1));
        assert!(reversed(2));
        assert!(reversed(4));
        assert!(!reversed(5));
    }
}
