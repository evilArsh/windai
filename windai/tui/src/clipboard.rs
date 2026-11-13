use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use std::io::{self, Write};

pub fn copy(text: &str) -> io::Result<()> {
    let mut stdout = io::stdout();
    stdout.write_all(sequence(text).as_bytes())?;
    stdout.flush()
}

/// OSC 52 替换剪贴板
pub fn sequence(text: &str) -> String {
    format!("\x1b]52;c;{}\x1b\\", STANDARD.encode(text))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequence_carries_the_base64_payload() {
        assert_eq!(sequence("hi"), "\u{1b}]52;c;aGk=\u{1b}\\");
        assert_eq!(sequence("中文"), "\u{1b}]52;c;5Lit5paH\u{1b}\\");
    }
}
