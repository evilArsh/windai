use std::io::{self, Stdout};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crossterm::cursor::{Hide, Show};
use crossterm::event::{
    self, DisableBracketedPaste, DisableMouseCapture, EnableBracketedPaste, EnableMouseCapture,
    Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers,
};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui_textarea::TextArea;
use tokio::runtime::Builder;
use tokio::sync::mpsc;
use tokio::time::{MissedTickBehavior, interval};

use crate::theme;
use crate::view;

const TICK: Duration = Duration::from_millis(33);
const EVENT_BUFFER: usize = 128;
const POLL_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Focus {
    Input,
    Content,
    Mods,
    Tree,
    Log,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Screen {
    Empty,
}

pub struct App {
    screen: Screen,
    input: TextArea<'static>,
    focus: Focus,
    quit: bool,
    dirty: bool,
}

impl App {
    pub fn new() -> Self {
        let mut input = TextArea::new(vec![String::new()]);
        input.set_placeholder_text("Input");
        input.set_placeholder_style(theme::PLACEHOLDER);
        input.set_cursor_style(theme::CURSOR);
        Self {
            screen: Screen::Empty,
            input,
            focus: Focus::Input,
            quit: false,
            dirty: true,
        }
    }

    pub fn input(&self) -> &TextArea<'static> {
        &self.input
    }

    pub fn focus(&self) -> Focus {
        self.focus
    }

    pub fn screen(&self) -> Screen {
        self.screen
    }

    pub fn handle_key(&mut self, key: KeyEvent) {
        if key.kind == KeyEventKind::Release {
            return;
        }
        if key.modifiers.contains(KeyModifiers::CONTROL)
            && matches!(key.code, KeyCode::Char('c' | 'C'))
        {
            self.quit = true;
            return;
        }
        match key.code {
            KeyCode::Enter if key.modifiers.contains(KeyModifiers::ALT) => {
                self.input.insert_newline();
                self.dirty = true;
            }
            KeyCode::Enter => {
                if !self.input.is_empty() {
                    self.input.clear();
                    self.dirty = true;
                }
            }
            _ => {
                if self.input.input(key) {
                    self.dirty = true;
                }
            }
        }
    }

    pub fn handle_paste(&mut self, text: &str) {
        if self.input.insert_str(text) {
            self.dirty = true;
        }
    }
}

impl Default for App {
    fn default() -> Self {
        Self::new()
    }
}

pub fn run() -> io::Result<()> {
    let mut guard = TerminalGuard::new()?;
    let runtime = Builder::new_multi_thread().enable_all().build()?;
    let (terminal_tx, terminal_events) = mpsc::channel(EVENT_BUFFER);
    let _reader = TerminalReader::spawn(terminal_tx)?;
    runtime.block_on(event_loop(guard.terminal_mut(), terminal_events))
}

async fn event_loop(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    mut terminal_events: mpsc::Receiver<Event>,
) -> io::Result<()> {
    let mut app = App::new();
    let mut ticker = interval(TICK);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            event = terminal_events.recv() => match event {
                Some(Event::Key(key)) => app.handle_key(key),
                Some(Event::Paste(text)) => app.handle_paste(&text),
                Some(Event::Resize(_, _)) => app.dirty = true,
                Some(_) => {}
                None => break,
            },
            _ = ticker.tick() => {
                if app.dirty {
                    terminal.draw(|frame| view::render(frame, &app))?;
                    app.dirty = false;
                }
            }
        }
        if app.quit {
            break;
        }
    }
    Ok(())
}

struct TerminalGuard {
    terminal: Terminal<CrosstermBackend<Stdout>>,
}

impl TerminalGuard {
    fn new() -> io::Result<Self> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        if let Err(error) = execute!(
            stdout,
            EnterAlternateScreen,
            EnableMouseCapture,
            EnableBracketedPaste,
            Hide
        ) {
            restore_terminal();
            return Err(error);
        }
        match Terminal::new(CrosstermBackend::new(stdout)) {
            Ok(terminal) => Ok(Self { terminal }),
            Err(error) => {
                restore_terminal();
                Err(error)
            }
        }
    }

    fn terminal_mut(&mut self) -> &mut Terminal<CrosstermBackend<Stdout>> {
        &mut self.terminal
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        restore_terminal();
    }
}

fn restore_terminal() {
    let _ = disable_raw_mode();
    let mut stdout = io::stdout();
    let _ = execute!(
        stdout,
        DisableMouseCapture,
        DisableBracketedPaste,
        LeaveAlternateScreen,
        Show
    );
}

struct TerminalReader {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl TerminalReader {
    fn spawn(sender: mpsc::Sender<Event>) -> io::Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let handle = thread::Builder::new()
            .name("wind-tui-input".to_owned())
            .spawn(move || {
                while !flag.load(Ordering::Relaxed) {
                    match event::poll(POLL_INTERVAL) {
                        Ok(true) => match event::read() {
                            Ok(event) => {
                                if sender.blocking_send(event).is_err() {
                                    break;
                                }
                            }
                            Err(_) => break,
                        },
                        Ok(false) => {}
                        Err(_) => break,
                    }
                }
            })?;
        Ok(Self {
            stop,
            handle: Some(handle),
        })
    }
}

impl Drop for TerminalReader {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn press(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    #[test]
    fn ctrl_c_quits() {
        let mut app = App::new();
        app.handle_key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL));
        assert!(app.quit);
    }

    #[test]
    fn enter_clears_non_empty_input() {
        let mut app = App::new();
        app.handle_key(press(KeyCode::Char('h')));
        app.handle_key(press(KeyCode::Char('i')));
        assert_eq!(app.input.lines(), ["hi"]);
        app.handle_key(press(KeyCode::Enter));
        assert_eq!(app.input.lines(), [""]);
        assert!(!app.quit);
    }

    #[test]
    fn enter_on_empty_input_does_nothing() {
        let mut app = App::new();
        app.handle_key(press(KeyCode::Enter));
        assert_eq!(app.input.lines(), [""]);
    }

    #[test]
    fn alt_enter_inserts_newline() {
        let mut app = App::new();
        app.handle_key(press(KeyCode::Char('a')));
        app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::ALT));
        app.handle_key(press(KeyCode::Char('b')));
        assert_eq!(app.input.lines(), ["a", "b"]);
    }

    #[test]
    fn navigation_and_backspace_edit_input() {
        let mut app = App::new();
        app.handle_key(press(KeyCode::Char('a')));
        app.handle_key(press(KeyCode::Char('b')));
        app.handle_key(press(KeyCode::Left));
        app.handle_key(press(KeyCode::Backspace));
        assert_eq!(app.input.lines(), ["b"]);
    }

    #[test]
    fn edits_mark_app_dirty() {
        let mut app = App::new();
        app.dirty = false;
        app.handle_key(press(KeyCode::Char('x')));
        assert!(app.dirty);
    }

    #[test]
    fn paste_inserts_text_verbatim() {
        let mut app = App::new();
        app.handle_paste("first\nsecond");
        assert_eq!(app.input.lines(), ["first", "second"]);
    }
}
