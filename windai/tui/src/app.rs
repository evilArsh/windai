use std::io::{self, Stdout};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::config::Selection;
use crate::fetch;
use crate::theme;
use crate::view;
use crate::view::log::LogRing;
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
use wind_core::feed::{FeedError, Subscription};
use wind_runtime::{Runtime, RuntimeError, RuntimeOptions};
use wind_storage::Topic;
use wind_wire::event::FeedEvent;
use wind_wire::{AppDirs, Content, Usage};

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

#[derive(Debug, Clone, PartialEq)]
pub enum Screen {
    Empty,
    Session(Session),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Session {
    pub topic: Topic,
    pub main: Option<i64>,
    pub log: LogRing,
    pub usage: Usage,
}

impl Session {
    fn new(topic: Topic, main: Option<i64>) -> Self {
        Self {
            topic,
            main,
            log: LogRing::new(),
            usage: Usage {
                input_tokens: 0,
                output_tokens: 0,
            },
        }
    }
}

pub struct App {
    runtime: Runtime,
    subscription: Subscription,
    screen: Screen,
    input: TextArea<'static>,
    focus: Focus,
    selection: Selection,
    notice: Option<String>,
    quit: bool,
    dirty: bool,
}

impl App {
    async fn open() -> Result<Self, RuntimeError> {
        Self::connect(AppDirs::resolve()).await
    }

    async fn connect(dirs: AppDirs) -> Result<Self, RuntimeError> {
        let runtime = Runtime::open(RuntimeOptions::new(dirs)).await?;
        let opening = runtime.subscribe().await?;
        let startup = fetch::load_startup(runtime.storage()).await?;
        let mut input = TextArea::new(vec![String::new()]);
        input.set_placeholder_text("Input");
        input.set_placeholder_style(theme::PLACEHOLDER);
        input.set_cursor_style(theme::CURSOR);
        Ok(Self {
            runtime,
            subscription: opening.subscription,
            screen: Screen::Empty,
            input,
            focus: Focus::Input,
            selection: startup.selection,
            notice: startup.notice,
            quit: false,
            dirty: true,
        })
    }

    pub fn input(&self) -> &TextArea<'static> {
        &self.input
    }

    pub fn focus(&self) -> Focus {
        self.focus
    }

    pub fn screen(&self) -> &Screen {
        &self.screen
    }

    pub fn notice(&self) -> Option<&str> {
        self.notice.as_deref()
    }

    pub async fn handle_key(&mut self, key: KeyEvent) {
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
                self.notice = None;
                self.dirty = true;
            }
            KeyCode::Enter => self.submit().await,
            _ => {
                if self.input.input(key) {
                    self.notice = None;
                    self.dirty = true;
                }
            }
        }
    }

    pub fn handle_paste(&mut self, text: &str) {
        if self.input.insert_str(text) {
            self.notice = None;
            self.dirty = true;
        }
    }

    fn handle_feed(&mut self, result: Result<Option<Arc<FeedEvent>>, FeedError>) {
        match result {
            Ok(Some(event)) => {
                let Screen::Session(session) = &mut self.screen else {
                    return;
                };
                if session.log.record(session.topic.id, &event) {
                    self.dirty = true;
                }
            }
            Ok(None) => self.push_local("subscription closed"),
            Err(error) => self.push_local(&format!("subscription error: {error}")),
        }
    }

    fn push_local(&mut self, message: &str) {
        let Screen::Session(session) = &mut self.screen else {
            return;
        };
        session.log.push_local(message);
        self.dirty = true;
    }

    async fn submit(&mut self) {
        let text = self.input.lines().join("\n");
        let text = text.trim();
        if text.is_empty() {
            return;
        }
        if text.starts_with('/') {
            self.notice = Some("unknown command".to_owned());
            self.dirty = true;
            return;
        }
        let content = vec![Content::new_text(text.to_owned())];
        let result = match &self.screen {
            Screen::Empty => {
                let label = text.lines().next().unwrap_or_default();
                let model_id = self.selection.model.as_ref().map(|model| model.id);
                match self
                    .runtime
                    .storage()
                    .create_topic(None, label, model_id, None, None)
                    .await
                {
                    Ok(topic_id) => self.start_topic(topic_id, content).await,
                    Err(error) => Err(format!("create topic failed: {error}")),
                }
            }
            Screen::Session(session) => {
                let topic_id = session.topic.id;
                match self.runtime.submit(topic_id, content).await {
                    Ok(ack) => {
                        if let Screen::Session(session) = &mut self.screen {
                            session.main = Some(ack.instance_id);
                        }
                        Ok(())
                    }
                    Err(error) => Err(format!("submit failed: {error}")),
                }
            }
        };
        match result {
            Ok(()) => {
                self.input.clear();
                self.notice = None;
                self.dirty = true;
            }
            Err(message) => {
                self.notice = Some(message);
                self.dirty = true;
            }
        }
    }

    async fn start_topic(&mut self, topic_id: i64, content: Vec<Content>) -> Result<(), String> {
        let ack = self
            .runtime
            .submit(topic_id, content)
            .await
            .map_err(|error| format!("submit failed: {error}"))?;
        let topic = self
            .runtime
            .storage()
            .get_topic_by_id(topic_id)
            .await
            .map_err(|error| format!("load topic failed: {error}"))?
            .ok_or_else(|| format!("topic {topic_id} is missing"))?;
        self.screen = Screen::Session(Session::new(topic, Some(ack.instance_id)));
        Ok(())
    }
}

pub fn run() -> io::Result<()> {
    let engine = Builder::new_multi_thread().enable_all().build()?;
    let mut app = engine
        .block_on(App::open())
        .map_err(|error| io::Error::other(format!("cannot open the runtime: {error}")))?;
    let mut guard = TerminalGuard::new()?;
    let (terminal_tx, terminal_events) = mpsc::channel(EVENT_BUFFER);
    let _reader = TerminalReader::spawn(terminal_tx)?;
    engine.block_on(event_loop(guard.terminal_mut(), terminal_events, &mut app))
}

async fn event_loop(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    mut terminal_events: mpsc::Receiver<Event>,
    app: &mut App,
) -> io::Result<()> {
    let mut ticker = interval(TICK);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            event = terminal_events.recv() => match event {
                Some(Event::Key(key)) => app.handle_key(key).await,
                Some(Event::Paste(text)) => app.handle_paste(&text),
                Some(Event::Resize(_, _)) => app.dirty = true,
                Some(_) => {}
                None => break,
            },
            feed = app.subscription.recv() => app.handle_feed(feed),
            _ = ticker.tick() => {
                if app.dirty {
                    terminal.draw(|frame| view::render(frame, app))?;
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
    use tempfile::TempDir;

    async fn test_app() -> (TempDir, App) {
        let dir = TempDir::new().expect("create a temp dir");
        let app = App::connect(AppDirs::under(dir.path()))
            .await
            .expect("open the app");
        (dir, app)
    }

    async fn close(app: App) {
        app.runtime.shutdown().await.expect("shut down the runtime");
    }

    fn press(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    #[tokio::test]
    async fn ctrl_c_quits() {
        let (dir, mut app) = test_app().await;
        app.handle_key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL))
            .await;
        assert!(app.quit);
        close(app).await;
        drop(dir);
    }

    #[tokio::test]
    async fn enter_clears_non_empty_input() {
        let (dir, mut app) = test_app().await;
        app.handle_key(press(KeyCode::Char('h'))).await;
        app.handle_key(press(KeyCode::Char('i'))).await;
        assert_eq!(app.input.lines(), ["hi"]);
        app.handle_key(press(KeyCode::Enter)).await;
        assert_eq!(app.input.lines(), [""]);
        assert!(!app.quit);
        close(app).await;
        drop(dir);
    }

    #[tokio::test]
    async fn enter_on_empty_input_does_nothing() {
        let (dir, mut app) = test_app().await;
        app.handle_key(press(KeyCode::Enter)).await;
        assert_eq!(app.input.lines(), [""]);
        assert!(matches!(app.screen, Screen::Empty));
        close(app).await;
        drop(dir);
    }

    #[tokio::test]
    async fn alt_enter_inserts_newline() {
        let (dir, mut app) = test_app().await;
        app.handle_key(press(KeyCode::Char('a'))).await;
        app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::ALT))
            .await;
        app.handle_key(press(KeyCode::Char('b'))).await;
        assert_eq!(app.input.lines(), ["a", "b"]);
        close(app).await;
        drop(dir);
    }

    #[tokio::test]
    async fn navigation_and_backspace_edit_input() {
        let (dir, mut app) = test_app().await;
        app.handle_key(press(KeyCode::Char('a'))).await;
        app.handle_key(press(KeyCode::Char('b'))).await;
        app.handle_key(press(KeyCode::Left)).await;
        app.handle_key(press(KeyCode::Backspace)).await;
        assert_eq!(app.input.lines(), ["b"]);
        close(app).await;
        drop(dir);
    }

    #[tokio::test]
    async fn edits_mark_app_dirty() {
        let (dir, mut app) = test_app().await;
        app.dirty = false;
        app.handle_key(press(KeyCode::Char('x'))).await;
        assert!(app.dirty);
        close(app).await;
        drop(dir);
    }

    #[tokio::test]
    async fn paste_inserts_text_verbatim() {
        let (dir, mut app) = test_app().await;
        app.handle_paste("first\nsecond");
        assert_eq!(app.input.lines(), ["first", "second"]);
        close(app).await;
        drop(dir);
    }

    #[tokio::test]
    async fn submit_records_scope_relevant_events_in_the_log() {
        let (dir, mut app) = test_app().await;
        app.handle_paste("hello");
        app.handle_key(press(KeyCode::Enter)).await;
        assert!(matches!(app.screen, Screen::Session(_)));
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let result = app
                    .subscription
                    .recv()
                    .await
                    .expect("receive a feed result");
                let Some(event) = result else {
                    panic!("the subscription closed unexpectedly");
                };
                let accepted = event.envelope.name == "core/input.accepted";
                app.handle_feed(Ok(Some(event)));
                if accepted {
                    break;
                }
            }
        })
        .await
        .expect("receive the accepted event");
        let Screen::Session(session) = &app.screen else {
            panic!("submit creates a session");
        };
        assert!(
            session
                .log
                .entries()
                .any(|entry| entry.name == "core/input.accepted")
        );
        close(app).await;
        drop(dir);
    }
}
