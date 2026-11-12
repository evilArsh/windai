use crate::command::{self, Command, CommandSpec};
use crate::config::{self, Selection};
use crate::fetch::{self, LoadedSession};
use crate::form::{Form, ModelForm, ProviderForm};
use crate::reduce;
use crate::theme;
use crate::view;
use crate::view::bar::{self, BarData, BarKind, BarLayout};
use crate::view::log::LogRing;
use crossterm::cursor::{Hide, Show};
use crossterm::event::{
    self, DisableBracketedPaste, DisableMouseCapture, EnableBracketedPaste, EnableMouseCapture,
    Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::Rect;
use ratatui::text::Text;
use ratatui_textarea::TextArea;
use std::io::{self, Stdout};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::sync::mpsc;
use tokio::task::JoinHandle as TaskHandle;
use tokio::time::{MissedTickBehavior, interval};
use wind_core::agent::Phase;
use wind_core::feed::{FeedError, Subscription};
use wind_core::llm::domain::LlmDomain;
use wind_runtime::{Runtime, RuntimeError, RuntimeOptions};
use wind_storage::error::StorageError;
use wind_storage::{Model, Provider, Topic};
use wind_wire::event::{FeedEvent, Scope};
use wind_wire::{AgentRole, AppDirs, Body, Content, Role, Usage};

const TICK: Duration = Duration::from_millis(33);
const EVENT_BUFFER: usize = 128;
const POLL_INTERVAL: Duration = Duration::from_millis(50);
const LOAD_BUFFER_CAP: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Focus {
    Input,
    Content,
    Mods,
    Tree,
    Log,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq)]
pub enum Screen {
    Empty,
    Session(Session),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Session {
    pub topic: Topic,
    pub main: Option<i64>,
    pub messages: Vec<MessageView>,
    pub mods: Vec<ModView>,
    pub nodes: Vec<NodeView>,
    pub log: LogRing,
    pub usage: Usage,
    pub reload_mods: bool,
    pub follow: bool,
    pub content_scroll: u16,
    pub content_max: u16,
    pub mods_scroll: u16,
    pub mods_max: u16,
    pub tree_scroll: u16,
    pub tree_max: u16,
    pub log_scroll: u16,
    pub log_max: u16,
}

impl Session {
    pub fn new(topic: Topic, main: Option<i64>) -> Self {
        Self {
            topic,
            main,
            messages: Vec::new(),
            mods: Vec::new(),
            nodes: Vec::new(),
            log: LogRing::new(),
            usage: Usage {
                input_tokens: 0,
                output_tokens: 0,
            },
            reload_mods: false,
            follow: true,
            content_scroll: 0,
            content_max: 0,
            mods_scroll: 0,
            mods_max: 0,
            tree_scroll: 0,
            tree_max: 0,
            log_scroll: u16::MAX,
            log_max: 0,
        }
    }

    pub fn scroll_content_up(&mut self) {
        if self.content_max == 0 {
            self.follow = true;
            self.content_scroll = 0;
            return;
        }
        let current = if self.follow {
            self.content_max
        } else {
            self.content_scroll.min(self.content_max)
        };
        self.content_scroll = current.saturating_sub(1);
        self.follow = false;
    }

    pub fn scroll_content_down(&mut self) {
        let current = if self.follow {
            self.content_max
        } else {
            self.content_scroll.min(self.content_max)
        };
        self.content_scroll = current.saturating_add(1).min(self.content_max);
        self.follow = self.content_scroll >= self.content_max;
    }

    pub fn scroll_content_end(&mut self) {
        self.follow = true;
        self.content_scroll = self.content_max;
    }

    pub fn scroll_content_home(&mut self) {
        self.follow = self.content_max == 0;
        self.content_scroll = 0;
    }

    pub fn scroll_mods(&mut self, code: KeyCode) {
        pane_scroll(&mut self.mods_scroll, self.mods_max, code);
    }

    pub fn scroll_tree(&mut self, code: KeyCode) {
        pane_scroll(&mut self.tree_scroll, self.tree_max, code);
    }

    pub fn scroll_log(&mut self, code: KeyCode) {
        let max = self.log_max;
        let current = if self.log_scroll == u16::MAX {
            max
        } else {
            self.log_scroll.min(max)
        };
        self.log_scroll = match code {
            KeyCode::Up => current.saturating_sub(1),
            KeyCode::Down if current >= max => u16::MAX,
            KeyCode::Down => current.saturating_add(1),
            KeyCode::Home => 0,
            KeyCode::End => u16::MAX,
            _ => return,
        };
    }
}

fn pane_scroll(scroll: &mut u16, max: u16, code: KeyCode) {
    let current = (*scroll).min(max);
    *scroll = match code {
        KeyCode::Up => current.saturating_sub(1),
        KeyCode::Down => current.saturating_add(1).min(max),
        KeyCode::Home => 0,
        KeyCode::End => max,
        _ => return,
    };
}

#[derive(Debug, Clone, PartialEq)]
pub struct MessageView {
    pub id: i64,
    pub role: Role,
    pub blocks: Vec<BlockView>,
    pub sealed: Option<String>,
    pub interrupted: Option<String>,
    pub from_history: bool,
}

impl MessageView {
    pub fn new(id: i64, role: Role) -> Self {
        Self {
            id,
            role,
            blocks: Vec::new(),
            sealed: None,
            interrupted: None,
            from_history: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BlockView {
    pub seq: i64,
    pub revision: i64,
    pub body: Body,
    pub committed: bool,
    pub rendered: Option<(i64, Text<'static>)>,
    pub rendered_width: Option<u16>,
}

impl BlockView {
    pub fn new(seq: i64, revision: i64, body: Body) -> Self {
        Self {
            seq,
            revision,
            body,
            committed: false,
            rendered: None,
            rendered_width: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ModView {
    pub name: String,
    pub version: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NodeView {
    pub id: i64,
    pub parent_id: Option<i64>,
    pub role: AgentRole,
    pub phase: String,
    pub suspended_wait: Option<i64>,
    pub reason: Option<String>,
    pub sealed: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResumeItem {
    pub topic: Topic,
    pub phase: String,
    pub created_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Overlay {
    Resume {
        items: Vec<ResumeItem>,
        selected: usize,
    },
    Providers {
        selected: usize,
    },
    Models {
        selected: usize,
    },
    Config {
        selected: usize,
    },
    Help,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Completion {
    pub items: Vec<&'static CommandSpec>,
    pub selected: usize,
}

pub(crate) const CONFIG_PROVIDER: usize = 0;
pub(crate) const CONFIG_MODEL: usize = 1;
pub(crate) const CONFIG_REASONING: usize = 2;
pub(crate) const CONFIG_SIDE_PANEL: usize = 3;
pub(crate) const CONFIG_NEW_PROVIDER: usize = 4;
pub(crate) const CONFIG_NEW_MODEL: usize = 5;
pub(crate) const CONFIG_ROWS: usize = 6;
pub(crate) const COMPLETION_ROWS: usize = 6;

fn move_selection(selected: &mut usize, len: usize, step: isize) {
    if len == 0 {
        *selected = 0;
        return;
    }
    let current = (*selected).min(len - 1) as isize;
    *selected = (current + step).rem_euclid(len as isize) as usize;
}

fn rect_contains(rect: Rect, x: u16, y: u16) -> bool {
    x >= rect.x && x < rect.right() && y >= rect.y && y < rect.bottom()
}

fn completion_prefix(text: &str) -> Option<&str> {
    let rest = text.strip_prefix('/')?;
    if rest.chars().any(char::is_whitespace) {
        return None;
    }
    Some(rest)
}

pub(crate) fn phase_detail(phase: &Phase<LlmDomain>) -> (Option<i64>, Option<String>) {
    match phase {
        Phase::Suspended(suspended) => (Some(suspended.external_wait_id), None),
        Phase::Failed(error) => (None, error.error.lines().next().map(str::to_owned)),
        Phase::Cancelled(_) => (None, Some("cancelled".to_owned())),
        _ => (None, None),
    }
}

pub(crate) fn phase_is_terminal(phase: &str) -> bool {
    matches!(phase, "done" | "failed" | "cancelled" | "aborted")
}

struct LoadState {
    topic_id: i64,
    task: TaskHandle<Result<LoadedSession, StorageError>>,
    buffer: Vec<FeedEvent>,
}

pub struct App {
    runtime: Runtime,
    subscription: Option<Subscription>,
    screen: Screen,
    input: TextArea<'static>,
    focus: Focus,
    overlay: Option<Overlay>,
    completion: Option<Completion>,
    selection: Selection,
    providers: Vec<Provider>,
    models: Vec<Model>,
    form: Option<Form>,
    show_side: bool,
    notice: Option<String>,
    storage_error: Option<String>,
    bar: BarLayout,
    bar_area: Rect,
    branch: Option<String>,
    dirs: AppDirs,
    quit: bool,
    dirty: bool,
    loading: Option<LoadState>,
}

impl App {
    async fn open() -> Result<Self, RuntimeError> {
        Self::connect(AppDirs::resolve()).await
    }

    async fn connect(dirs: AppDirs) -> Result<Self, RuntimeError> {
        let branch = bar::read_branch();
        let runtime = Runtime::open(RuntimeOptions::new(dirs.clone())).await?;
        let opening = runtime.subscribe().await?;
        let startup = fetch::load_startup(runtime.storage()).await?;
        let mut input = TextArea::new(vec![String::new()]);
        input.set_placeholder_text("Input");
        input.set_placeholder_style(theme::PLACEHOLDER);
        input.set_cursor_style(theme::CURSOR);
        Ok(Self {
            runtime,
            subscription: Some(opening.subscription),
            screen: Screen::Empty,
            input,
            focus: Focus::Input,
            overlay: None,
            completion: None,
            selection: startup.selection,
            providers: startup.providers,
            models: startup.models,
            form: None,
            show_side: true,
            notice: startup.notice,
            storage_error: None,
            bar: BarLayout::default(),
            bar_area: Rect::new(0, 0, 0, 0),
            branch,
            dirs,
            quit: false,
            dirty: true,
            loading: None,
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

    pub(crate) fn screen_mut(&mut self) -> &mut Screen {
        &mut self.screen
    }

    pub fn overlay(&self) -> Option<&Overlay> {
        self.overlay.as_ref()
    }

    pub fn completion(&self) -> Option<&Completion> {
        self.completion.as_ref()
    }

    pub fn selection(&self) -> &Selection {
        &self.selection
    }

    pub fn providers(&self) -> &[Provider] {
        &self.providers
    }

    pub fn models(&self) -> &[Model] {
        &self.models
    }

    pub fn form(&self) -> Option<&Form> {
        self.form.as_ref()
    }

    pub fn show_side(&self) -> bool {
        self.show_side
    }

    pub fn notice(&self) -> Option<&str> {
        self.notice.as_deref()
    }

    pub fn storage_error(&self) -> Option<&str> {
        self.storage_error.as_deref()
    }

    pub(crate) fn bar_layout(&self) -> &BarLayout {
        &self.bar
    }

    pub(crate) fn set_bar(&mut self, bar: BarLayout) {
        self.bar = bar;
    }

    pub(crate) fn set_bar_area(&mut self, area: Rect) {
        self.bar_area = area;
    }

    pub(crate) fn bar_data(&self) -> BarData {
        let (usage, directory, phase, phase_reason) = match &self.screen {
            Screen::Session(session) => {
                let directory = session
                    .topic
                    .workspace
                    .clone()
                    .filter(|workspace| !workspace.is_empty())
                    .unwrap_or_else(|| self.dirs.root().display().to_string());
                let node = session
                    .main
                    .and_then(|id| session.nodes.iter().find(|node| node.id == id));
                (
                    session.usage,
                    Some(directory),
                    node.map(|node| node.phase.clone()),
                    node.and_then(|node| node.reason.clone()),
                )
            }
            Screen::Empty => (
                Usage {
                    input_tokens: 0,
                    output_tokens: 0,
                },
                Some(self.dirs.root().display().to_string()),
                None,
                None,
            ),
        };
        BarData {
            provider: self
                .selection
                .provider
                .as_ref()
                .map(|provider| provider.name.clone()),
            model: self
                .selection
                .model
                .as_ref()
                .map(|model| model.name.clone()),
            thinking: self.selection.reasoning_effort.clone(),
            directory,
            branch: self.branch.clone(),
            usage,
            phase,
            phase_reason,
        }
    }

    pub async fn handle_mouse(&mut self, mouse: MouseEvent) {
        if mouse.kind != MouseEventKind::Down(MouseButton::Left)
            || self.form.is_some()
            || self.overlay.is_some()
        {
            return;
        }
        let area = self.bar_area;
        if area.is_empty() || !rect_contains(area, mouse.column, mouse.row) {
            return;
        }
        let x = mouse.column.saturating_sub(area.x);
        let y = mouse.row.saturating_sub(area.y);
        let kind = self
            .bar
            .segments
            .iter()
            .find(|segment| rect_contains(segment.rect, x, y))
            .and_then(|segment| segment.kind);
        match kind {
            Some(BarKind::Provider) => self.open_providers_overlay().await,
            Some(BarKind::Model) => self.open_models_overlay().await,
            None => {}
        }
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
        if self.form.is_some() {
            self.handle_form_key(key).await;
            return;
        }
        if self.overlay.is_some() {
            self.handle_overlay_key(key).await;
            return;
        }
        if key.code == KeyCode::Enter && key.modifiers.contains(KeyModifiers::ALT) {
            self.input.insert_newline();
            self.notice = None;
            self.refresh_completion();
            self.dirty = true;
            return;
        }
        if self.completion.is_some() {
            match key.code {
                KeyCode::Tab if !key.modifiers.contains(KeyModifiers::SHIFT) => {
                    self.apply_completion();
                    return;
                }
                KeyCode::Up => {
                    self.move_completion(-1);
                    return;
                }
                KeyCode::Down => {
                    self.move_completion(1);
                    return;
                }
                KeyCode::Esc => {
                    self.completion = None;
                    self.dirty = true;
                    return;
                }
                KeyCode::Enter => {
                    self.submit().await;
                    return;
                }
                _ => {}
            }
        }
        if key.code == KeyCode::Tab || key.code == KeyCode::BackTab {
            let backward =
                key.code == KeyCode::BackTab || key.modifiers.contains(KeyModifiers::SHIFT);
            self.cycle_focus(if backward { -1 } else { 1 });
            return;
        }
        if self.focus != Focus::Input {
            self.handle_focused_key(key);
            return;
        }
        match key.code {
            KeyCode::Enter => self.submit().await,
            _ => {
                if self.input.input(key) {
                    self.notice = None;
                    self.refresh_completion();
                    self.dirty = true;
                }
            }
        }
    }

    fn cycle_focus(&mut self, step: isize) {
        const ORDER: [Focus; 5] = [
            Focus::Input,
            Focus::Content,
            Focus::Mods,
            Focus::Tree,
            Focus::Log,
        ];
        let index = ORDER
            .iter()
            .position(|focus| *focus == self.focus)
            .unwrap_or(0) as isize;
        let next = (index + step).rem_euclid(ORDER.len() as isize) as usize;
        self.focus = ORDER[next];
        self.completion = None;
        self.dirty = true;
    }

    fn handle_focused_key(&mut self, key: KeyEvent) {
        if key.code == KeyCode::Esc {
            self.focus = Focus::Input;
            self.dirty = true;
            return;
        }
        let focus = self.focus;
        let Screen::Session(session) = &mut self.screen else {
            return;
        };
        match focus {
            Focus::Content => match key.code {
                KeyCode::Up => session.scroll_content_up(),
                KeyCode::Down => session.scroll_content_down(),
                KeyCode::Home => session.scroll_content_home(),
                KeyCode::End => session.scroll_content_end(),
                _ => return,
            },
            Focus::Mods => session.scroll_mods(key.code),
            Focus::Tree => session.scroll_tree(key.code),
            Focus::Log => session.scroll_log(key.code),
            Focus::Input => return,
        }
        self.dirty = true;
    }

    fn refresh_completion(&mut self) {
        let text = self.input.lines().join("\n");
        let items = completion_prefix(&text).map(command::candidates);
        match items {
            Some(items) if !items.is_empty() => {
                let selected = self
                    .completion
                    .as_ref()
                    .map(|completion| completion.selected.min(items.len() - 1))
                    .unwrap_or(0);
                self.completion = Some(Completion { items, selected });
            }
            _ => self.completion = None,
        }
    }

    fn apply_completion(&mut self) {
        let Some(completion) = self.completion.take() else {
            return;
        };
        let Some(spec) = completion.items.get(completion.selected).copied() else {
            return;
        };
        self.input.clear();
        self.input.insert_str(spec.name);
        self.notice = None;
        self.dirty = true;
    }

    fn move_completion(&mut self, step: isize) {
        if let Some(completion) = self.completion.as_mut() {
            move_selection(&mut completion.selected, completion.items.len(), step);
            self.dirty = true;
        }
    }

    pub fn handle_paste(&mut self, text: &str) {
        if self.input.insert_str(text) {
            self.notice = None;
            self.refresh_completion();
            self.dirty = true;
        }
    }

    async fn handle_feed(&mut self, result: Result<Option<Arc<FeedEvent>>, FeedError>) {
        match result {
            Ok(Some(event)) => self.accept_event(event).await,
            Ok(None) => {
                self.subscription = None;
                self.push_local("subscription closed; resubscribing");
                self.resubscribe().await;
            }
            Err(error) => self.push_local(&format!("subscription error: {error}")),
        }
    }

    async fn resubscribe(&mut self) {
        match self.runtime.subscribe().await {
            Ok(opening) => {
                self.subscription = Some(opening.subscription);
                self.dirty = true;
                if let Screen::Session(session) = &self.screen {
                    let topic_id = session.topic.id;
                    self.begin_load(topic_id);
                }
            }
            Err(error) => {
                self.notice = Some(format!("resubscribe failed: {error}"));
                self.dirty = true;
            }
        }
    }

    async fn accept_event(&mut self, event: Arc<FeedEvent>) {
        if self.loading.is_some() {
            self.buffer_event(event);
            return;
        }
        let Screen::Session(session) = &mut self.screen else {
            return;
        };
        let logged = match &event.envelope.scope {
            Scope::Global => true,
            Scope::Topic { topic_id, .. } => *topic_id == session.topic.id,
        };
        if !logged {
            return;
        }
        reduce::apply(session, &event);
        self.dirty = true;
        self.reload_mods_if_needed().await;
    }

    fn buffer_event(&mut self, event: Arc<FeedEvent>) {
        let Some(topic_id) = self.loading.as_ref().map(|loading| loading.topic_id) else {
            return;
        };
        match &event.envelope.scope {
            Scope::Global => {
                if let Screen::Session(session) = &mut self.screen {
                    reduce::apply(session, &event);
                    self.dirty = true;
                }
            }
            Scope::Topic {
                topic_id: event_topic,
                ..
            } if *event_topic == topic_id => {
                let overflow = match &mut self.loading {
                    Some(loading) if loading.buffer.len() < LOAD_BUFFER_CAP => {
                        loading.buffer.push((*event).clone());
                        false
                    }
                    Some(_) => true,
                    None => false,
                };
                if overflow {
                    if let Screen::Session(session) = &mut self.screen {
                        session
                            .log
                            .push_local("event buffer overflow; reloading the session");
                    }
                    self.dirty = true;
                    self.begin_load(topic_id);
                    if let Some(loading) = &mut self.loading {
                        loading.buffer.push((*event).clone());
                    }
                }
            }
            _ => {}
        }
    }

    fn begin_load(&mut self, topic_id: i64) {
        if let Some(previous) = self.loading.take() {
            previous.task.abort();
        }
        let storage = self.runtime.storage().clone();
        let task = tokio::spawn(async move { fetch::load_session(&storage, topic_id).await });
        self.loading = Some(LoadState {
            topic_id,
            task,
            buffer: Vec::new(),
        });
    }

    async fn finish_load(&mut self, result: Result<LoadedSession, StorageError>) {
        let Some(loading) = self.loading.take() else {
            return;
        };
        let Screen::Session(session) = &mut self.screen else {
            return;
        };
        if session.topic.id != loading.topic_id {
            return;
        }
        match result {
            Ok(loaded) => {
                session.topic = loaded.topic;
                session.main = loaded.main.or(session.main);
                session.messages = loaded.messages;
                session.usage = loaded.usage;
                session.nodes = loaded.nodes;
                session.mods = loaded.mods;
                for event in &loading.buffer {
                    reduce::apply(session, event);
                }
            }
            Err(error) => {
                session
                    .log
                    .push_local(&format!("load session failed: {error}"));
                self.storage_error = Some(format!("load session failed: {error}"));
            }
        }
        self.dirty = true;
        self.reload_mods_if_needed().await;
    }

    async fn reload_mods_if_needed(&mut self) {
        let (topic_id, needed) = match &mut self.screen {
            Screen::Session(session) if session.reload_mods => {
                session.reload_mods = false;
                (session.topic.id, true)
            }
            _ => (0, false),
        };
        if !needed {
            return;
        }
        let result = fetch::load_mods(self.runtime.storage(), topic_id).await;
        let Screen::Session(session) = &mut self.screen else {
            return;
        };
        match result {
            Ok(mods) => session.mods = mods,
            Err(error) => {
                session
                    .log
                    .push_local(&format!("load mods failed: {error}"));
                self.storage_error = Some(format!("load mods failed: {error}"));
            }
        }
        self.dirty = true;
    }

    fn retry_storage(&mut self) {
        if self.storage_error.take().is_none() {
            return;
        }
        self.dirty = true;
        let Screen::Session(session) = &mut self.screen else {
            return;
        };
        session.reload_mods = true;
        let topic_id = session.topic.id;
        if self.loading.is_none() {
            self.begin_load(topic_id);
        }
    }

    fn push_local(&mut self, message: &str) {
        let Screen::Session(session) = &mut self.screen else {
            return;
        };
        session.log.push_local(message);
        self.dirty = true;
    }

    fn storage_failed(&mut self, message: String) {
        self.storage_error = Some(message);
        self.dirty = true;
    }

    async fn submit(&mut self) {
        let text = self.input.lines().join("\n");
        let text = text.trim();
        if text.is_empty() {
            return;
        }
        if text.starts_with('/') {
            match command::parse(text) {
                Ok(command) => {
                    self.input.clear();
                    self.completion = None;
                    self.notice = None;
                    self.execute(command).await;
                    self.dirty = true;
                }
                Err(message) => {
                    self.notice = Some(message);
                    self.dirty = true;
                }
            }
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
                self.completion = None;
                self.notice = None;
                self.dirty = true;
            }
            Err(message) => {
                self.notice = Some(message);
                self.dirty = true;
            }
        }
    }

    async fn execute(&mut self, command: Command) {
        match command {
            Command::New => self.new_view(),
            Command::Quit => self.quit = true,
            Command::Help => self.overlay = Some(Overlay::Help),
            Command::Cancel => self.cancel_current().await,
            Command::Resume => self.open_resume().await,
            Command::Providers => self.open_providers_overlay().await,
            Command::Models => self.open_models_overlay().await,
            Command::Config => {
                self.overlay = Some(Overlay::Config { selected: 0 });
            }
        }
    }

    fn new_view(&mut self) {
        if let Some(loading) = self.loading.take() {
            loading.task.abort();
        }
        self.screen = Screen::Empty;
        self.input.clear();
        self.overlay = None;
        self.completion = None;
        self.form = None;
        self.notice = None;
        self.focus = Focus::Input;
        self.dirty = true;
    }

    async fn cancel_current(&mut self) {
        let main = match &self.screen {
            Screen::Session(session) => session.main,
            Screen::Empty => None,
        };
        let Some(main) = main else {
            self.notice = Some("enter a topic first".to_owned());
            return;
        };
        if let Err(error) = self.runtime.cancel(main).await {
            self.notice = Some(format!("cancel failed: {error}"));
        }
    }

    async fn open_resume(&mut self) {
        if matches!(&self.screen, Screen::Empty) {
            self.notice = Some("enter a topic first".to_owned());
            return;
        }
        match self.resume_items().await {
            Ok(items) => self.overlay = Some(Overlay::Resume { items, selected: 0 }),
            Err(error) => self.storage_failed(format!("load topics failed: {error}")),
        }
    }

    async fn resume_items(&self) -> Result<Vec<ResumeItem>, StorageError> {
        let storage = self.runtime.storage();
        let topics = storage.list_topics().await?;
        let mut items = Vec::new();
        for topic in topics {
            let instances = storage.list_instances_by_topic(topic.id).await?;
            if instances
                .iter()
                .all(|instance| phase_is_terminal(&instance.phase))
            {
                continue;
            }
            let main = instances
                .iter()
                .find(|instance| instance.role == AgentRole::Main);
            let phase = main
                .map(|instance| instance.phase.clone())
                .or_else(|| {
                    instances
                        .iter()
                        .find(|instance| !phase_is_terminal(&instance.phase))
                        .map(|instance| instance.phase.clone())
                })
                .unwrap_or_default();
            let created_at = main.and_then(|instance| instance.created_at);
            items.push(ResumeItem {
                topic,
                phase,
                created_at,
            });
        }
        Ok(items)
    }

    fn open_topic(&mut self, topic: Topic) {
        let topic_id = topic.id;
        self.screen = Screen::Session(Session::new(topic, None));
        self.begin_load(topic_id);
        self.input.clear();
        self.completion = None;
        self.notice = None;
        self.focus = Focus::Input;
        self.dirty = true;
    }

    async fn open_providers_overlay(&mut self) {
        match self.runtime.storage().list_providers().await {
            Ok(providers) => {
                self.providers = providers;
                let selected = self.current_provider_index();
                self.overlay = Some(Overlay::Providers { selected });
                self.notice = None;
            }
            Err(error) => self.storage_failed(format!("load providers failed: {error}")),
        }
        self.dirty = true;
    }

    fn current_provider_index(&self) -> usize {
        let provider_id = self.selection.provider.as_ref().map(|provider| provider.id);
        provider_id
            .and_then(|id| self.providers.iter().position(|provider| provider.id == id))
            .unwrap_or(0)
    }

    async fn open_models_overlay(&mut self) {
        let Some(provider_id) = self.selection.provider.as_ref().map(|provider| provider.id) else {
            self.notice = Some("this provider has no models".to_owned());
            self.dirty = true;
            return;
        };
        match self.runtime.storage().list_models(Some(provider_id)).await {
            Ok(models) => {
                self.models = models;
                if self.models.is_empty() {
                    self.notice = Some("this provider has no models".to_owned());
                } else {
                    let selected = self.current_model_index();
                    self.overlay = Some(Overlay::Models { selected });
                    self.notice = None;
                }
            }
            Err(error) => self.storage_failed(format!("load models failed: {error}")),
        }
        self.dirty = true;
    }

    fn current_model_index(&self) -> usize {
        let model_id = self.selection.model.as_ref().map(|model| model.id);
        model_id
            .and_then(|id| self.models.iter().position(|model| model.id == id))
            .unwrap_or(0)
    }

    async fn handle_overlay_key(&mut self, key: KeyEvent) {
        if key.code == KeyCode::Esc {
            self.close_overlay();
            return;
        }
        match self.overlay.as_ref() {
            Some(Overlay::Resume { .. }) => self.handle_resume_key(key).await,
            Some(Overlay::Providers { .. }) => self.handle_providers_key(key).await,
            Some(Overlay::Models { .. }) => self.handle_models_key(key),
            Some(Overlay::Config { .. }) => self.handle_config_key(key).await,
            Some(Overlay::Help) if key.code == KeyCode::Enter => self.close_overlay(),
            Some(Overlay::Help) => {}
            None => {}
        }
    }

    fn close_overlay(&mut self) {
        self.overlay = None;
        self.focus = Focus::Input;
        self.dirty = true;
    }

    async fn handle_resume_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Up | KeyCode::Down => {
                let step = if key.code == KeyCode::Up { -1 } else { 1 };
                if let Some(Overlay::Resume { items, selected }) = self.overlay.as_mut() {
                    move_selection(selected, items.len(), step);
                    self.dirty = true;
                }
            }
            KeyCode::Enter => {
                let topic = match self.overlay.as_ref() {
                    Some(Overlay::Resume { items, selected }) => {
                        items.get(*selected).map(|item| item.topic.clone())
                    }
                    _ => None,
                };
                if let Some(topic) = topic {
                    self.close_overlay();
                    self.open_topic(topic);
                }
            }
            _ => {}
        }
    }

    async fn handle_providers_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Up | KeyCode::Down => {
                let step = if key.code == KeyCode::Up { -1 } else { 1 };
                if let Some(Overlay::Providers { selected }) = self.overlay.as_mut() {
                    move_selection(selected, self.providers.len(), step);
                    self.dirty = true;
                }
            }
            KeyCode::Enter => {
                let provider_id = match self.overlay.as_ref() {
                    Some(Overlay::Providers { selected }) => {
                        self.providers.get(*selected).map(|provider| provider.id)
                    }
                    _ => None,
                };
                if let Some(provider_id) = provider_id {
                    self.select_provider_id(provider_id).await;
                }
            }
            _ => {}
        }
    }

    async fn select_provider_id(&mut self, provider_id: i64) {
        let models = match self.runtime.storage().list_models(Some(provider_id)).await {
            Ok(models) => models,
            Err(error) => {
                self.storage_failed(format!("load models failed: {error}"));
                return;
            }
        };
        self.models = models;
        self.notice = self
            .selection
            .select_provider(&self.providers, &self.models, provider_id);
        self.overlay = None;
        self.focus = Focus::Input;
        self.dirty = true;
    }

    fn handle_models_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Up | KeyCode::Down => {
                let step = if key.code == KeyCode::Up { -1 } else { 1 };
                if let Some(Overlay::Models { selected }) = self.overlay.as_mut() {
                    move_selection(selected, self.models.len(), step);
                    self.dirty = true;
                }
            }
            KeyCode::Enter => {
                let model_id = match self.overlay.as_ref() {
                    Some(Overlay::Models { selected }) => {
                        self.models.get(*selected).map(|model| model.id)
                    }
                    _ => None,
                };
                if let Some(model_id) = model_id {
                    self.notice = self.selection.select_model(&self.models, model_id);
                    self.overlay = None;
                    self.focus = Focus::Input;
                    self.dirty = true;
                }
            }
            _ => {}
        }
    }

    async fn handle_config_key(&mut self, key: KeyEvent) {
        let selected = match self.overlay.as_ref() {
            Some(Overlay::Config { selected }) => *selected,
            _ => return,
        };
        match key.code {
            KeyCode::Up => {
                if let Some(Overlay::Config { selected }) = self.overlay.as_mut() {
                    move_selection(selected, CONFIG_ROWS, -1);
                }
                self.dirty = true;
            }
            KeyCode::Down => {
                if let Some(Overlay::Config { selected }) = self.overlay.as_mut() {
                    move_selection(selected, CONFIG_ROWS, 1);
                }
                self.dirty = true;
            }
            KeyCode::Left if selected == CONFIG_REASONING => {
                self.selection.cycle_reasoning_back();
                self.dirty = true;
            }
            KeyCode::Right if selected == CONFIG_REASONING => {
                self.selection.cycle_reasoning();
                self.dirty = true;
            }
            KeyCode::Enter => match selected {
                CONFIG_PROVIDER => self.open_providers_overlay().await,
                CONFIG_MODEL => self.open_models_overlay().await,
                CONFIG_SIDE_PANEL => {
                    self.show_side = !self.show_side;
                    self.dirty = true;
                }
                CONFIG_NEW_PROVIDER => self.open_provider_form(),
                CONFIG_NEW_MODEL => self.open_model_form(),
                _ => {}
            },
            _ => {}
        }
    }

    fn open_provider_form(&mut self) {
        self.form = Some(Form::Provider(ProviderForm::default()));
        self.dirty = true;
    }

    fn open_model_form(&mut self) {
        let provider_index = self.current_provider_index();
        self.form = Some(Form::Model(ModelForm {
            provider_index,
            ..ModelForm::default()
        }));
        self.dirty = true;
    }

    async fn handle_form_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => {
                self.form = None;
                self.focus = Focus::Input;
                self.dirty = true;
            }
            KeyCode::Enter => self.save_form().await,
            KeyCode::Tab | KeyCode::Down => {
                if let Some(form) = self.form.as_mut() {
                    form.next_field();
                }
                self.dirty = true;
            }
            KeyCode::Up => {
                if let Some(form) = self.form.as_mut() {
                    form.previous_field();
                }
                self.dirty = true;
            }
            KeyCode::Left => {
                if let Some(form) = self.form.as_mut() {
                    form.move_left(&self.providers);
                }
                self.dirty = true;
            }
            KeyCode::Right => {
                if let Some(form) = self.form.as_mut() {
                    form.move_right(&self.providers);
                }
                self.dirty = true;
            }
            KeyCode::Home => {
                if let Some(form) = self.form.as_mut() {
                    form.home();
                }
                self.dirty = true;
            }
            KeyCode::End => {
                if let Some(form) = self.form.as_mut() {
                    form.end();
                }
                self.dirty = true;
            }
            KeyCode::Backspace => {
                if let Some(form) = self.form.as_mut() {
                    form.backspace();
                }
                self.dirty = true;
            }
            KeyCode::Delete => {
                if let Some(form) = self.form.as_mut() {
                    form.delete();
                }
                self.dirty = true;
            }
            KeyCode::Char(ch) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
                if let Some(form) = self.form.as_mut() {
                    form.insert_char(ch);
                }
                self.dirty = true;
            }
            _ => {}
        }
    }

    async fn save_form(&mut self) {
        let Some(form) = self.form.clone() else {
            return;
        };
        match form {
            Form::Provider(provider_form) => match provider_form.validate(&self.providers) {
                Err((field, message)) => self.set_form_error(field, message),
                Ok(fields) => match config::save_provider(self.runtime.storage(), fields).await {
                    Ok(provider_id) => self.finish_provider_save(provider_id).await,
                    Err(error) => self.set_form_error(0, error.to_string()),
                },
            },
            Form::Model(model_form) => {
                let Some(provider) = self.providers.get(model_form.provider_index).cloned() else {
                    self.set_form_error(0, "provider is required".to_owned());
                    return;
                };
                let models = match self.runtime.storage().list_models(Some(provider.id)).await {
                    Ok(models) => models,
                    Err(error) => {
                        self.set_form_error(0, error.to_string());
                        return;
                    }
                };
                match model_form.validate(&self.providers, &models) {
                    Err((field, message)) => self.set_form_error(field, message),
                    Ok(fields) => {
                        let provider_id = fields.provider_id;
                        match config::save_model(self.runtime.storage(), fields).await {
                            Ok(model_id) => self.finish_model_save(provider_id, model_id).await,
                            Err(error) => self.set_form_error(0, error.to_string()),
                        }
                    }
                }
            }
        }
    }

    async fn finish_provider_save(&mut self, provider_id: i64) {
        if let Err(error) = self.reload_lists(Some(provider_id)).await {
            self.notice = Some(format!("refresh failed: {error}"));
            self.form = None;
            self.dirty = true;
            return;
        }
        self.form = None;
        let notice = self
            .selection
            .select_provider(&self.providers, &self.models, provider_id);
        self.notice = match notice.as_deref() {
            Some("this provider has no models") => {
                Some("create a model for this provider".to_owned())
            }
            _ => notice,
        };
        self.dirty = true;
    }

    async fn finish_model_save(&mut self, provider_id: i64, model_id: i64) {
        if let Err(error) = self.reload_lists(Some(provider_id)).await {
            self.notice = Some(format!("refresh failed: {error}"));
            self.form = None;
            self.dirty = true;
            return;
        }
        self.form = None;
        let _ = self
            .selection
            .select_provider(&self.providers, &self.models, provider_id);
        self.notice = self.selection.select_model(&self.models, model_id);
        self.dirty = true;
    }

    async fn reload_lists(&mut self, provider_id: Option<i64>) -> Result<(), StorageError> {
        let storage = self.runtime.storage();
        let providers = storage.list_providers().await?;
        let models = match provider_id {
            Some(id) => storage.list_models(Some(id)).await?,
            None => Vec::new(),
        };
        self.providers = providers;
        self.models = models;
        Ok(())
    }

    fn set_form_error(&mut self, field: usize, message: String) {
        if let Some(form) = self.form.as_mut() {
            form.set_error(field, message);
        }
        self.dirty = true;
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
        self.begin_load(topic_id);
        self.dirty = true;
        Ok(())
    }
}

async fn load_result(loading: &mut Option<LoadState>) -> Result<LoadedSession, StorageError> {
    let Some(state) = loading.as_mut() else {
        return std::future::pending().await;
    };
    match (&mut state.task).await {
        Ok(result) => result,
        Err(error) => Err(StorageError::Internal(format!(
            "session load task failed: {error}"
        ))),
    }
}

pub fn run() -> io::Result<()> {
    let engine = Builder::new_multi_thread().enable_all().build()?;
    let mut app = engine
        .block_on(App::open())
        .map_err(|error| io::Error::other(format!("cannot open the runtime: {error}")))?;
    let mut guard = TerminalGuard::new()?;
    let (terminal_tx, terminal_events) = mpsc::channel(EVENT_BUFFER);
    let reader = TerminalReader::spawn(terminal_tx)?;
    let result = engine.block_on(event_loop(guard.terminal_mut(), terminal_events, &mut app));
    drop(reader);
    let shutdown = engine.block_on(app.runtime.shutdown());
    drop(guard);
    if let Err(error) = shutdown {
        eprintln!("wind-tui: runtime shutdown failed: {error}");
    }
    result
}

async fn receive(
    subscription: &mut Option<Subscription>,
) -> Result<Option<Arc<FeedEvent>>, FeedError> {
    match subscription {
        Some(subscription) => subscription.recv().await,
        None => std::future::pending().await,
    }
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
                Some(Event::Mouse(mouse)) => app.handle_mouse(mouse).await,
                Some(Event::Paste(text)) => app.handle_paste(&text),
                Some(Event::Resize(_, _)) => app.dirty = true,
                Some(_) => {}
                None => break,
            },
            feed = receive(&mut app.subscription) => app.handle_feed(feed).await,
            loaded = load_result(&mut app.loading) => app.finish_load(loaded).await,
            _ = ticker.tick() => {
                app.retry_storage();
                if app.subscription.is_none() {
                    app.resubscribe().await;
                }
                if app.dirty {
                    if let Err(error) = terminal.draw(|frame| view::render(frame, app)) {
                        app.quit = true;
                        return Err(error);
                    }
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
    use wind_wire::AdapterType;
    use wind_wire::event::{Envelope, EventKey, Origin, Shape, Source, Target};

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

    fn topic(id: i64) -> Topic {
        Topic {
            id,
            parent_id: None,
            label: "topic".to_owned(),
            model_id: None,
            icon: None,
            workspace: None,
        }
    }

    fn event(seq: i64, topic_id: i64) -> FeedEvent {
        FeedEvent {
            key: EventKey::new(0, seq).expect("build an event key"),
            envelope: Envelope::new(
                "core/agent.sealed",
                Scope::Topic {
                    topic_id,
                    instance_id: 1,
                },
                Origin::Internal,
                Source::core("test"),
                Some(serde_json::json!({"instance_id": 1})),
                Shape::Observe,
            ),
            target: Target::Core,
        }
    }

    async fn settle_load(app: &mut App) {
        if app.loading.is_some() {
            let result = load_result(&mut app.loading).await;
            app.finish_load(result).await;
        }
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
    async fn clicking_status_segments_opens_overlays_without_changing_focus() {
        let (dir, mut app) = test_app().await;
        let storage = app.runtime.storage();
        let provider_id = storage
            .create_provider("alpha", "https://alpha.example")
            .await
            .expect("create the provider");
        storage
            .create_model(
                provider_id,
                "alpha-model",
                AdapterType::OpenAICompletion,
                None,
                None,
            )
            .await
            .expect("create the model");
        app.providers = storage.list_providers().await.expect("list providers");
        app.models = storage
            .list_models(Some(provider_id))
            .await
            .expect("list models");
        app.selection = Selection::new(app.providers.first().cloned(), app.models.first().cloned());
        let bar = bar::compose(&app.bar_data(), 80, 30);
        let provider = bar
            .segments
            .iter()
            .find(|segment| segment.kind == Some(BarKind::Provider))
            .expect("the provider segment exists")
            .rect;
        let model = bar
            .segments
            .iter()
            .find(|segment| segment.kind == Some(BarKind::Model))
            .expect("the model segment exists")
            .rect;
        let usage = bar
            .segments
            .iter()
            .find(|segment| segment.text.starts_with('◫'))
            .expect("the usage segment exists")
            .rect;
        app.set_bar(bar);
        app.set_bar_area(Rect::new(0, 5, 80, 1));
        let click = |column| MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Left),
            column,
            row: 5,
            modifiers: KeyModifiers::NONE,
        };

        app.handle_mouse(click(provider.x)).await;
        assert!(matches!(app.overlay, Some(Overlay::Providers { .. })));
        assert_eq!(app.focus, Focus::Input);

        app.overlay = None;
        app.handle_mouse(click(model.x)).await;
        assert!(matches!(app.overlay, Some(Overlay::Models { .. })));
        assert_eq!(app.focus, Focus::Input);

        app.overlay = None;
        app.handle_mouse(click(usage.x)).await;
        assert!(app.overlay.is_none());
        assert_eq!(app.focus, Focus::Input);

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
                    .as_mut()
                    .expect("the app holds a subscription")
                    .recv()
                    .await
                    .expect("receive a feed result");
                let Some(event) = result else {
                    panic!("the subscription closed unexpectedly");
                };
                let accepted = event.envelope.name == "core/input.accepted";
                app.handle_feed(Ok(Some(event))).await;
                if accepted {
                    break;
                }
            }
        })
        .await
        .expect("receive the accepted event");
        settle_load(&mut app).await;
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

    #[tokio::test]
    async fn subscription_closure_resubscribes_and_reloads_the_topic() {
        let (dir, mut app) = test_app().await;
        app.screen = Screen::Session(Session::new(topic(7), None));
        app.handle_feed(Ok(None)).await;
        assert!(app.subscription.is_some());
        let loading = app.loading.as_ref().expect("the topic reload starts");
        assert_eq!(loading.topic_id, 7);
        let Screen::Session(session) = &app.screen else {
            panic!("the test installs a session");
        };
        assert_eq!(
            session
                .log
                .entries()
                .filter(|entry| entry.summary == "subscription closed; resubscribing")
                .count(),
            1
        );
        if let Some(loading) = app.loading.take() {
            loading.task.abort();
        }
        close(app).await;
        drop(dir);
    }

    #[tokio::test]
    async fn resubscribe_failure_sets_a_notice_and_keeps_retrying() {
        let (dir, mut app) = test_app().await;
        app.runtime.shutdown().await.expect("shut down the runtime");
        app.handle_feed(Ok(None)).await;
        assert!(app.subscription.is_none());
        assert!(
            app.notice
                .as_deref()
                .is_some_and(|notice| notice.starts_with("resubscribe failed"))
        );
        app.resubscribe().await;
        assert!(app.subscription.is_none());
        drop(dir);
    }

    #[tokio::test]
    async fn load_buffer_overflow_restarts_the_load_with_the_triggering_event() {
        let (dir, mut app) = test_app().await;
        app.screen = Screen::Session(Session::new(topic(7), None));
        app.begin_load(7);
        let loading = app.loading.as_mut().expect("begin_load starts a load");
        for seq in 1..=LOAD_BUFFER_CAP as i64 {
            loading.buffer.push(event(seq, 7));
        }
        let trigger = Arc::new(event(LOAD_BUFFER_CAP as i64 + 1, 7));
        app.buffer_event(Arc::clone(&trigger));
        let loading = app.loading.as_ref().expect("the load restarted");
        assert_eq!(loading.topic_id, 7);
        assert_eq!(loading.buffer.len(), 1);
        assert_eq!(loading.buffer[0].key, trigger.key);
        let Screen::Session(session) = &app.screen else {
            panic!("the test installs a session");
        };
        assert!(
            session
                .log
                .entries()
                .any(|entry| entry.summary == "event buffer overflow; reloading the session")
        );
        if let Some(loading) = app.loading.take() {
            loading.task.abort();
        }
        close(app).await;
        drop(dir);
    }

    #[tokio::test]
    async fn tab_cycles_focus_through_all_five_areas() {
        let (dir, mut app) = test_app().await;
        assert_eq!(app.focus, Focus::Input);
        for expected in [Focus::Content, Focus::Mods, Focus::Tree, Focus::Log] {
            app.handle_key(press(KeyCode::Tab)).await;
            assert_eq!(app.focus, expected);
        }
        app.handle_key(press(KeyCode::Tab)).await;
        assert_eq!(app.focus, Focus::Input);
        app.handle_key(KeyEvent::new(KeyCode::BackTab, KeyModifiers::SHIFT))
            .await;
        assert_eq!(app.focus, Focus::Log);
        close(app).await;
        drop(dir);
    }

    #[tokio::test]
    async fn scroll_keys_only_act_on_the_focused_area() {
        let (dir, mut app) = test_app().await;
        let mut session = Session::new(topic(7), None);
        session.content_max = 5;
        session.content_scroll = 5;
        app.screen = Screen::Session(session);
        app.handle_key(press(KeyCode::Up)).await;
        let Screen::Session(session) = &app.screen else {
            panic!("the test installs a session");
        };
        assert!(session.follow);
        assert_eq!(session.content_scroll, 5);

        app.focus = Focus::Content;
        app.handle_key(press(KeyCode::Up)).await;
        let Screen::Session(session) = &app.screen else {
            panic!("the test installs a session");
        };
        assert!(!session.follow);
        assert_eq!(session.content_scroll, 4);
        close(app).await;
        drop(dir);
    }

    #[tokio::test]
    async fn storage_failure_shows_an_error_and_retries_on_the_next_tick() {
        let (dir, mut app) = test_app().await;
        app.screen = Screen::Session(Session::new(topic(7), None));
        app.begin_load(7);
        settle_load(&mut app).await;
        assert!(app.storage_error.is_some());
        assert!(app.loading.is_none());

        app.retry_storage();
        assert!(app.storage_error.is_none());
        let loading = app.loading.as_ref().expect("the retry starts a load");
        assert_eq!(loading.topic_id, 7);
        if let Some(loading) = app.loading.take() {
            loading.task.abort();
        }
        close(app).await;
        drop(dir);
    }

    #[test]
    fn scrolling_up_stops_following_the_content_tail() {
        let mut session = Session::new(topic(1), None);
        session.content_max = 10;
        session.content_scroll = 10;
        assert!(session.follow);
        session.scroll_content_up();
        assert!(!session.follow);
        assert_eq!(session.content_scroll, 9);
        session.scroll_content_up();
        assert_eq!(session.content_scroll, 8);
    }

    #[test]
    fn end_and_reaching_the_bottom_restore_following() {
        let mut session = Session::new(topic(1), None);
        session.content_max = 10;
        session.content_scroll = 3;
        session.follow = false;
        session.scroll_content_end();
        assert!(session.follow);
        assert_eq!(session.content_scroll, 10);

        session.follow = false;
        session.content_scroll = 9;
        session.scroll_content_down();
        assert!(session.follow);
        assert_eq!(session.content_scroll, 10);
    }

    #[test]
    fn short_content_keeps_following_at_the_top() {
        let mut session = Session::new(topic(1), None);
        session.content_max = 0;
        session.follow = false;
        session.content_scroll = 5;
        session.scroll_content_up();
        assert!(session.follow);
        assert_eq!(session.content_scroll, 0);
        session.follow = false;
        session.scroll_content_down();
        assert!(session.follow);
        assert_eq!(session.content_scroll, 0);
    }

    #[test]
    fn side_panes_scroll_between_home_and_end() {
        let mut session = Session::new(topic(1), None);
        session.mods_max = 3;
        session.scroll_mods(KeyCode::End);
        assert_eq!(session.mods_scroll, 3);
        session.scroll_mods(KeyCode::Up);
        assert_eq!(session.mods_scroll, 2);
        session.scroll_mods(KeyCode::Home);
        assert_eq!(session.mods_scroll, 0);

        session.tree_max = 4;
        session.scroll_tree(KeyCode::Down);
        assert_eq!(session.tree_scroll, 1);
        session.scroll_tree(KeyCode::End);
        assert_eq!(session.tree_scroll, 4);
    }

    #[test]
    fn the_log_defaults_to_the_tail_and_returns_to_it_with_end() {
        let mut session = Session::new(topic(1), None);
        assert_eq!(session.log_scroll, u16::MAX);
        session.log_max = 8;
        session.scroll_log(KeyCode::Up);
        assert_eq!(session.log_scroll, 7);
        session.scroll_log(KeyCode::Down);
        assert_eq!(session.log_scroll, 8);
        session.scroll_log(KeyCode::Down);
        assert_eq!(session.log_scroll, u16::MAX);
        session.scroll_log(KeyCode::Home);
        assert_eq!(session.log_scroll, 0);
        session.scroll_log(KeyCode::Down);
        assert_eq!(session.log_scroll, 1);
        session.scroll_log(KeyCode::End);
        assert_eq!(session.log_scroll, u16::MAX);
    }
}
