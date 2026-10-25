use super::block::{Block, BlockKind, MEDIA_FILE, MEDIA_IMAGE, Role};
use super::id::{BlockId, MessageId};
use super::payload::BlockCommit;
use std::collections::{BTreeMap, HashMap};
use wind_wire::{Content, FunctionCall, Message};

#[derive(Debug, Default)]
pub struct BlockAccumulator {
    open: BTreeMap<BlockId, Block>,
    current: HashMap<(MessageId, BlockKind), BlockId>,
    calls: HashMap<(MessageId, String), BlockId>,
    next_id: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BlockChange {
    Opened(Block),
    Grew {
        id: BlockId,
        revision: u64,
        fragment: String,
    },
}

impl BlockAccumulator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn after(last: BlockId) -> Self {
        Self {
            next_id: last.0,
            ..Self::default()
        }
    }

    pub fn absorb(&mut self, of_message: MessageId, partial: &Message) -> Vec<BlockChange> {
        let mut changes = Vec::new();
        if let Some(reasoning) = partial.reasoning_content.as_deref() {
            self.continue_streaming(
                of_message,
                partial.role,
                BlockKind::Reasoning,
                reasoning,
                &mut changes,
            );
        }
        for item in &partial.content {
            match item {
                Content::Text { data } => {
                    self.continue_streaming(
                        of_message,
                        partial.role,
                        BlockKind::Text,
                        data,
                        &mut changes,
                    );
                }
                Content::Image { .. } | Content::File { .. } | Content::Audio { .. } => {
                    // TODO: 流式 media 数据一次性到达待验证
                    self.open_media(of_message, partial.role, item, &mut changes);
                }
                Content::FunctionCall { data } => {
                    self.open_whole(
                        of_message,
                        partial.role,
                        BlockKind::ToolResult,
                        Some(data.id.clone()),
                        String::new(),
                        Some(data.content.clone()),
                        &mut changes,
                    );
                }
            }
        }
        for call in partial.tool_calls.iter().flatten() {
            match self.tool_call_target(of_message, &call.id) {
                Some(id) => self.grow(id, &call.arguments, &mut changes),
                None => self.open_tool_call(of_message, partial.role, call, &mut changes),
            }
        }
        changes
    }

    fn tool_call_target(&self, of_message: MessageId, call_id: &str) -> Option<BlockId> {
        if call_id.is_empty() {
            return self
                .current
                .get(&(of_message, BlockKind::ToolCall))
                .copied();
        }
        self.calls.get(&(of_message, call_id.to_string())).copied()
    }

    pub fn get(&self, id: BlockId) -> Option<&Block> {
        self.open.get(&id)
    }

    pub fn is_empty(&self) -> bool {
        self.open.is_empty()
    }

    pub fn candidate(&self, id: BlockId) -> Option<BlockCommit> {
        let block = self.open.get(&id)?;
        if block.kind != BlockKind::ToolCall {
            return Some(BlockCommit {
                block_id: id,
                kind: block.kind,
                call_id: block.call_id.clone(),
                content: block.content.clone(),
                data: block.data.clone(),
            });
        }
        let name = block
            .data
            .as_ref()
            .and_then(|data| data.get("name"))
            .cloned();
        let parsed = serde_json::from_str::<serde_json::Value>(&block.content).ok();
        let content = if parsed.is_some() {
            String::new()
        } else {
            block.content.clone()
        };
        let data = match (name, parsed) {
            (Some(name), Some(arguments)) => Some(serde_json::json!({
                "name": name,
                "arguments": arguments,
            })),
            (Some(name), None) => Some(serde_json::json!({ "name": name })),
            (None, Some(arguments)) => Some(serde_json::json!({ "arguments": arguments })),
            (None, None) => None,
        };
        Some(BlockCommit {
            block_id: id,
            kind: block.kind,
            call_id: block.call_id.clone(),
            content,
            data,
        })
    }

    /// 定稿一份副本，原块留在累加器里
    pub fn settled(
        &self,
        id: BlockId,
        content: String,
        data: Option<serde_json::Value>,
    ) -> Option<Block> {
        let mut block = self.open.get(&id)?.clone();
        block.content = content;
        block.data = data;
        block.finished = true;
        Some(block)
    }

    pub fn commit(
        &mut self,
        id: BlockId,
        content: String,
        data: Option<serde_json::Value>,
    ) -> Option<Block> {
        let mut block = self.open.remove(&id)?;
        block.content = content;
        block.data = data;
        block.finished = true;
        self.forget_targets(id);
        Some(block)
    }

    pub fn discard(&mut self, id: BlockId) -> Option<Block> {
        let block = self.open.remove(&id)?;
        self.forget_targets(id);
        Some(block)
    }

    pub fn snapshot(&self) -> Vec<Block> {
        self.open.values().cloned().collect()
    }

    fn forget_targets(&mut self, id: BlockId) {
        self.current.retain(|_, target| *target != id);
        self.calls.retain(|_, target| *target != id);
    }

    fn continue_streaming(
        &mut self,
        of_message: MessageId,
        role: Role,
        kind: BlockKind,
        fragment: &str,
        out: &mut Vec<BlockChange>,
    ) {
        if fragment.is_empty() {
            return;
        }
        let id = match self.current.get(&(of_message, kind)).copied() {
            Some(id) => id,
            None => {
                let id = self.open_block(of_message, role, kind, None, None);
                self.current.insert((of_message, kind), id);
                self.push_opened(id, out);
                id
            }
        };
        self.grow(id, fragment, out);
    }

    /// 开一个一次到达的块
    /// 参数分为块身份、一次到达的内容与变化收集处三组，都是它固有的输入。
    #[allow(clippy::too_many_arguments)]
    fn open_whole(
        &mut self,
        of_message: MessageId,
        role: Role,
        kind: BlockKind,
        call_id: Option<String>,
        content: String,
        data: Option<serde_json::Value>,
        out: &mut Vec<BlockChange>,
    ) {
        let id = self.open_block(of_message, role, kind, call_id, data);
        self.push_opened(id, out);
        self.grow(id, &content, out);
    }

    /// 把一个片段追加到指定块，并把增长记成变化
    fn grow(&mut self, id: BlockId, fragment: &str, out: &mut Vec<BlockChange>) {
        if fragment.is_empty() {
            return;
        }
        if let Some(revision) = self.append(id, fragment) {
            out.push(BlockChange::Grew {
                id,
                revision,
                fragment: fragment.to_string(),
            });
        }
    }

    /// 开一个媒体块
    /// 媒体一次到达，没有续写；具体的 mime 放进载荷，判别式里只有种类
    fn open_media(
        &mut self,
        of_message: MessageId,
        role: Role,
        item: &Content,
        out: &mut Vec<BlockChange>,
    ) {
        let (mime, payload) = match item {
            Content::Image { data } => (MEDIA_IMAGE, data.clone()),
            Content::File { data } => (MEDIA_FILE, data.clone()),
            Content::Audio { data } => (data.format.as_str(), data.content.clone()),
            Content::Text { .. } | Content::FunctionCall { .. } => return,
        };
        let data = serde_json::json!({ "mime": mime });
        self.open_whole(
            of_message,
            role,
            BlockKind::Media,
            None,
            payload,
            Some(data),
            out,
        );
    }

    /// 开一个工具调用块，把已知的工具名记进载荷，并让它成为本消息的续写目标
    /// 工具名只在首个片段出现，标识与名字都缺时留空而不是记成空串
    fn open_tool_call(
        &mut self,
        of_message: MessageId,
        role: Role,
        call: &FunctionCall,
        out: &mut Vec<BlockChange>,
    ) {
        let call_id = (!call.id.is_empty()).then(|| call.id.clone());
        let data = (!call.name.is_empty()).then(|| serde_json::json!({ "name": call.name }));
        let id = self.open_block(of_message, role, BlockKind::ToolCall, call_id.clone(), data);
        self.current.insert((of_message, BlockKind::ToolCall), id);
        if let Some(call_id) = call_id {
            self.calls.insert((of_message, call_id), id);
        }
        self.push_opened(id, out);
        self.grow(id, &call.arguments, out);
    }

    /// 分配一个块标识，不建立块
    pub fn allocate_id(&mut self) -> BlockId {
        self.next_id += 1;
        BlockId(self.next_id)
    }

    /// 开一个块，返回它的标识
    /// 只建身份，内容一律经增长变化交付，修订号因此从 0 起算
    fn open_block(
        &mut self,
        of_message: MessageId,
        role: Role,
        kind: BlockKind,
        call_id: Option<String>,
        data: Option<serde_json::Value>,
    ) -> BlockId {
        self.next_id += 1;
        let id = BlockId(self.next_id);
        self.open.insert(
            id,
            Block {
                id,
                of_message,
                role,
                kind,
                call_id,
                revision: 0,
                content: String::new(),
                data,
                finished: false,
            },
        );
        id
    }

    /// 把开块变化记进结果
    fn push_opened(&self, id: BlockId, out: &mut Vec<BlockChange>) {
        if let Some(block) = self.open.get(&id) {
            out.push(BlockChange::Opened(block.clone()));
        }
    }

    /// 追加一段原始片段，返回新的修订号
    /// **不做任何解析或校验**，分片会把 JSON 与多字节字符切成两半，
    /// 在拼装完整之前它们都是无意义的字节序列；解析是提交阶段的事
    fn append(&mut self, id: BlockId, fragment: &str) -> Option<u64> {
        let block = self.open.get_mut(&id)?;
        block.content.push_str(fragment);
        block.revision += 1;
        Some(block.revision)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::BlockKind;
    use wind_wire::{Content, FunctionCall, FunctionCallOutput, Role};

    fn acc() -> BlockAccumulator {
        BlockAccumulator::new()
    }

    /// 一段只有正文的增量
    fn text(role: Role, data: &str) -> Message {
        Message {
            role,
            content: vec![Content::Text { data: data.into() }],
            ..Message::default()
        }
    }

    /// 一段工具调用增量，`id` 为空表示它在续写已经打开的那一个
    fn call(id: &str, name: &str, arguments: &str) -> Message {
        Message {
            tool_calls: Some(vec![FunctionCall {
                id: id.into(),
                name: name.into(),
                arguments: arguments.into(),
            }]),
            ..Message::default()
        }
    }

    fn opened(changes: &[BlockChange]) -> Vec<Block> {
        changes
            .iter()
            .filter_map(|change| match change {
                BlockChange::Opened(block) => Some(block.clone()),
                BlockChange::Grew { .. } => None,
            })
            .collect()
    }

    #[test]
    fn the_first_text_increment_opens_a_block() {
        let mut a = acc();
        let changes = a.absorb(MessageId(1), &text(Role::Assistant, "he"));
        let blocks = opened(&changes);
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].kind, BlockKind::Text);
        assert_eq!(
            blocks[0].content, "",
            "opening a block carries no content; content is always delivered through growth changes"
        );
        assert_eq!(
            blocks[0].revision, 0,
            "a freshly opened block has not grown yet"
        );
        assert_eq!(blocks[0].of_message, MessageId(1));
        assert_eq!(blocks[0].role, Role::Assistant);
        assert_eq!(
            changes.len(),
            2,
            "opening the block and the first fragment each produce one change"
        );
        assert!(
            matches!(
                &changes[1],
                BlockChange::Grew { revision: 1, fragment, .. } if fragment.as_str() == "he"
            ),
            "the first fragment must be emitted as a growth change, or subscribers never receive it"
        );
        assert_eq!(a.get(blocks[0].id).expect("the block exists").content, "he");
    }

    #[test]
    fn later_text_increments_grow_the_same_block() {
        let mut a = acc();
        let first = opened(&a.absorb(MessageId(1), &text(Role::Assistant, "he")))[0].id;
        let second = a.absorb(MessageId(1), &text(Role::Assistant, "llo"));
        assert!(
            opened(&second).is_empty(),
            "a second increment must not open a new block: text in the same message is a continuation"
        );
        assert_eq!(second.len(), 1);
        assert!(matches!(second[0], BlockChange::Grew { id, .. } if id == first));
        assert_eq!(a.get(first).expect("the block exists").content, "hello");
        assert_eq!(
            a.get(first).expect("the block exists").revision,
            2,
            "the revision number is the number of fragments appended so far"
        );
    }

    #[test]
    fn reasoning_and_text_get_separate_blocks_in_one_increment() {
        let mut a = acc();
        let both = Message {
            role: Role::Assistant,
            content: vec![Content::Text { data: "ans".into() }],
            reasoning_content: Some("id".into()),
            ..Message::default()
        };
        let blocks = opened(&a.absorb(MessageId(1), &both));
        let kinds: Vec<BlockKind> = blocks.iter().map(|b| b.kind).collect();
        assert!(kinds.contains(&BlockKind::Reasoning));
        assert!(kinds.contains(&BlockKind::Text));
        assert_eq!(
            blocks.len(),
            2,
            "the two kinds of content each get their own block"
        );
    }

    #[test]
    fn reasoning_text_and_tool_call_in_one_increment_get_three_blocks() {
        // 有的模型在同一次增量里既给推理、又给正文、又给工具调用
        let mut a = acc();
        let all = Message {
            role: Role::Assistant,
            content: vec![Content::Text { data: "ans".into() }],
            reasoning_content: Some("id".into()),
            tool_calls: Some(vec![FunctionCall {
                id: "call_abc".into(),
                name: "get_weather".into(),
                arguments: "{\"ci".into(),
            }]),
            ..Message::default()
        };
        let changes = a.absorb(MessageId(1), &all);
        let blocks = opened(&changes);
        assert_eq!(
            blocks.len(),
            3,
            "the three kinds each get their own block and never overwrite each other"
        );
        for kind in [BlockKind::Reasoning, BlockKind::Text, BlockKind::ToolCall] {
            let count = blocks.iter().filter(|b| b.kind == kind).count();
            assert_eq!(count, 1, "exactly one block of {kind:?} per increment");
        }
        let id_of = |kind: BlockKind| {
            blocks
                .iter()
                .find(|b| b.kind == kind)
                .expect("the block exists")
                .id
        };
        assert_eq!(
            a.get(id_of(BlockKind::Reasoning))
                .expect("the block exists")
                .content,
            "id"
        );
        assert_eq!(
            a.get(id_of(BlockKind::Text))
                .expect("the block exists")
                .content,
            "ans"
        );
        let call = a.get(id_of(BlockKind::ToolCall)).expect("the block exists");
        assert_eq!(call.call_id.as_deref(), Some("call_abc"));
        assert_eq!(
            call.data.as_ref().expect("the payload exists")["name"],
            serde_json::json!("get_weather")
        );
        assert_eq!(call.content, "{\"ci");
    }

    #[test]
    fn a_later_increment_grows_all_three_blocks_at_once() {
        let mut a = acc();
        let first = Message {
            role: Role::Assistant,
            content: vec![Content::Text { data: "ans".into() }],
            reasoning_content: Some("id".into()),
            tool_calls: Some(vec![FunctionCall {
                id: "c1".into(),
                name: "get_weather".into(),
                arguments: "{\"ci".into(),
            }]),
            ..Message::default()
        };
        let first_blocks = opened(&a.absorb(MessageId(1), &first));
        let id_of = |kind: BlockKind| {
            first_blocks
                .iter()
                .find(|b| b.kind == kind)
                .expect("the block exists")
                .id
        };
        let second = Message {
            role: Role::Assistant,
            content: vec![Content::Text { data: "wer".into() }],
            reasoning_content: Some("ea".into()),
            tool_calls: Some(vec![FunctionCall {
                id: "".into(),
                name: "".into(),
                arguments: "ty\":1}".into(),
            }]),
            ..Message::default()
        };
        let changes = a.absorb(MessageId(1), &second);
        assert!(
            opened(&changes).is_empty(),
            "all three kinds of content in the same message are continuations"
        );
        assert_eq!(
            changes.len(),
            3,
            "each of the three kinds produces one growth"
        );
        assert_eq!(a.snapshot().len(), 3, "no new block was opened");
        assert_eq!(
            a.get(id_of(BlockKind::Reasoning))
                .expect("the block exists")
                .content,
            "idea"
        );
        assert_eq!(
            a.get(id_of(BlockKind::Text))
                .expect("the block exists")
                .content,
            "answer"
        );
        assert_eq!(
            a.get(id_of(BlockKind::ToolCall))
                .expect("the block exists")
                .content,
            "{\"city\":1}"
        );
    }

    #[test]
    fn a_complete_message_absorbed_once_is_committable() {
        // 非流式请求下，一次模型往返就是一个完整的增量，而且只有这一次
        let mut a = acc();
        let whole = Message {
            role: Role::Assistant,
            content: vec![Content::Text {
                data: "22 degrees".into(),
            }],
            reasoning_content: Some("look it up".into()),
            tool_calls: Some(vec![
                FunctionCall {
                    id: "c1".into(),
                    name: "get_weather".into(),
                    arguments: "{\"city\":\"SF\"}".into(),
                },
                FunctionCall {
                    id: "c2".into(),
                    name: "get_time".into(),
                    arguments: "{}".into(),
                },
            ]),
            ..Message::default()
        };
        let changes = a.absorb(MessageId(7), &whole);
        assert_eq!(
            opened(&changes).len(),
            4,
            "reasoning, text, and two tool calls each get a block; two calls arriving in the same increment are not merged"
        );
        let ids: Vec<BlockId> = a.snapshot().iter().map(|b| b.id).collect();
        assert_eq!(ids.len(), 4);
        assert!(
            a.snapshot().iter().all(|b| b.revision == 1),
            "a complete increment is a single append, so every revision stays at 1"
        );

        let candidate_of = |call_id: &str| {
            let id = a
                .snapshot()
                .iter()
                .find(|b| b.call_id.as_deref() == Some(call_id))
                .expect("the block exists")
                .id;
            a.candidate(id).expect("the candidate exists")
        };
        let weather = candidate_of("c1");
        assert!(
            weather.content.is_empty(),
            "the raw arguments are in the payload"
        );
        let data = weather.data.as_ref().expect("the payload exists");
        assert_eq!(data["name"], serde_json::json!("get_weather"));
        assert_eq!(
            data["arguments"]["city"],
            serde_json::json!("SF"),
            "arguments that arrive non-streamed are complete JSON and are parsed on commit"
        );
        assert_eq!(
            candidate_of("c2")
                .data
                .as_ref()
                .expect("the payload exists")["name"],
            serde_json::json!("get_time"),
            "the two parallel calls are each paired with their own id"
        );

        for id in ids {
            let candidate = a.candidate(id).expect("the candidate exists");
            let committed = a
                .commit(id, candidate.content, candidate.data)
                .expect("the block exists");
            assert!(committed.finished);
        }
        assert!(
            a.is_empty(),
            "no uncommitted blocks remain after one non-streamed round trip is committed"
        );
        assert!(a.snapshot().is_empty());
    }

    #[test]
    fn two_messages_never_share_a_block() {
        let mut a = acc();
        let one = opened(&a.absorb(MessageId(1), &text(Role::Assistant, "first")))[0].id;
        let two = opened(&a.absorb(MessageId(2), &text(Role::Assistant, "second")))[0].id;
        assert_ne!(
            one, two,
            "continuation is scoped to the current message, not globally"
        );
        assert_eq!(a.get(one).expect("the block exists").content, "first");
        assert_eq!(a.get(two).expect("the block exists").content, "second");
    }

    #[test]
    fn every_media_item_opens_a_fresh_block() {
        let mut a = acc();
        let two = Message {
            role: Role::User,
            content: vec![
                Content::Image {
                    data: "AAAA".into(),
                },
                Content::Image {
                    data: "BBBB".into(),
                },
            ],
            ..Message::default()
        };
        let blocks = opened(&a.absorb(MessageId(1), &two));
        assert_eq!(
            blocks.len(),
            2,
            "media arrives all at once with no continuation, so each gets its own block"
        );
        assert!(blocks.iter().all(|b| b.kind == BlockKind::Media));
    }

    #[test]
    fn a_tool_result_opens_its_own_block_keyed_by_call_id() {
        let mut a = acc();
        let result = Message {
            role: Role::Tool,
            content: vec![Content::FunctionCall {
                data: FunctionCallOutput {
                    id: "call_abc".into(),
                    content: serde_json::json!({ "temp": 22 }),
                },
            }],
            ..Message::default()
        };
        let blocks = opened(&a.absorb(MessageId(2), &result));
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].kind, BlockKind::ToolResult);
        assert_eq!(blocks[0].call_id.as_deref(), Some("call_abc"));
        assert_eq!(
            blocks[0].data.as_ref().expect("the payload exists")["temp"],
            serde_json::json!(22)
        );
    }

    #[test]
    fn a_new_tool_call_opens_a_block_and_keeps_the_name() {
        let mut a = acc();
        let changes = a.absorb(MessageId(1), &call("call_abc", "get_weather", "{\"ci"));
        let blocks = opened(&changes);
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].kind, BlockKind::ToolCall);
        assert_eq!(blocks[0].call_id.as_deref(), Some("call_abc"));
        assert_eq!(
            blocks[0].data.as_ref().expect("the payload exists")["name"],
            serde_json::json!("get_weather"),
            "the tool name appears only in the first fragment and is recorded when the block opens"
        );
        assert_eq!(
            a.get(blocks[0].id).expect("the block exists").content,
            "{\"ci"
        );
    }

    #[test]
    fn argument_fragments_grow_the_open_tool_call() {
        let mut a = acc();
        let id = opened(&a.absorb(MessageId(1), &call("call_abc", "get_weather", "{\"ci")))[0].id;
        let changes = a.absorb(MessageId(1), &call("", "", "ty\":\"SF\"}"));
        assert!(
            opened(&changes).is_empty(),
            "a continuation does not open a new block"
        );
        assert!(matches!(changes[0], BlockChange::Grew { id: grew, .. } if grew == id));
        assert_eq!(
            a.get(id).expect("the block exists").content,
            "{\"city\":\"SF\"}",
            "argument fragments are concatenated, not replaced"
        );
    }

    #[test]
    fn a_second_tool_call_does_not_steal_the_first_ones_fragments() {
        let mut a = acc();
        let first = opened(&a.absorb(MessageId(1), &call("c1", "a", "{\"x\"")))[0].id;
        let second = opened(&a.absorb(MessageId(1), &call("c2", "b", "{\"y\"")))[0].id;
        a.absorb(MessageId(1), &call("", "", ":1}"));
        assert_eq!(
            a.get(second).expect("the block exists").content,
            "{\"y\":1}",
            "a continuation lands on the most recently opened block"
        );
        assert_eq!(a.get(first).expect("the block exists").content, "{\"x\"");
    }

    #[test]
    fn identifiers_are_assigned_ascending() {
        let mut a = acc();
        let first = opened(&a.absorb(MessageId(1), &text(Role::Assistant, "first")))[0].id;
        let second = opened(&a.absorb(MessageId(2), &text(Role::User, "second")))[0].id;
        let third = opened(&a.absorb(MessageId(3), &text(Role::User, "third")))[0].id;
        assert!(first < second && second < third);
    }

    #[test]
    fn an_empty_increment_changes_nothing() {
        let mut a = acc();
        let changes = a.absorb(MessageId(1), &Message::default());
        assert!(changes.is_empty());
        assert!(
            a.is_empty(),
            "an increment carrying nothing must not open an empty block"
        );
    }

    #[test]
    fn a_text_candidate_carries_the_accumulated_content() {
        let mut a = acc();
        let id = opened(&a.absorb(MessageId(1), &text(Role::Assistant, "hello")))[0].id;
        let candidate = a.candidate(id).expect("the candidate exists");
        assert_eq!(candidate.kind, BlockKind::Text);
        assert_eq!(candidate.content, "hello");
        assert!(candidate.data.is_none());
    }

    #[test]
    fn a_tool_call_candidate_moves_the_arguments_into_the_payload() {
        let mut a = acc();
        let id =
            opened(&a.absorb(MessageId(1), &call("call_abc", "get_weather", "{\"city\":")))[0].id;
        a.absorb(MessageId(1), &call("", "", "\"SF\"}"));

        let candidate = a.candidate(id).expect("the candidate exists");
        assert!(
            candidate.content.is_empty(),
            "the raw source is now in the payload and the body is cleared"
        );
        assert_eq!(
            candidate.call_id.as_deref(),
            Some("call_abc"),
            "the pairing key must be handed to the interception chain along with the candidate"
        );
        let data = candidate.data.as_ref().expect("the payload exists");
        assert_eq!(data["name"], serde_json::json!("get_weather"));
        assert_eq!(data["arguments"]["city"], serde_json::json!("SF"));
    }

    #[test]
    fn unparseable_arguments_are_reported_not_silently_emptied() {
        // 模型给出半截 JSON 是真实会发生的，退回空参数会让调用方以为它没有参数，
        // 而真相是"参数坏了"——两者必须可区分
        let mut a = acc();
        let id =
            opened(&a.absorb(MessageId(1), &call("call_abc", "get_weather", "{\"city\":")))[0].id;
        let candidate = a.candidate(id).expect("the candidate exists");
        assert_eq!(
            candidate.content, "{\"city\":",
            "when parsing fails the raw source stays in the body so the caller can see the format is wrong"
        );
        assert!(
            candidate
                .data
                .as_ref()
                .expect("the payload exists")
                .get("arguments")
                .is_none(),
            "must not synthesize an empty arguments object"
        );
    }

    #[test]
    fn committing_replaces_the_accumulated_content_wholesale() {
        let mut a = acc();
        let id = opened(&a.absorb(MessageId(1), &text(Role::Assistant, "original")))[0].id;
        let committed = a
            .commit(id, "rewritten".into(), None)
            .expect("the block exists");
        assert_eq!(
            committed.content, "rewritten",
            "commit replaces wholesale, it does not append"
        );
        assert!(committed.finished);
    }

    #[test]
    fn a_committed_block_leaves_the_accumulator() {
        let mut a = acc();
        let id = opened(&a.absorb(MessageId(1), &text(Role::Assistant, "x")))[0].id;
        a.commit(id, "x".into(), None).expect("the block exists");
        assert!(
            a.get(id).is_none(),
            "a committed block no longer stays in the accumulator"
        );
        assert!(a.is_empty());
        assert!(a.snapshot().is_empty());
    }

    #[test]
    fn a_discarded_block_stops_being_the_continuation_target() {
        // 被否决的块不会写入数据库，后续片段必须另开一块；若留在累加器里当续写目标，
        // 那些片段会追加到一个永远不会成为事实的块上
        let mut a = acc();
        let denied = opened(&a.absorb(MessageId(1), &text(Role::Assistant, "denied")))[0].id;
        let discarded = a.discard(denied).expect("the block exists");
        assert_eq!(
            discarded.content, "denied",
            "discarding still hands back that block"
        );
        assert!(
            a.is_empty(),
            "a discarded block no longer stays in the accumulator"
        );

        let again = opened(&a.absorb(MessageId(1), &text(Role::Assistant, "follow-up")));
        assert_eq!(again.len(), 1, "a later fragment opens another block");
        assert_ne!(again[0].id, denied, "it is not the discarded one");
        assert_eq!(
            a.get(again[0].id).expect("the block exists").content,
            "follow-up"
        );
    }

    #[test]
    fn discarding_an_unknown_block_changes_nothing() {
        let mut a = acc();
        assert!(a.discard(BlockId(9)).is_none());
        assert!(a.is_empty());
    }

    #[test]
    fn a_snapshot_holds_every_uncommitted_block_in_identifier_order() {
        let mut a = acc();
        let first = opened(&a.absorb(MessageId(1), &text(Role::Assistant, "first")))[0].id;
        let second = opened(&a.absorb(MessageId(2), &text(Role::User, "second")))[0].id;
        let ids: Vec<BlockId> = a.snapshot().iter().map(|b| b.id).collect();
        assert_eq!(ids, vec![first, second]);
    }

    #[test]
    fn a_repeated_call_id_continues_the_same_tool_call() {
        // 有的提供商每个片段都重发标识，若按"标识非空即开新块"处理，
        // 一次调用会被劈成多个块，参数各自都是残句
        let mut a = acc();
        let first = opened(&a.absorb(MessageId(1), &call("c1", "get_weather", "{\"ci")))[0].id;
        let changes = a.absorb(MessageId(1), &call("c1", "", "ty\":\"SF\"}"));
        assert!(
            opened(&changes).is_empty(),
            "later fragments with the same id do not open a new block"
        );
        assert!(matches!(changes[0], BlockChange::Grew { id, .. } if id == first));
        assert_eq!(a.snapshot().len(), 1, "one call has only one block");
        let block = a.get(first).expect("the block exists");
        assert_eq!(block.content, "{\"city\":\"SF\"}");
        assert_eq!(
            block.data.as_ref().expect("the payload exists")["name"],
            serde_json::json!("get_weather"),
            "a later fragment with no name must not overwrite an existing name with empty"
        );
    }

    #[test]
    fn an_unnamed_tool_call_keeps_its_arguments_in_the_candidate() {
        // 累加器在流中途建立、或提供商的首个片段不带标识时，工具调用块没有名字，
        // 参数解析成功也不能因为缺名字被丢掉
        let mut a = acc();
        let id = opened(&a.absorb(MessageId(1), &call("", "", "{\"city\":\"SF\"}")))[0].id;
        let candidate = a.candidate(id).expect("the candidate exists");
        assert!(
            candidate.content.is_empty(),
            "the arguments are now in the payload"
        );
        assert_eq!(
            candidate.data.as_ref().expect("the payload exists")["arguments"]["city"],
            serde_json::json!("SF")
        );
    }

    #[test]
    fn an_accumulator_can_start_after_an_existing_identifier() {
        // 块标识是持久层的行标识，进程重启后必须接着库中已有的号发，否则与已提交的块撞号
        let mut a = BlockAccumulator::after(BlockId(100));
        let first = opened(&a.absorb(MessageId(1), &text(Role::Assistant, "first")))[0].id;
        assert_eq!(first, BlockId(101));
        let second = opened(&a.absorb(MessageId(2), &text(Role::Assistant, "second")))[0].id;
        assert_eq!(second, BlockId(102));
    }

    #[test]
    fn committing_an_unknown_block_changes_nothing() {
        let mut a = acc();
        assert!(a.commit(BlockId(9), "x".into(), None).is_none());
        assert!(a.candidate(BlockId(9)).is_none());
    }

    #[test]
    fn settled_returns_a_finished_clone_without_removing_the_block() {
        let mut a = acc();
        let id = opened(&a.absorb(MessageId(1), &text(Role::Assistant, "draft")))[0].id;
        let settled = a
            .settled(id, "final".into(), None)
            .expect("the block exists");
        assert_eq!(settled.content, "final");
        assert!(settled.finished);
        assert!(
            a.snapshot().iter().any(|block| block.id == id),
            "settling does not remove the block; only a successful persist may"
        );
        assert_eq!(
            a.get(id).expect("the block exists").content,
            "draft",
            "the clone is settled, the open block keeps its draft"
        );
    }

    #[test]
    fn allocate_id_does_not_open_a_block() {
        let mut a = acc();
        let first = a.allocate_id();
        assert!(
            a.snapshot().is_empty(),
            "allocating an id reserves a number without opening a block"
        );
        assert_eq!(a.allocate_id().0, first.0 + 1);
    }
}
