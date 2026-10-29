use std::collections::BTreeMap;
use wind_wire::{Block, BlockKind, Content, FunctionCall, Message, Role};

#[derive(Debug, Default)]
pub struct BlockAbsorb {
    messages: BTreeMap<i64, Vec<Block>>,
}

impl BlockAbsorb {
    pub fn new() -> Self {
        Self::default()
    }

    /// 把一条消息并入它分解出的块
    pub fn absorb(&mut self, message_id: i64, partial: Message) {
        let role = partial.role;
        if let Some(reasoning) = partial.reasoning_content {
            self.extend(
                message_id,
                BlockKind::Reasoning,
                || Block::new_reasoning(role),
                &reasoning,
            );
        }
        for item in partial.content {
            match item {
                Content::Text { data } => {
                    self.extend(message_id, BlockKind::Text, || Block::new_text(role), &data);
                }
                Content::Image { data } => self.push(message_id, Block::new_image(role, data)),
                Content::File { data } => self.push(message_id, Block::new_file(role, data)),
                Content::Audio { data } => self.push(
                    message_id,
                    Block::new_audio(role, data.format, data.content),
                ),
                Content::FunctionCall { data } => {
                    self.result(message_id, role, data.id, data.content)
                }
            }
        }
        for call in partial.tool_calls.into_iter().flatten() {
            self.call(message_id, role, call);
        }
    }

    /// 取出一条消息的块，按块种类排序后移出累加器
    pub fn settle(&mut self, message_id: i64) -> Vec<Block> {
        let Some(mut blocks) = self.messages.remove(&message_id) else {
            return Vec::new();
        };
        blocks.sort_by_key(|block| rank(block.kind()));
        blocks
    }

    /// 取走全部消息的块，同一条消息内仍按块种类排序
    pub fn settle_all(&mut self) -> Vec<(i64, Vec<Block>)> {
        let messages: Vec<i64> = self.messages.keys().copied().collect();
        messages
            .into_iter()
            .map(|message_id| (message_id, self.settle(message_id)))
            .collect()
    }

    /// 某条消息当前的块，不移出累加器
    pub fn blocks(&self, message_id: i64) -> &[Block] {
        self.messages
            .get(&message_id)
            .map(Vec::as_slice)
            .unwrap_or_default()
    }

    pub fn is_empty(&self) -> bool {
        self.messages.values().all(Vec::is_empty)
    }

    /// 清空数据
    pub fn clean(&mut self) {
        self.messages.clear();
    }

    fn push(&mut self, message_id: i64, block: Block) {
        self.messages.entry(message_id).or_default().push(block);
    }

    fn extend(
        &mut self,
        message_id: i64,
        kind: BlockKind,
        open: impl FnOnce() -> Block,
        fragment: &str,
    ) {
        if fragment.is_empty() {
            return;
        }
        let blocks = self.messages.entry(message_id).or_default();
        let index = match blocks.iter().position(|block| block.is(kind)) {
            Some(index) => index,
            None => {
                blocks.push(open());
                blocks.len() - 1
            }
        };
        blocks[index].append_text(fragment);
    }

    fn call(&mut self, message_id: i64, role: Role, call: FunctionCall) {
        let blocks = self.messages.entry(message_id).or_default();
        let index = if call.id.is_empty() {
            blocks
                .iter()
                .rposition(|block| block.is(BlockKind::ToolCall))
        } else {
            blocks.iter().position(|block| {
                block.is(BlockKind::ToolCall) && block.call_id() == Some(call.id.as_str())
            })
        };
        let index = match index {
            Some(index) => index,
            None => {
                blocks.push(Block::new_tool_call(
                    role,
                    call.id.clone(),
                    call.name.clone(),
                    None,
                ));
                blocks.len() - 1
            }
        };
        let block = &mut blocks[index];
        block.fill_call(&call.id, &call.name);
        block.append_arguments(&call.arguments);
    }

    fn result(&mut self, message_id: i64, role: Role, call_id: String, content: serde_json::Value) {
        let blocks = self.messages.entry(message_id).or_default();
        let found = blocks.iter().position(|block| {
            block.is(BlockKind::ToolResult) && block.call_id() == Some(call_id.as_str())
        });
        match found {
            Some(index) => blocks[index].set_result(content),
            None => blocks.push(Block::new_tool_result(role, call_id, content)),
        }
    }
}

/// 块写入数据库时的固定顺序
fn rank(kind: BlockKind) -> u8 {
    match kind {
        BlockKind::Reasoning => 0,
        BlockKind::Text => 1,
        BlockKind::Image => 2,
        BlockKind::File => 3,
        BlockKind::Audio => 4,
        BlockKind::ToolCall => 5,
        BlockKind::ToolResult => 6,
    }
}
