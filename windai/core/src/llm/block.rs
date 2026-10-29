use std::collections::BTreeMap;
use wind_wire::{Block, BlockKind, Content, FunctionCall, Message, Role};

#[derive(Debug, Default)]
pub struct BlockAbsorb {
    messages: BTreeMap<i64, Vec<Block>>,
    touched: Vec<usize>,
}

impl BlockAbsorb {
    pub fn new() -> Self {
        Self::default()
    }

    /// 把一条消息并入它分解出的块，返回本次变更的块及其在消息内的序号
    pub fn absorb(&mut self, message_id: i64, partial: Message) -> Vec<(i64, Block)> {
        let role = partial.role;
        self.touched.clear();
        if let Some(reasoning) = partial.reasoning_content {
            if let Some(index) = self.extend(
                message_id,
                BlockKind::Reasoning,
                || Block::new_reasoning(role),
                &reasoning,
            ) {
                self.touched.push(index);
            }
        }
        for item in partial.content {
            match item {
                Content::Text { data } => {
                    let open = || Block::new_text(role);
                    if let Some(index) = self.extend(message_id, BlockKind::Text, open, &data) {
                        self.touched.push(index);
                    }
                }
                Content::Image { data } => {
                    let index = self.open(message_id, Block::new_image(role, data));
                    self.touched.push(index);
                }
                Content::File { data } => {
                    let index = self.open(message_id, Block::new_file(role, data));
                    self.touched.push(index);
                }
                Content::Audio { data } => {
                    let index = self.open(
                        message_id,
                        Block::new_audio(role, data.format, data.content),
                    );
                    self.touched.push(index);
                }
                Content::FunctionCall { data } => {
                    let index = self.result(message_id, role, data.id, data.content);
                    self.touched.push(index);
                }
            }
        }
        for call in partial.tool_calls.into_iter().flatten() {
            let index = self.call(message_id, role, call);
            self.touched.push(index);
        }

        let mut touched = std::mem::take(&mut self.touched);
        if touched.len() > 1 {
            touched.sort_unstable();
            touched.dedup();
        }
        let blocks = self.messages.get(&message_id);
        let changed = touched
            .iter()
            .filter_map(|index| {
                blocks
                    .and_then(|blocks| blocks.get(*index))
                    .map(|block| (*index as i64, block.clone()))
            })
            .collect();
        touched.clear();
        self.touched = touched;
        changed
    }

    /// 取出一条消息的块，下标即序号
    pub fn settle(&mut self, message_id: i64) -> Vec<Block> {
        self.messages.remove(&message_id).unwrap_or_default()
    }

    /// 取走全部消息的块，每条消息内仍按序号排列
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
        self.touched.clear();
    }

    fn open(&mut self, message_id: i64, block: Block) -> usize {
        place(self.messages.entry(message_id).or_default(), block)
    }

    fn extend(
        &mut self,
        message_id: i64,
        kind: BlockKind,
        open: impl FnOnce() -> Block,
        fragment: &str,
    ) -> Option<usize> {
        if fragment.is_empty() {
            return None;
        }
        let blocks = self.messages.entry(message_id).or_default();
        let index = match blocks.iter().position(|block| block.is(kind)) {
            Some(index) => index,
            None => place(blocks, open()),
        };
        blocks[index].append_text(fragment);
        Some(index)
    }

    fn call(&mut self, message_id: i64, role: Role, call: FunctionCall) -> usize {
        let blocks = self.messages.entry(message_id).or_default();
        let found = if call.id.is_empty() {
            blocks
                .iter()
                .rposition(|block| block.is(BlockKind::ToolCall))
        } else {
            blocks.iter().position(|block| {
                block.is(BlockKind::ToolCall) && block.call_id() == Some(call.id.as_str())
            })
        };
        let index = match found {
            Some(index) => index,
            None => place(
                blocks,
                Block::new_tool_call(role, call.id.clone(), call.name.clone(), None),
            ),
        };
        let block = &mut blocks[index];
        block.fill_call(&call.id, &call.name);
        block.append_arguments(&call.arguments);
        index
    }

    fn result(
        &mut self,
        message_id: i64,
        role: Role,
        call_id: String,
        content: serde_json::Value,
    ) -> usize {
        let blocks = self.messages.entry(message_id).or_default();
        let found = blocks.iter().position(|block| {
            block.is(BlockKind::ToolResult) && block.call_id() == Some(call_id.as_str())
        });
        let index = match found {
            Some(index) => index,
            None => return place(blocks, Block::new_tool_result(role, call_id, content)),
        };
        blocks[index].set_result(content);
        index
    }
}

/// 按块种类插入，同种类内保持开块顺序
fn place(blocks: &mut Vec<Block>, block: Block) -> usize {
    let rank = rank(block.kind());
    let index = blocks
        .iter()
        .position(|existing| rank_of(existing) > rank)
        .unwrap_or(blocks.len());
    blocks.insert(index, block);
    index
}

/// 块在消息内的种类顺序
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

fn rank_of(block: &Block) -> u8 {
    rank(block.kind())
}
