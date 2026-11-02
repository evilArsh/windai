use std::vec::IntoIter;
use wind_wire::{Block, BlockKind, Content, FunctionCall, Message, Role, Usage};

#[derive(Debug, Default)]
pub struct BlockAbsorb {
    blocks: Vec<Block>,
    // 下一块的序号
    seq: i64,
    input_tokens: i32,
    output_tokens: i32,
    created_at: i64,
    finish_reason: String,
}

pub struct Changed<'a> {
    blocks: &'a [Block],
    indices: IntoIter<usize>,
}

impl<'a> Iterator for Changed<'a> {
    type Item = &'a Block;
    fn next(&mut self) -> Option<Self::Item> {
        self.indices.next().map(|index| &self.blocks[index])
    }
}

impl BlockAbsorb {
    pub fn new() -> Self {
        Self::default()
    }

    /// 把一条消息的片段并入它分解出的块，返回本次变更的块
    pub fn absorb(&mut self, partial: Message) -> Changed<'_> {
        let role = partial.role;
        self.input_tokens = self.input_tokens.max(partial.input_tokens);
        self.output_tokens = self.output_tokens.max(partial.output_tokens);
        if self.created_at == 0 {
            self.created_at = partial.created_at;
        }
        let mut changed: Vec<usize> = Vec::new();
        if let Some(reasoning) = partial.reasoning_content
            && !reasoning.is_empty()
        {
            let open = |seq| Block::new_reasoning(role, seq);
            self.extend(&mut changed, BlockKind::Reasoning, open, &reasoning);
        }
        for item in partial.content {
            match item {
                Content::Text { data } => {
                    let open = |seq| Block::new_text(role, seq);
                    self.extend(&mut changed, BlockKind::Text, open, &data);
                }
                Content::Image { data } => {
                    self.open(&mut changed, |seq| Block::new_image(role, seq, data));
                }
                Content::File { data } => {
                    self.open(&mut changed, |seq| Block::new_file(role, seq, data));
                }
                Content::Audio { data } => {
                    self.open(&mut changed, |seq| {
                        Block::new_audio(role, seq, data.format, data.content)
                    });
                }
                Content::FunctionCall { data } => {
                    self.result(&mut changed, role, data.id, data.content);
                }
            }
        }
        for call in partial.tool_calls.into_iter().flatten() {
            self.call(&mut changed, role, call);
        }
        Changed {
            blocks: &self.blocks,
            indices: changed.into_iter(),
        }
    }

    /// 组装成完整 Message
    pub fn compose(&self) -> Message {
        let mut content = Vec::new();
        let mut reasoning = String::new();
        let mut calls = Vec::new();
        for block in &self.blocks {
            match block.kind() {
                BlockKind::Reasoning => {
                    if let Some(text) = block.reasoning() {
                        reasoning.push_str(text);
                    }
                }
                BlockKind::ToolCall => {
                    if let Some(call) = block.tool_call() {
                        calls.push(call);
                    }
                }
                _ => {
                    if let Some(item) = block.content() {
                        content.push(item);
                    }
                }
            }
        }
        Message {
            role: self
                .blocks
                .first()
                .map(Block::role)
                .unwrap_or(Role::Assistant),
            content,
            reasoning_content: (!reasoning.is_empty()).then_some(reasoning),
            created_at: self.created_at,
            input_tokens: self.input_tokens,
            output_tokens: self.output_tokens,
            tool_calls: (!calls.is_empty()).then_some(calls),
        }
    }

    /// 取走块并把序号归零，供下一条消息复用
    pub fn settle(&mut self) -> Vec<Block> {
        let blocks = std::mem::take(&mut self.blocks);
        self.clean();
        blocks
    }

    /// 当前的块，不移出累加器
    pub fn blocks(&self) -> &[Block] {
        &self.blocks
    }

    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    /// 本次模型往返的用量
    pub fn usage(&self) -> Usage {
        Usage {
            input_tokens: self.input_tokens,
            output_tokens: self.output_tokens,
        }
    }

    pub fn finish_reason(&self) -> &str {
        &self.finish_reason
    }

    /// 记录停止原因
    pub fn set_finish_reason(&mut self, reason: impl ToString) {
        self.finish_reason = reason.to_string();
    }

    /// 清空数据
    pub fn clean(&mut self) {
        self.blocks.clear();
        self.seq = 0;
        self.input_tokens = 0;
        self.output_tokens = 0;
        self.created_at = 0;
        self.finish_reason.clear();
    }

    fn open(&mut self, changed: &mut Vec<usize>, open: impl FnOnce(i64) -> Block) {
        let seq = self.seq;
        self.seq += 1;
        let index = self.blocks.len();
        self.blocks.push(open(seq));
        changed.push(index);
    }

    fn extend(
        &mut self,
        changed: &mut Vec<usize>,
        kind: BlockKind,
        open: impl FnOnce(i64) -> Block,
        fragment: &str,
    ) {
        if fragment.is_empty() {
            return;
        }
        let index = match self.blocks.iter().position(|block| block.is(kind)) {
            Some(index) => index,
            None => {
                let seq = self.seq;
                self.seq += 1;
                self.blocks.push(open(seq));
                self.blocks.len() - 1
            }
        };
        self.blocks[index].append_text(fragment);
        changed.push(index);
    }

    fn call(&mut self, changed: &mut Vec<usize>, role: Role, call: FunctionCall) {
        let found = if call.id.is_empty() {
            self.blocks
                .iter()
                .rposition(|block| block.is(BlockKind::ToolCall))
        } else {
            self.blocks.iter().position(|block| {
                block.is(BlockKind::ToolCall) && block.call_id() == Some(call.id.as_str())
            })
        };
        let index = match found {
            Some(index) => index,
            None => {
                let seq = self.seq;
                self.seq += 1;
                self.blocks.push(Block::new_tool_call(
                    role,
                    seq,
                    call.id.clone(),
                    call.name.clone(),
                    None,
                ));
                self.blocks.len() - 1
            }
        };
        let block = &mut self.blocks[index];
        block.fill_call(&call.id, &call.name);
        block.append_arguments(&call.arguments);
        changed.push(index);
    }

    fn result(
        &mut self,
        changed: &mut Vec<usize>,
        role: Role,
        call_id: String,
        content: serde_json::Value,
    ) {
        let seq = self.seq;
        self.seq += 1;
        let index = self.blocks.len();
        self.blocks
            .push(Block::new_tool_result(role, seq, call_id, content));
        changed.push(index);
    }
}
