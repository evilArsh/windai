use wind_wire::{Block, BlockKind, Content, FunctionCall, Message, Role};

#[derive(Debug, Default)]
pub struct BlockAbsorb {
    blocks: Vec<Block>,
    // 下一块的序号
    seq: i64,
}

impl BlockAbsorb {
    pub fn new() -> Self {
        Self::default()
    }

    /// 把一条消息的片段并入它分解出的块，返回本次变更的块
    pub fn absorb(&mut self, partial: Message) -> Vec<Block> {
        let role = partial.role;
        let mut changed: Vec<Block> = Vec::new();
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
        changed
    }

    /// 组装成完整 Message
    pub fn compose(&mut self) -> Message {
        todo!()
    }

    /// 取走块并把序号归零，供下一条消息复用
    pub fn settle(&mut self) -> Vec<Block> {
        self.seq = 0;
        std::mem::take(&mut self.blocks)
    }

    /// 当前的块，不移出累加器
    pub fn blocks(&self) -> &[Block] {
        &self.blocks
    }

    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    /// 清空数据
    pub fn clean(&mut self) {
        self.blocks.clear();
        self.seq = 0;
    }

    fn open(&mut self, changed: &mut Vec<Block>, open: impl FnOnce(i64) -> Block) {
        let seq = self.seq;
        self.seq += 1;
        let block = open(seq);
        changed.push(block.clone());
        self.blocks.push(block);
    }

    fn extend(
        &mut self,
        changed: &mut Vec<Block>,
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
        let block = &mut self.blocks[index];
        block.append_text(fragment);
        changed.push(block.clone());
    }

    fn call(&mut self, changed: &mut Vec<Block>, role: Role, call: FunctionCall) {
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
        changed.push(block.clone());
    }

    fn result(
        &mut self,
        changed: &mut Vec<Block>,
        role: Role,
        call_id: String,
        content: serde_json::Value,
    ) {
        let seq = self.seq;
        self.seq += 1;
        let block = Block::new_tool_result(role, seq, call_id, content);
        changed.push(block.clone());
        self.blocks.push(block);
    }
}
