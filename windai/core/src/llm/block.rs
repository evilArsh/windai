use std::vec::IntoIter;
use wind_wire::{Block, Body, Content, FunctionCall, Message, Role, Usage};

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
            self.extend(
                &mut changed,
                |body| matches!(body, Body::Reasoning { .. }),
                |seq| Block::new_reasoning(role, seq),
                &reasoning,
            );
        }
        for item in partial.content {
            match item {
                Content::Text { data } => {
                    self.extend(
                        &mut changed,
                        |body| matches!(body, Body::Text { .. }),
                        |seq| Block::new_text(role, seq),
                        &data,
                    );
                }
                Content::Image { data } => {
                    if data.is_empty() {
                        continue;
                    }
                    self.open(&mut changed, |seq| Block::new_image(role, seq, data));
                }
                Content::File { data } => {
                    if data.is_empty() {
                        continue;
                    }
                    self.open(&mut changed, |seq| Block::new_file(role, seq, data));
                }
                Content::Audio { data } => {
                    if data.content.is_empty() {
                        continue;
                    }
                    self.extend_audio(
                        &mut changed,
                        role,
                        &data.item_id,
                        &data.format,
                        &data.content,
                    );
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
        let mut message = Message {
            role: self
                .blocks
                .first()
                .map(|block| block.role)
                .unwrap_or(Role::Assistant),
            created_at: self.created_at,
            input_tokens: self.input_tokens,
            output_tokens: self.output_tokens,
            ..Message::default()
        };
        for block in &self.blocks {
            message.push_block(block);
        }
        message
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
        same: impl Fn(&Body) -> bool,
        open: impl FnOnce(i64) -> Block,
        fragment: &str,
    ) {
        if fragment.is_empty() {
            return;
        }
        let index = match self.blocks.last() {
            Some(block) if same(&block.body) => self.blocks.len() - 1,
            _ => {
                let seq = self.seq;
                self.seq += 1;
                self.blocks.push(open(seq));
                self.blocks.len() - 1
            }
        };
        self.blocks[index].append_text(fragment);
        changed.push(index);
    }

    fn extend_audio(
        &mut self,
        changed: &mut Vec<usize>,
        role: Role,
        item_id: &Option<String>,
        format: &str,
        fragment: &str,
    ) {
        let found = match item_id {
            Some(item_id) => self.blocks.iter().position(|block| {
                matches!(&block.body, Body::Audio { item_id: current, .. }
                    if current.as_deref() == Some(item_id.as_str()))
            }),
            None => None,
        };
        let tail = self.blocks.len().checked_sub(1).filter(|&index| {
            let Body::Audio {
                item_id: current, ..
            } = &self.blocks[index].body
            else {
                return false;
            };
            current.is_none() || item_id.is_none() || current == item_id
        });
        let index = match found.or(tail) {
            Some(index) => index,
            None => {
                let seq = self.seq;
                self.seq += 1;
                self.blocks.push(Block::new_audio(
                    role,
                    seq,
                    item_id.clone(),
                    format.to_owned(),
                    String::new(),
                ));
                self.blocks.len() - 1
            }
        };
        self.blocks[index].append_audio(item_id, fragment);
        changed.push(index);
    }

    fn call(&mut self, changed: &mut Vec<usize>, role: Role, call: FunctionCall) {
        let found = if let Some(item_id) = &call.item_id {
            self.blocks.iter().position(|block| {
                matches!(&block.body, Body::ToolCall { item_id: current, .. }
                    if current.as_deref() == Some(item_id.as_str()))
            })
        } else if !call.call_id.is_empty() {
            self.blocks.iter().position(|block| {
                matches!(&block.body, Body::ToolCall { call_id, .. } if call_id == &call.call_id)
            })
        } else {
            self.blocks
                .iter()
                .rposition(|block| matches!(&block.body, Body::ToolCall { .. }))
        };
        let index = match found {
            Some(index) => index,
            None => {
                let seq = self.seq;
                self.seq += 1;
                self.blocks.push(Block::new_tool_call(
                    role,
                    seq,
                    call.item_id.clone(),
                    call.call_id.clone(),
                    call.name.clone(),
                ));
                self.blocks.len() - 1
            }
        };
        let block = &mut self.blocks[index];
        block.fill_call(&call.item_id, &call.call_id, &call.name);
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
