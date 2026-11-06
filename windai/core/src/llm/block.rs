use wind_wire::{Block, Body, Content, Message, Role, Usage};

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

pub struct Changed {
    blocks: Vec<Block>,
}

impl IntoIterator for Changed {
    type Item = Block;
    type IntoIter = std::vec::IntoIter<Block>;

    fn into_iter(self) -> Self::IntoIter {
        self.blocks.into_iter()
    }
}

impl BlockAbsorb {
    pub fn new() -> Self {
        Self::default()
    }

    /// 拆解一条消息的片段并入累加块，返回本次的增量块
    pub fn absorb(&mut self, partial: Message) -> Changed {
        let role = partial.role;
        self.input_tokens = self.input_tokens.max(partial.input_tokens);
        self.output_tokens = self.output_tokens.max(partial.output_tokens);
        if self.created_at == 0 {
            self.created_at = partial.created_at;
        }
        Changed {
            blocks: split(partial)
                .into_iter()
                .map(|body| self.merge(role, body))
                .collect(),
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

    fn merge(&mut self, role: Role, body: Body) -> Block {
        let index = self.place(role, &body);
        self.blocks[index].absorb_body(&body);
        let target = &self.blocks[index];
        Block {
            role: target.role,
            seq: target.seq,
            revision: target.revision,
            body,
        }
    }

    fn place(&mut self, role: Role, body: &Body) -> usize {
        let found = match body {
            Body::Reasoning { .. } => self.tail(|body| matches!(body, Body::Reasoning { .. })),
            Body::Text { .. } => self.tail(|body| matches!(body, Body::Text { .. })),
            Body::Audio { item_id, .. } => self.audio_block(item_id),
            Body::ToolCall {
                item_id, call_id, ..
            } => self.call_block(item_id, call_id),
            Body::Image { .. } | Body::File { .. } | Body::ToolResult { .. } => None,
        };
        match found {
            Some(index) => index,
            None => self.open(role, body),
        }
    }

    fn open(&mut self, role: Role, body: &Body) -> usize {
        let seq = self.seq;
        self.seq += 1;
        let block = match body {
            Body::Reasoning { .. } => Block::new_reasoning(role, seq),
            Body::Text { .. } => Block::new_text(role, seq),
            Body::Audio {
                item_id, format, ..
            } => Block::new_audio(role, seq, item_id.clone(), format.clone(), String::new()),
            Body::ToolCall {
                item_id,
                call_id,
                call_name,
                ..
            } => Block::new_tool_call(
                role,
                seq,
                item_id.clone(),
                call_id.clone(),
                call_name.clone(),
            ),
            Body::Image { content } => Block::new_image(role, seq, content.clone()),
            Body::File { content } => Block::new_file(role, seq, content.clone()),
            Body::ToolResult { call_id, call_res } => {
                Block::new_tool_result(role, seq, call_id.clone(), call_res.clone())
            }
        };
        let index = self.blocks.len();
        self.blocks.push(block);
        index
    }

    fn tail(&self, same: impl Fn(&Body) -> bool) -> Option<usize> {
        let index = self.blocks.len().checked_sub(1)?;
        same(&self.blocks[index].body).then_some(index)
    }

    fn audio_block(&self, item_id: &Option<String>) -> Option<usize> {
        let found = item_id.as_ref().and_then(|item_id| {
            self.blocks.iter().position(|block| {
                matches!(&block.body, Body::Audio { item_id: current, .. }
                    if current.as_deref() == Some(item_id.as_str()))
            })
        });
        found.or_else(|| {
            self.tail(|body| match body {
                Body::Audio {
                    item_id: current, ..
                } => current.is_none() || item_id.is_none() || current == item_id,
                _ => false,
            })
        })
    }

    fn call_block(&self, item_id: &Option<String>, call_id: &str) -> Option<usize> {
        if let Some(item_id) = item_id {
            self.blocks.iter().position(|block| {
                matches!(&block.body, Body::ToolCall { item_id: current, .. }
                    if current.as_deref() == Some(item_id.as_str()))
            })
        } else if !call_id.is_empty() {
            self.blocks.iter().position(|block| {
                matches!(&block.body, Body::ToolCall { call_id: current, .. } if current == call_id)
            })
        } else {
            self.blocks
                .iter()
                .rposition(|block| matches!(&block.body, Body::ToolCall { .. }))
        }
    }
}

fn split(partial: Message) -> Vec<Body> {
    let mut bodies = Vec::new();
    let reasoning_item_id = partial.reasoning_item_id;
    if let Some(reasoning) = partial.reasoning_content
        && !reasoning.is_empty()
    {
        bodies.push(Body::Reasoning {
            item_id: reasoning_item_id,
            content: reasoning,
        });
    }
    for item in partial.content {
        match item {
            Content::Text { data } => {
                if !data.is_empty() {
                    bodies.push(Body::Text { content: data });
                }
            }
            Content::Image { data } => {
                if !data.is_empty() {
                    bodies.push(Body::Image { content: data });
                }
            }
            Content::File { data } => {
                if !data.is_empty() {
                    bodies.push(Body::File { content: data });
                }
            }
            Content::Audio { data } => {
                if !data.content.is_empty() {
                    bodies.push(Body::Audio {
                        item_id: data.item_id,
                        format: data.format,
                        content: data.content,
                    });
                }
            }
            Content::FunctionCall { data } => bodies.push(Body::ToolResult {
                call_id: data.id,
                call_res: data.content,
            }),
        }
    }
    for call in partial.tool_calls.into_iter().flatten() {
        bodies.push(Body::ToolCall {
            item_id: call.item_id,
            call_id: call.call_id,
            call_name: call.name,
            call_arguments: call.arguments,
        });
    }
    bodies
}
