//! HTTP SSE（Server-Sent Events）数据解析器
//!
//! 参考：https://developer.mozilla.org/zh-CN/docs/Web/API/Server-sent_events/Using_server-sent_events
//! 参考：https://html.spec.whatwg.org/multipage/server-sent-events.html#dispatchMessage

use bytes::Bytes;
// /// 有状态的 SSE 解析器，维护内部缓冲区以处理跨 chunk 的不完整事件。
// ///
// /// SSE 协议以 `\n\n` 分隔事件。当流式数据在事件中间被拆分时，
// /// 缓冲区会保存不完整的数据，等待下一个 chunk 到达后拼接再解析。
// #[derive(Debug, Default)]
// pub struct SseParser {
//     buffer: BytesMut,
// }

// impl SseParser {
//     pub fn new() -> Self {
//         Self { buffer: BytesMut::with_capacity(256) }
//     }

//     /// 返回所有可解析的完整 `SseBlock`。
//     /// - 内部缓冲区会保留不完整的尾部数据，等待下一次调用时拼接。
//     pub fn parse(&mut self, input: Bytes) -> Vec<SseBlock> {
//         self.buffer.extend_from_slice(input);

//         let boundary = find_last_event_boundary(&self.buffer);
//         if boundary == 0 {
//             return Vec::new();
//         }

//         let complete = Bytes::copy_from_slice(&self.buffer[..boundary]);
//         self.buffer.drain(..boundary);

//         SseBlock::parse_all(complete)
//     }

//     /// 流结束时调用，清空缓冲区并尝试解析剩余数据。
//     pub fn flush(&mut self) -> Vec<SseBlock> {
//         if self.buffer.is_empty() {
//             return Vec::new();
//         }
//         let remaining = std::mem::take(&mut self.buffer);
//         SseBlock::parse_all(Bytes::from(remaining))
//     }
// }

// /// 找到缓冲区中最后一个完整 SSE 事件的结束位置（最后一个 `\n\n` 之后）。
// /// 返回 0 表示没有完整事件。
// fn find_last_event_boundary(buffer: &[u8]) -> usize {
//     buffer
//         .windows(2)
//         .enumerate()
//         .rev()
//         .find(|(_, w)| w[0] == b'\n' && w[1] == b'\n')
//         .map(|(i, _)| i + 2)
//         .unwrap_or(0)
// }

/// 解析后的单个 SSE 事件
///
/// 五个字段对应 SSE 协议的五个字段前缀，并非所有字段都会同时出现。
#[derive(Debug, Clone, Default)]
pub struct SseBlock {
    /// data: 消息数据，多行连续data: 会以\n拼接
    pub data: Option<String>,
    /// event: 事件类型
    pub event: Option<String>,
    /// id: 事件唯一标识
    pub id: Option<String>,
    /// retry: 重连间隔（毫秒）
    pub retry: Option<u64>,
    /// comment: 以 : 开头的注释行
    pub comment: Option<String>,
}
impl SseBlock {
    /// 解析原始 SSE bytes，返回所有解析的结果
    /// - 输入可能包含多个拼接的事件，以空行分隔。
    pub fn parse_all(input: Bytes) -> Vec<Self> {
        let text = String::from_utf8_lossy(&input);
        log::debug!("[sse block raw] {:?}", text);
        let mut events = Vec::new();
        let mut current = SseBlock::default();
        for line in text.lines() {
            if line.is_empty() {
                // 空行表示当前事件结束
                if current.has_data() || current.has_event() {
                    events.push(std::mem::take(&mut current));
                }
                continue;
            }
            // 以 : 开头的行 为注释
            if line.starts_with(':') {
                current.append_comment(line);
                continue;
            }
            let Some((key, value)) = line.split_once(':') else {
                continue;
            };
            let value = value.strip_prefix(' ').unwrap_or(value);
            match key {
                "data" => current.append_data(value),
                "event" => current.event = Some(value.to_string()),
                "id" => current.id = Some(value.to_string()),
                "retry" => {
                    if let Ok(ms) = value.parse::<u64>() {
                        current.retry = Some(ms);
                    }
                }
                _ => {}
            }
        }
        if current.has_data() || current.has_event() {
            events.push(current);
        }
        events
    }

    /// 解析原始 SSE bytes。
    /// 如果包含多个块，则只返回第一个解析结果
    pub fn parse(input: Bytes) -> Option<Self> {
        Self::parse_all(input).into_iter().next()
    }

    fn has_data(&self) -> bool {
        self.data.is_some()
    }
    fn has_event(&self) -> bool {
        self.event.is_some()
    }
    /// 追加数据,如果已经有数据，会先追加一个\n，最终的字符串末尾不会添加\n
    pub fn append_data(&mut self, value: &str) {
        log::debug!("[sse data block] {:?}", value);
        if value == "[DONE]" {
            return;
        }

        let buf = self.data.get_or_insert_with(String::new);
        if !buf.is_empty() {
            buf.push('\n');
        }
        buf.push_str(value);
    }

    pub fn append_comment(&mut self, line: &str) {
        let buf = self.comment.get_or_insert_with(String::new);
        if !buf.is_empty() {
            buf.push('\n');
        }
        buf.push_str(line);
    }
}
