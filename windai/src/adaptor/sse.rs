//! HTTP SSE（Server-Sent Events）数据解析器
//!
//! 参考：https://developer.mozilla.org/zh-CN/docs/Web/API/Server-sent_events/Using_server-sent_events
//! 参考：https://html.spec.whatwg.org/multipage/server-sent-events.html#dispatchMessage

use bytes::Bytes;

/// 解析后的单个 SSE 事件
///
/// 五个字段对应 SSE 协议的五个字段前缀，并非所有字段都会同时出现。
#[derive(Debug, Clone, Default)]
pub struct SSEBlock {
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
impl SSEBlock {
    /// 解析原始 SSE bytes，返回所有解析的结果
    ///
    /// 输入可能包含多个拼接的事件，以空行分隔。
    pub fn parse_all(input: Bytes) -> Vec<Self> {
        let text = String::from_utf8_lossy(&input);
        let mut events = Vec::new();
        let mut current = SSEBlock::default();
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
