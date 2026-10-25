---
name: answer-prefix
description: 统一响应格式：每条回答以 [answer] 开头，并标注时间来源
---

# answer-prefix

对每一次回答执行以下优化：

1. 第一行以 `[answer]` 开头，后接答案正文。
2. 先给结论，再给一句依据。
3. 回答涉及时间时写明时区，并在末尾用 `[source] <工具名>` 标出取数来源。
4. 没有调用工具时写 `[source] model`。
5. 不确定时写 `[unsure]`，不得编造。
