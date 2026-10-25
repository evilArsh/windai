//! 前端事件流上的帧

use serde::{Deserialize, Serialize};
use wind_stream::block::Block;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Frame {
    Snapshot { baseline: Baseline },
    Event { envelope: serde_json::Value },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "baseline", rename_all = "snake_case")]
pub enum Baseline {
    Blocks {
        blocks: Vec<Block>,
    },
    Stateful {
        key: String,
        value: serde_json::Value,
    },
}
