//! 有界帧队列

use super::frame::Frame;
use crate::conversation::events::CORE_BLOCK_DELTA;
use std::collections::VecDeque;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Channel {
    /// 分片类，后续帧整体替换
    Fragment,
    /// 新值取代旧值
    Stateful,
    /// 其余事件，一条都不能丢
    Journal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Overflow {
    DropOldest,
    Disconnect,
}

pub fn channel_of(name: &str) -> Channel {
    if name == CORE_BLOCK_DELTA.name {
        Channel::Fragment
    } else {
        Channel::Journal
    }
}

#[derive(Debug)]
pub struct FrameQueue {
    frames: VecDeque<Frame>,
    cap: usize,
}

impl FrameQueue {
    pub fn new(cap: usize) -> Self {
        debug_assert!(cap > 0, "require a positive capacity");
        Self {
            frames: VecDeque::with_capacity(cap),
            cap,
        }
    }

    pub fn push(&mut self, frame: Frame, channel: Channel) -> Option<Overflow> {
        if self.frames.len() < self.cap {
            self.frames.push_back(frame);
            return None;
        }
        match channel {
            Channel::Fragment | Channel::Stateful => {
                self.frames.push_back(frame);
                self.frames.pop_front();
                Some(Overflow::DropOldest)
            }
            Channel::Journal => Some(Overflow::Disconnect),
        }
    }

    pub fn pop(&mut self) -> Option<Frame> {
        self.frames.pop_front()
    }

    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    pub fn len(&self) -> usize {
        self.frames.len()
    }
}
