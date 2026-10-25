//! 事件总线
//!
//! # 四个部件
//!
//! ```text
//! EventDirectory      编译期常量：事件名 → 形态、是否写入日志、挂载点
//! SubscriptionIndex   运行期：事件名 → 排好序的订阅者，随 mod 启停重建
//! Dispatcher          一次派发的执行者
//! BuiltinObserver     常驻观察者，事件产生时收到整份信封
//! ```

pub mod context;
pub mod def;
pub mod dispatch;
pub mod envelope;
pub mod events;
pub mod guard;
pub mod host;
pub mod observer;
pub mod pool;
pub mod subscription;
pub mod tracking;
