//! QuickJS 上的 mod 宿主
//!
//! 一次调用：
//!
//! ```text
//! 建运行时      每次调用新建，结束即销毁
//! 装 ctx        按声明与调用模式裁剪，未声明的属性不存在
//! 建加载器      相对说明符解析进该版本目录，模块取自安装期编译的字节码
//! 调处理函数    清单指认的那个导出
//! 读返回值      转成改写后的载荷，或什么都不返回
//! ```
//!
//! 解释器跑在自己的标准线程上，不在任何 tokio 运行时里

pub mod assembly;
pub mod bridge;
pub mod bytecode;
pub mod circuit;
pub mod deployment;
pub mod events;
pub mod facade;
pub mod loader;
pub mod request;
pub mod runtime;
pub mod threads;

pub use deployment::{DeployError, Deployment, NotDeployed};
