use serde::{Deserialize, Serialize};

pub trait Domain: 'static + Clone {
    type Spec: Serialize + for<'de> Deserialize<'de> + Clone + std::fmt::Debug + Send + Sync;
    type Suspend: Serialize
        + for<'de> Deserialize<'de>
        + Clone
        + PartialEq
        + std::fmt::Debug
        + Send
        + Sync;
    type Resume: std::fmt::Debug + Send + Sync;
    type Output: Serialize + for<'de> Deserialize<'de> + Clone + std::fmt::Debug + Send + Sync;
    type Error: Serialize + for<'de> Deserialize<'de> + Clone + std::fmt::Debug + Send + Sync;
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::Domain;

    /// 脚本化的节点启动参数
    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    pub enum TestSpec {
        /// 立刻完成
        Noop,
        /// 派生给定数量的子节点
        Children(usize),
        /// 立刻挂起
        Suspend,
        /// 立刻失败
        Fail,
    }

    /// 测试用的挂起标记
    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    pub enum TestSuspend {
        /// 主挂起标记
        Ask,
        /// 与 Ask 相异的挂起标记，用于构造不匹配的回显
        Other,
    }

    /// 测试用的外部输入
    #[derive(Debug, Clone, PartialEq)]
    pub enum TestResume {
        /// 对挂起的应答
        Answer,
    }

    /// 测试用的节点产出
    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    pub enum TestOutput {
        /// 成功产出
        Ok,
    }

    /// 测试用的领域错误
    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    pub enum TestError {
        /// 一个失败
        Boom,
    }

    /// 测试用的领域标签
    #[derive(Debug, Clone)]
    pub struct TestDomain;

    impl Domain for TestDomain {
        type Spec = TestSpec;
        type Suspend = TestSuspend;
        type Resume = TestResume;
        type Output = TestOutput;
        type Error = TestError;
    }
}
