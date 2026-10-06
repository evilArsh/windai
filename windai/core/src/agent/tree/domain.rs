use serde::{Deserialize, Serialize};

/// 领域载荷定义
pub trait Domain: 'static + Clone {
    /// 节点启动参数
    type Spec: Serialize + for<'de> Deserialize<'de> + Clone + std::fmt::Debug + Send + Sync;
    /// 挂起标记，外部输入需原样回显
    type Suspend: Serialize
        + for<'de> Deserialize<'de>
        + Clone
        + PartialEq
        + std::fmt::Debug
        + Send
        + Sync;
    /// 外部输入
    type Resume: std::fmt::Debug + Send + Sync;
    /// 节点产出
    type Output: Serialize + for<'de> Deserialize<'de> + Clone + std::fmt::Debug + Send + Sync;
    /// 领域错误
    type Error: Serialize + for<'de> Deserialize<'de> + Clone + std::fmt::Debug + Send + Sync;
    /// 进度增量
    type Progress: std::fmt::Debug + Send + Sync + Clone;
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::Domain;

    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    pub enum TestSpec {
        Noop,
        Children(usize),
        Suspend,
        Fail,
    }

    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    pub enum TestSuspend {
        Ask,
        /// 与 Ask 相异的挂起标记，用于构造不匹配的回显
        Other,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub enum TestResume {
        Answer,
    }

    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    pub enum TestOutput {
        Ok,
    }

    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    pub enum TestError {
        Boom,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub enum TestProgress {
        Tick,
        /// 取消之后才发的分片，用于区分门控前后的两片
        Tock,
    }

    #[derive(Clone)]
    pub struct TestDomain;

    impl Domain for TestDomain {
        type Spec = TestSpec;
        type Suspend = TestSuspend;
        type Resume = TestResume;
        type Output = TestOutput;
        type Error = TestError;
        type Progress = TestProgress;
    }
}
