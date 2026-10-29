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
