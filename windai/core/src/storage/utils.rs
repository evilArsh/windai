use crate::error::{CoreError, Result};
use serde::{Serialize, de::DeserializeOwned};

/// 将vec序列化为json字符串。
/// 如果vec为None，则返回"[]"。
pub fn vec_to_str<T>(vec: Option<&[T]>) -> Result<String>
where
    T: Serialize,
{
    match vec {
        Some(v) => serde_json::to_string(v).map_err(Into::into),
        None => Ok("[]".to_string()),
    }
}

/// 将map序列化为json字符串。
/// 如果map为None，则返回"{}"。
pub fn map_to_str<T>(map: Option<&T>) -> Result<String>
where
    T: Serialize,
{
    match map {
        Some(m) => serde_json::to_string(m).map_err(Into::into),
        None => Ok("{}".to_string()),
    }
}

/// 将json字符串反序列化为 T。
pub fn de_str_to<T>(s: &str) -> Result<T>
where
    T: DeserializeOwned,
{
    serde_json::from_str(s).map_err(Into::into)
}
/// 将字符串转换为指定类型
pub fn parse_str_to<T>(value: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: Into<CoreError>,
{
    value.parse::<T>().map_err(Into::into)
}
