use super::error::{Error, Result};
use serde_json::{Map, Value};

/// 将路径分割
/// - eg: `"thinking.type"` --> `["thinking", "type"]`
/// TODO: 返回 Vec<&str>
pub fn segments(path: &str) -> Vec<String> {
    path.split('.')
        .collect::<Vec<_>>()
        .iter()
        .map(|s| s.to_string())
        .collect()
}

/// 按路径段从 JSON 中读取值
pub fn get<'a>(root: &'a Value, segs: &[String]) -> Result<&'a Value> {
    let mut cur = root;
    for seg in segs {
        cur = cur
            .as_object()
            .ok_or_else(|| {
                Error::Path(format!(
                    "non-object value in the middle of path {}",
                    segs.join(".")
                ))
            })?
            .get(seg)
            .ok_or_else(|| Error::Path(format!("path {:?} not found", segs)))?;
    }
    Ok(cur)
}

/// 按路径段从 JSON 中读取值
pub fn get_mut<'a>(root: &'a mut Value, segs: &[String]) -> Result<&'a mut Value> {
    let mut cur = root;
    for seg in segs {
        cur = cur
            .as_object_mut()
            .ok_or_else(|| {
                Error::Path(format!(
                    "non-object value in the middle of path {}",
                    segs.join(".")
                ))
            })?
            .get_mut(seg)
            .ok_or_else(|| {
                Error::Path(format!(
                    "non-object value in the middle of path {}",
                    segs.join(".")
                ))
            })?;
    }
    Ok(cur)
}

/// 确保路径存在，返回最终位置的引用。
/// 如果子路径不存在，则创建空对象。
/// 如果子路径中间有非对象值则会被忽略。
pub fn walk<'a>(root: &'a mut Value, segs: &[String]) -> Result<&'a mut Value> {
    let mut cur = root;
    for seg in segs {
        cur = cur
            .as_object_mut()
            .ok_or_else(|| {
                Error::Path(format!(
                    "non-object value in the middle of path {}",
                    segs.join(".")
                ))
            })?
            .entry(seg)
            .or_insert_with(|| Value::Object(Map::new()));
    }
    Ok(cur)
}

/// 删除路径指向的值，如果父对象不存在则报错
pub fn remove(root: &mut Value, segs: &[String]) -> Result<()> {
    if segs.is_empty() {
        return Err(Error::Path("empty path".into()));
    }
    let (parent_segs, key) = segs.split_at(segs.len() - 1);
    let parent = get_mut(root, parent_segs)?;
    match parent.as_object_mut() {
        Some(obj) => {
            obj.remove(&key[0]);
            Ok(())
        }
        None => Err(Error::Path(format!(
            "non-object value in the middle of path {}",
            segs.join(".")
        ))),
    }
}
