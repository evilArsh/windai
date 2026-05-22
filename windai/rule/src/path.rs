use serde_json::{Map, Value};

/// 将路径分割
/// - eg: `"thinking.type"` --> `["thinking", "type"]`
pub fn segments(path: &str) -> Vec<String> {
    path.split('.')
        .collect::<Vec<_>>()
        .iter()
        .map(|s| s.to_string())
        .collect()
}

// OK
/// 按路径段从 JSON 中读取值
pub fn get<'a>(root: &'a Value, segs: &[String]) -> Option<&'a Value> {
    let mut cur = root;
    for seg in segs {
        cur = cur.as_object()?.get(seg)?;
    }
    Some(cur)
}

// OK
/// 按路径段从 JSON 中读取值
pub fn get_mut<'a>(root: &'a mut Value, segs: &[String]) -> Option<&'a mut Value> {
    let mut cur = root;
    for seg in segs {
        cur = cur.as_object_mut()?.get_mut(seg)?;
    }
    Some(cur)
}

// OK
/// 确保路径存在，返回最终位置的引用。
/// 如果子路径不存在，则创建空对象。
/// 如果子路径中间有非对象值则会被忽略。
pub fn walk<'a>(root: &'a mut Value, segs: &[String]) -> Option<&'a mut Value> {
    let mut cur = root;
    for seg in segs {
        cur = cur
            .as_object_mut()?
            .entry(seg)
            .or_insert_with(|| Value::Object(Map::new()));
    }
    Some(cur)
}

// OK
/// 删除路径指向的值，如果父对象不存在则无操作
pub fn remove(root: &mut Value, segs: &[String]) {
    if segs.is_empty() {
        return;
    }
    let (parent_segs, key) = segs.split_at(segs.len() - 1);
    if let Some(parent) = get_mut(root, parent_segs) {
        if let Some(obj) = parent.as_object_mut() {
            obj.remove(&key[0]);
        }
    }
}
