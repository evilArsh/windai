use super::error::{Error, Result};
use serde_json::{Map, Value};

/// TODO: 改成返回 `Vec<&str>`，省掉每段的字符串分配。
pub fn segments(path: &str) -> Vec<String> {
    path.split('.')
        .collect::<Vec<_>>()
        .iter()
        .map(|s| s.to_string())
        .collect()
}

pub fn get<'a>(root: &'a Value, segs: &[String]) -> Result<Option<&'a Value>> {
    if segs.is_empty() {
        return Err(Error::Path("empty path".into()));
    }
    let (parent_segs, key) = segs.split_at(segs.len() - 1);
    let mut cur = root;
    for seg in parent_segs {
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
    let obj = cur.as_object().ok_or_else(|| {
        Error::Path(format!(
            "non-object value at the end of path {}",
            segs.join(".")
        ))
    })?;
    Ok(obj.get(&key[0]))
}

pub fn get_mut<'a>(root: &'a mut Value, segs: &[String]) -> Result<Option<&'a mut Value>> {
    if segs.is_empty() {
        return Err(Error::Path("empty path".into()));
    }
    let (parent_segs, key) = segs.split_at(segs.len() - 1);
    let mut cur = root;
    for seg in parent_segs {
        cur = cur
            .as_object_mut()
            .ok_or_else(|| {
                Error::Path(format!(
                    "non-object value in the middle of path {}",
                    segs.join(".")
                ))
            })?
            .get_mut(seg)
            .ok_or_else(|| Error::Path(format!("path {:?} not found", segs)))?;
    }
    let obj = cur.as_object_mut().ok_or_else(|| {
        Error::Path(format!(
            "non-object value at the end of path {}",
            segs.join(".")
        ))
    })?;
    Ok(obj.get_mut(&key[0]))
}

pub fn remove(root: &mut Value, segs: &[String]) -> Result<()> {
    if segs.is_empty() {
        return Err(Error::Path("empty path".into()));
    }
    let (parent_segs, key) = segs.split_at(segs.len() - 1);
    let mut cur = root;
    for seg in parent_segs {
        cur = cur
            .as_object_mut()
            .ok_or_else(|| {
                Error::Path(format!(
                    "non-object value in the middle of path {}",
                    segs.join(".")
                ))
            })?
            .get_mut(seg)
            .ok_or_else(|| Error::Path(format!("path {:?} not found", segs)))?;
    }
    let obj = cur.as_object_mut().ok_or_else(|| {
        Error::Path(format!(
            "non-object value at the end of path {}",
            segs.join(".")
        ))
    })?;
    obj.remove(&key[0]);
    Ok(())
}

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
