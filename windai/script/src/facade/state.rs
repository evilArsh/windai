use super::{FacadeCall, FacadeError, HostApi, throw};
use rquickjs::{Array, Ctx, Function, Object, Result as JsResult, Value};
use std::sync::Arc;

/// 一个键值接口读写的是哪一边
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    /// 对外状态，写入会广播
    State,
    /// 私有存储，不广播
    Storage,
}

impl Side {
    fn read(self, host: &Arc<dyn HostApi>, key: &str) -> Option<serde_json::Value> {
        match self {
            Side::State => host.state_read(key),
            Side::Storage => host.storage_read(key),
        }
    }

    /// 对外状态会拒绝超过上限的值，私有存储不设上限
    fn write(
        self,
        host: &Arc<dyn HostApi>,
        key: &str,
        value: serde_json::Value,
    ) -> Result<(), FacadeError> {
        match self {
            Side::State => host.state_write(key, value),
            Side::Storage => {
                host.storage_write(key, value);
                Ok(())
            }
        }
    }

    fn delete(self, host: &Arc<dyn HostApi>, key: &str) {
        match self {
            Side::State => host.state_delete(key),
            Side::Storage => host.storage_delete(key),
        }
    }

    fn keys(self, host: &Arc<dyn HostApi>) -> Vec<String> {
        match self {
            Side::State => host.state_keys(),
            Side::Storage => host.storage_keys(),
        }
    }
}

pub fn build<'js>(ctx: &Ctx<'js>, call: &FacadeCall, private: bool) -> JsResult<Object<'js>> {
    let side = if private { Side::Storage } else { Side::State };
    let object = Object::new(ctx.clone())?;

    let read_host = call.host.clone();
    let get = Function::new(
        ctx.clone(),
        move |ctx: Ctx<'js>, key: String| -> JsResult<Option<Value<'js>>> {
            let Some(value) = side.read(&read_host, &key) else {
                return Ok(None);
            };
            let text = serde_json::to_string(&value)
                .map_err(|error| FacadeError::Failed(error.to_string()))
                .map_err(|error| throw(&ctx, error))?;
            Ok(Some(ctx.json_parse(text)?))
        },
    )?;

    let write_host = call.host.clone();
    let set = Function::new(
        ctx.clone(),
        move |ctx: Ctx<'js>, key: String, value: Value<'js>| -> JsResult<()> {
            let text = ctx
                .json_stringify(value)?
                .ok_or_else(|| FacadeError::Failed(format!("{key} is not serializable")))
                .map_err(|error| throw(&ctx, error))?;
            let text = text.to_string()?;
            let decoded: serde_json::Value = serde_json::from_str(&text)
                .map_err(|error| FacadeError::Failed(error.to_string()))
                .map_err(|error| throw(&ctx, error))?;
            side.write(&write_host, &key, decoded)
                .map_err(|error| throw(&ctx, error))
        },
    )?;

    let delete_host = call.host.clone();
    let delete = Function::new(ctx.clone(), move |key: String| -> JsResult<()> {
        side.delete(&delete_host, &key);
        Ok(())
    })?;

    let keys_host = call.host.clone();
    let keys = Function::new(ctx.clone(), move |ctx: Ctx<'js>| -> JsResult<Array<'js>> {
        let array = Array::new(ctx)?;
        for (index, key) in side.keys(&keys_host).into_iter().enumerate() {
            array.set(index, key)?;
        }
        Ok(array)
    })?;

    object.prop("get", get)?;
    object.prop("set", set)?;
    object.prop("delete", delete)?;
    object.prop("keys", keys)?;
    Ok(object)
}
