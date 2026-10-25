use super::{FacadeCall, throw};
use rquickjs::function::Opt;
use rquickjs::{Array, Ctx, Function, Object, Result as JsResult};

fn strings<'js>(ctx: &Ctx<'js>, values: Vec<String>) -> JsResult<Array<'js>> {
    let array = Array::new(ctx.clone())?;
    for (index, value) in values.into_iter().enumerate() {
        array.set(index, value)?;
    }
    Ok(array)
}

pub fn build<'js>(ctx: &Ctx<'js>, call: &FacadeCall) -> JsResult<Object<'js>> {
    let object = Object::new(ctx.clone())?;
    object.prop(
        "dir",
        call.host
            .fs_dir(&call.identity.name)
            .to_string_lossy()
            .to_string(),
    )?;

    let read_host = call.host.clone();
    let read_name = call.identity.name.clone();
    let read = Function::new(
        ctx.clone(),
        move |ctx: Ctx<'js>, path: String| -> JsResult<String> {
            read_host
                .fs_read(&read_name, &path)
                .map_err(|error| throw(&ctx, error))
        },
    )?;

    let write_host = call.host.clone();
    let write_name = call.identity.name.clone();
    let write = Function::new(
        ctx.clone(),
        move |ctx: Ctx<'js>, path: String, data: String| -> JsResult<()> {
            write_host
                .fs_write(&write_name, &path, &data)
                .map_err(|error| throw(&ctx, error))
        },
    )?;

    let list_host = call.host.clone();
    let list_name = call.identity.name.clone();
    let list = Function::new(
        ctx.clone(),
        move |ctx: Ctx<'js>, path: Opt<Option<String>>| -> JsResult<Array<'js>> {
            let path = path.into_inner().flatten();
            let entries = list_host
                .fs_list(&list_name, path.as_deref().unwrap_or(""))
                .map_err(|error| throw(&ctx, error))?;
            strings(&ctx, entries)
        },
    )?;

    let remove_host = call.host.clone();
    let remove_name = call.identity.name.clone();
    let remove = Function::new(
        ctx.clone(),
        move |ctx: Ctx<'js>, path: String| -> JsResult<()> {
            remove_host
                .fs_remove(&remove_name, &path)
                .map_err(|error| throw(&ctx, error))
        },
    )?;

    object.prop("readText", read)?;
    object.prop("writeText", write)?;
    object.prop("list", list)?;
    object.prop("remove", remove)?;
    Ok(object)
}
