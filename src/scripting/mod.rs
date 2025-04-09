use crate::scripting::cass_error::CassError;
use crate::scripting::context::Context;
use rune::{ContextError, Module};
use rust_embed::RustEmbed;
use std::collections::HashMap;

pub mod bind;
pub mod cass_error;
pub mod connect;
pub mod context;
pub mod cql_types;
pub mod executor;
mod functions;

#[derive(RustEmbed)]
#[folder = "resources/"]
struct Resources;

pub fn install(rune_ctx: &mut rune::Context, params: HashMap<String, String>) {
    try_install(rune_ctx, params).unwrap()
}

fn try_install(
    rune_ctx: &mut rune::Context,
    params: HashMap<String, String>,
) -> Result<(), ContextError> {
    let mut context_module = Module::default();
    context_module.ty::<Context>()?;
    context_module.function_meta(functions::execute)?;
    context_module.function_meta(functions::prepare)?;
    context_module.function_meta(functions::execute_prepared)?;
    context_module.function_meta(functions::query_prepared)?;
    context_module.function_meta(functions::get)?;
    context_module.function_meta(functions::put)?;
    context_module.function_meta(functions::elapsed_secs)?;

    let mut err_module = Module::default();
    err_module.ty::<CassError>()?;
    err_module.function_meta(CassError::string_display)?;

    let mut uuid_module = Module::default();
    uuid_module.ty::<cql_types::Uuid>()?;
    uuid_module.function_meta(cql_types::Uuid::string_display)?;

    let mut latte_module = Module::with_crate("latte")?;
    latte_module.macro_("param", move |ctx, ts| functions::param(ctx, &params, ts))?;

    latte_module.function_meta(functions::blob)?;
    latte_module.function_meta(functions::text)?;
    latte_module.function_meta(functions::vector)?;
    latte_module.function_meta(functions::join)?;
    latte_module.function_meta(functions::now_timestamp)?;
    latte_module.function_meta(functions::hash)?;
    latte_module.function_meta(functions::hash2)?;
    latte_module.function_meta(functions::hash_range)?;
    latte_module.function_meta(functions::hash_select)?;
    latte_module.function_meta(functions::uuid)?;
    latte_module.function_meta(functions::normal)?;
    latte_module.function_meta(functions::uniform)?;

    latte_module.function_meta(cql_types::i64::to_i32)?;
    latte_module.function_meta(cql_types::i64::to_i16)?;
    latte_module.function_meta(cql_types::i64::to_i8)?;
    latte_module.function_meta(cql_types::i64::to_f32)?;
    latte_module.function_meta(cql_types::i64::clamp)?;

    latte_module.function_meta(cql_types::f64::to_i8)?;
    latte_module.function_meta(cql_types::f64::to_i16)?;
    latte_module.function_meta(cql_types::f64::to_i32)?;
    latte_module.function_meta(cql_types::f64::to_f32)?;
    latte_module.function_meta(cql_types::f64::clamp)?;

    let mut fs_module = Module::with_crate("fs")?;
    fs_module.function_meta(functions::read_to_string)?;
    fs_module.function_meta(functions::read_lines)?;
    fs_module.function_meta(functions::read_words)?;
    fs_module.function_meta(functions::read_resource_to_string)?;
    fs_module.function_meta(functions::read_resource_lines)?;
    fs_module.function_meta(functions::read_resource_words)?;

    rune_ctx.install(&context_module)?;
    rune_ctx.install(&err_module)?;
    rune_ctx.install(&uuid_module)?;
    rune_ctx.install(&latte_module)?;
    rune_ctx.install(&fs_module)?;

    Ok(())
}
