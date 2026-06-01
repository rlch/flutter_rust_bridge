use crate::codegen::ir::hir::flat::constant::HirFlatConstant;
use crate::codegen::ir::hir::flat::function::HirFlatFunction;
use crate::codegen::ir::hir::flat::struct_or_enum::HirFlatStruct;
use crate::codegen::ir::mir::func::{MirFunc, MirFuncMode};
use crate::codegen::ir::misc::skip::{IrSkip, IrValueOrSkip};
use crate::codegen::parser::mir::internal_config::ParserMirInternalConfig;
use crate::codegen::parser::mir::parser::ty::TypeParser;
use crate::codegen::parser::mir::ParseMode;
use anyhow::bail;
use itertools::{concat, Itertools};
use std::collections::HashMap;

pub(crate) mod auto_accessor;
pub(crate) mod const_getter;
pub(crate) mod real;
pub(crate) mod ui_related;

pub(crate) fn parse(
    config: &ParserMirInternalConfig,
    src_fns: &[HirFlatFunction],
    src_constants: &[HirFlatConstant],
    type_parser: &mut TypeParser,
    src_structs: &HashMap<String, &HirFlatStruct>,
    parse_mode: ParseMode,
) -> anyhow::Result<(Vec<MirFunc>, Vec<IrSkip>)> {
    let items = concat([
        real::parse(src_fns, type_parser, config, parse_mode)?,
        auto_accessor::parse(config, src_structs, type_parser, parse_mode)?,
        const_getter::parse(config, src_constants, type_parser, parse_mode)?,
    ]);
    let (funcs, skips) = IrValueOrSkip::split(items);
    let funcs = sort_and_add_func_id(funcs);
    validate_frb_categories(&funcs)?;
    Ok((funcs, skips))
}

/// Hybrid enforcement for `#[frb(category = ...)]` (the executor routing lane).
///
/// A function that may touch live CRDT/session state — it runs on the main
/// thread via `sync`, OR carries a session-handle argument — MUST declare a
/// category so the custom executor routes it to the right worker. Pure getters
/// may omit it and default to `Category::Main`.
///
/// This runs as a post-parse pass (not inside per-function parsing) because an
/// error raised during `Parser::parse_function` with `stop_on_error=false` is
/// swallowed into a silent function *skip*, which would drop the offending
/// functions instead of failing the build. Returning the error here propagates
/// it to the top level.
fn validate_frb_categories(funcs: &[MirFunc]) -> anyhow::Result<()> {
    const SESSION_HANDLE_ARGS: &[&str] =
        &["session_id", "drive_id", "browser_id", "wizard_id"];

    let offenders = funcs
        .iter()
        .filter(|func| func.category.is_none())
        .filter(|func| {
            let is_sync = matches!(func.mode, MirFuncMode::Sync);
            let has_session_arg = func.inputs.iter().any(|input| {
                SESSION_HANDLE_ARGS.contains(&input.inner.name.rust_style(true).as_str())
            });
            is_sync || has_session_arg
        })
        .map(|func| func.name.rust_style(true))
        .collect_vec();

    if !offenders.is_empty() {
        bail!(
            "the following functions may touch live CRDT/session state (they are \
             `sync` or carry a session/drive/browser/wizard id) but have no \
             `#[frb(category = ...)]` annotation; add \
             `#[frb(category = Main|Loro|Ai|Export|Sync)]` to each so the custom \
             executor routes it to the correct worker: {offenders:?}"
        );
    }

    Ok(())
}

fn sort_and_add_func_id(funcs: Vec<MirFunc>) -> Vec<MirFunc> {
    (funcs.into_iter())
        // to give downstream a stable output
        .sorted_by_cached_key(|func| func.name.rust_style(true).clone())
        .enumerate()
        .map(|(index, f)| MirFunc {
            id: Some((index + 1) as _),
            ..f
        })
        .collect_vec()
}
