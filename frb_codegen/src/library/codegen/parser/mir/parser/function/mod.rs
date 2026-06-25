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
    validate_frb_threads(&funcs, config)?;
    Ok((funcs, skips))
}

/// Optional enforcement for the `#[frb(thread = ...)]` executor-routing lane,
/// driven entirely by the embedder's `lane_routing` config — FRB assigns no
/// meaning to lanes itself.
///
/// With config present it errors when an annotation names a lane outside the
/// configured set, and when a function matching `require_annotation_when`
/// carries no annotation. Without config it is a no-op (any lane key accepted,
/// nothing required).
///
/// Runs as a post-parse pass (not inside per-function parsing) because an error
/// raised with `stop_on_error=false` is swallowed into a silent function *skip*,
/// which would drop the offending functions instead of failing the build.
/// Returning the error here propagates it to the top level.
fn validate_frb_threads(
    funcs: &[MirFunc],
    config: &ParserMirInternalConfig,
) -> anyhow::Result<()> {
    let Some(lane_routing) = &config.lane_routing else {
        return Ok(());
    };

    // 1. Lane-name validation: every annotation must name `Main` or a configured lane.
    if let Some(lanes) = &lane_routing.lanes {
        for func in funcs {
            if let Some(lane) = &func.thread {
                if lane != "Main" && !lanes.iter().any(|l| l == lane) {
                    bail!(
                        "function `{}` is annotated `#[frb(thread = {lane})]`, but `{lane}` \
                         is not a configured lane; valid lanes: Main, {}",
                        func.name.rust_style(true),
                        lanes.join(", "),
                    );
                }
            }
        }
    }

    // 2. Required-annotation policy.
    if let Some(require) = &lane_routing.require_annotation_when {
        let require_sync = require.sync.unwrap_or(false);
        let param_named = require.param_named.clone().unwrap_or_default();
        let offenders = funcs
            .iter()
            .filter(|func| func.thread.is_none())
            .filter(|func| {
                let is_sync = require_sync && matches!(func.mode, MirFuncMode::Sync);
                let has_named_param = func
                    .inputs
                    .iter()
                    .any(|input| param_named.contains(&input.inner.name.rust_style(true)));
                is_sync || has_named_param
            })
            .map(|func| func.name.rust_style(true))
            .collect_vec();

        if !offenders.is_empty() {
            let lanes_hint = lane_routing
                .lanes
                .as_ref()
                .map(|lanes| format!("Main|{}", lanes.join("|")))
                .unwrap_or_else(|| "Main".to_owned());
            bail!(
                "these functions match the configured `require_annotation_when` policy but have \
                 no `#[frb(thread = ...)]` annotation; add `#[frb(thread = {lanes_hint})]` to \
                 each so the custom executor routes it: {offenders:?}"
            );
        }
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
