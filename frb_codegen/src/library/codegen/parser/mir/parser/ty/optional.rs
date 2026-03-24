use crate::codegen::ir::mir::ty::delegate::MirTypeDelegate;
use crate::codegen::ir::mir::ty::optional::MirTypeOptional;
use crate::codegen::ir::mir::ty::MirType;
use crate::codegen::ir::mir::ty::MirType::{
    Boxed, DartFn, DartOpaque, Delegate, Dynamic, EnumRef, GeneralList, Optional, Primitive,
    PrimitiveList, Record, RustAutoOpaqueImplicit, RustOpaque, StructRef,
};
use crate::codegen::parser::mir::parser::ty::unencodable::SplayedSegment;
use crate::codegen::parser::mir::parser::ty::TypeParserWithContext;
use quote::ToTokens;
use syn::TypePath;

impl TypeParserWithContext<'_, '_, '_> {
    pub(crate) fn parse_type_path_data_optional(
        &mut self,
        type_path: &TypePath,
        last_segment: &SplayedSegment,
    ) -> anyhow::Result<Option<MirType>> {
        Ok(Some(match last_segment {
            ("Option", [inner]) => {
                let inner = self.parse_type(inner)?;

                Optional(match inner {
                    StructRef(..)
                    | EnumRef(..)
                    | RustAutoOpaqueImplicit(..)
                    | RustOpaque(..)
                    | DartOpaque(..)
                    | DartFn(..)
                    | Primitive(..)
                    | Record(..)
                    | Delegate(MirTypeDelegate::PrimitiveEnum(..)) => {
                        MirTypeOptional::new_with_boxed_wrapper(inner.clone())
                    }
                    Delegate(MirTypeDelegate::Time(..)) => {
                        MirTypeOptional::new_with_boxed_wrapper(inner.clone())
                    }
                    PrimitiveList(_) | GeneralList(_) | Boxed(_) | Dynamic(_) | Delegate(_) => {
                        MirTypeOptional::new(inner.clone())
                    }
                    // Nested optionals: wrap inner Optional with boxed wrapper.
                    // Dart codegen renders this as `Option<T>?` (using oxidized)
                    // when use_oxidized is enabled, or `T?` (flattened) otherwise.
                    Optional(_) => {
                        MirTypeOptional::new_with_boxed_wrapper(inner.clone())
                    }
                    // frb-coverage:ignore-start
                    MirType::TraitDef(_) => {
                        panic!("FATAL: Unsupported type inside Option: {:?}", inner);
                    } // frb-coverage:ignore-end
                })
            }

            _ => return Ok(None),
        }))
    }
}
