use hdv::serde::{HdvScheme, HdvSerialize};

use crate::row::TableRow;

impl<T> TableRow for T
where
    T: HdvScheme + HdvSerialize,
{
    fn schema() -> Vec<(String, crate::row::LiteralType)> {
        let object_scheme = <Self as HdvScheme>::object_scheme();
        let atom_schemes = object_scheme.atom_schemes();
        atom_schemes
            .into_iter()
            .map(|atom| {
                (
                    atom.name,
                    match atom.r#type {
                        hdv::format::AtomType::String => crate::row::LiteralType::String,
                        hdv::format::AtomType::Bytes => crate::row::LiteralType::String,
                        hdv::format::AtomType::U64 => crate::row::LiteralType::UInt,
                        hdv::format::AtomType::I64 => crate::row::LiteralType::Int,
                        hdv::format::AtomType::F32 => crate::row::LiteralType::Float,
                        hdv::format::AtomType::F64 => crate::row::LiteralType::Float,
                        hdv::format::AtomType::Bool => crate::row::LiteralType::Bool,
                    },
                )
            })
            .collect()
    }

    fn fields(&self) -> Vec<Option<crate::row::LiteralValue>> {
        let mut atoms = vec![];
        <Self as HdvSerialize>::serialize(self, &mut atoms);
        atoms
            .into_iter()
            .map(|x| {
                x.map(|x| match x {
                    hdv::format::AtomValue::String(x) => crate::row::LiteralValue::String(x),
                    hdv::format::AtomValue::Bytes(x) => {
                        crate::row::LiteralValue::String(format!("{x:x?}").into())
                    }
                    hdv::format::AtomValue::U64(x) => crate::row::LiteralValue::UInt(x),
                    hdv::format::AtomValue::I64(x) => crate::row::LiteralValue::Int(x),
                    hdv::format::AtomValue::F32(x) => crate::row::LiteralValue::Float(x as f64),
                    hdv::format::AtomValue::F64(x) => crate::row::LiteralValue::Float(x),
                    hdv::format::AtomValue::Bool(x) => crate::row::LiteralValue::Bool(x),
                })
            })
            .collect()
    }
}
