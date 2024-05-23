use core::fmt;

use crate::ArcStr;

pub trait TableRow {
    /// Return all the header and the value type.
    fn schema() -> Vec<(String, LiteralType)>;
    /// Return all of the values corresponding to the schema.
    fn fields(&self) -> Vec<Option<LiteralValue>>;
    /// Convert the value to a user-friendly one.
    fn display_value(header: &str, value: Option<LiteralValue>) -> String {
        let _ = header;
        match value {
            Some(v) => v.to_string(),
            None => String::new(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum LiteralType {
    String,
    UInt,
    Int,
    Float,
    Bool,
}

#[derive(Debug, Clone)]
pub enum LiteralValue {
    String(ArcStr),
    UInt(u64),
    Int(i64),
    Float(f64),
    Bool(bool),
}
impl TryFrom<LiteralValue> for String {
    type Error = ();

    fn try_from(value: LiteralValue) -> Result<Self, Self::Error> {
        let LiteralValue::String(v) = value else {
            return Err(());
        };
        Ok(v.to_string())
    }
}
impl TryFrom<LiteralValue> for ArcStr {
    type Error = ();

    fn try_from(value: LiteralValue) -> Result<Self, Self::Error> {
        let LiteralValue::String(v) = value else {
            return Err(());
        };
        Ok(v)
    }
}
impl TryFrom<LiteralValue> for u64 {
    type Error = ();

    fn try_from(value: LiteralValue) -> Result<Self, Self::Error> {
        let LiteralValue::UInt(v) = value else {
            return Err(());
        };
        Ok(v)
    }
}
impl TryFrom<LiteralValue> for i64 {
    type Error = ();

    fn try_from(value: LiteralValue) -> Result<Self, Self::Error> {
        let LiteralValue::Int(v) = value else {
            return Err(());
        };
        Ok(v)
    }
}
impl TryFrom<LiteralValue> for f64 {
    type Error = ();

    fn try_from(value: LiteralValue) -> Result<Self, Self::Error> {
        let LiteralValue::Float(v) = value else {
            return Err(());
        };
        Ok(v)
    }
}
impl TryFrom<LiteralValue> for bool {
    type Error = ();

    fn try_from(value: LiteralValue) -> Result<Self, Self::Error> {
        let LiteralValue::Bool(v) = value else {
            return Err(());
        };
        Ok(v)
    }
}
impl From<String> for LiteralValue {
    fn from(value: String) -> Self {
        Self::String(value.into())
    }
}
impl From<ArcStr> for LiteralValue {
    fn from(value: ArcStr) -> Self {
        Self::String(value)
    }
}
impl From<u64> for LiteralValue {
    fn from(value: u64) -> Self {
        Self::UInt(value)
    }
}
impl From<i64> for LiteralValue {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}
impl From<f64> for LiteralValue {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}
impl From<bool> for LiteralValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}
impl fmt::Display for LiteralValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LiteralValue::String(v) => write!(f, "{v}"),
            LiteralValue::UInt(v) => write!(f, "{v}"),
            LiteralValue::Int(v) => write!(f, "{v}"),
            LiteralValue::Float(v) => write!(f, "{v}"),
            LiteralValue::Bool(v) => write!(f, "{v}"),
        }
    }
}
