use std::sync::{Arc, RwLock};

use anyhow::{Context, bail};
use polars::{frame::DataFrame, lazy::frame::IntoLazy, prelude::Column};
use primitive::iter::vec_zip::VecZip;
use slotmap::{HopSlotMap, new_key_type};

use crate::{
    row::{LiteralType, LiteralValue, TableRow, ValueDisplay},
    table_view::{
        TableView,
        en::{Alignment, TableViewWrite},
    },
};

#[derive(Debug)]
pub struct Table<R> {
    rows: Arc<RwLock<HopSlotMap<RowKey, R>>>,
}
impl<R: TableRow + ValueDisplay> Table<R> {
    pub fn to_view(&self, sql: &str) -> anyhow::Result<TableViewWrite> {
        let sql = dfsql::sql::parse(sql)?;

        let schema = R::schema();

        let mut columns: Vec<Vec<Option<LiteralValue>>> =
            std::iter::repeat(vec![]).take(schema.len()).collect();
        {
            let rows = self.rows.read().unwrap();
            for (_k, r) in rows.iter() {
                for (i, cell) in r.fields().into_iter().enumerate() {
                    columns[i].push(cell);
                }
            }
        }

        let mut series = vec![];
        for ((header, ty), column) in schema.iter().zip(columns.into_iter()) {
            let header = header.clone().into();
            let s = match ty {
                LiteralType::String => {
                    let column: Vec<Option<String>> = column
                        .into_iter()
                        .map(|cell| cell.map(|v| v.try_into().unwrap()))
                        .collect();
                    Column::new(header, column)
                }
                LiteralType::UInt => {
                    let column: Vec<Option<u64>> = column
                        .into_iter()
                        .map(|cell| cell.map(|v| v.try_into().unwrap()))
                        .collect();
                    Column::new(header, column)
                }
                LiteralType::Int => {
                    let column: Vec<Option<i64>> = column
                        .into_iter()
                        .map(|cell| cell.map(|v| v.try_into().unwrap()))
                        .collect();
                    Column::new(header, column)
                }
                LiteralType::Float => {
                    let column: Vec<Option<f64>> = column
                        .into_iter()
                        .map(|cell| cell.map(|v| v.try_into().unwrap()))
                        .collect();
                    Column::new(header, column)
                }
                LiteralType::Bool => {
                    let column: Vec<Option<bool>> = column
                        .into_iter()
                        .map(|cell| cell.map(|v| v.try_into().unwrap()))
                        .collect();
                    Column::new(header, column)
                }
            };
            series.push(s);
        }
        let df = DataFrame::new(series).unwrap();
        let input = [("table".into(), df.lazy())].into_iter().collect();
        let mut executor = dfsql::df::DfExecutor::new("table".into(), input).unwrap();
        executor.execute(&sql)?;
        let df = executor.df().clone().collect()?;

        let series = df.get_columns();
        let headers: Vec<String> = series.iter().map(|s| s.name().to_string()).collect();
        let mut columns = vec![];
        let mut alignments = vec![];
        for s in series.iter() {
            let t = literal_type(s.dtype())?;
            let column: Vec<Option<LiteralValue>> = match t {
                LiteralType::Bool => s
                    .bool()
                    .unwrap()
                    .into_iter()
                    .map(|v| v.map(|v| v.into()))
                    .collect(),
                LiteralType::UInt => s
                    .cast(&polars::datatypes::DataType::UInt64)?
                    .u64()
                    .unwrap()
                    .into_iter()
                    .map(|v| v.map(|v| v.into()))
                    .collect(),
                LiteralType::Int => s
                    .cast(&polars::datatypes::DataType::Int64)?
                    .i64()
                    .unwrap()
                    .into_iter()
                    .map(|v| v.map(|v| v.into()))
                    .collect(),
                LiteralType::Float => s
                    .f64()
                    .unwrap()
                    .into_iter()
                    .map(|v| v.map(|v| v.into()))
                    .collect(),
                LiteralType::String => s
                    .str()
                    .unwrap()
                    .into_iter()
                    .map(|v| v.map(|v| v.to_owned().into()))
                    .collect(),
            };
            columns.push(column.into_iter());
            alignments.push(alignment(t));
        }

        let rows = VecZip::new(columns)
            .map(|r| {
                let r: Arc<[Arc<str>]> = r
                    .into_iter()
                    .enumerate()
                    .map(|(i, c)| {
                        let header = &headers[i];
                        let c: Arc<str> = R::display_value(header, c).into();
                        c
                    })
                    .collect();
                r
            })
            .collect();
        let titles = headers.into_iter().map(|t| t.into()).collect();

        let t = TableView::new(titles, rows).context("Failed to build the table view")?;
        Ok(TableViewWrite::new(t, alignments.into()).unwrap())
    }
}
impl<R> Table<R> {
    #[must_use]
    pub fn new() -> Self {
        Self {
            rows: Arc::new(RwLock::new(HopSlotMap::with_key())),
        }
    }

    #[must_use]
    pub fn insert(&self, row: R) -> RowKey {
        let mut map = self.rows.write().unwrap();
        map.insert(row)
    }

    #[must_use]
    pub fn set_scope(&self, row: R) -> RowGuard<'_, R> {
        let key = self.insert(row);
        RowGuard { table: self, key }
    }

    #[must_use]
    pub fn set_scope_owned(&self, row: R) -> RowOwnedGuard<R> {
        let key = self.insert(row);
        RowOwnedGuard {
            table: self.clone(),
            key,
        }
    }

    pub fn remove(&self, key: RowKey) -> Option<R> {
        let mut map = self.rows.write().unwrap();
        map.remove(key)
    }
}
impl<R> Default for Table<R> {
    fn default() -> Self {
        Self::new()
    }
}
impl<R> Clone for Table<R> {
    fn clone(&self) -> Self {
        Self {
            rows: self.rows.clone(),
        }
    }
}

fn literal_type(t: &polars::datatypes::DataType) -> anyhow::Result<LiteralType> {
    Ok(match t {
        polars::datatypes::DataType::Boolean => LiteralType::Bool,
        polars::datatypes::DataType::UInt8
        | polars::datatypes::DataType::UInt16
        | polars::datatypes::DataType::UInt32
        | polars::datatypes::DataType::UInt64 => LiteralType::UInt,
        polars::datatypes::DataType::Int8
        | polars::datatypes::DataType::Int16
        | polars::datatypes::DataType::Int32
        | polars::datatypes::DataType::Int64
        | polars::datatypes::DataType::Int128 => LiteralType::Int,
        polars::datatypes::DataType::Float32 | polars::datatypes::DataType::Float64 => {
            LiteralType::Float
        }
        polars::datatypes::DataType::String => LiteralType::String,
        polars::datatypes::DataType::Binary
        | polars::datatypes::DataType::BinaryOffset
        | polars::datatypes::DataType::Date
        | polars::datatypes::DataType::Datetime(_, _)
        | polars::datatypes::DataType::Duration(_)
        | polars::datatypes::DataType::Time
        | polars::datatypes::DataType::List(_)
        | polars::datatypes::DataType::Null
        | polars::datatypes::DataType::Categorical(_, _)
        | polars::datatypes::DataType::Enum(_, _)
        | polars::datatypes::DataType::Struct(_)
        | polars::datatypes::DataType::Unknown(_) => {
            bail!("Data types other than boolean, integer, float, or string are unsupported")
        }
    })
}

fn alignment(value: LiteralType) -> Alignment {
    match value {
        LiteralType::String => Alignment::Left,
        LiteralType::UInt => Alignment::Right,
        LiteralType::Int => Alignment::Right,
        LiteralType::Float => Alignment::Right,
        LiteralType::Bool => Alignment::Right,
    }
}

#[derive(Debug)]
pub struct RowGuard<'table, R> {
    table: &'table Table<R>,
    key: RowKey,
}
impl<R> RowGuard<'_, R> {
    pub fn inspect_mut(&self, f: fn(&mut R)) {
        inspect_mut(self.table, self.key, f)
    }
}
impl<R> Drop for RowGuard<'_, R> {
    fn drop(&mut self) {
        self.table.remove(self.key);
    }
}

#[derive(Debug)]
pub struct RowOwnedGuard<R> {
    table: Table<R>,
    key: RowKey,
}
impl<R> RowOwnedGuard<R> {
    pub fn inspect_mut(&self, f: fn(&mut R)) {
        inspect_mut(&self.table, self.key, f)
    }
}
impl<R> Drop for RowOwnedGuard<R> {
    fn drop(&mut self) {
        self.table.remove(self.key);
    }
}

fn inspect_mut<R>(table: &Table<R>, key: RowKey, f: fn(&mut R)) {
    let mut map = table.rows.write().unwrap();
    let Some(session) = map.get_mut(key) else {
        return;
    };
    f(session)
}

new_key_type! { pub struct RowKey; }
