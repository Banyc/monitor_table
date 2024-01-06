use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use math::two_dim::VecZip;
use polars::{frame::DataFrame, lazy::frame::IntoLazy, prelude::NamedFrom, series::Series};
use slotmap::{new_key_type, HopSlotMap};

use crate::{
    protocol::TableView,
    row::{LiteralType, LiteralValue, TableRow},
};

#[derive(Debug)]
pub struct Table<R> {
    rows: Arc<RwLock<HopSlotMap<RowKey, R>>>,
}
impl<R: TableRow> Table<R> {
    pub fn to_view(&self, sql: &str) -> Option<TableView> {
        let sql = dfsql::sql::parse(sql)?;

        let schema = R::schema();
        let rows = self.rows.read().unwrap();
        let mut columns: Vec<Vec<Option<LiteralValue>>> =
            std::iter::repeat(vec![]).take(schema.len()).collect();
        for (_k, r) in rows.iter() {
            for (i, cell) in r.fields().into_iter().enumerate() {
                columns[i].push(cell);
            }
        }

        let mut series = vec![];
        for ((title, ty), column) in schema.iter().zip(columns.into_iter()) {
            let s = match ty {
                LiteralType::String => {
                    let column: Vec<Option<String>> = column
                        .into_iter()
                        .map(|cell| cell.map(|v| v.try_into().unwrap()))
                        .collect();
                    Series::new(title, column)
                }
                LiteralType::Int => {
                    let column: Vec<Option<i64>> = column
                        .into_iter()
                        .map(|cell| cell.map(|v| v.try_into().unwrap()))
                        .collect();
                    Series::new(title, column)
                }
                LiteralType::Float => {
                    let column: Vec<Option<f64>> = column
                        .into_iter()
                        .map(|cell| cell.map(|v| v.try_into().unwrap()))
                        .collect();
                    Series::new(title, column)
                }
                LiteralType::Bool => {
                    let column: Vec<Option<bool>> = column
                        .into_iter()
                        .map(|cell| cell.map(|v| v.try_into().unwrap()))
                        .collect();
                    Series::new(title, column)
                }
            };
            series.push(s);
        }
        let df = DataFrame::new(series).unwrap();

        let df = dfsql::df::apply(df.lazy(), &sql, &HashMap::new()).ok()?;
        let df = df.collect().ok()?;

        let series = df.get_columns();
        let titles: Arc<[Arc<str>]> = series.iter().map(|s| s.name().into()).collect();
        let mut columns = vec![];
        for s in series.iter() {
            let t = s.dtype();
            let column: Vec<Option<LiteralValue>> = match t {
                polars::datatypes::DataType::Boolean => s
                    .bool()
                    .unwrap()
                    .into_iter()
                    .map(|v| v.map(|v| v.into()))
                    .collect(),
                polars::datatypes::DataType::UInt8
                | polars::datatypes::DataType::UInt16
                | polars::datatypes::DataType::UInt32
                | polars::datatypes::DataType::UInt64
                | polars::datatypes::DataType::Int8
                | polars::datatypes::DataType::Int16
                | polars::datatypes::DataType::Int32
                | polars::datatypes::DataType::Int64 => s
                    .i64()
                    .ok()?
                    .into_iter()
                    .map(|v| v.map(|v| v.into()))
                    .collect(),
                polars::datatypes::DataType::Float32 | polars::datatypes::DataType::Float64 => s
                    .f64()
                    .unwrap()
                    .into_iter()
                    .map(|v| v.map(|v| v.into()))
                    .collect(),
                polars::datatypes::DataType::String => s
                    .str()
                    .unwrap()
                    .into_iter()
                    .map(|v| v.map(|v| v.to_owned().into()))
                    .collect(),
                polars::datatypes::DataType::Binary
                | polars::datatypes::DataType::Date
                | polars::datatypes::DataType::Datetime(_, _)
                | polars::datatypes::DataType::Duration(_)
                | polars::datatypes::DataType::Time
                | polars::datatypes::DataType::List(_)
                | polars::datatypes::DataType::Null
                | polars::datatypes::DataType::Struct(_)
                | polars::datatypes::DataType::Unknown => return None,
            };
            columns.push(column.into_iter());
        }

        let rows = VecZip::new(columns)
            .map(|r| {
                let r: Arc<[Arc<str>]> = r
                    .into_iter()
                    .enumerate()
                    .map(|(i, c)| {
                        let title = &titles[i];
                        let c: Arc<str> = R::display(title, c).into();
                        c
                    })
                    .collect();
                r
            })
            .collect();

        TableView::new(titles, rows)
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
