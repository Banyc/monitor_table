use std::sync::{Arc, RwLock};

use anyhow::Context;
use dfsql::backend::{
    Frame,
    dynamic::{Column, Value},
};
use primitive::iter::vec_zip::VecZip;
use slotmap::{SlotMap, new_key_type};

use crate::{
    row::{LiteralType, LiteralValue, TableRow, ValueDisplay},
    table_view::{
        TableView,
        en::{Alignment, TableViewWrite},
    },
};

#[derive(Debug)]
pub struct Table<R> {
    rows: Arc<RwLock<SlotMap<RowKey, R>>>,
}
impl<R: TableRow + ValueDisplay> Table<R> {
    pub fn to_view(&self, sql: &str) -> anyhow::Result<TableViewWrite> {
        let sql = dfsql::sql::parse(sql)?;

        let schema = R::schema();

        let mut columns: Vec<Vec<Option<LiteralValue>>> =
            std::iter::repeat_n(vec![], schema.len()).collect();
        {
            let rows = self.rows.read().unwrap();
            for (_k, r) in rows.iter() {
                for (i, cell) in r.fields().into_iter().enumerate() {
                    columns[i].push(cell);
                }
            }
        }

        let mut dyn_columns = vec![];
        for ((header, ty), column) in schema.iter().zip(columns) {
            let header = header.clone();
            let c = match ty {
                LiteralType::String => {
                    let v: Vec<Option<String>> = column
                        .into_iter()
                        .map(|cell| cell.map(|v| v.try_into().unwrap()))
                        .collect();
                    Column::new(header, v)
                }
                LiteralType::UInt => {
                    let v: Vec<Option<u64>> = column
                        .into_iter()
                        .map(|cell| cell.map(|v| v.try_into().unwrap()))
                        .collect();
                    Column::new(header, v)
                }
                LiteralType::Int => {
                    let v: Vec<Option<i64>> = column
                        .into_iter()
                        .map(|cell| cell.map(|v| v.try_into().unwrap()))
                        .collect();
                    Column::new(header, v)
                }
                LiteralType::Float => {
                    let v: Vec<Option<f64>> = column
                        .into_iter()
                        .map(|cell| cell.map(|v| v.try_into().unwrap()))
                        .collect();
                    Column::new(header, v)
                }
                LiteralType::Bool => {
                    let v: Vec<Option<bool>> = column
                        .into_iter()
                        .map(|cell| cell.map(|v| v.try_into().unwrap()))
                        .collect();
                    Column::new(header, v)
                }
            };
            dyn_columns.push(c);
        }

        let frame = Frame::new(dyn_columns)?;
        let mut executor = dfsql::backend::DynamicExecutor::from_frame("table", frame);
        executor.execute(&sql)?;

        let frame = executor.collect()?;
        let headers = frame.column_names();
        let dyn_frame = frame.to_dynamic()?;
        let mut out_columns = vec![];
        let mut alignments = vec![];
        for col in dyn_frame.columns() {
            let values = col.values();
            let t = infer_type(&values);
            let column: Vec<Option<LiteralValue>> = values
                .into_iter()
                .map(|v| match v {
                    Value::Null => None,
                    Value::Bool(b) => Some(b.into()),
                    Value::UInt(u) => Some(u.into()),
                    Value::Int(i) => Some(i.into()),
                    Value::Float(f) => Some(f.into()),
                    Value::String(s) => Some(LiteralValue::String(s)),
                    Value::Bytes(_) | Value::List(_) => None,
                })
                .collect();
            out_columns.push(column.into_iter());
            alignments.push(alignment(t));
        }

        let rows = VecZip::new(out_columns)
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
            rows: Arc::new(RwLock::new(SlotMap::with_key())),
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

fn infer_type(values: &[Value]) -> LiteralType {
    for v in values {
        match v {
            Value::Bool(_) => return LiteralType::Bool,
            Value::UInt(_) => return LiteralType::UInt,
            Value::Int(_) => return LiteralType::Int,
            Value::Float(_) => return LiteralType::Float,
            Value::String(_) => return LiteralType::String,
            Value::Bytes(_) | Value::List(_) | Value::Null => continue,
        }
    }
    LiteralType::String
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
