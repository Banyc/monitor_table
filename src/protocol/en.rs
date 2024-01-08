use core::fmt;
use std::sync::Arc;

use super::TableView;

pub struct TableViewWrite {
    t: TableView,
    alignments: Arc<[Alignment]>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Alignment {
    Left,
    Right,
}
impl TableViewWrite {
    pub fn new(t: TableView, alignments: Arc<[Alignment]>) -> Option<Self> {
        if t.titles.len() != alignments.len() {
            return None;
        }
        Some(Self { t, alignments })
    }
}

impl fmt::Display for TableViewWrite {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut column_lengths = self
            .t
            .titles
            .iter()
            .map(|t| t.len())
            .collect::<Vec<usize>>();
        for r in self.t.rows.iter() {
            for (i, c) in r.iter().enumerate() {
                column_lengths[i] = column_lengths[i].max(c.len());
            }
        }

        for (t, len) in self.t.titles.iter().zip(column_lengths.iter()) {
            write(f, t, Alignment::Left, *len)?;
        }
        writeln!(f)?;
        for r in self.t.rows.iter() {
            for ((c, len), a) in r
                .iter()
                .zip(column_lengths.iter())
                .zip(self.alignments.iter())
            {
                write(f, c, *a, *len)?;
            }
            writeln!(f)?;
        }
        Ok(())
    }
}

fn write(f: &mut fmt::Formatter<'_>, s: &str, a: Alignment, len: usize) -> fmt::Result {
    let padding = len - s.len();
    match a {
        Alignment::Left => {
            write!(f, "{s}")?;
            for _ in 0..padding {
                write!(f, " ")?;
            }
        }
        Alignment::Right => {
            for _ in 0..padding {
                write!(f, " ")?;
            }
            write!(f, "{s}")?;
        }
    }
    write!(f, " ")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_en() {
        let titles = vec!["id", "usage"];
        let rows = vec![
            vec!["cpu", "80"], //
            vec!["mem", "20"],
        ];
        let t = TableView {
            titles: titles.into_iter().map(|t| t.into()).collect(),
            rows: rows
                .into_iter()
                .map(|r| r.into_iter().map(|c| c.into()).collect())
                .collect(),
        };
        t.check_rep();
        let t = TableViewWrite::new(t, [Alignment::Left, Alignment::Left].into()).unwrap();
        let t = t.to_string();
        assert_eq!(
            t,
            "id  usage 
cpu 80    
mem 20    
"
        )
    }
}
