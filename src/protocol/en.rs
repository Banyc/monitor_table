use core::fmt;

use super::TableView;

impl fmt::Display for TableView {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut column_lengths = self.titles.iter().map(|t| t.len()).collect::<Vec<usize>>();
        for r in self.rows.iter() {
            for (i, c) in r.iter().enumerate() {
                column_lengths[i] = column_lengths[i].max(c.len());
            }
        }

        for (t, len) in self.titles.iter().zip(column_lengths.iter()) {
            write(f, t, *len)?;
        }
        writeln!(f)?;
        for r in self.rows.iter() {
            for (c, len) in r.iter().zip(column_lengths.iter()) {
                write(f, c, *len)?;
            }
            writeln!(f)?;
        }
        Ok(())
    }
}

fn write(f: &mut fmt::Formatter<'_>, s: &str, len: usize) -> fmt::Result {
    write!(f, "{s}")?;
    let padding = len - s.len();
    for _ in 0..padding {
        write!(f, " ")?;
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
