use std::{str::FromStr, sync::Arc};

use super::TableView;

impl FromStr for TableView {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut lines = s.split('\n');
        let mut column_lengths = vec![];
        let mut titles = vec![];
        {
            let titles_str = lines.next().ok_or(())?;
            let mut title = String::new();
            let mut padding = 0;
            for c in titles_str.chars() {
                if c == ' ' {
                    padding += 1;
                    continue;
                }
                if !title.is_empty() && padding > 0 {
                    let len = title.len() + padding - 1;
                    column_lengths.push(len);
                    padding = 0;
                    let title: Arc<str> = std::mem::take(&mut title).into();
                    titles.push(title);
                }
                title.push(c);
            }
            let len = title.len() + padding - 1;
            column_lengths.push(len);
            titles.push(title.into());
        }
        let titles = titles.into();
        let mut rows = vec![];
        {
            for r in lines {
                if r.is_empty() {
                    break;
                }
                let mut row = vec![];
                let chars = r.chars();
                let mut read = 0;
                for len in &column_lengths {
                    let cell = chars.clone().skip(read).take(*len).collect::<String>();
                    let cell = cell.trim();
                    row.push(cell.into());
                    read += len + 1;
                }
                rows.push(row.into());
            }
        }
        let rows = rows.into();
        let t = TableView { titles, rows };
        t.check_rep();
        Ok(t)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_de() {
        let s = "id  usage 
cpu 80    
mem 20    
";
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

        assert_eq!(t, TableView::from_str(s).unwrap());
    }
}
