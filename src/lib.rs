use std::sync::Arc;

pub mod protocol;
pub mod row;
pub mod table;

type ArcStr = Arc<str>;

#[cfg(test)]
mod tests {
    use crate::{
        row::{LiteralType, LiteralValue, TableRow},
        table::Table,
    };

    #[test]
    fn test_basics() {
        struct Row {
            x: i64,
        }
        impl TableRow for Row {
            fn schema() -> Vec<(String, LiteralType)> {
                // The table has only one column which stores integers and is called "x"
                vec![("x".to_string(), LiteralType::Int)]
            }

            fn fields(&self) -> Vec<Option<LiteralValue>> {
                // Return values in a row
                vec![Some(self.x.into())]
            }
        }

        let table = Table::new();

        // Add entries to the table
        let scope_1 = table.set_scope(Row { x: 0 });
        scope_1.inspect_mut(|r| {
            r.x = 1;
        });
        let scope_2 = table.set_scope(Row { x: 0 });

        // Query the table using SQL
        let view = table.to_view("sort x").unwrap();
        assert_eq!(
            view.to_string(),
            "x 
0 
1 
"
        );

        // Remove an entry from the table
        drop(scope_1);
        let view = table.to_view("").unwrap();
        assert_eq!(
            view.to_string(),
            "x 
0 
"
        );

        // Remove another entry from the table
        drop(scope_2);
        let view = table.to_view("").unwrap();
        assert_eq!(
            view.to_string(),
            "x 
"
        );
    }
}
