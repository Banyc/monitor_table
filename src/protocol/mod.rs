use std::sync::Arc;

pub mod de;
pub mod en;

pub type ArcStr = Arc<str>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableView {
    titles: Arc<[ArcStr]>,
    rows: Arc<[Arc<[ArcStr]>]>,
}

impl TableView {
    fn check_rep(&self) {
        for t in self.titles.iter() {
            if t.is_empty() {
                panic!("All titles are not allowed to be empty");
            }
            if t.contains(' ') {
                panic!("Spaces are not allowed in titles");
            }
            if t.contains('\n') {
                panic!("New lines are not allowed in titles");
            }
        }
        for r in self.rows.iter() {
            if r.len() != self.titles.len() {
                panic!("Unaligned columns");
            }
            for c in r.iter() {
                if c.contains('\n') {
                    panic!("New lines are not allowed in cells");
                }
            }
        }
    }

    pub fn new(titles: Arc<[ArcStr]>, rows: Arc<[Arc<[ArcStr]>]>) -> Option<Self> {
        for t in titles.iter() {
            if t.is_empty() {
                return None;
            }
            if t.contains(' ') {
                return None;
            }
            if t.contains('\n') {
                return None;
            }
        }
        for r in rows.iter() {
            if r.len() != titles.len() {
                return None;
            }
            for c in r.iter() {
                if c.contains('\n') {
                    return None;
                }
            }
        }
        Some(Self { titles, rows })
    }
}
