use console::style;
use std::fmt::{Display, Formatter};

pub trait Row {
    fn cell_value(&self, column: &str) -> Option<String>;
}

pub struct Table<R> {
    columns: Vec<Column>,
    rows: Vec<R>,
}

struct Column {
    name: String,
    width: usize,
    alignment: Alignment,
}

pub enum Alignment {
    Left,
    Right,
}

impl<R: Row> Table<R> {
    pub fn new<C: AsRef<str>>(columns: &[C]) -> Table<R> {
        let columns: Vec<Column> = columns
            .iter()
            .map(|name| Column {
                name: name.as_ref().to_owned(),
                width: name.as_ref().len(),
                alignment: Alignment::Left,
            })
            .collect();

        Table {
            columns,
            rows: vec![],
        }
    }

    pub fn align(&mut self, column_index: usize, alignment: Alignment) {
        self.columns[column_index].alignment = alignment;
    }

    pub fn push(&mut self, row: R) {
        for column in self.columns.iter_mut() {
            let len = row
                .cell_value(column.name.as_str())
                .map(|v| v.to_string().len())
                .unwrap_or_default();
            column.width = column.width.max(len);
        }
        self.rows.push(row);
    }

    fn header(&self, column: &Column) -> String {
        let column_name = column.name.as_str();
        let column_width = column.width;
        let padding = column_width - column_name.len();
        match column.alignment {
            Alignment::Left => format!("{}{}", column_name, Self::right_padding(padding)),
            Alignment::Right => format!("{}{}", Self::left_padding(padding), column_name),
        }
    }

    fn value(&self, row: &R, column: &Column) -> String {
        let column_name = column.name.as_str();
        let column_value = row
            .cell_value(column_name)
            .map(|v| v.to_string())
            .unwrap_or_default();
        let column_width = column.width;
        let padding = column_width - column_value.len();
        match column.alignment {
            Alignment::Left => format!("{}{}", column_value, " ".repeat(padding)),
            Alignment::Right => format!("{}{}", " ".repeat(padding), column_value),
        }
    }

    fn left_padding(n: usize) -> String {
        match n {
            0 => "".to_string(),
            1 => " ".to_string(),
            2.. => format!("{} ", "─".repeat(n - 1)),
        }
    }

    fn right_padding(n: usize) -> String {
        match n {
            0 => "".to_string(),
            1 => " ".to_string(),
            2.. => format!(" {}", "─".repeat(n - 1)),
        }
    }
}

impl<R: Row> Display for Table<R> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for column in &self.columns {
            write!(
                f,
                "{}   ",
                style(self.header(column))
                    .yellow()
                    .bold()
                    .bright()
                    .for_stdout()
            )?;
        }
        writeln!(f)?;

        for row in &self.rows {
            for column in &self.columns {
                write!(f, "{}   ", self.value(row, column))?;
            }
            writeln!(f)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::table::{Alignment, Row, Table};

    #[test]
    fn render_table() {
        struct DataPoint {
            benchmark: &'static str,
            result: u64,
        }
        impl Row for DataPoint {
            fn cell_value(&self, column: &str) -> Option<String> {
                match column {
                    "A" => Some(self.benchmark.to_string()),
                    "Result" => Some(self.result.to_string()),
                    _ => None,
                }
            }
        }

        let mut table = Table::new(&["A", "Result"]);
        table.push(DataPoint {
            benchmark: "foo",
            result: 10000000,
        });
        table.push(DataPoint {
            benchmark: "long name",
            result: 1,
        });
        table.align(0, Alignment::Left);
        table.align(1, Alignment::Right);
        println!("{}", table);
    }
}
