//! Utility functions for the playground.
//!
//! This module provides formatting and display utilities for
//! working with query results.

use std::fmt::Write;

use arrow::record_batch::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_schema::DataType;

use grism_engine::ExecutionResult;

/// Print execution results in a formatted table.
pub fn print_results(result: &ExecutionResult) {
    println!("\n{}", "=".repeat(60));
    println!("Query Results");
    println!("{}", "=".repeat(60));

    if result.is_empty() {
        println!("(empty result set)");
        println!("{}", "=".repeat(60));
        return;
    }

    // Print schema
    let schema = result.schema();
    print!("| ");
    for field in schema.arrow_schema().fields() {
        print!("{:15} | ", field.name());
    }
    println!();

    // Print separator
    print!("|");
    for _ in schema.arrow_schema().fields() {
        print!("{:-<17}|", "");
    }
    println!();

    // Print rows
    let mut row_count = 0;
    for batch in &result.batches {
        for row in 0..batch.num_rows() {
            print!("| ");
            for (col_idx, col) in batch.columns().iter().enumerate() {
                let value = format_value(col, row);
                print!("{:15} | ", truncate(&value, 15));
            }
            println!();
            row_count += 1;

            // Limit output for large results
            if row_count >= 100 {
                println!("... (showing first 100 of {} rows)", result.total_rows());
                break;
            }
        }
        if row_count >= 100 {
            break;
        }
    }

    println!("{}", "=".repeat(60));
    println!("Total rows: {}", result.total_rows());
    println!("Execution time: {:?}", result.elapsed);
    println!("{}", "=".repeat(60));
}

/// Format a single batch as a string table.
pub fn format_batch(batch: &RecordBatch) -> String {
    let mut output = String::new();

    // Header
    write!(output, "| ").unwrap();
    for field in batch.schema().fields() {
        write!(output, "{:15} | ", field.name()).unwrap();
    }
    writeln!(output).unwrap();

    // Separator
    write!(output, "|").unwrap();
    for _ in batch.schema().fields() {
        write!(output, "{:-<17}|", "").unwrap();
    }
    writeln!(output).unwrap();

    // Rows
    for row in 0..batch.num_rows().min(50) {
        write!(output, "| ").unwrap();
        for col in batch.columns() {
            let value = format_value(col, row);
            write!(output, "{:15} | ", truncate(&value, 15)).unwrap();
        }
        writeln!(output).unwrap();
    }

    if batch.num_rows() > 50 {
        writeln!(output, "... ({} more rows)", batch.num_rows() - 50).unwrap();
    }

    output
}

/// Format an Arrow array value at a specific row.
fn format_value(array: &arrow_array::ArrayRef, row: usize) -> String {
    if array.is_null(row) {
        return "NULL".to_string();
    }

    match array.data_type() {
        DataType::Null => "NULL".to_string(),
        DataType::Boolean => {
            let arr = array.as_boolean();
            arr.value(row).to_string()
        }
        DataType::Int8 => {
            let arr = array.as_primitive::<arrow_array::types::Int8Type>();
            arr.value(row).to_string()
        }
        DataType::Int16 => {
            let arr = array.as_primitive::<arrow_array::types::Int16Type>();
            arr.value(row).to_string()
        }
        DataType::Int32 => {
            let arr = array.as_primitive::<arrow_array::types::Int32Type>();
            arr.value(row).to_string()
        }
        DataType::Int64 => {
            let arr = array.as_primitive::<arrow_array::types::Int64Type>();
            arr.value(row).to_string()
        }
        DataType::UInt8 => {
            let arr = array.as_primitive::<arrow_array::types::UInt8Type>();
            arr.value(row).to_string()
        }
        DataType::UInt16 => {
            let arr = array.as_primitive::<arrow_array::types::UInt16Type>();
            arr.value(row).to_string()
        }
        DataType::UInt32 => {
            let arr = array.as_primitive::<arrow_array::types::UInt32Type>();
            arr.value(row).to_string()
        }
        DataType::UInt64 => {
            let arr = array.as_primitive::<arrow_array::types::UInt64Type>();
            arr.value(row).to_string()
        }
        DataType::Float32 => {
            let arr = array.as_primitive::<arrow_array::types::Float32Type>();
            format!("{:.2}", arr.value(row))
        }
        DataType::Float64 => {
            let arr = array.as_primitive::<arrow_array::types::Float64Type>();
            format!("{:.2}", arr.value(row))
        }
        DataType::Utf8 => {
            let arr = array.as_string::<i32>();
            arr.value(row).to_string()
        }
        DataType::LargeUtf8 => {
            let arr = array.as_string::<i64>();
            arr.value(row).to_string()
        }
        _ => format!("{:?}", array.data_type()),
    }
}

/// Truncate a string to a maximum length.
fn truncate(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len.saturating_sub(3)])
    }
}

/// Print a divider line.
pub fn print_divider() {
    println!("{}", "-".repeat(60));
}

/// Print a section header.
pub fn print_header(title: &str) {
    println!();
    println!("{}", "=".repeat(60));
    println!("  {}", title);
    println!("{}", "=".repeat(60));
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_format_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("Alice"), Some("Bob"), None])),
            ],
        )
        .unwrap();

        let output = format_batch(&batch);
        assert!(output.contains("id"));
        assert!(output.contains("name"));
        assert!(output.contains("Alice"));
        assert!(output.contains("NULL"));
    }

    #[test]
    fn test_truncate() {
        assert_eq!(truncate("hello", 10), "hello");
        assert_eq!(truncate("hello world", 8), "hello...");
    }
}
