//! Arrow IPC transport for data serialization.

use std::io::Cursor;

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;

use common_error::{GrismError, GrismResult};

/// Arrow IPC transport for serializing/deserializing record batches.
pub struct ArrowTransport;

impl ArrowTransport {
    /// Serialize record batches to Arrow IPC format.
    pub fn serialize(batches: &[RecordBatch]) -> GrismResult<Vec<u8>> {
        if batches.is_empty() {
            return Ok(Vec::new());
        }

        let schema = batches[0].schema();
        let mut buffer = Vec::new();

        {
            let mut writer = StreamWriter::try_new(&mut buffer, &schema).map_err(|e| {
                GrismError::InternalError(format!("Failed to create IPC writer: {e}"))
            })?;

            for batch in batches {
                writer.write(batch).map_err(|e| {
                    GrismError::InternalError(format!("Failed to write batch: {e}"))
                })?;
            }

            writer.finish().map_err(|e| {
                GrismError::InternalError(format!("Failed to finish IPC stream: {e}"))
            })?;
        }

        Ok(buffer)
    }

    /// Deserialize record batches from Arrow IPC format.
    pub fn deserialize(data: &[u8]) -> GrismResult<Vec<RecordBatch>> {
        if data.is_empty() {
            return Ok(Vec::new());
        }

        let cursor = Cursor::new(data);
        let reader = StreamReader::try_new(cursor, None)
            .map_err(|e| GrismError::InternalError(format!("Failed to create IPC reader: {e}")))?;

        let batches: Result<Vec<_>, _> = reader.collect();
        batches.map_err(|e| GrismError::InternalError(format!("Failed to read batches: {e}")))
    }

    /// Get the schema from IPC data without deserializing all batches.
    pub fn read_schema(data: &[u8]) -> GrismResult<arrow_schema::SchemaRef> {
        if data.is_empty() {
            return Err(GrismError::ValueError("Empty IPC data".to_string()));
        }

        let cursor = Cursor::new(data);
        let reader = StreamReader::try_new(cursor, None)
            .map_err(|e| GrismError::InternalError(format!("Failed to read schema: {e}")))?;

        Ok(reader.schema())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_roundtrip_empty() {
        let serialized = ArrowTransport::serialize(&[]).unwrap();
        let deserialized = ArrowTransport::deserialize(&serialized).unwrap();
        assert!(deserialized.is_empty());
    }

    #[test]
    fn test_roundtrip_with_data() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let id_array = Int64Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec![Some("Alice"), Some("Bob"), None]);

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)]).unwrap();

        let serialized = ArrowTransport::serialize(&[batch.clone()]).unwrap();
        let deserialized = ArrowTransport::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.len(), 1);
        assert_eq!(deserialized[0].num_rows(), 3);
    }
}
