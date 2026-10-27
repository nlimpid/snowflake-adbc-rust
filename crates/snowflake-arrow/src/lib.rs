//! Strict Arrow conversion for Snowflake results.
//!
//! JSON rowsets and Arrow IPC chunks for the same `rowtype` produce the same
//! schema. A cell that does not match the declared type is [`Error::Decode`](snowflake_core::Error::Decode),
//! not a NULL.

mod json;
mod normalize;
mod schema;
mod stream;

pub use schema::{
    data_type_for, field_for, schema_for, timestamp_unit, ArrowOptions, TimestampTzMode,
    MD_BYTE_LENGTH, MD_LENGTH, MD_LOGICAL_TYPE, MD_PRECISION, MD_SCALE,
};
pub use stream::{record_batches, record_batches_with, BlockingBatchReader};

pub const ARROW_RS_VERSION: &str = "59";

use arrow_array::RecordBatch;

/// Decode every chunk of `result` into record batches.
pub async fn collect(
    result: snowflake_core::QueryResult,
    opts: ArrowOptions,
) -> snowflake_core::Result<Vec<RecordBatch>> {
    use futures::StreamExt;
    let mut out = Vec::new();
    let mut stream = std::pin::pin!(record_batches_with(result, opts));
    while let Some(batch) = stream.next().await {
        out.push(batch?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::{Array, Decimal128Array, Int64Array, RecordBatch};
    use arrow_ipc::writer::StreamWriter;
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use snowflake_core::{RowType, SessionParameters};

    fn fixed_column() -> RowType {
        RowType {
            name: "V".into(),
            database: String::new(),
            schema: String::new(),
            table: String::new(),
            logical_type: "fixed".into(),
            nullable: true,
            precision: Some(10),
            scale: Some(2),
            length: None,
            byte_length: None,
        }
    }

    fn ipc_for(batch: &RecordBatch) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut writer = StreamWriter::try_new(&mut buf, &batch.schema()).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
        buf
    }

    #[test]
    fn json_and_arrow_number_agree() {
        let column = fixed_column();
        let params = SessionParameters::default();
        let opts = ArrowOptions::default();
        let json = crate::json::batch_from_json(
            std::slice::from_ref(&column),
            &[vec![Some("123.45".into())], vec![None]],
            &params,
            &opts,
        )
        .unwrap();

        let mut metadata = std::collections::HashMap::new();
        metadata.insert("logicalType".into(), "FIXED".into());
        metadata.insert("scale".into(), "2".into());
        metadata.insert("precision".into(), "10".into());
        let physical = Int64Array::from(vec![Some(12345), None]);
        let field = Field::new("V", DataType::Int64, true).with_metadata(metadata);
        let physical_batch =
            RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![Arc::new(physical)])
                .unwrap();
        let ipc = ipc_for(&physical_batch);
        let arrow = crate::normalize::batches_from_ipc(&ipc, &[column], &params, &opts).unwrap();
        assert_eq!(arrow.len(), 1);
        assert_eq!(arrow[0], json);
        let decimal = arrow[0]
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(decimal.value(0), 12345);
        assert!(decimal.is_null(1));
    }

    #[test]
    fn timestamp_struct_normalizes_to_nanoseconds() {
        use arrow_array::{Int32Array, StructArray};
        use arrow_schema::Fields;

        let column = RowType {
            name: "T".into(),
            database: String::new(),
            schema: String::new(),
            table: String::new(),
            logical_type: "timestamp_ntz".into(),
            nullable: true,
            precision: Some(0),
            scale: Some(9),
            length: None,
            byte_length: None,
        };
        let epoch = Int64Array::from(vec![1_700_000_000_i64]);
        let fraction = Int32Array::from(vec![123_000_000_i32]);
        let mut epoch_meta = std::collections::HashMap::new();
        epoch_meta.insert("logicalType".into(), "TIMESTAMP_NTZ".into());
        epoch_meta.insert("scale".into(), "9".into());
        let struct_array = StructArray::try_new(
            Fields::from(vec![
                Field::new("epoch", DataType::Int64, true),
                Field::new("fraction", DataType::Int32, true),
            ]),
            vec![Arc::new(epoch), Arc::new(fraction)],
            None,
        )
        .unwrap();
        let field =
            Field::new("T", struct_array.data_type().clone(), true).with_metadata(epoch_meta);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![field])),
            vec![Arc::new(struct_array)],
        )
        .unwrap();
        let normalized = crate::normalize::batches_from_ipc(
            &ipc_for(&batch),
            std::slice::from_ref(&column),
            &SessionParameters::default(),
            &ArrowOptions::default(),
        )
        .unwrap();
        let from_json = crate::json::batch_from_json(
            &[column],
            &[vec![Some("1700000000.123000000".into())]],
            &SessionParameters::default(),
            &ArrowOptions::default(),
        )
        .unwrap();
        assert_eq!(
            normalized[0].schema().field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        assert_eq!(normalized[0], from_json);
    }

    #[tokio::test]
    async fn live_both_formats_agree_on_number() {
        let Some(session) = snowflake_testing::test_session().await else {
            return;
        };
        let (arrow, json) = snowflake_testing::exec_both_formats(
            &session,
            "select 123.456::number(10,3) as v, null::number(10,3)",
        )
        .await
        .unwrap();
        assert_eq!(arrow, json);
        assert_eq!(
            arrow[0].schema().field(0).data_type(),
            &DataType::Decimal128(10, 3)
        );
        session.close().await.unwrap();
    }
}
