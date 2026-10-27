//! `get_info` and `get_table_types` record batches.

use std::sync::Arc;

use adbc_core::constants::ADBC_VERSION_1_1_0;
use adbc_core::options::InfoCode;
use arrow_array::builder::{BooleanBuilder, Int64Builder, StringBuilder, UInt32Builder};
use arrow_array::{new_empty_array, ArrayRef, RecordBatch, RecordBatchReader, UnionArray};
use arrow_buffer::ScalarBuffer;
use arrow_schema::{
    ArrowError, DataType, Field, Fields, Schema, SchemaRef, UnionFields, UnionMode,
};

use crate::error::adbc_error;

const TABLE_TYPES: &[&str] = &[
    "TABLE",
    "VIEW",
    "TEMPORARY TABLE",
    "TRANSIENT TABLE",
    "EXTERNAL TABLE",
    "DYNAMIC TABLE",
    "HYBRID TABLE",
];

pub(crate) struct OnceReader {
    schema: SchemaRef,
    batch: Option<RecordBatch>,
}

impl Iterator for OnceReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.batch.take().map(Ok)
    }
}

impl RecordBatchReader for OnceReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

pub fn table_types_reader() -> adbc_core::error::Result<Box<dyn RecordBatchReader + Send>> {
    let mut builder = StringBuilder::new();
    for name in TABLE_TYPES {
        builder.append_value(name);
    }
    let schema = Arc::new(Schema::new(vec![Field::new(
        "table_type",
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(builder.finish())])
        .map_err(|err| adbc_error(snowflake_core::Error::protocol(err.to_string())))?;
    Ok(Box::new(OnceReader {
        schema,
        batch: Some(batch),
    }))
}

pub fn info_reader(
    codes: Option<std::collections::HashSet<InfoCode>>,
    server_version: &str,
) -> adbc_core::error::Result<Box<dyn RecordBatchReader + Send>> {
    let wanted = |code: InfoCode| codes.as_ref().is_none_or(|set| set.contains(&code));
    let mut rows: Vec<(u32, InfoValue)> = Vec::new();
    let push = |rows: &mut Vec<(u32, InfoValue)>, code: InfoCode, value: InfoValue| {
        if wanted(code) {
            rows.push((u32::from(&code), value));
        }
    };
    push(
        &mut rows,
        InfoCode::VendorName,
        InfoValue::String(Some("Snowflake".into())),
    );
    push(
        &mut rows,
        InfoCode::VendorVersion,
        InfoValue::String(Some(server_version.to_string())),
    );
    push(
        &mut rows,
        InfoCode::VendorArrowVersion,
        InfoValue::String(None),
    );
    push(&mut rows, InfoCode::VendorSql, InfoValue::Bool(true));
    push(&mut rows, InfoCode::VendorSubstrait, InfoValue::Bool(false));
    push(
        &mut rows,
        InfoCode::VendorSubstraitMinVersion,
        InfoValue::String(None),
    );
    push(
        &mut rows,
        InfoCode::VendorSubstraitMaxVersion,
        InfoValue::String(None),
    );
    push(
        &mut rows,
        InfoCode::DriverName,
        InfoValue::String(Some("snowflake-adbc-rs".into())),
    );
    push(
        &mut rows,
        InfoCode::DriverVersion,
        InfoValue::String(Some(env!("CARGO_PKG_VERSION").into())),
    );
    push(
        &mut rows,
        InfoCode::DriverArrowVersion,
        InfoValue::String(Some(snowflake_arrow::ARROW_RS_VERSION.into())),
    );
    push(
        &mut rows,
        InfoCode::DriverAdbcVersion,
        InfoValue::Int(i64::from(ADBC_VERSION_1_1_0)),
    );

    let list_field = Arc::new(Field::new("item", DataType::Utf8, true));
    let list_type = DataType::List(Arc::clone(&list_field));
    let map_entries = Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("key", DataType::Int32, false),
            Field::new(
                "value",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            ),
        ])),
        false,
    );
    let map_type = DataType::Map(Arc::new(map_entries), false);
    let union_fields = UnionFields::try_new(
        [0i8, 1, 2, 3, 4, 5],
        [
            Field::new("string_value", DataType::Utf8, true),
            Field::new("bool_value", DataType::Boolean, true),
            Field::new("int64_value", DataType::Int64, true),
            Field::new("int32_bitmask", DataType::Int32, true),
            Field::new("string_list", list_type.clone(), true),
            Field::new("int32_to_int32_list_map", map_type.clone(), true),
        ],
    )
    .map_err(|err| adbc_error(snowflake_core::Error::protocol(err.to_string())))?;

    let mut names = UInt32Builder::new();
    let mut type_ids = Vec::new();
    let mut offsets = Vec::new();
    let mut strings = StringBuilder::new();
    let mut bools = BooleanBuilder::new();
    let mut ints = Int64Builder::new();
    let mut string_len = 0i32;
    let mut bool_len = 0i32;
    let mut int_len = 0i32;
    for (code, value) in rows {
        names.append_value(code);
        match value {
            InfoValue::String(Some(text)) => {
                type_ids.push(0);
                offsets.push(string_len);
                strings.append_value(text);
                string_len += 1;
            }
            InfoValue::String(None) => {
                type_ids.push(0);
                offsets.push(string_len);
                strings.append_null();
                string_len += 1;
            }
            InfoValue::Bool(value) => {
                type_ids.push(1);
                offsets.push(bool_len);
                bools.append_value(value);
                bool_len += 1;
            }
            InfoValue::Int(value) => {
                type_ids.push(2);
                offsets.push(int_len);
                ints.append_value(value);
                int_len += 1;
            }
        }
    }
    let children: Vec<ArrayRef> = vec![
        Arc::new(strings.finish()),
        Arc::new(bools.finish()),
        Arc::new(ints.finish()),
        new_empty_array(&DataType::Int32),
        new_empty_array(&list_type),
        new_empty_array(&map_type),
    ];
    let union_array = UnionArray::try_new(
        union_fields.clone(),
        ScalarBuffer::from(type_ids),
        Some(ScalarBuffer::from(offsets)),
        children,
    )
    .map_err(|err| adbc_error(snowflake_core::Error::protocol(err.to_string())))?;
    let info_type = DataType::Union(union_fields, UnionMode::Dense);
    let schema = Arc::new(Schema::new(vec![
        Field::new("info_name", DataType::UInt32, false),
        Field::new("info_value", info_type, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(names.finish()), Arc::new(union_array)],
    )
    .map_err(|err| adbc_error(snowflake_core::Error::protocol(err.to_string())))?;
    Ok(Box::new(OnceReader {
        schema,
        batch: Some(batch),
    }))
}

enum InfoValue {
    String(Option<String>),
    Bool(bool),
    Int(i64),
}
