//! Normalize Snowflake's Arrow IPC physical types onto the schema in [`crate::schema`].

use std::io::Cursor;
use std::sync::Arc;

use arrow_array::builder::{Decimal128Builder, Float64Builder, Int64Builder};
use arrow_array::{
    types::*, Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array,
    FixedSizeListArray, Float64Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray,
    RecordBatch, StringArray, StructArray, Time64NanosecondArray,
};
use arrow_ipc::reader::StreamReader;
use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
use snowflake_core::{Error, Result, RowType, SessionParameters};

use crate::schema::{
    self, rescale_i128, timestamp_unit, unit_digits, ArrowOptions, TimestampTzMode,
};

pub fn batches_from_ipc(
    bytes: &[u8],
    row_type: &[RowType],
    params: &SessionParameters,
    opts: &ArrowOptions,
) -> Result<Vec<RecordBatch>> {
    let schema = schema::schema_for(row_type, params, opts)?;
    if bytes.is_empty() {
        return Ok(vec![RecordBatch::new_empty(Arc::new(schema))]);
    }
    let reader = StreamReader::try_new(Cursor::new(bytes), None)
        .map_err(|err| Error::protocol(format!("arrow ipc: {err}")))?;
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|err| Error::protocol(format!("arrow ipc batch: {err}")))?;
        out.push(normalize_batch(&batch, row_type, params, opts, &schema)?);
    }
    if out.is_empty() {
        out.push(RecordBatch::new_empty(Arc::new(schema)));
    }
    Ok(out)
}

fn normalize_batch(
    batch: &RecordBatch,
    row_type: &[RowType],
    params: &SessionParameters,
    opts: &ArrowOptions,
    schema: &Schema,
) -> Result<RecordBatch> {
    if batch.num_columns() != row_type.len() {
        return Err(Error::protocol(format!(
            "arrow batch has {} columns, rowtype has {}",
            batch.num_columns(),
            row_type.len()
        )));
    }
    let mut columns = Vec::with_capacity(row_type.len());
    for (index, column) in row_type.iter().enumerate() {
        let batch_schema = batch.schema();
        let field = batch_schema.field(index);
        let logical = field
            .metadata()
            .get("logicalType")
            .map(String::as_str)
            .ok_or_else(|| {
                Error::decode(
                    &column.name,
                    0,
                    column.logical_type(),
                    None,
                    "arrow field is missing logicalType metadata",
                )
            })?;
        if !logical.eq_ignore_ascii_case(column.logical_type()) {
            return Err(Error::decode(
                &column.name,
                0,
                column.logical_type(),
                Some(logical.to_string()),
                "arrow logicalType does not match rowtype",
            ));
        }
        columns.push(normalize_column(
            batch.column(index),
            field,
            column,
            params,
            opts,
        )?);
    }
    RecordBatch::try_new(Arc::new(schema.clone()), columns)
        .map_err(|err| Error::protocol(format!("normalized record batch: {err}")))
}

fn normalize_column(
    array: &ArrayRef,
    field: &Field,
    column: &RowType,
    params: &SessionParameters,
    opts: &ArrowOptions,
) -> Result<ArrayRef> {
    let logical = column.logical_type().to_ascii_lowercase();
    let normalized = match logical.as_str() {
        "fixed" => normalize_fixed(array, field, column, opts),
        "real" => expect_float64(array, column),
        "text" | "variant" | "object" | "array" | "geography" | "geometry" => {
            expect_utf8(array, column)
        }
        "binary" => expect_binary(array, column),
        "boolean" => expect_bool(array, column),
        "date" => expect_date(array, column),
        "time" => normalize_time(array, field, column),
        "timestamp_ntz" => normalize_timestamp(array, field, column, params, opts, false),
        "timestamp_ltz" => normalize_timestamp(array, field, column, params, opts, false),
        "timestamp_tz" => normalize_timestamp(array, field, column, params, opts, true),
        "vector" => normalize_vector(array, column),
        other => Err(Error::decode(
            &column.name,
            0,
            other,
            None,
            "unsupported logical type",
        )),
    };
    normalized.map_err(|err| annotate(err, column))
}

fn annotate(err: Error, column: &RowType) -> Error {
    match err {
        Error::Protocol(reason) => {
            Error::decode(&column.name, 0, column.logical_type(), None, reason)
        }
        other => other,
    }
}

fn meta_i64(field: &Field, key: &str) -> Option<i64> {
    field.metadata().get(key)?.parse().ok()
}

fn physical_rejected(column: &RowType, data_type: &DataType) -> Error {
    Error::decode(
        &column.name,
        0,
        column.logical_type(),
        Some(format!("{data_type:?}")),
        "unexpected arrow physical type",
    )
}

fn normalize_fixed(
    array: &ArrayRef,
    field: &Field,
    column: &RowType,
    opts: &ArrowOptions,
) -> Result<ArrayRef> {
    let precision = column.precision_or(38);
    let scale = column.scale_or(0);
    let source_scale = meta_i64(field, "scale").unwrap_or(scale);
    if source_scale != scale {
        return Err(Error::decode(
            &column.name,
            0,
            "fixed",
            None,
            format!("arrow scale {source_scale} does not match rowtype scale {scale}"),
        ));
    }
    let values = signed_values(array, column)?;
    if !opts.use_high_precision {
        return fixed_low_precision(&values, column, scale);
    }
    if let Some(decimal) = array.as_any().downcast_ref::<Decimal128Array>() {
        let DataType::Decimal128(p, s) = decimal.data_type() else {
            return Err(physical_rejected(column, array.data_type()));
        };
        if i64::from(*p) != precision || i64::from(*s) != scale {
            return Err(Error::decode(
                &column.name,
                0,
                "fixed",
                None,
                format!("decimal({p}, {s}) does not match rowtype ({precision}, {scale})"),
            ));
        }
    }
    let p = u8::try_from(precision).map_err(|_| {
        Error::decode(
            &column.name,
            0,
            "fixed",
            None,
            format!("precision {precision} does not fit Decimal128"),
        )
    })?;
    let s = i8::try_from(scale).map_err(|_| {
        Error::decode(
            &column.name,
            0,
            "fixed",
            None,
            format!("scale {scale} does not fit Decimal128"),
        )
    })?;
    let mut builder = Decimal128Builder::with_capacity(values.len())
        .with_precision_and_scale(p, s)
        .map_err(|err| Error::decode(&column.name, 0, "fixed", None, err.to_string()))?;
    for (row, value) in values.iter().enumerate() {
        match value {
            None => builder.append_null(),
            Some(value) => {
                let digits = if *value == 0 {
                    1
                } else {
                    value.unsigned_abs().to_string().len() as i64
                };
                if digits > precision {
                    return Err(Error::decode(
                        &column.name,
                        row,
                        "fixed",
                        Some(value.to_string()),
                        format!("{digits} significant digits exceed precision {precision}"),
                    ));
                }
                builder.append_value(*value);
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn fixed_low_precision(values: &[Option<i128>], column: &RowType, scale: i64) -> Result<ArrayRef> {
    if scale == 0 {
        let mut out = Int64Builder::with_capacity(values.len());
        for (row, value) in values.iter().enumerate() {
            match value {
                None => out.append_null(),
                Some(value) => {
                    let value = i64::try_from(*value).map_err(|_| {
                        Error::decode(
                            &column.name,
                            row,
                            "fixed",
                            Some(value.to_string()),
                            "value does not fit Int64",
                        )
                    })?;
                    out.append_value(value);
                }
            }
        }
        return Ok(Arc::new(out.finish()));
    }
    let factor = schema::pow10_i128(scale)?;
    let mut out = Float64Builder::with_capacity(values.len());
    for (row, value) in values.iter().enumerate() {
        match value {
            None => out.append_null(),
            Some(value) => {
                let value = (*value as f64) / (factor as f64);
                if !value.is_finite() {
                    return Err(Error::decode(
                        &column.name,
                        row,
                        "fixed",
                        Some(value.to_string()),
                        "value is not a finite Float64",
                    ));
                }
                out.append_value(value);
            }
        }
    }
    Ok(Arc::new(out.finish()))
}

fn signed_values(array: &ArrayRef, column: &RowType) -> Result<Vec<Option<i128>>> {
    let mut out = Vec::with_capacity(array.len());
    match array.data_type() {
        DataType::Int8 => push_primitive::<Int8Type>(array, column, &mut out)?,
        DataType::Int16 => push_primitive::<Int16Type>(array, column, &mut out)?,
        DataType::Int32 => push_primitive::<Int32Type>(array, column, &mut out)?,
        DataType::Int64 => push_primitive::<Int64Type>(array, column, &mut out)?,
        DataType::Decimal128(_, _) => {
            let decimal = prim::<Decimal128Type>(array, column)?;
            for index in 0..decimal.len() {
                out.push(if decimal.is_null(index) {
                    None
                } else {
                    Some(decimal.value(index))
                });
            }
        }
        other => return Err(physical_rejected(column, other)),
    }
    Ok(out)
}

fn push_primitive<T>(array: &ArrayRef, column: &RowType, out: &mut Vec<Option<i128>>) -> Result<()>
where
    T: ArrowPrimitiveType,
    T::Native: Into<i128>,
{
    let primitive = prim::<T>(array, column)?;
    for index in 0..primitive.len() {
        out.push(if primitive.is_null(index) {
            None
        } else {
            Some(primitive.value(index).into())
        });
    }
    Ok(())
}

fn prim<'a, T: ArrowPrimitiveType>(
    array: &'a ArrayRef,
    column: &RowType,
) -> Result<&'a arrow_array::PrimitiveArray<T>> {
    array
        .as_any()
        .downcast_ref()
        .ok_or_else(|| physical_rejected(column, array.data_type()))
}

fn expect_float64(array: &ArrayRef, column: &RowType) -> Result<ArrayRef> {
    array
        .as_any()
        .downcast_ref::<Float64Array>()
        .map(|array| Arc::new(array.clone()) as ArrayRef)
        .ok_or_else(|| physical_rejected(column, array.data_type()))
}

fn expect_bool(array: &ArrayRef, column: &RowType) -> Result<ArrayRef> {
    array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .map(|array| Arc::new(array.clone()) as ArrayRef)
        .ok_or_else(|| physical_rejected(column, array.data_type()))
}

fn expect_date(array: &ArrayRef, column: &RowType) -> Result<ArrayRef> {
    if let Some(array) = array.as_any().downcast_ref::<Date32Array>() {
        return Ok(Arc::new(array.clone()));
    }
    if let Some(array) = array.as_any().downcast_ref::<Int32Array>() {
        let values: Vec<Option<i32>> = (0..array.len())
            .map(|index| {
                if array.is_null(index) {
                    None
                } else {
                    Some(array.value(index))
                }
            })
            .collect();
        return Ok(Arc::new(Date32Array::from(values)));
    }
    Err(physical_rejected(column, array.data_type()))
}

fn expect_utf8(array: &ArrayRef, column: &RowType) -> Result<ArrayRef> {
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(Arc::new(array.clone()));
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
        let mut total = 0i64;
        for index in 0..array.len() {
            if array.is_valid(index) {
                total += array.value(index).len() as i64;
            }
        }
        if total > i64::from(i32::MAX) {
            return Err(Error::decode(
                &column.name,
                0,
                column.logical_type(),
                None,
                "large utf8 does not fit utf8 offsets",
            ));
        }
        let mut values = Vec::with_capacity(array.len());
        for index in 0..array.len() {
            values.push(if array.is_null(index) {
                None
            } else {
                Some(array.value(index).to_string())
            });
        }
        return Ok(Arc::new(StringArray::from(values)));
    }
    Err(physical_rejected(column, array.data_type()))
}

fn expect_binary(array: &ArrayRef, column: &RowType) -> Result<ArrayRef> {
    if let Some(array) = array.as_any().downcast_ref::<BinaryArray>() {
        return Ok(Arc::new(array.clone()));
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeBinaryArray>() {
        let mut values = Vec::with_capacity(array.len());
        for index in 0..array.len() {
            values.push(if array.is_null(index) {
                None
            } else {
                Some(array.value(index).to_vec())
            });
        }
        let mut builder = arrow_array::builder::BinaryBuilder::new();
        for value in values {
            match value {
                None => builder.append_null(),
                Some(value) => builder.append_value(value),
            }
        }
        return Ok(Arc::new(builder.finish()));
    }
    Err(physical_rejected(column, array.data_type()))
}

fn normalize_time(array: &ArrayRef, field: &Field, column: &RowType) -> Result<ArrayRef> {
    if let Some(array) = array.as_any().downcast_ref::<Time64NanosecondArray>() {
        return Ok(Arc::new(array.clone()));
    }
    let scale = meta_i64(field, "scale").or(column.scale).unwrap_or(9);
    let values = match array.data_type() {
        DataType::Int32 => prim::<Int32Type>(array, column)?
            .iter()
            .map(|v| v.map(i128::from))
            .collect::<Vec<_>>(),
        DataType::Int64 => prim::<Int64Type>(array, column)?
            .iter()
            .map(|v| v.map(i128::from))
            .collect::<Vec<_>>(),
        other => return Err(physical_rejected(column, other)),
    };
    let mut out = Vec::with_capacity(values.len());
    for (row, value) in values.into_iter().enumerate() {
        let Some(value) = value else {
            out.push(None);
            continue;
        };
        let nanos = rescale_i128(value, scale, 9).map_err(|err| {
            Error::decode(
                &column.name,
                row,
                "time",
                Some(value.to_string()),
                err.to_string(),
            )
        })?;
        out.push(Some(i64::try_from(nanos).map_err(|_| {
            Error::decode(
                &column.name,
                row,
                "time",
                Some(value.to_string()),
                "time does not fit nanoseconds",
            )
        })?));
    }
    Ok(Arc::new(Time64NanosecondArray::from(out)))
}

fn normalize_timestamp(
    array: &ArrayRef,
    field: &Field,
    column: &RowType,
    params: &SessionParameters,
    opts: &ArrowOptions,
    with_offset: bool,
) -> Result<ArrayRef> {
    let unit = timestamp_unit(column.scale_or(9));
    let digits = unit_digits(&unit);
    let tz = timestamp_timezone(column, params, opts, with_offset);
    if let DataType::Struct(_) = array.data_type() {
        return normalize_timestamp_struct(
            array,
            column,
            unit,
            digits,
            tz.as_deref(),
            with_offset,
            opts,
        );
    }
    let scale = meta_i64(field, "scale").or(column.scale).unwrap_or(9);
    let DataType::Int64 = array.data_type() else {
        return Err(physical_rejected(column, array.data_type()));
    };
    let primitive = prim::<Int64Type>(array, column)?;
    let mut values = Vec::with_capacity(primitive.len());
    for index in 0..primitive.len() {
        if primitive.is_null(index) {
            values.push(None);
            continue;
        }
        let value = i128::from(primitive.value(index));
        let scaled = rescale_i128(value, scale, digits).map_err(|err| {
            Error::decode(
                &column.name,
                index,
                column.logical_type(),
                Some(value.to_string()),
                err.to_string(),
            )
        })?;
        values.push(Some(i64::try_from(scaled).map_err(|_| {
            Error::decode(
                &column.name,
                index,
                column.logical_type(),
                Some(value.to_string()),
                "timestamp does not fit the target unit",
            )
        })?));
    }
    Ok(timestamp_array(unit, tz.as_deref(), &values))
}

fn timestamp_timezone(
    column: &RowType,
    params: &SessionParameters,
    opts: &ArrowOptions,
    with_offset: bool,
) -> Option<String> {
    if with_offset {
        Some("UTC".into())
    } else if column.logical_type().eq_ignore_ascii_case("timestamp_ltz") {
        Some(schema::ltz_timezone(params, opts))
    } else {
        None
    }
}

fn normalize_timestamp_struct(
    array: &ArrayRef,
    column: &RowType,
    unit: TimeUnit,
    digits: i64,
    tz: Option<&str>,
    with_offset: bool,
    opts: &ArrowOptions,
) -> Result<ArrayRef> {
    let struct_array = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| physical_rejected(column, array.data_type()))?;
    let epoch = child_i64(struct_array, &["epoch"], column)?;
    let fraction = child_i32_optional(struct_array, &["fraction"])?;
    let timezone = child_i32_optional(struct_array, &["timezone", "tz"])?;
    if with_offset && timezone.is_none() {
        return Err(Error::decode(
            &column.name,
            0,
            column.logical_type(),
            None,
            "timestamp_tz struct is missing timezone",
        ));
    }
    if !with_offset && fraction.is_none() && timezone.is_some() {
        return Err(Error::decode(
            &column.name,
            0,
            column.logical_type(),
            None,
            "timestamp struct fields do not match logical type",
        ));
    }
    let mut instants = Vec::with_capacity(struct_array.len());
    let mut offsets = Vec::with_capacity(struct_array.len());
    for index in 0..struct_array.len() {
        if struct_array.is_null(index) {
            if epoch.is_valid(index) || fraction.as_ref().is_some_and(|f| f.is_valid(index)) {
                return Err(Error::decode(
                    &column.name,
                    index,
                    column.logical_type(),
                    None,
                    "null timestamp struct has a non-null child",
                ));
            }
            instants.push(None);
            offsets.push(None);
            continue;
        }
        let epoch_value = if epoch.is_null(index) {
            return Err(Error::decode(
                &column.name,
                index,
                column.logical_type(),
                None,
                "timestamp epoch is null while the struct is not",
            ));
        } else {
            i128::from(epoch.value(index))
        };
        let nanos = if let Some(fraction) = &fraction {
            if fraction.is_null(index) {
                return Err(Error::decode(
                    &column.name,
                    index,
                    column.logical_type(),
                    None,
                    "timestamp fraction is null while the struct is not",
                ));
            }
            let fraction = i128::from(fraction.value(index));
            let whole = rescale_i128(epoch_value, 0, 9)?;
            whole.checked_add(fraction).ok_or_else(|| {
                Error::decode(
                    &column.name,
                    index,
                    column.logical_type(),
                    None,
                    "timestamp epoch and fraction overflow",
                )
            })?
        } else {
            rescale_i128(epoch_value, column.scale_or(0), 9)?
        };
        let scaled = rescale_i128(nanos, 9, digits)?;
        instants.push(Some(i64::try_from(scaled).map_err(|_| {
            Error::decode(
                &column.name,
                index,
                column.logical_type(),
                None,
                "timestamp does not fit the target unit",
            )
        })?));
        if let Some(timezone) = &timezone {
            if timezone.is_null(index) {
                return Err(Error::decode(
                    &column.name,
                    index,
                    column.logical_type(),
                    None,
                    "timestamp timezone is null while the struct is not",
                ));
            }
            offsets.push(Some(timezone.value(index) - 1440));
        } else {
            offsets.push(None);
        }
    }
    let epoch_array = timestamp_array(unit, tz, &instants);
    if with_offset && opts.timestamp_tz == TimestampTzMode::Struct {
        let offset_array = Int32Array::from(offsets);
        let fields = Fields::from(vec![
            Field::new("epoch", epoch_array.data_type().clone(), true),
            Field::new("offset_minutes", DataType::Int32, true),
        ]);
        let combined = StructArray::try_new(
            fields,
            vec![epoch_array, Arc::new(offset_array)],
            struct_array.nulls().cloned(),
        )
        .map_err(|err| Error::protocol(format!("timestamp_tz struct: {err}")))?;
        return Ok(Arc::new(combined));
    }
    Ok(epoch_array)
}

fn timestamp_array(unit: TimeUnit, tz: Option<&str>, values: &[Option<i64>]) -> ArrayRef {
    match unit {
        TimeUnit::Second => {
            let array = arrow_array::TimestampSecondArray::from(values.to_vec());
            Arc::new(match tz {
                Some(tz) => array.with_timezone(tz),
                None => array,
            })
        }
        TimeUnit::Millisecond => {
            let array = arrow_array::TimestampMillisecondArray::from(values.to_vec());
            Arc::new(match tz {
                Some(tz) => array.with_timezone(tz),
                None => array,
            })
        }
        TimeUnit::Microsecond => {
            let array = arrow_array::TimestampMicrosecondArray::from(values.to_vec());
            Arc::new(match tz {
                Some(tz) => array.with_timezone(tz),
                None => array,
            })
        }
        TimeUnit::Nanosecond => {
            let array = arrow_array::TimestampNanosecondArray::from(values.to_vec());
            Arc::new(match tz {
                Some(tz) => array.with_timezone(tz),
                None => array,
            })
        }
    }
}

fn child_i64(array: &StructArray, names: &[&str], column: &RowType) -> Result<Int64Array> {
    let child = find_child(array, names).ok_or_else(|| {
        Error::decode(
            &column.name,
            0,
            column.logical_type(),
            None,
            format!("struct is missing {}", names.join("/")),
        )
    })?;
    child
        .as_any()
        .downcast_ref::<Int64Array>()
        .cloned()
        .ok_or_else(|| physical_rejected(column, child.data_type()))
}

fn child_i32_optional(array: &StructArray, names: &[&str]) -> Result<Option<Int32Array>> {
    let Some(child) = find_child(array, names) else {
        return Ok(None);
    };
    child
        .as_any()
        .downcast_ref::<Int32Array>()
        .cloned()
        .map(Some)
        .ok_or_else(|| Error::protocol(format!("{:?} is not int32", child.data_type())))
}

fn find_child<'a>(array: &'a StructArray, names: &[&str]) -> Option<&'a ArrayRef> {
    array
        .column_names()
        .iter()
        .position(|name| {
            names
                .iter()
                .any(|candidate| name.eq_ignore_ascii_case(candidate))
        })
        .map(|index| array.column(index))
}

fn normalize_vector(array: &ArrayRef, column: &RowType) -> Result<ArrayRef> {
    let Some(list) = array.as_any().downcast_ref::<FixedSizeListArray>() else {
        return Err(physical_rejected(column, array.data_type()));
    };
    let expected = column.length.unwrap_or(0);
    if i64::from(list.value_length()) != expected {
        return Err(Error::decode(
            &column.name,
            0,
            "vector",
            None,
            format!(
                "vector width {} does not match rowtype length {expected}",
                list.value_length()
            ),
        ));
    }
    match list.values().data_type() {
        DataType::Float32 | DataType::Int32 => Ok(Arc::new(list.clone())),
        other => Err(physical_rejected(column, other)),
    }
}
