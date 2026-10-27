//! Strict JSON cell decoding. A bad cell is an error, never a NULL.

use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, FixedSizeListBuilder,
    Float32Builder, Float64Builder, Int32Builder, Int64Builder, StringBuilder,
    Time64NanosecondBuilder, TimestampMicrosecondBuilder, TimestampMillisecondBuilder,
    TimestampNanosecondBuilder, TimestampSecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch, StructArray};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field, Fields, TimeUnit};
use snowflake_core::{Error, Result, RowType, SessionParameters};

use crate::schema::{
    self, pow10_i128, rescale_i128, timestamp_unit, unit_digits, ArrowOptions, TimestampTzMode,
};

pub fn batch_from_json(
    row_type: &[RowType],
    rows: &[Vec<Option<String>>],
    params: &SessionParameters,
    opts: &ArrowOptions,
) -> Result<RecordBatch> {
    let schema = schema::schema_for(row_type, params, opts)?;
    let mut decoders = Vec::with_capacity(row_type.len());
    for column in row_type {
        decoders.push(decoder_for(column, params, opts)?);
    }
    for (row_index, row) in rows.iter().enumerate() {
        if row.len() != row_type.len() {
            return Err(Error::protocol(format!(
                "row {row_index} has {} cells, schema has {}",
                row.len(),
                row_type.len()
            )));
        }
        for (decoder, cell) in decoders.iter_mut().zip(row.iter()) {
            decoder.append(row_index, cell.as_deref())?;
        }
    }
    let mut columns = Vec::with_capacity(decoders.len());
    for decoder in decoders {
        columns.push(decoder.finish()?);
    }
    RecordBatch::try_new(Arc::new(schema), columns)
        .map_err(|err| Error::protocol(format!("json record batch: {err}")))
}

trait ColumnDecoder {
    fn append(&mut self, row: usize, raw: Option<&str>) -> Result<()>;
    fn finish(self: Box<Self>) -> Result<ArrayRef>;
}

fn decoder_for(
    column: &RowType,
    params: &SessionParameters,
    opts: &ArrowOptions,
) -> Result<Box<dyn ColumnDecoder>> {
    let logical = column.logical_type().to_ascii_lowercase();
    let decoder: Box<dyn ColumnDecoder> = match logical.as_str() {
        "fixed" => Box::new(FixedDecoder::new(column, opts)?),
        "real" => Box::new(RealDecoder::new(column)),
        "text" | "variant" | "object" | "array" | "geography" | "geometry" => {
            Box::new(TextDecoder::new(column))
        }
        "binary" => Box::new(BinaryDecoder::new(column)),
        "boolean" => Box::new(BoolDecoder::new(column)),
        "date" => Box::new(DateDecoder::new(column)),
        "time" => Box::new(TimeDecoder::new(column)),
        "timestamp_ntz" | "timestamp_ltz" => {
            Box::new(TimestampDecoder::new(column, params, opts, false)?)
        }
        "timestamp_tz" => Box::new(TimestampDecoder::new(column, params, opts, true)?),
        "vector" => Box::new(VectorDecoder::new(column)?),
        other => {
            return Err(Error::decode(
                &column.name,
                0,
                other,
                None,
                "unsupported logical type",
            ));
        }
    };
    Ok(decoder)
}

fn fail(column: &RowType, row: usize, raw: Option<&str>, reason: impl Into<String>) -> Error {
    Error::decode(
        column.name.clone(),
        row,
        column.logical_type().to_string(),
        raw.map(str::to_string),
        reason,
    )
}

struct FixedDecoder {
    column: RowType,
    precision: i64,
    scale: i64,
    high: bool,
    decimal: Decimal128Builder,
    int: Int64Builder,
    float: Float64Builder,
}

impl FixedDecoder {
    fn new(column: &RowType, opts: &ArrowOptions) -> Result<Self> {
        let precision = column.precision_or(38);
        let scale = column.scale_or(0);
        let mut decimal = Decimal128Builder::with_capacity(0);
        if opts.use_high_precision {
            let p = u8::try_from(precision).map_err(|_| {
                fail(
                    column,
                    0,
                    None,
                    format!("precision {precision} does not fit Decimal128"),
                )
            })?;
            let s = i8::try_from(scale).map_err(|_| {
                fail(
                    column,
                    0,
                    None,
                    format!("scale {scale} does not fit Decimal128"),
                )
            })?;
            decimal = decimal
                .with_precision_and_scale(p, s)
                .map_err(|err| fail(column, 0, None, err.to_string()))?;
        }
        Ok(Self {
            column: column.clone(),
            precision,
            scale,
            high: opts.use_high_precision,
            decimal,
            int: Int64Builder::new(),
            float: Float64Builder::new(),
        })
    }
}

impl ColumnDecoder for FixedDecoder {
    fn append(&mut self, row: usize, raw: Option<&str>) -> Result<()> {
        let Some(raw) = raw else {
            if self.high {
                self.decimal.append_null();
            } else if self.scale == 0 {
                self.int.append_null();
            } else {
                self.float.append_null();
            }
            return Ok(());
        };
        let unscaled = parse_scaled_integer(raw, self.scale)
            .map_err(|reason| fail(&self.column, row, Some(raw), reason))?;
        let digits = significant_digits(unscaled);
        if digits > self.precision {
            return Err(fail(
                &self.column,
                row,
                Some(raw),
                format!(
                    "{digits} significant digits exceed precision {}",
                    self.precision
                ),
            ));
        }
        if self.high {
            self.decimal.append_value(unscaled);
            return Ok(());
        }
        if self.scale == 0 {
            let value = i64::try_from(unscaled)
                .map_err(|_| fail(&self.column, row, Some(raw), "value does not fit Int64"))?;
            self.int.append_value(value);
        } else {
            let factor = pow10_i128(self.scale)
                .map_err(|err| fail(&self.column, row, Some(raw), err.to_string()))?;
            let value = (unscaled as f64) / (factor as f64);
            if !value.is_finite() {
                return Err(fail(
                    &self.column,
                    row,
                    Some(raw),
                    "value is not a finite Float64",
                ));
            }
            self.float.append_value(value);
        }
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<ArrayRef> {
        if self.high {
            Ok(Arc::new(self.decimal.finish()))
        } else if self.scale == 0 {
            Ok(Arc::new(self.int.finish()))
        } else {
            Ok(Arc::new(self.float.finish()))
        }
    }
}

struct RealDecoder {
    column: RowType,
    builder: Float64Builder,
}

impl RealDecoder {
    fn new(column: &RowType) -> Self {
        Self {
            column: column.clone(),
            builder: Float64Builder::new(),
        }
    }
}

impl ColumnDecoder for RealDecoder {
    fn append(&mut self, row: usize, raw: Option<&str>) -> Result<()> {
        let Some(raw) = raw else {
            self.builder.append_null();
            return Ok(());
        };
        self.builder.append_value(
            parse_f64(raw).map_err(|reason| fail(&self.column, row, Some(raw), reason))?,
        );
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<ArrayRef> {
        Ok(Arc::new(self.builder.finish()))
    }
}

struct TextDecoder {
    builder: StringBuilder,
}

impl TextDecoder {
    fn new(_column: &RowType) -> Self {
        Self {
            builder: StringBuilder::new(),
        }
    }
}

impl ColumnDecoder for TextDecoder {
    fn append(&mut self, _row: usize, raw: Option<&str>) -> Result<()> {
        match raw {
            None => self.builder.append_null(),
            Some(value) => self.builder.append_value(value),
        }
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<ArrayRef> {
        Ok(Arc::new(self.builder.finish()))
    }
}

struct BinaryDecoder {
    column: RowType,
    builder: BinaryBuilder,
}

impl BinaryDecoder {
    fn new(column: &RowType) -> Self {
        Self {
            column: column.clone(),
            builder: BinaryBuilder::new(),
        }
    }
}

impl ColumnDecoder for BinaryDecoder {
    fn append(&mut self, row: usize, raw: Option<&str>) -> Result<()> {
        let Some(raw) = raw else {
            self.builder.append_null();
            return Ok(());
        };
        let bytes = parse_hex(raw).map_err(|reason| fail(&self.column, row, Some(raw), reason))?;
        self.builder.append_value(bytes);
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<ArrayRef> {
        Ok(Arc::new(self.builder.finish()))
    }
}

struct BoolDecoder {
    column: RowType,
    builder: BooleanBuilder,
}

impl BoolDecoder {
    fn new(column: &RowType) -> Self {
        Self {
            column: column.clone(),
            builder: BooleanBuilder::new(),
        }
    }
}

impl ColumnDecoder for BoolDecoder {
    fn append(&mut self, row: usize, raw: Option<&str>) -> Result<()> {
        let Some(raw) = raw else {
            self.builder.append_null();
            return Ok(());
        };
        let value = match raw.to_ascii_lowercase().as_str() {
            "1" | "true" => true,
            "0" | "false" => false,
            _ => {
                return Err(fail(
                    &self.column,
                    row,
                    Some(raw),
                    "expected 0, 1, true, or false",
                ));
            }
        };
        self.builder.append_value(value);
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<ArrayRef> {
        Ok(Arc::new(self.builder.finish()))
    }
}

struct DateDecoder {
    column: RowType,
    builder: Date32Builder,
}

impl DateDecoder {
    fn new(column: &RowType) -> Self {
        Self {
            column: column.clone(),
            builder: Date32Builder::new(),
        }
    }
}

impl ColumnDecoder for DateDecoder {
    fn append(&mut self, row: usize, raw: Option<&str>) -> Result<()> {
        let Some(raw) = raw else {
            self.builder.append_null();
            return Ok(());
        };
        let days: i32 = raw.parse().map_err(|_| {
            fail(
                &self.column,
                row,
                Some(raw),
                "expected an integer day count",
            )
        })?;
        self.builder.append_value(days);
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<ArrayRef> {
        Ok(Arc::new(self.builder.finish()))
    }
}

struct TimeDecoder {
    column: RowType,
    builder: Time64NanosecondBuilder,
}

impl TimeDecoder {
    fn new(column: &RowType) -> Self {
        Self {
            column: column.clone(),
            builder: Time64NanosecondBuilder::new(),
        }
    }
}

impl ColumnDecoder for TimeDecoder {
    fn append(&mut self, row: usize, raw: Option<&str>) -> Result<()> {
        let Some(raw) = raw else {
            self.builder.append_null();
            return Ok(());
        };
        let nanos = parse_scaled_integer(raw, 9)
            .map_err(|reason| fail(&self.column, row, Some(raw), reason))?;
        if !(0..=86_400_000_000_000).contains(&nanos) {
            return Err(fail(
                &self.column,
                row,
                Some(raw),
                "time is outside 00:00:00 through 24:00:00",
            ));
        }
        let nanos = i64::try_from(nanos).map_err(|_| {
            fail(
                &self.column,
                row,
                Some(raw),
                "time does not fit nanoseconds",
            )
        })?;
        self.builder.append_value(nanos);
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<ArrayRef> {
        Ok(Arc::new(self.builder.finish()))
    }
}

struct TimestampDecoder {
    column: RowType,
    with_offset: bool,
    mode: TimestampTzMode,
    unit: TimeUnit,
    tz: Option<String>,
    seconds: TimestampSecondBuilder,
    millis: TimestampMillisecondBuilder,
    micros: TimestampMicrosecondBuilder,
    nanos: TimestampNanosecondBuilder,
    offsets: Int32Builder,
    validity: Vec<bool>,
}

impl TimestampDecoder {
    fn new(
        column: &RowType,
        params: &SessionParameters,
        opts: &ArrowOptions,
        with_offset: bool,
    ) -> Result<Self> {
        let logical = column.logical_type().to_ascii_lowercase();
        let tz = if with_offset {
            Some("UTC".to_string())
        } else if logical == "timestamp_ltz" {
            Some(schema::ltz_timezone(params, opts))
        } else {
            None
        };
        Ok(Self {
            column: column.clone(),
            with_offset,
            mode: opts.timestamp_tz,
            unit: timestamp_unit(column.scale_or(9)),
            tz,
            seconds: TimestampSecondBuilder::new(),
            millis: TimestampMillisecondBuilder::new(),
            micros: TimestampMicrosecondBuilder::new(),
            nanos: TimestampNanosecondBuilder::new(),
            offsets: Int32Builder::new(),
            validity: Vec::new(),
        })
    }

    fn append_instant(&mut self, value: i64, valid: bool) {
        if !valid {
            self.seconds.append_null();
            self.millis.append_null();
            self.micros.append_null();
            self.nanos.append_null();
            return;
        }
        match self.unit {
            TimeUnit::Second => self.seconds.append_value(value),
            TimeUnit::Millisecond => self.millis.append_value(value),
            TimeUnit::Microsecond => self.micros.append_value(value),
            TimeUnit::Nanosecond => self.nanos.append_value(value),
        }
    }
}

impl ColumnDecoder for TimestampDecoder {
    fn append(&mut self, row: usize, raw: Option<&str>) -> Result<()> {
        let Some(raw) = raw else {
            self.append_instant(0, false);
            self.offsets.append_null();
            self.validity.push(false);
            return Ok(());
        };
        let (instant, offset) = raw
            .split_once(' ')
            .map(|(a, b)| (a, Some(b)))
            .unwrap_or((raw, None));
        if self.with_offset && offset.is_none() {
            return Err(fail(
                &self.column,
                row,
                Some(raw),
                "timestamp_tz is missing its offset",
            ));
        }
        let nanos = parse_scaled_integer(instant, 9)
            .map_err(|reason| fail(&self.column, row, Some(raw), reason))?;
        let digits = unit_digits(&self.unit);
        let scaled = rescale_i128(nanos, 9, digits)
            .map_err(|err| fail(&self.column, row, Some(raw), err.to_string()))?;
        let scaled = i64::try_from(scaled).map_err(|_| {
            fail(
                &self.column,
                row,
                Some(raw),
                "timestamp does not fit the target unit",
            )
        })?;
        self.append_instant(scaled, true);
        self.validity.push(true);
        if let Some(code) = offset {
            let code: i32 = code.parse().map_err(|_| {
                fail(
                    &self.column,
                    row,
                    Some(raw),
                    "timestamp offset is not an integer",
                )
            })?;
            self.offsets.append_value(code - 1440);
        } else {
            self.offsets.append_null();
        }
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<ArrayRef> {
        let tz = self.tz.clone();
        let apply = |array: ArrayRef| -> ArrayRef {
            let Some(tz) = tz.clone() else {
                return array;
            };
            match array.data_type() {
                DataType::Timestamp(TimeUnit::Second, _) => Arc::new(
                    array
                        .as_any()
                        .downcast_ref::<arrow_array::TimestampSecondArray>()
                        .unwrap()
                        .clone()
                        .with_timezone(tz),
                ),
                DataType::Timestamp(TimeUnit::Millisecond, _) => Arc::new(
                    array
                        .as_any()
                        .downcast_ref::<arrow_array::TimestampMillisecondArray>()
                        .unwrap()
                        .clone()
                        .with_timezone(tz),
                ),
                DataType::Timestamp(TimeUnit::Microsecond, _) => Arc::new(
                    array
                        .as_any()
                        .downcast_ref::<arrow_array::TimestampMicrosecondArray>()
                        .unwrap()
                        .clone()
                        .with_timezone(tz),
                ),
                DataType::Timestamp(TimeUnit::Nanosecond, _) => Arc::new(
                    array
                        .as_any()
                        .downcast_ref::<arrow_array::TimestampNanosecondArray>()
                        .unwrap()
                        .clone()
                        .with_timezone(tz),
                ),
                _ => array,
            }
        };
        let epoch: ArrayRef = apply(match self.unit {
            TimeUnit::Second => Arc::new(self.seconds.finish()),
            TimeUnit::Millisecond => Arc::new(self.millis.finish()),
            TimeUnit::Microsecond => Arc::new(self.micros.finish()),
            TimeUnit::Nanosecond => Arc::new(self.nanos.finish()),
        });
        if self.with_offset && self.mode == TimestampTzMode::Struct {
            let offsets = self.offsets.finish();
            let nulls = NullBuffer::from(self.validity);
            let epoch_field = Field::new("epoch", epoch.data_type().clone(), true);
            let offset_field = Field::new("offset_minutes", DataType::Int32, true);
            let array = StructArray::try_new(
                Fields::from(vec![epoch_field, offset_field]),
                vec![epoch, Arc::new(offsets)],
                Some(nulls),
            )
            .map_err(|err| Error::protocol(format!("timestamp_tz struct: {err}")))?;
            return Ok(Arc::new(array));
        }
        Ok(epoch)
    }
}

struct VectorDecoder {
    column: RowType,
    size: i32,
    builder: FixedSizeListBuilder<Float32Builder>,
}

impl VectorDecoder {
    fn new(column: &RowType) -> Result<Self> {
        let Some(length) = column.length.filter(|n| *n > 0) else {
            return Err(fail(
                column,
                0,
                None,
                "vector column has no positive length",
            ));
        };
        let size = i32::try_from(length)
            .map_err(|_| fail(column, 0, None, "vector length does not fit i32"))?;
        Ok(Self {
            column: column.clone(),
            size,
            builder: FixedSizeListBuilder::with_capacity(Float32Builder::new(), size, 0),
        })
    }
}

impl ColumnDecoder for VectorDecoder {
    fn append(&mut self, row: usize, raw: Option<&str>) -> Result<()> {
        let Some(raw) = raw else {
            self.builder.append(false);
            return Ok(());
        };
        let value: serde_json::Value = serde_json::from_str(raw).map_err(|err| {
            fail(
                &self.column,
                row,
                Some(raw),
                format!("vector is not JSON: {err}"),
            )
        })?;
        let serde_json::Value::Array(items) = value else {
            return Err(fail(
                &self.column,
                row,
                Some(raw),
                "vector is not a JSON array",
            ));
        };
        if items.len() != self.size as usize {
            return Err(fail(
                &self.column,
                row,
                Some(raw),
                format!("vector length is {}, expected {}", items.len(), self.size),
            ));
        }
        let values = self.builder.values();
        for item in items {
            let number = item.as_f64().ok_or_else(|| {
                fail(
                    &self.column,
                    row,
                    Some(raw),
                    "vector element is not a number",
                )
            })?;
            if !number.is_finite() || number < f32::MIN as f64 || number > f32::MAX as f64 {
                return Err(fail(
                    &self.column,
                    row,
                    Some(raw),
                    "vector element does not fit Float32",
                ));
            }
            values.append_value(number as f32);
        }
        self.builder.append(true);
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> Result<ArrayRef> {
        Ok(Arc::new(self.builder.finish()))
    }
}

pub fn parse_scaled_integer(raw: &str, scale: i64) -> std::result::Result<i128, String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err("empty numeric value".into());
    }
    let (negative, rest) = if let Some(rest) = raw.strip_prefix('-') {
        (true, rest)
    } else if let Some(rest) = raw.strip_prefix('+') {
        (false, rest)
    } else {
        (false, raw)
    };
    if rest.is_empty() {
        return Err("empty numeric value".into());
    }
    let (whole, frac) = match rest.split_once('.') {
        Some((whole, frac)) => (whole, frac),
        None => (rest, ""),
    };
    if !whole.is_empty() && !whole.bytes().all(|b| b.is_ascii_digit()) {
        return Err("not a base-10 integer".into());
    }
    if !frac.bytes().all(|b| b.is_ascii_digit()) {
        return Err("not a base-10 fraction".into());
    }
    if scale < 0 {
        if !frac.is_empty() {
            return Err("negative scale does not allow a fraction".into());
        }
        let magnitude: i128 = if whole.is_empty() {
            0
        } else {
            whole.parse().map_err(|_| "integer overflow".to_string())?
        };
        let factor = pow10_i128(-scale).map_err(|err| err.to_string())?;
        if magnitude % factor != 0 {
            return Err(format!("value is not aligned to scale {scale}"));
        }
        let unscaled = magnitude / factor;
        return Ok(if negative { -unscaled } else { unscaled });
    }
    if frac.len() as i64 > scale {
        return Err(format!(
            "fraction has {} digits but scale is {scale}",
            frac.len()
        ));
    }
    let mut digits = String::new();
    digits.push_str(if whole.is_empty() { "0" } else { whole });
    digits.push_str(frac);
    for _ in (frac.len() as i64)..scale {
        digits.push('0');
    }
    let magnitude: i128 = digits.parse().map_err(|_| "integer overflow".to_string())?;
    Ok(if negative { -magnitude } else { magnitude })
}

fn significant_digits(value: i128) -> i64 {
    if value == 0 {
        1
    } else {
        value.unsigned_abs().to_string().len() as i64
    }
}

pub fn parse_f64(raw: &str) -> std::result::Result<f64, String> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "inf" | "+inf" | "infinity" | "+infinity" => Ok(f64::INFINITY),
        "-inf" | "-infinity" => Ok(f64::NEG_INFINITY),
        "nan" => Ok(f64::NAN),
        other => other
            .parse::<f64>()
            .map_err(|_| format!("not a float: {raw}")),
    }
}

pub fn parse_hex(raw: &str) -> std::result::Result<Vec<u8>, String> {
    if !raw.len().is_multiple_of(2) {
        return Err("hex length is odd".into());
    }
    let mut out = Vec::with_capacity(raw.len() / 2);
    let bytes = raw.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let hi = hex_nibble(bytes[index])?;
        let lo = hex_nibble(bytes[index + 1])?;
        out.push((hi << 4) | lo);
        index += 2;
    }
    Ok(out)
}

fn hex_nibble(byte: u8) -> std::result::Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err("non-hex character".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Decimal128Array, Float64Array};
    use snowflake_core::RowType;

    fn column(name: &str, logical: &str, precision: Option<i64>, scale: Option<i64>) -> RowType {
        RowType {
            name: name.into(),
            database: String::new(),
            schema: String::new(),
            table: String::new(),
            logical_type: logical.into(),
            nullable: true,
            precision,
            scale,
            length: None,
            byte_length: None,
        }
    }

    fn one(column: RowType, raw: Option<&str>) -> Result<RecordBatch> {
        let rows = vec![vec![raw.map(str::to_string)]];
        batch_from_json(
            std::slice::from_ref(&column),
            &rows,
            &SessionParameters::default(),
            &ArrowOptions::default(),
        )
    }

    #[test]
    fn fixed_keeps_scale_and_rejects_extra_digits() {
        let batch = one(column("v", "fixed", Some(10), Some(2)), Some("123.40")).unwrap();
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(array.value(0), 12340);
        let err = one(column("v", "fixed", Some(10), Some(2)), Some("123.456")).unwrap_err();
        let Error::Decode {
            column, row, raw, ..
        } = err
        else {
            panic!("expected decode error");
        };
        assert_eq!(column, "v");
        assert_eq!(row, 0);
        assert_eq!(raw.as_deref(), Some("123.456"));
    }

    #[test]
    fn strict_cells_do_not_become_null() {
        let cases = [
            (column("n", "fixed", Some(10), Some(0)), "abc"),
            (column("b", "binary", None, None), "abc"),
            (column("flag", "boolean", None, None), "maybe"),
        ];
        for (column, raw) in cases {
            let err = one(column, Some(raw)).unwrap_err();
            assert!(matches!(err, Error::Decode { .. }), "{err}");
            assert!(err.to_string().contains(raw));
        }
    }

    #[test]
    fn float_specials_and_timestamp_tz() {
        let batch = one(column("r", "real", None, None), Some("inf")).unwrap();
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(array.value(0).is_infinite());

        let batch = one(
            column("t", "timestamp_tz", None, Some(9)),
            Some("1700000000.000000000 1500"),
        )
        .unwrap();
        assert_eq!(
            batch.schema().field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC")))
        );
    }

    #[test]
    fn unknown_type_errors() {
        let err = one(column("x", "widget", None, None), Some("1")).unwrap_err();
        assert!(err.to_string().contains("unsupported logical type"));
    }

    #[test]
    fn timestamp_overflow_errors() {
        let err = one(
            column("t", "timestamp_ntz", None, Some(9)),
            Some("999999999999999.0"),
        )
        .unwrap_err();
        assert!(matches!(err, Error::Decode { .. }), "{err}");
    }
}
