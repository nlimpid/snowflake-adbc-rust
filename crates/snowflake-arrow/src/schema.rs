//! Logical Snowflake types to a single Arrow schema, shared by both decoders.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
use snowflake_core::{Error, Result, RowType, SessionParameters};

pub const MD_LOGICAL_TYPE: &str = "snowflake.logical_type";
pub const MD_PRECISION: &str = "snowflake.precision";
pub const MD_SCALE: &str = "snowflake.scale";
pub const MD_LENGTH: &str = "snowflake.length";
pub const MD_BYTE_LENGTH: &str = "snowflake.byte_length";

/// How `TIMESTAMP_TZ` is exposed. The value is always a UTC instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TimestampTzMode {
    /// One `Timestamp` column zoned UTC. The per-row offset is discarded.
    #[default]
    Utc,
    /// `Struct { epoch: Timestamp(UTC), offset_minutes: Int32 }`.
    Struct,
}

#[derive(Debug, Clone)]
pub struct ArrowOptions {
    /// `NUMBER` becomes `Decimal128`. When false, scale 0 becomes `Int64` and
    /// any other scale becomes `Float64`; values that do not fit still error.
    pub use_high_precision: bool,
    pub timestamp_tz: TimestampTzMode,
    /// Used for `TIMESTAMP_LTZ` when the session has no `TIMEZONE` parameter.
    pub session_timezone: String,
}

impl Default for ArrowOptions {
    fn default() -> Self {
        Self {
            use_high_precision: true,
            timestamp_tz: TimestampTzMode::Utc,
            session_timezone: "UTC".into(),
        }
    }
}

pub fn schema_for(
    row_type: &[RowType],
    params: &SessionParameters,
    opts: &ArrowOptions,
) -> Result<Schema> {
    let mut fields = Vec::with_capacity(row_type.len());
    for column in row_type {
        fields.push(field_for(column, params, opts)?);
    }
    Ok(Schema::new(fields))
}

pub fn field_for(
    column: &RowType,
    params: &SessionParameters,
    opts: &ArrowOptions,
) -> Result<Field> {
    let data_type = data_type_for(column, params, opts)?;
    let mut metadata = HashMap::new();
    metadata.insert(
        MD_LOGICAL_TYPE.to_string(),
        column.logical_type().to_string(),
    );
    metadata.insert(
        MD_PRECISION.to_string(),
        column.precision.map(|v| v.to_string()).unwrap_or_default(),
    );
    metadata.insert(
        MD_SCALE.to_string(),
        column.scale.map(|v| v.to_string()).unwrap_or_default(),
    );
    metadata.insert(
        MD_LENGTH.to_string(),
        column.length.map(|v| v.to_string()).unwrap_or_default(),
    );
    metadata.insert(
        MD_BYTE_LENGTH.to_string(),
        column
            .byte_length
            .map(|v| v.to_string())
            .unwrap_or_default(),
    );
    Ok(Field::new(column.name.clone(), data_type, column.nullable).with_metadata(metadata))
}

pub fn data_type_for(
    column: &RowType,
    params: &SessionParameters,
    opts: &ArrowOptions,
) -> Result<DataType> {
    let logical = column.logical_type().to_ascii_lowercase();
    match logical.as_str() {
        "fixed" => fixed_type(column, opts),
        "real" => Ok(DataType::Float64),
        "text" | "variant" | "object" | "array" | "geography" | "geometry" => Ok(DataType::Utf8),
        "binary" => Ok(DataType::Binary),
        "boolean" => Ok(DataType::Boolean),
        "date" => Ok(DataType::Date32),
        "time" => Ok(DataType::Time64(TimeUnit::Nanosecond)),
        "timestamp_ntz" => Ok(DataType::Timestamp(
            timestamp_unit(column.scale_or(9)),
            None,
        )),
        "timestamp_ltz" => Ok(DataType::Timestamp(
            timestamp_unit(column.scale_or(9)),
            Some(Arc::from(ltz_timezone(params, opts))),
        )),
        "timestamp_tz" => timestamp_tz_type(column, opts),
        "vector" => vector_type(column),
        other => Err(Error::decode(
            &column.name,
            0,
            other,
            None,
            "unsupported logical type",
        )),
    }
}

pub fn timestamp_unit(scale: i64) -> TimeUnit {
    match scale {
        i64::MIN..=0 => TimeUnit::Second,
        1..=3 => TimeUnit::Millisecond,
        4..=6 => TimeUnit::Microsecond,
        _ => TimeUnit::Nanosecond,
    }
}

pub fn unit_digits(unit: &TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 3,
        TimeUnit::Microsecond => 6,
        TimeUnit::Nanosecond => 9,
    }
}

pub fn ltz_timezone(params: &SessionParameters, opts: &ArrowOptions) -> String {
    params
        .get("TIMEZONE")
        .filter(|tz| !tz.is_empty())
        .unwrap_or(opts.session_timezone.as_str())
        .to_string()
}

fn fixed_type(column: &RowType, opts: &ArrowOptions) -> Result<DataType> {
    let precision = i64_to_u8(column.precision_or(38), &column.name, "precision")?;
    let scale = i64_to_i8(column.scale_or(0), &column.name, "scale")?;
    if precision > 38 {
        return Err(Error::decode(
            &column.name,
            0,
            "fixed",
            None,
            format!("precision {precision} does not fit Decimal128"),
        ));
    }
    if !opts.use_high_precision {
        return Ok(if scale == 0 {
            DataType::Int64
        } else {
            DataType::Float64
        });
    }
    Ok(DataType::Decimal128(precision, scale))
}

fn timestamp_tz_type(column: &RowType, opts: &ArrowOptions) -> Result<DataType> {
    let unit = timestamp_unit(column.scale_or(9));
    let epoch = DataType::Timestamp(unit, Some(Arc::from("UTC")));
    match opts.timestamp_tz {
        TimestampTzMode::Utc => Ok(epoch),
        TimestampTzMode::Struct => Ok(DataType::Struct(Fields::from(vec![
            Field::new("epoch", epoch, true),
            Field::new("offset_minutes", DataType::Int32, true),
        ]))),
    }
}

fn vector_type(column: &RowType) -> Result<DataType> {
    let Some(length) = column.length.filter(|n| *n > 0) else {
        return Err(Error::decode(
            &column.name,
            0,
            "vector",
            None,
            "vector column has no positive length",
        ));
    };
    let size = i32::try_from(length).map_err(|_| {
        Error::decode(
            &column.name,
            0,
            "vector",
            None,
            "vector length does not fit i32",
        )
    })?;
    Ok(DataType::FixedSizeList(
        Arc::new(Field::new("item", DataType::Float32, true)),
        size,
    ))
}

fn i64_to_u8(value: i64, column: &str, what: &str) -> Result<u8> {
    u8::try_from(value).map_err(|_| {
        Error::decode(
            column,
            0,
            "fixed",
            None,
            format!("{what} {value} is outside 0..=255"),
        )
    })
}

fn i64_to_i8(value: i64, column: &str, what: &str) -> Result<i8> {
    i8::try_from(value).map_err(|_| {
        Error::decode(
            column,
            0,
            "fixed",
            None,
            format!("{what} {value} does not fit i8"),
        )
    })
}

/// `10^exp` for exp in `0..=38`.
pub fn pow10_i128(exp: i64) -> Result<i128> {
    if !(0..=38).contains(&exp) {
        return Err(Error::protocol(format!(
            "scale factor 10^{exp} is out of range"
        )));
    }
    let mut value = 1i128;
    for _ in 0..exp {
        value *= 10;
    }
    Ok(value)
}

pub fn rescale_i128(value: i128, from_scale: i64, to_scale: i64) -> Result<i128> {
    if from_scale == to_scale {
        return Ok(value);
    }
    if to_scale > from_scale {
        let factor = pow10_i128(to_scale - from_scale)?;
        return value.checked_mul(factor).ok_or_else(|| {
            Error::protocol(format!(
                "rescaling {value} by 10^{} overflowed",
                to_scale - from_scale
            ))
        });
    }
    let factor = pow10_i128(from_scale - to_scale)?;
    if value % factor != 0 {
        return Err(Error::protocol(format!(
            "rescaling {value} from scale {from_scale} to {to_scale} would drop digits"
        )));
    }
    Ok(value / factor)
}
