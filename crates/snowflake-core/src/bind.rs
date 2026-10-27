//! Server-side `?` bindings. Values are decimal/hex strings, never native numbers.

use serde_json::{json, Value};

use crate::error::{Error, Result};

/// One bound value, or an array binding for a bulk insert.
#[derive(Clone, Debug)]
pub enum Bind {
    Null,
    Text(String),
    Fixed(String),
    Real(String),
    Boolean(bool),
    /// Days since Unix epoch. Encoded as milliseconds, which is what Snowflake expects for `DATE`.
    Date(i32),
    /// Nanoseconds since midnight.
    Time(i64),
    /// Nanoseconds since Unix epoch, no time zone.
    TimestampNtz(i64),
    /// Nanoseconds since Unix epoch, local time zone.
    TimestampLtz(i64),
    /// Nanoseconds since Unix epoch plus the original offset in minutes.
    TimestampTz {
        nanos: i64,
        offset_minutes: i32,
    },
    Binary(Vec<u8>),
    /// Array binding. Every element must be the same scalar kind (or [`Bind::Null`]).
    Array(Vec<Bind>),
}

impl Bind {
    pub fn to_binding(&self) -> Result<Value> {
        match self {
            Self::Array(items) => encode_array(items),
            other => {
                let (ty, value) = encode_scalar(other)?;
                Ok(json!({ "type": ty, "value": value }))
            }
        }
    }
}

fn encode_array(items: &[Bind]) -> Result<Value> {
    if items.iter().any(|item| matches!(item, Bind::Array(_))) {
        return Err(Error::protocol("nested array bindings are not supported"));
    }
    let ty = items
        .iter()
        .find_map(|item| match item {
            Bind::Null | Bind::Array(_) => None,
            other => encode_scalar(other).ok().map(|(ty, _)| ty),
        })
        .unwrap_or("TEXT");
    let mut values = Vec::with_capacity(items.len());
    for item in items {
        if matches!(item, Bind::Null) {
            values.push(Value::Null);
            continue;
        }
        let (item_ty, value) = encode_scalar(item)?;
        if item_ty != ty {
            return Err(Error::protocol(format!(
                "array binding mixes {ty} and {item_ty}"
            )));
        }
        values.push(value);
    }
    Ok(json!({ "type": ty, "value": values }))
}

fn encode_scalar(bind: &Bind) -> Result<(&'static str, Value)> {
    let (ty, text) = match bind {
        Bind::Null => return Ok(("TEXT", Value::Null)),
        Bind::Text(value) => ("TEXT", value.clone()),
        Bind::Fixed(value) => ("FIXED", value.clone()),
        Bind::Real(value) => ("REAL", value.clone()),
        Bind::Boolean(value) => ("BOOLEAN", if *value { "true" } else { "false" }.to_string()),
        Bind::Date(days) => (
            "DATE",
            i64::from(*days).saturating_mul(86_400_000).to_string(),
        ),
        Bind::Time(nanos) => ("TIME", nanos.to_string()),
        Bind::TimestampNtz(nanos) => ("TIMESTAMP_NTZ", nanos.to_string()),
        Bind::TimestampLtz(nanos) => ("TIMESTAMP_LTZ", nanos.to_string()),
        Bind::TimestampTz {
            nanos,
            offset_minutes,
        } => (
            "TIMESTAMP_TZ",
            format!("{nanos} {}", offset_minutes.saturating_add(1440)),
        ),
        Bind::Binary(bytes) => ("BINARY", hex_encode(bytes)),
        Bind::Array(_) => {
            return Err(Error::protocol("internal: array binding encoded as scalar"));
        }
    };
    Ok((ty, Value::String(text)))
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

/// Build the `bindings` object. Keys are 1-based decimal strings.
pub fn bindings_object(binds: &[Bind]) -> Result<Value> {
    if binds.is_empty() {
        return Ok(Value::Null);
    }
    let mut map = serde_json::Map::new();
    for (index, bind) in binds.iter().enumerate() {
        map.insert((index + 1).to_string(), bind.to_binding()?);
    }
    Ok(Value::Object(map))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_encodings() {
        let text = Bind::Text("abc".into()).to_binding().unwrap();
        assert_eq!(text, json!({"type": "TEXT", "value": "abc"}));

        let date = Bind::Date(1).to_binding().unwrap();
        assert_eq!(date["value"], "86400000");

        let tz = Bind::TimestampTz {
            nanos: 5,
            offset_minutes: 60,
        }
        .to_binding()
        .unwrap();
        assert_eq!(tz["type"], "TIMESTAMP_TZ");
        assert_eq!(tz["value"], "5 1500");

        let binary = Bind::Binary(vec![0x0a, 0xff]).to_binding().unwrap();
        assert_eq!(binary["value"], "0aff");

        let null = Bind::Null.to_binding().unwrap();
        assert!(null["value"].is_null());
    }

    #[test]
    fn array_binding_and_index_keys() {
        let value = bindings_object(&[
            Bind::Array(vec![
                Bind::Fixed("1".into()),
                Bind::Null,
                Bind::Fixed("3".into()),
            ]),
            Bind::Boolean(true),
        ])
        .unwrap();
        assert_eq!(value["1"]["type"], "FIXED");
        assert_eq!(value["1"]["value"], json!(["1", null, "3"]));
        assert_eq!(value["2"]["value"], "true");
    }

    #[test]
    fn mixed_array_is_an_error() {
        let err = Bind::Array(vec![Bind::Text("a".into()), Bind::Fixed("1".into())])
            .to_binding()
            .unwrap_err();
        assert!(err.to_string().contains("mixes"));
    }
}
