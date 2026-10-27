//! JSON shapes of the Snowflake SQL API.

use std::collections::HashMap;

use serde::Deserialize;
use serde_json::Value;

use crate::error::{Error, Result};

#[derive(Debug, Clone)]
pub struct Envelope {
    pub success: bool,
    pub code: Option<String>,
    pub message: Option<String>,
    pub data: Option<Value>,
}

impl Envelope {
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        let value: Value = serde_json::from_slice(bytes)
            .map_err(|err| Error::protocol(format!("response is not JSON: {err}")))?;
        let success = value
            .get("success")
            .and_then(Value::as_bool)
            .ok_or_else(|| Error::protocol("response has no boolean `success`"))?;
        let code = value.get("code").and_then(json_code);
        let message = value.get("message").and_then(|v| match v {
            Value::Null => None,
            Value::String(s) => Some(s.clone()),
            other => Some(other.to_string()),
        });
        let data = match value.get("data") {
            None | Some(Value::Null) => None,
            Some(data) => Some(data.clone()),
        };
        Ok(Self {
            success,
            code,
            message,
            data,
        })
    }

    pub fn data(&self) -> Result<&Value> {
        self.data
            .as_ref()
            .ok_or_else(|| Error::protocol("response data is empty"))
    }
}

fn json_code(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(s) if s.is_empty() => None,
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        other => Some(other.to_string()),
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct NameValue {
    pub name: String,
    pub value: Option<Value>,
}

impl NameValue {
    pub fn value_string(&self) -> Option<String> {
        self.value.as_ref().and_then(|value| match value {
            Value::Null => None,
            Value::String(s) => Some(s.clone()),
            Value::Bool(b) => Some(b.to_string()),
            Value::Number(n) => Some(n.to_string()),
            other => Some(other.to_string()),
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct SessionParameters {
    map: HashMap<String, String>,
}

impl SessionParameters {
    pub fn insert(&mut self, name: impl Into<String>, value: impl Into<String>) {
        self.map.insert(name.into(), value.into());
    }

    pub fn extend_from_response(&mut self, parameters: &[NameValue]) {
        for parameter in parameters {
            if let Some(value) = parameter.value_string() {
                self.map.insert(parameter.name.clone(), value);
            }
        }
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.map.get(key).map(String::as_str)
    }

    pub fn timezone(&self) -> &str {
        self.get("TIMEZONE").unwrap_or("UTC")
    }

    pub fn flag(&self, key: &str) -> bool {
        matches!(
            self.get(key)
                .map(|v| v.eq_ignore_ascii_case("true") || v == "1"),
            Some(true)
        )
    }

    pub fn as_map(&self) -> &HashMap<String, String> {
        &self.map
    }
}

/// Column metadata from `rowtype`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RowType {
    pub name: String,
    #[serde(default)]
    pub database: String,
    #[serde(default)]
    pub schema: String,
    #[serde(default)]
    pub table: String,
    #[serde(rename = "type", default)]
    pub logical_type: String,
    #[serde(default)]
    pub nullable: bool,
    #[serde(default)]
    pub precision: Option<i64>,
    #[serde(default)]
    pub scale: Option<i64>,
    #[serde(default)]
    pub length: Option<i64>,
    #[serde(default)]
    pub byte_length: Option<i64>,
}

impl RowType {
    pub fn logical_type(&self) -> &str {
        &self.logical_type
    }

    pub fn precision_or(&self, default: i64) -> i64 {
        self.precision.unwrap_or(default)
    }

    pub fn scale_or(&self, default: i64) -> i64 {
        self.scale.unwrap_or(default)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct RawChunk {
    pub url: String,
    #[serde(default)]
    pub row_count: i64,
    #[serde(default)]
    pub uncompressed_size: Option<i64>,
    #[serde(default)]
    pub compressed_size: Option<i64>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct QueryData {
    pub query_id: Option<String>,
    pub sql_state: Option<String>,
    pub query_result_format: Option<String>,
    pub row_type: Vec<RowType>,
    pub rowset: Option<Vec<Vec<Option<String>>>>,
    pub rowset_base64: Option<String>,
    pub chunks: Vec<RawChunk>,
    pub chunk_headers: HashMap<String, String>,
    pub qrmk: Option<String>,
    pub total: i64,
    pub returned: i64,
    pub parameters: Vec<NameValue>,
    pub statement_type_id: Option<i64>,
    pub number_of_binds: i64,
    pub final_database_name: Option<String>,
    pub final_schema_name: Option<String>,
    pub final_warehouse_name: Option<String>,
    pub final_role_name: Option<String>,
    pub query_context: Option<Value>,
    pub get_result_url: Option<String>,
    pub result_ids: Vec<String>,
}

impl QueryData {
    pub fn from_value(value: &Value) -> Result<Self> {
        let query_id = value
            .get("queryId")
            .and_then(Value::as_str)
            .map(str::to_string);
        let sql_state = value
            .get("sqlState")
            .and_then(Value::as_str)
            .map(str::to_string);
        let query_result_format = value
            .get("queryResultFormat")
            .and_then(Value::as_str)
            .map(str::to_string);
        let row_type = match value.get("rowtype") {
            Some(Value::Array(_)) => serde_json::from_value(value["rowtype"].clone())
                .map_err(|err| Error::protocol(format!("rowtype: {err}")))?,
            _ => Vec::new(),
        };
        let rowset = match value.get("rowset") {
            None | Some(Value::Null) => None,
            Some(raw) => Some(parse_rowset_value(raw)?),
        };
        let rowset_base64 = value
            .get("rowsetBase64")
            .and_then(Value::as_str)
            .map(str::to_string);
        let chunks = match value.get("chunks") {
            Some(Value::Array(_)) => serde_json::from_value(value["chunks"].clone())
                .map_err(|err| Error::protocol(format!("chunks: {err}")))?,
            _ => Vec::new(),
        };
        let chunk_headers = match value.get("chunkHeaders") {
            Some(Value::Object(map)) => map
                .iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect(),
            _ => HashMap::new(),
        };
        let parameters = match value.get("parameters") {
            Some(Value::Array(_)) => {
                serde_json::from_value(value["parameters"].clone()).unwrap_or_default()
            }
            _ => Vec::new(),
        };
        let result_ids = value
            .get("resultIds")
            .and_then(Value::as_str)
            .map(|s| {
                s.split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        Ok(Self {
            query_id,
            sql_state,
            query_result_format,
            row_type,
            rowset,
            rowset_base64,
            chunks,
            chunk_headers,
            qrmk: value
                .get("qrmk")
                .and_then(Value::as_str)
                .map(str::to_string),
            total: json_i64(value.get("total")).unwrap_or(0),
            returned: json_i64(value.get("returned")).unwrap_or(0),
            parameters,
            statement_type_id: json_i64(value.get("statementTypeId")),
            number_of_binds: json_i64(value.get("numberOfBinds")).unwrap_or(0),
            final_database_name: value
                .get("finalDatabaseName")
                .and_then(Value::as_str)
                .map(str::to_string),
            final_schema_name: value
                .get("finalSchemaName")
                .and_then(Value::as_str)
                .map(str::to_string),
            final_warehouse_name: value
                .get("finalWarehouseName")
                .and_then(Value::as_str)
                .map(str::to_string),
            final_role_name: value
                .get("finalRoleName")
                .and_then(Value::as_str)
                .map(str::to_string),
            query_context: value.get("queryContext").cloned().filter(|v| !v.is_null()),
            get_result_url: value
                .get("getResultUrl")
                .and_then(Value::as_str)
                .map(str::to_string),
            result_ids,
        })
    }
}

fn json_i64(value: Option<&Value>) -> Option<i64> {
    match value? {
        Value::Number(n) => n
            .as_i64()
            .or_else(|| n.as_u64().and_then(|u| i64::try_from(u).ok())),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

pub fn parse_rowset_value(value: &Value) -> Result<Vec<Vec<Option<String>>>> {
    let Value::Array(rows) = value else {
        return Err(Error::protocol("rowset is not an array"));
    };
    let mut out = Vec::with_capacity(rows.len());
    for (row_index, row) in rows.iter().enumerate() {
        let Value::Array(cells) = row else {
            return Err(Error::protocol(format!(
                "rowset row {row_index} is not an array"
            )));
        };
        let mut parsed = Vec::with_capacity(cells.len());
        for cell in cells {
            parsed.push(cell_to_string(cell)?);
        }
        out.push(parsed);
    }
    Ok(out)
}

pub fn cell_to_string(value: &Value) -> Result<Option<String>> {
    match value {
        Value::Null => Ok(None),
        Value::String(s) => Ok(Some(s.clone())),
        Value::Number(n) => Ok(Some(n.to_string())),
        Value::Bool(b) => Ok(Some(b.to_string())),
        other => Err(Error::protocol(format!(
            "unexpected JSON cell {other}; refusing to coerce it to NULL"
        ))),
    }
}

/// Parse a downloaded JSON chunk.
///
/// The first chunk is a JSON array of rows. Later chunks are comma-separated
/// rows with the outer brackets removed, which is not a JSON value by itself.
pub fn parse_json_chunk(bytes: &[u8]) -> Result<Vec<Vec<Option<String>>>> {
    let text = std::str::from_utf8(bytes)
        .map_err(|err| Error::protocol(format!("json chunk is not utf-8: {err}")))?;
    let trimmed = text.trim().trim_end_matches(',').trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
        if let Value::Array(items) = &value {
            if items
                .first()
                .is_none_or(|item| matches!(item, Value::Array(_)))
            {
                return parse_rowset_value(&value);
            }
            return Ok(vec![items
                .iter()
                .map(cell_to_string)
                .collect::<Result<Vec<_>>>()?]);
        }
    }
    let wrapped = format!("[{trimmed}]");
    let value: Value = serde_json::from_str(&wrapped)
        .map_err(|err| Error::protocol(format!("json chunk: {err}")))?;
    parse_rowset_value(&value)
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct LoginData {
    pub token: String,
    pub master_token: String,
    pub validity_in_seconds: i64,
    #[serde(default)]
    pub master_validity_in_seconds: Option<i64>,
    #[serde(default)]
    pub session_id: Option<i64>,
    #[serde(default)]
    pub server_version: Option<String>,
    #[serde(default)]
    pub parameters: Vec<NameValue>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct RenewData {
    pub session_token: String,
    #[serde(default, alias = "validityInSeconds")]
    pub validity_in_seconds_st: Option<i64>,
    #[serde(default)]
    pub master_token: Option<String>,
    #[serde(default)]
    pub validity_in_seconds_mt: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_reads_string_or_numeric_code() {
        let env =
            Envelope::parse(br#"{"success":false,"code":"390100","message":"nope","data":null}"#)
                .unwrap();
        assert!(!env.success);
        assert_eq!(env.code.as_deref(), Some("390100"));

        let env =
            Envelope::parse(br#"{"success":true,"code":null,"message":null,"data":{"token":"t"}}"#)
                .unwrap();
        assert!(env.success);
        assert!(env.code.is_none());
    }

    #[test]
    fn json_chunk_fragment_and_single_row() {
        let rows = parse_json_chunk(br#"["a","b"],["c",null],"#).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1][1], None);

        let rows = parse_json_chunk(br#"["only"]"#).unwrap();
        assert_eq!(rows, vec![vec![Some("only".into())]]);

        let rows = parse_json_chunk(br#"[["only"]]"#).unwrap();
        assert_eq!(rows, vec![vec![Some("only".into())]]);
    }

    #[test]
    fn object_cell_is_an_error() {
        let err = parse_json_chunk(br#"[{"a":1}]"#).unwrap_err();
        assert!(err.to_string().contains("refusing to coerce"));
    }
}
