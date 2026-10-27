//! Map driver errors onto ADBC status codes.

use std::os::raw::c_char;

use adbc_core::error::{Error, Status};
use snowflake_core::Error as SnowflakeError;

pub fn adbc_error(err: SnowflakeError) -> Error {
    let mut out = Error::with_message_and_status(err.to_string(), status_for(&err));
    if let SnowflakeError::Server {
        code, sql_state, ..
    } = &err
    {
        if let Ok(code) = code.parse::<i32>() {
            out.vendor_code = code;
        }
        if let Some(sql_state) = sql_state {
            out.sqlstate = sqlstate_bytes(sql_state);
        }
    }
    out
}

fn status_for(err: &SnowflakeError) -> Status {
    match err {
        SnowflakeError::Auth { .. } | SnowflakeError::SessionExpired => Status::Unauthenticated,
        SnowflakeError::Timeout => Status::Timeout,
        SnowflakeError::Cancelled => Status::Cancelled,
        SnowflakeError::Http(_) | SnowflakeError::HttpStatus { .. } => Status::IO,
        SnowflakeError::Server { sql_state, .. } => match sql_state.as_deref() {
            Some(state) if state.starts_with("42") => Status::InvalidArguments,
            Some(state) if state.starts_with("08") => Status::IO,
            _ => Status::Internal,
        },
        SnowflakeError::Decode { .. } | SnowflakeError::Protocol(_) => Status::Internal,
    }
}

fn sqlstate_bytes(value: &str) -> [c_char; 5] {
    let mut out = [0; 5];
    for (index, byte) in value.bytes().take(5).enumerate() {
        out[index] = byte as c_char;
    }
    out
}

pub fn not_implemented(what: &str) -> Error {
    Error::with_message_and_status(format!("{what} is not implemented"), Status::NotImplemented)
}

pub fn invalid(message: impl Into<String>) -> Error {
    Error::with_message_and_status(message, Status::InvalidArguments)
}

pub fn invalid_state(message: impl Into<String>) -> Error {
    Error::with_message_and_status(message, Status::InvalidState)
}

pub fn not_found(message: impl Into<String>) -> Error {
    Error::with_message_and_status(message, Status::NotFound)
}
