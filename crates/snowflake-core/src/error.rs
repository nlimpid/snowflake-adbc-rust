//! Driver errors. Decode failures name the column, row, and raw text.

use thiserror::Error;

/// Snowflake server codes the session layer treats specially.
pub const CODE_AUTH_FAILED: &str = "390100";
pub const CODE_MFA_REQUIRED: &str = "390102";
pub const CODE_MFA_ENROLL: &str = "390127";
pub const CODE_SESSION_EXPIRED: &str = "390112";
pub const CODE_MASTER_EXPIRED: &str = "390114";
pub const CODE_SESSION_GONE: &str = "390115";
pub const CODE_JWT_INVALID: &str = "390144";
pub const CODE_QUERY_RUNNING: &str = "333333";
pub const CODE_QUERY_WAITING: &str = "333334";

pub const MFA_MESSAGE: &str =
    "该用户需要 MFA，驱动不支持交互式 MFA，请使用 Key-Pair 或把用户设为 LEGACY_SERVICE";
pub const JWT_MESSAGE: &str = "JWT is invalid. The public key fingerprint registered for the user does not match this key, or the client and server clocks differ by more than Snowflake allows";

#[derive(Error, Debug)]
pub enum Error {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("http status {status}: {body}")]
    HttpStatus { status: u16, body: String },
    #[error("authentication failed ({code}): {message}")]
    Auth { code: String, message: String },
    #[error("session expired")]
    SessionExpired,
    #[error("snowflake error {code} (sqlstate {sql_state:?}, query {query_id:?}): {message}")]
    Server {
        code: String,
        message: String,
        sql_state: Option<String>,
        query_id: Option<String>,
    },
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error(
        "decode error at column `{column}` row {row} ({logical_type}): {reason}; raw value: {raw:?}"
    )]
    Decode {
        column: String,
        row: usize,
        logical_type: String,
        raw: Option<String>,
        reason: String,
    },
    #[error("query cancelled")]
    Cancelled,
    #[error("timeout")]
    Timeout,
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub fn decode(
        column: impl Into<String>,
        row: usize,
        logical_type: impl Into<String>,
        raw: Option<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self::Decode {
            column: column.into(),
            row,
            logical_type: logical_type.into(),
            raw,
            reason: reason.into(),
        }
    }

    pub fn protocol(message: impl Into<String>) -> Self {
        Self::Protocol(message.into())
    }

    pub fn is_mfa_code(code: &str) -> bool {
        matches!(code, CODE_MFA_REQUIRED | CODE_MFA_ENROLL)
    }

    pub fn is_query_in_progress(code: &str) -> bool {
        matches!(code, CODE_QUERY_RUNNING | CODE_QUERY_WAITING)
    }

    pub fn is_session_expired(code: &str) -> bool {
        code == CODE_SESSION_EXPIRED
    }

    pub fn is_master_expired(code: &str) -> bool {
        matches!(code, CODE_MASTER_EXPIRED | CODE_SESSION_GONE)
    }
}
