//! Errors returned by every public function in this crate.
//!
//! `Display` is one line and names the failure. `Debug` is the same text plus
//! the source error, so logs do not print secrets that [`crate::SecretString`]
//! already hides.

use std::error::Error as StdError;
use std::fmt;
use std::fmt::Formatter;

/// Snowflake server codes the session layer treats specially.
///
/// Login was rejected.
pub const CODE_AUTH_FAILED: &str = "390100";
/// The user must complete MFA. This driver does not prompt.
pub const CODE_MFA_REQUIRED: &str = "390102";
/// The user must enroll in MFA. This driver does not prompt.
pub const CODE_MFA_ENROLL: &str = "390127";
/// The session token expired.
pub const CODE_SESSION_EXPIRED: &str = "390112";
/// The master token can no longer renew the session.
pub const CODE_MASTER_EXPIRED: &str = "390114";
/// The session no longer exists.
pub const CODE_SESSION_GONE: &str = "390115";
/// The JWT was rejected.
pub const CODE_JWT_INVALID: &str = "390144";
/// The statement is still running.
pub const CODE_QUERY_RUNNING: &str = "333333";
/// The statement is waiting to run.
pub const CODE_QUERY_WAITING: &str = "333334";

/// Message returned when the user requires interactive MFA.
pub const MFA_MESSAGE: &str = "this user requires MFA; the driver does not support interactive MFA. Use key-pair authentication or set the user TYPE to LEGACY_SERVICE";
/// Message returned when Snowflake rejects a key-pair JWT.
pub const JWT_MESSAGE: &str = "JWT was rejected. It may have expired during a retry, the client and server clocks may differ, or the public key fingerprint registered for the user does not match this key";

/// Result returned by every public function in this crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Why a call failed. Callers match on [`ErrorKind`] and read [`Error::message`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The HTTP client failed before a Snowflake body was parsed.
    Http,
    /// Snowflake returned a non-success HTTP status.
    HttpStatus,
    /// Login was rejected. MFA and JWT rejections use this kind.
    Auth,
    /// The session token expired and renewal did not recover it.
    SessionExpired,
    /// Snowflake returned a SQL or session error.
    Server,
    /// The request or response did not match the SQL API contract.
    Protocol,
    /// A cell did not match its declared logical type.
    Decode,
    /// The caller cancelled the in-flight query.
    Cancelled,
    /// Retries exhausted `retry_timeout` or a single attempt timed out.
    Timeout,
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Http => "Http",
            Self::HttpStatus => "HttpStatus",
            Self::Auth => "Auth",
            Self::SessionExpired => "SessionExpired",
            Self::Server => "Server",
            Self::Protocol => "Protocol",
            Self::Decode => "Decode",
            Self::Cancelled => "Cancelled",
            Self::Timeout => "Timeout",
        })
    }
}

/// Error returned by every public function in this crate.
///
/// The payload is boxed so returning `Err` does not move the strings.
pub struct Error {
    inner: Box<ErrorInner>,
}

struct ErrorInner {
    kind: ErrorKind,
    message: String,
    status: Option<u16>,
    code: Option<String>,
    sql_state: Option<String>,
    query_id: Option<String>,
    column: Option<String>,
    row: Option<usize>,
    logical_type: Option<String>,
    raw: Option<String>,
    source: Option<Box<dyn StdError + Send + Sync>>,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} => {}", self.inner.kind, self.inner.message)
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")?;
        if let Some(source) = &self.inner.source {
            write!(f, ", source: {source}")?;
        }
        Ok(())
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.inner
            .source
            .as_ref()
            .map(|err| err.as_ref() as &(dyn StdError + 'static))
    }
}

impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        Self::http(err)
    }
}

impl Error {
    /// Create an error with a kind and a message.
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            inner: Box::new(ErrorInner {
                kind,
                message: message.into(),
                status: None,
                code: None,
                sql_state: None,
                query_id: None,
                column: None,
                row: None,
                logical_type: None,
                raw: None,
                source: None,
            }),
        }
    }

    /// Attach the error that caused this one.
    pub fn with_source(mut self, source: impl StdError + Send + Sync + 'static) -> Self {
        self.inner.source = Some(Box::new(source));
        self
    }

    /// Kind of this error.
    pub fn kind(&self) -> ErrorKind {
        self.inner.kind
    }

    /// Message returned to the caller. This does not include the source error.
    pub fn message(&self) -> &str {
        &self.inner.message
    }

    /// HTTP status, when [`ErrorKind::HttpStatus`].
    pub fn status(&self) -> Option<u16> {
        self.inner.status
    }

    /// Snowflake error code, when the server sent one.
    pub fn code(&self) -> Option<&str> {
        self.inner.code.as_deref()
    }

    /// SQLSTATE, when the server sent one.
    pub fn sql_state(&self) -> Option<&str> {
        self.inner.sql_state.as_deref()
    }

    /// Query id, when the failure belongs to a statement.
    pub fn query_id(&self) -> Option<&str> {
        self.inner.query_id.as_deref()
    }

    /// Column name, when [`ErrorKind::Decode`].
    pub fn column(&self) -> Option<&str> {
        self.inner.column.as_deref()
    }

    /// Zero-based row, when [`ErrorKind::Decode`].
    pub fn row(&self) -> Option<usize> {
        self.inner.row
    }

    /// Declared logical type, when [`ErrorKind::Decode`].
    pub fn logical_type(&self) -> Option<&str> {
        self.inner.logical_type.as_deref()
    }

    /// Raw cell text, when [`ErrorKind::Decode`].
    pub fn raw(&self) -> Option<&str> {
        self.inner.raw.as_deref()
    }

    /// HTTP client failure. The reqwest error is the source.
    pub fn http(err: reqwest::Error) -> Self {
        Self::new(ErrorKind::Http, format!("http error: {err}")).with_source(err)
    }

    /// Non-success HTTP status. `body` is the response text, truncated by the caller.
    pub fn http_status(status: u16, body: impl Into<String>) -> Self {
        let body = body.into();
        let mut err = Self::new(
            ErrorKind::HttpStatus,
            format!("http status {status}: {body}"),
        );
        err.inner.status = Some(status);
        err
    }

    /// Login was rejected.
    pub fn auth(code: impl Into<String>, message: impl Into<String>) -> Self {
        let code = code.into();
        let message = message.into();
        let mut err = Self::new(
            ErrorKind::Auth,
            format!("authentication failed ({code}): {message}"),
        );
        err.inner.code = Some(code);
        err
    }

    /// Session renewal failed because the master token is no longer valid.
    pub fn session_expired() -> Self {
        Self::new(ErrorKind::SessionExpired, "session expired")
    }

    /// Snowflake returned a SQL or session error.
    pub fn server(
        code: impl Into<String>,
        message: impl Into<String>,
        sql_state: Option<String>,
        query_id: Option<String>,
    ) -> Self {
        let code = code.into();
        let message = message.into();
        let mut err = Self::new(
            ErrorKind::Server,
            format!(
                "snowflake error {code} (sqlstate {sql_state:?}, query {query_id:?}): {message}"
            ),
        );
        err.inner.code = Some(code);
        err.inner.sql_state = sql_state;
        err.inner.query_id = query_id;
        err
    }

    /// The request or response did not match the SQL API contract.
    pub fn protocol(message: impl Into<String>) -> Self {
        Self::new(
            ErrorKind::Protocol,
            format!("protocol error: {}", message.into()),
        )
    }

    /// A cell did not match its declared logical type.
    pub fn decode(
        column: impl Into<String>,
        row: usize,
        logical_type: impl Into<String>,
        raw: Option<String>,
        reason: impl Into<String>,
    ) -> Self {
        let column = column.into();
        let logical_type = logical_type.into();
        let reason = reason.into();
        let mut err = Self::new(
            ErrorKind::Decode,
            format!(
                "decode error at column `{column}` row {row} ({logical_type}): {reason}; raw value: {raw:?}"
            ),
        );
        err.inner.column = Some(column);
        err.inner.row = Some(row);
        err.inner.logical_type = Some(logical_type);
        err.inner.raw = raw;
        err
    }

    /// The caller cancelled the in-flight query.
    pub fn cancelled() -> Self {
        Self::new(ErrorKind::Cancelled, "query cancelled")
    }

    /// Retries exhausted the budget, or one attempt timed out.
    pub fn timeout() -> Self {
        Self::new(ErrorKind::Timeout, "timeout")
    }

    /// `code` is an MFA enrollment or challenge code.
    pub fn is_mfa_code(code: &str) -> bool {
        matches!(code, CODE_MFA_REQUIRED | CODE_MFA_ENROLL)
    }

    /// `code` means the statement is still running and should be polled.
    pub fn is_query_in_progress(code: &str) -> bool {
        matches!(code, CODE_QUERY_RUNNING | CODE_QUERY_WAITING)
    }

    /// `code` means the session token expired.
    pub fn is_session_expired(code: &str) -> bool {
        code == CODE_SESSION_EXPIRED
    }

    /// `code` means the master token can no longer renew the session.
    pub fn is_master_expired(code: &str) -> bool {
        matches!(code, CODE_MASTER_EXPIRED | CODE_SESSION_GONE)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messages_returned_to_callers_are_english() {
        assert!(MFA_MESSAGE.is_ascii());
        assert!(JWT_MESSAGE.is_ascii());
    }

    #[test]
    fn display_names_the_kind_and_keeps_the_old_wording() {
        let err = Error::decode("V", 1, "fixed", Some("nope".into()), "not a number");
        assert_eq!(err.kind(), ErrorKind::Decode);
        assert!(err.to_string().contains("column `V` row 1"));
        assert_eq!(err.column(), Some("V"));
        assert_eq!(err.raw(), Some("nope"));
    }

    #[test]
    fn debug_does_not_dump_the_source_struct() {
        let err = Error::protocol("bad").with_source(std::io::Error::other("disk"));
        let debug = format!("{err:?}");
        assert!(debug.contains("protocol error: bad"));
        assert!(debug.contains("source: disk"));
    }
}
