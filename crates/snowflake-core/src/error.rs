//! Errors returned by every public function in this crate.
//!
//! The shape follows OpenDAL: [`ErrorKind`] classifies the failure, `Display`
//! is one line, and `Debug` prints context, the `anyhow` source, and a
//! backtrace when one was captured. Retryable transport failures are
//! temporary. Everything else is permanent. The payload is boxed so `Err`
//! stays small.

use std::backtrace::Backtrace;
use std::backtrace::BacktraceStatus;
use std::error::Error as StdError;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;

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

/// Why a call failed. Callers match on this and read [`Error::message`].
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

impl Display for ErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.into_static())
    }
}

impl ErrorKind {
    /// Stable name of this kind.
    pub fn into_static(self) -> &'static str {
        match self {
            Self::Http => "Http",
            Self::HttpStatus => "HttpStatus",
            Self::Auth => "Auth",
            Self::SessionExpired => "SessionExpired",
            Self::Server => "Server",
            Self::Protocol => "Protocol",
            Self::Decode => "Decode",
            Self::Cancelled => "Cancelled",
            Self::Timeout => "Timeout",
        }
    }

    fn enable_backtrace(self) -> bool {
        !matches!(self, Self::Cancelled)
    }
}

/// Whether the caller should retry the same call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RetryStatus {
    /// The error will not change without an external change. Do not retry.
    Permanent,
    /// The same call may succeed if retried. HTTP 429, 408, and 5xx are temporary.
    Temporary,
    /// The error was temporary and stayed that way after retries. Do not retry again.
    Persistent,
}

impl Display for RetryStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Permanent => "permanent",
            Self::Temporary => "temporary",
            Self::Persistent => "persistent",
        })
    }
}

struct ErrorInner {
    error_kind: ErrorKind,
    error_message: String,
    retry_status: RetryStatus,
    error_operation: &'static str,
    error_context: Vec<(&'static str, String)>,
    error_source: Option<anyhow::Error>,
    error_backtrace: Option<Box<Backtrace>>,
    http_status: Option<u16>,
    server_code: Option<String>,
    server_sql_state: Option<String>,
    server_query_id: Option<String>,
    decode_column: Option<String>,
    decode_row: Option<usize>,
    decode_logical_type: Option<String>,
    decode_raw: Option<String>,
}

/// Error returned by every public function in this crate.
///
/// The payload is boxed so returning `Err` does not move the context and source.
pub struct Error {
    inner: Box<ErrorInner>,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let inner = &self.inner;
        write!(
            f,
            "{} ({}) at {}",
            inner.error_kind, inner.retry_status, inner.error_operation
        )?;
        if !inner.error_context.is_empty() {
            write!(f, ", context: {{ ")?;
            write!(
                f,
                "{}",
                inner
                    .error_context
                    .iter()
                    .map(|(key, value)| format!("{key}: {value}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )?;
            write!(f, " }}")?;
        }
        if !inner.error_message.is_empty() {
            write!(f, " => {}", inner.error_message)?;
        }
        if let Some(source) = &inner.error_source {
            write!(f, ", source: {source}")?;
        }
        Ok(())
    }
}

impl Debug for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let inner = &self.inner;
        if f.alternate() {
            // `decode_raw` is the cell text. Keep it off this view.
            return f
                .debug_struct("Error")
                .field("kind", &inner.error_kind)
                .field("status", &inner.retry_status)
                .field("operation", &inner.error_operation)
                .field("context", &inner.error_context)
                .field("code", &inner.server_code)
                .field("sql_state", &inner.server_sql_state)
                .field("query_id", &inner.server_query_id)
                .field("column", &inner.decode_column)
                .field("row", &inner.decode_row)
                .finish_non_exhaustive();
        }

        write!(
            f,
            "{} ({}) at {}",
            inner.error_kind, inner.retry_status, inner.error_operation
        )?;
        if !inner.error_message.is_empty() {
            write!(f, " => {}", inner.error_message)?;
        }
        writeln!(f)?;

        if !inner.error_context.is_empty() {
            writeln!(f)?;
            writeln!(f, "Context:")?;
            for (key, value) in &inner.error_context {
                writeln!(f, "   {key}: {value}")?;
            }
        }
        if let Some(source) = &inner.error_source {
            writeln!(f)?;
            writeln!(f, "Source:")?;
            writeln!(f, "   {source:#}")?;
        }
        if let Some(backtrace) = &inner.error_backtrace {
            writeln!(f)?;
            writeln!(f, "Backtrace:")?;
            writeln!(f, "{backtrace}")?;
        }
        Ok(())
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.inner
            .error_source
            .as_ref()
            .map(|err| err.as_ref() as &(dyn StdError + 'static))
    }
}

impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        let retryable = err.is_timeout() || err.is_connect() || err.is_request();
        Error::new(ErrorKind::Http, format!("http error: {err}"))
            .with_operation("http")
            .set_source(err)
            .with_temporary(retryable)
    }
}

impl Error {
    /// Create a new error with kind and message. The status is permanent.
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            inner: Box::new(ErrorInner {
                error_kind: kind,
                error_message: message.into(),
                retry_status: RetryStatus::Permanent,
                error_operation: "",
                error_context: Vec::new(),
                error_source: None,
                error_backtrace: kind
                    .enable_backtrace()
                    .then(Backtrace::capture)
                    .filter(|bt| bt.status() == BacktraceStatus::Captured)
                    .map(Box::new),
                http_status: None,
                server_code: None,
                server_sql_state: None,
                server_query_id: None,
                decode_column: None,
                decode_row: None,
                decode_logical_type: None,
                decode_raw: None,
            }),
        }
    }

    /// Set the operation that failed.
    ///
    /// A second call keeps the previous operation as context `called`.
    pub fn with_operation(mut self, operation: impl Into<&'static str>) -> Self {
        if !self.inner.error_operation.is_empty() {
            self.inner
                .error_context
                .push(("called", self.inner.error_operation.to_string()));
        }
        self.inner.error_operation = operation.into();
        self
    }

    /// Add one context entry. Keys are static so they cannot carry a secret.
    pub fn with_context(mut self, key: &'static str, value: impl ToString) -> Self {
        self.inner.error_context.push((key, value.to_string()));
        self
    }

    /// Set the source error. Panics in debug builds if a source is already set.
    pub fn set_source(mut self, src: impl Into<anyhow::Error>) -> Self {
        debug_assert!(
            self.inner.error_source.is_none(),
            "the source error has been set"
        );
        self.inner.error_source = Some(src.into());
        self
    }

    /// Mark the error permanent. Retries must stop.
    pub fn set_permanent(mut self) -> Self {
        self.inner.retry_status = RetryStatus::Permanent;
        self
    }

    /// Mark the error temporary. The same call may be retried.
    pub fn set_temporary(mut self) -> Self {
        self.inner.retry_status = RetryStatus::Temporary;
        self
    }

    /// Mark the error persistent. It was temporary and stayed that way after retries.
    pub fn set_persistent(mut self) -> Self {
        self.inner.retry_status = RetryStatus::Persistent;
        self
    }

    /// Set permanent when `permanent` is true.
    pub fn with_permanent(mut self, permanent: bool) -> Self {
        if permanent {
            self.inner.retry_status = RetryStatus::Permanent;
        }
        self
    }

    /// Set temporary when `temporary` is true.
    pub fn with_temporary(mut self, temporary: bool) -> Self {
        if temporary {
            self.inner.retry_status = RetryStatus::Temporary;
        }
        self
    }

    /// Set persistent when `persistent` is true.
    pub fn with_persistent(mut self, persistent: bool) -> Self {
        if persistent {
            self.inner.retry_status = RetryStatus::Persistent;
        }
        self
    }

    /// Kind of this error.
    pub fn kind(&self) -> ErrorKind {
        self.inner.error_kind
    }

    /// The error will not change without an external change.
    pub fn is_permanent(&self) -> bool {
        self.inner.retry_status == RetryStatus::Permanent
    }

    /// The same call may succeed if retried.
    pub fn is_temporary(&self) -> bool {
        self.inner.retry_status == RetryStatus::Temporary
    }

    /// The error was temporary and stayed that way after retries.
    pub fn is_persistent(&self) -> bool {
        self.inner.retry_status == RetryStatus::Persistent
    }

    /// Message returned to the caller. This does not include the source error.
    pub fn message(&self) -> &str {
        &self.inner.error_message
    }

    /// Operation set with [`Error::with_operation`].
    pub fn operation(&self) -> &'static str {
        self.inner.error_operation
    }

    /// Context entries. Values must not be secrets.
    pub fn context(&self) -> &[(&'static str, String)] {
        &self.inner.error_context
    }

    /// Captured backtrace, when `RUST_BACKTRACE` or `RUST_LIB_BACKTRACE` enabled it.
    pub fn backtrace(&self) -> Option<&Backtrace> {
        self.inner.error_backtrace.as_deref()
    }

    /// HTTP status, when [`ErrorKind::HttpStatus`].
    pub fn status(&self) -> Option<u16> {
        self.inner.http_status
    }

    /// Snowflake error code, when the server sent one.
    pub fn code(&self) -> Option<&str> {
        self.inner.server_code.as_deref()
    }

    /// SQLSTATE, when the server sent one.
    pub fn sql_state(&self) -> Option<&str> {
        self.inner.server_sql_state.as_deref()
    }

    /// Query id, when the failure belongs to a statement.
    pub fn query_id(&self) -> Option<&str> {
        self.inner.server_query_id.as_deref()
    }

    /// Column name, when [`ErrorKind::Decode`].
    pub fn column(&self) -> Option<&str> {
        self.inner.decode_column.as_deref()
    }

    /// Zero-based row, when [`ErrorKind::Decode`].
    pub fn row(&self) -> Option<usize> {
        self.inner.decode_row
    }

    /// Declared logical type, when [`ErrorKind::Decode`].
    pub fn logical_type(&self) -> Option<&str> {
        self.inner.decode_logical_type.as_deref()
    }

    /// Raw cell text, when [`ErrorKind::Decode`].
    pub fn raw(&self) -> Option<&str> {
        self.inner.decode_raw.as_deref()
    }

    /// HTTP client failure. The reqwest error is the source.
    pub fn http(err: reqwest::Error) -> Self {
        err.into()
    }

    /// Non-success HTTP status. `body` is the response text, truncated by the caller.
    ///
    /// 408, 429, and 5xx are temporary. Other statuses are permanent.
    pub fn http_status(status: u16, body: impl Into<String>) -> Self {
        let body = body.into();
        let mut err = Self::new(
            ErrorKind::HttpStatus,
            format!("http status {status}: {body}"),
        )
        .with_operation("http")
        .with_context("status", status);
        err.inner.http_status = Some(status);
        if status == 429 || status == 408 || status >= 500 {
            err.set_temporary()
        } else {
            err
        }
    }

    /// Login was rejected.
    pub fn auth(code: impl Into<String>, message: impl Into<String>) -> Self {
        let code = code.into();
        let message = message.into();
        let mut err = Self::new(
            ErrorKind::Auth,
            format!("authentication failed ({code}): {message}"),
        )
        .with_operation("login")
        .with_context("code", &code);
        err.inner.server_code = Some(code);
        err
    }

    /// Session renewal failed because the master token is no longer valid.
    pub fn session_expired() -> Self {
        Self::new(ErrorKind::SessionExpired, "session expired").with_operation("renew")
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
        )
        .with_operation("query")
        .with_context("code", &code);
        if let Some(state) = sql_state.clone() {
            err = err.with_context("sql_state", state);
        }
        err.inner.server_code = Some(code);
        err.inner.server_sql_state = sql_state;
        err.inner.server_query_id = query_id;
        err
    }

    /// The request or response did not match the SQL API contract.
    pub fn protocol(message: impl Into<String>) -> Self {
        Self::new(
            ErrorKind::Protocol,
            format!("protocol error: {}", message.into()),
        )
        .with_operation("protocol")
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
        )
        .with_operation("decode")
        .with_context("column", &column)
        .with_context("row", row)
        .with_context("logical_type", &logical_type);
        err.inner.decode_column = Some(column);
        err.inner.decode_row = Some(row);
        err.inner.decode_logical_type = Some(logical_type);
        err.inner.decode_raw = raw;
        err
    }

    /// The caller cancelled the in-flight query.
    pub fn cancelled() -> Self {
        Self::new(ErrorKind::Cancelled, "query cancelled").with_operation("cancel")
    }

    /// Retries exhausted the budget, or one attempt timed out.
    pub fn timeout() -> Self {
        Self::new(ErrorKind::Timeout, "timeout")
            .with_operation("retry")
            .set_persistent()
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
    fn display_names_kind_status_and_keeps_the_old_wording() {
        let err = Error::decode("V", 1, "fixed", Some("nope".into()), "not a number");
        assert_eq!(err.kind(), ErrorKind::Decode);
        assert!(err.is_permanent());
        assert!(err.to_string().contains("column `V` row 1"));
        assert!(err.to_string().contains("permanent"));
        assert_eq!(err.column(), Some("V"));
        assert_eq!(err.raw(), Some("nope"));
        assert_eq!(err.operation(), "decode");
    }

    #[test]
    fn http_status_429_is_temporary_and_400_is_permanent() {
        assert!(Error::http_status(429, "slow down").is_temporary());
        assert!(Error::http_status(503, "unavailable").is_temporary());
        assert!(Error::http_status(400, "bad").is_permanent());
        assert!(Error::timeout().is_persistent());
    }

    #[test]
    fn debug_alternate_omits_the_raw_cell() {
        let err = Error::decode("V", 0, "fixed", Some("secret-cell".into()), "bad");
        let debug = format!("{err:#?}");
        assert!(!debug.contains("secret-cell"));
        assert!(debug.contains("Decode"));
    }

    #[test]
    fn set_source_accepts_anyhow() {
        let err = Error::protocol("bad").set_source(anyhow::anyhow!("disk"));
        assert!(err.to_string().contains("source: disk"));
    }
}
