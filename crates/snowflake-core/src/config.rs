//! Account, host, and client builder.

use std::collections::HashMap;
use std::time::Duration;

use url::Url;

use crate::auth::Authenticator;
use crate::error::{Error, Result};

/// Default `CLIENT_ENVIRONMENT.APPLICATION` value.
pub const DEFAULT_APP_NAME: &str = "snowflake-rs";
/// This crate's version, sent in the user agent. Not the Go compatibility pin.
pub const DRIVER_VERSION: &str = env!("CARGO_PKG_VERSION");
/// `CLIENT_APP_ID` / version sent so Snowflake honors `GO_QUERY_RESULT_FORMAT`.
pub const GOSNOWFLAKE_APP_ID: &str = "Go";
/// Fixed `CLIENT_APP_VERSION` sent so Snowflake honors `GO_QUERY_RESULT_FORMAT`.
pub const GOSNOWFLAKE_COMPAT_VERSION: &str = "1.13.3";
/// Default number of result chunks downloaded at once.
pub const DEFAULT_PREFETCH: usize = 4;
/// Default budget for HTTP retries.
pub const DEFAULT_RETRY_TIMEOUT: Duration = Duration::from_secs(300);

/// Host Snowflake would use for `account` when `host` is not overridden.
///
/// Underscores become hyphens. Region and cloud segments are kept
/// (`xy12345.us-east-1.aws` stays a single hostname). An account that already
/// ends in `snowflakecomputing.com` or `.cn` is returned unchanged.
pub fn default_host(account: &str) -> String {
    let account = account.trim().to_ascii_lowercase().replace('_', "-");
    if account.ends_with(".snowflakecomputing.com") || account.ends_with(".snowflakecomputing.cn") {
        account
    } else {
        format!("{account}.snowflakecomputing.com")
    }
}

#[derive(Clone)]
/// Resolved client settings. Build one with [`ClientBuilder`].
pub struct ClientConfig {
    /// Account identifier.
    pub account: String,
    /// Login name.
    pub user: String,
    /// How to obtain a session.
    pub authenticator: Authenticator,
    /// Warehouse.
    pub warehouse: Option<String>,
    /// Database.
    pub database: Option<String>,
    /// Schema.
    pub schema: Option<String>,
    /// Role.
    pub role: Option<String>,
    /// Host override.
    pub host: Option<String>,
    /// Port override.
    pub port: Option<u16>,
    /// URL scheme, `https` by default.
    pub protocol: String,
    /// Value sent as `CLIENT_ENVIRONMENT.APPLICATION`.
    pub app_name: String,
    /// How many result chunks to download at once.
    pub prefetch_concurrency: usize,
    /// Deadline for one statement, including polls.
    pub query_timeout: Option<Duration>,
    /// Deadline for one login HTTP attempt.
    pub login_timeout: Option<Duration>,
    /// Budget for HTTP retries.
    pub retry_timeout: Duration,
    /// Deadline for one HTTP attempt.
    pub http_attempt_timeout: Duration,
    /// Session parameters sent at login.
    pub session_parameters: HashMap<String, String>,
    /// Whether to send session heartbeats.
    pub keep_alive: bool,
}

impl std::fmt::Debug for ClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientConfig")
            .field("account", &self.account)
            .field("user", &self.user)
            .field("authenticator", &self.authenticator)
            .field("warehouse", &self.warehouse)
            .field("database", &self.database)
            .field("schema", &self.schema)
            .field("role", &self.role)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("protocol", &self.protocol)
            .field("app_name", &self.app_name)
            .field("prefetch_concurrency", &self.prefetch_concurrency)
            .finish_non_exhaustive()
    }
}

impl ClientConfig {
    /// Account URL, including an explicit host and port when set.
    pub fn base_url(&self) -> Result<Url> {
        let host = match &self.host {
            Some(host) => host.trim().to_string(),
            None => default_host(&self.account),
        };
        if host.is_empty() {
            return Err(Error::protocol("host is empty"));
        }
        let raw = match self.port {
            Some(port) => format!("{}://{}:{}/", self.protocol, host, port),
            None => format!("{}://{}/", self.protocol, host),
        };
        Url::parse(&raw).map_err(|err| Error::protocol(format!("invalid base url {raw}: {err}")))
    }

    /// User-Agent header: app name, crate version, and OS.
    pub fn user_agent(&self) -> String {
        format!(
            "{}/{} (rust; {})",
            self.app_name,
            DRIVER_VERSION,
            std::env::consts::OS
        )
    }
}

#[derive(Clone)]
/// Builds a [`crate::SnowflakeClient`]. `account`, `user`, and `authenticator` are required.
pub struct ClientBuilder {
    account: Option<String>,
    user: Option<String>,
    authenticator: Option<Authenticator>,
    warehouse: Option<String>,
    database: Option<String>,
    schema: Option<String>,
    role: Option<String>,
    host: Option<String>,
    port: Option<u16>,
    protocol: String,
    app_name: String,
    prefetch_concurrency: usize,
    query_timeout: Option<Duration>,
    login_timeout: Option<Duration>,
    retry_timeout: Duration,
    http_attempt_timeout: Duration,
    session_parameters: HashMap<String, String>,
    keep_alive: bool,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self {
            account: None,
            user: None,
            authenticator: None,
            warehouse: None,
            database: None,
            schema: None,
            role: None,
            host: None,
            port: None,
            protocol: "https".into(),
            app_name: DEFAULT_APP_NAME.into(),
            prefetch_concurrency: DEFAULT_PREFETCH,
            query_timeout: None,
            login_timeout: Some(Duration::from_secs(60)),
            retry_timeout: DEFAULT_RETRY_TIMEOUT,
            http_attempt_timeout: Duration::from_secs(120),
            session_parameters: HashMap::new(),
            keep_alive: false,
        }
    }
}

impl ClientBuilder {
    /// Empty builder. Protocol is `https`, login timeout is 60s, retry budget is 300s.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the account identifier. Underscores become hyphens in the default host.
    pub fn account(mut self, account: impl Into<String>) -> Self {
        self.account = Some(account.into());
        self
    }

    /// Set the login name.
    pub fn user(mut self, user: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self
    }

    /// Set how the client logs in.
    pub fn authenticator(mut self, authenticator: Authenticator) -> Self {
        self.authenticator = Some(authenticator);
        self
    }

    /// Set the warehouse sent on login and on relogin.
    pub fn warehouse(mut self, warehouse: impl Into<String>) -> Self {
        self.warehouse = Some(warehouse.into());
        self
    }

    /// Set the database sent on login and on relogin.
    pub fn database(mut self, database: impl Into<String>) -> Self {
        self.database = Some(database.into());
        self
    }

    /// Set the schema sent on login and on relogin.
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Set the role sent on login and on relogin.
    pub fn role(mut self, role: impl Into<String>) -> Self {
        self.role = Some(role.into());
        self
    }

    /// Override the host. Use this for PrivateLink.
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    /// Override the port. The default is 443.
    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    /// Set the URL scheme. The default is `https`.
    pub fn protocol(mut self, protocol: impl Into<String>) -> Self {
        self.protocol = protocol.into();
        self
    }

    /// Set `CLIENT_ENVIRONMENT.APPLICATION`. This is not `CLIENT_APP_ID`.
    pub fn app_name(mut self, app_name: impl Into<String>) -> Self {
        self.app_name = app_name.into();
        self
    }

    /// Set how many result chunks to download at once. Values below 1 become 1.
    pub fn prefetch_concurrency(mut self, threads: usize) -> Self {
        self.prefetch_concurrency = threads.max(1);
        self
    }

    /// Set the deadline for one statement, including polls. Unset means no statement deadline.
    pub fn query_timeout(mut self, timeout: Duration) -> Self {
        self.query_timeout = Some(timeout);
        self
    }

    /// Set the deadline for one login HTTP attempt.
    pub fn login_timeout(mut self, timeout: Duration) -> Self {
        self.login_timeout = Some(timeout);
        self
    }

    /// Set the budget for HTTP retries.
    pub fn retry_timeout(mut self, timeout: Duration) -> Self {
        self.retry_timeout = timeout;
        self
    }

    /// Set one `SESSION_PARAMETERS` entry sent at login.
    pub fn session_parameter(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.session_parameters.insert(key.into(), value.into());
        self
    }

    /// Send session heartbeats. A `CLIENT_SESSION_KEEP_ALIVE` parameter also enables them.
    pub fn keep_alive(mut self, enabled: bool) -> Self {
        self.keep_alive = enabled;
        self
    }

    /// Validate the account, user, authenticator, and base URL, then build the client.
    pub fn build(self) -> Result<crate::client::SnowflakeClient> {
        let account = self
            .account
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| Error::protocol("account is required"))?;
        let user = self
            .user
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| Error::protocol("user is required"))?;
        let authenticator = self
            .authenticator
            .ok_or_else(|| Error::protocol("authenticator is required"))?;
        let config = ClientConfig {
            account,
            user,
            authenticator,
            warehouse: self.warehouse,
            database: self.database,
            schema: self.schema,
            role: self.role,
            host: self.host,
            port: self.port,
            protocol: self.protocol,
            app_name: self.app_name,
            prefetch_concurrency: self.prefetch_concurrency,
            query_timeout: self.query_timeout,
            login_timeout: self.login_timeout,
            retry_timeout: self.retry_timeout,
            http_attempt_timeout: self.http_attempt_timeout,
            session_parameters: self.session_parameters,
            keep_alive: self.keep_alive,
        };
        // Validate the URL before the first network call.
        let _ = config.base_url()?;
        crate::client::SnowflakeClient::from_config(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_from_account() {
        assert_eq!(default_host("xy12345"), "xy12345.snowflakecomputing.com");
        assert_eq!(
            default_host("xy12345.us-east-1.aws"),
            "xy12345.us-east-1.aws.snowflakecomputing.com"
        );
        assert_eq!(
            default_host("MYORG_MYACCT"),
            "myorg-myacct.snowflakecomputing.com"
        );
        assert_eq!(
            default_host("myorg-myacct.snowflakecomputing.com"),
            "myorg-myacct.snowflakecomputing.com"
        );
        assert_eq!(
            default_host("acct.snowflakecomputing.cn"),
            "acct.snowflakecomputing.cn"
        );
    }

    #[test]
    fn explicit_host_is_kept() {
        let cfg = ClientConfig {
            account: "acct".into(),
            user: "u".into(),
            authenticator: Authenticator::password("pw"),
            warehouse: None,
            database: None,
            schema: None,
            role: None,
            host: Some("acct.privatelink.snowflakecomputing.com".into()),
            port: Some(8443),
            protocol: "https".into(),
            app_name: DEFAULT_APP_NAME.into(),
            prefetch_concurrency: 4,
            query_timeout: None,
            login_timeout: None,
            retry_timeout: DEFAULT_RETRY_TIMEOUT,
            http_attempt_timeout: Duration::from_secs(120),
            session_parameters: HashMap::new(),
            keep_alive: false,
        };
        let url = cfg.base_url().unwrap();
        assert_eq!(
            url.host_str(),
            Some("acct.privatelink.snowflakecomputing.com")
        );
        assert_eq!(url.port(), Some(8443));
    }

    #[test]
    fn builder_requires_account_user_and_auth() {
        let err = ClientBuilder::new().user("u").build().unwrap_err();
        assert!(err.to_string().contains("account"));
    }
}
