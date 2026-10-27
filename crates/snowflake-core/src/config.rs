//! Account, host, and client builder.

use std::collections::HashMap;
use std::time::Duration;

use url::Url;

use crate::auth::Authenticator;
use crate::error::{Error, Result};

pub const DEFAULT_APP_NAME: &str = "snowflake-rs";
pub const DRIVER_VERSION: &str = env!("CARGO_PKG_VERSION");
/// `CLIENT_APP_ID` / version sent so Snowflake honors `GO_QUERY_RESULT_FORMAT`.
pub const GOSNOWFLAKE_APP_ID: &str = "Go";
pub const GOSNOWFLAKE_COMPAT_VERSION: &str = "1.13.3";
pub const DEFAULT_PREFETCH: usize = 4;
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
pub struct ClientConfig {
    pub account: String,
    pub user: String,
    pub authenticator: Authenticator,
    pub warehouse: Option<String>,
    pub database: Option<String>,
    pub schema: Option<String>,
    pub role: Option<String>,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub protocol: String,
    pub app_name: String,
    pub prefetch_concurrency: usize,
    pub query_timeout: Option<Duration>,
    pub login_timeout: Option<Duration>,
    pub retry_timeout: Duration,
    pub http_attempt_timeout: Duration,
    pub session_parameters: HashMap<String, String>,
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
    pub fn new() -> Self {
        Self::default()
    }

    pub fn account(mut self, account: impl Into<String>) -> Self {
        self.account = Some(account.into());
        self
    }

    pub fn user(mut self, user: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self
    }

    pub fn authenticator(mut self, authenticator: Authenticator) -> Self {
        self.authenticator = Some(authenticator);
        self
    }

    pub fn warehouse(mut self, warehouse: impl Into<String>) -> Self {
        self.warehouse = Some(warehouse.into());
        self
    }

    pub fn database(mut self, database: impl Into<String>) -> Self {
        self.database = Some(database.into());
        self
    }

    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    pub fn role(mut self, role: impl Into<String>) -> Self {
        self.role = Some(role.into());
        self
    }

    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    pub fn protocol(mut self, protocol: impl Into<String>) -> Self {
        self.protocol = protocol.into();
        self
    }

    pub fn app_name(mut self, app_name: impl Into<String>) -> Self {
        self.app_name = app_name.into();
        self
    }

    pub fn prefetch_concurrency(mut self, threads: usize) -> Self {
        self.prefetch_concurrency = threads.max(1);
        self
    }

    pub fn query_timeout(mut self, timeout: Duration) -> Self {
        self.query_timeout = Some(timeout);
        self
    }

    pub fn login_timeout(mut self, timeout: Duration) -> Self {
        self.login_timeout = Some(timeout);
        self
    }

    pub fn retry_timeout(mut self, timeout: Duration) -> Self {
        self.retry_timeout = timeout;
        self
    }

    pub fn session_parameter(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.session_parameters.insert(key.into(), value.into());
        self
    }

    pub fn keep_alive(mut self, enabled: bool) -> Self {
        self.keep_alive = enabled;
        self
    }

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
