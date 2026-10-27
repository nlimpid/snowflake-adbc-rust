//! Synchronous configuration, async connect.

use crate::config::{ClientBuilder, ClientConfig};
use crate::error::Result;
use crate::session::Session;

/// A Snowflake client. [`SnowflakeClient::connect`] performs the login.
#[derive(Clone, Debug)]
pub struct SnowflakeClient {
    config: ClientConfig,
    http: reqwest::Client,
}

impl SnowflakeClient {
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    pub(crate) fn from_config(config: ClientConfig) -> Result<Self> {
        let http = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(30))
            .user_agent(config.user_agent())
            .gzip(true)
            .build()?;
        Ok(Self { config, http })
    }

    pub async fn connect(&self) -> Result<Session> {
        Session::login(self.http.clone(), self.config.clone()).await
    }

    pub fn config(&self) -> &ClientConfig {
        &self.config
    }
}
