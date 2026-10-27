//! Real-account test helpers.
//!
//! Tests call [`test_session`]. When `SNOWFLAKE_TEST_*` is unset the helper
//! returns [`None`] and the test returns. CI sets `SNOWFLAKE_TEST_REQUIRED=1`,
//! which panics instead so a missing secret cannot look like a pass.

use std::env;

use arrow_array::RecordBatch;
use snowflake_arrow::{record_batches_with, ArrowOptions};
use snowflake_core::{Authenticator, Session, SnowflakeClient};

/// Connection settings read from the environment.
#[derive(Debug, Clone)]
pub struct TestConfig {
    pub account: String,
    pub user: String,
    pub password: Option<String>,
    pub private_key: Option<String>,
    pub private_key_path: Option<String>,
    pub private_key_passphrase: Option<String>,
    pub warehouse: String,
    pub database: String,
    pub schema: String,
    pub role: String,
    pub host: Option<String>,
    pub protocol: String,
    pub port: Option<u16>,
}

pub fn test_config() -> Option<TestConfig> {
    let required = env::var("SNOWFLAKE_TEST_REQUIRED").ok().as_deref() == Some("1");
    let missing = |name: &str| -> Option<String> {
        match env::var(name) {
            Ok(value) if !value.is_empty() => Some(value),
            _ => None,
        }
    };
    let account = missing("SNOWFLAKE_TEST_ACCOUNT");
    let user = missing("SNOWFLAKE_TEST_USER");
    let warehouse = missing("SNOWFLAKE_TEST_WAREHOUSE");
    let database = missing("SNOWFLAKE_TEST_DATABASE");
    let schema = missing("SNOWFLAKE_TEST_SCHEMA");
    let role = missing("SNOWFLAKE_TEST_ROLE");
    let password = missing("SNOWFLAKE_TEST_PASSWORD");
    let private_key = missing("SNOWFLAKE_TEST_PRIVATE_KEY");
    let private_key_path = missing("SNOWFLAKE_TEST_PRIVATE_KEY_PATH");
    if account.is_none()
        || user.is_none()
        || warehouse.is_none()
        || database.is_none()
        || schema.is_none()
        || role.is_none()
        || (password.is_none() && private_key.is_none() && private_key_path.is_none())
    {
        if required {
            panic!(
                "SNOWFLAKE_TEST_REQUIRED=1 but account, user, warehouse, database, schema, role, and password or private key are not all set"
            );
        }
        eprintln!("skipping real-account test: SNOWFLAKE_TEST_* is not set");
        return None;
    }
    Some(TestConfig {
        account: account.unwrap_or_default(),
        user: user.unwrap_or_default(),
        password,
        private_key,
        private_key_path,
        private_key_passphrase: missing("SNOWFLAKE_TEST_PRIVATE_KEY_PASSPHRASE"),
        warehouse: warehouse.unwrap_or_default(),
        database: database.unwrap_or_default(),
        schema: schema.unwrap_or_default(),
        role: role.unwrap_or_default(),
        host: missing("SNOWFLAKE_TEST_HOST"),
        protocol: missing("SNOWFLAKE_TEST_PROTOCOL").unwrap_or_else(|| "https".into()),
        port: missing("SNOWFLAKE_TEST_PORT").and_then(|value| value.parse().ok()),
    })
}

pub async fn test_session() -> Option<Session> {
    let config = test_config()?;
    Some(connect(&config).await.expect("snowflake test login"))
}

async fn connect(config: &TestConfig) -> snowflake_core::Result<Session> {
    let authenticator = if let Some(pem) = &config.private_key {
        Authenticator::key_pair_from_pem(pem, config.private_key_passphrase.as_deref())?
    } else if let Some(path) = &config.private_key_path {
        Authenticator::key_pair_from_path(path, config.private_key_passphrase.as_deref())?
    } else {
        Authenticator::password(config.password.clone().unwrap_or_default())
    };
    let mut builder = SnowflakeClient::builder()
        .account(&config.account)
        .user(&config.user)
        .authenticator(authenticator)
        .warehouse(&config.warehouse)
        .database(&config.database)
        .schema(&config.schema)
        .role(&config.role)
        .protocol(&config.protocol);
    if let Some(host) = &config.host {
        builder = builder.host(host);
    }
    if let Some(port) = config.port {
        builder = builder.port(port);
    }
    let client = builder.build()?;
    let session = client.connect().await?;
    session
        .query(format!("USE WAREHOUSE {}", quote_ident(&config.warehouse)))
        .execute()
        .await?;
    session
        .query(format!("USE DATABASE {}", quote_ident(&config.database)))
        .execute()
        .await?;
    session
        .query(format!("USE SCHEMA {}", quote_ident(&config.schema)))
        .execute()
        .await?;
    session
        .query(format!("USE ROLE {}", quote_ident(&config.role)))
        .execute()
        .await?;
    Ok(session)
}

pub fn unique_name(prefix: &str) -> String {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    format!("{prefix}_{}", &suffix[..12])
}

/// Run `sql` once as JSON and once as Arrow and collect both results.
pub async fn exec_both_formats(
    session: &Session,
    sql: &str,
) -> snowflake_core::Result<(Vec<RecordBatch>, Vec<RecordBatch>)> {
    let json = run_with_format(session, sql, "JSON").await?;
    let arrow = run_with_format(session, sql, "ARROW").await?;
    Ok((arrow, json))
}

async fn run_with_format(
    session: &Session,
    sql: &str,
    format: &str,
) -> snowflake_core::Result<Vec<RecordBatch>> {
    session
        .query(format!(
            "ALTER SESSION SET GO_QUERY_RESULT_FORMAT = '{format}'"
        ))
        .execute()
        .await?;
    let result = session
        .query(sql)
        .parameter("GO_QUERY_RESULT_FORMAT", format)
        .execute()
        .await?;
    let opts = ArrowOptions {
        session_timezone: result.parameters().timezone().to_string(),
        ..ArrowOptions::default()
    };
    let mut stream = std::pin::pin!(record_batches_with(result, opts));
    let mut batches = Vec::new();
    use futures::StreamExt;
    while let Some(batch) = stream.next().await {
        batches.push(batch?);
    }
    Ok(batches)
}

pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
