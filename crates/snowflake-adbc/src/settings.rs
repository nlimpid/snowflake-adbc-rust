//! ADBC option keys. Names match the Go Snowflake driver where the design says so.

use adbc_core::options::{OptionDatabase, OptionValue};
use snowflake_arrow::{ArrowOptions, TimestampTzMode};
use snowflake_core::{Authenticator, ClientBuilder, Error as SnowflakeError, SnowflakeClient};
use url::Url;

use crate::error::{self, adbc_error};

pub const OPT_ACCOUNT: &str = "adbc.snowflake.sql.account";
pub const OPT_HOST: &str = "adbc.snowflake.sql.uri";
pub const OPT_AUTH_TYPE: &str = "adbc.snowflake.sql.auth_type";
pub const OPT_JWT_KEY_PATH: &str = "adbc.snowflake.sql.client_option.jwt_private_key";
pub const OPT_JWT_KEY_VALUE: &str = "adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_value";
pub const OPT_JWT_KEY_PASSWORD: &str =
    "adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_password";
pub const OPT_AUTH_TOKEN: &str = "adbc.snowflake.sql.client_option.auth_token";
pub const OPT_DATABASE: &str = "adbc.snowflake.sql.db";
pub const OPT_SCHEMA: &str = "adbc.snowflake.sql.schema";
pub const OPT_WAREHOUSE: &str = "adbc.snowflake.sql.warehouse";
pub const OPT_ROLE: &str = "adbc.snowflake.sql.role";
pub const OPT_APP_NAME: &str = "adbc.snowflake.sql.client_option.app_name";
pub const OPT_HIGH_PRECISION: &str = "adbc.snowflake.sql.client_option.use_high_precision";
pub const OPT_TIMESTAMP_TZ: &str = "adbc.snowflake.rs.timestamp_tz";
pub const OPT_PREFETCH: &str = "adbc.snowflake.sql.client_option.prefetch_concurrency";
pub const OPT_QUERY_TIMEOUT: &str = "adbc.snowflake.sql.client_option.query_timeout";
pub const OPT_LOGIN_TIMEOUT: &str = "adbc.snowflake.sql.client_option.login_timeout";

#[derive(Clone, Debug, Default)]
pub struct Settings {
    pub uri: Option<String>,
    pub account: Option<String>,
    pub host: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub auth_type: Option<String>,
    pub jwt_path: Option<String>,
    pub jwt_pem: Option<String>,
    pub jwt_password: Option<String>,
    pub auth_token: Option<String>,
    pub database: Option<String>,
    pub schema: Option<String>,
    pub warehouse: Option<String>,
    pub role: Option<String>,
    pub app_name: Option<String>,
    pub use_high_precision: bool,
    pub timestamp_tz: TimestampTzMode,
    pub prefetch: Option<usize>,
    pub query_timeout_secs: Option<u64>,
    pub login_timeout_secs: Option<u64>,
}

impl Settings {
    pub fn new() -> Self {
        Self {
            use_high_precision: true,
            ..Self::default()
        }
    }

    pub fn set_database_option(
        &mut self,
        key: OptionDatabase,
        value: OptionValue,
    ) -> adbc_core::error::Result<()> {
        let text = option_string(&value)?;
        match key {
            OptionDatabase::Uri => apply_uri(self, &text)?,
            OptionDatabase::Username => self.username = Some(text),
            OptionDatabase::Password => self.password = Some(text),
            OptionDatabase::Other(ref name) if name == OPT_ACCOUNT => self.account = Some(text),
            OptionDatabase::Other(ref name) if name == OPT_HOST => self.host = Some(text),
            OptionDatabase::Other(ref name) if name == OPT_AUTH_TYPE => self.auth_type = Some(text),
            OptionDatabase::Other(ref name) if name == OPT_JWT_KEY_PATH => {
                self.jwt_path = Some(text)
            }
            OptionDatabase::Other(ref name) if name == OPT_JWT_KEY_VALUE => {
                self.jwt_pem = Some(text)
            }
            OptionDatabase::Other(ref name) if name == OPT_JWT_KEY_PASSWORD => {
                self.jwt_password = Some(text)
            }
            OptionDatabase::Other(ref name) if name == OPT_AUTH_TOKEN => {
                self.auth_token = Some(text)
            }
            OptionDatabase::Other(ref name) if name == OPT_DATABASE => self.database = Some(text),
            OptionDatabase::Other(ref name) if name == OPT_SCHEMA => self.schema = Some(text),
            OptionDatabase::Other(ref name) if name == OPT_WAREHOUSE => self.warehouse = Some(text),
            OptionDatabase::Other(ref name) if name == OPT_ROLE => self.role = Some(text),
            OptionDatabase::Other(ref name) if name == OPT_APP_NAME => self.app_name = Some(text),
            OptionDatabase::Other(ref name) if name == OPT_HIGH_PRECISION => {
                self.use_high_precision = parse_bool(&text)?;
            }
            OptionDatabase::Other(ref name) if name == OPT_TIMESTAMP_TZ => {
                self.timestamp_tz = match text.as_str() {
                    "utc" => TimestampTzMode::Utc,
                    "struct" => TimestampTzMode::Struct,
                    other => {
                        return Err(error::invalid(format!(
                            "adbc.snowflake.rs.timestamp_tz must be utc or struct, got {other}"
                        )));
                    }
                };
            }
            OptionDatabase::Other(ref name) if name == OPT_PREFETCH => {
                self.prefetch = Some(parse_u64(&text)? as usize);
            }
            OptionDatabase::Other(ref name) if name == OPT_QUERY_TIMEOUT => {
                self.query_timeout_secs = Some(parse_u64(&text)?);
            }
            OptionDatabase::Other(ref name) if name == OPT_LOGIN_TIMEOUT => {
                self.login_timeout_secs = Some(parse_u64(&text)?);
            }
            OptionDatabase::Other(name) => {
                return Err(error::not_implemented(&format!("database option {name}")));
            }
            _ => return Err(error::not_implemented(key.as_ref())),
        }
        Ok(())
    }

    pub fn get(&self, key: &str) -> adbc_core::error::Result<String> {
        let value = match key {
            "uri" => self.uri.clone(),
            "username" => self.username.clone(),
            OPT_ACCOUNT => self.account.clone(),
            OPT_HOST => self.host.clone(),
            OPT_AUTH_TYPE => self.auth_type.clone(),
            OPT_DATABASE => self.database.clone(),
            OPT_SCHEMA => self.schema.clone(),
            OPT_WAREHOUSE => self.warehouse.clone(),
            OPT_ROLE => self.role.clone(),
            OPT_APP_NAME => self.app_name.clone(),
            OPT_HIGH_PRECISION => Some(self.use_high_precision.to_string()),
            OPT_TIMESTAMP_TZ => Some(
                match self.timestamp_tz {
                    TimestampTzMode::Utc => "utc",
                    TimestampTzMode::Struct => "struct",
                }
                .to_string(),
            ),
            _ => None,
        };
        value.ok_or_else(|| error::not_found(format!("option {key} is not set")))
    }

    pub fn arrow_options(&self) -> ArrowOptions {
        ArrowOptions {
            use_high_precision: self.use_high_precision,
            timestamp_tz: self.timestamp_tz,
            session_timezone: "UTC".into(),
        }
    }

    pub fn build_client(&self) -> adbc_core::error::Result<SnowflakeClient> {
        let account = self
            .account
            .clone()
            .ok_or_else(|| error::invalid("adbc.snowflake.sql.account or uri is required"))?;
        let user = self
            .username
            .clone()
            .filter(|user| !user.is_empty())
            .ok_or_else(|| error::invalid("username is required"))?;
        let authenticator = authenticator(self).map_err(adbc_error)?;
        let mut builder = ClientBuilder::new()
            .account(account)
            .user(user)
            .authenticator(authenticator);
        if let Some(value) = &self.warehouse {
            builder = builder.warehouse(value);
        }
        if let Some(value) = &self.database {
            builder = builder.database(value);
        }
        if let Some(value) = &self.schema {
            builder = builder.schema(value);
        }
        if let Some(value) = &self.role {
            builder = builder.role(value);
        }
        if let Some(value) = &self.host {
            builder = builder.host(value);
        }
        if let Some(value) = &self.app_name {
            builder = builder.app_name(value);
        }
        if let Some(value) = self.prefetch {
            builder = builder.prefetch_concurrency(value);
        }
        if let Some(seconds) = self.query_timeout_secs {
            builder = builder.query_timeout(std::time::Duration::from_secs(seconds));
        }
        if let Some(seconds) = self.login_timeout_secs {
            builder = builder.login_timeout(std::time::Duration::from_secs(seconds));
        }
        builder.build().map_err(adbc_error)
    }
}

fn authenticator(settings: &Settings) -> snowflake_core::Result<Authenticator> {
    let kind = settings.auth_type.as_deref().unwrap_or("auth_snowflake");
    match kind {
        "auth_snowflake" => Ok(Authenticator::password(
            settings.password.clone().unwrap_or_default(),
        )),
        "auth_jwt" => {
            let passphrase = settings.jwt_password.as_deref();
            if let Some(pem) = &settings.jwt_pem {
                Authenticator::key_pair_from_pem(pem, passphrase)
            } else if let Some(path) = &settings.jwt_path {
                Authenticator::key_pair_from_path(path, passphrase)
            } else {
                Err(SnowflakeError::protocol(
                    "auth_jwt requires a PKCS#8 private key path or value",
                ))
            }
        }
        "auth_pat" => Ok(Authenticator::programmatic_access_token(
            settings.auth_token.clone().unwrap_or_default(),
        )),
        "auth_oauth" => Ok(Authenticator::oauth(
            settings.auth_token.clone().unwrap_or_default(),
        )),
        other => Err(SnowflakeError::protocol(format!(
            "unsupported auth_type {other}"
        ))),
    }
}

pub fn apply_uri(settings: &mut Settings, uri: &str) -> adbc_core::error::Result<()> {
    let with_scheme = if uri.contains("://") {
        uri.to_string()
    } else {
        format!("snowflake://{uri}")
    };
    let url = Url::parse(&with_scheme).map_err(|err| error::invalid(format!("uri: {err}")))?;
    settings.uri = Some(uri.to_string());
    if let Some(host) = url.host_str() {
        settings.account = Some(host.to_string());
    }
    if !url.username().is_empty() {
        settings.username = Some(percent_decode(url.username()));
    }
    if let Some(password) = url.password() {
        settings.password = Some(percent_decode(password));
    }
    let mut path = url
        .path()
        .trim_matches('/')
        .split('/')
        .filter(|part| !part.is_empty());
    if let Some(database) = path.next() {
        settings.database = Some(percent_decode(database));
    }
    if let Some(schema) = path.next() {
        settings.schema = Some(percent_decode(schema));
    }
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "warehouse" => settings.warehouse = Some(value.into_owned()),
            "role" => settings.role = Some(value.into_owned()),
            "account" => settings.account = Some(value.into_owned()),
            _ => {}
        }
    }
    Ok(())
}

fn percent_decode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            if let Ok(byte) = u8::from_str_radix(
                std::str::from_utf8(&bytes[index + 1..index + 3]).unwrap_or(""),
                16,
            ) {
                out.push(byte as char);
                index += 3;
                continue;
            }
        }
        out.push(bytes[index] as char);
        index += 1;
    }
    out
}

pub fn option_string(value: &OptionValue) -> adbc_core::error::Result<String> {
    match value {
        OptionValue::String(value) => Ok(value.clone()),
        OptionValue::Int(value) => Ok(value.to_string()),
        OptionValue::Double(value) => Ok(value.to_string()),
        OptionValue::Bytes(value) => String::from_utf8(value.clone())
            .map_err(|err| error::invalid(format!("option is not utf-8: {err}"))),
        _ => Err(error::invalid(format!(
            "unsupported option value {value:?}"
        ))),
    }
}

fn parse_bool(value: &str) -> adbc_core::error::Result<bool> {
    match value {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        other => Err(error::invalid(format!(
            "expected true or false, got {other}"
        ))),
    }
}

fn parse_u64(value: &str) -> adbc_core::error::Result<u64> {
    value
        .parse()
        .map_err(|err| error::invalid(format!("expected an integer: {err}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uri_fills_account_database_and_query() {
        let mut settings = Settings::new();
        apply_uri(
            &mut settings,
            "snowflake://svc:s3cret@myorg-myacct/DB/PUBLIC?warehouse=WH&role=R",
        )
        .unwrap();
        assert_eq!(settings.account.as_deref(), Some("myorg-myacct"));
        assert_eq!(settings.username.as_deref(), Some("svc"));
        assert_eq!(settings.password.as_deref(), Some("s3cret"));
        assert_eq!(settings.database.as_deref(), Some("DB"));
        assert_eq!(settings.schema.as_deref(), Some("PUBLIC"));
        assert_eq!(settings.warehouse.as_deref(), Some("WH"));
        assert_eq!(settings.role.as_deref(), Some("R"));
    }
}
