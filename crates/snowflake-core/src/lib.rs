//! Snowflake SQL API client.
//!
//! This crate speaks the same REST protocol as the official Go, Python, and JDBC
//! drivers. It does not depend on Arrow: result chunks are either JSON rows or
//! raw Arrow IPC bytes. [`snowflake_arrow`](https://docs.rs/snowflake-arrow) turns
//! both into `RecordBatch` values.
//!
//! ```no_run
//! use snowflake_core::{Authenticator, SnowflakeClient};
//!
//! # async fn example() -> snowflake_core::Result<()> {
//! let client = SnowflakeClient::builder()
//!     .account("myorg-myacct")
//!     .user("SVC_USER")
//!     .authenticator(Authenticator::password("..."))
//!     .warehouse("WH")
//!     .database("DB")
//!     .schema("PUBLIC")
//!     .role("R")
//!     .build()?;
//! let session = client.connect().await?;
//! let result = session
//!     .query("select * from t where id > ?")
//!     .bind(snowflake_core::Bind::Fixed("10".into()))
//!     .execute()
//!     .await?;
//! let _chunks = result.into_chunk_stream();
//! session.close().await?;
//! # Ok(())
//! # }
//! ```

pub mod auth;
pub mod bind;
pub mod config;
mod protocol;
mod result;
mod session;
mod transport;

mod client;
mod error;

pub use auth::{normalize_jwt_account, normalize_jwt_user, Authenticator, SecretString};
pub use bind::Bind;
pub use client::SnowflakeClient;
pub use config::{default_host, ClientBuilder, ClientConfig};
pub use error::{Error, Result};
pub use protocol::{RowType, SessionParameters};
pub use result::{QueryResult, ResultChunk, ResultFormat};
pub use session::{Query, QueryStatus, Session};
