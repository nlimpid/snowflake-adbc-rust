# snowflake-adbc-rust

A Snowflake driver written in Rust. It speaks the same REST SQL API as the official Go, Python, and JDBC drivers, decodes both Arrow and JSON results with a strict type mapping, and exposes that through [ADBC](https://arrow.apache.org/adbc/).

The design is in [`docs/snowflake-rust-driver-design.md`](docs/snowflake-rust-driver-design.md).

## Crates.io

```toml
[dependencies]
snowflake-core = "0.1"
snowflake-arrow = "0.1"
snowflake-adbc = "0.1"
```

The `publish` workflow uploads the workspace version when a GitHub Release is published, or when the workflow is started by hand. A version that is already on crates.io is skipped. The repository secret `CARGO_REGISTRY_TOKEN` is the crates.io API token.

## Crates

| Crate | Role |
| --- | --- |
| `snowflake-core` | Login, session, query, chunk download. No Arrow dependency. |
| `snowflake-arrow` | `rowtype` → Arrow schema. JSON and Arrow IPC decode to the same schema. Bad cells return `Error::Decode` instead of NULL. |
| `snowflake-adbc` | `adbc_core` Driver / Database / Connection / Statement. |
| `snowflake-adbc-ffi` | `libadbc_driver_snowflake_rs` with entry point `SnowflakeDriverInit`. |
| `snowflake-testing` | Optional real-account helpers. Those tests skip unless `SNOWFLAKE_TEST_*` is set. |

## Run

```bash
cargo test --workspace
cargo build -p snowflake-adbc-ffi
```

Rust 1.99 or newer. Arrow is 59.3.1 and reqwest is 0.13.5.

`cargo test` does not contact Snowflake unless `SNOWFLAKE_TEST_ACCOUNT`, `SNOWFLAKE_TEST_USER`, `SNOWFLAKE_TEST_PASSWORD` (or a private key), `SNOWFLAKE_TEST_WAREHOUSE`, `SNOWFLAKE_TEST_DATABASE`, `SNOWFLAKE_TEST_SCHEMA`, and `SNOWFLAKE_TEST_ROLE` are set. The four tests in `crates/snowflake-testing/src/live.rs` then log in; otherwise they print a skip line and pass. GitHub Actions runs fmt, clippy, build, and `cargo test` only. It does not decrypt credentials and does not set `SNOWFLAKE_TEST_REQUIRED`. A green CI run means the unit tests passed. See [`ci/parameters/README.md`](ci/parameters/README.md).

Arrow IPC normalization, SSE-C chunk download, long-query polling, and cancel have not been confirmed against a live account. Design doc section 9 is still open.

Password login needs a `LEGACY_SERVICE` user. Key-pair JWT is `Authenticator::key_pair_from_pem`.

## Native API

```rust
use snowflake_core::{Authenticator, SnowflakeClient};
use futures::StreamExt;

# async fn example() -> snowflake_core::Result<()> {
let client = SnowflakeClient::builder()
    .account("myorg-myacct")
    .user("SVC_USER")
    .authenticator(Authenticator::password("..."))
    .warehouse("WH")
    .database("DB")
    .schema("PUBLIC")
    .role("R")
    .build()?;
let session = client.connect().await?;
let result = session.query("select 1 as v").execute().await?;
let mut batches = snowflake_arrow::record_batches(result);
while let Some(batch) = batches.next().await {
    let batch = batch?;
    println!("{batch:?}");
}
session.close().await?;
# Ok(())
# }
```

## ADBC

```rust
use adbc_core::{Connection, Database, Driver, Statement};
use adbc_core::options::{OptionDatabase, OptionValue};
use snowflake_adbc::SnowflakeDriver;

let mut driver = SnowflakeDriver;
let database = driver.new_database_with_opts([
    (OptionDatabase::Uri, OptionValue::String(
        "snowflake://user:password@myorg-myacct/DB/PUBLIC?warehouse=WH&role=R".into(),
    )),
])?;
let mut connection = database.new_connection()?;
let mut statement = connection.new_statement()?;
statement.set_sql_query("select 1")?;
let reader = statement.execute()?;
```

`NUMBER(p, s)` is `Decimal128(p, s)`. Set `adbc.snowflake.sql.client_option.use_high_precision` to `false` to decode scale 0 as `Int64` and any other scale as `Float64` (values that do not fit still error). The Go ADBC driver defaults this option to `true` as well; `false` is the lossy mode in both drivers. `adbc.snowflake.rs.timestamp_tz=struct` keeps each `TIMESTAMP_TZ` offset.

Login sends `CLIENT_APP_ID=Go` and `CLIENT_APP_VERSION=1.13.3` so Snowflake will honor `GO_QUERY_RESULT_FORMAT=ARROW`. `app_name` is only `CLIENT_ENVIRONMENT.APPLICATION`. That version is a fixed compatibility pin, not this crate's version, and Snowflake's minimum driver version policy can reject it. This has not been checked against a current account.

Load `libadbc_driver_snowflake_rs.so` from Python, Go, or C++ with init function `SnowflakeDriverInit`.

## Type mapping

| Snowflake | Arrow |
| --- | --- |
| `NUMBER(p, s)` | `Decimal128(p, s)` |
| `FLOAT` | `Float64` (`inf`, `-inf`, `NaN` included) |
| `TEXT`, `VARIANT`, `OBJECT`, `ARRAY`, `GEOGRAPHY`, `GEOMETRY` | `Utf8` |
| `BINARY` | `Binary` |
| `BOOLEAN` | `Boolean` |
| `DATE` | `Date32` |
| `TIME` | `Time64(ns)` |
| `TIMESTAMP_NTZ` | `Timestamp` with unit taken from scale, no zone |
| `TIMESTAMP_LTZ` | `Timestamp` in the session `TIMEZONE` |
| `TIMESTAMP_TZ` | `Timestamp` in UTC, or a struct when requested |
| `VECTOR` | `FixedSizeList(Float32, n)` |
| anything else | error |

## Not implemented

PUT/GET file transfer, browser SSO, interactive MFA, OCSP stapling, Substrait, and partition hand-off (`execute_partitions`). `bindStage` uploads for very large array binds are not implemented; the server error is returned as-is.
