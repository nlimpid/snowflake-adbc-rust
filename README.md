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

The `publish` workflow uploads `0.1.0` when a GitHub Release is published, or when the workflow is started by hand. The repository needs a `CARGO_REGISTRY_TOKEN` secret from crates.io.

## Crates

| Crate | Role |
| --- | --- |
| `snowflake-core` | Login, session, query, chunk download. No Arrow dependency. |
| `snowflake-arrow` | `rowtype` → Arrow schema. JSON and Arrow IPC decode to the same schema. Bad cells return `Error::Decode` instead of NULL. |
| `snowflake-adbc` | `adbc_core` Driver / Database / Connection / Statement. |
| `snowflake-adbc-ffi` | `libadbc_driver_snowflake_rs` with entry point `SnowflakeDriverInit`. |
| `snowflake-testing` | Real-account helpers used by `cargo test`. |

## Run

```bash
cargo test --workspace
cargo build -p snowflake-adbc-ffi
```

Rust 1.99 or newer, matching castor-agent. Arrow is 59.3.1 and reqwest is 0.13.5. Real-account tests skip unless `SNOWFLAKE_TEST_ACCOUNT`, `SNOWFLAKE_TEST_USER`, `SNOWFLAKE_TEST_PASSWORD` (or a private key), `SNOWFLAKE_TEST_WAREHOUSE`, `SNOWFLAKE_TEST_DATABASE`, `SNOWFLAKE_TEST_SCHEMA`, and `SNOWFLAKE_TEST_ROLE` are set. CI decrypts `ci/parameters/parameters_<cloud>.json.gpg` with `PARAMETERS_SECRET` and sets `SNOWFLAKE_TEST_REQUIRED=1`. See [`ci/parameters/README.md`](ci/parameters/README.md).

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
use adbc_core::{Connection, Database, Driver};
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

`NUMBER(p, s)` is `Decimal128(p, s)`. Set `adbc.snowflake.sql.client_option.use_high_precision` to `false` for the Go driver's int64/float64 behavior. `adbc.snowflake.rs.timestamp_tz=struct` keeps each `TIMESTAMP_TZ` offset.

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
