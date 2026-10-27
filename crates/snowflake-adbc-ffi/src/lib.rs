//! C entry point `SnowflakeDriverInit` for `libadbc_driver_snowflake_rs`.

use snowflake_adbc::SnowflakeDriver;

adbc_ffi::export_driver!(SnowflakeDriverInit, SnowflakeDriver);
