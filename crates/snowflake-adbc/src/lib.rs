//! ADBC driver for Snowflake.
//!
//! Option keys match the Go ADBC Snowflake driver (`adbc.snowflake.sql.*`) so
//! existing URIs and configuration keep working. `NUMBER` is `Decimal128`
//! unless `use_high_precision` is set false.

mod driver;
mod error;
mod info;
mod objects;
mod settings;

pub use driver::SnowflakeDriver;
pub use info::info_reader as info_batch;

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};

pub(crate) struct OnceBatch {
    schema: SchemaRef,
    batch: Option<RecordBatch>,
}

impl OnceBatch {
    fn new(batch: RecordBatch) -> Self {
        Self {
            schema: batch.schema(),
            batch: Some(batch),
        }
    }
}

impl Iterator for OnceBatch {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.batch.take().map(Ok)
    }
}

impl RecordBatchReader for OnceBatch {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

pub(crate) fn info_reader_once(batch: RecordBatch) -> OnceBatch {
    OnceBatch::new(batch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use adbc_core::options::{OptionDatabase, OptionValue};
    use adbc_core::{Database, Driver, Optionable};

    #[test]
    fn database_requires_an_account() {
        let mut driver = SnowflakeDriver;
        let database = driver.new_database().unwrap();
        let err = database.new_connection().unwrap_err();
        assert!(err.to_string().contains("account"), "{err}");
    }

    #[test]
    fn uri_option_is_stored() {
        let mut driver = SnowflakeDriver;
        let mut database = driver
            .new_database_with_opts([(
                OptionDatabase::Uri,
                OptionValue::String("snowflake://svc:pw@myorg-acct/DB/PUBLIC?warehouse=WH".into()),
            )])
            .unwrap();
        database
            .set_option(OptionDatabase::Username, OptionValue::String("svc".into()))
            .unwrap();
        let account = database
            .get_option_string(OptionDatabase::Other("adbc.snowflake.sql.account".into()))
            .unwrap();
        assert_eq!(account, "myorg-acct");
    }
}
