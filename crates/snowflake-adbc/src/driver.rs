//! ADBC Driver, Database, Connection, and Statement.

use std::collections::HashMap;
use std::sync::Arc;

use adbc_core::error::Result;
use adbc_core::options::{
    InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionStatement, OptionValue,
};
use adbc_core::{Connection, Database, Driver, Optionable, PartitionedResult, Statement};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::Schema;
use snowflake_arrow::{schema_for, ArrowOptions};
use snowflake_core::{Bind, Session};

use crate::error::{self, adbc_error};
use crate::info::{info_reader, table_types_reader};
use crate::objects::objects_reader;
use crate::settings::Settings;

#[derive(Debug, Default)]
pub struct SnowflakeDriver;

impl Driver for SnowflakeDriver {
    type DatabaseType = SnowflakeDatabase;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        Ok(SnowflakeDatabase::new())
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        let mut database = self.new_database()?;
        for (key, value) in opts {
            database.set_option(key, value)?;
        }
        Ok(database)
    }
}

pub struct SnowflakeDatabase {
    settings: Settings,
    runtime: tokio::runtime::Runtime,
}

impl SnowflakeDatabase {
    fn new() -> Self {
        Self {
            settings: Settings::new(),
            runtime: tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_name("snowflake-adbc")
                .build()
                .expect("tokio runtime"),
        }
    }

    fn connect(&self) -> Result<Session> {
        let client = self.settings.build_client()?;
        self.runtime.block_on(client.connect()).map_err(adbc_error)
    }
}

impl Optionable for SnowflakeDatabase {
    type Option = OptionDatabase;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        self.settings.set_database_option(key, value)
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        self.settings.get(key.as_ref())
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        Ok(self.get_option_string(key)?.into_bytes())
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        self.get_option_string(key)?
            .parse()
            .map_err(|err| error::invalid(format!("{err}")))
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        self.get_option_string(key)?
            .parse()
            .map_err(|err| error::invalid(format!("{err}")))
    }
}

impl Database for SnowflakeDatabase {
    type ConnectionType = SnowflakeConnection;

    fn new_connection(&self) -> Result<Self::ConnectionType> {
        let session = self.connect()?;
        Ok(SnowflakeConnection {
            session,
            handle: self.runtime.handle().clone(),
            arrow: self.settings.arrow_options(),
            autocommit: true,
            options: HashMap::new(),
        })
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
    ) -> Result<Self::ConnectionType> {
        let mut connection = self.new_connection()?;
        for (key, value) in opts {
            connection.set_option(key, value)?;
        }
        Ok(connection)
    }
}

pub struct SnowflakeConnection {
    session: Session,
    handle: tokio::runtime::Handle,
    arrow: ArrowOptions,
    autocommit: bool,
    options: HashMap<String, String>,
}

impl std::fmt::Debug for SnowflakeConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnowflakeConnection")
            .field("autocommit", &self.autocommit)
            .finish_non_exhaustive()
    }
}

impl SnowflakeConnection {
    fn block<T>(
        &self,
        future: impl std::future::Future<Output = snowflake_core::Result<T>>,
    ) -> Result<T> {
        self.handle.block_on(future).map_err(adbc_error)
    }

    fn arrow_opts(&self) -> ArrowOptions {
        let mut opts = self.arrow.clone();
        opts.session_timezone = self.session.parameters().timezone().to_string();
        opts
    }
}

impl Optionable for SnowflakeConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match key {
            OptionConnection::AutoCommit => {
                let enabled = bool::try_from(&value)?;
                let sql = if enabled {
                    "ALTER SESSION SET AUTOCOMMIT = TRUE"
                } else {
                    "ALTER SESSION SET AUTOCOMMIT = FALSE"
                };
                self.block(self.session.query(sql).execute())?;
                self.autocommit = enabled;
            }
            OptionConnection::CurrentCatalog => {
                let name = crate::settings::option_string(&value)?;
                let sql = format!("USE DATABASE {}", quote_ident(&name));
                self.block(self.session.query(sql).execute())?;
            }
            OptionConnection::CurrentSchema => {
                let name = crate::settings::option_string(&value)?;
                let sql = format!("USE SCHEMA {}", quote_ident(&name));
                self.block(self.session.query(sql).execute())?;
            }
            OptionConnection::ReadOnly | OptionConnection::IsolationLevel => {
                return Err(error::not_implemented(key.as_ref()));
            }
            OptionConnection::Other(name) => {
                self.options
                    .insert(name, crate::settings::option_string(&value)?);
            }
            _ => return Err(error::not_implemented(key.as_ref())),
        }
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match key {
            OptionConnection::AutoCommit => Ok(self.autocommit.to_string()),
            OptionConnection::CurrentCatalog => self
                .session
                .database()
                .ok_or_else(|| error::not_found("no current database")),
            OptionConnection::CurrentSchema => self
                .session
                .schema_name()
                .ok_or_else(|| error::not_found("no current schema")),
            OptionConnection::Other(name) => self
                .options
                .get(&name)
                .cloned()
                .ok_or_else(|| error::not_found(format!("option {name} is not set"))),
            _ => Err(error::not_implemented(key.as_ref())),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        Ok(self.get_option_string(key)?.into_bytes())
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        self.get_option_string(key)?
            .parse()
            .map_err(|err| error::invalid(format!("{err}")))
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        self.get_option_string(key)?
            .parse()
            .map_err(|err| error::invalid(format!("{err}")))
    }
}

impl Connection for SnowflakeConnection {
    type StatementType = SnowflakeStatement;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        Ok(SnowflakeStatement {
            session: self.session.clone(),
            handle: self.handle.clone(),
            arrow: self.arrow_opts(),
            sql: None,
            parameter_schema: None,
            parameter_count: None,
            bound: Vec::new(),
            options: HashMap::new(),
        })
    }

    fn cancel(&mut self) -> Result<()> {
        self.block(self.session.cancel())
    }

    fn get_info(
        &self,
        codes: Option<std::collections::HashSet<InfoCode>>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        info_reader(codes, &self.session.server_version())
    }

    fn get_objects(
        &self,
        depth: ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<Vec<&str>>,
        column_name: Option<&str>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        objects_reader(
            &self.session,
            &self.handle,
            &self.arrow_opts(),
            depth,
            catalog,
            db_schema,
            table_name,
            table_type,
            column_name,
        )
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<Schema> {
        let mut name = String::new();
        if let Some(catalog) = catalog {
            name.push_str(&quote_ident(catalog));
            name.push('.');
        }
        if let Some(schema) = db_schema {
            name.push_str(&quote_ident(schema));
            name.push('.');
        }
        name.push_str(&quote_ident(table_name));
        let sql = format!("SELECT * FROM {name} WHERE 1 = 0");
        let result = self.block(self.session.query(sql).describe_only(true).execute())?;
        schema_for(result.row_type(), result.parameters(), &self.arrow_opts()).map_err(adbc_error)
    }

    fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        table_types_reader()
    }

    fn get_statistic_names(&self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(error::not_implemented("get_statistic_names"))
    }

    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(error::not_implemented("get_statistics"))
    }

    fn commit(&mut self) -> Result<()> {
        self.block(self.session.query("COMMIT").execute())
            .map(|_| ())
    }

    fn rollback(&mut self) -> Result<()> {
        self.block(self.session.query("ROLLBACK").execute())
            .map(|_| ())
    }

    fn read_partition(
        &self,
        _partition: impl AsRef<[u8]>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Err(error::not_implemented(
            "read_partition: Snowflake chunk URLs are session-scoped",
        ))
    }
}

pub struct SnowflakeStatement {
    session: Session,
    handle: tokio::runtime::Handle,
    arrow: ArrowOptions,
    sql: Option<String>,
    parameter_schema: Option<Schema>,
    parameter_count: Option<i64>,
    bound: Vec<RecordBatch>,
    options: HashMap<String, String>,
}

impl SnowflakeStatement {
    fn sql(&self) -> Result<&str> {
        self.sql
            .as_deref()
            .ok_or_else(|| error::invalid_state("statement has no SQL"))
    }

    fn block<T>(
        &self,
        future: impl std::future::Future<Output = snowflake_core::Result<T>>,
    ) -> Result<T> {
        self.handle.block_on(future).map_err(adbc_error)
    }

    fn run(&self, describe_only: bool) -> Result<snowflake_core::QueryResult> {
        let sql = self.sql()?.to_string();
        let binds = binds_from_batches(&self.bound)?;
        let session = self.session.clone();
        self.block(async move {
            let mut query = session.query(sql).describe_only(describe_only);
            for bind in binds {
                query = query.bind(bind);
            }
            query.execute().await
        })
    }
}

impl Optionable for SnowflakeStatement {
    type Option = OptionStatement;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match key {
            OptionStatement::IngestMode
            | OptionStatement::TargetTable
            | OptionStatement::TargetCatalog
            | OptionStatement::TargetDbSchema
            | OptionStatement::Temporary
            | OptionStatement::Incremental => Err(error::not_implemented(key.as_ref())),
            OptionStatement::Progress | OptionStatement::MaxProgress => Err(error::invalid_state(
                format!("{} is read-only", key.as_ref()),
            )),
            OptionStatement::Other(name) => {
                self.options
                    .insert(name, crate::settings::option_string(&value)?);
                Ok(())
            }
            _ => Err(error::not_implemented(key.as_ref())),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match key {
            OptionStatement::Other(name) => self
                .options
                .get(&name)
                .cloned()
                .ok_or_else(|| error::not_found(format!("option {name} is not set"))),
            _ => Err(error::not_implemented(key.as_ref())),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        Ok(self.get_option_string(key)?.into_bytes())
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        self.get_option_string(key)?
            .parse()
            .map_err(|err| error::invalid(format!("{err}")))
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        match &key {
            OptionStatement::Progress | OptionStatement::MaxProgress => {
                Err(error::not_implemented(key.as_ref()))
            }
            _ => self
                .get_option_string(key)?
                .parse()
                .map_err(|err| error::invalid(format!("{err}"))),
        }
    }
}

impl Statement for SnowflakeStatement {
    fn bind(&mut self, batch: RecordBatch) -> Result<()> {
        self.bound = vec![batch];
        Ok(())
    }

    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        let mut batches = Vec::new();
        let mut schema = None;
        for batch in reader {
            let batch = batch?;
            if let Some(schema) = &schema {
                if batch.schema().as_ref() != schema {
                    return Err(error::invalid("bind stream batches have different schemas"));
                }
            } else {
                schema = Some(batch.schema().as_ref().clone());
            }
            batches.push(batch);
        }
        if batches.is_empty() {
            return Err(error::invalid("bind stream is empty"));
        }
        self.bound = batches;
        Ok(())
    }

    fn execute(&mut self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let result = self.run(false)?;
        let opts = self.arrow.clone();
        let schema =
            schema_for(result.row_type(), result.parameters(), &opts).map_err(adbc_error)?;
        let stream = snowflake_arrow::record_batches_with(result, opts);
        Ok(Box::new(snowflake_arrow::BlockingBatchReader::new(
            self.handle.clone(),
            Arc::new(schema),
            Box::pin(stream),
        )))
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        let sql = self.sql()?.to_string();
        let result = self.run(false)?;
        let opts = self.arrow.clone();
        let batches = self
            .handle
            .block_on(snowflake_arrow::collect(result, opts))
            .map_err(adbc_error)?;
        Ok(affected_rows(&sql, &batches))
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        let result = self.run(true)?;
        schema_for(result.row_type(), result.parameters(), &self.arrow).map_err(adbc_error)
    }

    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        Err(error::not_implemented(
            "execute_partitions: Snowflake chunk URLs are session-scoped",
        ))
    }

    fn get_parameter_schema(&self) -> Result<Schema> {
        let count = self.parameter_count.ok_or_else(|| {
            error::invalid_state("prepare the statement before reading parameter schema")
        })?;
        let fields = (0..count)
            .map(|_| arrow_schema::Field::new("", arrow_schema::DataType::Null, true))
            .collect::<Vec<_>>();
        Ok(Schema::new(fields))
    }

    fn prepare(&mut self) -> Result<()> {
        let result = self.run(true)?;
        self.parameter_count = Some(result.number_of_binds());
        self.parameter_schema = Some(
            schema_for(result.row_type(), result.parameters(), &self.arrow).map_err(adbc_error)?,
        );
        Ok(())
    }

    fn set_sql_query(&mut self, query: impl AsRef<str>) -> Result<()> {
        self.sql = Some(query.as_ref().to_string());
        self.parameter_count = None;
        self.parameter_schema = None;
        Ok(())
    }

    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> Result<()> {
        Err(error::not_implemented("substrait"))
    }

    fn cancel(&mut self) -> Result<()> {
        self.block(self.session.cancel())
    }
}

fn affected_rows(sql: &str, batches: &[RecordBatch]) -> Option<i64> {
    if let Some(batch) = batches
        .iter()
        .find(|batch| batch.num_rows() > 0 && batch.num_columns() > 0)
    {
        let name = batch.schema().field(0).name().to_ascii_lowercase();
        if name.contains("number of rows") {
            return int_cell(batch, 0, 0);
        }
    }
    let keyword = sql
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_ascii_uppercase();
    if matches!(
        keyword.as_str(),
        "CREATE" | "ALTER" | "DROP" | "USE" | "GRANT" | "REVOKE" | "COMMENT" | "UNDROP"
    ) {
        return Some(-1);
    }
    None
}

fn int_cell(batch: &RecordBatch, column: usize, row: usize) -> Option<i64> {
    use arrow_array::cast::AsArray;
    let array = batch.column(column);
    if array.is_null(row) {
        return None;
    }
    match array.data_type() {
        arrow_schema::DataType::Int64 => Some(
            array
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(row),
        ),
        arrow_schema::DataType::Decimal128(_, scale) => {
            let value = array
                .as_primitive::<arrow_array::types::Decimal128Type>()
                .value(row);
            let factor = 10i128.pow((*scale).unsigned_abs() as u32);
            let whole = if *scale >= 0 {
                value / factor
            } else {
                value * factor
            };
            i64::try_from(whole).ok()
        }
        arrow_schema::DataType::Utf8 => array.as_string::<i32>().value(row).parse().ok(),
        _ => None,
    }
}

fn binds_from_batches(batches: &[RecordBatch]) -> Result<Vec<Bind>> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    let columns = batches[0].num_columns();
    let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    if total_rows == 0 {
        return Err(error::invalid("bind batch has no rows"));
    }
    let mut columns_of_values = vec![Vec::with_capacity(total_rows); columns];
    for batch in batches {
        if batch.num_columns() != columns {
            return Err(error::invalid("bind batches have different widths"));
        }
        for row in 0..batch.num_rows() {
            for (column, values) in columns_of_values.iter_mut().enumerate() {
                values.push(cell_to_bind(
                    batch.column(column),
                    batch.schema().field(column),
                    row,
                )?);
            }
        }
    }
    let mut binds = Vec::with_capacity(columns);
    for mut values in columns_of_values {
        if values.len() == 1 {
            binds.push(values.pop().unwrap_or(Bind::Null));
        } else {
            binds.push(Bind::Array(values));
        }
    }
    Ok(binds)
}

fn cell_to_bind(
    array: &arrow_array::ArrayRef,
    field: &arrow_schema::Field,
    row: usize,
) -> Result<Bind> {
    use arrow_array::cast::AsArray;
    if array.is_null(row) {
        return Ok(Bind::Null);
    }
    let logical = field
        .metadata()
        .get(snowflake_arrow::MD_LOGICAL_TYPE)
        .map(String::as_str);
    let bind = match array.data_type() {
        arrow_schema::DataType::Utf8 => Bind::Text(array.as_string::<i32>().value(row).to_string()),
        arrow_schema::DataType::LargeUtf8 => {
            Bind::Text(array.as_string::<i64>().value(row).to_string())
        }
        arrow_schema::DataType::Int8
        | arrow_schema::DataType::Int16
        | arrow_schema::DataType::Int32
        | arrow_schema::DataType::Int64
        | arrow_schema::DataType::UInt8
        | arrow_schema::DataType::UInt16
        | arrow_schema::DataType::UInt32
        | arrow_schema::DataType::UInt64 => Bind::Fixed(integer_string(array, row)?),
        arrow_schema::DataType::Decimal128(_, scale) => {
            let unscaled = array
                .as_primitive::<arrow_array::types::Decimal128Type>()
                .value(row);
            Bind::Fixed(format_decimal(unscaled, *scale))
        }
        arrow_schema::DataType::Float32 => Bind::Real(
            array
                .as_primitive::<arrow_array::types::Float32Type>()
                .value(row)
                .to_string(),
        ),
        arrow_schema::DataType::Float64 => Bind::Real(
            array
                .as_primitive::<arrow_array::types::Float64Type>()
                .value(row)
                .to_string(),
        ),
        arrow_schema::DataType::Boolean => Bind::Boolean(array.as_boolean().value(row)),
        arrow_schema::DataType::Date32 => Bind::Date(
            array
                .as_primitive::<arrow_array::types::Date32Type>()
                .value(row),
        ),
        arrow_schema::DataType::Date64 => {
            let millis = array
                .as_primitive::<arrow_array::types::Date64Type>()
                .value(row);
            Bind::Date((millis / 86_400_000) as i32)
        }
        arrow_schema::DataType::Time64(unit) => {
            let value = array.as_primitive::<arrow_array::types::Time64NanosecondType>();
            let _ = (unit, value);
            Bind::Time(time_to_nanos(array, row)?)
        }
        arrow_schema::DataType::Time32(_) => Bind::Time(time_to_nanos(array, row)?),
        arrow_schema::DataType::Timestamp(unit, tz) => {
            let nanos = timestamp_to_nanos(array, row, unit)?;
            if logical == Some("timestamp_tz") {
                Bind::TimestampTz {
                    nanos,
                    offset_minutes: 0,
                }
            } else if tz.is_some() {
                Bind::TimestampLtz(nanos)
            } else {
                Bind::TimestampNtz(nanos)
            }
        }
        arrow_schema::DataType::Binary => {
            Bind::Binary(array.as_binary::<i32>().value(row).to_vec())
        }
        arrow_schema::DataType::Null => Bind::Null,
        other => {
            return Err(error::invalid(format!("cannot bind arrow type {other:?}")));
        }
    };
    Ok(bind)
}

fn integer_string(array: &arrow_array::ArrayRef, row: usize) -> Result<String> {
    use arrow_array::cast::AsArray;
    let text = match array.data_type() {
        arrow_schema::DataType::Int8 => array
            .as_primitive::<arrow_array::types::Int8Type>()
            .value(row)
            .to_string(),
        arrow_schema::DataType::Int16 => array
            .as_primitive::<arrow_array::types::Int16Type>()
            .value(row)
            .to_string(),
        arrow_schema::DataType::Int32 => array
            .as_primitive::<arrow_array::types::Int32Type>()
            .value(row)
            .to_string(),
        arrow_schema::DataType::Int64 => array
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(row)
            .to_string(),
        arrow_schema::DataType::UInt8 => array
            .as_primitive::<arrow_array::types::UInt8Type>()
            .value(row)
            .to_string(),
        arrow_schema::DataType::UInt16 => array
            .as_primitive::<arrow_array::types::UInt16Type>()
            .value(row)
            .to_string(),
        arrow_schema::DataType::UInt32 => array
            .as_primitive::<arrow_array::types::UInt32Type>()
            .value(row)
            .to_string(),
        arrow_schema::DataType::UInt64 => array
            .as_primitive::<arrow_array::types::UInt64Type>()
            .value(row)
            .to_string(),
        other => return Err(error::invalid(format!("not an integer: {other:?}"))),
    };
    Ok(text)
}

fn format_decimal(unscaled: i128, scale: i8) -> String {
    if scale == 0 {
        return unscaled.to_string();
    }
    if scale < 0 {
        let factor = 10i128.pow(scale.unsigned_abs() as u32);
        return (unscaled * factor).to_string();
    }
    let negative = unscaled < 0;
    let digits = unscaled.unsigned_abs().to_string();
    let scale = scale as usize;
    let text = if digits.len() <= scale {
        format!("0.{}{}", "0".repeat(scale - digits.len()), digits)
    } else {
        let split = digits.len() - scale;
        format!("{}.{}", &digits[..split], &digits[split..])
    };
    if negative {
        format!("-{text}")
    } else {
        text
    }
}

fn time_to_nanos(array: &arrow_array::ArrayRef, row: usize) -> Result<i64> {
    use arrow_array::cast::AsArray;
    use arrow_schema::TimeUnit;
    match array.data_type() {
        arrow_schema::DataType::Time32(TimeUnit::Second) => scale_time(
            i64::from(
                array
                    .as_primitive::<arrow_array::types::Time32SecondType>()
                    .value(row),
            ),
            TimeUnit::Second,
        ),
        arrow_schema::DataType::Time32(TimeUnit::Millisecond) => scale_time(
            i64::from(
                array
                    .as_primitive::<arrow_array::types::Time32MillisecondType>()
                    .value(row),
            ),
            TimeUnit::Millisecond,
        ),
        arrow_schema::DataType::Time64(TimeUnit::Microsecond) => scale_time(
            array
                .as_primitive::<arrow_array::types::Time64MicrosecondType>()
                .value(row),
            TimeUnit::Microsecond,
        ),
        arrow_schema::DataType::Time64(TimeUnit::Nanosecond) => scale_time(
            array
                .as_primitive::<arrow_array::types::Time64NanosecondType>()
                .value(row),
            TimeUnit::Nanosecond,
        ),
        other => Err(error::invalid(format!("not a time: {other:?}"))),
    }
}

fn scale_time(value: i64, unit: arrow_schema::TimeUnit) -> Result<i64> {
    use arrow_schema::TimeUnit;
    let scaled = match unit {
        TimeUnit::Second => value.checked_mul(1_000_000_000),
        TimeUnit::Millisecond => value.checked_mul(1_000_000),
        TimeUnit::Microsecond => value.checked_mul(1_000),
        TimeUnit::Nanosecond => Some(value),
    };
    scaled.ok_or_else(|| error::invalid("time value overflows nanoseconds"))
}

fn timestamp_to_nanos(
    array: &arrow_array::ArrayRef,
    row: usize,
    unit: &arrow_schema::TimeUnit,
) -> Result<i64> {
    use arrow_array::cast::AsArray;
    use arrow_schema::TimeUnit;
    let value = match unit {
        TimeUnit::Second => array
            .as_primitive::<arrow_array::types::TimestampSecondType>()
            .value(row),
        TimeUnit::Millisecond => array
            .as_primitive::<arrow_array::types::TimestampMillisecondType>()
            .value(row),
        TimeUnit::Microsecond => array
            .as_primitive::<arrow_array::types::TimestampMicrosecondType>()
            .value(row),
        TimeUnit::Nanosecond => array
            .as_primitive::<arrow_array::types::TimestampNanosecondType>()
            .value(row),
    };
    scale_time(value, *unit)
}

pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
