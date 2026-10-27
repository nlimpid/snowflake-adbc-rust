//! Stream a [`QueryResult`](snowflake_core::QueryResult) as `RecordBatch` values.

use std::pin::Pin;
use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use futures::stream::Stream;
use futures::StreamExt;
use snowflake_core::{QueryResult, Result, ResultChunk};

use crate::json::batch_from_json;
use crate::normalize::batches_from_ipc;
use crate::schema::{self, ArrowOptions};

pub fn record_batches(result: QueryResult) -> impl Stream<Item = Result<RecordBatch>> + Send {
    let opts = ArrowOptions {
        session_timezone: result.parameters().timezone().to_string(),
        ..ArrowOptions::default()
    };
    record_batches_with(result, opts)
}

pub fn record_batches_with(
    result: QueryResult,
    opts: ArrowOptions,
) -> impl Stream<Item = Result<RecordBatch>> + Send {
    let row_type = result.row_type().to_vec();
    let params = result.parameters().clone();
    let schema = schema::schema_for(&row_type, &params, &opts);
    async_stream::try_stream! {
        let schema = schema?;
        let mut emitted = false;
        let mut chunks = result.into_chunk_stream();
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk?;
            match chunk {
                ResultChunk::Json(rows) => {
                    if rows.is_empty() {
                        continue;
                    }
                    emitted = true;
                    yield batch_from_json(&row_type, &rows, &params, &opts)?;
                }
                ResultChunk::ArrowIpc(bytes) => {
                    let batches = batches_from_ipc(&bytes, &row_type, &params, &opts)?;
                    for batch in batches {
                        emitted = true;
                        yield batch;
                    }
                }
            }
        }
        if !emitted {
            yield RecordBatch::new_empty(Arc::new(schema));
        }
    }
}

/// Pulls an async batch stream from a multi-thread Tokio runtime. Used by ADBC.
pub struct BlockingBatchReader {
    schema: SchemaRef,
    handle: tokio::runtime::Handle,
    stream: Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>,
}

impl BlockingBatchReader {
    pub fn new(
        handle: tokio::runtime::Handle,
        schema: SchemaRef,
        stream: Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>,
    ) -> Self {
        Self {
            schema,
            handle,
            stream,
        }
    }
}

impl Iterator for BlockingBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.stream.next();
        match self.handle.block_on(next) {
            None => None,
            Some(Ok(batch)) => Some(Ok(batch)),
            Some(Err(err)) => Some(Err(ArrowError::ExternalError(Box::new(err)))),
        }
    }
}

impl RecordBatchReader for BlockingBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
