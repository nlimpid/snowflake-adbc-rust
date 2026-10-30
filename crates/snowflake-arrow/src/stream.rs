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

/// Multi-thread runtime kept alive by every database, connection, statement,
/// and batch reader that shares it.
///
/// The last owner calls `Runtime::shutdown_background` when the drop runs
/// inside some other Tokio runtime. `Runtime`'s own drop panics in that case,
/// and a detached thread would race unloading this crate from a process.
pub struct SharedRuntime {
    runtime: std::mem::ManuallyDrop<tokio::runtime::Runtime>,
}

impl SharedRuntime {
    pub fn new(runtime: tokio::runtime::Runtime) -> Self {
        Self {
            runtime: std::mem::ManuallyDrop::new(runtime),
        }
    }
}

impl Drop for SharedRuntime {
    fn drop(&mut self) {
        // Safety: `runtime` is initialized in `new` and this is the only take.
        let runtime = unsafe { std::mem::ManuallyDrop::take(&mut self.runtime) };
        if tokio::runtime::Handle::try_current().is_ok() {
            runtime.shutdown_background();
        } else {
            drop(runtime);
        }
    }
}

impl std::ops::Deref for SharedRuntime {
    type Target = tokio::runtime::Runtime;

    fn deref(&self) -> &Self::Target {
        &self.runtime
    }
}

/// Drive `future` on `runtime` from a synchronous caller.
///
/// `Handle::block_on` panics inside any runtime. `spawn_blocking` only moves a
/// closure onto the blocking pool; it does not drive a future on this runtime,
/// and waiting for that task from a current-thread runtime panics too.
/// Callers outside a runtime use `Runtime::block_on`. Callers already on this
/// runtime use `block_in_place`, which requires the multi-thread runtime built
/// by the driver. Callers on any other runtime, including current-thread, hop
/// to a fresh thread that is allowed to `block_on`. That thread is joined
/// before return, so each foreign call pays a thread spawn.
pub fn block_on_runtime<F, T>(runtime: &tokio::runtime::Runtime, future: F) -> T
where
    F: std::future::Future<Output = T> + Send,
    T: Send,
{
    match tokio::runtime::Handle::try_current() {
        Err(_) => runtime.block_on(future),
        Ok(current) if current.id() == runtime.handle().id() => {
            tokio::task::block_in_place(|| current.block_on(future))
        }
        Ok(_) => std::thread::scope(|scope| {
            let worker = scope.spawn(|| runtime.block_on(future));
            match worker.join() {
                Ok(value) => value,
                Err(payload) => std::panic::resume_unwind(payload),
            }
        }),
    }
}

/// Pulls an async batch stream from a multi-thread Tokio runtime. Used by ADBC.
///
/// Holds the runtime so the stream still runs after the database that created
/// it is dropped.
pub struct BlockingBatchReader {
    schema: SchemaRef,
    runtime: Arc<SharedRuntime>,
    stream: Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>,
}

impl BlockingBatchReader {
    pub fn new(
        runtime: Arc<SharedRuntime>,
        schema: SchemaRef,
        stream: Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>,
    ) -> Self {
        Self {
            schema,
            runtime,
            stream,
        }
    }
}

impl Iterator for BlockingBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let runtime = Arc::clone(&self.runtime);
        let next = self.stream.next();
        match block_on_runtime(&runtime, next) {
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
