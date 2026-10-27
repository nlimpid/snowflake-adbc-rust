//! Query results: inline first chunk plus ordered, bounded chunk downloads.

use bytes::Bytes;
use std::pin::Pin;

use futures::stream::{self, Stream, StreamExt};
use reqwest::Client;

use crate::error::{Error, Result};
use crate::protocol::{self, RawChunk, RowType, SessionParameters};
use crate::transport;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultFormat {
    Arrow,
    Json,
}

#[derive(Debug, Clone)]
pub enum ResultChunk {
    Json(Vec<Vec<Option<String>>>),
    ArrowIpc(Bytes),
}

#[derive(Clone)]
#[allow(dead_code)]
struct ChunkMeta {
    url: String,
    row_count: i64,
}

/// One statement's result. Chunks are delivered in order.
pub struct QueryResult {
    query_id: String,
    format: ResultFormat,
    row_type: Vec<RowType>,
    total_rows: i64,
    returned_rows: i64,
    statement_type_id: Option<i64>,
    number_of_binds: i64,
    sql_state: Option<String>,
    result_ids: Vec<String>,
    parameters: SessionParameters,
    pending: bool,
    initial_json: Option<Vec<Vec<Option<String>>>>,
    initial_arrow: Option<Bytes>,
    chunks: Vec<ChunkMeta>,
    http: Client,
    headers: Vec<(String, String)>,
    prefetch: usize,
    user_agent: String,
}

impl std::fmt::Debug for QueryResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryResult")
            .field("query_id", &self.query_id)
            .field("format", &self.format)
            .field("columns", &self.row_type.len())
            .field("total_rows", &self.total_rows)
            .field("chunks", &self.chunks.len())
            .field("pending", &self.pending)
            .finish_non_exhaustive()
    }
}

impl QueryResult {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        query_id: String,
        format: ResultFormat,
        row_type: Vec<RowType>,
        total_rows: i64,
        returned_rows: i64,
        statement_type_id: Option<i64>,
        number_of_binds: i64,
        sql_state: Option<String>,
        result_ids: Vec<String>,
        parameters: SessionParameters,
        pending: bool,
        initial_json: Option<Vec<Vec<Option<String>>>>,
        initial_arrow: Option<Bytes>,
        chunks: Vec<RawChunk>,
        http: Client,
        headers: Vec<(String, String)>,
        prefetch: usize,
        user_agent: String,
    ) -> Self {
        Self {
            query_id,
            format,
            row_type,
            total_rows,
            returned_rows,
            statement_type_id,
            number_of_binds,
            sql_state,
            result_ids,
            parameters,
            pending,
            initial_json,
            initial_arrow,
            chunks: chunks
                .into_iter()
                .map(|chunk| ChunkMeta {
                    url: chunk.url,
                    row_count: chunk.row_count,
                })
                .collect(),
            http,
            headers,
            prefetch,
            user_agent,
        }
    }

    pub fn query_id(&self) -> &str {
        &self.query_id
    }

    pub fn format(&self) -> ResultFormat {
        self.format
    }

    pub fn row_type(&self) -> &[RowType] {
        &self.row_type
    }

    pub fn total_rows(&self) -> i64 {
        self.total_rows
    }

    pub fn returned_rows(&self) -> i64 {
        self.returned_rows
    }

    pub fn statement_type_id(&self) -> Option<i64> {
        self.statement_type_id
    }

    pub fn number_of_binds(&self) -> i64 {
        self.number_of_binds
    }

    pub fn sql_state(&self) -> Option<&str> {
        self.sql_state.as_deref()
    }

    pub fn result_ids(&self) -> &[String] {
        &self.result_ids
    }

    pub fn parameters(&self) -> &SessionParameters {
        &self.parameters
    }

    pub fn is_pending(&self) -> bool {
        self.pending
    }

    pub(crate) fn set_query_id_if_empty(&mut self, query_id: String) {
        if self.query_id.is_empty() {
            self.query_id = query_id;
        }
    }

    /// Number of chunks still to download (the inline first chunk is not included).
    pub fn extra_chunk_count(&self) -> usize {
        self.chunks.len()
    }

    pub fn into_chunk_stream(self) -> Pin<Box<dyn Stream<Item = Result<ResultChunk>> + Send>> {
        let pending = self.pending;
        let initial = if pending {
            None
        } else if let Some(bytes) = self.initial_arrow {
            Some(Ok(ResultChunk::ArrowIpc(bytes)))
        } else {
            self.initial_json.map(|rows| Ok(ResultChunk::Json(rows)))
        };
        let http = self.http;
        let headers = self.headers;
        let user_agent = self.user_agent;
        let format = self.format;
        let prefetch = self.prefetch.max(1);
        let downloads = stream::iter(self.chunks.into_iter().map(move |chunk| {
            let http = http.clone();
            let headers = headers.clone();
            let user_agent = user_agent.clone();
            async move {
                let bytes =
                    transport::download_bytes(&http, &chunk.url, &headers, &user_agent).await?;
                decode_downloaded(format, bytes)
            }
        }))
        .buffered(prefetch);

        let head_items: Vec<Result<ResultChunk>> = if pending {
            vec![Err(Error::protocol(
                "query is still running; call fetch_result",
            ))]
        } else {
            initial.into_iter().collect()
        };
        Box::pin(stream::iter(head_items).chain(downloads))
    }
}

fn decode_downloaded(format: ResultFormat, bytes: Vec<u8>) -> Result<ResultChunk> {
    match format {
        ResultFormat::Arrow => Ok(ResultChunk::ArrowIpc(Bytes::from(bytes))),
        ResultFormat::Json => Ok(ResultChunk::Json(protocol::parse_json_chunk(&bytes)?)),
    }
}
