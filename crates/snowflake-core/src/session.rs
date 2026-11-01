//! Login, token renewal, query execution, cancellation, heartbeat.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use base64::Engine;
use bytes::Bytes;
use reqwest::Method;
use serde_json::{json, Value};
use tokio::sync::{Mutex, Notify};
use url::Url;

use crate::auth::normalize_jwt_account;
use crate::bind::{self, Bind};
use crate::config::{ClientConfig, GOSNOWFLAKE_APP_ID, GOSNOWFLAKE_COMPAT_VERSION};
use crate::error::{self, Error, Result};
use crate::protocol::{self, LoginData, QueryData, RenewData, SessionInfo, SessionParameters};
use crate::result::{QueryResult, ResultFormat};
use crate::transport::{self, JsonRequest, TokenKind};

const POLL_INITIAL: Duration = Duration::from_millis(500);
const POLL_MAX: Duration = Duration::from_secs(5);

struct Tokens {
    session_token: String,
    master_token: String,
}

struct SessionState {
    server_version: String,
    session_id: Option<i64>,
    database: Option<String>,
    schema: Option<String>,
    warehouse: Option<String>,
    role: Option<String>,
    parameters: SessionParameters,
    closed: bool,
}

#[derive(Clone)]
struct Inflight {
    /// Client `requestId` for a query this session submitted.
    request_id: Option<String>,
    /// Server query id, for `fetch_result` and for abort once it is known.
    query_id: Option<String>,
    sql: String,
}

struct Inner {
    http: reqwest::Client,
    base_url: Url,
    user_agent: String,
    config: ClientConfig,
    tokens: RwLock<Tokens>,
    state: RwLock<SessionState>,
    sequence: AtomicU64,
    query_context: Mutex<Value>,
    renew_lock: Mutex<()>,
    inflight: std::sync::Mutex<Option<Inflight>>,
    /// `Statement::cancel` arrived before `execute` recorded this request id.
    pending_cancel: std::sync::Mutex<Option<String>>,
    cancel_generation: AtomicU64,
    cancel_notify: Notify,
    heartbeat: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    closed: AtomicBool,
}

/// A logged-in Snowflake session. Cheap to clone; clones share the connection.
#[derive(Clone)]
pub struct Session {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("session_id", &self.session_id())
            .finish_non_exhaustive()
    }
}

impl Session {
    pub(crate) async fn login(http: reqwest::Client, config: ClientConfig) -> Result<Self> {
        let base_url = config.base_url()?;
        let user_agent = config.user_agent();
        let config_for_login = config.clone();
        let query = context_query(&config, None);
        let envelope = transport::send_json(
            &http,
            &base_url,
            JsonRequest {
                method: Method::POST,
                path: "session/v1/login-request",
                query,
                body: None,
                token: TokenKind::None,
                user_agent: &user_agent,
                attempt_timeout: config.login_timeout.unwrap_or(Duration::from_secs(60)),
                retry_budget: config.retry_timeout,
                request_id: None,
                body_factory: Some(Box::new(move || login_body(&config_for_login))),
            },
        )
        .await?;
        if !envelope.success {
            return Err(error_from_envelope(&envelope));
        }
        let data: LoginData = serde_json::from_value(envelope.data()?.clone())
            .map_err(|err| Error::protocol(format!("login response: {err}")))?;
        let mut parameters = SessionParameters::default();
        parameters.extend_from_response(&data.parameters);
        let mut state = SessionState {
            server_version: data.server_version.clone().unwrap_or_default(),
            session_id: data.session_id,
            database: None,
            schema: None,
            warehouse: None,
            role: None,
            parameters,
            closed: false,
        };
        if let Some(info) = &data.session_info {
            apply_session_info(&mut state, info);
        }
        let keep_alive = config.keep_alive || state.parameters.flag("CLIENT_SESSION_KEEP_ALIVE");
        let session = Self {
            inner: Arc::new(Inner {
                http,
                base_url,
                user_agent,
                config,
                tokens: RwLock::new(Tokens {
                    session_token: data.token,
                    master_token: data.master_token,
                }),
                state: RwLock::new(state),
                sequence: AtomicU64::new(1),
                query_context: Mutex::new(json!({ "entries": [] })),
                renew_lock: Mutex::new(()),
                inflight: std::sync::Mutex::new(None),
                pending_cancel: std::sync::Mutex::new(None),
                cancel_generation: AtomicU64::new(0),
                cancel_notify: Notify::new(),
                heartbeat: std::sync::Mutex::new(None),
                closed: AtomicBool::new(false),
            }),
        };
        if keep_alive {
            session.spawn_heartbeat();
        }
        tracing::info!(session_id = data.session_id, "snowflake session opened");
        Ok(session)
    }

    pub fn query(&self, sql: impl Into<String>) -> Query<'_> {
        Query {
            session: self,
            sql: sql.into(),
            binds: Vec::new(),
            describe_only: false,
            async_exec: false,
            parameters: HashMap::new(),
            request_id: None,
            prefetch: None,
        }
    }

    pub fn parameters(&self) -> SessionParameters {
        read_lock(&self.inner.state).parameters.clone()
    }

    pub fn server_version(&self) -> String {
        read_lock(&self.inner.state).server_version.clone()
    }

    pub fn session_id(&self) -> Option<i64> {
        read_lock(&self.inner.state).session_id
    }

    pub fn database(&self) -> Option<String> {
        read_lock(&self.inner.state).database.clone()
    }

    pub fn schema_name(&self) -> Option<String> {
        read_lock(&self.inner.state).schema.clone()
    }

    pub fn warehouse(&self) -> Option<String> {
        read_lock(&self.inner.state).warehouse.clone()
    }

    pub fn role(&self) -> Option<String> {
        read_lock(&self.inner.state).role.clone()
    }

    pub async fn get_query_status(&self, query_id: &str) -> Result<QueryStatus> {
        self.ensure_open()?;
        let path = format!("monitoring/queries/{query_id}");
        let envelope = self
            .authed(Method::GET, &path, Vec::new(), None, None)
            .await?;
        if !envelope.success {
            return Err(error_from_envelope(&envelope));
        }
        let data = envelope.data()?;
        let status = data
            .get("queries")
            .and_then(Value::as_array)
            .and_then(|q| q.first())
            .and_then(|q| q.get("status"))
            .or_else(|| data.get("status"))
            .and_then(Value::as_str)
            .unwrap_or("UNKNOWN");
        Ok(QueryStatus::parse(status))
    }

    pub async fn fetch_result(&self, query_id: &str) -> Result<QueryResult> {
        self.ensure_open()?;
        let path = format!("queries/{query_id}/result");
        let generation = self.inner.cancel_generation.load(Ordering::SeqCst);
        // The original client request id is not known here. Abort must use queryId.
        self.set_inflight(None, Some(query_id.to_string()), "");
        let result = self
            .poll_until_complete(&path, None, generation, None)
            .await;
        if matches!(result, Err(Error::Timeout)) {
            self.abort_after_timeout().await;
        }
        self.clear_inflight();
        result
    }

    pub async fn cancel(&self) -> Result<()> {
        self.inner.cancel_generation.fetch_add(1, Ordering::SeqCst);
        self.inner.cancel_notify.notify_waiters();
        self.abort_inflight().await
    }

    /// Abort the in-flight query submitted with `request_id`.
    ///
    /// An idle statement must not call [`Session::cancel`]: that bumps the
    /// session generation and cancels every query on the connection.
    pub async fn cancel_request(&self, request_id: &str) -> Result<()> {
        if self.inflight_request_is(request_id) {
            return self.cancel().await;
        }
        *self
            .inner
            .pending_cancel
            .lock()
            .unwrap_or_else(|err| err.into_inner()) = Some(request_id.to_string());
        if self.inflight_request_is(request_id) {
            *self
                .inner
                .pending_cancel
                .lock()
                .unwrap_or_else(|err| err.into_inner()) = None;
            return self.cancel().await;
        }
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        if self.inner.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        write_lock(&self.inner.state).closed = true;
        if let Some(handle) = self
            .inner
            .heartbeat
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            handle.abort();
        }
        let _ = self
            .authed(
                Method::POST,
                "session",
                vec![("delete", "true".into())],
                Some(json!({})),
                None,
            )
            .await;
        Ok(())
    }

    /// Replace the session token so the next request exercises renewal.
    /// Intended for the real-account renewal test.
    pub fn invalidate_session_token(&self) {
        write_lock(&self.inner.tokens).session_token = "invalid-session-token".into();
    }

    async fn execute(&self, query: QueryParts) -> Result<QueryResult> {
        self.ensure_open()?;
        let generation = self.inner.cancel_generation.load(Ordering::SeqCst);
        let request_id = query
            .request_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        if self.take_pending_cancel(&request_id) {
            return Err(Error::Cancelled);
        }
        self.set_inflight(Some(request_id.clone()), None, &query.sql);
        if self.take_pending_cancel(&request_id) {
            self.clear_inflight();
            return Err(Error::Cancelled);
        }
        let result = self.execute_inner(query, &request_id, generation).await;
        if matches!(result, Err(Error::Timeout)) {
            self.abort_after_timeout().await;
        }
        self.clear_inflight();
        result
    }

    async fn execute_inner(
        &self,
        query: QueryParts,
        request_id: &str,
        generation: u64,
    ) -> Result<QueryResult> {
        let sequence_id = self.inner.sequence.fetch_add(1, Ordering::SeqCst);
        let context = self.inner.query_context.lock().await.clone();
        let mut parameters = serde_json::Map::new();
        parameters.insert("GO_QUERY_RESULT_FORMAT".into(), json!("ARROW"));
        for (key, value) in &query.parameters {
            parameters.insert(key.clone(), json!(value));
        }
        let body = json!({
            "sqlText": query.sql,
            "asyncExec": query.async_exec,
            "sequenceId": sequence_id,
            "isInternal": false,
            "describeOnly": query.describe_only,
            "parameters": Value::Object(parameters),
            "bindings": bind::bindings_object(&query.binds)?,
            "bindStage": Value::Null,
            "queryContextDTO": context,
        });
        let deadline = self
            .inner
            .config
            .query_timeout
            .map(|timeout| Instant::now() + timeout);
        let envelope = self
            .authed_with_id(
                Method::POST,
                "queries/v1/query-request",
                Vec::new(),
                Some(body),
                Some(request_id),
                generation,
                deadline,
            )
            .await?;
        self.finish_query(
            envelope,
            query.async_exec,
            generation,
            deadline,
            query.prefetch,
        )
        .await
    }

    async fn finish_query(
        &self,
        envelope: protocol::Envelope,
        async_exec: bool,
        generation: u64,
        deadline: Option<Instant>,
        prefetch: Option<usize>,
    ) -> Result<QueryResult> {
        if envelope.success {
            return self.result_from_data(envelope.data()?, false, prefetch);
        }
        let code = envelope.code.clone().unwrap_or_default();
        if Error::is_query_in_progress(&code) {
            let data = envelope.data().ok().cloned();
            let url = data
                .as_ref()
                .and_then(|d| d.get("getResultUrl"))
                .and_then(Value::as_str)
                .map(str::to_string);
            let query_id = data
                .as_ref()
                .and_then(|d| d.get("queryId"))
                .and_then(Value::as_str)
                .map(str::to_string);
            if let Some(query_id) = query_id.as_deref() {
                self.note_query_id(query_id);
            }
            if async_exec {
                if let Some(data) = data.as_ref() {
                    let mut result = self.result_from_data(data, true, prefetch)?;
                    if let Some(query_id) = query_id {
                        result.set_query_id_if_empty(query_id);
                    }
                    return Ok(result);
                }
                return Err(Error::protocol("async query returned no query id"));
            }
            let path = match url {
                Some(url) => url,
                None => format!(
                    "queries/{}/result",
                    query_id.ok_or_else(|| Error::protocol("running query has no id"))?
                ),
            };
            return self
                .poll_until_complete(&path, deadline, generation, prefetch)
                .await;
        }
        Err(error_from_envelope(&envelope))
    }

    async fn poll_until_complete(
        &self,
        path: &str,
        deadline: Option<Instant>,
        generation: u64,
        prefetch: Option<usize>,
    ) -> Result<QueryResult> {
        let mut delay = POLL_INITIAL;
        loop {
            self.ensure_generation(generation)?;
            let envelope = self
                .authed_with_id(
                    Method::GET,
                    path,
                    Vec::new(),
                    None,
                    None,
                    generation,
                    deadline,
                )
                .await?;
            if envelope.success {
                return self.result_from_data(envelope.data()?, false, prefetch);
            }
            let code = envelope.code.clone().unwrap_or_default();
            if !Error::is_query_in_progress(&code) {
                return Err(error_from_envelope(&envelope));
            }
            self.sleep_or_cancel(delay, generation).await?;
            delay = (delay * 2).min(POLL_MAX);
        }
    }

    fn result_from_data(
        &self,
        data: &Value,
        pending: bool,
        prefetch: Option<usize>,
    ) -> Result<QueryResult> {
        let parsed = QueryData::from_value(data)?;
        {
            let mut state = write_lock(&self.inner.state);
            state.parameters.extend_from_response(&parsed.parameters);
            if let Some(name) = parsed.final_database_name.clone().filter(|s| !s.is_empty()) {
                state.database = Some(name);
            }
            if let Some(name) = parsed.final_schema_name.clone().filter(|s| !s.is_empty()) {
                state.schema = Some(name);
            }
            if let Some(name) = parsed
                .final_warehouse_name
                .clone()
                .filter(|s| !s.is_empty())
            {
                state.warehouse = Some(name);
            }
            if let Some(name) = parsed.final_role_name.clone().filter(|s| !s.is_empty()) {
                state.role = Some(name);
            }
        }
        if let Some(context) = parsed.query_context.clone() {
            // Best-effort: if the mutex is busy the next statement still sends the previous context.
            if let Ok(mut slot) = self.inner.query_context.try_lock() {
                *slot = context;
            }
        }
        let format = match parsed
            .query_result_format
            .as_deref()
            .map(|s| s.to_ascii_lowercase())
            .as_deref()
        {
            Some("arrow") => ResultFormat::Arrow,
            Some("json") => ResultFormat::Json,
            _ if parsed.rowset_base64.as_ref().is_some_and(|s| !s.is_empty()) => {
                ResultFormat::Arrow
            }
            _ => ResultFormat::Json,
        };
        let initial_arrow = if format == ResultFormat::Arrow {
            decode_rowset_base64(parsed.rowset_base64.as_deref())?
        } else {
            None
        };
        let initial_json = if format == ResultFormat::Json {
            parsed.rowset.filter(|rows| !rows.is_empty())
        } else {
            None
        };
        let parameters = self.parameters();
        let headers = chunk_request_headers(parsed.chunk_headers, parsed.qrmk);
        Ok(QueryResult::new(
            parsed.query_id.unwrap_or_default(),
            format,
            parsed.row_type,
            parsed.total,
            parsed.returned,
            parsed.statement_type_id,
            parsed.number_of_binds,
            parsed.sql_state,
            parsed.result_ids,
            parameters,
            pending,
            initial_json,
            initial_arrow,
            parsed.chunks,
            self.inner.http.clone(),
            headers,
            prefetch
                .unwrap_or(self.inner.config.prefetch_concurrency)
                .max(1),
            self.inner.user_agent.clone(),
        ))
    }

    async fn authed(
        &self,
        method: Method,
        path: &str,
        query: Vec<(&str, String)>,
        body: Option<Value>,
        deadline: Option<Instant>,
    ) -> Result<protocol::Envelope> {
        let generation = self.inner.cancel_generation.load(Ordering::SeqCst);
        self.authed_with_id(method, path, query, body, None, generation, deadline)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn authed_with_id(
        &self,
        method: Method,
        path: &str,
        query: Vec<(&str, String)>,
        body: Option<Value>,
        request_id: Option<&str>,
        generation: u64,
        deadline: Option<Instant>,
    ) -> Result<protocol::Envelope> {
        let mut renewed = false;
        let mut relogged = false;
        loop {
            self.ensure_generation(generation)?;
            let attempt_timeout = attempt_timeout(&self.inner.config, deadline)?;
            let session_token = read_lock(&self.inner.tokens).session_token.clone();
            let send = transport::send_json(
                &self.inner.http,
                &self.inner.base_url,
                JsonRequest {
                    method: method.clone(),
                    path,
                    query: query.clone(),
                    body: body.clone(),
                    token: TokenKind::Session(&session_token),
                    user_agent: &self.inner.user_agent,
                    attempt_timeout,
                    retry_budget: retry_budget(&self.inner.config, deadline)?,
                    request_id: request_id.map(str::to_string),
                    body_factory: None,
                },
            );
            let envelope = tokio::select! {
                result = send => result?,
                _ = self.wait_cancel(generation) => {
                    return Err(Error::Cancelled);
                }
            };
            if envelope.success
                || envelope
                    .code
                    .as_deref()
                    .is_some_and(Error::is_query_in_progress)
            {
                return Ok(envelope);
            }
            let code = envelope.code.clone().unwrap_or_default();
            if Error::is_session_expired(&code) && !renewed {
                self.renew().await?;
                renewed = true;
                continue;
            }
            if Error::is_master_expired(&code) && !relogged {
                self.relogin().await?;
                relogged = true;
                renewed = false;
                continue;
            }
            return Ok(envelope);
        }
    }

    async fn renew(&self) -> Result<()> {
        let _guard = self.inner.renew_lock.lock().await;
        match self.renew_locked().await {
            Ok(()) => Ok(()),
            Err(Error::SessionExpired) => self.login_locked().await,
            Err(err) => Err(err),
        }
    }

    async fn relogin(&self) -> Result<()> {
        let _guard = self.inner.renew_lock.lock().await;
        self.login_locked().await
    }

    async fn renew_locked(&self) -> Result<()> {
        let tokens = {
            let tokens = read_lock(&self.inner.tokens);
            (tokens.session_token.clone(), tokens.master_token.clone())
        };
        let envelope = transport::send_json(
            &self.inner.http,
            &self.inner.base_url,
            JsonRequest {
                method: Method::POST,
                path: "session/token-request",
                query: Vec::new(),
                body: Some(json!({
                    "oldSessionToken": tokens.0,
                    "requestType": "RENEW",
                })),
                token: TokenKind::Master(&tokens.1),
                user_agent: &self.inner.user_agent,
                attempt_timeout: Duration::from_secs(60),
                retry_budget: self.inner.config.retry_timeout,
                request_id: None,
                body_factory: None,
            },
        )
        .await?;
        if !envelope.success {
            let code = envelope.code.clone().unwrap_or_default();
            if Error::is_master_expired(&code) || Error::is_session_expired(&code) {
                return Err(Error::SessionExpired);
            }
            return Err(error_from_envelope(&envelope));
        }
        let data: RenewData = serde_json::from_value(envelope.data()?.clone())
            .map_err(|err| Error::protocol(format!("renew response: {err}")))?;
        let mut tokens = write_lock(&self.inner.tokens);
        tokens.session_token = data.session_token;
        if let Some(master) = data.master_token {
            tokens.master_token = master;
        }
        Ok(())
    }

    async fn login_locked(&self) -> Result<()> {
        let query = {
            let state = read_lock(&self.inner.state);
            context_query(&self.inner.config, Some(&state))
        };
        let config = self.inner.config.clone();
        let envelope = transport::send_json(
            &self.inner.http,
            &self.inner.base_url,
            JsonRequest {
                method: Method::POST,
                path: "session/v1/login-request",
                query,
                body: None,
                token: TokenKind::None,
                user_agent: &self.inner.user_agent,
                attempt_timeout: self
                    .inner
                    .config
                    .login_timeout
                    .unwrap_or(Duration::from_secs(60)),
                retry_budget: self.inner.config.retry_timeout,
                request_id: None,
                body_factory: Some(Box::new(move || login_body(&config))),
            },
        )
        .await?;
        if !envelope.success {
            // A rotated password or rejected key is an auth failure. Calling it
            // "session expired" hides the reason the new login did not succeed.
            return Err(error_from_envelope(&envelope));
        }
        let data: LoginData = serde_json::from_value(envelope.data()?.clone())
            .map_err(|err| Error::protocol(format!("relogin response: {err}")))?;
        {
            let mut tokens = write_lock(&self.inner.tokens);
            tokens.session_token = data.token;
            tokens.master_token = data.master_token;
        }
        {
            let mut state = write_lock(&self.inner.state);
            state.parameters.extend_from_response(&data.parameters);
            if let Some(info) = &data.session_info {
                apply_session_info(&mut state, info);
            }
            if let Some(session_id) = data.session_id {
                state.session_id = Some(session_id);
            }
            if let Some(version) = data.server_version.clone().filter(|s| !s.is_empty()) {
                state.server_version = version;
            }
        }
        Ok(())
    }

    async fn abort_inflight(&self) -> Result<()> {
        let inflight = self
            .inner
            .inflight
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .as_ref()
            .cloned();
        let Some(inflight) = inflight else {
            return Ok(());
        };
        let mut body = serde_json::Map::new();
        body.insert("sqlText".into(), json!(inflight.sql));
        if let Some(request_id) = inflight.request_id {
            body.insert("requestId".into(), json!(request_id));
        }
        if let Some(query_id) = inflight.query_id {
            body.insert("queryId".into(), json!(query_id));
        }
        let body = Value::Object(body);
        let _ = self
            .authed(
                Method::POST,
                "queries/v1/abort-request",
                Vec::new(),
                Some(body),
                None,
            )
            .await;
        Ok(())
    }

    fn spawn_heartbeat(&self) {
        let interval = heartbeat_interval(&read_lock(&self.inner.state).parameters);
        // Weak, so dropping the last Session drops Inner and aborts this task.
        // An Arc here would keep the server session alive after the user drops it.
        let weak = Arc::downgrade(&self.inner);
        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let Some(inner) = weak.upgrade() else {
                    break;
                };
                if inner.closed.load(Ordering::SeqCst) {
                    break;
                }
                let session = Session { inner };
                if session
                    .authed(
                        Method::POST,
                        "session/heartbeat",
                        Vec::new(),
                        Some(json!({})),
                        None,
                    )
                    .await
                    .is_err()
                {
                    tracing::debug!("session heartbeat failed");
                }
            }
        });
        *self
            .inner
            .heartbeat
            .lock()
            .unwrap_or_else(|err| err.into_inner()) = Some(handle);
    }

    fn set_inflight(&self, request_id: Option<String>, query_id: Option<String>, sql: &str) {
        *self
            .inner
            .inflight
            .lock()
            .unwrap_or_else(|err| err.into_inner()) = Some(Inflight {
            request_id,
            query_id,
            sql: sql.to_string(),
        });
    }

    fn note_query_id(&self, query_id: &str) {
        if query_id.is_empty() {
            return;
        }
        let mut inflight = self
            .inner
            .inflight
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        if let Some(inflight) = inflight.as_mut() {
            inflight.query_id = Some(query_id.to_string());
        }
    }

    fn inflight_request_is(&self, request_id: &str) -> bool {
        self.inner
            .inflight
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .as_ref()
            .is_some_and(|item| item.request_id.as_deref() == Some(request_id))
    }

    fn take_pending_cancel(&self, request_id: &str) -> bool {
        let mut pending = self
            .inner
            .pending_cancel
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        if pending.as_deref() == Some(request_id) {
            *pending = None;
            true
        } else {
            false
        }
    }

    async fn abort_after_timeout(&self) {
        if self.abort_inflight().await.is_err() {
            tracing::debug!("abort after query timeout failed");
        }
    }

    fn clear_inflight(&self) {
        *self
            .inner
            .inflight
            .lock()
            .unwrap_or_else(|err| err.into_inner()) = None;
    }

    fn ensure_open(&self) -> Result<()> {
        if self.inner.closed.load(Ordering::SeqCst) {
            Err(Error::protocol("session is closed"))
        } else {
            Ok(())
        }
    }

    fn ensure_generation(&self, generation: u64) -> Result<()> {
        if self.inner.cancel_generation.load(Ordering::SeqCst) != generation {
            Err(Error::Cancelled)
        } else {
            Ok(())
        }
    }

    async fn wait_cancel(&self, generation: u64) {
        loop {
            // Register the waiter before reading the generation. Otherwise a
            // cancel between the load and the first poll of `notified` is lost.
            let notified = self.inner.cancel_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.inner.cancel_generation.load(Ordering::SeqCst) != generation {
                return;
            }
            notified.await;
        }
    }

    async fn sleep_or_cancel(&self, delay: Duration, generation: u64) -> Result<()> {
        tokio::select! {
            _ = tokio::time::sleep(delay) => self.ensure_generation(generation),
            _ = self.wait_cancel(generation) => Err(Error::Cancelled),
        }
    }

    #[cfg(test)]
    fn idle_for_test(keep_alive: bool) -> Self {
        let config = test_client_config();
        let base_url = config.base_url().expect("base url");
        let user_agent = config.user_agent();
        let session = Self {
            inner: Arc::new(Inner {
                http: reqwest::Client::new(),
                base_url,
                user_agent,
                config,
                tokens: RwLock::new(Tokens {
                    session_token: "session".into(),
                    master_token: "master".into(),
                }),
                state: RwLock::new(SessionState {
                    server_version: String::new(),
                    session_id: None,
                    database: None,
                    schema: None,
                    warehouse: None,
                    role: None,
                    parameters: SessionParameters::default(),
                    closed: false,
                }),
                sequence: AtomicU64::new(1),
                query_context: Mutex::new(json!({ "entries": [] })),
                renew_lock: Mutex::new(()),
                inflight: std::sync::Mutex::new(None),
                pending_cancel: std::sync::Mutex::new(None),
                cancel_generation: AtomicU64::new(0),
                cancel_notify: Notify::new(),
                heartbeat: std::sync::Mutex::new(None),
                closed: AtomicBool::new(false),
            }),
        };
        if keep_alive {
            session.spawn_heartbeat();
        }
        session
    }
}

#[cfg(test)]
fn test_client_config() -> ClientConfig {
    ClientConfig {
        account: "xy12345.us-east-1.aws".into(),
        user: "svc".into(),
        authenticator: crate::auth::Authenticator::password("pw"),
        warehouse: Some("WH".into()),
        database: Some("DB".into()),
        schema: Some("PUBLIC".into()),
        role: Some("R".into()),
        host: None,
        port: None,
        protocol: "https".into(),
        app_name: crate::config::DEFAULT_APP_NAME.into(),
        prefetch_concurrency: 4,
        query_timeout: None,
        login_timeout: Some(Duration::from_secs(60)),
        retry_timeout: crate::config::DEFAULT_RETRY_TIMEOUT,
        http_attempt_timeout: Duration::from_secs(120),
        session_parameters: HashMap::new(),
        keep_alive: false,
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(handle) = self
            .heartbeat
            .get_mut()
            .unwrap_or_else(|err| err.into_inner())
            .take()
        {
            handle.abort();
        }
    }
}

fn login_body(config: &ClientConfig) -> Result<Value> {
    let material = config
        .authenticator
        .login_material(&config.account, &config.user)?;
    let mut session_parameters = serde_json::Map::new();
    session_parameters.insert("GO_QUERY_RESULT_FORMAT".into(), json!("ARROW"));
    for (key, value) in &config.session_parameters {
        session_parameters.insert(key.clone(), json!(value));
    }
    let mut data = serde_json::Map::new();
    data.insert("CLIENT_APP_ID".into(), json!(GOSNOWFLAKE_APP_ID));
    data.insert(
        "CLIENT_APP_VERSION".into(),
        json!(GOSNOWFLAKE_COMPAT_VERSION),
    );
    let account_name = normalize_jwt_account(&config.account);
    if account_name.is_empty() {
        return Err(Error::protocol("account name is empty"));
    }
    data.insert("ACCOUNT_NAME".into(), json!(account_name));
    data.insert("LOGIN_NAME".into(), json!(config.user));
    data.insert("AUTHENTICATOR".into(), json!(material.authenticator));
    if let Some(password) = material.password {
        data.insert("PASSWORD".into(), json!(password));
    }
    if let Some(token) = material.token {
        data.insert("TOKEN".into(), json!(token));
    }
    data.insert(
        "CLIENT_ENVIRONMENT".into(),
        json!({
            "APPLICATION": config.app_name,
            "OS": std::env::consts::OS,
            "OS_VERSION": "unknown",
            "OCSP_MODE": "FAIL_OPEN",
        }),
    );
    data.insert(
        "SESSION_PARAMETERS".into(),
        Value::Object(session_parameters),
    );
    Ok(json!({ "data": Value::Object(data) }))
}

fn error_from_envelope(envelope: &protocol::Envelope) -> Error {
    let code = envelope.code.clone().unwrap_or_default();
    let server_message = envelope.message.clone().unwrap_or_default();
    let sql_state = envelope
        .data
        .as_ref()
        .and_then(|data| data.get("sqlState"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let query_id = envelope
        .data
        .as_ref()
        .and_then(|data| data.get("queryId"))
        .and_then(Value::as_str)
        .map(str::to_string);
    if code == error::CODE_AUTH_FAILED
        || code == error::CODE_JWT_INVALID
        || Error::is_mfa_code(&code)
        || server_message.to_ascii_lowercase().contains("multi-factor")
    {
        let message = if Error::is_mfa_code(&code)
            || server_message.to_ascii_lowercase().contains("multi-factor")
        {
            format!("{} ({server_message})", error::MFA_MESSAGE)
        } else if code == error::CODE_JWT_INVALID {
            format!("{}. {server_message}", error::JWT_MESSAGE)
        } else {
            server_message
        };
        return Error::Auth { code, message };
    }
    if code.is_empty() {
        return Error::protocol(format!("request failed: {server_message}"));
    }
    Error::Server {
        code,
        message: server_message,
        sql_state,
        query_id,
    }
}

fn decode_rowset_base64(value: Option<&str>) -> Result<Option<Bytes>> {
    let Some(value) = value.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(value)
        .map_err(|err| Error::protocol(format!("rowsetBase64: {err}")))?;
    Ok(Some(Bytes::from(bytes)))
}

fn heartbeat_interval(parameters: &SessionParameters) -> Duration {
    let seconds = parameters
        .get("CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(3600)
        .max(1);
    Duration::from_secs(seconds)
}

fn retry_budget(config: &ClientConfig, deadline: Option<Instant>) -> Result<Duration> {
    let mut budget = config.retry_timeout;
    if let Some(deadline) = deadline {
        let now = Instant::now();
        if now >= deadline {
            return Err(Error::Timeout);
        }
        budget = budget.min(deadline.saturating_duration_since(now));
    }
    Ok(budget.max(Duration::from_millis(1)))
}

fn attempt_timeout(config: &ClientConfig, deadline: Option<Instant>) -> Result<Duration> {
    let mut timeout = config.http_attempt_timeout;
    if let Some(deadline) = deadline {
        let now = Instant::now();
        if now >= deadline {
            return Err(Error::Timeout);
        }
        timeout = timeout.min(deadline.saturating_duration_since(now));
    }
    Ok(timeout.max(Duration::from_millis(1)))
}

fn context_query(
    config: &ClientConfig,
    state: Option<&SessionState>,
) -> Vec<(&'static str, String)> {
    let mut query = Vec::new();
    push_owned(
        &mut query,
        "databaseName",
        prefer(
            state.and_then(|state| state.database.as_deref()),
            config.database.as_deref(),
        ),
    );
    push_owned(
        &mut query,
        "schemaName",
        prefer(
            state.and_then(|state| state.schema.as_deref()),
            config.schema.as_deref(),
        ),
    );
    push_owned(
        &mut query,
        "warehouse",
        prefer(
            state.and_then(|state| state.warehouse.as_deref()),
            config.warehouse.as_deref(),
        ),
    );
    push_owned(
        &mut query,
        "roleName",
        prefer(
            state.and_then(|state| state.role.as_deref()),
            config.role.as_deref(),
        ),
    );
    query
}

fn prefer(primary: Option<&str>, fallback: Option<&str>) -> Option<String> {
    if let Some(value) = nonempty(primary) {
        return Some(value.to_string());
    }
    nonempty(fallback).map(str::to_string)
}

fn push_owned(query: &mut Vec<(&'static str, String)>, key: &'static str, value: Option<String>) {
    if let Some(value) = value.filter(|value| !value.is_empty()) {
        query.push((key, value));
    }
}

fn nonempty(value: Option<&str>) -> Option<&str> {
    value.filter(|value| !value.is_empty())
}

fn apply_session_info(state: &mut SessionState, info: &SessionInfo) {
    let assign = |slot: &mut Option<String>, value: &Option<String>| {
        if let Some(value) = nonempty(value.as_deref()) {
            *slot = Some(value.to_string());
        }
    };
    assign(&mut state.database, &info.database_name);
    assign(&mut state.schema, &info.schema_name);
    assign(&mut state.warehouse, &info.warehouse_name);
    assign(&mut state.role, &info.role_name);
}

/// Headers for a chunk download.
///
/// When Snowflake omits `chunkHeaders`, the Go driver sends the SSE-C
/// algorithm and the customer key. S3 rejects the key without `AES256`.
fn chunk_request_headers(
    chunk_headers: HashMap<String, String>,
    qrmk: Option<String>,
) -> Vec<(String, String)> {
    if !chunk_headers.is_empty() {
        return chunk_headers.into_iter().collect();
    }
    let mut headers = Vec::new();
    if let Some(qrmk) = qrmk.filter(|value| !value.is_empty()) {
        headers.push((
            "x-amz-server-side-encryption-customer-algorithm".into(),
            "AES256".into(),
        ));
        headers.push(("x-amz-server-side-encryption-customer-key".into(), qrmk));
    }
    headers
}

fn read_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|err| err.into_inner())
}

fn write_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(|err| err.into_inner())
}

struct QueryParts {
    sql: String,
    binds: Vec<Bind>,
    describe_only: bool,
    async_exec: bool,
    parameters: HashMap<String, String>,
    request_id: Option<String>,
    prefetch: Option<usize>,
}

/// A statement built on a [`Session`].
pub struct Query<'a> {
    session: &'a Session,
    sql: String,
    binds: Vec<Bind>,
    describe_only: bool,
    async_exec: bool,
    parameters: HashMap<String, String>,
    request_id: Option<String>,
    prefetch: Option<usize>,
}

impl<'a> Query<'a> {
    pub fn bind(mut self, bind: Bind) -> Self {
        self.binds.push(bind);
        self
    }

    pub fn binds(mut self, binds: impl IntoIterator<Item = Bind>) -> Self {
        self.binds.extend(binds);
        self
    }

    pub fn describe_only(mut self, describe_only: bool) -> Self {
        self.describe_only = describe_only;
        self
    }

    pub fn async_exec(mut self, async_exec: bool) -> Self {
        self.async_exec = async_exec;
        self
    }

    pub fn parameter(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.parameters.insert(key.into(), value.into());
        self
    }

    /// Use this id for the query-request and for [`Session::cancel_request`].
    pub fn request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }

    /// Chunk-download concurrency for this statement. Falls back to the session default.
    pub fn prefetch(mut self, threads: usize) -> Self {
        self.prefetch = Some(threads.max(1));
        self
    }

    pub async fn execute(self) -> Result<QueryResult> {
        self.session
            .execute(QueryParts {
                sql: self.sql,
                binds: self.binds,
                describe_only: self.describe_only,
                async_exec: self.async_exec,
                parameters: self.parameters,
                request_id: self.request_id,
                prefetch: self.prefetch,
            })
            .await
    }
}

/// Status reported by the monitoring endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryStatus {
    Running,
    Success,
    Failed,
    Aborted,
    Other(String),
}

impl QueryStatus {
    fn parse(status: &str) -> Self {
        match status {
            "RUNNING"
            | "QUEUED"
            | "RESUMING_WAREHOUSE"
            | "QUEUED_REPARING_WAREHOUSE"
            | "BLOCKED"
            | "NO_DATA" => Self::Running,
            "SUCCESS" => Self::Success,
            "FAILED_WITH_ERROR" | "FAILED_WITH_INCIDENT" => Self::Failed,
            "ABORTED" | "ABORTING" | "DISCONNECTED" => Self::Aborted,
            other => Self::Other(other.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::Ordering;

    use super::{
        apply_session_info, chunk_request_headers, context_query, login_body, test_client_config,
        QueryStatus, Session, SessionInfo, SessionParameters, SessionState,
    };

    #[test]
    fn query_status_parse() {
        assert_eq!(QueryStatus::parse("SUCCESS"), QueryStatus::Success);
        assert_eq!(QueryStatus::parse("RUNNING"), QueryStatus::Running);
        assert_eq!(QueryStatus::parse("ABORTED"), QueryStatus::Aborted);
    }

    #[test]
    fn login_account_name_strips_region_and_host() {
        let mut config = test_client_config();
        let body = login_body(&config).unwrap();
        assert_eq!(body["data"]["ACCOUNT_NAME"], "XY12345");

        config.account = "myorg-myacct.snowflakecomputing.com".into();
        let body = login_body(&config).unwrap();
        assert_eq!(body["data"]["ACCOUNT_NAME"], "MYORG-MYACCT");
    }

    #[test]
    fn relogin_query_keeps_session_context() {
        let config = test_client_config();
        let fresh = context_query(&config, None);
        assert_eq!(query_value(&fresh, "databaseName"), Some("DB"));
        assert_eq!(query_value(&fresh, "schemaName"), Some("PUBLIC"));
        assert_eq!(query_value(&fresh, "warehouse"), Some("WH"));
        assert_eq!(query_value(&fresh, "roleName"), Some("R"));

        let mut state = SessionState {
            server_version: String::new(),
            session_id: None,
            database: Some("OTHER".into()),
            schema: Some("NEW".into()),
            warehouse: None,
            role: Some(String::new()),
            parameters: SessionParameters::default(),
            closed: false,
        };
        let relogin = context_query(&config, Some(&state));
        assert_eq!(query_value(&relogin, "databaseName"), Some("OTHER"));
        assert_eq!(query_value(&relogin, "schemaName"), Some("NEW"));
        assert_eq!(query_value(&relogin, "warehouse"), Some("WH"));
        assert_eq!(query_value(&relogin, "roleName"), Some("R"));

        apply_session_info(
            &mut state,
            &SessionInfo {
                database_name: Some("FROM_LOGIN".into()),
                schema_name: None,
                warehouse_name: Some("WH2".into()),
                role_name: Some("SYSADMIN".into()),
            },
        );
        assert_eq!(state.database.as_deref(), Some("FROM_LOGIN"));
        assert_eq!(state.schema.as_deref(), Some("NEW"));
        assert_eq!(state.warehouse.as_deref(), Some("WH2"));
        assert_eq!(state.role.as_deref(), Some("SYSADMIN"));
    }

    #[test]
    fn missing_chunk_headers_include_sse_c_algorithm() {
        let headers = chunk_request_headers(HashMap::new(), Some("qrmk".into()));
        assert!(headers.iter().any(|(key, value)| {
            key == "x-amz-server-side-encryption-customer-algorithm" && value == "AES256"
        }));
        assert!(headers.iter().any(|(key, value)| key
            == "x-amz-server-side-encryption-customer-key"
            && value == "qrmk"));
        let mut provided = HashMap::new();
        provided.insert("x-ms-blob-type".into(), "BlockBlob".into());
        let headers = chunk_request_headers(provided, Some("qrmk".into()));
        assert_eq!(headers.len(), 1);
        assert_eq!(headers[0].0, "x-ms-blob-type");
    }

    #[tokio::test]
    async fn dropping_keep_alive_session_drops_inner() {
        let session = Session::idle_for_test(true);
        let weak = std::sync::Arc::downgrade(&session.inner);
        drop(session);
        assert!(weak.upgrade().is_none());
    }

    #[tokio::test]
    async fn cancel_request_does_not_bump_generation_for_another_statement() {
        let session = Session::idle_for_test(false);
        session.set_inflight(Some("mine".into()), None, "select 1");
        let before = session.inner.cancel_generation.load(Ordering::SeqCst);
        session.cancel_request("other").await.unwrap();
        assert_eq!(
            session.inner.cancel_generation.load(Ordering::SeqCst),
            before
        );
        assert!(session.take_pending_cancel("other"));
    }

    fn query_value<'a>(query: &'a [(&str, String)], key: &str) -> Option<&'a str> {
        query
            .iter()
            .find(|(name, _)| *name == key)
            .map(|(_, value)| value.as_str())
    }
}
