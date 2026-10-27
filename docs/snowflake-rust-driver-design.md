# Snowflake Rust 驱动技术方案

> 目标：在本仓库自研一个 Snowflake 的 Rust 驱动，基于 Snowflake 的 REST 协议（与官方 Go / Python / JDBC 驱动走同一套接口），原生输出 Arrow `RecordBatch`，并对外提供 ADBC 接口。认证优先支持密码（Legacy 单因素），随后支持 Key-Pair JWT。列解码采用严格类型映射，解析失败即报错，不悄悄降级为 NULL。

---

## 1. 背景与决策

### 1.1 为什么自研而不是直接用社区库

| 方案 | 结果格式 | 认证 | 依赖 | 问题 |
| --- | --- | --- | --- | --- |
| `snowflake-connector-rs`（estie） | 仅 JSON rowset | 密码 / Key-Pair | reqwest，不依赖 arrow | 没有 Arrow 结果格式，大结果集走 JSON 字符串解析，数值/时间全部是文本再转换；要做 ADBC 还得自己把 JSON 转 Arrow，等于重做一半 |
| `snowflake-api`（mycelial） | Arrow（伪装 Go 驱动）+ JSON | 密码 / Key-Pair | reqwest + arrow | 维护活跃度一般，arrow 版本容易和我们锁死的版本冲突；错误处理与类型映射不够严格（部分类型退化为字符串） |
| 通过 ADBC driver manager 加载官方 Go 驱动（`libadbc_driver_snowflake.so`） | Arrow | 全部 | cgo 产物、C FFI | 需要分发平台相关的动态库，构建链路复杂；类型映射由 Go 驱动决定（默认 NUMBER 会被转成 int64/float64，有精度丢失），无法按我们的严格要求定制 |
| **自研（本方案）** | Arrow + JSON 双解码 | 密码 → JWT → 其他 | reqwest（与 castor 对齐）、arrow（可选 feature） | 需要自己实现协议，但协议面不大（登录、查询、分片下载、续期、取消），且可以完全控制类型映射和错误语义 |

选择自研的核心原因：

1. **类型映射必须严格可控**。Snowflake 的 Arrow 物理编码比较特殊（`NUMBER` 可能是带 `scale` 元数据的 `Int64`，`TIMESTAMP` 可能是 `Struct{epoch, fraction}`），现有库的处理方式要么丢精度，要么静默转字符串。
2. **双结果格式不可避免**。即使协商了 Arrow，DDL / `SHOW` / `DESCRIBE` 等语句仍然返回 JSON rowset，任何驱动都必须同时实现两套解码，社区库在 JSON 分支普遍比较随意。
3. **依赖版本自主**。reqwest 版本和 castor 一致，arrow 版本和 ADBC 一致，不被第三方库锁死。
4. **协议本身稳定**。Snowflake 的 REST 接口（`/session/v1/login-request`、`/queries/v1/query-request` 等）多年未变，官方 Go / Python 驱动是可参考的开源实现。

### 1.2 认证现状

Snowflake 正在全面禁用“仅密码”的单因素登录（人类用户已强制 MFA；服务账号需要显式设为 `TYPE = LEGACY_SERVICE` 才能继续用密码）。因此：

- **P0：密码（Legacy 单因素）**：`AUTHENTICATOR = "SNOWFLAKE"`，用于 `LEGACY_SERVICE` 类型的服务账号以及过渡期。
- **P1：Key-Pair JWT**：`AUTHENTICATOR = "SNOWFLAKE_JWT"`，Snowflake 推荐的服务账号方案，长期主力。
- **P2（低成本顺带）**：Programmatic Access Token（`AUTHENTICATOR = "PROGRAMMATIC_ACCESS_TOKEN"`）、OAuth（`AUTHENTICATOR = "OAUTH"`）——登录请求体只是多一个 `TOKEN` 字段，复用同一条路径。
- 不做：外部浏览器 SSO、Okta 原生、MFA push/TOTP 交互（需要用户交互，不适合服务端驱动）。

---

## 2. 总体架构

### 2.1 Crate 划分

```
snowflake-rs/                      # Cargo workspace
├── crates/
│   ├── snowflake-core/            # 协议层：HTTP、认证、会话、查询、分片下载、JSON 结果解码（不依赖 arrow）
│   ├── snowflake-arrow/           # Arrow 层：Arrow IPC 解码 + Snowflake 物理类型 → 标准 Arrow 类型归一化 + JSON rowset → RecordBatch
│   ├── snowflake-adbc/            # ADBC 层：实现 adbc_core 的 Driver/Database/Connection/Statement
│   └── snowflake-adbc-ffi/        # cdylib，用 adbc_ffi::export_driver! 导出 C ABI 的 ADBC 驱动（可被任何语言的 driver manager 加载）
├── tests/                         # 集成测试（需要真实账号，默认 #[ignore]）
└── fixtures/                      # 录制的协议响应 / Arrow IPC 样本，供单元测试使用
```

分层原则：

- `snowflake-core` 只做协议和“原始结果”：JSON rowset 保持为 `serde_json::Value`/字符串，Arrow 结果保持为未解码的 IPC 字节。它不依赖 arrow，方便只想用轻量 JSON 的调用方（和 `snowflake-connector-rs` 的定位一致）。
- `snowflake-arrow` 是唯一知道 Arrow 的地方，所有类型映射规则集中在这里，便于审计“严格映射”是否被破坏。
- `snowflake-adbc` 只做接口适配和选项解析，不包含业务逻辑。

### 2.2 核心依赖

| 用途 | crate | 说明 |
| --- | --- | --- |
| HTTP | `reqwest`（features: `rustls-tls`, `json`, `gzip`, `stream`） | **版本与 castor 在 workspace 中统一**。`gzip` 用于分片下载的 `Content-Encoding: gzip` 透明解压 |
| 异步运行时 | `tokio` | 核心 API 是 async；ADBC 层内部持有/借用 runtime 做同步桥接 |
| 序列化 | `serde`, `serde_json` | 协议请求/响应 |
| Arrow | `arrow-array`, `arrow-schema`, `arrow-ipc`, `arrow-buffer` | **版本与 `adbc_core` 依赖的 arrow 大版本对齐**（arrow-rs 每月出大版本，必须锁定） |
| ADBC | `adbc_core`, `adbc_ffi`（导出）, `adbc_driver_manager`（仅测试） | Apache Arrow ADBC 官方 Rust crate |
| JWT / 密钥 | `rsa`, `pkcs8`, `sha2`, `base64`, `jsonwebtoken`（或手工拼 RS256） | 支持 PKCS#8 PEM，含 PBES2 加密私钥 |
| 其他 | `uuid`（requestId）, `thiserror`, `tracing`, `bytes`, `futures` | |

不引入数据库连接池、不引入 ORM，保持驱动定位。

### 2.3 Feature flags

- `snowflake-core`：无可选 feature（保持最小）。
- `snowflake-arrow`：默认开启。
- `snowflake-adbc`：`ffi`（构建 cdylib）。

---

## 3. 协议层设计（`snowflake-core`）

### 3.1 端点与通用约定

Base URL：`https://{host}` ，`host` 默认由 `account` 推导：`{account}.snowflakecomputing.com`。必须允许显式覆盖 `host`，以支持：

- PrivateLink（`xxx.privatelink.snowflakecomputing.com`）
- 中国区（`snowflakecomputing.cn`）
- 内部代理

所有请求：

- Query 参数：`requestId={uuid4}`、`request_guid={uuid4}`；重试时 `requestId` 保持不变（服务端用它去重），`request_guid` 每次新生成，并追加 `retryCount`、`clientStartTime`。
- Header：`Content-Type: application/json`、`Accept: application/snowflake`、`User-Agent: {client_app_id}/{version} (rust ...)`。
- 登录后的请求带 `Authorization: Snowflake Token="{session_token}"`。
- 响应统一包裹：`{ "data": ..., "code": "390112" | null, "message": ..., "success": bool }`。**HTTP 200 不代表成功**，必须看 `success`。

| 功能 | 方法 & 路径 | 说明 |
| --- | --- | --- |
| 登录 | `POST /session/v1/login-request?databaseName=&schemaName=&warehouse=&roleName=` | 返回 `token`、`masterToken`、`validityInSeconds`、`sessionId`、`parameters[]`（含 `TIMEZONE` 等会话参数） |
| 续期 | `POST /session/token-request`，Header 用 `masterToken`，Body `{"oldSessionToken": ..., "requestType": "RENEW"}` | 收到错误码 `390112`（session token 过期）时触发，然后重放原请求 |
| 执行 | `POST /queries/v1/query-request` | 见 3.3 |
| 轮询结果 | `GET /queries/{queryId}/result` 或响应中的 `data.getResultUrl` | 执行返回 `333333` / `333334`（查询仍在运行）时轮询 |
| 取消 | `POST /queries/v1/abort-request`，Body `{"sqlText": ..., "requestId": 原请求的 requestId}` | |
| 心跳 | `POST /session/heartbeat` | 开启 `CLIENT_SESSION_KEEP_ALIVE` 时按 `CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY` 发送 |
| 关闭 | `POST /session?delete=true` | |

### 3.2 认证

#### 登录请求体

```json
{
  "data": {
    "CLIENT_APP_ID": "...",
    "CLIENT_APP_VERSION": "...",
    "ACCOUNT_NAME": "<account>",
    "LOGIN_NAME": "<user>",
    "AUTHENTICATOR": "SNOWFLAKE" | "SNOWFLAKE_JWT" | "PROGRAMMATIC_ACCESS_TOKEN" | "OAUTH",
    "PASSWORD": "<仅 SNOWFLAKE>",
    "TOKEN": "<JWT / PAT / OAuth token>",
    "CLIENT_ENVIRONMENT": { "APPLICATION": "...", "OS": "...", "OS_VERSION": "...", "OCSP_MODE": "FAIL_OPEN" },
    "SESSION_PARAMETERS": { ... }
  }
}
```

代码上用一个 `enum Authenticator` 建模，`LoginRequest` 由它生成，后续新增认证方式只需加一个变体：

```rust
pub enum Authenticator {
    Password { password: SecretString },
    KeyPair { private_key: RsaPrivateKey, public_key_fingerprint: String },
    ProgrammaticAccessToken { token: SecretString },
    OAuth { token: SecretString },
}
```

#### 密码（P0）

`AUTHENTICATOR = "SNOWFLAKE"`，`PASSWORD` 明文放请求体（走 TLS）。需要识别的失败：

- `390100` 用户名/密码错误
- `390102` / `390127` 等 MFA 相关错误码：明确报错“该用户需要 MFA，驱动不支持交互式 MFA，请使用 Key-Pair 或把用户设为 LEGACY_SERVICE”，而不是笼统的认证失败。

#### Key-Pair JWT（P1）

- 输入：PKCS#8 PEM 私钥（文件路径或内容），可选 passphrase（PBES2 解密）。
- 公钥指纹：`SHA256:` + base64(SHA-256(DER(SubjectPublicKeyInfo)))。
- Claims：
  - `iss = "{ACCOUNT}.{USER}.SHA256:{fp}"`
  - `sub = "{ACCOUNT}.{USER}"`
  - `iat = now`，`exp = now + 60s`（Snowflake 要求 ≤ 1 小时；每次登录重新签发，不缓存）
  - `ACCOUNT`、`USER` 全部大写；`ACCOUNT` 要去掉 region / cloud 后缀（`xy12345.us-east-1` → `XY12345`；`myorg-myacct` 保持 `MYORG-MYACCT`）。
- 签名 RS256，放入 `TOKEN`，`AUTHENTICATOR = "SNOWFLAKE_JWT"`。
- 常见失败：`390144`（JWT 无效/公钥不匹配）给出明确提示（指纹不匹配、时钟漂移）。

#### 会话与令牌生命周期

```rust
struct Session {
    http: reqwest::Client,
    base_url: Url,
    tokens: RwLock<Tokens>,            // session_token, master_token, 过期时间
    params: RwLock<SessionParameters>, // TIMEZONE、CLIENT_RESULT_CHUNK_SIZE 等，随每个响应的 parameters[] 更新
    sequence_id: AtomicU64,
    query_context: Mutex<QueryContext>, // 回显 queryContext（hybrid table 一致性），初期可发空
}
```

续期策略：请求返回 `390112` → 用 master token 调 `/session/token-request` → 更新 session token → 重放原请求一次。续期本身失败（master token 也过期，`390114`）→ 如果持有凭据则重新登录一次，否则向上抛 `Error::SessionExpired`。用一个 `Mutex` 串行化续期，避免并发请求同时触发多次续期。

### 3.3 查询执行

请求体：

```json
{
  "sqlText": "select ...",
  "asyncExec": false,
  "sequenceId": 7,
  "isInternal": false,
  "describeOnly": false,
  "parameters": { },
  "bindings": { "1": { "type": "TEXT", "value": "abc" } },
  "bindStage": null,
  "queryContextDTO": { "entries": [] }
}
```

响应 `data` 中关心的字段：

| 字段 | 含义 |
| --- | --- |
| `queryId` | 后续取消 / 轮询 / 诊断用 |
| `queryResultFormat` | `"arrow"` 或 `"json"`，**逐查询判断**，不要假设协商了 Arrow 就一定是 Arrow |
| `rowtype[]` | 列元数据：`name`, `type`（小写逻辑类型）, `nullable`, `precision`, `scale`, `length`, `byteLength`, `database`/`schema`/`table` |
| `rowset` | JSON 格式时的首批数据（二维数组，元素是字符串或 null） |
| `rowsetBase64` | Arrow 格式时的首批数据，base64 编码的 Arrow IPC stream；0 行时可能是空字符串 |
| `chunks[]` | 剩余分片：`url`, `rowCount`, `uncompressedSize`, `compressedSize` |
| `chunkHeaders` / `qrmk` | 下载分片要带的 header（S3 SSE-C 密钥）；没有 `chunkHeaders` 时用 `x-amz-server-side-encryption-customer-key: {qrmk}` |
| `total` / `returned` | 总行数 / 首批行数 |
| `parameters[]` | 会话参数更新（如 `TIMEZONE` 被 `ALTER SESSION` 改了） |
| `statementTypeId` | 判断是 DML / DDL / SELECT，用于 ADBC `execute_update` 的受影响行数 |
| `finalDatabaseName` 等 | `USE DATABASE` 之后的会话状态 |
| `queryContext` | 回显到下一次请求的 `queryContextDTO` |

#### Arrow 结果格式协商

Snowflake 服务端按 `CLIENT_APP_ID` 决定结果格式参数名：Go 驱动是 `GO_QUERY_RESULT_FORMAT`，Python 是 `PYTHON_CONNECTOR_QUERY_RESULT_FORMAT`，JDBC 是 `JDBC_QUERY_RESULT_FORMAT`。没有面向未知客户端的通用参数。做法与 `snowflake-api`、ADBC Go 驱动一致：

- 登录时 `CLIENT_APP_ID = "Go"`、`CLIENT_APP_VERSION` 填一个较新的 gosnowflake 版本号，`SESSION_PARAMETERS` 中带 `"GO_QUERY_RESULT_FORMAT": "ARROW"`。
- `User-Agent` 和 `CLIENT_ENVIRONMENT.APPLICATION` 仍然如实写我们自己的名字，便于在 `QUERY_HISTORY` 里区分。
- 该参数的具体名字与是否必须显式传递，在首次联调时用真实账号确认（这是整个协议中唯一的灰色地带，其余均有公开驱动源码可对照）。

无论协商结果如何，解码器都必须同时支持两种格式：DDL、`SHOW ...`、`DESCRIBE ...`、`USE ...` 以及部分很小的结果集在 Arrow 模式下仍然以 JSON 返回。

#### 长查询轮询

执行请求的 HTTP 调用服务端最多挂起约 45 秒；超过后返回 `code = "333334"`（或 `"333333"`）并带 `data.getResultUrl`。驱动需要循环 `GET` 该 URL（退避 0.5s → 1s → … → 上限 5s）直到 `success = true` 或出错。这是 P0 必须实现的功能，否则任何超过 45 秒的查询都会失败。

同时支持 `asyncExec = true` 的真正异步执行：立即返回 `queryId`，调用方之后用 `get_query_status(query_id)` / `fetch_result(query_id)` 取结果（原生 API 暴露，ADBC 不暴露）。

#### 绑定参数

Snowflake 服务端绑定（server-side binding），`?` 位置占位，`bindings` 的 key 是从 1 开始的字符串序号：

| Rust/Arrow 类型 | 绑定 `type` | `value` 编码 |
| --- | --- | --- |
| 字符串 | `TEXT` | 原文 |
| 整数 / Decimal | `FIXED` | 十进制字符串 |
| 浮点 | `REAL` | 十进制字符串 |
| bool | `BOOLEAN` | `"true"/"false"` |
| Date32 | `DATE` | 自 epoch 的毫秒数字符串 |
| Time64 | `TIME` | 自午夜的纳秒数字符串 |
| Timestamp(NTZ/LTZ/TZ) | `TIMESTAMP_NTZ` / `TIMESTAMP_LTZ` / `TIMESTAMP_TZ` | 纳秒 epoch 字符串（TZ 附带偏移） |
| 二进制 | `BINARY` | hex 字符串 |
| NULL | 任意 | `null` |
| 数组（批量插入） | 同上 | `value` 为数组，服务端做 array binding（超过阈值会要求 `bindStage`，P2 再做） |

ADBC 的 `bind(RecordBatch)` 对应：每一行一次执行（P0），或在 `arrayBindSupported = true` 时整列转成数组绑定（P2）。

### 3.4 分片下载

- 首批数据在响应里，之后 `chunks[]` 按顺序下载。每个分片是独立的 Arrow IPC stream（Arrow 格式）或去掉外层 `[` `]` 的 JSON 行数组片段（JSON 格式），HTTP body 通常是 gzip，由 reqwest 透明解压。
- 下载 header：`chunkHeaders` 中的全部键值（SSE-C）或 `qrmk`，加 `User-Agent`。不带 `Authorization`（URL 是预签名 S3/Azure/GCS）。
- 并发预取：固定大小的下载任务池（默认 4，对齐 `CLIENT_PREFETCH_THREADS`），结果通过有界 channel 按分片顺序交付，保证内存上限 ≈ 并发数 × 分片大小。
- 分片下载失败（网络、403 预签名过期）：重试 3 次带退避；最终失败向上抛错，而不是跳过分片。
- 对外暴露为 `Stream<Item = Result<ResultChunk>>`，`ResultChunk` 是 `Json(Vec<Vec<Option<String>>>)` 或 `ArrowIpc(Bytes)`。

### 3.5 重试与错误

| 情况 | 处理 |
| --- | --- |
| 连接失败 / 超时 / HTTP 5xx / 429 / 408 | 指数退避重试（1s 起，上限 16s，总时长上限可配置，默认 300s），同一个 `requestId` |
| HTTP 4xx（非 429/408） | 不重试，`Error::Http` |
| `success = false`，`code = 390112` | 续期后重放一次 |
| `success = false`，`code = 390114` / `390115` | 重新登录（有凭据）或 `Error::SessionExpired` |
| `success = false`，`code = 333333/333334` | 进入轮询 |
| `success = false`，其他 | `Error::Server { code, message, sql_state, query_id }`，不重试 |
| 结果解码失败 | `Error::Decode { column, row, logical_type, raw, reason }`，不重试，不降级 |

```rust
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("http error: {0}")]            Http(#[from] reqwest::Error),
    #[error("authentication failed ({code}): {message}")] Auth { code: String, message: String },
    #[error("session expired")]            SessionExpired,
    #[error("snowflake error {code} (sqlstate {sql_state:?}, query {query_id:?}): {message}")]
    Server { code: String, message: String, sql_state: Option<String>, query_id: Option<String> },
    #[error("protocol error: {0}")]        Protocol(String),
    #[error("decode error at column `{column}` row {row} ({logical_type}): {reason}; raw value: {raw:?}")]
    Decode { column: String, row: usize, logical_type: String, raw: Option<String>, reason: String },
    #[error("query cancelled")]            Cancelled,
    #[error("timeout")]                    Timeout,
}
```

### 3.6 原生 API 形态

```rust
let client = SnowflakeClient::builder()
    .account("myorg-myacct")
    .user("SVC_USER")
    .authenticator(Authenticator::password("..."))        // 或 Authenticator::key_pair_from_pem(pem, passphrase)?
    .warehouse("WH").database("DB").schema("PUBLIC").role("R")
    .build()?;

let session = client.connect().await?;

let result = session
    .query("select * from t where id > ?")
    .bind(Bind::Fixed("10".into()))
    .execute().await?;

// 协议层：原始分片流
let mut chunks = result.chunks();            // impl Stream<Item = Result<ResultChunk>>

// Arrow 层（snowflake-arrow）：
let schema  = snowflake_arrow::schema_for(&result.row_type(), &session.parameters())?;
let batches = snowflake_arrow::record_batches(result);    // impl Stream<Item = Result<RecordBatch>>

session.close().await?;
```

---

## 4. Arrow 层设计（`snowflake-arrow`）

这是“严格映射”的落点。两条输入路径，一个输出：

```
Arrow IPC bytes ──arrow-ipc StreamReader──▶ Snowflake 物理 RecordBatch ──normalize──▶ 标准 RecordBatch
JSON rowset     ──strict parse per rowtype──────────────────────────────────────────▶ 标准 RecordBatch
```

两条路径对同一个 `rowtype` 必须产出**完全相同的 Arrow Schema**，这样调用方不需要关心服务端这次返回的是哪种格式。单元测试要用同一份数据分别走两条路径并断言 `RecordBatch` 相等。

### 4.1 逻辑类型 → Arrow 目标类型（严格映射表）

| Snowflake 逻辑类型（`rowtype.type`） | 目标 Arrow 类型 | 说明 |
| --- | --- | --- |
| `fixed`（NUMBER(p, s)） | `Decimal128(p, s)` | 始终用 Decimal，保精度。不像 Go ADBC 默认转 int64/float64。调用方要 int64 可以自己 cast |
| `real`（FLOAT） | `Float64` | JSON 中 `"inf"`、`"-inf"`、`"NaN"` 要能解析 |
| `text` | `Utf8` | `length` 写入 field metadata |
| `binary` | `Binary` | JSON 中为 hex 字符串，奇数长度或非 hex 字符 → 错误 |
| `boolean` | `Boolean` | JSON 中 `"1"/"0"/"true"/"false"`（不区分大小写），其他 → 错误 |
| `date` | `Date32` | JSON 中为自 epoch 的天数（整数字符串） |
| `time` | `Time64(Nanosecond)` | 统一纳秒，无损且不会溢出 |
| `timestamp_ntz` | `Timestamp(unit, None)` | `unit` 由 `scale` 决定：0 → Second，1–3 → Millisecond，4–6 → Microsecond，7–9 → Nanosecond |
| `timestamp_ltz` | `Timestamp(unit, Some(session TIMEZONE))` | 时区取会话参数 `TIMEZONE`；值本身是 UTC 瞬时 |
| `timestamp_tz` | `Timestamp(unit, Some("UTC"))` | Snowflake 每行有独立偏移，Arrow 列只能有一个时区。默认折叠为 UTC 瞬时（与 Go ADBC 一致）；提供选项 `timestamp_tz = Struct` 输出 `Struct{ epoch: Timestamp(unit, UTC), offset_minutes: Int32 }` 保留原始偏移 |
| `variant` / `object` / `array`（半结构化） | `Utf8` + field metadata `snowflake.logical_type` | 内容是 JSON 文本。结构化类型（`OBJECT(a INT, ...)`、`ARRAY(INT)`）P2 再映射到 `Struct`/`List` |
| `geography` / `geometry` | `Utf8` + metadata | 输出格式由会话参数 `GEOGRAPHY_OUTPUT_FORMAT` 决定（默认 GeoJSON），驱动不解析 |
| `vector` | `FixedSizeList(Int32 \| Float32, n)` | P2 |
| 其他未知类型 | **错误** `Error::Decode { reason: "unsupported logical type" }` | 不退化为字符串 |

每个 Arrow `Field` 带 metadata：`snowflake.logical_type`、`snowflake.precision`、`snowflake.scale`、`snowflake.length`、`snowflake.byte_length`，以及 `nullable` 来自 `rowtype.nullable`。

### 4.2 Arrow IPC 路径：Snowflake 物理编码归一化

Snowflake 返回的 IPC stream 里，字段 metadata 含 `logicalType`、`scale`、`precision`、`charLength`、`byteLength`、`physicalType`。常见物理编码及归一化规则：

| 逻辑类型 | 服务端物理类型 | 归一化 |
| --- | --- | --- |
| `FIXED` | `Int8/16/32/64`（metadata `scale` 可能 > 0） | 原样当作未缩放整数，重新解释为 `Decimal128(p, s)`（整数值不变，仅改类型）；`Decimal128` 直接校验 `(p, s)` 一致 |
| `REAL` | `Float64` | 不变 |
| `TEXT` / `VARIANT` 等 | `Utf8` | 不变；若是 `LargeUtf8` 则 cast（须无溢出） |
| `BINARY` | `Binary` | 不变 |
| `BOOLEAN` | `Boolean` | 不变 |
| `DATE` | `Date32` | 不变 |
| `TIME` | `Int32`/`Int64` + `scale` | `value × 10^(9 − scale)` → `Time64(ns)` |
| `TIMESTAMP_NTZ/LTZ` | `Int64` + `scale`，或 `Struct{ epoch: Int64, fraction: Int32 }` | `Int64`：按 `scale` 换算到目标 `unit`；`Struct`：`epoch × 10^unit_scale + fraction / 10^(9 − unit_scale)`。换算溢出 → 错误 |
| `TIMESTAMP_TZ` | `Struct{ epoch: Int64, timezone: Int32 }`（scale = 0）或 `Struct{ epoch, fraction, timezone }` | `timezone` 字段是 `offset_minutes + 1440`。按 4.1 折叠为 UTC 或输出 Struct |

验证规则（任一不满足都报 `Error::Decode`，不猜测）：

- IPC schema 字段数、顺序、`logicalType` 必须与 `rowtype` 一致。
- 物理类型不在上表预期集合内 → 错误。
- `Struct` 子字段名/数量不符 → 错误。
- NULL 只来自 Arrow validity bitmap；`Struct` 中父级 null 子级非 null 的异常组合 → 错误。

解码器用 `arrow_ipc::reader::StreamReader` 读取，Snowflake 的 IPC 是标准 Arrow stream（含 schema message），arrow-rs 可直接读取；对老的无 continuation marker 的 legacy IPC 格式 arrow-rs 也兼容。

### 4.3 JSON 路径：严格解析

每个单元格是 `Option<String>`，按 `rowtype` 解析：

| 逻辑类型 | JSON 文本形态 | 解析 |
| --- | --- | --- |
| `fixed` | `"123.45"`、`"-7"` | 十进制字符串 → `i128`（按 `scale` 对齐，小数位多于 `scale` 或超出 `precision` → 错误） |
| `real` | `"1.5e10"`、`"inf"`、`"-inf"`、`"NaN"` | `f64::from_str` + 特殊值表 |
| `date` | `"19000"` | `i32` 天数 |
| `time` | `"45296.123456789"` | 秒 `.` 小数 → 纳秒 |
| `timestamp_ntz/ltz` | `"1700000000.123456789"` | 整数秒 + 小数（最多 9 位），再换算到目标 `unit`，溢出 → 错误 |
| `timestamp_tz` | `"1700000000.123456789 1440"` | 空格后是 `offset_minutes + 1440` |
| `boolean` | `"1"`、`"0"`、`"true"`、"false"` | 其他 → 错误 |
| `binary` | hex | 严格 hex |
| `text`、半结构化、geo | 原文 | 不变 |

实现上为每个列建一个 `ColumnDecoder` trait 对象（持有对应的 `ArrayBuilder`），逐行 `append(raw: Option<&str>) -> Result<()>`，错误带上列名、行号、原文。禁止任何 `unwrap_or_default()` / `ok()` 式的吞错。

### 4.4 批次切分与流式输出

- Arrow 模式：服务端每个分片内可能有多个 record batch，原样透传（归一化后），不合并、不重切。
- JSON 模式：首批 `rowset` 一个 batch，之后每个分片一个 batch。
- 对外统一是 `impl Stream<Item = Result<RecordBatch>>` + 一个同步的 `RecordBatchReader` 适配器（ADBC 用）。
- 零行结果：仍然产出带正确 Schema 的空 batch（ADBC 要求 `execute` 返回的 reader 有 schema）。

---

## 5. ADBC 层设计（`snowflake-adbc`）

实现 `adbc_core` 的四个 trait。Rust ADBC trait 是同步的，驱动核心是 async，桥接方式：`Database` 内部持有一个 `tokio::runtime::Runtime`（或在已有 runtime 中用 `Handle::block_on` + `block_in_place`），`Statement::execute` 返回的 `RecordBatchReader` 每次 `next()` 时 `block_on` 拉取下一个 batch。

### 5.1 选项键

兼容官方 Go ADBC Snowflake 驱动的键名，这样现有用 Go 驱动的 ADBC 调用方（Python `adbc_driver_snowflake`、DuckDB、Ibis 等的配置）能原样迁移：

| 选项 | 说明 |
| --- | --- |
| `uri` | `snowflake://user:pass@account/db/schema?warehouse=&role=`，与分字段选项二选一 |
| `adbc.snowflake.sql.account` | 账号标识 |
| `adbc.snowflake.sql.uri`（host 覆盖）| PrivateLink / 中国区 |
| `username` / `password` | 密码认证 |
| `adbc.snowflake.sql.auth_type` | `auth_snowflake`（默认）/ `auth_jwt` / `auth_pat` / `auth_oauth` |
| `adbc.snowflake.sql.client_option.jwt_private_key` | 私钥文件路径 |
| `adbc.snowflake.sql.client_option.jwt_private_key_pkcs8_value` / `_password` | 私钥内容 / passphrase |
| `adbc.snowflake.sql.client_option.auth_token` | PAT / OAuth token |
| `adbc.snowflake.sql.db` / `schema` / `warehouse` / `role` | 会话上下文 |
| `adbc.snowflake.sql.client_option.app_name` | 写入 `CLIENT_ENVIRONMENT.APPLICATION` |
| `adbc.snowflake.sql.client_option.use_high_precision` | 本驱动默认 `true`（Decimal128）；设为 `false` 时 scale=0 转 `Int64`、否则 `Float64`，仅为兼容 Go 驱动行为，溢出仍报错 |
| `adbc.snowflake.rs.timestamp_tz` | `utc`（默认）/ `struct` |
| `adbc.snowflake.sql.client_option.prefetch_concurrency` | 分片并发数 |
| `adbc.snowflake.sql.client_option.query_timeout` 等 | 超时 |

### 5.2 Trait 映射

| ADBC | 实现 |
| --- | --- |
| `Driver::new_database_with_opts` | 解析选项，构建 `SnowflakeClient`（不发请求） |
| `Database::new_connection` | `client.connect()` → `Session` |
| `Connection::new_statement` | 空 `Statement` 持有 `Arc<Session>` |
| `Connection::get_info` | `VENDOR_NAME = "Snowflake"`、`VENDOR_VERSION`（登录响应里的 `serverVersion`）、`DRIVER_NAME/VERSION`、`DRIVER_ARROW_VERSION` |
| `Connection::get_objects` | 查询 `INFORMATION_SCHEMA`（或 `SHOW TERSE ...`），按 ADBC `GetObjects` 嵌套 schema 组装（catalog → db_schema → table → column） |
| `Connection::get_table_schema` | `describeOnly = true` 执行 `SELECT * FROM <table>`，由 `rowtype` 生成 Arrow Schema（与查询结果 Schema 100% 一致） |
| `Connection::get_table_types` | 静态：`TABLE`, `VIEW`, `TEMPORARY TABLE`, `TRANSIENT TABLE`, `EXTERNAL TABLE`, `DYNAMIC TABLE`, `HYBRID TABLE` |
| `Connection::commit` / `rollback` | 执行 `COMMIT` / `ROLLBACK`；`autocommit` 选项对应 `ALTER SESSION SET AUTOCOMMIT` |
| `Connection::cancel` | 取消当前进行中的查询（abort-request） |
| `Statement::set_sql_query` / `prepare` | `prepare` 执行 `describeOnly = true` 获取结果 Schema 和 `numberOfBinds` |
| `Statement::execute` | 执行并返回 `RecordBatchReader` |
| `Statement::execute_update` | 执行，返回受影响行数（DML 结果集第一行第一列 `number of rows inserted/updated/deleted`；DDL 返回 -1） |
| `Statement::execute_schema` | `describeOnly` |
| `Statement::bind` / `bind_stream` | 转换为服务端绑定（见 3.3） |
| `Statement::get_parameter_schema` | 基于 `numberOfBinds` 产出 `Null` 类型占位 Schema（Snowflake 不返回参数类型） |
| `Statement::execute_partitions` | 不支持 → `Status::NotImplemented`（Snowflake 分片 URL 是预签名且绑定会话加密密钥，不适合跨进程分发） |
| `set_substrait_plan` | `NotImplemented` |

### 5.3 错误映射

`Error::Auth` → `Status::Unauthenticated`；`Error::Server` 按 `sql_state` 前缀映射（`42xxx` → `InvalidArgument`，`08xxx` → `IO`，其他 `Internal`），`vendor_code` 填 Snowflake 的 6 位 code，`sqlstate` 填充；`Error::Decode` → `Status::Internal`（并保留完整 message）；`Error::Timeout` → `Status::Timeout`；`Error::Cancelled` → `Status::Cancelled`。

### 5.4 FFI 导出

`snowflake-adbc-ffi` 用 `adbc_ffi::export_driver!(SnowflakeDriverInit, SnowflakeDriver)` 生成 `libadbc_driver_snowflake_rs.{so,dylib,dll}`，可被 Python `adbc_driver_manager`、Go、C++ 的 driver manager 加载。集成测试里用 Rust 的 `adbc_driver_manager` 加载该动态库，走一遍完整 ADBC 流程。

---

## 6. 实现里程碑

按可独立验证的切片排列，不估算日历时间。

### M1：密码登录 + 同步查询 + 双格式解码

- `snowflake-core`：账号/host 解析、登录（密码）、`Session`、`query-request`、长查询轮询、分片顺序下载、JSON rowset、`close`。
- `snowflake-arrow`：4.1 映射表中除 `vector`/结构化类型外的全部类型，Arrow IPC 归一化 + JSON 严格解析，两路径等价测试。
- 原生 API 可用；用真实账号跑通类型矩阵。

### M2：健壮性 + JWT

- Key-Pair JWT 认证（PKCS#8 明文 / 加密）。
- 令牌续期、重新登录、重试策略、取消、心跳。
- 并发分片预取、有界内存。
- `queryContext` 回显。

### M3：ADBC

- `snowflake-adbc` 全部 trait、选项解析、错误映射。
- `snowflake-adbc-ffi` cdylib；用 `adbc_driver_manager` 做端到端测试；用 Python `adbc_driver_manager` 手动验证可加载。

### M4：扩展

- 绑定参数（标量 → 数组绑定 → `bindStage`）、`execute_update` 行数。
- PAT / OAuth 认证。
- 多语句（`MULTI_STATEMENT_COUNT`）、真正的异步执行 API。
- `vector`、结构化 `OBJECT/ARRAY/MAP` → `Struct/List/Map`。

明确不做：PUT/GET 文件传输、外部浏览器 SSO、交互式 MFA、OCSP 校验（使用 rustls 标准证书链校验，等价于官方驱动的 `OCSP_MODE = FAIL_OPEN` 语义）。

---

## 7. 测试策略

| 层 | 方法 |
| --- | --- |
| 协议序列化 | 请求体 / 响应体的 serde 单元测试，固定 fixture 来自真实抓取并脱敏 |
| HTTP 流程 | `wiremock` 模拟服务端：登录成功/失败、`390112` 续期重放、`333334` 轮询、5xx 重试、分片下载 header 校验 |
| Arrow 归一化 | 用真实账号录制每种类型的 `rowsetBase64`（覆盖 scale 0/3/6/9、NULL、极值、负数、TZ 不同偏移）存入 `fixtures/`，断言归一化结果；同一数据的 JSON 版本走 JSON 路径，断言两路径 `RecordBatch` 相等 |
| 严格性 | 构造非法 JSON 单元格（`"abc"` 当 fixed、奇数长 hex、`"maybe"` 当 boolean、超范围 timestamp）断言返回 `Error::Decode` 且带列名/行号/原文，且绝不产出 NULL |
| 集成 | `tests/` 下 `#[ignore]` 测试，由 `SNOWFLAKE_ACCOUNT / USER / PASSWORD / PRIVATE_KEY_PATH / WAREHOUSE` 环境变量驱动；CI 中有凭据时开启。覆盖：类型矩阵 `SELECT`、`SHOW`（JSON 分支）、> 1 个分片的大结果（`TABLE(GENERATOR(ROWCOUNT => 5_000_000))`）、> 45 秒的查询（`SYSTEM$WAIT(60)`）、取消 |
| ADBC | `adbc_driver_manager` 加载 cdylib，跑 `get_info` / `get_objects` / `execute` / `execute_update` / `bind` |

---

## 8. 风险与应对

| 风险 | 应对 |
| --- | --- |
| Arrow 结果格式协商依赖伪装 `CLIENT_APP_ID`，服务端行为可能变化 | 逐查询读 `queryResultFormat`，JSON 解码路径永远可用，退化只影响性能不影响正确性 |
| Snowflake 新增物理编码（例如新的 `TIMESTAMP` 结构体形态） | 归一化对未知形态直接报错并打印 IPC schema，fixtures 回归能第一时间发现 |
| arrow-rs 与 `adbc_core` 版本错位 | workspace 中由 `adbc_core` 的 arrow 版本反推锁定，CI 加 `cargo tree -d` 检查重复 arrow |
| 密码认证被 Snowflake 彻底关闭 | 认证层已抽象为 `Authenticator` 枚举，JWT 在 M2 落地；文档明确要求服务账号设为 `TYPE = SERVICE` + Key-Pair |
| 大结果内存 | 有界预取 channel；Arrow 分片直接零拷贝进 `RecordBatch`，JSON 分片解析后即释放原文 |
| 时间类型溢出（纳秒 Timestamp 只覆盖 1677–2262） | 按 `scale` 选 `unit`，scale ≤ 6 的列不受影响；scale 7–9 超范围值报错而非回绕 |
| 中国区 / PrivateLink 域名 | `host` 可显式覆盖，不依赖账号推导 |

---

## 9. 待首次联调确认的事项

1. 结果格式参数的确切名字与传递位置（`SESSION_PARAMETERS` 还是查询 `parameters`），以及目标账号是否对 Arrow 有账号级限制。
2. 目标账号中 `LEGACY_SERVICE` 用户的实际可用性（密码登录是否已被策略阻断），决定 M1 验证时是否需要提前引入 JWT。
3. 当前服务端对 `TIMESTAMP_*` 返回 `Int64` 还是 `Struct` 编码（两者都实现，但 fixtures 需要以真实返回为准）。
