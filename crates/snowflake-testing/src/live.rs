use futures::StreamExt;
use snowflake_core::{Authenticator, Error, SnowflakeClient};

use crate::{exec_both_formats, test_config, test_session};

#[tokio::test]
async fn live_select_one() {
    let Some(session) = test_session().await else {
        return;
    };
    let result = session.query("select 1 as v").execute().await.unwrap();
    assert!(!result.query_id().is_empty());
    assert!(!result.is_pending());
    let mut rows = 0;
    let mut chunks = result.into_chunk_stream();
    while let Some(chunk) = chunks.next().await {
        chunk.unwrap();
        rows += 1;
    }
    assert!(rows >= 1);
    session.close().await.unwrap();
    let err = session.query("select 1").execute().await.unwrap_err();
    assert!(err.to_string().contains("closed"));
}

#[tokio::test]
async fn live_wrong_password() {
    let Some(config) = test_config() else {
        return;
    };
    let client = SnowflakeClient::builder()
        .account(config.account)
        .user(config.user)
        .authenticator(Authenticator::password("definitely-not-the-password"))
        .warehouse(config.warehouse)
        .database(config.database)
        .schema(config.schema)
        .role(config.role)
        .build()
        .unwrap();
    let err = client.connect().await.unwrap_err();
    match err {
        Error::Auth { code, message } => {
            assert!(
                code == "390100" || code == "390102" || code == "390127",
                "{code}: {message}"
            );
        }
        other => panic!("expected authentication failure, got {other}"),
    }
}

#[tokio::test]
async fn live_renew_after_invalid_token() {
    let Some(session) = test_session().await else {
        return;
    };
    session.invalidate_session_token();
    let result = session.query("select 1").execute().await.unwrap();
    assert!(!result.query_id().is_empty());
    session.close().await.unwrap();
}

#[tokio::test]
async fn live_both_formats_agree_on_number() {
    let Some(session) = test_session().await else {
        return;
    };
    let (arrow, json) = exec_both_formats(
        &session,
        "select 123.456::number(10,3) as v, null::number(10,3)",
    )
    .await
    .unwrap();
    assert_eq!(arrow, json);
    assert_eq!(
        arrow[0].schema().field(0).data_type(),
        &arrow_schema::DataType::Decimal128(10, 3)
    );
    session.close().await.unwrap();
}
