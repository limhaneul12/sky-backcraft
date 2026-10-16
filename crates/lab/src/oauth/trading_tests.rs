use super::*;

const PUBLIC_URL: &str = "https://skybackcraft.store";
const REDIRECT_URI: &str = "http://127.0.0.1/callback";
const OWNER_CODE: &str = "owner-approval-code-123456";
const PKCE_VERIFIER: &str = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
const PKCE_CHALLENGE: &str = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM";
const CHATGPT_CLIENT_ID: &str = "8F0StuL0EU12ERL2cVoTZW0ZrvGYB0MJ";
const CHATGPT_CALLBACK: &str = "https://chatgpt.com/connector_platform_oauth_redirect";

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn test_error(message: impl Into<String>) -> std::io::Error {
    std::io::Error::other(message.into())
}

struct OAuthTempRoot(std::path::PathBuf);

impl OAuthTempRoot {
    fn new() -> TestResult<Self> {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "sky-backcraft-oauth-control-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path)?;
        Ok(Self(path))
    }
}

impl Drop for OAuthTempRoot {
    fn drop(&mut self) {
        let _ignored = std::fs::remove_dir_all(&self.0);
    }
}

fn enabled_state() -> TestResult<OAuthState> {
    OAuthState::new_with_trading_scopes(PUBLIC_URL, OWNER_CODE.into())
        .map_err(|error| test_error(error).into())
}

fn full_trading_state() -> TestResult<OAuthState> {
    OAuthState::new_with_full_trading_on_consent(PUBLIC_URL, OWNER_CODE.into())
        .map_err(|error| test_error(error).into())
}

fn chatgpt_client_registry(issuer: &str) -> OAuthClientRegistryConfig {
    OAuthClientRegistryConfig {
        issuer: issuer.into(),
        clients: vec![OAuthClientConfig {
            client_id: CHATGPT_CLIENT_ID.into(),
            redirect_uris: vec![CHATGPT_CALLBACK.into()],
            client_name: "ChatGPT".into(),
        }],
    }
}

fn add_test_client(state: &OAuthState, client_id: &str) {
    let mut grants = state
        .inner
        .grants
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    grants.clients.insert(
        client_id.into(),
        Client {
            redirect_uris: vec![REDIRECT_URI.into()],
            display_name: "OAuth scope test".into(),
            registered_at: std::time::Instant::now(),
            pinned: false,
        },
    );
}

fn encoded_scope(permissions: OAuthPermissions) -> &'static str {
    match permissions {
        OAuthPermissions::Research => "mcp",
        OAuthPermissions::TradingRead => "mcp%20trading%3Aread",
        OAuthPermissions::TradingControl => "mcp%20trading%3Aread%20trading%3Acontrol",
    }
}

fn authorization_query(client_id: &str, state: &str, encoded_scope: &str) -> String {
    authorization_query_with_redirect(client_id, REDIRECT_URI, state, encoded_scope)
}

fn authorization_query_with_redirect(
    client_id: &str,
    redirect_uri: &str,
    state: &str,
    encoded_scope: &str,
) -> String {
    format!(
        "client_id={}&redirect_uri={}&state={}&response_type=code&resource=https%3A%2F%2Fskybackcraft.store%2Fmcp&code_challenge={PKCE_CHALLENGE}&code_challenge_method=S256&scope={encoded_scope}",
        percent_encode(client_id),
        percent_encode(redirect_uri),
        percent_encode(state),
    )
}

fn authorization_query_without_scope(client_id: &str, redirect_uri: &str, state: &str) -> String {
    format!(
        "client_id={}&redirect_uri={}&state={}&response_type=code&resource=https%3A%2F%2Fskybackcraft.store%2Fmcp&code_challenge={PKCE_CHALLENGE}&code_challenge_method=S256",
        percent_encode(client_id),
        percent_encode(redirect_uri),
        percent_encode(state),
    )
}

async fn response_text(response: axum::response::Response) -> TestResult<String> {
    let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024).await?;
    Ok(String::from_utf8(bytes.to_vec())?)
}

async fn response_json(response: axum::response::Response) -> TestResult<serde_json::Value> {
    let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

fn authorization_code_form(code: &str, client_id: &str) -> String {
    authorization_code_form_with_redirect(code, client_id, REDIRECT_URI)
}

fn authorization_code_form_with_redirect(
    code: &str,
    client_id: &str,
    redirect_uri: &str,
) -> String {
    authorization_code_form_with_verifier(code, client_id, redirect_uri, PKCE_VERIFIER)
}

fn authorization_code_form_with_verifier(
    code: &str,
    client_id: &str,
    redirect_uri: &str,
    verifier: &str,
) -> String {
    format!(
        "grant_type=authorization_code&code={}&client_id={}&redirect_uri={}&code_verifier={}&resource=https%3A%2F%2Fskybackcraft.store%2Fmcp",
        percent_encode(code),
        percent_encode(client_id),
        percent_encode(redirect_uri),
        percent_encode(verifier),
    )
}

fn refresh_form(refresh_token: &str, client_id: &str, encoded_scope: Option<&str>) -> String {
    let scope = encoded_scope.map_or_else(String::new, |scope| format!("&scope={scope}"));
    format!(
        "grant_type=refresh_token&refresh_token={refresh_token}&client_id={client_id}&resource=https%3A%2F%2Fskybackcraft.store%2Fmcp{scope}"
    )
}

fn bearer_headers(token: &str) -> TestResult<axum::http::HeaderMap> {
    let mut headers = axum::http::HeaderMap::new();
    let authorization = format!("Bearer {token}").parse::<axum::http::HeaderValue>()?;
    headers.insert(axum::http::header::AUTHORIZATION, authorization);
    Ok(headers)
}

fn response_string<'a>(body: &'a serde_json::Value, name: &str) -> TestResult<&'a str> {
    let value = body
        .get(name)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| test_error(format!("OAuth JSON response has string field {name}")))?;
    Ok(value)
}

async fn automation_session_count(database: &crate::database::DatabaseHandle) -> TestResult<u64> {
    Ok(database
        .call("oauth_managed_consent_session_count", |store| {
            store
                .automation_list_sessions_with_policy(
                    0,
                    1,
                    crate::contracts::UtcTimestamp::now(),
                    crate::upbit::execution::LiveExecutionPolicy::Disabled,
                )
                .map(|page| page.total_count)
        })
        .await?)
}

async fn approve_authorization(
    state: &OAuthState,
    client_id: &str,
    state_param: &str,
    permissions: OAuthPermissions,
) -> TestResult<String> {
    approve_authorization_with_redirect(state, client_id, REDIRECT_URI, state_param, permissions)
        .await
}

async fn approve_authorization_with_redirect(
    state: &OAuthState,
    client_id: &str,
    redirect_uri: &str,
    state_param: &str,
    permissions: OAuthPermissions,
) -> TestResult<String> {
    let consent = authorize(
        axum::extract::State(state.clone()),
        axum::extract::RawQuery(Some(authorization_query_with_redirect(
            client_id,
            redirect_uri,
            state_param,
            encoded_scope(permissions),
        ))),
    )
    .await;
    assert_eq!(consent.status(), axum::http::StatusCode::OK);

    let (flow_id, csrf) = {
        let grants = state
            .inner
            .grants
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        grants
            .pending
            .iter()
            .find(|(_, pending)| pending.state == state_param)
            .map(|(flow_id, pending)| (flow_id.clone(), pending.csrf.clone()))
            .ok_or_else(|| test_error(format!("authorization created {state_param}")))?
    };
    let approval = approve(
        axum::extract::State(state.clone()),
        RawForm(axum::body::Bytes::from(format!(
            "flow_id={flow_id}&csrf={csrf}&owner_code={OWNER_CODE}"
        ))),
    )
    .await;
    assert!(approval.status().is_redirection());
    let location = approval
        .headers()
        .get(axum::http::header::LOCATION)
        .ok_or_else(|| test_error("owner approval redirects to the registered callback"))?
        .to_str()?;
    let (_, redirect_query) = location
        .split_once('?')
        .ok_or_else(|| test_error("approval redirect includes the authorization code"))?;
    let mut redirect_params =
        parse_form(redirect_query).map_err(|()| test_error("approval redirect query is valid"))?;
    let code = redirect_params
        .remove("code")
        .ok_or_else(|| test_error("approval redirect contains the one-use code"))?;
    Ok(code)
}

#[test]
fn scope_parsing_requires_research_and_read_before_control() {
    use OAuthPermissions::{Research, TradingControl, TradingRead};

    assert_eq!(OAuthPermissions::parse(None, false), Some(Research));
    assert_eq!(OAuthPermissions::parse(None, true), Some(Research));
    assert_eq!(OAuthPermissions::parse(Some("mcp"), true), Some(Research));
    assert_eq!(
        OAuthPermissions::parse(Some("mcp trading:read"), true),
        Some(TradingRead)
    );
    assert_eq!(
        OAuthPermissions::parse(Some("mcp trading:read trading:control"), true),
        Some(TradingControl)
    );

    assert_eq!(OAuthPermissions::parse(Some("trading:read"), true), None);
    assert_eq!(
        OAuthPermissions::parse(Some("trading:read trading:control"), true),
        None
    );
    assert_eq!(
        OAuthPermissions::parse(Some("mcp trading:control"), true),
        None
    );
    assert_eq!(
        OAuthPermissions::parse(Some("mcp future:scope"), true),
        None
    );
    assert_eq!(
        OAuthPermissions::parse(Some("mcp trading:read"), false),
        None
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one restart journey covers client restore, owner consent, PKCE, and ephemeral grants"
)]
async fn operator_registered_client_survives_restart_without_persisting_grants() -> TestResult {
    let configuration = chatgpt_client_registry(PUBLIC_URL);
    let before_restart = enabled_state()?
        .with_pre_registered_clients(configuration.clone())
        .map_err(test_error)?;
    drop(before_restart);

    let restarted = enabled_state()?
        .with_pre_registered_clients(configuration.clone())
        .map_err(test_error)?;
    let known_client = authorize(
        axum::extract::State(restarted.clone()),
        axum::extract::RawQuery(Some(authorization_query_without_scope(
            CHATGPT_CLIENT_ID,
            CHATGPT_CALLBACK,
            "known-client-default-scope",
        ))),
    )
    .await;
    assert_eq!(known_client.status(), axum::http::StatusCode::OK);
    let consent = response_text(known_client).await?;
    assert!(consent.contains("ChatGPT"));
    assert!(consent.contains(CHATGPT_CALLBACK));
    {
        let grants = restarted
            .inner
            .grants
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let client = grants
            .clients
            .get(CHATGPT_CLIENT_ID)
            .ok_or_else(|| test_error("configured client survived restart"))?;
        assert!(client.pinned);
        let pending = grants
            .pending
            .values()
            .find(|pending| pending.state == "known-client-default-scope")
            .ok_or_else(|| test_error("known client created an owner-consent flow"))?;
        assert_eq!(pending.permissions, OAuthPermissions::Research);
        assert!(grants.codes.is_empty());
        assert!(grants.tokens.is_empty());
        assert!(grants.refresh_tokens.is_empty());
    }

    let unknown = authorize(
        axum::extract::State(restarted.clone()),
        axum::extract::RawQuery(Some(authorization_query_with_redirect(
            "unknown-chat-client",
            CHATGPT_CALLBACK,
            "unknown-client",
            encoded_scope(OAuthPermissions::Research),
        ))),
    )
    .await;
    assert_eq!(unknown.status(), axum::http::StatusCode::BAD_REQUEST);
    assert_eq!(
        response_string(&response_json(unknown).await?, "error")?,
        "unauthorized_client"
    );

    let changed_callback = authorize(
        axum::extract::State(restarted.clone()),
        axum::extract::RawQuery(Some(authorization_query_with_redirect(
            CHATGPT_CLIENT_ID,
            "https://chatgpt.com/changed-callback",
            "changed-callback",
            encoded_scope(OAuthPermissions::Research),
        ))),
    )
    .await;
    assert_eq!(
        changed_callback.status(),
        axum::http::StatusCode::BAD_REQUEST
    );
    assert_eq!(
        response_string(&response_json(changed_callback).await?, "error")?,
        "invalid_request"
    );

    let code = approve_authorization_with_redirect(
        &restarted,
        CHATGPT_CLIENT_ID,
        CHATGPT_CALLBACK,
        "owner-approved-default-research",
        OAuthPermissions::Research,
    )
    .await?;
    let issued = token(
        axum::extract::State(restarted.clone()),
        RawForm(axum::body::Bytes::from(
            authorization_code_form_with_redirect(&code, CHATGPT_CLIENT_ID, CHATGPT_CALLBACK),
        )),
    )
    .await;
    assert_eq!(issued.status(), axum::http::StatusCode::OK);
    let issued = response_json(issued).await?;
    assert_eq!(response_string(&issued, "scope")?, "mcp");
    let headers = bearer_headers(response_string(&issued, "access_token")?)?;
    assert_eq!(
        restarted.permissions_for_headers(&headers),
        Some(OAuthPermissions::Research)
    );

    let next_restart = enabled_state()?
        .with_pre_registered_clients(configuration)
        .map_err(test_error)?;
    assert_eq!(next_restart.permissions_for_headers(&headers), None);
    Ok(())
}

#[test]
fn operator_client_registry_rejects_malformed_documents_and_issuer_mismatches() -> TestResult {
    assert!(serde_json::from_str::<OAuthClientRegistryConfig>("{").is_err());
    assert!(
        serde_json::from_value::<OAuthClientRegistryConfig>(serde_json::json!({
            "issuer": PUBLIC_URL,
            "clients": [],
            "unexpected": true
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<OAuthClientRegistryConfig>(serde_json::json!({
            "issuer": PUBLIC_URL,
            "clients": [{
                "client_id": CHATGPT_CLIENT_ID,
                "redirect_uris": [CHATGPT_CALLBACK],
                "client_name": "ChatGPT",
                "unexpected": true
            }]
        }))
        .is_err()
    );

    assert!(
        enabled_state()?
            .with_pre_registered_clients(chatgpt_client_registry("https://other.example"))
            .is_err()
    );
    assert!(
        enabled_state()?
            .with_pre_registered_clients(chatgpt_client_registry("https://skybackcraft.store/"))
            .is_err(),
        "issuer comparison must use the exact validated public URL"
    );
    Ok(())
}

#[test]
fn operator_client_registry_rejects_invalid_and_colliding_clients() -> TestResult {
    let mut duplicate_ids = chatgpt_client_registry(PUBLIC_URL);
    duplicate_ids.clients.push(duplicate_ids.clients[0].clone());
    assert!(
        enabled_state()?
            .with_pre_registered_clients(duplicate_ids)
            .is_err()
    );

    let mut invalid_redirect = chatgpt_client_registry(PUBLIC_URL);
    invalid_redirect.clients[0].redirect_uris = vec!["http://example.com/callback".into()];
    assert!(
        enabled_state()?
            .with_pre_registered_clients(invalid_redirect)
            .is_err()
    );

    let mut too_many_clients = chatgpt_client_registry(PUBLIC_URL);
    too_many_clients.clients = (0..=MAX_CLIENTS)
        .map(|index| OAuthClientConfig {
            client_id: format!("configured-{index}"),
            redirect_uris: vec![CHATGPT_CALLBACK.into()],
            client_name: "ChatGPT".into(),
        })
        .collect();
    assert!(
        enabled_state()?
            .with_pre_registered_clients(too_many_clients)
            .is_err()
    );

    let mut too_many_redirects = chatgpt_client_registry(PUBLIC_URL);
    too_many_redirects.clients[0].redirect_uris = vec![CHATGPT_CALLBACK.into(); 9];
    assert!(
        enabled_state()?
            .with_pre_registered_clients(too_many_redirects)
            .is_err()
    );

    let existing = enabled_state()?;
    add_test_client(&existing, CHATGPT_CLIENT_ID);
    assert!(
        existing
            .with_pre_registered_clients(chatgpt_client_registry(PUBLIC_URL))
            .is_err()
    );

    let now = std::time::Instant::now();
    let mut grants = Grants::default();
    assert!(
        grants
            .insert_client_bounded(
                "duplicate-id".into(),
                Client {
                    redirect_uris: vec![REDIRECT_URI.into()],
                    display_name: "Original client".into(),
                    registered_at: now,
                    pinned: false,
                },
            )
            .is_ok()
    );
    assert!(
        grants
            .insert_client_bounded(
                "duplicate-id".into(),
                Client {
                    redirect_uris: vec![CHATGPT_CALLBACK.into()],
                    display_name: "Conflicting client".into(),
                    registered_at: now,
                    pinned: false,
                },
            )
            .is_err()
    );
    assert_eq!(
        grants.clients["duplicate-id"].display_name,
        "Original client"
    );
    Ok(())
}

#[test]
fn operator_registered_clients_are_pinned_under_dynamic_registration_pressure() -> TestResult {
    let state = enabled_state()?
        .with_pre_registered_clients(chatgpt_client_registry(PUBLIC_URL))
        .map_err(test_error)?;
    let now = std::time::Instant::now();
    let mut grants = state
        .inner
        .grants
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for index in 0..(MAX_CLIENTS - 1) {
        let registered = grants.insert_client_bounded(
            format!("dynamic-{index}"),
            Client {
                redirect_uris: vec![REDIRECT_URI.into()],
                display_name: format!("Dynamic {index}"),
                registered_at: now + Duration::from_secs(u64::try_from(index).unwrap_or(u64::MAX)),
                pinned: false,
            },
        );
        assert!(registered.is_ok());
    }
    assert_eq!(grants.clients.len(), MAX_CLIENTS);
    assert!(
        grants
            .insert_client_bounded(
                "dynamic-replacement".into(),
                Client {
                    redirect_uris: vec![REDIRECT_URI.into()],
                    display_name: "Dynamic replacement".into(),
                    registered_at: now + Duration::from_hours(2),
                    pinned: false,
                },
            )
            .is_ok()
    );
    assert!(grants.clients.contains_key(CHATGPT_CLIENT_ID));
    assert!(!grants.clients.contains_key("dynamic-0"));
    assert!(grants.clients.contains_key("dynamic-1"));
    assert!(grants.clients.contains_key("dynamic-replacement"));
    assert_eq!(grants.clients.len(), MAX_CLIENTS);
    Ok(())
}

#[tokio::test]
async fn production_full_consent_discloses_all_capabilities_for_supported_requests() -> TestResult {
    let state = full_trading_state()?;
    add_test_client(&state, "full-consent-client");
    for (state_param, requested_scope) in [
        ("full-consent-omitted", None),
        (
            "full-consent-research",
            Some(encoded_scope(OAuthPermissions::Research)),
        ),
        (
            "full-consent-read",
            Some(encoded_scope(OAuthPermissions::TradingRead)),
        ),
    ] {
        let query = requested_scope.map_or_else(
            || authorization_query_without_scope("full-consent-client", REDIRECT_URI, state_param),
            |scope| {
                authorization_query_with_redirect(
                    "full-consent-client",
                    REDIRECT_URI,
                    state_param,
                    scope,
                )
            },
        );
        let response = authorize(
            axum::extract::State(state.clone()),
            axum::extract::RawQuery(Some(query)),
        )
        .await;
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let consent = response_text(response).await?;
        assert!(consent.contains(OAuthPermissions::TradingControl.scope()));
        assert!(
            consent.contains(
                OAuthConsentPolicy::FullTradingOnOwnerApproval
                    .consent_description(OAuthPermissions::TradingControl, false)
            )
        );
        assert!(consent.contains("관리형 실주문 전송 제어가 없어"));
        assert!(!consent.contains("전송 스위치가 ON됩니다"));
        assert!(
            consent.contains(
                OAuthConsentPolicy::FullTradingOnOwnerApproval
                    .consent_button_label(OAuthPermissions::TradingControl, false)
            )
        );

        let grants = state
            .inner
            .grants
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let pending = grants
            .pending
            .values()
            .find(|pending| pending.state == state_param)
            .ok_or_else(|| test_error("full-scope consent waits for owner approval"))?;
        assert_eq!(pending.permissions, OAuthPermissions::TradingControl);
        assert!(grants.codes.is_empty());
        assert!(grants.tokens.is_empty());
        assert!(grants.refresh_tokens.is_empty());
    }
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one owner-consent test covers denial, PKCE, token scope and refresh continuity"
)]
async fn production_full_consent_requires_owner_and_pkce_and_refreshes_full_scope() -> TestResult {
    let state = full_trading_state()?;
    add_test_client(&state, "full-consent-client");
    let wrong_owner_query = authorization_query_with_redirect(
        "full-consent-client",
        REDIRECT_URI,
        "full-consent-wrong-owner",
        encoded_scope(OAuthPermissions::Research),
    );
    let consent = authorize(
        axum::extract::State(state.clone()),
        axum::extract::RawQuery(Some(wrong_owner_query)),
    )
    .await;
    assert_eq!(consent.status(), axum::http::StatusCode::OK);
    let (flow_id, csrf) = {
        let grants = state
            .inner
            .grants
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        grants
            .pending
            .iter()
            .find(|(_, pending)| pending.state == "full-consent-wrong-owner")
            .map(|(flow_id, pending)| (flow_id.clone(), pending.csrf.clone()))
            .ok_or_else(|| test_error("owner approval flow exists"))?
    };
    let wrong_owner = approve(
        axum::extract::State(state.clone()),
        RawForm(axum::body::Bytes::from(format!(
            "flow_id={flow_id}&csrf={csrf}&owner_code=wrong-owner-code"
        ))),
    )
    .await;
    assert_eq!(wrong_owner.status(), axum::http::StatusCode::UNAUTHORIZED);
    {
        let grants = state
            .inner
            .grants
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(grants.codes.is_empty());
        assert!(grants.tokens.is_empty());
        assert!(grants.refresh_tokens.is_empty());
    }

    let bad_pkce_code = approve_authorization_with_redirect(
        &state,
        "full-consent-client",
        REDIRECT_URI,
        "full-consent-bad-pkce",
        OAuthPermissions::TradingRead,
    )
    .await?;
    {
        let grants = state
            .inner
            .grants
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let code = grants
            .codes
            .get(&bad_pkce_code)
            .ok_or_else(|| test_error("owner approval creates the authorization code"))?;
        assert_eq!(code.permissions, OAuthPermissions::TradingControl);
    }
    let wrong_verifier = format!("{}A", &PKCE_VERIFIER[..PKCE_VERIFIER.len() - 1]);
    let denied_pkce = token(
        axum::extract::State(state.clone()),
        RawForm(axum::body::Bytes::from(
            authorization_code_form_with_verifier(
                &bad_pkce_code,
                "full-consent-client",
                REDIRECT_URI,
                &wrong_verifier,
            ),
        )),
    )
    .await;
    assert_eq!(denied_pkce.status(), axum::http::StatusCode::BAD_REQUEST);
    assert_eq!(
        response_string(&response_json(denied_pkce).await?, "error")?,
        "invalid_grant"
    );
    {
        let grants = state
            .inner
            .grants
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(grants.codes.is_empty());
        assert!(grants.tokens.is_empty());
        assert!(grants.refresh_tokens.is_empty());
    }

    let code = approve_authorization_with_redirect(
        &state,
        "full-consent-client",
        REDIRECT_URI,
        "full-consent-owner-approved",
        OAuthPermissions::TradingRead,
    )
    .await?;
    let issued = token(
        axum::extract::State(state.clone()),
        RawForm(axum::body::Bytes::from(
            authorization_code_form_with_redirect(&code, "full-consent-client", REDIRECT_URI),
        )),
    )
    .await;
    assert_eq!(issued.status(), axum::http::StatusCode::OK);
    let issued = response_json(issued).await?;
    assert_eq!(
        response_string(&issued, "scope")?,
        OAuthPermissions::TradingControl.scope()
    );
    let access_token = response_string(&issued, "access_token")?.to_owned();
    let refresh_token = response_string(&issued, "refresh_token")?;
    let headers = bearer_headers(&access_token)?;
    assert_eq!(
        state.permissions_for_headers(&headers),
        Some(OAuthPermissions::TradingControl)
    );

    let refreshed = token(
        axum::extract::State(state.clone()),
        RawForm(axum::body::Bytes::from(refresh_form(
            refresh_token,
            "full-consent-client",
            None,
        ))),
    )
    .await;
    assert_eq!(refreshed.status(), axum::http::StatusCode::OK);
    let refreshed = response_json(refreshed).await?;
    assert_eq!(
        response_string(&refreshed, "scope")?,
        OAuthPermissions::TradingControl.scope()
    );
    let refreshed_headers = bearer_headers(response_string(&refreshed, "access_token")?)?;
    assert_eq!(
        state.permissions_for_headers(&refreshed_headers),
        Some(OAuthPermissions::TradingControl)
    );
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "mock integration verifies gate audit, failure paths, refresh behavior and no session effects"
)]
async fn managed_oauth_consent_opens_shared_mock_gate_without_orders_or_sessions() -> TestResult {
    use crate::database::DatabaseOwner;
    use crate::upbit::execution::LiveExecutionPolicy;
    use crate::upbit::live_order_control::{
        LiveOrderControlAction, LiveOrderControlOutcome, LiveOrderControlRequest,
        LiveOrderControlService, LiveOrderSendGate,
    };

    let root = OAuthTempRoot::new()?;
    let owner = DatabaseOwner::open_with_live_order_control(root.0.clone(), true)?;
    let database = owner.handle();
    let gate = LiveOrderSendGate::new_disabled();
    let exchange = crate::upbit::UpbitExchangeService::new_mock_with_policy_and_send_gate(
        LiveExecutionPolicy::OperatorEnabled,
        gate.clone(),
    );
    let live_order_control = LiveOrderControlService::new(
        database.clone(),
        exchange.live_execution_policy(),
        exchange.live_order_send_gate(),
        exchange.exchange_read_mode(),
    );
    let state = full_trading_state()?;
    state
        .bind_managed_live_order_control(live_order_control.clone())
        .map_err(test_error)?;
    add_test_client(&state, "managed-consent-client");
    assert!(!gate.is_enabled());

    let managed_consent = authorize(
        axum::extract::State(state.clone()),
        axum::extract::RawQuery(Some(authorization_query(
            "managed-consent-client",
            "managed-consent-disclosure",
            encoded_scope(OAuthPermissions::Research),
        ))),
    )
    .await;
    assert_eq!(managed_consent.status(), axum::http::StatusCode::OK);
    let managed_consent = response_text(managed_consent).await?;
    assert!(managed_consent.contains("공유 실주문 전송 스위치가 ON됩니다"));
    assert!(
        managed_consent.contains("이미 실행 중인 전략이 있으면 실제 주문이 전송될 수 있습니다")
    );
    assert!(managed_consent.contains("전략을 시작하거나 재개하지 않습니다"));

    let unknown = authorize(
        axum::extract::State(state.clone()),
        axum::extract::RawQuery(Some(authorization_query(
            "unknown-managed-client",
            "unknown-before-enable",
            encoded_scope(OAuthPermissions::Research),
        ))),
    )
    .await;
    assert_eq!(unknown.status(), axum::http::StatusCode::BAD_REQUEST);
    assert!(!gate.is_enabled());

    let wrong_owner_consent = authorize(
        axum::extract::State(state.clone()),
        axum::extract::RawQuery(Some(authorization_query(
            "managed-consent-client",
            "wrong-owner-before-enable",
            encoded_scope(OAuthPermissions::Research),
        ))),
    )
    .await;
    assert_eq!(wrong_owner_consent.status(), axum::http::StatusCode::OK);
    let (wrong_flow, wrong_csrf) = {
        let grants = state
            .inner
            .grants
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        grants
            .pending
            .iter()
            .find(|(_, pending)| pending.state == "wrong-owner-before-enable")
            .map(|(flow, pending)| (flow.clone(), pending.csrf.clone()))
            .ok_or_else(|| test_error("wrong-owner consent is pending"))?
    };
    let wrong_owner = approve(
        axum::extract::State(state.clone()),
        RawForm(axum::body::Bytes::from(format!(
            "flow_id={wrong_flow}&csrf={wrong_csrf}&owner_code=wrong-owner-code"
        ))),
    )
    .await;
    assert_eq!(wrong_owner.status(), axum::http::StatusCode::UNAUTHORIZED);
    assert!(!gate.is_enabled());

    let bad_pkce_code = approve_authorization_with_redirect(
        &state,
        "managed-consent-client",
        REDIRECT_URI,
        "bad-pkce-before-enable",
        OAuthPermissions::Research,
    )
    .await?;
    let wrong_verifier = format!("{}A", &PKCE_VERIFIER[..PKCE_VERIFIER.len() - 1]);
    let bad_pkce = token(
        axum::extract::State(state.clone()),
        RawForm(axum::body::Bytes::from(
            authorization_code_form_with_verifier(
                &bad_pkce_code,
                "managed-consent-client",
                REDIRECT_URI,
                &wrong_verifier,
            ),
        )),
    )
    .await;
    assert_eq!(bad_pkce.status(), axum::http::StatusCode::BAD_REQUEST);
    assert!(!gate.is_enabled());
    assert_eq!(automation_session_count(&database).await?, 0);

    let code = approve_authorization_with_redirect(
        &state,
        "managed-consent-client",
        REDIRECT_URI,
        "managed-owner-approved",
        OAuthPermissions::Research,
    )
    .await?;
    let request_id =
        consent_live_order_request_id("managed-consent-client", &code, &state.inner.resource);
    let issued = token(
        axum::extract::State(state.clone()),
        RawForm(axum::body::Bytes::from(
            authorization_code_form_with_redirect(&code, "managed-consent-client", REDIRECT_URI),
        )),
    )
    .await;
    assert_eq!(issued.status(), axum::http::StatusCode::OK);
    let issued = response_json(issued).await?;
    assert_eq!(
        response_string(&issued, "scope")?,
        OAuthPermissions::TradingControl.scope()
    );
    assert!(gate.is_enabled());
    assert!(exchange.live_trading_enabled());

    let enable_receipt = live_order_control
        .execute(LiveOrderControlRequest {
            action: LiveOrderControlAction::Request,
            request_id: Some(request_id.clone()),
            expected_sequence: None,
            expected_epoch: None,
            confirmation: None,
        })
        .await?
        .receipt
        .ok_or_else(|| test_error("OAuth consent writes a durable live-order receipt"))?;
    assert_eq!(enable_receipt.request_id, request_id);
    assert_eq!(enable_receipt.action, LiveOrderControlAction::Enable);
    assert_eq!(
        enable_receipt.outcome,
        LiveOrderControlOutcome::Acknowledged
    );
    assert_eq!(automation_session_count(&database).await?, 0);

    let sequence_before_noop = live_order_control
        .execute(LiveOrderControlRequest {
            action: LiveOrderControlAction::Status,
            request_id: None,
            expected_sequence: None,
            expected_epoch: None,
            confirmation: None,
        })
        .await?
        .sequence;
    let already_on_code = approve_authorization_with_redirect(
        &state,
        "managed-consent-client",
        REDIRECT_URI,
        "managed-consent-already-on",
        OAuthPermissions::Research,
    )
    .await?;
    let already_on_request_id = consent_live_order_request_id(
        "managed-consent-client",
        &already_on_code,
        &state.inner.resource,
    );
    let already_on_token = token(
        axum::extract::State(state.clone()),
        RawForm(axum::body::Bytes::from(
            authorization_code_form_with_redirect(
                &already_on_code,
                "managed-consent-client",
                REDIRECT_URI,
            ),
        )),
    )
    .await;
    assert_eq!(already_on_token.status(), axum::http::StatusCode::OK);
    assert!(gate.is_enabled());
    let sequence_after_noop = live_order_control
        .execute(LiveOrderControlRequest {
            action: LiveOrderControlAction::Status,
            request_id: None,
            expected_sequence: None,
            expected_epoch: None,
            confirmation: None,
        })
        .await?
        .sequence;
    assert_eq!(sequence_after_noop, sequence_before_noop);
    assert!(
        live_order_control
            .execute(LiveOrderControlRequest {
                action: LiveOrderControlAction::Request,
                request_id: Some(already_on_request_id),
                expected_sequence: None,
                expected_epoch: None,
                confirmation: None,
            })
            .await
            .is_err(),
        "already-on OAuth consent must not create another enable receipt"
    );

    let status = live_order_control
        .execute(LiveOrderControlRequest {
            action: LiveOrderControlAction::Status,
            request_id: None,
            expected_sequence: None,
            expected_epoch: None,
            confirmation: None,
        })
        .await?;
    let disabled = live_order_control
        .execute(LiveOrderControlRequest {
            action: LiveOrderControlAction::Disable,
            request_id: Some("operator-disabled-after-oauth".into()),
            expected_sequence: Some(status.sequence),
            expected_epoch: Some(status.boot_epoch),
            confirmation: None,
        })
        .await?;
    assert_eq!(
        disabled.receipt.as_ref().map(|receipt| receipt.outcome),
        Some(LiveOrderControlOutcome::Acknowledged)
    );
    assert!(!gate.is_enabled());
    assert!(!exchange.live_trading_enabled());

    let refresh_token = response_string(&issued, "refresh_token")?;
    let refreshed = token(
        axum::extract::State(state.clone()),
        RawForm(axum::body::Bytes::from(refresh_form(
            refresh_token,
            "managed-consent-client",
            None,
        ))),
    )
    .await;
    assert_eq!(refreshed.status(), axum::http::StatusCode::OK);
    let refreshed = response_json(refreshed).await?;
    assert_eq!(
        response_string(&refreshed, "scope")?,
        OAuthPermissions::TradingControl.scope()
    );
    assert!(
        !gate.is_enabled(),
        "refresh must not reopen an operator-disabled gate"
    );

    let fresh_code = approve_authorization_with_redirect(
        &state,
        "managed-consent-client",
        REDIRECT_URI,
        "fresh-consent-after-disable",
        OAuthPermissions::TradingRead,
    )
    .await?;
    let fresh_request_id =
        consent_live_order_request_id("managed-consent-client", &fresh_code, &state.inner.resource);
    assert_ne!(fresh_request_id, request_id);
    let fresh_issued = token(
        axum::extract::State(state.clone()),
        RawForm(axum::body::Bytes::from(
            authorization_code_form_with_redirect(
                &fresh_code,
                "managed-consent-client",
                REDIRECT_URI,
            ),
        )),
    )
    .await;
    assert_eq!(fresh_issued.status(), axum::http::StatusCode::OK);
    assert!(gate.is_enabled());
    assert!(exchange.live_trading_enabled());
    assert_eq!(automation_session_count(&database).await?, 0);
    owner.shutdown()?;

    let restarted_owner = DatabaseOwner::open_with_live_order_control(root.0.clone(), true)?;
    let restarted_database = restarted_owner.handle();
    let restarted_gate = LiveOrderSendGate::new_disabled();
    let restarted_exchange = crate::upbit::UpbitExchangeService::new_mock_with_policy_and_send_gate(
        LiveExecutionPolicy::OperatorEnabled,
        restarted_gate.clone(),
    );
    let restarted_control = LiveOrderControlService::new(
        restarted_database.clone(),
        restarted_exchange.live_execution_policy(),
        restarted_exchange.live_order_send_gate(),
        restarted_exchange.exchange_read_mode(),
    );
    let status = restarted_control
        .execute(LiveOrderControlRequest {
            action: LiveOrderControlAction::Status,
            request_id: None,
            expected_sequence: None,
            expected_epoch: None,
            confirmation: None,
        })
        .await?;
    assert!(!status.live_trading_enabled);
    assert!(!restarted_gate.is_enabled());

    let restarted_state = full_trading_state()?;
    restarted_state
        .bind_managed_live_order_control(restarted_control)
        .map_err(test_error)?;
    add_test_client(&restarted_state, "managed-consent-client");
    let restarted_code = approve_authorization(
        &restarted_state,
        "managed-consent-client",
        "fresh-consent-after-restart",
        OAuthPermissions::Research,
    )
    .await?;
    let restarted_token = token(
        axum::extract::State(restarted_state),
        RawForm(axum::body::Bytes::from(
            authorization_code_form_with_redirect(
                &restarted_code,
                "managed-consent-client",
                REDIRECT_URI,
            ),
        )),
    )
    .await;
    assert_eq!(restarted_token.status(), axum::http::StatusCode::OK);
    let restarted_token = response_json(restarted_token).await?;
    assert_eq!(
        response_string(&restarted_token, "scope")?,
        OAuthPermissions::TradingControl.scope()
    );
    assert!(restarted_gate.is_enabled());
    assert_eq!(automation_session_count(&restarted_database).await?, 0);
    restarted_owner.shutdown()?;
    Ok(())
}

#[tokio::test]
async fn managed_consent_gate_failure_withholds_tokens_without_retry() -> TestResult {
    use crate::database::DatabaseOwner;
    use crate::upbit::execution::LiveExecutionPolicy;
    use crate::upbit::live_order_control::{
        LiveOrderControlAction, LiveOrderControlOutcome, LiveOrderControlRequest,
        LiveOrderControlService, LiveOrderSendGate,
    };

    let root = OAuthTempRoot::new()?;
    let owner = DatabaseOwner::open_with_live_order_control(root.0.clone(), true)?;
    let gate = LiveOrderSendGate::new_disabled();
    let exchange = crate::upbit::UpbitExchangeService::new_mock_with_policy_and_send_gate(
        LiveExecutionPolicy::Disabled,
        gate.clone(),
    );
    let control = LiveOrderControlService::new(
        owner.handle(),
        exchange.live_execution_policy(),
        exchange.live_order_send_gate(),
        exchange.exchange_read_mode(),
    );
    let state = full_trading_state()?;
    state
        .bind_managed_live_order_control(control.clone())
        .map_err(test_error)?;
    add_test_client(&state, "managed-failed-enable-client");

    let code = approve_authorization_with_redirect(
        &state,
        "managed-failed-enable-client",
        REDIRECT_URI,
        "managed-enable-denied",
        OAuthPermissions::Research,
    )
    .await?;
    let request_id =
        consent_live_order_request_id("managed-failed-enable-client", &code, &state.inner.resource);
    let denied = token(
        axum::extract::State(state.clone()),
        RawForm(axum::body::Bytes::from(
            authorization_code_form_with_redirect(
                &code,
                "managed-failed-enable-client",
                REDIRECT_URI,
            ),
        )),
    )
    .await;
    assert_eq!(denied.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
    assert!(!gate.is_enabled());
    {
        let grants = state
            .inner
            .grants
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(grants.codes.is_empty());
        assert!(grants.tokens.is_empty());
        assert!(grants.refresh_tokens.is_empty());
    }
    let receipt = control
        .execute(LiveOrderControlRequest {
            action: LiveOrderControlAction::Request,
            request_id: Some(request_id),
            expected_sequence: None,
            expected_epoch: None,
            confirmation: None,
        })
        .await?
        .receipt
        .ok_or_else(|| test_error("failed gate request remains auditable"))?;
    assert_eq!(receipt.action, LiveOrderControlAction::Enable);
    assert_eq!(receipt.outcome, LiveOrderControlOutcome::Denied);
    assert_eq!(automation_session_count(&owner.handle()).await?, 0);
    owner.shutdown()?;
    Ok(())
}

#[tokio::test]
async fn authorization_rejects_disabled_trading_and_describes_each_consent_scope() -> TestResult {
    let disabled = OAuthState::new(PUBLIC_URL, OWNER_CODE.into()).map_err(test_error)?;
    let rejected = authorize(
        axum::extract::State(disabled),
        axum::extract::RawQuery(Some(authorization_query(
            "client",
            "disabled-scope",
            encoded_scope(OAuthPermissions::TradingRead),
        ))),
    )
    .await;
    assert_eq!(rejected.status(), axum::http::StatusCode::BAD_REQUEST);
    let rejection = response_json(rejected).await?;
    assert_eq!(response_string(&rejection, "error")?, "invalid_scope");

    let enabled = enabled_state()?;
    add_test_client(&enabled, "client");
    for (index, permissions) in [
        OAuthPermissions::Research,
        OAuthPermissions::TradingRead,
        OAuthPermissions::TradingControl,
    ]
    .into_iter()
    .enumerate()
    {
        let state_param = format!("consent-{index}");
        let response = authorize(
            axum::extract::State(enabled.clone()),
            axum::extract::RawQuery(Some(authorization_query(
                "client",
                &state_param,
                encoded_scope(permissions),
            ))),
        )
        .await;
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let consent = response_text(response).await?;
        assert!(consent.contains(permissions.scope()));
        assert!(consent.contains(permissions.description()));

        let grants = enabled
            .inner
            .grants
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(
            grants
                .pending
                .values()
                .any(|pending| pending.state == state_param && pending.permissions == permissions)
        );
    }
    Ok(())
}

#[tokio::test]
async fn owner_approved_pkce_and_refresh_preserve_each_scope() -> TestResult {
    let state = enabled_state()?;
    for (index, permissions) in [
        OAuthPermissions::Research,
        OAuthPermissions::TradingRead,
        OAuthPermissions::TradingControl,
    ]
    .into_iter()
    .enumerate()
    {
        let client_id = format!("client-{index}");
        let state_param = format!("pkce-{index}");
        add_test_client(&state, &client_id);

        let code = approve_authorization(&state, &client_id, &state_param, permissions).await?;

        let code_form = authorization_code_form(&code, &client_id);
        let issued = token(
            axum::extract::State(state.clone()),
            RawForm(axum::body::Bytes::from(code_form.clone())),
        )
        .await;
        assert_eq!(issued.status(), axum::http::StatusCode::OK);
        let issued = response_json(issued).await?;
        assert_eq!(response_string(&issued, "scope")?, permissions.scope());
        let access_token = response_string(&issued, "access_token")?.to_owned();
        let refresh_token = response_string(&issued, "refresh_token")?.to_owned();
        let access_headers = bearer_headers(&access_token)?;
        assert_eq!(
            state.permissions_for_headers(&access_headers),
            Some(permissions)
        );

        let replayed_code = token(
            axum::extract::State(state.clone()),
            RawForm(axum::body::Bytes::from(code_form)),
        )
        .await;
        assert_eq!(replayed_code.status(), axum::http::StatusCode::BAD_REQUEST);
        let replayed_code = response_json(replayed_code).await?;
        assert_eq!(response_string(&replayed_code, "error")?, "invalid_grant");

        let attempted_scope = match permissions {
            OAuthPermissions::TradingRead => OAuthPermissions::TradingControl,
            OAuthPermissions::Research | OAuthPermissions::TradingControl => {
                OAuthPermissions::TradingRead
            }
        };
        let denied_refresh = token(
            axum::extract::State(state.clone()),
            RawForm(axum::body::Bytes::from(refresh_form(
                &refresh_token,
                &client_id,
                Some(encoded_scope(attempted_scope)),
            ))),
        )
        .await;
        assert_eq!(denied_refresh.status(), axum::http::StatusCode::BAD_REQUEST);
        let denied_refresh = response_json(denied_refresh).await?;
        assert_eq!(response_string(&denied_refresh, "error")?, "invalid_scope");

        let rotated = token(
            axum::extract::State(state.clone()),
            RawForm(axum::body::Bytes::from(refresh_form(
                &refresh_token,
                &client_id,
                None,
            ))),
        )
        .await;
        assert_eq!(rotated.status(), axum::http::StatusCode::OK);
        let rotated = response_json(rotated).await?;
        assert_eq!(response_string(&rotated, "scope")?, permissions.scope());
        let rotated_access = response_string(&rotated, "access_token")?.to_owned();
        let rotated_headers = bearer_headers(&rotated_access)?;
        assert_eq!(
            state.permissions_for_headers(&rotated_headers),
            Some(permissions)
        );

        let replayed_refresh = token(
            axum::extract::State(state.clone()),
            RawForm(axum::body::Bytes::from(refresh_form(
                &refresh_token,
                &client_id,
                None,
            ))),
        )
        .await;
        assert_eq!(
            replayed_refresh.status(),
            axum::http::StatusCode::BAD_REQUEST
        );
        let replayed_refresh = response_json(replayed_refresh).await?;
        assert_eq!(
            response_string(&replayed_refresh, "error")?,
            "invalid_grant"
        );
    }
    Ok(())
}

#[test]
fn bearer_permission_lookup_requires_resource_match_and_unexpired_token() -> TestResult {
    let state = enabled_state()?;
    let now = std::time::Instant::now();
    {
        let mut grants = state
            .inner
            .grants
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        grants.tokens.insert(
            "valid-read".into(),
            AccessToken {
                permissions: OAuthPermissions::TradingRead,
                client_id: "client".into(),
                resource: state.inner.resource.clone(),
                expires_at: now + TOKEN_TTL,
            },
        );
        grants.tokens.insert(
            "wrong-resource".into(),
            AccessToken {
                permissions: OAuthPermissions::TradingControl,
                client_id: "client".into(),
                resource: "https://other.example/mcp".into(),
                expires_at: now + TOKEN_TTL,
            },
        );
        grants.tokens.insert(
            "expired".into(),
            AccessToken {
                permissions: OAuthPermissions::Research,
                client_id: "client".into(),
                resource: state.inner.resource.clone(),
                expires_at: now,
            },
        );
    }

    let valid_headers = bearer_headers("valid-read")?;
    assert_eq!(
        state.permissions_for_headers(&valid_headers),
        Some(OAuthPermissions::TradingRead)
    );
    let wrong_resource_headers = bearer_headers("wrong-resource")?;
    assert_eq!(state.permissions_for_headers(&wrong_resource_headers), None);
    let expired_headers = bearer_headers("expired")?;
    assert_eq!(state.permissions_for_headers(&expired_headers), None);
    Ok(())
}
