//! Small, bounded OAuth 2.1 authorization server for the MCP HTTP transport.
//!
//! The server intentionally keeps grants in process memory. Restarting the
//! process invalidates clients, pending approvals, authorization codes and
//! access tokens, which is appropriate for the operator-owned desktop app.

use axum::Router;
use axum::extract::{RawForm, RawQuery, State};
use axum::http::{HeaderMap, StatusCode, Uri, header};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::Read as _;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const MAX_CLIENTS: usize = 64;
const MAX_PENDING_APPROVALS: usize = 128;
const MAX_CODES: usize = 128;
const MAX_TOKENS: usize = 128;
const APPROVAL_TTL: Duration = Duration::from_mins(10);
const CODE_TTL: Duration = Duration::from_mins(5);
const TOKEN_TTL: Duration = Duration::from_hours(1);
const REFRESH_TOKEN_TTL: Duration = Duration::from_hours(30 * 24);
const SCOPE: &str = "mcp";

#[derive(Clone)]
pub struct OAuthState {
    inner: Arc<OAuthInner>,
}

struct OAuthInner {
    public_url: String,
    public_host: String,
    resource: String,
    owner_code: String,
    grants: Mutex<Grants>,
}

#[derive(Default)]
struct Grants {
    clients: HashMap<String, Client>,
    pending: HashMap<String, PendingApproval>,
    codes: HashMap<String, AuthorizationCode>,
    tokens: HashMap<String, AccessToken>,
    refresh_tokens: HashMap<String, AccessToken>,
}

#[derive(Clone)]
struct Client {
    redirect_uris: Vec<String>,
    display_name: String,
    registered_at: Instant,
}

struct PendingApproval {
    client_id: String,
    redirect_uri: String,
    state: String,
    resource: String,
    code_challenge: String,
    csrf: String,
    expires_at: Instant,
}

struct AuthorizationCode {
    client_id: String,
    redirect_uri: String,
    resource: String,
    code_challenge: String,
    expires_at: Instant,
}

struct AccessToken {
    client_id: String,
    resource: String,
    expires_at: Instant,
}

struct ApprovalRedirect {
    redirect_uri: String,
    state: String,
}

#[derive(Clone, Copy)]
enum GrantError {
    InvalidRequest,
    AccessDenied,
    InvalidGrant,
    Capacity,
}

#[derive(Deserialize)]
struct RegistrationRequest {
    redirect_uris: Vec<String>,
    #[serde(default)]
    client_name: String,
    #[serde(default)]
    grant_types: Vec<String>,
    #[serde(default)]
    response_types: Vec<String>,
    #[serde(default)]
    token_endpoint_auth_method: String,
    #[serde(default)]
    application_type: String,
}

#[derive(Serialize)]
struct RegistrationResponse {
    client_id: String,
    redirect_uris: Vec<String>,
    client_name: String,
    grant_types: [&'static str; 2],
    response_types: [&'static str; 1],
    token_endpoint_auth_method: &'static str,
}

impl OAuthState {
    /// Builds an isolated in-memory grant owner for one HTTPS MCP origin.
    ///
    /// # Errors
    ///
    /// Returns an error when the public URL is not an HTTPS origin or the
    /// owner approval code does not satisfy the bounded secret contract.
    pub fn new(public_url: &str, owner_code: String) -> Result<Self, String> {
        let public_url = public_url.trim_end_matches('/').to_owned();
        let public_host = parse_public_origin(&public_url).ok_or_else(|| {
            "--public-url must be an https origin without a path, query, or fragment".to_owned()
        })?;
        if owner_code.len() < 16 || owner_code.len() > 256 {
            return Err("SPOT_LAB_OAUTH_OWNER_CODE must contain 16..=256 characters".into());
        }
        Ok(Self {
            inner: Arc::new(OAuthInner {
                resource: format!("{public_url}/mcp"),
                public_url,
                public_host,
                owner_code,
                grants: Mutex::new(Grants::default()),
            }),
        })
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route(
                "/.well-known/oauth-protected-resource",
                get(protected_resource),
            )
            .route(
                "/.well-known/oauth-protected-resource/mcp",
                get(protected_resource),
            )
            .route(
                "/.well-known/oauth-authorization-server",
                get(authorization_server),
            )
            .route("/register", post(register))
            .route("/authorize", get(authorize))
            .route("/authorize/approve", post(approve))
            .route("/token", post(token))
            .with_state(self.clone())
    }

    #[must_use]
    pub fn resource_metadata_url(&self) -> String {
        format!(
            "{}/.well-known/oauth-protected-resource",
            self.inner.public_url
        )
    }

    #[must_use]
    pub fn public_host(&self) -> &str {
        &self.inner.public_host
    }

    #[must_use]
    pub fn accepts_bearer(&self, headers: &HeaderMap) -> bool {
        let Some(token) = bearer_token(headers) else {
            return false;
        };
        let Ok(mut grants) = self.inner.grants.lock() else {
            return false;
        };
        grants.valid_access_token(token, &self.inner.resource, Instant::now())
    }
}

#[must_use]
pub fn accepts_legacy_bearer(headers: &HeaderMap, expected: &str) -> bool {
    bearer_token(headers)
        .is_some_and(|present| constant_time_eq(present.as_bytes(), expected.as_bytes()))
}

impl Grants {
    fn prune(&mut self, now: Instant) {
        self.pending.retain(|_, value| value.expires_at > now);
        self.codes.retain(|_, value| value.expires_at > now);
        self.tokens.retain(|_, value| value.expires_at > now);
        self.refresh_tokens
            .retain(|_, value| value.expires_at > now);
    }

    fn insert_client_bounded(&mut self, client_id: String, client: Client) -> Result<(), ()> {
        if self.clients.len() >= MAX_CLIENTS {
            let oldest_unused = self
                .clients
                .iter()
                .filter(|(candidate, _)| !self.client_has_issued_grant(candidate))
                .min_by_key(|(_, registered)| registered.registered_at)
                .map(|(candidate, _)| candidate.clone());
            let Some(oldest_unused) = oldest_unused else {
                return Err(());
            };
            self.clients.remove(&oldest_unused);
            self.pending
                .retain(|_, pending| pending.client_id != oldest_unused);
        }
        self.clients.insert(client_id, client);
        Ok(())
    }

    fn client_has_issued_grant(&self, client_id: &str) -> bool {
        self.codes
            .values()
            .any(|grant| grant.client_id == client_id)
            || self
                .tokens
                .values()
                .any(|grant| grant.client_id == client_id)
            || self
                .refresh_tokens
                .values()
                .any(|grant| grant.client_id == client_id)
    }

    fn insert_pending_bounded(
        &mut self,
        flow_id: String,
        pending: PendingApproval,
    ) -> Result<(), ()> {
        if !self.clients.contains_key(&pending.client_id) {
            return Err(());
        }
        if self.pending.len() >= MAX_PENDING_APPROVALS
            && let Some(oldest) = self
                .pending
                .iter()
                .min_by_key(|(_, approval)| approval.expires_at)
                .map(|(flow_id, _)| flow_id.clone())
        {
            self.pending.remove(&oldest);
        }
        self.pending.insert(flow_id, pending);
        Ok(())
    }

    fn approve_pending(
        &mut self,
        request: ApprovalRequest<'_>,
        code: String,
        now: Instant,
    ) -> Result<ApprovalRedirect, GrantError> {
        self.prune(now);
        let Some(pending) = self.pending.get(request.flow_id) else {
            return Err(GrantError::InvalidRequest);
        };
        if !constant_time_eq(request.csrf.as_bytes(), pending.csrf.as_bytes())
            || !constant_time_eq(
                request.owner_code.as_bytes(),
                request.expected_owner.as_bytes(),
            )
        {
            self.pending.remove(request.flow_id);
            return Err(GrantError::AccessDenied);
        }
        if self.codes.len() >= MAX_CODES {
            return Err(GrantError::Capacity);
        }
        let Some(pending) = self.pending.remove(request.flow_id) else {
            return Err(GrantError::InvalidRequest);
        };
        let redirect = ApprovalRedirect {
            redirect_uri: pending.redirect_uri.clone(),
            state: pending.state,
        };
        self.codes.insert(
            code,
            AuthorizationCode {
                client_id: pending.client_id,
                redirect_uri: pending.redirect_uri,
                resource: pending.resource,
                code_challenge: pending.code_challenge,
                expires_at: now + CODE_TTL,
            },
        );
        Ok(redirect)
    }

    fn redeem_code(
        &mut self,
        request: CodeRedemption<'_>,
        issued: IssuedTokenPair,
        now: Instant,
    ) -> Result<(), GrantError> {
        self.prune(now);
        let Some(grant) = self.codes.get(request.code) else {
            return Err(GrantError::InvalidGrant);
        };
        if grant.client_id != request.client_id
            || grant.redirect_uri != request.redirect_uri
            || grant.resource != request.resource
            || grant.resource != request.expected_resource
            || !constant_time_eq(
                request.challenge.as_bytes(),
                grant.code_challenge.as_bytes(),
            )
        {
            self.codes.remove(request.code);
            return Err(GrantError::InvalidGrant);
        }
        if self.tokens.len() >= MAX_TOKENS || self.refresh_tokens.len() >= MAX_TOKENS {
            return Err(GrantError::Capacity);
        }
        let Some(grant) = self.codes.remove(request.code) else {
            return Err(GrantError::InvalidGrant);
        };
        self.tokens.insert(
            issued.access_token,
            AccessToken {
                client_id: request.client_id.into(),
                resource: grant.resource,
                expires_at: now + TOKEN_TTL,
            },
        );
        self.refresh_tokens.insert(
            issued.refresh_token,
            AccessToken {
                client_id: request.client_id.into(),
                resource: request.expected_resource.into(),
                expires_at: now + REFRESH_TOKEN_TTL,
            },
        );
        Ok(())
    }

    fn rotate_refresh(
        &mut self,
        request: RefreshRequest<'_>,
        issued: IssuedTokenPair,
        now: Instant,
    ) -> Result<(), GrantError> {
        self.prune(now);
        if !self.clients.contains_key(request.client_id) {
            return Err(GrantError::InvalidGrant);
        }
        let Some(grant) = self.refresh_tokens.get(request.refresh_token) else {
            return Err(GrantError::InvalidGrant);
        };
        if grant.client_id != request.client_id
            || grant.resource != request.resource
            || grant.resource != request.expected_resource
        {
            self.refresh_tokens.remove(request.refresh_token);
            return Err(GrantError::InvalidGrant);
        }
        if self.tokens.len() >= MAX_TOKENS {
            return Err(GrantError::Capacity);
        }
        let Some(_grant) = self.refresh_tokens.remove(request.refresh_token) else {
            return Err(GrantError::InvalidGrant);
        };
        self.tokens.insert(
            issued.access_token,
            AccessToken {
                client_id: request.client_id.into(),
                resource: request.expected_resource.into(),
                expires_at: now + TOKEN_TTL,
            },
        );
        self.refresh_tokens.insert(
            issued.refresh_token,
            AccessToken {
                client_id: request.client_id.into(),
                resource: request.expected_resource.into(),
                expires_at: now + REFRESH_TOKEN_TTL,
            },
        );
        Ok(())
    }

    fn valid_access_token(&mut self, token: &str, resource: &str, now: Instant) -> bool {
        let Some(access) = self.tokens.get(token) else {
            return false;
        };
        if access.resource == resource && access.expires_at > now {
            return true;
        }
        self.tokens.remove(token);
        false
    }
}

#[derive(Clone, Copy)]
struct ApprovalRequest<'a> {
    flow_id: &'a str,
    csrf: &'a str,
    owner_code: &'a str,
    expected_owner: &'a str,
}

#[derive(Clone, Copy)]
struct CodeRedemption<'a> {
    code: &'a str,
    client_id: &'a str,
    redirect_uri: &'a str,
    resource: &'a str,
    expected_resource: &'a str,
    challenge: &'a str,
}

#[derive(Clone, Copy)]
struct RefreshRequest<'a> {
    refresh_token: &'a str,
    client_id: &'a str,
    resource: &'a str,
    expected_resource: &'a str,
}

struct IssuedTokenPair {
    access_token: String,
    refresh_token: String,
}

async fn protected_resource(State(state): State<OAuthState>) -> Response {
    json_response(
        StatusCode::OK,
        &serde_json::json!({
            "resource": state.inner.resource,
            "authorization_servers": [state.inner.public_url],
            "scopes_supported": [SCOPE],
            "bearer_methods_supported": ["header"],
            "resource_name": "Sky Backcraft MCP"
        }),
    )
}

async fn authorization_server(State(state): State<OAuthState>) -> Response {
    let issuer = &state.inner.public_url;
    json_response(
        StatusCode::OK,
        &serde_json::json!({
            "issuer": issuer,
            "authorization_endpoint": format!("{issuer}/authorize"),
            "token_endpoint": format!("{issuer}/token"),
            "registration_endpoint": format!("{issuer}/register"),
            "response_types_supported": ["code"],
            "grant_types_supported": ["authorization_code", "refresh_token"],
            "code_challenge_methods_supported": ["S256"],
            "token_endpoint_auth_methods_supported": ["none"],
            "scopes_supported": [SCOPE],
            "authorization_response_iss_parameter_supported": true
        }),
    )
}

async fn register(State(state): State<OAuthState>, body: String) -> Response {
    let Ok(request) = serde_json::from_str::<RegistrationRequest>(&body) else {
        return oauth_error(StatusCode::BAD_REQUEST, "invalid_client_metadata");
    };
    if request.redirect_uris.is_empty()
        || request.redirect_uris.len() > 8
        || request
            .redirect_uris
            .iter()
            .any(|uri| !valid_redirect_uri(uri))
        || (!request.grant_types.is_empty()
            && (!request
                .grant_types
                .iter()
                .any(|grant| grant == "authorization_code")
                || request.grant_types.iter().any(|grant| {
                    !matches!(grant.as_str(), "authorization_code" | "refresh_token")
                })))
        || (!request.response_types.is_empty() && request.response_types != ["code".to_owned()])
        || (!request.token_endpoint_auth_method.is_empty()
            && request.token_endpoint_auth_method != "none")
        || (!request.application_type.is_empty()
            && !matches!(request.application_type.as_str(), "native" | "web"))
    {
        return oauth_error(StatusCode::BAD_REQUEST, "invalid_client_metadata");
    }
    let Ok(client_id) = random_token(24) else {
        return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, "server_error");
    };
    let name = if request.client_name.trim().is_empty() {
        "MCP client".to_owned()
    } else {
        request.client_name.chars().take(120).collect()
    };
    let client = Client {
        redirect_uris: request.redirect_uris.clone(),
        display_name: name.clone(),
        registered_at: Instant::now(),
    };
    {
        let Ok(mut grants) = state.inner.grants.lock() else {
            return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, "server_error");
        };
        grants.prune(Instant::now());
        if grants
            .insert_client_bounded(client_id.clone(), client)
            .is_err()
        {
            return oauth_error(StatusCode::SERVICE_UNAVAILABLE, "temporarily_unavailable");
        }
    }
    json_response(
        StatusCode::CREATED,
        &RegistrationResponse {
            client_id,
            redirect_uris: request.redirect_uris,
            client_name: name,
            grant_types: ["authorization_code", "refresh_token"],
            response_types: ["code"],
            token_endpoint_auth_method: "none",
        },
    )
}

async fn authorize(State(state): State<OAuthState>, RawQuery(query): RawQuery) -> Response {
    let Some(query) = query else {
        return oauth_error(StatusCode::BAD_REQUEST, "invalid_request");
    };
    let Ok(params) = parse_form(&query) else {
        return oauth_error(StatusCode::BAD_REQUEST, "invalid_request");
    };
    let required = |name: &str| params.get(name).filter(|value| !value.is_empty());
    let (Some(client_id), Some(redirect_uri), Some(state_param), Some(resource), Some(challenge)) = (
        required("client_id"),
        required("redirect_uri"),
        required("state"),
        required("resource"),
        required("code_challenge"),
    ) else {
        return oauth_error(StatusCode::BAD_REQUEST, "invalid_request");
    };
    if params.get("response_type").map(String::as_str) != Some("code")
        || params.get("code_challenge_method").map(String::as_str) != Some("S256")
        || params.get("scope").is_some_and(|scope| scope != SCOPE)
        || resource != &state.inner.resource
        || challenge.len() != 43
        || !challenge.bytes().all(is_base64url_byte)
    {
        return oauth_error(StatusCode::BAD_REQUEST, "invalid_request");
    }
    let (client_name, redirect_registered) = {
        let Ok(grants) = state.inner.grants.lock() else {
            return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, "server_error");
        };
        match grants.clients.get(client_id) {
            Some(client) => (
                client.display_name.clone(),
                client.redirect_uris.iter().any(|uri| uri == redirect_uri),
            ),
            None => return oauth_error(StatusCode::BAD_REQUEST, "unauthorized_client"),
        }
    };
    if !redirect_registered {
        return oauth_error(StatusCode::BAD_REQUEST, "invalid_request");
    }
    let Ok(flow_id) = random_token(24) else {
        return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, "server_error");
    };
    let Ok(csrf) = random_token(24) else {
        return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, "server_error");
    };
    let pending = PendingApproval {
        client_id: client_id.clone(),
        redirect_uri: redirect_uri.clone(),
        state: state_param.clone(),
        resource: resource.clone(),
        code_challenge: challenge.clone(),
        csrf: csrf.clone(),
        expires_at: Instant::now() + APPROVAL_TTL,
    };
    {
        let Ok(mut grants) = state.inner.grants.lock() else {
            return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, "server_error");
        };
        grants.prune(Instant::now());
        if grants
            .insert_pending_bounded(flow_id.clone(), pending)
            .is_err()
        {
            return oauth_error(StatusCode::BAD_REQUEST, "unauthorized_client");
        }
    }
    consent_response(consent_page(&client_name, redirect_uri, &flow_id, &csrf))
}

async fn approve(State(state): State<OAuthState>, RawForm(body): RawForm) -> Response {
    let Ok(text) = std::str::from_utf8(&body) else {
        return oauth_error(StatusCode::BAD_REQUEST, "invalid_request");
    };
    let Ok(params) = parse_form(text) else {
        return oauth_error(StatusCode::BAD_REQUEST, "invalid_request");
    };
    let (Some(flow_id), Some(csrf), Some(raw_owner_code)) = (
        params.get("flow_id"),
        params.get("csrf"),
        params.get("owner_code"),
    ) else {
        return oauth_error(StatusCode::BAD_REQUEST, "invalid_request");
    };
    let owner_code = raw_owner_code.trim();
    let Ok(code) = random_token(32) else {
        return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, "server_error");
    };
    let redirect = {
        let Ok(mut grants) = state.inner.grants.lock() else {
            return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, "server_error");
        };
        match grants.approve_pending(
            ApprovalRequest {
                flow_id,
                csrf,
                owner_code,
                expected_owner: &state.inner.owner_code,
            },
            code.clone(),
            Instant::now(),
        ) {
            Ok(redirect) => redirect,
            Err(GrantError::AccessDenied) => {
                return (
                    StatusCode::FORBIDDEN,
                    [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
                    r#"<!doctype html><html lang="ko"><head><meta charset="utf-8"><title>승인 실패</title></head>
<body style="font-family:-apple-system,sans-serif;max-width:560px;margin:60px auto;padding:0 20px;line-height:1.6">
<h2 style="color:#d32f2f">승인 코드가 올바르지 않습니다 (403)</h2>
<p>입력하신 운영자 승인 코드가 현재 실행 중인 Sky Backcraft 앱의 코드와 일치하지 않습니다.</p>
<ol style="padding-left:20px">
<li>macOS 상단 메뉴 막대의 <strong>Sky Backcraft 아이콘</strong>을 클릭하세요.</li>
<li><strong>'OAuth 로그인 코드 복사'</strong>를 클릭하여 최신 코드를 복사하세요.</li>
<li>연결하려는 MCP 클라이언트(Cursor, Claude Desktop 등)에서 다시 연결을 시도하여 열리는 승인 창에 붙여넣으세요.</li>
</ol>
<p style="color:#888;font-size:13px">※ 앱이 재시작되면 보안을 위해 승인 코드가 새로 생성됩니다.</p>
</body></html>"#,
                )
                    .into_response();
            }
            Err(error) => return grant_error_response(error),
        }
    };
    let location = append_query(
        &redirect.redirect_uri,
        &[
            ("code", &code),
            ("state", &redirect.state),
            ("iss", &state.inner.public_url),
        ],
    );
    Redirect::to(&location).into_response()
}

async fn token(State(state): State<OAuthState>, RawForm(body): RawForm) -> Response {
    let Ok(text) = std::str::from_utf8(&body) else {
        return oauth_error(StatusCode::BAD_REQUEST, "invalid_request");
    };
    let Ok(params) = parse_form(text) else {
        return oauth_error(StatusCode::BAD_REQUEST, "invalid_request");
    };
    if params.get("grant_type").map(String::as_str) == Some("refresh_token") {
        return refresh_token(&state, &params);
    }
    if params.get("grant_type").map(String::as_str) != Some("authorization_code") {
        return oauth_error(StatusCode::BAD_REQUEST, "unsupported_grant_type");
    }
    let (Some(code), Some(client_id), Some(redirect_uri), Some(verifier), Some(resource)) = (
        params.get("code"),
        params.get("client_id"),
        params.get("redirect_uri"),
        params.get("code_verifier"),
        params.get("resource"),
    ) else {
        return oauth_error(StatusCode::BAD_REQUEST, "invalid_request");
    };
    if !(43..=128).contains(&verifier.len()) || !verifier.bytes().all(is_pkce_verifier_byte) {
        return oauth_error(StatusCode::BAD_REQUEST, "invalid_grant");
    }
    let challenge = base64url(&Sha256::digest(verifier.as_bytes()));
    let Ok(access_token) = random_token(32) else {
        return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, "server_error");
    };
    let Ok(refresh_token) = random_token(32) else {
        return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, "server_error");
    };
    {
        let Ok(mut grants) = state.inner.grants.lock() else {
            return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, "server_error");
        };
        if let Err(error) = grants.redeem_code(
            CodeRedemption {
                code,
                client_id,
                redirect_uri,
                resource,
                expected_resource: &state.inner.resource,
                challenge: &challenge,
            },
            IssuedTokenPair {
                access_token: access_token.clone(),
                refresh_token: refresh_token.clone(),
            },
            Instant::now(),
        ) {
            return grant_error_response(error);
        }
    }
    token_response(
        StatusCode::OK,
        &serde_json::json!({
            "access_token": access_token,
            "token_type": "Bearer",
            "expires_in": TOKEN_TTL.as_secs(),
            "scope": SCOPE,
            "refresh_token": refresh_token
        }),
    )
}

fn refresh_token(state: &OAuthState, params: &HashMap<String, String>) -> Response {
    let (Some(refresh_token), Some(client_id), Some(resource)) = (
        params.get("refresh_token"),
        params.get("client_id"),
        params.get("resource"),
    ) else {
        return oauth_error(StatusCode::BAD_REQUEST, "invalid_request");
    };
    let Ok(access_token) = random_token(32) else {
        return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, "server_error");
    };
    let Ok(next_refresh) = random_token(32) else {
        return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, "server_error");
    };
    {
        let Ok(mut grants) = state.inner.grants.lock() else {
            return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, "server_error");
        };
        if let Err(error) = grants.rotate_refresh(
            RefreshRequest {
                refresh_token,
                client_id,
                resource,
                expected_resource: &state.inner.resource,
            },
            IssuedTokenPair {
                access_token: access_token.clone(),
                refresh_token: next_refresh.clone(),
            },
            Instant::now(),
        ) {
            return grant_error_response(error);
        }
    }
    token_response(
        StatusCode::OK,
        &serde_json::json!({
            "access_token": access_token,
            "token_type": "Bearer",
            "expires_in": TOKEN_TTL.as_secs(),
            "scope": SCOPE,
            "refresh_token": next_refresh
        }),
    )
}

fn consent_page(client_name: &str, redirect_uri: &str, flow_id: &str, csrf: &str) -> String {
    format!(
        "<!doctype html><html lang=\"ko\"><head><meta charset=\"utf-8\"><title>Sky Backcraft 승인</title></head>\
         <body style=\"font-family:-apple-system,sans-serif;max-width:560px;margin:60px auto;padding:0 20px;line-height:1.6\">\
         <h2>Sky Backcraft MCP 연결 승인</h2><p><strong>{}</strong> 앱이 Sky Backcraft MCP에 접근하려고 합니다.</p>\
         <p style=\"word-break:break-all;color:#555\">돌아갈 주소: {}</p>\
         <form method=\"post\" action=\"/authorize/approve\">\
         <input type=\"hidden\" name=\"flow_id\" value=\"{}\"><input type=\"hidden\" name=\"csrf\" value=\"{}\">\
         <label for=\"owner_code\" style=\"font-weight:600\">앱에 표시된 운영자 승인 코드</label>\
         <p style=\"font-size:13px;color:#666;margin:4px 0 8px 0\">macOS 상단 메뉴 막대 아이콘 &gt; <strong>'OAuth 로그인 코드 복사'</strong>를 클릭하여 복사된 64자리 코드를 붙여넣으세요.</p>\
         <input id=\"owner_code\" name=\"owner_code\" type=\"password\" autocomplete=\"one-time-code\" required style=\"display:block;width:100%;padding:10px;margin:8px 0;box-sizing:border-box\">\
         <button type=\"submit\" style=\"padding:10px 18px;cursor:pointer\">승인</button></form></body></html>",
        html_escape(client_name),
        html_escape(redirect_uri),
        html_escape(flow_id),
        html_escape(csrf)
    )
}

fn oauth_error(status: StatusCode, error: &'static str) -> Response {
    json_response(status, &serde_json::json!({ "error": error }))
}

fn grant_error_response(error: GrantError) -> Response {
    match error {
        GrantError::InvalidRequest => oauth_error(StatusCode::BAD_REQUEST, "invalid_request"),
        GrantError::AccessDenied => oauth_error(StatusCode::FORBIDDEN, "access_denied"),
        GrantError::InvalidGrant => oauth_error(StatusCode::BAD_REQUEST, "invalid_grant"),
        GrantError::Capacity => {
            oauth_error(StatusCode::SERVICE_UNAVAILABLE, "temporarily_unavailable")
        }
    }
}

fn token_response(status: StatusCode, value: &impl Serialize) -> Response {
    let mut response = json_response(status, value);
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("no-store"),
    );
    response.headers_mut().insert(
        header::PRAGMA,
        axum::http::HeaderValue::from_static("no-cache"),
    );
    response
}

fn consent_response(body: String) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .header(header::CACHE_CONTROL, "no-store")
        .header(header::PRAGMA, "no-cache")
        .header("x-frame-options", "DENY")
        .header("x-content-type-options", "nosniff")
        .header("referrer-policy", "no-referrer")
        .header(
            "content-security-policy",
            "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'",
        )
        .body(axum::body::Body::from(body))
        .unwrap_or_else(|_| Response::new(axum::body::Body::empty()))
}

fn json_response(status: StatusCode, value: &impl Serialize) -> Response {
    let body =
        serde_json::to_vec(&value).unwrap_or_else(|_| b"{\"error\":\"server_error\"}".to_vec());
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(body))
        .unwrap_or_else(|_| Response::new(axum::body::Body::empty()))
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .filter(|value| !value.is_empty())
}

fn parse_public_origin(value: &str) -> Option<String> {
    let uri = value.parse::<Uri>().ok()?;
    if uri.scheme_str() != Some("https") || uri.query().is_some() || uri.path() != "/" {
        return None;
    }
    let authority = uri.authority()?;
    valid_authority(authority).then(|| authority.as_str().to_owned())
}

fn valid_redirect_uri(value: &str) -> bool {
    if value
        .bytes()
        .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
        || value.contains('#')
    {
        return false;
    }
    let Ok(uri) = value.parse::<Uri>() else {
        return false;
    };
    let Some(authority) = uri
        .authority()
        .filter(|authority| valid_authority(authority))
    else {
        return false;
    };
    match uri.scheme_str() {
        Some("https") => true,
        Some("http") => matches!(authority.host(), "localhost" | "127.0.0.1" | "[::1]"),
        _ => false,
    }
}

fn valid_authority(authority: &axum::http::uri::Authority) -> bool {
    let has_port_suffix = authority.as_str().len() != authority.host().len();
    !authority.host().is_empty()
        && !authority.as_str().contains('@')
        && (!has_port_suffix || authority.port_u16().is_some_and(|port| port != 0))
}

fn parse_form(input: &str) -> Result<HashMap<String, String>, ()> {
    let mut values = HashMap::new();
    for pair in input.split('&') {
        let (key, value) = pair.split_once('=').ok_or(())?;
        let key = percent_decode(key)?;
        let value = percent_decode(value)?;
        if values.insert(key, value).is_some() {
            return Err(());
        }
    }
    Ok(values)
}

fn percent_decode(input: &str) -> Result<String, ()> {
    let bytes = input.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => decoded.push(b' '),
            b'%' => {
                let pair = bytes.get(index + 1..index + 3).ok_or(())?;
                let pair = std::str::from_utf8(pair).map_err(|_| ())?;
                decoded.push(u8::from_str_radix(pair, 16).map_err(|_| ())?);
                index += 2;
            }
            byte => decoded.push(byte),
        }
        index += 1;
    }
    String::from_utf8(decoded).map_err(|_| ())
}

fn append_query(base: &str, values: &[(&str, &str)]) -> String {
    let separator = if base.contains('?') { '&' } else { '?' };
    let mut result = format!("{base}{separator}");
    for (index, (key, value)) in values.iter().enumerate() {
        if index > 0 {
            result.push('&');
        }
        result.push_str(key);
        result.push('=');
        result.push_str(&percent_encode(value));
    }
    result
}

fn percent_encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(char::from(byte));
        } else {
            use std::fmt::Write as _;
            let _ = write!(encoded, "%{byte:02X}");
        }
    }
    encoded
}

fn random_token(byte_count: usize) -> Result<String, ()> {
    let mut bytes = vec![0_u8; byte_count];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut file| file.read_exact(&mut bytes))
        .map_err(|_| ())?;
    Ok(base64url(&bytes))
}

fn base64url(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut output = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let a = chunk[0];
        let b = chunk.get(1).copied().unwrap_or(0);
        let c = chunk.get(2).copied().unwrap_or(0);
        output.push(char::from(ALPHABET[usize::from(a >> 2)]));
        output.push(char::from(ALPHABET[usize::from((a & 0x03) << 4 | b >> 4)]));
        if chunk.len() > 1 {
            output.push(char::from(ALPHABET[usize::from((b & 0x0f) << 2 | c >> 6)]));
        }
        if chunk.len() > 2 {
            output.push(char::from(ALPHABET[usize::from(c & 0x3f)]));
        }
    }
    output
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    let length = left.len().max(right.len());
    for index in 0..length {
        difference |= usize::from(
            left.get(index).copied().unwrap_or(0) ^ right.get(index).copied().unwrap_or(0),
        );
    }
    difference == 0
}

fn is_base64url_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')
}

fn is_pkce_verifier_byte(byte: u8) -> bool {
    is_base64url_byte(byte) || matches!(byte, b'.' | b'~')
}

fn html_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_client(registered_at: Instant) -> Client {
        Client {
            redirect_uris: vec!["http://127.0.0.1/callback".into()],
            display_name: "Test client".into(),
            registered_at,
        }
    }

    fn test_pending(client_id: &str, expires_at: Instant) -> PendingApproval {
        PendingApproval {
            client_id: client_id.into(),
            redirect_uri: "http://127.0.0.1/callback".into(),
            state: "state".into(),
            resource: "https://skybackcraft.store/mcp".into(),
            code_challenge: "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM".into(),
            csrf: "csrf".into(),
            expires_at,
        }
    }

    fn test_access(client_id: &str, expires_at: Instant) -> AccessToken {
        AccessToken {
            client_id: client_id.into(),
            resource: "https://skybackcraft.store/mcp".into(),
            expires_at,
        }
    }

    #[test]
    fn base64url_matches_pkce_reference_vector() {
        let digest = Sha256::digest(b"dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk");
        assert_eq!(
            base64url(&digest),
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
    }

    #[test]
    fn public_and_redirect_urls_fail_closed() {
        let state = OAuthState::new(
            "https://skybackcraft.store:8443",
            "owner-code-123456".into(),
        )
        .unwrap_or_else(|error| unreachable!("valid test state: {error}"));
        assert_eq!(state.public_host(), "skybackcraft.store:8443");
        assert!(OAuthState::new("http://skybackcraft.store", "owner-code-123456".into()).is_err());
        assert!(
            OAuthState::new(
                "https://skybackcraft.store:not-a-port",
                "owner-code-123456".into()
            )
            .is_err()
        );
        assert!(valid_redirect_uri(
            "https://chatgpt.com/connector/oauth/callback"
        ));
        assert!(valid_redirect_uri("http://127.0.0.1:49152/callback"));
        assert!(!valid_redirect_uri("http://example.com/callback"));
        assert!(!valid_redirect_uri("http://localhost.evil/callback"));
        assert!(!valid_redirect_uri("https://"));
        assert!(!valid_redirect_uri("https://user@example.com/callback"));
        assert!(!valid_redirect_uri(
            "https://example.com/callback\r\nX-Injected: value"
        ));
        assert!(!valid_redirect_uri("https://example.com/callback#fragment"));
    }

    #[test]
    fn form_parser_rejects_duplicates_and_bad_encoding() {
        assert_eq!(
            parse_form("state=a%2Bb")
                .ok()
                .and_then(|form| form.get("state").cloned())
                .as_deref(),
            Some("a+b")
        );
        assert!(parse_form("code=one&code=two").is_err());
        assert!(parse_form("code=%zz").is_err());
    }

    #[test]
    fn constant_time_comparison_checks_length_and_content() {
        assert!(constant_time_eq(b"owner-code", b"owner-code"));
        assert!(!constant_time_eq(b"owner-code", b"owner-codf"));
        assert!(!constant_time_eq(b"owner-code", b"owner-code-long"));
    }

    #[test]
    fn bearer_lookup_does_not_prune_unrelated_grants() {
        let now = Instant::now();
        let mut grants = Grants::default();
        grants.tokens.insert(
            "valid".into(),
            test_access("client", now + Duration::from_secs(1)),
        );
        grants.tokens.insert(
            "expired".into(),
            test_access(
                "client",
                now.checked_sub(Duration::from_secs(1)).unwrap_or(now),
            ),
        );
        grants.pending.insert(
            "pending".into(),
            test_pending(
                "client",
                now.checked_sub(Duration::from_secs(1)).unwrap_or(now),
            ),
        );

        assert!(grants.valid_access_token("valid", "https://skybackcraft.store/mcp", now));
        assert!(grants.pending.contains_key("pending"));
        assert!(!grants.valid_access_token("expired", "https://skybackcraft.store/mcp", now));
        assert!(!grants.tokens.contains_key("expired"));
        assert!(grants.tokens.contains_key("valid"));
    }

    #[test]
    fn invalid_approval_and_pkce_consume_their_one_use_grants() {
        let now = Instant::now();
        let mut grants = Grants::default();
        grants
            .pending
            .insert("flow".into(), test_pending("client", now + APPROVAL_TTL));
        let denied = grants.approve_pending(
            ApprovalRequest {
                flow_id: "flow",
                csrf: "csrf",
                owner_code: "wrong-owner-code",
                expected_owner: "owner-code-123456",
            },
            "unused-code".into(),
            now,
        );
        assert!(matches!(denied, Err(GrantError::AccessDenied)));
        assert!(!grants.pending.contains_key("flow"));

        grants.codes.insert(
            "code".into(),
            AuthorizationCode {
                client_id: "client".into(),
                redirect_uri: "http://127.0.0.1/callback".into(),
                resource: "https://skybackcraft.store/mcp".into(),
                code_challenge: "expected-challenge".into(),
                expires_at: now + CODE_TTL,
            },
        );
        let denied = grants.redeem_code(
            CodeRedemption {
                code: "code",
                client_id: "client",
                redirect_uri: "http://127.0.0.1/callback",
                resource: "https://skybackcraft.store/mcp",
                expected_resource: "https://skybackcraft.store/mcp",
                challenge: "wrong-challenge",
            },
            IssuedTokenPair {
                access_token: "unused-access".into(),
                refresh_token: "unused-refresh".into(),
            },
            now,
        );
        assert!(matches!(denied, Err(GrantError::InvalidGrant)));
        assert!(!grants.codes.contains_key("code"));
        assert!(grants.tokens.is_empty());
        assert!(grants.refresh_tokens.is_empty());
    }

    #[test]
    fn token_responses_forbid_storage() {
        let response = token_response(StatusCode::OK, &serde_json::json!({ "token": "secret" }));
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL),
            Some(&axum::http::HeaderValue::from_static("no-store"))
        );
        assert_eq!(
            response.headers().get(header::PRAGMA),
            Some(&axum::http::HeaderValue::from_static("no-cache"))
        );
    }

    #[test]
    fn consent_response_cannot_be_framed_or_cached() {
        let response = consent_response("<form></form>".into());
        assert_eq!(
            response.headers().get("x-frame-options"),
            Some(&axum::http::HeaderValue::from_static("DENY"))
        );
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL),
            Some(&axum::http::HeaderValue::from_static("no-store"))
        );
        assert!(
            response
                .headers()
                .get("content-security-policy")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.contains("frame-ancestors 'none'"))
        );
        assert_eq!(
            response.headers().get("x-content-type-options"),
            Some(&axum::http::HeaderValue::from_static("nosniff"))
        );
    }

    #[test]
    fn client_capacity_evicts_oldest_unused_but_preserves_live_grants() {
        let now = Instant::now();
        let mut grants = Grants::default();
        for index in 0..MAX_CLIENTS {
            let inserted = grants.insert_client_bounded(
                format!("client-{index}"),
                Client {
                    redirect_uris: vec!["http://127.0.0.1/callback".into()],
                    display_name: format!("Client {index}"),
                    registered_at: now
                        + Duration::from_secs(u64::try_from(index).unwrap_or(u64::MAX)),
                },
            );
            assert!(inserted.is_ok());
        }
        grants.refresh_tokens.insert(
            "active-refresh".into(),
            AccessToken {
                client_id: "client-0".into(),
                resource: "https://skybackcraft.store/mcp".into(),
                expires_at: now + Duration::from_hours(1),
            },
        );

        assert!(
            grants
                .insert_client_bounded(
                    "new-client".into(),
                    Client {
                        redirect_uris: vec!["http://127.0.0.1/callback".into()],
                        display_name: "New client".into(),
                        registered_at: now + Duration::from_hours(2),
                    },
                )
                .is_ok()
        );
        assert_eq!(grants.clients.len(), MAX_CLIENTS);
        assert!(grants.clients.contains_key("client-0"));
        assert!(!grants.clients.contains_key("client-1"));
        assert!(grants.clients.contains_key("new-client"));
    }

    #[tokio::test]
    async fn code_capacity_rejection_preserves_pending_approval() {
        let state = OAuthState::new("https://skybackcraft.store", "owner-code-123456".into())
            .unwrap_or_else(|error| unreachable!("valid test state: {error}"));
        let now = Instant::now();
        {
            let mut grants = state
                .inner
                .grants
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            grants
                .pending
                .insert("flow".into(), test_pending("client", now + APPROVAL_TTL));
            for index in 0..MAX_CODES {
                grants.codes.insert(
                    format!("code-{index}"),
                    AuthorizationCode {
                        client_id: "other".into(),
                        redirect_uri: "http://127.0.0.1/callback".into(),
                        resource: "https://skybackcraft.store/mcp".into(),
                        code_challenge: "challenge".into(),
                        expires_at: now + CODE_TTL,
                    },
                );
            }
        }
        let response = approve(
            State(state.clone()),
            RawForm(axum::body::Bytes::from_static(
                b"flow_id=flow&csrf=csrf&owner_code=owner-code-123456",
            )),
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let grants = state
            .inner
            .grants
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(grants.pending.contains_key("flow"));
    }

    #[tokio::test]
    async fn token_capacity_rejection_preserves_code_without_partial_access_token() {
        let state = OAuthState::new("https://skybackcraft.store", "owner-code-123456".into())
            .unwrap_or_else(|error| unreachable!("valid test state: {error}"));
        let now = Instant::now();
        {
            let mut grants = state
                .inner
                .grants
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            grants.clients.insert("client".into(), test_client(now));
            grants.codes.insert(
                "code".into(),
                AuthorizationCode {
                    client_id: "client".into(),
                    redirect_uri: "http://127.0.0.1/callback".into(),
                    resource: "https://skybackcraft.store/mcp".into(),
                    code_challenge: "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM".into(),
                    expires_at: now + CODE_TTL,
                },
            );
            for index in 0..MAX_TOKENS {
                grants.refresh_tokens.insert(
                    format!("refresh-{index}"),
                    test_access("other", now + REFRESH_TOKEN_TTL),
                );
            }
        }
        let response = token(
            State(state.clone()),
            RawForm(axum::body::Bytes::from_static(
                concat!(
                    "grant_type=authorization_code&client_id=client&",
                    "redirect_uri=http://127.0.0.1/callback&code=code&",
                    "code_verifier=dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk&",
                    "resource=https://skybackcraft.store/mcp"
                )
                .as_bytes(),
            )),
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let grants = state
            .inner
            .grants
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(grants.codes.contains_key("code"));
        assert!(grants.tokens.is_empty());
    }

    #[test]
    fn access_capacity_rejection_preserves_refresh_token() {
        let state = OAuthState::new("https://skybackcraft.store", "owner-code-123456".into())
            .unwrap_or_else(|error| unreachable!("valid test state: {error}"));
        let now = Instant::now();
        {
            let mut grants = state
                .inner
                .grants
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            grants.clients.insert("client".into(), test_client(now));
            grants.refresh_tokens.insert(
                "refresh".into(),
                test_access("client", now + REFRESH_TOKEN_TTL),
            );
            for index in 0..MAX_TOKENS {
                grants.tokens.insert(
                    format!("access-{index}"),
                    test_access("other", now + TOKEN_TTL),
                );
            }
        }
        let params = parse_form(concat!(
            "grant_type=refresh_token&client_id=client&refresh_token=refresh&",
            "resource=https://skybackcraft.store/mcp"
        ))
        .unwrap_or_else(|()| unreachable!("valid test form"));
        let response = refresh_token(&state, &params);
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let grants = state
            .inner
            .grants
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(grants.refresh_tokens.contains_key("refresh"));
    }

    #[test]
    fn registration_pressure_evicts_pending_only_client_but_preserves_issued_client() {
        let now = Instant::now();
        let mut grants = Grants::default();
        for index in 0..MAX_CLIENTS {
            let client_id = format!("client-{index}");
            grants.clients.insert(
                client_id.clone(),
                test_client(now + Duration::from_secs(u64::try_from(index).unwrap_or(u64::MAX))),
            );
            grants.pending.insert(
                format!("flow-{index}"),
                test_pending(&client_id, now + APPROVAL_TTL),
            );
        }
        grants.codes.insert(
            "issued-code".into(),
            AuthorizationCode {
                client_id: "client-0".into(),
                redirect_uri: "http://127.0.0.1/callback".into(),
                resource: "https://skybackcraft.store/mcp".into(),
                code_challenge: "challenge".into(),
                expires_at: now + CODE_TTL,
            },
        );

        assert!(
            grants
                .insert_client_bounded(
                    "new-client".into(),
                    test_client(now + Duration::from_hours(1)),
                )
                .is_ok()
        );
        assert!(grants.clients.contains_key("client-0"));
        assert!(grants.pending.contains_key("flow-0"));
        assert!(!grants.clients.contains_key("client-1"));
        assert!(!grants.pending.contains_key("flow-1"));
        assert!(grants.clients.contains_key("new-client"));
    }

    #[tokio::test]
    async fn pending_capacity_replaces_oldest_unapproved_flow() {
        let state = OAuthState::new("https://skybackcraft.store", "owner-code-123456".into())
            .unwrap_or_else(|error| unreachable!("valid test state: {error}"));
        let now = Instant::now();
        {
            let mut grants = state
                .inner
                .grants
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            grants.clients.insert("client".into(), test_client(now));
            for index in 0..MAX_PENDING_APPROVALS {
                grants.pending.insert(
                    format!("flow-{index}"),
                    test_pending(
                        "other",
                        now + Duration::from_mins(1)
                            + Duration::from_secs(u64::try_from(index).unwrap_or(u64::MAX)),
                    ),
                );
            }
        }
        let query = concat!(
            "response_type=code&client_id=client&",
            "redirect_uri=http://127.0.0.1/callback&state=new-state&",
            "resource=https://skybackcraft.store/mcp&",
            "code_challenge=E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM&",
            "code_challenge_method=S256"
        );
        let response = authorize(State(state.clone()), RawQuery(Some(query.into()))).await;
        assert_eq!(response.status(), StatusCode::OK);
        let grants = state
            .inner
            .grants
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(grants.pending.len(), MAX_PENDING_APPROVALS);
        assert!(!grants.pending.contains_key("flow-0"));
    }
}
