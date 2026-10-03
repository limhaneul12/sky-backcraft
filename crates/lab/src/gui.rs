//! Minimal localhost setup GUI: a single form (domain, local port, auth
//! mode) that persists `spot-lab-gui.json` and can apply it by running
//! `docker compose up -d app` with the matching environment.
//!
//! Localhost-only by design: the form can start public exposures, so it must
//! never be reachable from the network.

use crate::contracts::LabError;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

pub const DEFAULT_LISTEN: &str = "127.0.0.1:9090";
pub const DEFAULT_CONFIG_FILE: &str = "spot-lab-gui.json";
pub(crate) const DEFAULT_APP_PORT: u16 = 8130;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GuiConfig {
    /// Operator-owned public domain routed to the local port (empty = local
    /// only).
    #[serde(default)]
    pub domain: String,
    /// Local MCP port for the app container/native process.
    #[serde(default = "default_app_port")]
    pub port: u16,
    /// `none` (public no-auth) or `oauth` (not implemented server-side yet).
    #[serde(default)]
    pub auth_mode: String,
}

fn default_app_port() -> u16 {
    DEFAULT_APP_PORT
}

impl Default for GuiConfig {
    fn default() -> Self {
        Self {
            domain: String::new(),
            port: DEFAULT_APP_PORT,
            auth_mode: "none".to_owned(),
        }
    }
}

impl GuiConfig {
    fn validate(&self) -> Result<(), String> {
        if !matches!(self.auth_mode.as_str(), "none" | "oauth") {
            return Err(format!(
                "인증 모드는 none 또는 oauth여야 합니다: {:?}",
                self.auth_mode
            ));
        }
        if self.domain.contains(['/', ' ', ':']) {
            return Err("도메인은 호스트명만 입력하세요 (예: skygptcodex.store)".to_owned());
        }
        Ok(())
    }

    fn load(config_path: &PathBuf) -> Self {
        std::fs::read(config_path)
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or_default()
    }

    fn store(&self, config_path: &PathBuf) -> Result<(), LabError> {
        let json = serde_json::to_vec_pretty(self).map_err(LabError::from)?;
        std::fs::write(config_path, json).map_err(|error| {
            LabError::Internal(format!("write {}: {error}", config_path.display()))
        })?;
        Ok(())
    }
}

#[derive(Clone)]
struct GuiState {
    config_path: PathBuf,
}

fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'%' if index + 2 < bytes.len() + 1 && index + 2 <= bytes.len() - 1 + 1 => {
                let hex = bytes.get(index + 1..index + 3);
                if let Some(byte) = hex.and_then(|pair| {
                    std::str::from_utf8(pair)
                        .ok()
                        .and_then(|pair| u8::from_str_radix(pair, 16).ok())
                }) {
                    out.push(byte);
                    index += 3;
                } else {
                    out.push(bytes[index]);
                    index += 1;
                }
            }
            b'+' => {
                out.push(b' ');
                index += 1;
            }
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn parse_form(body: &str) -> BTreeMap<String, String> {
    let mut fields = std::collections::BTreeMap::new();
    for pair in body.split('&') {
        let Some((key, value)) = pair.split_once('=') else {
            continue;
        };
        fields.insert(percent_decode(key), percent_decode(value));
    }
    fields
}

fn render_page(config: &GuiConfig, notice: &str, notice_kind: &str) -> String {
    let auth_none_checked = if config.auth_mode == "oauth" {
        ""
    } else {
        "checked"
    };
    let auth_oauth_checked = if config.auth_mode == "oauth" {
        "checked"
    } else {
        ""
    };
    let banner = if notice.is_empty() {
        String::new()
    } else {
        format!(
            "<div class=\"notice {notice_kind}\">{notice}</div>",
            notice_kind = if notice_kind == "error" {
                "error"
            } else {
                "ok"
            },
        )
    };
    let domain_value = html_escape(&config.domain);
    let port_value = config.port;
    format!(
        r#"<!DOCTYPE html>
<html lang="ko"><head><meta charset="utf-8">
<title>Sky Backcraft 설정</title>
<style>
body {{ font-family: -apple-system, sans-serif; max-width: 560px; margin: 40px auto; color: #222; }}
label {{ display: block; margin: 18px 0 6px; font-weight: 600; }}
input[type=text] {{ width: 100%; padding: 8px; font-size: 15px; box-sizing: border-box; }}
.notice {{ padding: 10px 12px; margin: 14px 0; border-radius: 6px; }}
.notice.ok {{ background: #e8f6e8; }}
.notice.error {{ background: #fde8e8; }}
button {{ padding: 8px 18px; font-size: 15px; margin: 14px 8px 0 0; }}
.hint {{ color: #666; font-size: 13px; }}
</style></head><body>
<h2>Sky Backcraft 설정</h2>
{banner}
<form onsubmit="return save(this);">
<label>공개 도메인 (선택 — 자체 터널이 로컬 포트로 라우팅될 때)</label>
<input type="text" id="domain" value="{domain_value}" placeholder="예: skygptcodex.store">
<label>로컬 포트</label>
<input type="text" id="port" value="{port_value}">
<label>인증 모드</label>
<label style="font-weight:400"><input type="radio" name="auth" value="none" {auth_none_checked}> 인증 없음 (public no-auth)</label>
<label style="font-weight:400"><input type="radio" name="auth" value="oauth" {auth_oauth_checked}> OAuth <span class="hint">(서버 미구현 — 적용 시 오류)</span></label>
<button type="submit">저장</button>
<button type="button" onclick="apply()">적용 (docker compose up)</button>
</form>
<p class="hint" id="mcpUrl"></p>
<script>
function mcpUrl() {{
  const d = document.getElementById("domain").value.trim();
  const p = document.getElementById("port").value.trim() || "8130";
  document.getElementById("mcpUrl").textContent =
    d ? ("커넥터 URL: https://" + d + "/mcp") : ("커넥터 URL: http://localhost:" + p + "/mcp (로컬 전용)");
}}
function save(form) {{
  const body = new URLSearchParams(new FormData(form));
  fetch("/save", {{ method: "POST", body }}).then(r => r.text()).then(t => location.href = "/" + (t === "ok" ? "" : "?error=" + encodeURIComponent(t)));
  return false;
}}
function apply() {{
  const body = new URLSearchParams(new FormData(document.forms[0]));
  fetch("/apply", {{ method: "POST", body }}).then(r => r.text()).then(t => location.href = "/" + (t === "ok" ? "?notice=" + encodeURIComponent("docker compose up -d app 실행 완료") : "?error=" + encodeURIComponent(t)));
  return false;
}}
mcpUrl();
document.getElementById("domain").addEventListener("input", mcpUrl);
document.getElementById("port").addEventListener("input", mcpUrl);
</script>
</body></html>"#
    )
}

fn html_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn query_param(query: &str, key: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (key_part, value_part) = pair.split_once('=')?;
        (percent_decode(key_part) == key).then(|| percent_decode(value_part))
    })
}

async fn page(State(state): State<GuiState>, raw_query: axum::extract::RawQuery) -> String {
    let config = GuiConfig::load(&state.config_path);
    let query = raw_query.0.unwrap_or_default();
    let error = query_param(&query, "error");
    let notice = query_param(&query, "notice");
    let (message, kind): (String, &str) = match (error, notice) {
        (Some(error), _) => (error, "error"),
        (None, Some(notice)) => (notice, "ok"),
        _ => (String::new(), "ok"),
    };
    render_page(&config, &message, kind)
}

async fn save(State(state): State<GuiState>, body: String) -> (StatusCode, &'static str) {
    let fields = parse_form(&body);
    let config = GuiConfig {
        domain: fields.get("domain").cloned().unwrap_or_default(),
        port: fields
            .get("port")
            .and_then(|port| port.parse().ok())
            .unwrap_or(DEFAULT_APP_PORT),
        auth_mode: fields.get("auth").cloned().unwrap_or_else(|| "none".into()),
    };
    if let Err(message) = config.validate() {
        return (StatusCode::BAD_REQUEST, Box::leak(message.into_boxed_str()));
    }
    if let Err(error) = config.store(&state.config_path) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Box::leak(error.to_string().into_boxed_str()),
        );
    }
    (StatusCode::OK, "ok")
}

async fn apply(State(state): State<GuiState>, body: String) -> (StatusCode, &'static str) {
    let save_state = State(GuiState {
        config_path: state.config_path.clone(),
    });
    let (status, message) = save(save_state, body).await;
    if status != StatusCode::OK {
        return (status, message);
    }
    let config = GuiConfig::load(&state.config_path);
    if config.auth_mode == "oauth" {
        return (
            StatusCode::BAD_REQUEST,
            "OAuth 인증 모드는 아직 서버에 구현되지 않았습니다. '인증 없음'을 사용하세요.",
        );
    }
    if config.domain.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            "도메인이 비어 있으면 공개 노출이 없습니다. 로컬 전용이라면 '인증 없음'으로 저장만 하세요.",
        );
    }
    let output = std::process::Command::new("docker")
        .args(["compose", "up", "-d", "app", "--remove-orphans"])
        .env("PUBLIC_DOMAIN", &config.domain)
        .env("SPOT_LAB_HOST_PORT", config.port.to_string())
        .output();
    match output {
        Ok(output) if output.status.success() => (StatusCode::OK, "ok"),
        Ok(output) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Box::leak(
                format!(
                    "docker compose 실패: {}",
                    String::from_utf8_lossy(&output.stderr)
                        .chars()
                        .take(400)
                        .collect::<String>()
                )
                .into_boxed_str(),
            ),
        ),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Box::leak(format!("docker 실행 실패: {error}").into_boxed_str()),
        ),
    }
}

/// Serve the localhost-only setup GUI.
///
/// # Errors
/// Reports bind failures.
pub fn serve(listen: std::net::SocketAddr, config_path: PathBuf) -> Result<(), LabError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| LabError::Internal(format!("gui runtime: {error}")))?;
    runtime.block_on(async move {
        let app = Router::new()
            .route("/", get(page))
            .route("/save", post(save))
            .route("/apply", post(apply))
            .with_state(GuiState { config_path });
        let listener = tokio::net::TcpListener::bind(listen)
            .await
            .map_err(|error| LabError::Internal(format!("gui bind {listen}: {error}")))?;
        tracing::info!(event = "gui_listening", %listen);
        axum::serve(listener, app)
            .await
            .map_err(|error| LabError::Internal(format!("gui serve: {error}")))
    })
}

/// Body parser smoke test coverage lives with the form contract.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn form_parse_decodes_fields() {
        let fields = parse_form("domain=skygptcodex.store&port=9000&auth=none");
        assert_eq!(
            fields.get("domain").map(String::as_str),
            Some("skygptcodex.store")
        );
        assert_eq!(fields.get("port").map(String::as_str), Some("9000"));
        assert_eq!(fields.get("auth").map(String::as_str), Some("none"));
    }

    #[test]
    fn percent_decode_handles_plus_and_hex() {
        assert_eq!(percent_decode("a+b%21"), "a b!");
    }

    #[test]
    fn config_validation_rejects_bad_inputs() {
        let mut config = GuiConfig::default();
        assert!(config.validate().is_ok());
        config.auth_mode = "basic".into();
        assert!(config.validate().is_err());
        config.auth_mode = "oauth".into();
        config.domain = "https://x.store/mcp".into();
        assert!(config.validate().is_err());
    }
}
