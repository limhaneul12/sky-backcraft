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
use std::io::BufRead;
use std::path::PathBuf;

pub const DEFAULT_LISTEN: &str = "127.0.0.1:9090";
pub const DEFAULT_CONFIG_FILE: &str = "spot-lab-gui.json";
pub(crate) const DEFAULT_APP_PORT: u16 = 8130;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GuiConfig {
    /// Operator-owned public domain routed to the local port (empty = quick
    /// tunnel with a free `*.trycloudflare.com` URL).
    #[serde(default)]
    pub domain: String,
    /// Local MCP port for the app container/native process.
    #[serde(default = "default_app_port")]
    pub port: u16,
    /// `none` (public no-auth) or `oauth` (token auth with a simple page).
    #[serde(default)]
    pub auth_mode: String,
    /// Bearer token for the oauth mode; generated on first start.
    #[serde(default)]
    pub auth_token: String,
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
            auth_token: String::new(),
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
        if self.auth_mode == "oauth" && self.auth_token.len() < 16 {
            return Err("oauth 모드에는 16자 이상 토큰이 필요합니다".to_owned());
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
    launcher: std::sync::Arc<std::sync::Mutex<LauncherState>>,
}

/// Background child processes started by the launcher.
#[derive(Debug, Clone, Default)]
struct LauncherState {
    mcp_pid: Option<u32>,
    tunnel_pid: Option<u32>,
    tunnel_url: Option<String>,
}

impl LauncherState {
    fn mcp_running(&self) -> bool {
        self.mcp_pid.is_some_and(|pid| {
            std::process::Command::new("kill")
                .args(["-0", &pid.to_string()])
                .output()
                .is_ok_and(|out| out.status.success())
        })
    }
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
<button type="button" onclick="apply()">MCP 서버 시작</button>
<button type="button" onclick="fetch('/stop', {{method:'POST'}}).then(()=>location.href='/')">중지</button>
</form>
<p class="hint" id="mcpUrl"></p>
<div id="status" class="hint">상태: 확인 중…</div>
<div id="tokenBox" style="display:none">
  <label>접속 토큰 (복사해서 커넥터 인증에 사용)</label>
  <input type="text" id="token" readonly>
  <button type="button" onclick="navigator.clipboard.writeText(document.getElementById('token').value)">복사</button>
</div>
<script>
function mcpUrl() {{
  const d = document.getElementById("domain").value.trim();
  const p = document.getElementById("port").value.trim() || "8130";
  document.getElementById("mcpUrl").textContent =
    d ? ("커넥터 URL: https://" + d + "/mcp") : ("커넥터 URL: (시작 후 Quick Tunnel URL 표시)");
}}
function save(form) {{
  const body = new URLSearchParams(new FormData(form));
  fetch("/save", {{ method: "POST", body }}).then(r => r.text()).then(t => location.href = "/" + (t === "ok" ? "" : "?error=" + encodeURIComponent(t)));
  return false;
}}
function apply() {{
  const body = new URLSearchParams(new FormData(document.forms[0]));
  fetch("/apply", {{ method: "POST", body }}).then(r => r.text()).then(t => location.href = "/" + (t === "ok" ? "?notice=" + encodeURIComponent("MCP 서버 시작됨 (백그라운드)") : "?error=" + encodeURIComponent(t)));
  return false;
}}
function poll() {{
  fetch("/status").then(r => r.json()).then(s => {{
    document.getElementById("status").textContent =
      "MCP: " + (s.mcp_running ? "실행 중" : "중지") +
      (s.tunnel_url ? " | 터널: " + s.tunnel_url : "");
    const show = s.auth_mode === "oauth" && s.auth_token;
    document.getElementById("tokenBox").style.display = show ? "block" : "none";
    if (show) document.getElementById("token").value = s.auth_token;
  }}).catch(() => {{}});
}}
mcpUrl();
poll();
setInterval(poll, 3000);
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
        auth_token: fields.get("auth_token").cloned().unwrap_or_default(),
    };
    // OAuth mode auto-generates its bearer token so the operator never has to
    // invent one; the value is stored with the config for stable restarts.
    let mut config = config;
    if config.auth_mode == "oauth" && config.auth_token.is_empty() {
        let digest = crate::contracts::ContentHash::of_bytes(
            format!(
                "gui-token:{}:{}:{}",
                config.port,
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |duration| duration.as_nanos())
            )
            .as_bytes(),
        );
        digest.as_str()[..32].clone_into(&mut config.auth_token);
    }
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
        launcher: state.launcher.clone(),
    });
    let (status, message) = save(save_state, body).await;
    if status != StatusCode::OK {
        return (status, message);
    }
    let config = GuiConfig::load(&state.config_path);
    match start_launcher(&state, &config) {
        Ok(()) => (StatusCode::OK, "ok"),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Box::leak(error.into_boxed_str()),
        ),
    }
}

/// Stop the background MCP server and quick tunnel.
async fn stop(State(state): State<GuiState>) -> (StatusCode, &'static str) {
    let Ok(mut launcher) = state.launcher.lock() else {
        return (StatusCode::INTERNAL_SERVER_ERROR, "launcher poisoned");
    };
    for pid in [launcher.mcp_pid, launcher.tunnel_pid]
        .into_iter()
        .flatten()
    {
        let _ignored = std::process::Command::new("kill")
            .arg(pid.to_string())
            .output();
    }
    launcher.mcp_pid = None;
    launcher.tunnel_pid = None;
    launcher.tunnel_url = None;
    (StatusCode::OK, "ok")
}

/// Launcher status for the page poller.
async fn status(State(state): State<GuiState>) -> (StatusCode, String) {
    let Some(launcher) = (state.launcher.lock().ok()).map(|guard| guard.clone()) else {
        return (StatusCode::INTERNAL_SERVER_ERROR, "{}".to_owned());
    };
    let config = GuiConfig::load(&state.config_path);
    let domain = if config.domain.is_empty() {
        launcher.tunnel_url.clone().unwrap_or_default()
    } else {
        format!("https://{}", config.domain)
    };
    let payload = serde_json::json!({
        "mcp_running": launcher.mcp_running(),
        "tunnel_url": launcher.tunnel_url,
        "connector_url": format!("{domain}/mcp"),
        "auth_mode": config.auth_mode,
        "auth_token": (config.auth_mode == "oauth").then(|| config.auth_token.clone()),
    });
    (
        StatusCode::OK,
        serde_json::to_string(&payload).unwrap_or_else(|_| "{}".to_owned()),
    )
}

/// Spawn the MCP server (and a free quick tunnel when no domain is set) as
/// background children of this GUI process.
fn start_launcher(state: &GuiState, config: &GuiConfig) -> Result<(), String> {
    let mut launcher = state
        .launcher
        .lock()
        .map_err(|_| "launcher state is poisoned".to_owned())?;
    if launcher.mcp_running() {
        return Ok(());
    }
    // Quick tunnel first: its issued host is required for --allow-host, and
    // the URL is captured from cloudflared's stderr by a reader thread.
    if config.domain.is_empty() {
        let mut tunnel = std::process::Command::new("cloudflared")
            .args([
                "tunnel",
                "--no-autoupdate",
                "--url",
                &format!("http://127.0.0.1:{}", config.port),
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|error| format!("cloudflared 실행 실패(설치 필요): {error}"))?;
        launcher.tunnel_pid = Some(tunnel.id());
        launcher.tunnel_url = None;
        if let Some(stderr) = tunnel.stderr.take() {
            let launcher_tracker = state.launcher.clone();
            std::thread::spawn(move || {
                let mut reader = std::io::BufReader::new(stderr);
                let mut line = String::new();
                while reader.read_line(&mut line).unwrap_or(0) > 0 {
                    if let Some(url) = line.split_whitespace().find(|part| {
                        part.starts_with("https://") && part.contains("trycloudflare.com")
                    }) {
                        if let Ok(mut guard) = launcher_tracker.lock() {
                            guard.tunnel_url = Some(url.trim_end_matches(',').to_owned());
                        }
                        break;
                    }
                    line.clear();
                }
            });
        }
    }
    let mut args = vec![
        "mcp-serve".to_owned(),
        "--bind".to_owned(),
        "127.0.0.1".to_owned(),
        "--port".to_owned(),
        config.port.to_string(),
        "--data-root".to_owned(),
        "data-gui".to_owned(),
        "--public-no-auth".to_owned(),
    ];
    if !config.domain.is_empty() {
        args.push("--allow-host".to_owned());
        args.push(config.domain.clone());
    }
    let mut token = config.auth_token.clone();
    if config.auth_mode == "oauth" {
        if token.is_empty() {
            let digest = crate::contracts::ContentHash::of_bytes(
                format!("{}:{}", config.port, std::process::id()).as_bytes(),
            );
            digest.as_str()[..32].clone_into(&mut token);
        }
        args.push("--auth-token".to_owned());
        args.push(token.clone());
    }
    let exe = std::env::current_exe().map_err(|error| format!("current_exe: {error}"))?;
    let mcp_child = std::process::Command::new(exe)
        .args(&args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .map_err(|error| format!("mcp-serve 실행 실패: {error}"))?;
    launcher.mcp_pid = Some(mcp_child.id());
    Ok(())
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
    let launcher = std::sync::Arc::new(std::sync::Mutex::new(LauncherState::default()));
    runtime.block_on(async move {
        let app = Router::new()
            .route("/", get(page))
            .route("/save", post(save))
            .route("/apply", post(apply))
            .route("/stop", post(stop))
            .route("/status", get(status))
            .with_state(GuiState {
                config_path,
                launcher: launcher.clone(),
            });
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
