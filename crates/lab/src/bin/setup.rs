//! Sky Backcraft 설정 앱 진입점: localhost 설정 GUI를 띄우고 브라우저를 연다.
//!
//! 설정 파일은 실행 파일 옆(`Contents/MacOS/` 안)에 저장되어 앱 번들이
//! 자기 완결적이다. `--config`로 다른 위치를 지정할 수도 있다.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};

const BASE_PORT: u16 = 9090;
const PORT_ATTEMPTS: u16 = 10;

fn default_config_path() -> PathBuf {
    let exe_dir = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(Path::to_path_buf));
    let base =
        exe_dir.unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
    base.join(spot_lab::gui::DEFAULT_CONFIG_FILE)
}

/// 9090부터 순서대로 비어 있는 포트를 찾는다. 이전 인스턴스가 살아 있어도
/// 다음 포트로 GUI가 뜨게 한다.
fn find_free_listen() -> SocketAddr {
    for offset in 0..PORT_ATTEMPTS {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), BASE_PORT + offset);
        if std::net::TcpListener::bind(addr).is_ok() {
            return addr;
        }
    }
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), BASE_PORT)
}

fn parse_args() -> Result<(SocketAddr, PathBuf), String> {
    let mut config_path = default_config_path();
    let mut listen = find_free_listen();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                let Some(path) = args.next() else {
                    return Err("--config 옵션에는 경로가 필요합니다".to_owned());
                };
                config_path = PathBuf::from(path);
            }
            "--port" => {
                let Some(port) = args.next() else {
                    return Err("--port 옵션에는 포트가 필요합니다".to_owned());
                };
                let port: u16 = port
                    .parse()
                    .map_err(|_| format!("--port: 숫자여야 합니다: {port}"))?;
                listen.set_port(port);
            }
            "--help" | "-h" => {
                println!("sky-backcraft-setup [--config <path>] [--port <port>]");
                println!("  localhost 설정 GUI를 띄우고 브라우저로 연다.");
                std::process::exit(0);
            }
            other => {
                return Err(format!("unknown argument: {other}"));
            }
        }
    }
    Ok((listen, config_path))
}

fn open_browser(url: &str) {
    #[cfg(target_os = "macos")]
    {
        let _ignored = std::process::Command::new("open").arg(url).spawn();
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        let _ignored = std::process::Command::new("xdg-open").arg(url).spawn();
    }
    #[cfg(not(unix))]
    {
        let _ = url;
    }
}

fn main() {
    let (listen, config_path) = match parse_args() {
        Ok(result) => result,
        Err(message) => {
            eprintln!("sky-backcraft-setup: {message}");
            std::process::exit(2);
        }
    };
    println!("Sky Backcraft 설정 GUI: http://{listen}/  (브라우저가 열립니다)");
    // The browser must find the listener already bound; open after a short
    // settle delay instead of racing the bind.
    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(800));
        open_browser(&format!("http://{listen}/"));
    });
    if let Err(error) = spot_lab::gui::serve(listen, config_path, false) {
        eprintln!("설정 GUI 실패: {error}");
        std::process::exit(1);
    }
}
