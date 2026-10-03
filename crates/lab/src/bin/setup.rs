//! Sky Backcraft 설정 앱 진입점: localhost 설정 GUI를 띄우고 브라우저를 연다.
//!
//! 설정 파일은 실행 파일 옆(`Contents/MacOS/` 안)에 저장되어 앱 번들이
//! 자기 완결적이다. `--config`로 다른 위치를 지정할 수도 있다.

use std::path::{Path, PathBuf};

const LISTEN: std::net::SocketAddr =
    std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 9090);

fn default_config_path() -> PathBuf {
    let exe_dir = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(Path::to_path_buf));
    let base =
        exe_dir.unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
    base.join(spot_lab::gui::DEFAULT_CONFIG_FILE)
}

fn parse_args() -> Result<PathBuf, String> {
    let mut config_path = default_config_path();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                let Some(path) = args.next() else {
                    return Err("--config 옵션에는 경로가 필요합니다".to_owned());
                };
                config_path = PathBuf::from(path);
            }
            "--help" | "-h" => {
                println!("sky-backcraft-setup [--config <path>]");
                println!("  localhost 설정 GUI(기본 {LISTEN})를 띄우고 브라우저로 연다.");
                std::process::exit(0);
            }
            other => {
                return Err(format!("unknown argument: {other}"));
            }
        }
    }
    Ok(config_path)
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
    let config_path = match parse_args() {
        Ok(path) => path,
        Err(message) => {
            eprintln!("sky-backcraft-setup: {message}");
            std::process::exit(2);
        }
    };
    // The browser must find the listener already bound; open after a short
    // settle delay instead of racing the bind.
    std::thread::spawn(|| {
        std::thread::sleep(std::time::Duration::from_millis(800));
        open_browser(&format!("http://{LISTEN}/"));
    });
    if let Err(error) = spot_lab::gui::serve(LISTEN, config_path, false) {
        eprintln!("설정 GUI 실패: {error}");
        std::process::exit(1);
    }
}
