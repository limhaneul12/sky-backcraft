#!/usr/bin/env python3
"""Build a private, self-contained Codex marketplace from Backcraft workflows."""

import argparse
import hashlib
import json
import re
import os
import shutil
import tempfile
from pathlib import Path
from urllib.parse import urlsplit
from zipfile import ZIP_DEFLATED, ZipFile

from dist_outputs import CANONICAL_DIST, PLUGIN_NAME, publish_outputs, validate_output_directory


ROOT = Path(__file__).resolve().parents[1]
SKILLS = (
    "backcraft-experiment",
    "backcraft-research",
    "backcraft-results",
    "backcraft-maintenance",
)
LINK = re.compile(r"\[[^\]]*\]\(([^)]+)\)")


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mcp-url", required=True, help="Existing OAuth-protected MCP URL")
    parser.add_argument("--output", type=Path, default=ROOT / "dist/SkyBackcraft-plugin-marketplace.zip")
    parser.add_argument("--evidence", type=Path, default=ROOT / ".omx/evidence/plugin-package")
    args = parser.parse_args()
    if args.output.is_symlink():
        parser.error("Plugin output cannot be a symlink")
    destination = args.output.resolve()
    if destination.is_relative_to(CANONICAL_DIST):
        if destination != CANONICAL_DIST / PLUGIN_NAME:
            parser.error("Write the latest plugin directly to dist/SkyBackcraft-plugin-marketplace.zip")
        validate_output_directory(args.output.parent)
    if args.evidence.resolve().is_relative_to(CANONICAL_DIST):
        parser.error("Keep packaging evidence outside dist")
    parsed = urlsplit(args.mcp_url)
    if (parsed.scheme != "https" or not parsed.hostname or parsed.username
            or parsed.password or parsed.query or parsed.fragment):
        parser.error("Use the existing HTTPS MCP URL without credentials/query/fragment")

    marketplace = ROOT / "plugin-marketplace"
    plugin = marketplace / "plugins/sky-backcraft"
    outputs = set()
    source_hashes = {}

    def write(path, body):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body)
        outputs.add(path)

    def json_output(path, value):
        write_json(path, value)
        outputs.add(path)

    references = {ROOT / "docs/live-trading-mcp.md"}
    for name in SKILLS:
        source = ROOT / ".agents/skills" / name / "SKILL.md"
        body = source.read_text()
        source_hashes[str(source.relative_to(ROOT))] = hashlib.sha256(source.read_bytes()).hexdigest()
        for link in LINK.findall(body):
            if link.startswith("../../../"):
                target = (source.parent / link.split("#", 1)[0]).resolve()
                if not target.is_relative_to(ROOT / "docs") and target != ROOT / "scripts/download-export.py":
                    raise ValueError(f"Unexpected support-file boundary: {link}")
                references.add(target)
        body = body.replace("../../../docs/", "../../references/docs/")
        body = body.replace("../../../scripts/", "../../references/scripts/")
        write(plugin / "skills" / name / "SKILL.md", body)
        ui = source.parent / "agents/openai.yaml"
        if ui.is_file():
            write(plugin / "skills" / name / "agents/openai.yaml", ui.read_text())

    # Include only the operational reference closure, never Harnesses or runtime data.
    pending = list(references)
    seen = set()
    while pending:
        source = pending.pop()
        if source in seen:
            continue
        if not source.is_file() or source.is_symlink():
            raise ValueError(f"Missing/non-regular packaged reference: {source}")
        if len(seen) >= 160 or source.stat().st_size > 2_000_000:
            raise ValueError("Operational reference closure exceeds package bounds")
        seen.add(source)
        body = source.read_text()
        relative = source.relative_to(ROOT)
        write(plugin / "references" / relative, body)
        source_hashes[str(relative)] = hashlib.sha256(source.read_bytes()).hexdigest()
        if source.suffix == ".md":
            for link in LINK.findall(body):
                if "://" in link or link.startswith("#"):
                    continue
                target = (source.parent / link.split("#", 1)[0]).resolve()
                if not target.is_relative_to(ROOT / "docs"):
                    raise ValueError(f"Operational documentation leaves docs boundary: {link}")
                pending.append(target)

    interface = {
        "displayName": "Sky Backcraft",
        "shortDescription": "전략 연구·실거래 제어·복기",
        "longDescription": "전략 연구, 백테스트, 실거래 제어, 체결 원장과 복기를 Sky Backcraft MCP에 연결합니다. 거래 OAuth 소유자 승인 시 거래 권한과 주문 전송 허용을 적용합니다.",
        "developerName": "Sky Backcraft",
        "category": "Productivity",
        "defaultPrompt": "Sky Backcraft 연결 상태를 확인하고 요청한 전략 연구 또는 결과 분석을 진행해 주세요.",
    }
    assert len(interface["shortDescription"]) <= 30
    json_output(plugin / "plugin.json", {
        "$schema": "https://agent-plugins.org/schemas/1.0.0/plugin.schema.json",
        "name": "sky-backcraft", "version": "0.1.7",
        "description": "Sky Backcraft skills and existing authenticated MCP connection.",
        "author": {"name": "Sky Backcraft"},
        "extensions": {"com.openai": {"interface": interface}},
    })
    json_output(plugin / "mcp.json", {
        "$schema": "https://agent-plugins.org/schemas/1.0.0/mcp.schema.json",
        "mcpServers": {"sky-backcraft": {
            "type": "streamable-http", "url": args.mcp_url,
        }},
    })
    # Older supported Codex clients use this compatibility overlay.
    json_output(plugin / ".codex-plugin/plugin.json", {
        "name": "sky-backcraft", "version": "0.1.7",
        "description": "Sky Backcraft skills and existing authenticated MCP connection.",
        "author": {"name": "Sky Backcraft"},
        "skills": "./skills/", "mcpServers": "./.mcp.json", "interface": interface,
    })
    json_output(plugin / ".mcp.json", {
        "mcpServers": {"sky-backcraft": {"url": args.mcp_url}},
    })
    write(plugin / "skills/backcraft-connect/SKILL.md", """---
name: backcraft-connect
description: Sky Backcraft 플러그인의 MCP 연결과 실제 제공 기능을 확인한다. 연결 오류, 인증, 모의와 실거래 구분에 사용한다.
---

# Sky Backcraft 연결 확인

설치된 플러그인의 실제 MCP 도구와 스키마를 조회한 뒤 `lab_status`를 읽는다.
OAuth 로그인은 호스트의 연결 화면을 사용한다. API 키, 토큰, 인증 파일을
채팅이나 플러그인 파일에 입력·저장하지 않는다. 인증 실패는 인증 실패로 보고하고
인증을 끄거나 public/desktop endpoint로 바꾸어 우회하지 않는다.
`lab_status.scope.private_api`로 이 연결의 권한을 확인한다. 서버의
`live_trading_enabled=true`는 이 연결에 거래 권한이 있다는 뜻이 아니다.
`SCOPED_OAUTH_TRADING_READ_AND_CONTROL`이면 거래 제어 권한까지 승인된 연결이고,
`SCOPED_OAUTH_TRADING_READ`는 조회만, `UNAVAILABLE_ON_THIS_EXPOSURE`는 거래 접근 불가다.
부족한 권한의 OAuth 도구가 재승인 challenge를 반환하면 호스트의 연결 화면에서
`mcp trading:read trading:control`을 명시적으로 요청하고 소유자 승인을 받는다.
기존 연구 토큰의 refresh만으로 거래 권한이 생기지 않는다. 다른 채팅·호스트의
연결 성공을 현재 ChatGPT 연결의 승인으로 간주하지 않는다.
`unauthorized_client`는 토큰 권한 부족과 다른 클라이언트 등록 오류다.
운영자에게 해당 ChatGPT 앱의 공개 client_id·정확한 callback 등록 복구를 요청한다.
알 수 없는 ID를 자동 등록하거나 Desktop 토큰으로 대체하지 않는다.
GUI의 전송 ON/OFF는 즉시 적용되어 별도 설정 저장이 필요 없다. 실제 네트워크·인증
설정을 바꿔 재시작하면 새 거래 OAuth 승인 전까지 전송은 OFF다. 새 승인을 완료하면
거래 권한과 전송 허용이 함께 적용된다. 토큰의 자동 갱신은 사용자가 끈 전송을 다시 켜지 않는다.

도구 이름이 목록에 있는 것과 호출 권한/실거래 실행 가능성은 다르다.
일반 MCP 연구 경로와 네이티브 앱의 private Upbit 경로를 구분한다.
MOCK/모의 응답을 실제 계좌·주문으로 보고하지 않는다. 운영자의 `--trading-mcp --live-order-control` 설정과 공유 전송 게이트가
연결된 서버는 Codex·ChatGPT 모두 새 OAuth 소유자 승인 한 번으로
`mcp trading:read trading:control`과 실주문 전송 허용을 함께 적용한다. 클라이언트가
`mcp`만 요청해도 승인 화면과 실제 발급 토큰에는 전체 거래 권한이 명시된다.
소유자 코드·PKCE 검증이 끝난 새 로그인에서만 서버가 같은 `live_order_control`
게이트를 연다. 추가 ON 호출은 필요 없다. 금융 MCP 설정이 꺼진 OAuth는 연구 전용이다.
설치나 로그인 준비 자체로 권한을 부여하지 않으며, 소유자가 금융·전송 허용이 표시된
승인 화면에서 직접 승인한다. 기존 연구 토큰의 권한을 임의로 바꾸지 않는다.

실주문 ON/OFF를 명시적으로 요청받았을 때만 `live_order_control`을 사용한다.
먼저 status에서 실제 모드·기능 가용성·boot_epoch·sequence·현재 ON/OFF와 별도
주문 승인 상태를 읽는다. enable에는 현재 expected_epoch/expected_sequence,
새 request_id와 정확한 confirmation `ENABLE_LIVE_ORDER_SENDING`이 필요하다.
disable도 현재 identity/sequence를 사용하고 typed receipt 후 새 status로 확인한다.
응답 손실은 같은 request_id의 request 조회로 확인하며, 과거 부팅의 ON receipt를
재실행하지 않는다. unknown·timeout·실패를 OFF 또는 ON 성공으로 보고하지 않는다.
ON은 세션 시작·재개나 긴급 정지 해제가 아니다. OFF는 실거래의 미전송 판단·대기 요청을 현지 거부하고
해당 예약을 풀지만, 이미 보낸 주문의 취소·보유 자산 청산을 하지 않는다.
운영 검증은 읽기만 하고, ON/OFF와 체결 검증은 명시된 격리 MOCK에서 수행한다.

전략 시작 요청은 정확한 policy_ref(정책/리비전/digest), market, decision_interval,
명시적 MOCK/LIVE mode와 risk 전체를 넣는다. 주문당·하루 주문액·최대 자산 노출·
손실 한도 및 주문 수·신선도 한도를 사용자 의도에 맞게 정하고, 숫자를 임의 추천하거나
누락 기본값으로 채우지 않는다. get 응답의 동결 정책과 risk를 대조한다.

권한이 있는 경우 `automation_session_write`의 start/pause/resume/stop과
`trading_control`을 사용한다. 일시중단·정지는 신규 주문을 막으며 자동 매도나
기존 주문 취소를 뜻하지 않는다. `request_id`와 sequence를 유지하고 응답이
불명확하면 `automation_session_query`로 durable readback한다. 원시 주문
execute/test/cancel 및 수동 기준 변경은 OAuth 제어 권한에 포함되지 않는다.

네이티브 앱의 전략 자동화 화면에서도 MOCK/LIVE를 명시적으로 선택하고 같은
전략 시작·일시중단·재개·정지를 사용할 수 있다. 새 세션의 기본 모드는 MOCK이다.
LIVE 시작·재개는 선택한 동결 전략과 위험 한도를 확인한 뒤 제출하며 전송 스위치를
자동으로 바꾸지 않는다. 버튼 활성 여부는 최신 서버 capabilities를 따른다.

실제 기록은 `upbit_orders_closed`, `upbit_order_get` 및 세션별 원장의
출처·모드를 확인한다. 손실 한도는 `effective_loss`의 기준을 확인하며,
`activation_loss_limit_krw`는 자정 기준 대신 명시적으로 선택하는 시작 이후
누적 손실 한도다. 재시작·재개·새 세션으로 기준을 초기화하지 않는다.
자세한 계약은 [거래 제어·내역 안내](../../references/docs/live-trading-mcp.md)를 읽는다.

복기는 `automation_session_query`의 decisions/decision/performance를 사용한다.
원본 캔들·지표·이유·정책 추적은 저장된 그대로 읽고 현재 시세로 재계산하지 않는다.
실거래 CSV·성과 파일은 private export_manifest/export_read를 사용해 snapshot과
파일·청크 SHA-256을 검증한 후 수신한다. 이 파일을 연구 artifact_query로 찾거나
모의 결과를 실거래 성과로 표시하지 않는다. 미확인 초기 보유 원가·체결·평가는
null/사유를 보존하며, 현금 유출입을 손익 또는 계좌 자본을 전략 자본으로 바꾸지 않는다.

`paper_session_query`가 실제 제공되면 list/get/start_options 스키마와 상태를
읽는다. 과거 run 또는 paused collector를 활성 paper로 표시하지 않는다.
세션 생성/제어는 사용자가 요청한 범위와 실제 `paper_session_write` 스키마를
따른다. request identity/sequence와 결과 불명 시 durable readback을 지킨다.

단순 실험은 ../backcraft-experiment/SKILL.md, 스윕·워크포워드는
../backcraft-research/SKILL.md, 결과는 ../backcraft-results/SKILL.md,
저장 관리는 ../backcraft-maintenance/SKILL.md를 읽는다.
""")
    json_output(marketplace / ".agents/plugins/marketplace.json", {
        "name": "sky-backcraft-local",
        "interface": {"displayName": "Sky Backcraft Local"},
        "plugins": [{
            "name": "sky-backcraft",
            "source": {"source": "local", "path": "./plugins/sky-backcraft"},
            "policy": {"installation": "AVAILABLE", "authentication": "ON_INSTALL"},
            "category": "Productivity",
        }],
    })
    write(marketplace / "README.md", f"""# Sky Backcraft local plugin marketplace

스킬 4개 + 연결 안내 스킬 1개 + 기존 OAuth MCP를 한 플러그인으로 묶었습니다.
개인 로컬 패키지입니다. 비공개 운영 자료가 포함되므로 공개 업로드/배포하지 않습니다.
API 키, .env, 사용자 DB, 원장, private Harness, 자동 실행 hook은 포함하지 않습니다.

Codex 공식 CLI로 마켓플레이스를 등록하고 플러그인을 설치합니다:

```sh
codex plugin marketplace add '{marketplace}'
codex plugin add sky-backcraft@sky-backcraft-local
codex plugin list --available --marketplace sky-backcraft-local --json
```

Git ref와 Sparse 경로는 로컬 폴더에 필요 없습니다. 'Sky Backcraft Local'에서
Sky Backcraft를 설치하면 스킬과 MCP 설정이 함께 등록됩니다. 연결 시 기존
OAuth 인증은 필요합니다. 백엔드는 기존 macOS 앱이 실행하는 서버를 사용합니다.
플러그인이 별도 DB owner/server를 시작하지 않습니다.

구성된 MCP: `{args.mcp_url}`. OAuth는 서버의 discovery metadata로 협상합니다.
운영자 승인 코드는 앱 메뉴의 'OAuth 로그인 코드 복사'를 사용하고,
승인 페이지에만 입력합니다. 키·코드·토큰을 채팅이나 패키지에 저장하지 않습니다.
호스트 설치, OAuth 완료, 인증된 도구 호출은 각각 검증해야 합니다.
패키지 생성 영수증은 파일 검증만 증명하며, 실제 연결 결과는 별도 검증 영수증을 확인합니다.

금융 MCP 설정이 켜진 서버에서 새 OAuth 로그인을 승인하면 Codex·ChatGPT 모두
계좌 조회·전략 제어 권한과 실주문 전송 허용이 함께 적용됩니다. 승인 화면에 전체
거래 권한과 전송 허용을 표시합니다. 새 서버는 승인 전까지 OFF이고, 추가 ON 조작은
필요 없습니다. 금융 MCP가 꺼진 서버는 연구 전용입니다. 패키지 설치만으로 로그인이나
주문을 시작하지 않습니다.
시작·일시중단·재개·정지, 실제 체결 기록과 손실 기준의 계약은
[거래 MCP 안내](plugins/sky-backcraft/references/docs/live-trading-mcp.md)를 참고하세요.
패키지 생성은 실행 중 서버 교체, OAuth 권한 추가 또는 실거래 활성화를 뜻하지 않습니다.

재생성: `python3 scripts/package-plugin.py --mcp-url {args.mcp_url}`
기본 산출물은 `dist/SkyBackcraft-plugin-marketplace.zip`이며 다음 생성 때 교체합니다.
""")
    for path in sorted(outputs):
        if path.suffix == ".json":
            json.loads(path.read_text())
        if path.name == "SKILL.md":
            body = path.read_text()
            assert body.startswith("---\n") and "\ndescription:" in body
            for link in LINK.findall(body):
                if "://" not in link and not link.startswith("#"):
                    target = (path.parent / link.split("#", 1)[0]).resolve()
                    if not target.is_relative_to(plugin) or not target.is_file():
                        raise ValueError(f"Broken skill reference: {path}: {link}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    # Close and CRC-check the archive before replacing the last good package.
    with tempfile.TemporaryDirectory(prefix="sky-backcraft-plugin-") as temporary:
        staged = Path(temporary) / PLUGIN_NAME
        with ZipFile(staged, "w", ZIP_DEFLATED) as archive:
            for path in sorted(outputs):
                archive.write(path, Path(marketplace.name) / path.relative_to(marketplace))
        with ZipFile(staged) as archive:
            if archive.testzip() is not None:
                raise ValueError("Plugin ZIP CRC validation failed")
        if destination.parent == CANONICAL_DIST:
            publish_outputs(destination.parent, {PLUGIN_NAME: staged})
        else:
            # Stage on the destination filesystem for an atomic replacement.
            with tempfile.TemporaryDirectory(prefix=".sky-backcraft-plugin-", dir=destination.parent) as publication:
                ready = Path(publication) / destination.name
                shutil.copy2(staged, ready)
                os.replace(ready, destination)
    evidence = args.evidence.resolve()
    write_json(evidence / "package-receipt.json", {
        "archive": str(destination), "sha256": hashlib.sha256(destination.read_bytes()).hexdigest(),
        "marketplace_root": str(marketplace), "skill_count": 5,
        "source_hashes": source_hashes,
        "packaged_files": [str(p.relative_to(marketplace)) for p in sorted(outputs)],
        "validation": "JSON/frontmatter/contained skill-link closure PASS",
        "host_install_and_oauth": "NOT_RUN", "real_exchange_effects": "NOT_RUN",
    })
    print(json.dumps({"archive": str(destination), "files": len(outputs), "skills": 5}))


if __name__ == "__main__":
    main()
