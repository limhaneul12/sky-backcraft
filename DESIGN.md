# Design

## Source of truth

- **Status:** Active
- **Last refreshed:** 2026-10-04
- **Primary product surfaces:** macOS menu bar menu and native settings window.
- **Mode:** Operate. This is a macOS utility, not a web surface.
- **Evidence reviewed:** [product scope](PRODUCT.md), [settings window](macos/SettingsWindowController.swift), [menu and application lifecycle](macos/AppDelegate.swift), [configuration and authentication modes](macos/Config.swift), and [runtime states](macos/RuntimeController.swift).
- **Screenshot confirmation:** The final local native capture confirms the rendered settings hierarchy, populated defaults, focused port field, no-auth selection, green running status, and trailing Cancel/Save buttons. Source code remains authoritative for behavior and states not visible in that capture.

## Brand

- **Personality:** quiet, direct, and operational.
- **Trust signals:** native AppKit controls, explicit status words, familiar macOS menu placement, and visible validation errors.
- **Avoid:** custom visual themes, decorative imagery, marketing copy, web-style navigation, and a project-folder chooser.

## Product goals

- Let an operator configure and run the local MCP server without a terminal.
- Keep setup to three values: optional public domain, local port, and `OAuth` or `인증 없음`.
- Make runtime state and the actions currently available clear from the menu and settings window.
- Treat a real MCP request and ChatGPT invocation as product acceptance evidence outside this visual contract; the UI must not imply either succeeded without runtime evidence.

## Personas and jobs

- **Primary persona:** the operator who runs Sky Backcraft locally and connects it to ChatGPT.
- **Jobs:** configure the public endpoint, choose authentication, start or stop MCP, copy the active MCP URL or OAuth login code, inspect logs, and reopen settings.
- **Context:** a compact menu bar workflow used alongside other macOS applications.

## Information architecture

- **Primary navigation:** one square menu bar item using the `server.rack` system symbol with accessibility description `Sky Backcraft`.
- **Menu order:** status; Start, Stop, Restart; Copy MCP URL, Copy OAuth Login Code; View Logs; Settings; Quit.
- **Settings hierarchy:** `MCP 연결 설정` heading; domain, port, and authentication rows; tunnel guidance; runtime status; Cancel and Save/Restart actions.
- There are no routes, tabs, sidebars, onboarding pages, or project-folder selection.

## Design principles

1. **Use macOS conventions.** Prefer AppKit system fonts, colors, controls, menus, focus indicators, alerts, and keyboard equivalents.
2. **State the system state in words.** Color reinforces status but never carries it alone.
3. **Keep the operator path compact.** Put common runtime actions in the menu and configuration in one window.
4. **Expose only valid actions.** Enable Start, Stop, Restart, URL copy, and OAuth-code copy according to runtime state and available values.

## Visual language

- **Color:** use semantic AppKit colors. Labels and guidance use `secondaryLabelColor`; running uses `systemGreen`; failure uses `systemRed`; other states use `secondaryLabelColor`.
- **Typography:** AppKit system font. The settings heading is 22 pt semibold; tunnel guidance is 12 pt; controls, labels, buttons, menu items, and status use native defaults.
- **Window and spacing:** 520 × 330 content area; 32 pt horizontal margins; 28 pt top margin; 20 pt vertical stack spacing; 16 pt grid row and column spacing; 10 pt button spacing. The value column is 300 pt wide.
- **Alignment:** field labels trail toward the controls; fields fill the value column; the footer pushes `취소` and `저장 및 재시작` to the right.
- **Shape and elevation:** inherit standard AppKit control geometry, borders, focus rings, menus, and window chrome. The project owns no custom radius or shadow token.
- **Motion:** no custom animation. Use platform window, menu, focus, and alert behavior.
- **Iconography:** use the SF Symbol `server.rack` for the status item; no decorative image system is defined.

## Components

| Component | Contract |
| --- | --- |
| Domain field | Optional host name. Placeholder explains that an empty value uses a Quick Tunnel. Input is trimmed, lowercased, and validated as a host name. |
| Port field | Integer-only value from 1 through 65535; default and placeholder are `8130`. |
| Authentication popup | Exactly two choices: `OAuth` and `인증 없음`. |
| Guidance text | Explains that a domain uses the existing Cloudflare Tunnel and an empty domain creates a temporary Quick Tunnel. |
| Runtime status | Uses the localized strings `MCP: 중지됨`, `MCP: 시작 중…`, `MCP: 중지 중…`, `MCP: 실행 중 (<host>)`, or `MCP 오류: <message>`. |
| Footer actions | `취소` closes without saving. `저장 및 재시작` validates, saves atomically, restarts the owned runtime, and is the Return-key default action. |
| Menu actions | Use native `NSMenuItem` state and keyboard equivalents. Settings uses Command-,; Restart uses Command-R; Quit uses Command-Q. |

AppKit owns the visual components. Do not introduce a parallel token or component layer for this surface.

## Authentication state

- **No authentication:** the runtime passes `--public-no-auth`, clears any OAuth owner code, uses the isolated `public-research` data root, and disables OAuth-code copying.
- **OAuth:** the runtime passes `--oauth` with the active public URL, generates an in-memory owner code for the running server, uses the isolated `oauth-research` data root, and enables code copying only when a code exists.
- Changing either mode takes effect through Save/Restart. Restarting replaces the running server state.

## Lifecycle and interaction states

| State | Visible status | Enabled menu actions |
| --- | --- | --- |
| Stopped | `MCP: 중지됨` in secondary color | Start |
| Starting | `MCP: 시작 중…` in secondary color | Stop |
| Running | `MCP: 실행 중 (<host>)` in green | Stop, Restart, URL copy when a URL exists, OAuth-code copy when a code exists |
| Stopping | `MCP: 중지 중…` in secondary color | None of the runtime or copy actions |
| Failed | `MCP 오류: <message>` in red | Start |
| Blocked shutdown | `MCP 종료 오류: <message>` in red | No new runtime actions until the owned children exit |

- First launch opens settings. Later launches load saved settings and start the runtime.
- Reopening the accessory app shows and centers settings.
- A configured domain starts the backend for that host. An empty domain first obtains a Quick Tunnel host, then starts the backend.
- Invalid settings and load failures appear in native alerts. Startup failures become explicit failed status text.
- Quitting waits for the app-owned backend and tunnel processes to stop before termination.
- Every run has a generation. Output, timeout, and exit callbacks from a previous run cannot start or reset a new run.
- Shutdown has one supervisor. Repeated restart requests retain the latest configuration; Quit cancels pending restart.
- Tunnel URLs are accumulated across bounded pipe chunks. Child output is backpressured rather than retained in an unbounded callback queue.

## Accessibility

- **Target:** follow native macOS accessibility conventions; no separate conformance claim is recorded.
- **Keyboard:** preserve native field and popup interaction, the Return-key Save/Restart action, and the defined menu equivalents.
- **Focus:** keep standard AppKit focus rings and control order. The final screenshot confirms the native blue focus treatment on the port field.
- **Readability:** use system typography and semantic system colors; always include `MCP:` plus an explicit localized state or error message.
- **Screen readers:** keep visible static labels and the status-item accessibility description. VoiceOver behavior has not been independently verified.
- **Reduced motion:** no custom motion exists.

## Responsive behavior

- **Supported device:** macOS desktop.
- The settings window is a fixed, titled, closable 520 × 330 AppKit content area with no responsive breakpoints or touch-specific behavior.
- System font rendering, display scale, appearance, and control metrics remain platform-owned.

## Content voice

- Use concise Korean operator language in the interface.
- Name the service consistently as `MCP`, authentication as `OAuth` or `인증 없음`, and the temporary tunnel as `Quick Tunnel`.
- Use direct verbs for actions: 시작, 중지, 재시작, 복사, 보기, 설정, 저장, 종료.
- Error copy must say what is invalid or unavailable and, where useful, what the operator can correct.

## Implementation constraints

- **Framework:** Swift with AppKit and system frameworks; no web view or third-party GUI framework.
- **Ownership:** the application owns its settings, logs, application data, backend process, and tunnel process.
- **Persistence:** settings live outside the app bundle and contain only domain, port, and authentication mode. There is no project-folder setting.
- **Compatibility:** retain the menu bar accessory activation policy and macOS-native application menu.
- **Verification:** source review establishes the contracts above; the final local screenshot confirms the rendered settings state described under Source of truth. Runtime, MCP, OAuth, and ChatGPT acceptance require separate fresh evidence.

## Open questions

- None for the implemented settings and menu surfaces. Add an open question here before expanding the product beyond those surfaces.
