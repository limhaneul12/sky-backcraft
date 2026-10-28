import AppKit

final class AppDelegate: NSObject, NSApplicationDelegate {
    private let store: ConfigurationStore
    private let runtime: RuntimeController
    private let settings: SettingsWindowController
    private let dashboard: DashboardWindowController
    private let statusItem = NSStatusBar.system.statusItem(withLength: NSStatusItem.squareLength)
    private let stateItem = NSMenuItem(title: "MCP: 중지됨", action: nil, keyEquivalent: "")
    private let dashboardItem = NSMenuItem(title: "퀀트 대시보드 열기…", action: #selector(openDashboard), keyEquivalent: "d")
    private let startItem = NSMenuItem(title: "MCP 시작", action: #selector(start), keyEquivalent: "")
    private let stopItem = NSMenuItem(title: "MCP 중지", action: #selector(stop), keyEquivalent: "")
    private let restartItem = NSMenuItem(title: "MCP 재시작", action: #selector(restart), keyEquivalent: "r")
    private let copyURLItem = NSMenuItem(title: "OAuth MCP URL 복사", action: #selector(copyURL), keyEquivalent: "")
    private let copyPublicURLItem = NSMenuItem(title: "무인증 MCP URL 복사", action: #selector(copyPublicURL), keyEquivalent: "")
    private let copyCodeItem = NSMenuItem(title: "OAuth 로그인 코드 복사", action: #selector(copyCode), keyEquivalent: "")
    private let isFirstLaunch: Bool

    init(store: ConfigurationStore, liveService: UpbitLiveService = .shared) {
        self.store = store
        isFirstLaunch = !FileManager.default.fileExists(atPath: store.settingsURL.path)
        runtime = RuntimeController(store: store)
        settings = SettingsWindowController(store: store, tradingService: liveService)
        dashboard = DashboardWindowController(store: store, liveService: liveService)
        super.init()

        // Wire bidirectional synchronization between Settings and Dashboard
        settings.onOpenDashboard = { [weak self] in
            self?.openDashboard()
        }
        settings.onOpenAutomationWorkspace = { [weak self] in
            self?.dashboard.showAutomationWorkspace()
        }
        dashboard.onOpenSettings = { [weak self] in self?.openSettings() }
        settings.onControlSnapshot = { [weak self] snapshot in self?.dashboard.updateControlSnapshot(snapshot) }
    }

    func applicationDidFinishLaunching(_ notification: Notification) {
        configureApplicationMenu()
        configureStatusItem()
        runtime.statusDidChange = { [weak self] status in self?.apply(status) }
        settings.configurationDidSave = { [weak self] configuration in self?.runtime.restart(configuration) }
        if isFirstLaunch {
            settings.show()
        } else {
            if !CommandLine.arguments.contains("--gui-review") {
                startFromStoredConfiguration()
            }
            dashboard.showDashboard()
        }
    }

    func applicationShouldHandleReopen(_ sender: NSApplication, hasVisibleWindows flag: Bool) -> Bool {
        dashboard.showDashboard()
        return true
    }

    func applicationShouldTerminate(_ sender: NSApplication) -> NSApplication.TerminateReply {
        runtime.terminate { sender.reply(toApplicationShouldTerminate: true) }
        return .terminateLater
    }

    private func configureStatusItem() {
        if #available(macOS 11.0, *) {
            statusItem.button?.image = NSImage(systemSymbolName: "chart.xyaxis.line", accessibilityDescription: "Sky Backcraft")
        }
        let menu = NSMenu()
        stateItem.isEnabled = false
        menu.addItem(stateItem)
        menu.addItem(.separator())
        dashboardItem.target = self
        menu.addItem(dashboardItem)
        menu.addItem(.separator())
        for item in [startItem, stopItem, restartItem] { item.target = self; menu.addItem(item) }
        menu.addItem(.separator())
        for item in [copyURLItem, copyPublicURLItem, copyCodeItem] { item.target = self; menu.addItem(item) }
        let logs = NSMenuItem(title: "로그 보기", action: #selector(openLogs), keyEquivalent: "")
        logs.target = self
        menu.addItem(logs)
        let preferences = NSMenuItem(title: "설정 및 실거래 제어…", action: #selector(openSettings), keyEquivalent: ",")
        preferences.target = self
        menu.addItem(preferences)
        menu.addItem(.separator())
        menu.addItem(NSMenuItem(title: "종료", action: #selector(NSApplication.terminate(_:)), keyEquivalent: "q"))
        statusItem.menu = menu
        apply(.stopped)
    }

    private func configureApplicationMenu() {
        let mainMenu = NSMenu(title: "Main Menu")
        let applicationItem = NSMenuItem()
        mainMenu.addItem(applicationItem)
        let applicationMenu = NSMenu(title: "Sky Backcraft")
        applicationItem.submenu = applicationMenu

        let openDash = NSMenuItem(title: "퀀트 대시보드 열기…", action: #selector(openDashboard), keyEquivalent: "d")
        openDash.target = self
        applicationMenu.addItem(openDash)

        let preferences = NSMenuItem(title: "Sky Backcraft 설정 및 실거래 제어…", action: #selector(openSettings), keyEquivalent: ",")
        preferences.target = self
        applicationMenu.addItem(preferences)
        applicationMenu.addItem(.separator())
        applicationMenu.addItem(NSMenuItem(
            title: "Sky Backcraft 종료",
            action: #selector(NSApplication.terminate(_:)),
            keyEquivalent: "q"
        ))
        let fileItem = NSMenuItem(title: "파일", action: nil, keyEquivalent: "")
        let fileMenu = NSMenu(title: "파일")
        fileMenu.addItem(NSMenuItem(title: "창 닫기", action: #selector(NSWindow.performClose(_:)), keyEquivalent: "w"))
        fileItem.submenu = fileMenu
        mainMenu.addItem(fileItem)
        let editItem = NSMenuItem(title: "편집", action: nil, keyEquivalent: "")
        let editMenu = NSMenu(title: "편집")
        editMenu.addItem(NSMenuItem(title: "실행 취소", action: Selector(("undo:")), keyEquivalent: "z"))
        let redo = NSMenuItem(title: "다시 실행", action: Selector(("redo:")), keyEquivalent: "z")
        redo.keyEquivalentModifierMask = [.command, .shift]
        editMenu.addItem(redo)
        editMenu.addItem(.separator())
        editMenu.addItem(NSMenuItem(title: "잘라내기", action: #selector(NSText.cut(_:)), keyEquivalent: "x"))
        editMenu.addItem(NSMenuItem(title: "복사", action: #selector(NSText.copy(_:)), keyEquivalent: "c"))
        editMenu.addItem(NSMenuItem(title: "붙여넣기", action: #selector(NSText.paste(_:)), keyEquivalent: "v"))
        editMenu.addItem(NSMenuItem(title: "모두 선택", action: #selector(NSText.selectAll(_:)), keyEquivalent: "a"))
        editItem.submenu = editMenu
        mainMenu.addItem(editItem)
        let viewItem = NSMenuItem(title: "보기", action: nil, keyEquivalent: "")
        let viewMenu = NSMenu(title: "보기")
        viewItem.submenu = viewMenu
        mainMenu.addItem(viewItem)
        let sidebarToggle = NSMenuItem(
            title: "사이드바 표시/가리기",
            action: #selector(DashboardWindowController.toggleSidebar(_:)),
            keyEquivalent: ""
        )
        sidebarToggle.target = dashboard
        viewMenu.addItem(sidebarToggle)
        viewMenu.addItem(.separator())
        for (tag, title) in ["시스템 모드", "라이트 모드", "다크 모드"].enumerated() {
            let item = NSMenuItem(title: title, action: #selector(changeAppearance(_:)), keyEquivalent: "")
            item.tag = tag
            item.target = self
            viewMenu.addItem(item)
        }
        if CommandLine.arguments.contains("--gui-review") {
            viewMenu.addItem(.separator())
            for size in [NSSize(width: 1200, height: 800), NSSize(width: 1440, height: 900), NSSize(width: 2048, height: 1235)] {
                let item = NSMenuItem(title: "검증 창 \(Int(size.width)) × \(Int(size.height))", action: #selector(changeReviewViewport(_:)), keyEquivalent: "")
                item.representedObject = NSValue(size: size)
                item.target = self
                viewMenu.addItem(item)
            }
        }
        NSApp.mainMenu = mainMenu
    }

    @objc private func changeAppearance(_ sender: NSMenuItem) {
        NSApp.appearance = sender.tag == 1 ? NSAppearance(named: .aqua)
            : (sender.tag == 2 ? NSAppearance(named: .darkAqua) : nil)
        dashboard.window?.contentView?.needsDisplay = true
        settings.window?.contentView?.needsDisplay = true
    }

    @objc private func changeReviewViewport(_ sender: NSMenuItem) {
        guard CommandLine.arguments.contains("--gui-review"), let value = sender.representedObject as? NSValue else { return }
        dashboard.window?.setContentSize(value.sizeValue)
        dashboard.window?.center()
    }

    private func apply(_ status: RuntimeStatus) {
        stateItem.title = status.menuTitle
        settings.updateStatus(status)
        dashboard.updateRuntimeStatus(status)
        if CommandLine.arguments.contains("--gui-review") {
            startItem.isEnabled = false
            stopItem.isEnabled = false
            restartItem.isEnabled = false
            copyURLItem.isEnabled = false
            copyPublicURLItem.isEnabled = false
            copyCodeItem.isEnabled = false
            return
        }
        switch status {
        case .stopped, .failed:
            startItem.isEnabled = true; stopItem.isEnabled = false; restartItem.isEnabled = false
            copyURLItem.isEnabled = false; copyPublicURLItem.isEnabled = false; copyCodeItem.isEnabled = false
        case .blocked:
            startItem.isEnabled = false; stopItem.isEnabled = false; restartItem.isEnabled = false
            copyURLItem.isEnabled = false; copyPublicURLItem.isEnabled = false; copyCodeItem.isEnabled = false
        case .starting, .stopping:
            startItem.isEnabled = false; stopItem.isEnabled = status == .starting; restartItem.isEnabled = false
            copyURLItem.isEnabled = false; copyPublicURLItem.isEnabled = false; copyCodeItem.isEnabled = false
        case .running:
            startItem.isEnabled = false; stopItem.isEnabled = true; restartItem.isEnabled = true
            copyURLItem.isEnabled = runtime.connectorURL() != nil
            copyPublicURLItem.isEnabled = runtime.publicConnectorURL() != nil
            copyCodeItem.isEnabled = runtime.oauthLoginCode() != nil
        }
    }

    @objc private func openDashboard() { dashboard.showDashboard() }
    @objc private func start() {
        guard !CommandLine.arguments.contains("--gui-review") else { return }
        startFromStoredConfiguration()
    }
    @objc private func stop() { runtime.stop() }
    @objc private func restart() {
        guard !CommandLine.arguments.contains("--gui-review") else { return }
        do { runtime.restart(try store.load()) }
        catch { settings.show(configurationError: error) }
    }
    @objc private func openSettings() { settings.show() }
    @objc private func copyURL() { copy(runtime.connectorURL()) }
    @objc private func copyPublicURL() { copy(runtime.publicConnectorURL()) }
    @objc private func copyCode() { copy(runtime.oauthLoginCode()) }
    private func copy(_ value: String?) {
        guard let value else { return }
        NSPasteboard.general.clearContents()
        NSPasteboard.general.setString(value, forType: .string)
    }
    @objc private func openLogs() {
        if !FileManager.default.fileExists(atPath: store.logURL.path) { _ = FileManager.default.createFile(atPath: store.logURL.path, contents: nil) }
        NSWorkspace.shared.open(store.logURL)
    }

    private func startFromStoredConfiguration() {
        guard !CommandLine.arguments.contains("--gui-review") else { return }
        do { runtime.start(try store.load()) }
        catch { settings.show(configurationError: error) }
    }
}
