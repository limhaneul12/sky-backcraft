import AppKit

final class AppDelegate: NSObject, NSApplicationDelegate {
    private let store: ConfigurationStore
    private let runtime: RuntimeController
    private let settings: SettingsWindowController
    private let statusItem = NSStatusBar.system.statusItem(withLength: NSStatusItem.squareLength)
    private let stateItem = NSMenuItem(title: "MCP: 중지됨", action: nil, keyEquivalent: "")
    private let startItem = NSMenuItem(title: "MCP 시작", action: #selector(start), keyEquivalent: "")
    private let stopItem = NSMenuItem(title: "MCP 중지", action: #selector(stop), keyEquivalent: "")
    private let restartItem = NSMenuItem(title: "MCP 재시작", action: #selector(restart), keyEquivalent: "r")
    private let copyURLItem = NSMenuItem(title: "MCP URL 복사", action: #selector(copyURL), keyEquivalent: "")
    private let copyCodeItem = NSMenuItem(title: "OAuth 로그인 코드 복사", action: #selector(copyCode), keyEquivalent: "")
    private let isFirstLaunch: Bool

    init(store: ConfigurationStore) {
        self.store = store
        isFirstLaunch = !FileManager.default.fileExists(atPath: store.settingsURL.path)
        runtime = RuntimeController(store: store)
        settings = SettingsWindowController(store: store)
        super.init()
    }

    func applicationDidFinishLaunching(_ notification: Notification) {
        configureApplicationMenu()
        configureStatusItem()
        runtime.statusDidChange = { [weak self] status in self?.apply(status) }
        settings.configurationDidSave = { [weak self] configuration in self?.runtime.restart(configuration) }
        if isFirstLaunch { settings.show() }
        else { startFromStoredConfiguration() }
    }

    func applicationShouldHandleReopen(_ sender: NSApplication, hasVisibleWindows flag: Bool) -> Bool {
        settings.show()
        return true
    }

    func applicationShouldTerminate(_ sender: NSApplication) -> NSApplication.TerminateReply {
        runtime.terminate { sender.reply(toApplicationShouldTerminate: true) }
        return .terminateLater
    }

    private func configureStatusItem() {
        if #available(macOS 11.0, *) { statusItem.button?.image = NSImage(systemSymbolName: "server.rack", accessibilityDescription: "Sky Backcraft") }
        let menu = NSMenu()
        stateItem.isEnabled = false
        menu.addItem(stateItem)
        menu.addItem(.separator())
        for item in [startItem, stopItem, restartItem] { item.target = self; menu.addItem(item) }
        menu.addItem(.separator())
        for item in [copyURLItem, copyCodeItem] { item.target = self; menu.addItem(item) }
        let logs = NSMenuItem(title: "로그 보기", action: #selector(openLogs), keyEquivalent: "")
        logs.target = self
        menu.addItem(logs)
        let preferences = NSMenuItem(title: "설정…", action: #selector(openSettings), keyEquivalent: ",")
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

        let preferences = NSMenuItem(title: "Sky Backcraft 설정…", action: #selector(openSettings), keyEquivalent: ",")
        preferences.target = self
        applicationMenu.addItem(preferences)
        applicationMenu.addItem(.separator())
        applicationMenu.addItem(NSMenuItem(
            title: "Sky Backcraft 종료",
            action: #selector(NSApplication.terminate(_:)),
            keyEquivalent: "q"
        ))
        NSApp.mainMenu = mainMenu
    }

    private func apply(_ status: RuntimeStatus) {
        stateItem.title = status.menuTitle
        settings.updateStatus(status)
        switch status {
        case .stopped, .failed:
            startItem.isEnabled = true; stopItem.isEnabled = false; restartItem.isEnabled = false
            copyURLItem.isEnabled = false; copyCodeItem.isEnabled = false
        case .blocked:
            startItem.isEnabled = false; stopItem.isEnabled = false; restartItem.isEnabled = false
            copyURLItem.isEnabled = false; copyCodeItem.isEnabled = false
        case .starting, .stopping:
            startItem.isEnabled = false; stopItem.isEnabled = status == .starting; restartItem.isEnabled = false
            copyURLItem.isEnabled = false; copyCodeItem.isEnabled = false
        case .running:
            startItem.isEnabled = false; stopItem.isEnabled = true; restartItem.isEnabled = true
            copyURLItem.isEnabled = runtime.connectorURL() != nil
            copyCodeItem.isEnabled = runtime.oauthLoginCode() != nil
        }
    }

    @objc private func start() { startFromStoredConfiguration() }
    @objc private func stop() { runtime.stop() }
    @objc private func restart() {
        do { runtime.restart(try store.load()) }
        catch { settings.show(configurationError: error) }
    }
    @objc private func openSettings() { settings.show() }
    @objc private func copyURL() { copy(runtime.connectorURL()) }
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
        do { runtime.start(try store.load()) }
        catch { settings.show(configurationError: error) }
    }
}
