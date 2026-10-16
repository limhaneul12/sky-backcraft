import AppKit
import Darwin
import Foundation

/// Isolated AppKit launch path for appearance checks against a loopback MCP fixture.
/// Build this source in a separate `.app` bundle with the required identifier prefix.
@main
private enum AutomationGUIFixture {
    private static let bundleIdentifierPrefix = "dev.skybackcraft.automation-gui-fixture"
    private static let fakeDesktopToken = String(repeating: "a", count: 64)
    private static let supportRootEnvironmentKey = "SKY_BACKCRAFT_AUTOMATION_FIXTURE_ROOT"
    private static let windowTitle = "MOCK 자동매매 검증 · 실제 주문 아님"

    private struct SupportRootSelection {
        let url: URL
        let removeWhenDone: Bool
    }

    static func main() {
        let lifecycleFixture = ProcessInfo.processInfo.environment["SKY_BACKCRAFT_AUTOMATION_FIXTURE_LIFECYCLE"] == "1"
        guard CommandLine.arguments.contains("--gui-review") || lifecycleFixture else {
            fail("Pass --gui-review so AppDelegate cannot start or restart the backend.", status: 64)
        }
        guard let bundleIdentifier = Bundle.main.bundleIdentifier,
              bundleIdentifier.hasPrefix(bundleIdentifierPrefix) else {
            fail("Build as an isolated app with bundle ID prefix \(bundleIdentifierPrefix).", status: 64)
        }
        guard let portValue = ProcessInfo.processInfo.environment["SKY_BACKCRAFT_AUTOMATION_FIXTURE_PORT"],
              let port = Int(portValue), (1024...65535).contains(port),
              port != AppConfiguration.defaultPort else {
            fail("Set SKY_BACKCRAFT_AUTOMATION_FIXTURE_PORT to the isolated loopback fake MCP port.", status: 64)
        }

        let supportRoot: SupportRootSelection
        do {
            supportRoot = try selectSupportRoot(
                environment: ProcessInfo.processInfo.environment,
                fileManager: .default
            )
        } catch {
            fail("Invalid automation GUI fixture support root: \(error.localizedDescription)", status: 64)
        }
        defer {
            if supportRoot.removeWhenDone {
                try? FileManager.default.removeItem(at: supportRoot.url)
            }
        }

        do {
            let store = try ConfigurationStore(supportDirectory: supportRoot.url)
            if lifecycleFixture {
                try store.save(AppConfiguration(domain: "synthetic-fixture.invalid", port: port, authenticationMode: .oauth, tradingMcpEnabled: true))
            } else if !FileManager.default.fileExists(atPath: store.settingsURL.path) {
                try store.save(AppConfiguration(domain: "", port: port, authenticationMode: .none))
            }
            NativeMCPClient.shared.configure(port: port, token: fakeDesktopToken)

            let application = NSApplication.shared
            let delegate: NSObject & NSApplicationDelegate
            if lifecycleFixture {
                delegate = LifecycleFixtureDelegate(store: store)
            } else {
                delegate = AppDelegate(store: store)
            }
            application.delegate = delegate
            application.setActivationPolicy(lifecycleFixture ? .regular : .accessory)
            DispatchQueue.main.async {
                guard let dashboard = application.windows.first(where: {
                    $0.windowController is DashboardWindowController
                }) else { return }
                let prefix = lifecycleFixture ? "SYNTHETIC LIVE 제어 검증 · 실제 주문 아님 — " : "\(windowTitle) — "
                if !dashboard.title.hasPrefix(prefix) {
                    dashboard.title = prefix + dashboard.title
                }
                if lifecycleFixture {
                    scheduleLifecycleWorkspace(dashboard: dashboard)
                } else {
                    scheduleReviewJourney(application: application, dashboard: dashboard)
                }
            }
            withExtendedLifetime(delegate) { application.run() }
        } catch {
            fail("Could not prepare isolated automation GUI fixture: \(error.localizedDescription)", status: 1)
        }
    }

    /// Uses production controllers without creating a second menu-bar server owner.
    private final class LifecycleFixtureDelegate: NSObject, NSApplicationDelegate {
        private let dashboard: DashboardWindowController
        private let settings: SettingsWindowController

        init(store: ConfigurationStore) {
            dashboard = DashboardWindowController(store: store)
            settings = SettingsWindowController(store: store)
            super.init()
            dashboard.onOpenSettings = { [weak self] in self?.settings.show() }
            settings.onOpenAutomationWorkspace = { [weak self] in self?.dashboard.showAutomationWorkspace() }
            settings.onControlSnapshot = { [weak self] snapshot in self?.dashboard.updateControlSnapshot(snapshot) }
        }

        func applicationDidFinishLaunching(_ notification: Notification) {
            dashboard.showDashboard()
        }

        func applicationShouldTerminateAfterLastWindowClosed(_ sender: NSApplication) -> Bool {
            true
        }
    }

    private static func selectSupportRoot(
        environment: [String: String],
        fileManager: FileManager
    ) throws -> SupportRootSelection {
        guard let suppliedPath = environment[supportRootEnvironmentKey], !suppliedPath.isEmpty else {
            let generatedRoot = fileManager.temporaryDirectory
                .appendingPathComponent("\(bundleIdentifierPrefix)-\(UUID().uuidString.lowercased())", isDirectory: true)
                .standardizedFileURL
            return SupportRootSelection(url: generatedRoot, removeWhenDone: true)
        }

        guard suppliedPath.hasPrefix("/") else {
            throw FixtureError.invalidRoot("\(supportRootEnvironmentKey) must be an absolute path.")
        }
        let requestedRoot = URL(fileURLWithPath: suppliedPath, isDirectory: true).standardizedFileURL
        guard isTemporaryDescendant(requestedRoot.path) else {
            throw FixtureError.invalidRoot("\(supportRootEnvironmentKey) must be inside /tmp or /private/tmp.")
        }

        var isDirectory: ObjCBool = false
        if fileManager.fileExists(atPath: requestedRoot.path, isDirectory: &isDirectory) {
            guard isDirectory.boolValue else {
                throw FixtureError.invalidRoot("The supplied support root exists but is not a directory.")
            }
        } else {
            try fileManager.createDirectory(at: requestedRoot, withIntermediateDirectories: true)
        }

        let resolvedRoot = requestedRoot.resolvingSymlinksInPath().standardizedFileURL
        guard isTemporaryDescendant(resolvedRoot.path) else {
            throw FixtureError.invalidRoot("The supplied support root resolves outside /tmp and /private/tmp.")
        }
        let researchDatabase = resolvedRoot.appendingPathComponent("research/lab.sqlite", isDirectory: false)
        var isDatabaseDirectory: ObjCBool = false
        guard fileManager.fileExists(atPath: researchDatabase.path, isDirectory: &isDatabaseDirectory),
              !isDatabaseDirectory.boolValue else {
            throw FixtureError.invalidRoot("The supplied fixture root must contain research/lab.sqlite from the fixture server's Research store.")
        }
        let resolvedDatabase = researchDatabase.resolvingSymlinksInPath().standardizedFileURL.path
        guard resolvedDatabase.hasPrefix(resolvedRoot.path + "/") else {
            throw FixtureError.invalidRoot("The fixture Research database must remain inside the supplied support root.")
        }
        return SupportRootSelection(url: resolvedRoot, removeWhenDone: false)
    }

    private static func isTemporaryDescendant(_ path: String) -> Bool {
        path.hasPrefix("/tmp/") || path.hasPrefix("/private/tmp/")
    }

    private static func scheduleLifecycleWorkspace(dashboard: NSWindow) {
        let deadline = Date().addingTimeInterval(35)
        Timer.scheduledTimer(withTimeInterval: 0.25, repeats: true) { timer in
            guard Date() < deadline, let root = dashboard.contentView else {
                timer.invalidate()
                fail("Timed out opening synthetic lifecycle workspace.", status: 1)
            }
            let tables = descendants(of: root).compactMap { $0 as? NSTableView }
            if let sidebar = tables.first(where: { $0.numberOfRows == 7 && $0.tableColumns.count == 1 }), sidebar.selectedRow != 4 {
                sidebar.selectRowIndexes(IndexSet(integer: 4), byExtendingSelection: false)
                return
            }
            if tables.contains(where: { $0.tableColumns.first?.identifier.rawValue == "session_id" }) {
                timer.invalidate()
            }
        }
    }

    private static func scheduleReviewJourney(application: NSApplication, dashboard: NSWindow) {
        let deadline = Date().addingTimeInterval(35)
        var unmaskedForFixture = false
        Timer.scheduledTimer(withTimeInterval: 0.25, repeats: true) { timer in
            guard Date() < deadline, let root = dashboard.contentView else {
                timer.invalidate()
                fail("Timed out opening the persisted automation review window.", status: 1)
            }
            let tables = descendants(of: root).compactMap { $0 as? NSTableView }
            if let sidebar = tables.first(where: { $0.numberOfRows == 7 && $0.tableColumns.count == 1 }),
               sidebar.selectedRow != 4 {
                sidebar.selectRowIndexes(IndexSet(integer: 4), byExtendingSelection: false)
                dashboard.contentView?.layoutSubtreeIfNeeded()
                return
            }
            guard let sessionTable = tables.first(where: {
                $0.tableColumns.first?.identifier.rawValue == "session_id"
            }) else { return }
            guard sessionTable.numberOfRows > 0 else { return }
            if sessionTable.selectedRow != 0 {
                sessionTable.selectRowIndexes(IndexSet(integer: 0), byExtendingSelection: false)
                dashboard.contentView?.layoutSubtreeIfNeeded()
                return
            }
            guard let reviewButton = descendants(of: root).compactMap({ $0 as? NSButton })
                .first(where: { $0.title == "복기 · 성과" }), reviewButton.isEnabled else { return }
            if !application.windows.contains(where: { $0.title == "전략 자동매매 복기 · 성과" }) {
                reviewButton.performClick(nil)
                dashboard.contentView?.layoutSubtreeIfNeeded()
                return
            }

            guard let reviewWindow = application.windows.first(where: {
                $0.title == "전략 자동매매 복기 · 성과"
            }), let reviewRoot = reviewWindow.contentView else { return }
            let reviewViews = descendants(of: reviewRoot)
            let reviewTables = reviewViews.compactMap { $0 as? NSTableView }
            let decisionTable = reviewTables.first {
                $0.tableColumns.first?.identifier.rawValue == "time"
                    && $0.accessibilityLabel() == "저장된 전략 판단 시간순 목록"
            }
            let indicatorTable = reviewTables.first {
                $0.tableColumns.first?.identifier.rawValue == "name"
                    && $0.accessibilityLabel() == "저장된 전략 지표 값"
            }
            let orderTable = reviewTables.first {
                $0.tableColumns.first?.identifier.rawValue == "client"
                    && $0.accessibilityLabel() == "선택 판단에 연결된 주문"
            }
            let labels = reviewViews.compactMap { $0 as? NSTextField }.map(\.stringValue)
            let hasDecision = (decisionTable?.numberOfRows ?? 0) > 0
            let hasIndicators = (indicatorTable?.numberOfRows ?? 0) > 0
            let hasOrders = (orderTable?.numberOfRows ?? 0) > 0
            let hasCandle = labels.contains(where: { $0.contains("시가") && $0.contains("종가") })
            let hasReason = labels.contains(where: { $0.contains("이유:") && !$0.contains("저장된 reason code 없음") })
            let hasReport = labels.contains(where: { $0.contains("판단 ") && $0.contains("순 현금 흐름") })
            let hasMockBadge = labels.contains(where: { $0.contains("MOCK · 모의 기록") })
            guard hasDecision, hasIndicators, hasOrders, hasCandle, hasReport, hasMockBadge else { return }
            if !unmaskedForFixture {
                guard labels.contains(where: { $0.contains("개인정보 숨김") }),
                      !hasReason,
                      let privacyButton = descendants(of: root).compactMap({ $0 as? NSButton })
                        .first(where: { $0.title == "계좌 금액 표시" }) else { return }
                privacyButton.performClick(nil)
                unmaskedForFixture = true
                return
            }
            guard hasReason,
                  let indicatorTable,
                  (0..<indicatorTable.numberOfRows).contains(where: { row in
                      guard let indicatorValue = indicatorTable.view(atColumn: 1, row: row, makeIfNecessary: true) as? NSTextField else {
                          return false
                      }
                      return Decimal(string: indicatorValue.stringValue, locale: Locale(identifier: "en_US_POSIX")) != nil
                  }) else { return }
            timer.invalidate()
        }
    }

    private static func descendants(of root: NSView) -> [NSView] {
        root.subviews.reduce(into: [root]) { result, child in
            result.append(contentsOf: descendants(of: child))
        }
    }

    private enum FixtureError: LocalizedError {
        case invalidRoot(String)

        var errorDescription: String? {
            switch self {
            case .invalidRoot(let detail): return detail
            }
        }
    }

    private static func fail(_ message: String, status: Int32) -> Never {
        fputs("Automation GUI fixture: \(message)\n", stderr)
        exit(status)
    }
}
