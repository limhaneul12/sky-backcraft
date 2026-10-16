import AppKit
import Darwin
import Foundation

/// Isolated Settings launch for CUA against a separate MOCK MCP helper.
/// Build as an app whose bundle identifier starts with the fixture prefix below.
@main
private enum LiveOrderSwitchGUIFixture {
    private static let bundleIdentifierPrefix = "dev.skybackcraft.live-order-switch-gui-fixture"
    private static let fakeDesktopToken = String(repeating: "a", count: 64)
    private static let titlePrefix = "MOCK · 주문 전송 제어 · 실제 거래소 효과 없음 — "

    static func main() {
        guard CommandLine.arguments.contains("--gui-review"),
              CommandLine.arguments.contains("--gui-review-read-only"),
              CommandLine.arguments.contains("--gui-review-mock-order-control") else {
            fail("Pass the review, read-only, and MOCK-order-control flags; the fixture never starts the backend.")
        }
        guard let bundleIdentifier = Bundle.main.bundleIdentifier,
              bundleIdentifier.hasPrefix(bundleIdentifierPrefix) else {
            fail("Build with bundle identifier prefix \(bundleIdentifierPrefix).")
        }
        guard let portText = ProcessInfo.processInfo.environment["SERVICE_P13_REVIEW_PORT"],
              let port = Int(portText), (1024...65535).contains(port),
              port != AppConfiguration.defaultPort else {
            fail("Set SERVICE_P13_REVIEW_PORT to the isolated loopback MOCK MCP port.")
        }
        guard ProcessInfo.processInfo.environment["SERVICE_DESKTOP_ACCESS_TOKEN"] == fakeDesktopToken else {
            fail("SERVICE_DESKTOP_ACCESS_TOKEN must be the fixture-only 64-character fake token.")
        }

        do {
            let root = try isolatedSupportRoot(environment: ProcessInfo.processInfo.environment)
            let store = try ConfigurationStore(supportDirectory: root)
            let configuration = try store.load()
            guard configuration.authenticationMode == .oauth,
                  configuration.tradingMcpEnabled else {
                fail("The isolated fixture root must already contain OAuth plus tradingMcpEnabled=true.")
            }
            guard let context = NativeMCPClient.shared.configure(port: port, token: fakeDesktopToken) else {
                fail("Could not configure the isolated local MCP transport.")
            }

            let application = NSApplication.shared
            let delegate = AppDelegate(store: store)
            application.delegate = delegate
            application.setActivationPolicy(.regular)
            let request = UpbitLiveService.shared.fetchLiveOrderControlStatus(expectedContext: context) { result in
                guard case .success(let snapshot) = result,
                      snapshot.exchange_read_mode == "MOCK",
                      snapshot.live_order_control_available else {
                    fail("The helper must report available MOCK live_order_control before Settings opens.")
                }
                DispatchQueue.main.async {
                    guard application.sendAction(NSSelectorFromString("openSettings"), to: delegate, from: nil) else {
                        fail("Could not open the standard Settings window.")
                    }
                    guard let settings = application.windows.first(where: {
                        $0.windowController is SettingsWindowController
                    }) else {
                        fail("The standard Settings window did not appear.")
                    }
                    settings.title = titlePrefix + settings.title
                    NSApp.activate(ignoringOtherApps: true)
                }
            }
            withExtendedLifetime((delegate, request)) { application.run() }
        } catch {
            fail("Could not prepare the isolated MOCK Settings fixture: \(error.localizedDescription)")
        }
    }

    private static func isolatedSupportRoot(environment: [String: String]) throws -> URL {
        guard let path = environment["SERVICE_P13_REVIEW_ROOT"], path.hasPrefix("/") else {
            throw FixtureError.invalidRoot("SERVICE_P13_REVIEW_ROOT must be an absolute temporary path.")
        }
        let root = URL(fileURLWithPath: path, isDirectory: true).standardizedFileURL
        guard isTemporaryDescendant(root.path) else {
            throw FixtureError.invalidRoot("SERVICE_P13_REVIEW_ROOT must be inside /tmp or /private/tmp.")
        }
        let resolvedRoot = root.resolvingSymlinksInPath().standardizedFileURL
        guard isTemporaryDescendant(resolvedRoot.path) else {
            throw FixtureError.invalidRoot("The fixture support root resolves outside the temporary directory.")
        }
        let database = resolvedRoot.appendingPathComponent("research/lab.sqlite")
        var isDirectory: ObjCBool = false
        guard FileManager.default.fileExists(atPath: database.path, isDirectory: &isDirectory),
              !isDirectory.boolValue,
              database.resolvingSymlinksInPath().standardizedFileURL.path.hasPrefix(resolvedRoot.path + "/") else {
            throw FixtureError.invalidRoot("The isolated fixture must contain research/lab.sqlite inside its support root.")
        }
        return resolvedRoot
    }

    private static func isTemporaryDescendant(_ path: String) -> Bool {
        path.hasPrefix("/tmp/") || path.hasPrefix("/private/tmp/")
    }

    private static func fail(_ message: String) -> Never {
        fputs("Live order switch GUI fixture: \(message)\n", stderr)
        exit(64)
    }

    private enum FixtureError: LocalizedError {
        case invalidRoot(String)

        var errorDescription: String? {
            switch self {
            case .invalidRoot(let message): return message
            }
        }
    }
}
