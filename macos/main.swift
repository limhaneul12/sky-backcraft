import AppKit
import Foundation

private func reviewSupportDirectory(arguments: [String], environment: [String: String]) throws -> URL? {
    guard arguments.contains("--gui-review"),
          let path = environment["SERVICE_P13_REVIEW_ROOT"] else { return nil }
    guard path.hasPrefix("/") else {
        throw AppError.configuration("검증용 데이터 폴더는 절대 경로여야 합니다.")
    }
    let root = URL(fileURLWithPath: path, isDirectory: true)
        .standardizedFileURL.resolvingSymlinksInPath()
    guard root.path.hasPrefix("/tmp/") || root.path.hasPrefix("/private/tmp/") else {
        throw AppError.configuration("검증용 데이터 폴더는 임시 폴더 안에 있어야 합니다.")
    }
    let database = root.appendingPathComponent("research/lab.sqlite").resolvingSymlinksInPath()
    var isDirectory: ObjCBool = false
    guard database.path.hasPrefix(root.path + "/"),
          FileManager.default.fileExists(atPath: database.path, isDirectory: &isDirectory),
          !isDirectory.boolValue else {
        throw AppError.configuration("검증용 폴더 안의 연구 데이터베이스를 확인할 수 없습니다.")
    }
    return root
}

private func runSelfTest() -> Int32 {
    do {
        let temporary = FileManager.default.temporaryDirectory.appendingPathComponent("sky-backcraft-self-test-\(UUID().uuidString)", isDirectory: true)
        defer { try? FileManager.default.removeItem(at: temporary) }
        let store = try ConfigurationStore(supportDirectory: temporary)
        let legacySettings = #"{"domain":"legacy.example","port":8130,"authenticationMode":"oauth"}"#
        try Data(legacySettings.utf8).write(to: store.settingsURL, options: .atomic)
        let legacyConfiguration = try store.load()
        guard !legacyConfiguration.tradingMcpEnabled else {
            throw AppError.runtime("기존 설정에서 MCP 계좌/전략 제어가 기본 활성화되었습니다")
        }
        try store.save(.default)
        guard try store.load() == .default else { throw AppError.runtime("설정 저장/복원 불일치") }
        guard (try? AppConfiguration(domain: "https://bad", port: 8130, authenticationMode: .none).validated()) == nil else { throw AppError.runtime("잘못된 도메인 허용") }
        guard (try? AppConfiguration(domain: "ok.example", port: 0, authenticationMode: .none).validated()) == nil else { throw AppError.runtime("잘못된 포트 허용") }
        guard (try? AppConfiguration(domain: "ok.example", port: 8130, authenticationMode: .none, tradingMcpEnabled: true).validated()) == nil else {
            throw AppError.runtime("OAuth 없이 MCP 계좌/전략 제어가 허용되었습니다")
        }
        let none = try RuntimeController.backendArguments(configuration: .default, dataDirectory: store.dataDirectory(for: .none))
        guard none.contains("--public-no-auth"), none.contains("skybackcraft.store"),
              !none.contains("--trading-mcp"), !none.contains("--allow-live-orders") else {
            throw AppError.runtime("인증 없음 실행 인자 불일치")
        }
        let oauthConfiguration = AppConfiguration(domain: "oauth.example", port: 8130, authenticationMode: .oauth)
        let oauth = try RuntimeController.backendArguments(configuration: oauthConfiguration, dataDirectory: store.dataDirectory(for: .oauth))
        guard oauth.contains("--oauth"), oauth.contains("https://oauth.example"),
              !oauth.contains("--public-no-auth"), !oauth.contains("--trading-mcp"), !oauth.contains("--allow-live-orders") else {
            throw AppError.runtime("OAuth 기본 실행 인자 불일치")
        }
        let tradingConfiguration = AppConfiguration(
            domain: "oauth.example",
            port: 8130,
            authenticationMode: .oauth,
            tradingMcpEnabled: true
        )
        try store.save(tradingConfiguration)
        guard try store.load() == tradingConfiguration else { throw AppError.runtime("MCP 계좌/전략 제어 설정 저장/복원 불일치") }
        let trading = try RuntimeController.backendArguments(configuration: tradingConfiguration, dataDirectory: store.dataDirectory(for: .oauth))
        guard trading.contains("--oauth"), trading.contains("--trading-mcp"), !trading.contains("--allow-live-orders") else {
            throw AppError.runtime("OAuth opt-in 실행 인자 또는 실주문 차단 불일치")
        }
        guard none.joined(separator: " ").contains("research"),
              oauth.joined(separator: " ").contains("research"),
              store.dataDirectory(for: .none) == store.dataDirectory(for: .oauth) else {
            throw AppError.runtime("인증 모드와 무관하게 동일한 연구 저장소를 사용해야 합니다")
        }
        let ownerCode = try OwnerCode.make()
        let settingsText = String(data: try Data(contentsOf: store.settingsURL), encoding: .utf8) ?? ""
        guard ownerCode.count == 64, !settingsText.contains(ownerCode) else { throw AppError.runtime("OAuth 코드 보안 계약 불일치") }
        let corruptOAuth = #"{"domain":"oauth.example","port":0,"authenticationMode":"oauth"}"#
        try Data(corruptOAuth.utf8).write(to: store.settingsURL, options: .atomic)
        guard (try? store.load()) == nil else { throw AppError.runtime("손상된 OAuth 설정이 인증 없음으로 하향되었습니다") }
        let testLog = temporary.appendingPathComponent("bounded.log")
        let writer = BoundedLogWriter(url: testLog, maximumBytes: 64)
        try writer.open()
        try writer.write(Data(repeating: 0x41, count: 48))
        try writer.write(Data(repeating: 0x42, count: 48))
        try writer.close()
        let currentSize = ((try FileManager.default.attributesOfItem(atPath: testLog.path)[.size]) as? NSNumber)?.intValue ?? 0
        guard currentSize <= 64,
              FileManager.default.fileExists(atPath: testLog.appendingPathExtension("previous").path) else {
            throw AppError.runtime("실행 중 로그 회전 계약 불일치")
        }
        let reviewEnvironment = ["SERVICE_P13_REVIEW_ROOT": temporary.path]
        guard try reviewSupportDirectory(arguments: [], environment: reviewEnvironment) == nil,
              (try? reviewSupportDirectory(arguments: ["--gui-review"], environment: reviewEnvironment)) == nil else {
            throw AppError.runtime("검증 모드 경계 또는 누락 DB 검사가 실패했습니다")
        }
        let reviewRoot = URL(fileURLWithPath: "/tmp/sky-backcraft-review-\(UUID().uuidString)", isDirectory: true)
        defer { try? FileManager.default.removeItem(at: reviewRoot) }
        let reviewResearch = reviewRoot.appendingPathComponent("research", isDirectory: true)
        try FileManager.default.createDirectory(at: reviewResearch, withIntermediateDirectories: true)
        try Data().write(to: reviewResearch.appendingPathComponent("lab.sqlite"))
        guard try reviewSupportDirectory(arguments: ["--gui-review"], environment: ["SERVICE_P13_REVIEW_ROOT": reviewRoot.path])
            == reviewRoot.resolvingSymlinksInPath() else {
            throw AppError.runtime("검증용 데이터 폴더 선택에 실패했습니다")
        }
        let outsideDatabase = temporary.appendingPathComponent("outside.sqlite")
        try Data().write(to: outsideDatabase)
        let reviewDatabase = reviewResearch.appendingPathComponent("lab.sqlite")
        try FileManager.default.removeItem(at: reviewDatabase)
        try FileManager.default.createSymbolicLink(at: reviewDatabase, withDestinationURL: outsideDatabase)
        guard (try? reviewSupportDirectory(arguments: ["--gui-review"], environment: ["SERVICE_P13_REVIEW_ROOT": reviewRoot.path])) == nil else {
            throw AppError.runtime("검증 폴더 밖의 DB 링크가 허용되었습니다")
        }
        print("Sky Backcraft native self-test: PASS")
        return 0
    } catch {
        fputs("Sky Backcraft native self-test: FAIL: \(error.localizedDescription)\n", stderr)
        return 1
    }
}

@main
private enum SkyBackcraftApplication {
    static func main() {
        if CommandLine.arguments.contains("--self-test") { exit(runSelfTest()) }
        let application = NSApplication.shared
        if CommandLine.arguments.contains("--gui-review"),
           let portText = ProcessInfo.processInfo.environment["SERVICE_P13_REVIEW_PORT"],
           let port = Int(portText),
           let token = ProcessInfo.processInfo.environment["SERVICE_DESKTOP_ACCESS_TOKEN"] {
            NativeMCPClient.shared.configure(port: port, token: token)
        }
        do {
            let supportDirectory = try reviewSupportDirectory(
                arguments: CommandLine.arguments,
                environment: ProcessInfo.processInfo.environment
            )
            let store = try ConfigurationStore(supportDirectory: supportDirectory)
            let delegate = AppDelegate(store: store)
            application.delegate = delegate
            application.setActivationPolicy(.accessory)
            withExtendedLifetime(delegate) { application.run() }
        } catch {
            let alert = NSAlert(error: error)
            alert.messageText = "Sky Backcraft를 시작할 수 없습니다"
            alert.runModal()
            exit(1)
        }
    }
}
