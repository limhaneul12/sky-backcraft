import AppKit
import Foundation

private func runSelfTest() -> Int32 {
    do {
        let temporary = FileManager.default.temporaryDirectory.appendingPathComponent("sky-backcraft-self-test-\(UUID().uuidString)", isDirectory: true)
        defer { try? FileManager.default.removeItem(at: temporary) }
        let store = try ConfigurationStore(supportDirectory: temporary)
        try store.save(.default)
        guard try store.load() == .default else { throw AppError.runtime("설정 저장/복원 불일치") }
        guard (try? AppConfiguration(domain: "https://bad", port: 8130, authenticationMode: .none).validated()) == nil else { throw AppError.runtime("잘못된 도메인 허용") }
        guard (try? AppConfiguration(domain: "ok.example", port: 0, authenticationMode: .none).validated()) == nil else { throw AppError.runtime("잘못된 포트 허용") }
        let none = try RuntimeController.backendArguments(configuration: .default, dataDirectory: store.dataDirectory(for: .none))
        guard none.contains("--public-no-auth"), none.contains("skybackcraft.store") else { throw AppError.runtime("인증 없음 실행 인자 불일치") }
        let oauth = try RuntimeController.backendArguments(configuration: AppConfiguration(domain: "oauth.example", port: 8130, authenticationMode: .oauth), dataDirectory: store.dataDirectory(for: .oauth))
        guard oauth.contains("--oauth"), oauth.contains("https://oauth.example"), !oauth.contains("--public-no-auth") else { throw AppError.runtime("OAuth 실행 인자 불일치") }
        guard none.joined(separator: " ").contains("public-research"),
              oauth.joined(separator: " ").contains("oauth-research") else {
            throw AppError.runtime("인증 모드별 데이터 분리 불일치")
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
        do {
            let store = try ConfigurationStore()
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
