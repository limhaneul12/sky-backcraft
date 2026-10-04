import Foundation
import Security

enum AuthenticationMode: String, Codable, CaseIterable {
    case oauth
    case none

    var displayName: String {
        switch self {
        case .oauth: return "OAuth"
        case .none: return "인증 없음"
        }
    }
}

struct AppConfiguration: Codable, Equatable {
    static let defaultDomain = "skybackcraft.store"
    static let defaultPort = 8130

    var domain: String
    var port: Int
    var authenticationMode: AuthenticationMode

    static let `default` = AppConfiguration(
        domain: defaultDomain,
        port: defaultPort,
        authenticationMode: .none
    )

    func validated() throws -> AppConfiguration {
        let normalizedDomain = domain.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        guard (1...65535).contains(port) else {
            throw AppError.configuration("포트는 1부터 65535 사이여야 합니다.")
        }
        if !normalizedDomain.isEmpty {
            guard normalizedDomain.count <= 253,
                  normalizedDomain.range(of: #"^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$"#,
                                         options: .regularExpression) != nil,
                  !normalizedDomain.contains("..") else {
                throw AppError.configuration("도메인에는 호스트 이름만 입력하세요.")
            }
        }
        return AppConfiguration(domain: normalizedDomain, port: port, authenticationMode: authenticationMode)
    }
}

enum AppError: LocalizedError {
    case configuration(String)
    case runtime(String)

    var errorDescription: String? {
        switch self {
        case .configuration(let message), .runtime(let message): return message
        }
    }
}

final class ConfigurationStore {
    let supportDirectory: URL
    let settingsURL: URL
    let logURL: URL

    init(supportDirectory: URL? = nil) throws {
        let base: URL
        if let supportDirectory {
            base = supportDirectory
        } else {
            let applicationSupport = try FileManager.default.url(
                for: .applicationSupportDirectory,
                in: .userDomainMask,
                appropriateFor: nil,
                create: true
            )
            base = applicationSupport.appendingPathComponent("Sky Backcraft", isDirectory: true)
        }
        self.supportDirectory = base
        settingsURL = base.appendingPathComponent("settings.json")
        logURL = base.appendingPathComponent("sky-backcraft.log")
        try FileManager.default.createDirectory(at: base, withIntermediateDirectories: true)
    }

    func dataDirectory(for mode: AuthenticationMode) -> URL {
        supportDirectory.appendingPathComponent(
            mode == .none ? "public-research" : "oauth-research",
            isDirectory: true
        )
    }

    func load() throws -> AppConfiguration {
        guard FileManager.default.fileExists(atPath: settingsURL.path) else { return .default }
        do {
            let data = try Data(contentsOf: settingsURL)
            let decoded = try JSONDecoder().decode(AppConfiguration.self, from: data)
            return try decoded.validated()
        } catch {
            throw AppError.configuration("저장된 설정을 읽을 수 없습니다. 설정을 확인하고 다시 저장하세요. (\(error.localizedDescription))")
        }
    }

    func save(_ configuration: AppConfiguration) throws {
        let validated = try configuration.validated()
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        let data = try encoder.encode(validated)
        try data.write(to: settingsURL, options: [.atomic])
        try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: settingsURL.path)
    }
}

enum OwnerCode {
    static func make() throws -> String {
        var bytes = [UInt8](repeating: 0, count: 32)
        let result = SecRandomCopyBytes(kSecRandomDefault, bytes.count, &bytes)
        guard result == errSecSuccess else {
            throw AppError.runtime("OAuth 로그인 코드를 안전하게 만들지 못했습니다. (\(result))")
        }
        return bytes.map { String(format: "%02x", $0) }.joined()
    }
}
