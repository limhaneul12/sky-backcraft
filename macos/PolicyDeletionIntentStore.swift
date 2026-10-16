import Foundation

struct PolicyDeletionExpectedItem: Codable, Equatable {
    let policy_id: String
    let scope_digest: String
}

struct PolicyDeletionPendingIntent: Codable, Equatable {
    let request_id: String
    let data_root_identity: String
    let expected_items: [PolicyDeletionExpectedItem]
    let transport_context: NativeMCPTransportContext?

    init(
        request_id: String,
        data_root_identity: String,
        expected_items: [PolicyDeletionExpectedItem],
        transport_context: NativeMCPTransportContext? = nil
    ) {
        self.request_id = request_id
        self.data_root_identity = data_root_identity
        self.expected_items = expected_items
        self.transport_context = transport_context
    }
}

enum PolicyDeletionIntentStoreError: LocalizedError {
    case invalidIntent
    case dataRootMismatch
    case anotherRequestIsPending
    case requestIdentityMismatch

    var errorDescription: String? {
        switch self {
        case .invalidIntent:
            return "저장된 정책 삭제 대기 요청이 유효하지 않습니다. 새 삭제 요청은 차단됩니다."
        case .dataRootMismatch:
            return "저장된 정책 삭제 요청이 다른 데이터 원본에 속합니다. 원본 확인 전에는 계속 차단합니다."
        case .anotherRequestIsPending:
            return "기존 정책 삭제 요청 상태를 확인하기 전에는 새 요청을 저장할 수 없습니다."
        case .requestIdentityMismatch:
            return "대기 중인 요청 ID와 다른 요청의 상태를 지울 수 없습니다."
        }
    }
}

struct PolicyDeletionIntentStore {
    private let fileURL: URL
    let dataRootIdentity: String

    init(supportDirectory: URL, dataRoot: URL) {
        fileURL = supportDirectory.appendingPathComponent("policy-deletion-pending.json")
        dataRootIdentity = Self.canonicalIdentity(dataRoot)
    }

    func load() throws -> PolicyDeletionPendingIntent? {
        guard FileManager.default.fileExists(atPath: fileURL.path) else { return nil }
        let data = try Data(contentsOf: fileURL)
        let intent: PolicyDeletionPendingIntent
        do {
            intent = try JSONDecoder().decode(PolicyDeletionPendingIntent.self, from: data)
        } catch {
            throw PolicyDeletionIntentStoreError.invalidIntent
        }
        try validate(intent)
        return intent
    }

    func save(_ intent: PolicyDeletionPendingIntent) throws {
        try validate(intent)
        if let current = try load() {
            guard current == intent else { throw PolicyDeletionIntentStoreError.anotherRequestIsPending }
            return
        }
        try FileManager.default.createDirectory(
            at: fileURL.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        try encoder.encode(intent).write(to: fileURL, options: .atomic)
    }

    func clear(requestID: String) throws {
        guard let current = try load() else { return }
        guard current.request_id == requestID else {
            throw PolicyDeletionIntentStoreError.requestIdentityMismatch
        }
        try FileManager.default.removeItem(at: fileURL)
    }

    private func validate(_ intent: PolicyDeletionPendingIntent) throws {
        let helper = intent.transport_context?.owned_helper
        guard Self.isCanonicalUUID(intent.request_id),
              intent.data_root_identity == dataRootIdentity,
              (1...100).contains(intent.expected_items.count),
              Set(intent.expected_items.map(\.policy_id)).count == intent.expected_items.count,
              intent.transport_context.map({ $0.generation > 0 }) ?? true,
              helper.map({
                  Self.isCanonicalUUID($0.instance_id)
                      && $0.process_id > 0
                      && $0.data_root_identity == dataRootIdentity
              }) ?? true,
              intent.expected_items.allSatisfy({
                  !$0.policy_id.isEmpty && PolicyDeletePreviewValidator.isContentHash($0.scope_digest)
              }) else {
            if intent.data_root_identity != dataRootIdentity {
                throw PolicyDeletionIntentStoreError.dataRootMismatch
            }
            throw PolicyDeletionIntentStoreError.invalidIntent
        }
    }

    private static func canonicalIdentity(_ url: URL) -> String {
        url.resolvingSymlinksInPath().standardizedFileURL.path
    }

    private static func isCanonicalUUID(_ value: String) -> Bool {
        guard let uuid = UUID(uuidString: value) else { return false }
        return uuid.uuidString.lowercased() == value
    }
}
