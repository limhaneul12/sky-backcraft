import Foundation
import Darwin

struct HistoryPresentationState: Codable, Equatable {
    var search: String
    var status: String?
    var market: String?
    var policyId: String?
    var policyRevisionId: String?
    var costAssumptionLabel: String?
    var dateEnabled: Bool
    var fromDayKST: String?
    var throughDayKST: String?
    var pinnedRunIds: [String]
    var pinnedOnly: Bool
    var selectedRunIds: [String]
    var detailedColumns: Bool
    var metricSort: String?
    var lastOpenedRunId: String?

    init(
        search: String = "",
        status: String? = nil,
        market: String? = nil,
        policyId: String? = nil,
        policyRevisionId: String? = nil,
        costAssumptionLabel: String? = nil,
        dateEnabled: Bool = false,
        fromDayKST: String? = nil,
        throughDayKST: String? = nil,
        pinnedRunIds: [String] = [],
        pinnedOnly: Bool = false,
        selectedRunIds: [String] = [],
        detailedColumns: Bool = false,
        metricSort: String? = nil,
        lastOpenedRunId: String? = nil
    ) {
        self.search = search
        self.status = status
        self.market = market
        self.policyId = policyId
        self.policyRevisionId = policyRevisionId
        self.costAssumptionLabel = costAssumptionLabel
        self.dateEnabled = dateEnabled
        self.fromDayKST = fromDayKST
        self.throughDayKST = throughDayKST
        self.pinnedRunIds = pinnedRunIds
        self.pinnedOnly = pinnedOnly
        self.selectedRunIds = selectedRunIds
        self.detailedColumns = detailedColumns
        self.metricSort = metricSort
        self.lastOpenedRunId = lastOpenedRunId
    }

    static let empty = HistoryPresentationState(
        search: "", status: nil, market: nil, policyId: nil,
        policyRevisionId: nil, costAssumptionLabel: nil,
        dateEnabled: false, fromDayKST: nil, throughDayKST: nil,
        pinnedRunIds: [], pinnedOnly: false, selectedRunIds: [], detailedColumns: false,
        metricSort: nil,
        lastOpenedRunId: nil
    )
}

enum HistoryWorkspaceError: LocalizedError {
    case unreadable(String)
    case corrupt(String)
    case unsupportedSchema(Int)
    case invalidState(String)
    case resourceLimit(String)

    var errorDescription: String? {
        switch self {
        case .unreadable(let detail): return "UI 작업공간 파일을 읽을 수 없습니다: \(detail)"
        case .corrupt(let detail): return "UI 작업공간 파일이 손상되었습니다: \(detail)"
        case .unsupportedSchema(let version): return "지원하지 않는 UI 작업공간 스키마입니다: \(version)"
        case .invalidState(let detail): return "UI 작업공간 값이 유효하지 않습니다: \(detail)"
        case .resourceLimit(let detail): return "UI 작업공간 크기 제한을 초과했습니다: \(detail)"
        }
    }
}

private struct HistoryStateEnvelope: Codable {
    let schemaVersion: Int
    let history: HistoryPresentationState

    private enum CodingKeys: String, CodingKey {
        case schemaVersion = "schema_version"
        case history
    }
}

/// User presentation state only. This store never opens the research SQLite database or settings.
final class HistoryWorkspaceStore {
    private let uiDirectory: URL
    private let historyURL: URL
    private let lock = NSLock()
    private let fileManager: FileManager
    private let encoder: JSONEncoder
    private let decoder = JSONDecoder()
    private static let schemaVersion = 1
    private static let historyFileLimit = 64 * 1024

    init(rootDirectory: URL, fileManager: FileManager = .default) {
        self.fileManager = fileManager
        uiDirectory = rootDirectory.appendingPathComponent("ui", isDirectory: true)
        historyURL = uiDirectory.appendingPathComponent("history-view.json", isDirectory: false)
        encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes]
    }

    func loadHistoryState() throws -> HistoryPresentationState? {
        try withLock {
            guard let envelope = try readHistoryEnvelope() else { return nil }
            try Self.validate(envelope.history)
            return envelope.history
        }
    }

    func saveHistoryState(_ state: HistoryPresentationState) throws {
        try withLock {
            try Self.validate(state)
            if let existing = try readHistoryEnvelope() { try Self.validate(existing.history) }
            let envelope = HistoryStateEnvelope(schemaVersion: Self.schemaVersion, history: state)
            try write(envelope, to: historyURL, maximumBytes: Self.historyFileLimit)
        }
    }

    private func readHistoryEnvelope() throws -> HistoryStateEnvelope? {
        guard let data = try read(historyURL, maximumBytes: Self.historyFileLimit) else { return nil }
        let envelope: HistoryStateEnvelope = try decode(data, source: historyURL.lastPathComponent)
        try validateSchema(envelope.schemaVersion)
        return envelope
    }

    private func read(_ url: URL, maximumBytes: Int) throws -> Data? {
        guard fileManager.fileExists(atPath: url.path) else { return nil }
        do {
            let attributes = try fileManager.attributesOfItem(atPath: url.path)
            guard let size = attributes[.size] as? NSNumber, size.intValue <= maximumBytes else {
                throw HistoryWorkspaceError.resourceLimit("\(url.lastPathComponent)은 \(maximumBytes) bytes 이하여야 합니다")
            }
            let data = try Data(contentsOf: url, options: [.mappedIfSafe])
            guard data.count <= maximumBytes else {
                throw HistoryWorkspaceError.resourceLimit("\(url.lastPathComponent)은 \(maximumBytes) bytes 이하여야 합니다")
            }
            return data
        } catch let error as HistoryWorkspaceError { throw error }
        catch { throw HistoryWorkspaceError.unreadable("\(url.lastPathComponent): \(error.localizedDescription)") }
    }

    private func decode<T: Decodable>(_ data: Data, source: String) throws -> T {
        do { return try decoder.decode(T.self, from: data) }
        catch { throw HistoryWorkspaceError.corrupt("\(source): \(error.localizedDescription)") }
    }

    private func validateSchema(_ version: Int) throws {
        guard version == Self.schemaVersion else { throw HistoryWorkspaceError.unsupportedSchema(version) }
    }

    private func write<T: Encodable>(_ value: T, to url: URL, maximumBytes: Int) throws {
        let data: Data
        do { data = try encoder.encode(value) }
        catch { throw HistoryWorkspaceError.corrupt("저장 JSON 생성 실패: \(error.localizedDescription)") }
        guard data.count <= maximumBytes else {
            throw HistoryWorkspaceError.resourceLimit("\(url.lastPathComponent)은 \(maximumBytes) bytes 이하여야 합니다")
        }
        let temporaryURL = uiDirectory.appendingPathComponent(".\(url.lastPathComponent).\(UUID().uuidString).tmp")
        do {
            try fileManager.createDirectory(at: uiDirectory, withIntermediateDirectories: true)
            try data.write(to: temporaryURL, options: [.withoutOverwriting])
            try fileManager.setAttributes([.posixPermissions: 0o600], ofItemAtPath: temporaryURL.path)
            let result = temporaryURL.path.withCString { source in
                url.path.withCString { destination in Darwin.rename(source, destination) }
            }
            guard result == 0 else {
                throw HistoryWorkspaceError.unreadable(
                    "\(url.lastPathComponent) 교체 실패: \(String(cString: strerror(errno)))"
                )
            }
        } catch {
            try? fileManager.removeItem(at: temporaryURL)
            if let error = error as? HistoryWorkspaceError { throw error }
            throw HistoryWorkspaceError.unreadable("\(url.lastPathComponent) 저장 실패: \(error.localizedDescription)")
        }
    }

    private func withLock<T>(_ operation: () throws -> T) throws -> T {
        lock.lock(); defer { lock.unlock() }
        return try operation()
    }

    private static func validate(_ state: HistoryPresentationState) throws {
        guard state.search.count <= 512 else { throw HistoryWorkspaceError.invalidState("검색어는 512자 이하여야 합니다") }
        for (name, value) in [
            ("status", state.status), ("market", state.market), ("policy_id", state.policyId),
            ("policy_revision_id", state.policyRevisionId), ("cost_assumption_label", state.costAssumptionLabel),
            ("last_opened_run_id", state.lastOpenedRunId)
        ] where (value?.count ?? 0) > 512 {
            throw HistoryWorkspaceError.invalidState("\(name)은 512자 이하여야 합니다")
        }
        try validateUniqueRunIds(state.pinnedRunIds, maximum: 100, field: "고정 실행")
        try validateUniqueRunIds(state.selectedRunIds, maximum: 4, field: "선택 실행")
        if let sort = state.metricSort,
           sort != "return_desc" && sort != "drawdown_asc" {
            throw HistoryWorkspaceError.invalidState("metric_sort는 return_desc 또는 drawdown_asc만 허용합니다")
        }
        if let day = state.fromDayKST { try validateKSTDay(day, field: "시작일") }
        if let day = state.throughDayKST { try validateKSTDay(day, field: "종료일") }
        if state.dateEnabled {
            guard let from = state.fromDayKST, let through = state.throughDayKST else {
                throw HistoryWorkspaceError.invalidState("날짜 필터에는 시작일과 종료일이 모두 필요합니다")
            }
            guard from <= through else { throw HistoryWorkspaceError.invalidState("시작일은 종료일보다 늦을 수 없습니다") }
        }
    }

    private static func validateUniqueRunIds(_ values: [String], maximum: Int, field: String) throws {
        guard values.count <= maximum else { throw HistoryWorkspaceError.resourceLimit("\(field)은 최대 \(maximum)개입니다") }
        guard Set(values).count == values.count else { throw HistoryWorkspaceError.invalidState("\(field)에 중복 ID가 있습니다") }
        try values.forEach(validateRunId)
    }

    private static func validateRunId(_ value: String) throws {
        guard !value.isEmpty, value.count <= 256,
              value.unicodeScalars.allSatisfy({
                  !CharacterSet.whitespacesAndNewlines.contains($0)
                      && !CharacterSet.controlCharacters.contains($0)
              }) else {
            throw HistoryWorkspaceError.invalidState("run_id는 공백 없는 1...256자여야 합니다")
        }
    }

    private static func validateKSTDay(_ value: String, field: String) throws {
        guard value.count == 10,
              value[value.index(value.startIndex, offsetBy: 4)] == "-",
              value[value.index(value.startIndex, offsetBy: 7)] == "-" else {
            throw HistoryWorkspaceError.invalidState("\(field)은 YYYY-MM-DD 형식이어야 합니다")
        }
        let formatter = DateFormatter()
        formatter.calendar = Calendar(identifier: .gregorian)
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(identifier: "Asia/Seoul")
        formatter.dateFormat = "yyyy-MM-dd"
        formatter.isLenient = false
        guard let date = formatter.date(from: value), formatter.string(from: date) == value else {
            throw HistoryWorkspaceError.invalidState("\(field)이 유효한 KST 날짜가 아닙니다")
        }
    }

}
