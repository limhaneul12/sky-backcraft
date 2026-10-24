import Foundation
import SQLite3

private let sqliteTransient = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

enum LiveTradingState: String {
    case standby = "STANDBY"
    case armed = "ARMED"
    case paused = "PAUSED"
    case emergencyStopped = "EMERGENCY_STOP"

    var localizedTitle: String {
        switch self {
        case .standby: return "실거래 대기 (STANDBY)"
        case .armed: return "실거래 가동 중 (ARMED)"
        case .paused: return "실거래 일시중단 (PAUSED)"
        case .emergencyStopped: return "긴급 중단 발동 (EMERGENCY_STOP)"
        }
    }
}

enum LedgerReadError: Error, LocalizedError {
    case invalidLimit(Int)
    case invalidFilter(String)
    case databaseUnavailable(String)
    case queryFailed(operation: String, detail: String)
    case invalidJSON(operation: String, detail: String)
    case invalidDecimal(operation: String, field: String, value: String)
    case invalidTimestamp(operation: String, field: String, value: String)
    case inconsistentData(operation: String, detail: String)
    case modelNotFound(runId: String, modelId: String)
    case accountMarksUnavailable(runId: String, modelId: String, ledgerState: String)

    var errorDescription: String? {
        switch self {
        case .invalidLimit(let limit):
            return "조회 제한값이 유효하지 않습니다: \(limit)"
        case .invalidFilter(let detail):
            return "조회 필터가 유효하지 않습니다: \(detail)"
        case .databaseUnavailable(let detail):
            return "연구 원장을 읽기 전용으로 열 수 없습니다: \(detail)"
        case .queryFailed(let operation, let detail):
            return "\(operation) 조회에 실패했습니다: \(detail)"
        case .invalidJSON(let operation, let detail):
            return "\(operation) JSON을 해석할 수 없습니다: \(detail)"
        case .invalidDecimal(let operation, let field, let value):
            return "\(operation)의 \(field) 값이 유효한 Decimal이 아닙니다: \(value)"
        case .invalidTimestamp(let operation, let field, let value):
            return "\(operation)의 \(field) 값이 유효한 UTC timestamp가 아닙니다: \(value)"
        case .inconsistentData(let operation, let detail):
            return "\(operation) 원장 데이터가 서로 일치하지 않습니다: \(detail)"
        case .modelNotFound(let runId, let modelId):
            return "선택한 모델을 찾을 수 없습니다: \(runId) / \(modelId)"
        case .accountMarksUnavailable(let runId, let modelId, let ledgerState):
            return "선택한 모델의 raw account marks를 읽을 수 없습니다 (ledger=\(ledgerState)): \(runId) / \(modelId)"
        }
    }
}

struct RealRun: Identifiable {
    var id: String
    var state: String
    var createdDateStr: String
    var createdAtMilliseconds: Int64
    var createdAtUTC: String?
    var planId: String
    var ledgerState: String
    var mode: String?
    var engineVersion: String?
    var codeRevision: String?
    var datasetIds: [String]?
    var policyRevisionIds: [String]?
    var markets: [String]?
    var windowStartUTC: String?
    var windowEndUTC: String?
    var capitalMode: String?
    var initialCashDecimal: Decimal?
    var costAssumptionLabel: String?
    var terminalPolicy: String?
    var f08Status: String?
    var f08CheckedFills: Int?
    var f08CheckedMarks: Int?
    var f08FindingsCount: Int?

    var initialCash: Double? {
        initialCashDecimal.map { NSDecimalNumber(decimal: $0).doubleValue }
    }
}

struct RunHistoryFilter {
    var search: String?
    var status: String?
    var market: String?
    var policyId: String?
    var datasetId: String?
    /// Inclusive lower bound for `runs.created_at_ms`.
    var createdFromMilliseconds: Int64?
    /// Exclusive upper bound for `runs.created_at_ms`.
    var createdBeforeMilliseconds: Int64?
    var policyRevisionId: String?
    var costAssumptionLabel: String?
    /// `nil` leaves history unrestricted; an empty array intentionally matches no runs.
    var pinnedRunIds: [String]?

    init(
        search: String? = nil,
        status: String? = nil,
        market: String? = nil,
        policyId: String? = nil,
        datasetId: String? = nil,
        createdFromMilliseconds: Int64? = nil,
        createdBeforeMilliseconds: Int64? = nil,
        policyRevisionId: String? = nil,
        costAssumptionLabel: String? = nil,
        pinnedRunIds: [String]? = nil
    ) {
        self.search = search
        self.status = status
        self.market = market
        self.policyId = policyId
        self.datasetId = datasetId
        self.createdFromMilliseconds = createdFromMilliseconds
        self.createdBeforeMilliseconds = createdBeforeMilliseconds
        self.policyRevisionId = policyRevisionId
        self.costAssumptionLabel = costAssumptionLabel
        self.pinnedRunIds = pinnedRunIds
    }
}

struct RunHistoryRevisionOption: Identifiable {
    let id: String
    let policyId: String
    let revisionNumber: Int
    let name: String
    let definitionDigest: String
}

struct RunHistoryFilterOptions {
    let costAssumptionLabels: [String]
    let policyRevisions: [RunHistoryRevisionOption]
}

struct RunHistoryCursor: Equatable {
    var createdAtMilliseconds: Int64
    var runId: String
}

struct RunHistoryPage {
    var items: [RealRun]
    var returnedCount: Int
    var totalCount: Int
    var nextCursor: RunHistoryCursor?
    var truncatedReason: String?
}

struct RealModelComparison: Identifiable {
    var id: String { "\(runId)-\(modelId)" }
    var runId: String
    var modelId: String
    var market: String
    var status: String
    var policyId: String?
    var policyRevisionId: String?
    var initialEquityDecimal: Decimal
    var finalEquityDecimal: Decimal
    var netPnlDecimal: Decimal
    var netReturnDecimal: Decimal?
    var maxDrawdownDecimal: Decimal?
    var turnoverDecimal: Decimal?
    var netReturnNullReason: String? = nil
    var maxDrawdownNullReason: String? = nil
    var turnoverNullReason: String? = nil
    var cumulativeFeesDecimal: Decimal
    var fillCount: Int
    var accountMarkCount: Int
    var episodeCount: Int
    var closedEpisodes: Int
    var f08Status: String?

    // Presentation adapters only. Economic values remain Decimal above.
    var policyDefinitionDigest: String? = nil
    var initialEquity: Double { NSDecimalNumber(decimal: initialEquityDecimal).doubleValue }
    var finalEquity: Double { NSDecimalNumber(decimal: finalEquityDecimal).doubleValue }
    var netPnl: Double { NSDecimalNumber(decimal: netPnlDecimal).doubleValue }
    var netReturn: Double? { netReturnDecimal.map { NSDecimalNumber(decimal: $0).doubleValue } }
    var maxDrawdown: Double? { maxDrawdownDecimal.map { NSDecimalNumber(decimal: $0).doubleValue } }
    var turnover: Double? { turnoverDecimal.map { NSDecimalNumber(decimal: $0).doubleValue } }
    var cumulativeFees: Double { NSDecimalNumber(decimal: cumulativeFeesDecimal).doubleValue }
}

enum PolicyUsageState: String {
    case historicalOnly = "HISTORICAL_ONLY"
    case unused = "UNUSED"
}

struct RealPolicy: Identifiable {
    var id: String { policyId }
    var policyId: String
    var family: String
    var origin: String
    var name: String
    var headRevisionId: String
    var revisionCount: Int
    var historicalRunCount: Int
    var historicalModelCount: Int
    var lastHistoricalRunDateStr: String?
    var usageState: PolicyUsageState
}

struct RealJob: Identifiable {
    var id: String
    var requestId: String
    var payloadKind: String
    var status: String
    var createdDateStr: String
}

struct RealCollectionSchedule: Identifiable {
    var id: String
    var status: String
    var createdAt: Date
    var nextActionAt: Date
    var lastSuccessDatasetId: String?
    var lastSuccessBoundary: Date?
    var failureCode: String?
}

struct RealAccountMark {
    var modelId: String
    var seq: Int
    var equityDecimal: Decimal
    var cashDecimal: Decimal
    var sourceBarId: String
    var timestamp: Date
    var sourceBarCloseTime: Date? = nil
    var kind: String? = nil
    var drawdownDecimal: Decimal? = nil

    // Chart rendering is the only lossy conversion boundary.
    var equity: Double { NSDecimalNumber(decimal: equityDecimal).doubleValue }
    var cash: Double { NSDecimalNumber(decimal: cashDecimal).doubleValue }
}

private struct RunManifest: Decodable {
    let createdAt: String?
    let codeRevision: String?
    let engineVersion: String?
    let mode: String?

    private enum CodingKeys: String, CodingKey {
        case createdAt = "created_at"
        case codeRevision = "code_revision"
        case engineVersion = "engine_version"
        case mode
    }
}

private struct ResolvedPlan: Decodable {
    let spec: PlanSpec
}

private struct PlanSpec: Decodable {
    let datasetIds: [String]?
    let markets: [String]?
    let range: PlanRange?
    let policySelections: [PlanPolicySelection]?
    let capitalMode: String?
    let initialCash: String?
    let costs: PlanCosts?
    let terminalPolicy: String?

    private enum CodingKeys: String, CodingKey {
        case datasetIds = "dataset_ids"
        case markets
        case range
        case policySelections = "policy_selections"
        case capitalMode = "capital_mode"
        case initialCash = "initial_cash"
        case costs
        case terminalPolicy = "terminal_policy"
    }
}

private struct PlanRange: Decodable {
    let start: String?
    let end: String?
}

private struct PlanPolicySelection: Decodable {
    let revisionId: String

    private enum CodingKeys: String, CodingKey {
        case revisionId = "revision_id"
    }
}

private struct PlanCosts: Decodable {
    let assumptionLabel: String?

    private enum CodingKeys: String, CodingKey {
        case assumptionLabel = "assumption_label"
    }
}

private struct F08Report: Decodable {
    let status: String
    let runId: String
    let checkedFills: Int
    let checkedMarks: Int
    let findings: [F08Finding]

    private enum CodingKeys: String, CodingKey {
        case status
        case runId = "run_id"
        case checkedFills = "checked_fills"
        case checkedMarks = "checked_marks"
        case findings
    }
}

private struct F08Finding: Decodable {}

private struct ScheduleFailure: Decodable {
    let code: String?
}

private struct AccountMarkRecord: Decodable {
    let context: AccountMarkContext
    let sourceBarId: String
    let kind: String?
    let drawdown: String?

    private enum CodingKeys: String, CodingKey {
        case context
        case sourceBarId = "source_bar_id"
        case kind
        case drawdown
    }
}

private struct AccountMarkContext: Decodable {
    let runId: String
    let modelId: String
    let eventSeq: Int64
    let accountingEventTime: String

    private enum CodingKeys: String, CodingKey {
        case runId = "run_id"
        case modelId = "model_id"
        case eventSeq = "event_seq"
        case accountingEventTime = "accounting_event_time"
    }
}

extension DatabaseReader {
    static let allowedMetricNullReasons: Set<String> = [
        "NO_ACCOUNT_MARKS", "INSUFFICIENT_SAMPLES", "INSUFFICIENT_CALENDAR_DURATION",
        "ZERO_DENOMINATOR", "NO_CLOSED_EPISODES", "NO_LOSING_EPISODES",
        "NO_POSITIVE_PROFIT", "DATA_NOT_CAPTURED", "NOT_APPLICABLE", "NON_FINITE_RESULT"
    ]
}

private struct DecimalMetric: Decodable {
    let value: Decimal?
    let nullReason: String?

    private enum CodingKeys: String, CodingKey {
        case value
        case nullReason = "null_reason"
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let statistic = try container.decodeIfPresent(Double.self, forKey: .value)
        let reason = try container.decodeIfPresent(String.self, forKey: .nullReason)
        switch (statistic, reason) {
        case (.some(let statistic), nil):
            guard statistic.isFinite else {
                throw DecodingError.dataCorruptedError(forKey: .value, in: container,
                    debugDescription: "metric value must be finite")
            }
            let displayValue = Decimal(statistic)
            guard !displayValue.isNaN else {
                throw DecodingError.dataCorruptedError(forKey: .value, in: container,
                    debugDescription: "metric value is outside the native Decimal display range")
            }
            value = displayValue
            nullReason = nil
        case (nil, .some(let reason)):
            guard DatabaseReader.allowedMetricNullReasons.contains(reason) else {
                throw DecodingError.dataCorruptedError(forKey: .nullReason, in: container,
                    debugDescription: "metric null_reason is not recognized")
            }
            value = nil
            nullReason = reason
        case (.some, .some):
            throw DecodingError.dataCorruptedError(forKey: .nullReason, in: container,
                debugDescription: "metric must contain value or null_reason, not both")
        case (nil, nil):
            throw DecodingError.dataCorruptedError(forKey: .value, in: container,
                debugDescription: "metric requires value or null_reason")
        }
    }
}

private struct ModelComparisonRecord: Decodable {
    let runId: String
    let modelId: String
    let market: String
    let status: String
    let initialEquity: String?
    let finalEquity: String?
    let netPnl: String?
    let netReturn: DecimalMetric
    let maxDrawdown: DecimalMetric
    let turnover: DecimalMetric
    let cumulativeFees: String?
    let closedEpisodes: Int?

    private enum CodingKeys: String, CodingKey {
        case runId = "run_id"
        case modelId = "model_id"
        case market
        case status
        case initialEquity = "initial_equity"
        case finalEquity = "final_equity"
        case netPnl = "net_pnl"
        case netReturn = "net_return"
        case maxDrawdown = "max_drawdown"
        case turnover
        case cumulativeFees = "cumulative_fees"
        case closedEpisodes = "closed_episodes"
    }
}

/// Thread-safe generation gate for coalescing rapid UI filter/selection reads.
/// Only the latest generation token is permitted to commit results to UI state.
final class ReadGenerationGate: @unchecked Sendable {
    private let lock = NSLock()
    private var generation: UInt64 = 0

    func begin() -> UInt64 {
        lock.lock()
        defer { lock.unlock() }
        generation &+= 1
        return generation
    }

    func isCurrent(_ token: UInt64) -> Bool {
        lock.lock()
        defer { lock.unlock() }
        return token == generation
    }
}

final class DatabaseReader: @unchecked Sendable {
    static let shared = DatabaseReader()
    private let dbPath: String
    private let readQueue = DispatchQueue(
        label: "com.skybackcraft.database.reader",
        qos: .userInitiated,
        attributes: .concurrent
    )

    init(supportDirectory: URL? = nil) {
        if let dir = supportDirectory {
            dbPath = dir.appendingPathComponent("research/lab.sqlite").path
        } else {
            let appSupport = FileManager.default.urls(
                for: .applicationSupportDirectory,
                in: .userDomainMask
            ).first!
            dbPath = appSupport.appendingPathComponent("Sky Backcraft/research/lab.sqlite").path
        }
    }

    /// Execute a read closure on the bounded concurrent reader queue, delivering
    /// the result on the main queue only if `gate` confirms `token` is still current.
    func performLatestRead<T>(
        gate: ReadGenerationGate,
        work: @escaping (DatabaseReader) throws -> T,
        completion: @escaping (Result<T, Error>) -> Void
    ) {
        let token = gate.begin()
        readQueue.async { [self] in
            guard gate.isCurrent(token) else { return }
            let result = Result { try work(self) }
            guard gate.isCurrent(token) else { return }
            DispatchQueue.main.async {
                guard gate.isCurrent(token) else { return }
                completion(result)
            }
        }
    }

    private func openDB() throws -> OpaquePointer {
        var db: OpaquePointer?
        let flags = SQLITE_OPEN_READONLY | SQLITE_OPEN_FULLMUTEX
        let result = sqlite3_open_v2(dbPath, &db, flags, nil)
        guard result == SQLITE_OK, let db else {
            let detail = db.map { String(cString: sqlite3_errmsg($0)) } ?? "SQLite error \(result)"
            if let db { sqlite3_close(db) }
            throw LedgerReadError.databaseUnavailable(detail)
        }
        sqlite3_busy_timeout(db, 5_000)
        _ = sqlite3_exec(db, "PRAGMA query_only = ON;", nil, nil, nil)
        return db
    }

    private func beginReadTransaction(_ db: OpaquePointer, operation: String) throws {
        var errorPointer: UnsafeMutablePointer<CChar>?
        let result = sqlite3_exec(db, "BEGIN DEFERRED TRANSACTION;", nil, nil, &errorPointer)
        guard result == SQLITE_OK else {
            let detail = errorPointer.map { String(cString: $0) } ?? String(cString: sqlite3_errmsg(db))
            if let errorPointer { sqlite3_free(errorPointer) }
            throw LedgerReadError.queryFailed(operation: operation, detail: detail)
        }
        if let errorPointer { sqlite3_free(errorPointer) }
    }

    private func endReadTransaction(_ db: OpaquePointer) {
        sqlite3_exec(db, "ROLLBACK;", nil, nil, nil)
    }

    private func prepare(_ db: OpaquePointer, sql: String, operation: String) throws -> OpaquePointer {
        var statement: OpaquePointer?
        let result = sqlite3_prepare_v2(db, sql, -1, &statement, nil)
        guard result == SQLITE_OK, let statement else {
            throw LedgerReadError.queryFailed(operation: operation, detail: String(cString: sqlite3_errmsg(db)))
        }
        return statement
    }

    private func bind(_ value: String, at index: Int32, to statement: OpaquePointer) {
        sqlite3_bind_text(statement, index, value, -1, sqliteTransient)
    }

    private func bindOptional(_ value: String?, at index: Int32, to statement: OpaquePointer) {
        if let value {
            bind(value, at: index, to: statement)
        } else {
            sqlite3_bind_null(statement, index)
        }
    }

    private func requiredText(_ statement: OpaquePointer, column: Int32, operation: String, field: String) throws -> String {
        guard let pointer = sqlite3_column_text(statement, column) else {
            throw LedgerReadError.inconsistentData(operation: operation, detail: "\(field) is NULL")
        }
        return String(cString: pointer)
    }

    private func optionalText(_ statement: OpaquePointer, column: Int32) -> String? {
        sqlite3_column_text(statement, column).map { String(cString: $0) }
    }

    private func requiredColumnBytes(
        _ statement: OpaquePointer, column: Int32, operation: String, field: String
    ) throws -> UnsafeBufferPointer<UInt8> {
        guard let pointer = sqlite3_column_text(statement, column) else {
            throw LedgerReadError.inconsistentData(operation: operation, detail: "\(field) is NULL")
        }
        return UnsafeBufferPointer(start: pointer, count: Int(sqlite3_column_bytes(statement, column)))
    }

    private func decimal(_ value: String, operation: String, field: String) throws -> Decimal {
        guard let parsed = StrictDecimal.parse(value) else {
            throw LedgerReadError.invalidDecimal(operation: operation, field: field, value: value)
        }
        return parsed
    }

    private func validateDecimalColumn(
        _ statement: OpaquePointer, column: Int32, operation: String, field: String
    ) throws -> UnsafeBufferPointer<UInt8> {
        let bytes = try requiredColumnBytes(statement, column: column, operation: operation, field: field)
        guard StrictDecimal.isValid(bytes) else {
            let value = String(decoding: bytes, as: UTF8.self)
            throw LedgerReadError.invalidDecimal(operation: operation, field: field, value: value)
        }
        return bytes
    }

    private func utcDate(
        _ value: String, operation: String, field: String, parser: ReadTimestampParser
    ) throws -> Date {
        if let parsed = parser.ordinaryThenFractional(value) { return parsed }
        throw LedgerReadError.invalidTimestamp(operation: operation, field: field, value: value)
    }

    private func decode<T: Decodable>(
        _ type: T.Type, text: String, operation: String, decoder: JSONDecoder = JSONDecoder()
    ) throws -> T {
        do {
            return try decoder.decode(type, from: Data(text.utf8))
        } catch {
            throw LedgerReadError.invalidJSON(operation: operation, detail: error.localizedDescription)
        }
    }

    private func decodeColumn<T: Decodable>(
        _ type: T.Type, statement: OpaquePointer, column: Int32, field: String,
        operation: String, jsonOperation: String, decoder: JSONDecoder
    ) throws -> T {
        let bytes = try requiredColumnBytes(statement, column: column, operation: operation, field: field)
        guard let baseAddress = bytes.baseAddress else {
            throw LedgerReadError.invalidJSON(operation: jsonOperation, detail: "empty JSON column")
        }
        let data = Data(
            bytesNoCopy: UnsafeMutableRawPointer(mutating: baseAddress),
            count: bytes.count,
            deallocator: .none
        )
        do {
            return try decoder.decode(type, from: data)
        } catch {
            throw LedgerReadError.invalidJSON(operation: jsonOperation, detail: error.localizedDescription)
        }
    }

    private func ensureDone(_ result: Int32, db: OpaquePointer, operation: String) throws {
        guard result == SQLITE_DONE else {
            throw LedgerReadError.queryFailed(operation: operation, detail: String(cString: sqlite3_errmsg(db)))
        }
    }

    func fetchRuns(limit: Int = 100) throws -> [RealRun] {
        try fetchRunHistoryPage(limit: limit).items
    }

    func fetchRunHistoryPage(
        filter: RunHistoryFilter = RunHistoryFilter(),
        cursor: RunHistoryCursor? = nil,
        limit: Int = 100
    ) throws -> RunHistoryPage {
        guard (1...100).contains(limit) else { throw LedgerReadError.invalidLimit(limit) }
        if let from = filter.createdFromMilliseconds, let before = filter.createdBeforeMilliseconds, from >= before {
            throw LedgerReadError.invalidFilter("기록 시작일은 마지막 날보다 늦을 수 없습니다")
        }
        if let pinnedRunIds = filter.pinnedRunIds, pinnedRunIds.count > 100 {
            throw LedgerReadError.invalidFilter("고정 실행 ID는 최대 100개까지 선택할 수 있습니다")
        }
        let operation = "run history"
        let db = try openDB()
        defer { sqlite3_close(db) }

        let pinnedSQL: String
        if let pinnedRunIds = filter.pinnedRunIds {
            pinnedSQL = pinnedRunIds.isEmpty
                ? "AND 0"
                : "AND r.id IN (\((0..<pinnedRunIds.count).map { "?\(13 + $0)" }.joined(separator: ",")))"
        } else { pinnedSQL = "" }
        let filterSQL = """
        WHERE (?1 IS NULL
               OR instr(lower(r.id), ?1) > 0
               OR instr(lower(r.plan_id), ?1) > 0
               OR EXISTS (
                    SELECT 1 FROM run_models AS sm
                    LEFT JOIN policies AS sp ON sp.policy_id = sm.policy_id
                    WHERE sm.run_id = r.id
                      AND (instr(lower(sm.model_id), ?1) > 0
                           OR instr(lower(sm.market), ?1) > 0
                           OR instr(lower(COALESCE(sm.policy_id, '')), ?1) > 0
                           OR instr(lower(COALESCE(sp.name, '')), ?1) > 0)
               )
               OR EXISTS (
                    SELECT 1 FROM plan_datasets AS sd
                    WHERE sd.plan_id = r.plan_id
                      AND instr(lower(sd.dataset_id), ?1) > 0
               ))
          AND (?2 IS NULL OR r.state = ?2)
          AND (?3 IS NULL OR EXISTS (
                SELECT 1 FROM run_models AS mm
                WHERE mm.run_id = r.id AND mm.market = ?3
          ))
          AND ((?4 IS NULL AND ?9 IS NULL) OR EXISTS (
                SELECT 1 FROM run_models AS pm
                WHERE pm.run_id = r.id
                  AND (?4 IS NULL OR pm.policy_id = ?4)
                  AND (?9 IS NULL OR pm.policy_revision_id = ?9)
          ))
          AND (?5 IS NULL OR EXISTS (
                SELECT 1 FROM plan_datasets AS pd
                WHERE pd.plan_id = r.plan_id AND pd.dataset_id = ?5
          ))
          AND (?10 IS NULL OR r.created_at_ms >= ?10)
          AND (?11 IS NULL OR r.created_at_ms < ?11)
          AND (?12 IS NULL OR json_extract(p.resolved_plan_json, '$.spec.costs.assumption_label') = ?12)
          \(pinnedSQL)
        """
        let normalizedSearch = filter.search?
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .lowercased()
        let search = normalizedSearch?.isEmpty == false ? normalizedSearch : nil

        let countSQL = "SELECT COUNT(*) FROM runs AS r JOIN plans AS p ON p.id = r.plan_id \(filterSQL);"
        let countStatement = try prepare(db, sql: countSQL, operation: operation)
        defer { sqlite3_finalize(countStatement) }
        bindHistoryFilter(filter, search: search, to: countStatement)
        guard sqlite3_step(countStatement) == SQLITE_ROW else {
            throw LedgerReadError.queryFailed(operation: operation, detail: String(cString: sqlite3_errmsg(db)))
        }
        let totalCount = Int(sqlite3_column_int64(countStatement, 0))

        let pageSQL = """
        SELECT r.id, r.state, r.created_at_ms,
               datetime(r.created_at_ms / 1000, 'unixepoch', '+9 hours'),
               r.plan_id, r.ledger_state, r.manifest_json, p.resolved_plan_json,
               v.status, v.report_json
        FROM runs AS r
        JOIN plans AS p ON p.id = r.plan_id
        LEFT JOIN validation_results AS v
          ON v.run_id = r.id AND v.check_id = 'F08_INDEPENDENT_LEDGER_V1'
        \(filterSQL)
          AND (?6 IS NULL
               OR r.created_at_ms < ?6
               OR (r.created_at_ms = ?6 AND r.id < ?7))
        ORDER BY r.created_at_ms DESC, r.id DESC
        LIMIT ?8;
        """
        let statement = try prepare(db, sql: pageSQL, operation: operation)
        defer { sqlite3_finalize(statement) }
        bindHistoryFilter(filter, search: search, to: statement)
        if let cursor {
            sqlite3_bind_int64(statement, 6, cursor.createdAtMilliseconds)
            bind(cursor.runId, at: 7, to: statement)
        } else {
            sqlite3_bind_null(statement, 6)
            sqlite3_bind_null(statement, 7)
        }
        sqlite3_bind_int64(statement, 8, Int64(limit + 1))

        var results: [RealRun] = []
        var step = sqlite3_step(statement)
        while step == SQLITE_ROW {
            results.append(try readRun(statement, operation: operation))
            step = sqlite3_step(statement)
        }
        try ensureDone(step, db: db, operation: operation)
        let hasMore = results.count > limit
        if hasMore { results.removeLast() }
        let nextCursor = hasMore ? results.last.map {
            RunHistoryCursor(createdAtMilliseconds: $0.createdAtMilliseconds, runId: $0.id)
        } : nil
        return RunHistoryPage(
            items: results,
            returnedCount: results.count,
            totalCount: totalCount,
            nextCursor: nextCursor,
            truncatedReason: hasMore ? "LIMIT" : nil
        )
    }

    private func bindHistoryFilter(
        _ filter: RunHistoryFilter,
        search: String?,
        to statement: OpaquePointer
    ) {
        bindOptional(search, at: 1, to: statement)
        bindOptional(filter.status, at: 2, to: statement)
        bindOptional(filter.market, at: 3, to: statement)
        bindOptional(filter.policyId, at: 4, to: statement)
        bindOptional(filter.datasetId, at: 5, to: statement)
        bindOptional(filter.policyRevisionId, at: 9, to: statement)
        if let value = filter.createdFromMilliseconds { sqlite3_bind_int64(statement, 10, value) }
        else { sqlite3_bind_null(statement, 10) }
        if let value = filter.createdBeforeMilliseconds { sqlite3_bind_int64(statement, 11, value) }
        else { sqlite3_bind_null(statement, 11) }
        bindOptional(filter.costAssumptionLabel, at: 12, to: statement)
        for (offset, runId) in (filter.pinnedRunIds ?? []).enumerated() {
            bind(runId, at: Int32(13 + offset), to: statement)
        }
    }

    func fetchHistoryFilterOptions() throws -> RunHistoryFilterOptions {
        let operation = "run history filter options"
        let db = try openDB()
        defer { sqlite3_close(db) }

        let costStatement = try prepare(db, sql: """
        SELECT DISTINCT json_extract(resolved_plan_json, '$.spec.costs.assumption_label')
        FROM plans
        WHERE json_type(resolved_plan_json, '$.spec.costs.assumption_label') = 'text'
        ORDER BY 1;
        """, operation: operation)
        defer { sqlite3_finalize(costStatement) }
        var costs: [String] = []
        var step = sqlite3_step(costStatement)
        while step == SQLITE_ROW {
            costs.append(try requiredText(costStatement, column: 0, operation: operation, field: "cost_assumption_label"))
            step = sqlite3_step(costStatement)
        }
        try ensureDone(step, db: db, operation: operation)

        let revisionStatement = try prepare(db, sql: """
        SELECT revision_id, policy_id, revision_number, name, definition_digest
        FROM policy_revisions
        ORDER BY name COLLATE NOCASE, policy_id, revision_number DESC;
        """, operation: operation)
        defer { sqlite3_finalize(revisionStatement) }
        var revisions: [RunHistoryRevisionOption] = []
        step = sqlite3_step(revisionStatement)
        while step == SQLITE_ROW {
            let revisionNumber = Int(sqlite3_column_int64(revisionStatement, 2))
            let digest = try requiredText(revisionStatement, column: 4, operation: operation, field: "definition_digest")
            guard revisionNumber > 0, digest.count == 64, digest.allSatisfy({ $0.isHexDigit }) else {
                throw LedgerReadError.inconsistentData(operation: operation, detail: "invalid revision number or digest")
            }
            revisions.append(RunHistoryRevisionOption(
                id: try requiredText(revisionStatement, column: 0, operation: operation, field: "revision_id"),
                policyId: try requiredText(revisionStatement, column: 1, operation: operation, field: "policy_id"),
                revisionNumber: revisionNumber,
                name: try requiredText(revisionStatement, column: 3, operation: operation, field: "name"),
                definitionDigest: digest
            ))
            step = sqlite3_step(revisionStatement)
        }
        try ensureDone(step, db: db, operation: operation)
        guard Set(costs).count == costs.count,
              Set(revisions.map { "\($0.policyId):\($0.id)" }).count == revisions.count else {
            throw LedgerReadError.inconsistentData(operation: operation, detail: "duplicate filter option identity")
        }
        return RunHistoryFilterOptions(costAssumptionLabels: costs, policyRevisions: revisions)
    }

    private func readRun(_ statement: OpaquePointer, operation: String) throws -> RealRun {
        let runId = try requiredText(statement, column: 0, operation: operation, field: "run_id")
        let manifestText = try requiredText(statement, column: 6, operation: operation, field: "manifest_json")
        let planText = try requiredText(statement, column: 7, operation: operation, field: "resolved_plan_json")
        let manifest: RunManifest = try decode(RunManifest.self, text: manifestText, operation: "run \(runId) manifest")
        let plan: ResolvedPlan = try decode(ResolvedPlan.self, text: planText, operation: "run \(runId) plan")
        let initialCash = try plan.spec.initialCash.map {
            try decimal($0, operation: "run \(runId) plan", field: "initial_cash")
        }
        var f08Report: F08Report?
        if let reportText = optionalText(statement, column: 9) {
            f08Report = try decode(F08Report.self, text: reportText, operation: "run \(runId) F08")
            guard f08Report?.runId == runId, f08Report?.status == optionalText(statement, column: 8) else {
                throw LedgerReadError.inconsistentData(operation: "run \(runId) F08", detail: "validation row and report identity/status differ")
            }
        }
        return RealRun(
            id: runId,
            state: try requiredText(statement, column: 1, operation: operation, field: "state"),
            createdDateStr: try requiredText(statement, column: 3, operation: operation, field: "created_at"),
            createdAtMilliseconds: sqlite3_column_int64(statement, 2),
            createdAtUTC: manifest.createdAt,
            planId: try requiredText(statement, column: 4, operation: operation, field: "plan_id"),
            ledgerState: try requiredText(statement, column: 5, operation: operation, field: "ledger_state"),
            mode: manifest.mode,
            engineVersion: manifest.engineVersion,
            codeRevision: manifest.codeRevision,
            datasetIds: plan.spec.datasetIds,
            policyRevisionIds: plan.spec.policySelections?.map(\.revisionId),
            markets: plan.spec.markets,
            windowStartUTC: plan.spec.range?.start,
            windowEndUTC: plan.spec.range?.end,
            capitalMode: plan.spec.capitalMode,
            initialCashDecimal: initialCash,
            costAssumptionLabel: plan.spec.costs?.assumptionLabel,
            terminalPolicy: plan.spec.terminalPolicy,
            f08Status: f08Report?.status,
            f08CheckedFills: f08Report?.checkedFills,
            f08CheckedMarks: f08Report?.checkedMarks,
            f08FindingsCount: f08Report?.findings.count
        )
    }

    func fetchModelComparisons(for runId: String) throws -> [RealModelComparison] {
        try fetchModelComparisons(for: [runId])
    }

    func fetchModelComparisons(for runIds: [String]) throws -> [RealModelComparison] {
        let uniqueRunIds = Array(Set(runIds)).sorted()
        guard !uniqueRunIds.isEmpty else { return [] }
        guard uniqueRunIds.count <= 100 else {
            throw LedgerReadError.invalidFilter("모델 비교 실행 ID는 최대 100개까지 조회할 수 있습니다")
        }
        let operation = "model comparisons"
        let db = try openDB()
        defer { sqlite3_close(db) }
        let placeholders = Array(repeating: "?", count: uniqueRunIds.count).joined(separator: ",")
        let sql = """
        SELECT c.run_id, c.model_id, c.comparison_json, rm.market, rm.status,
               rm.fill_count, rm.account_mark_count, rm.episode_count,
               rm.policy_id, rm.policy_revision_id, v.status, rm.policy_definition_digest,
               pr.definition_digest
        FROM run_model_comparisons AS c
        JOIN run_models AS rm ON rm.run_id = c.run_id AND rm.model_id = c.model_id
        LEFT JOIN policy_revisions AS pr
          ON pr.policy_id = rm.policy_id AND pr.revision_id = rm.policy_revision_id
        LEFT JOIN validation_results AS v
          ON v.run_id = c.run_id AND v.check_id = 'F08_INDEPENDENT_LEDGER_V1'
        WHERE c.run_id IN (\(placeholders))
        ORDER BY c.run_id, rm.market, c.model_id;
        """
        let statement = try prepare(db, sql: sql, operation: operation)
        defer { sqlite3_finalize(statement) }
        for (offset, runId) in uniqueRunIds.enumerated() {
            bind(runId, at: Int32(offset + 1), to: statement)
        }
        return try readModelComparisons(statement, expectedRunIds: Set(uniqueRunIds), operation: operation, db: db)
    }

    private func readModelComparisons(
        _ statement: OpaquePointer,
        expectedRunIds: Set<String>,
        operation: String,
        db: OpaquePointer
    ) throws -> [RealModelComparison] {
        var results: [RealModelComparison] = []
        var step = sqlite3_step(statement)
        while step == SQLITE_ROW {
            let storedRunId = try requiredText(statement, column: 0, operation: operation, field: "run_id")
            let modelId = try requiredText(statement, column: 1, operation: operation, field: "model_id")
            let recordText = try requiredText(statement, column: 2, operation: operation, field: "comparison_json")
            let record: ModelComparisonRecord = try decode(ModelComparisonRecord.self, text: recordText, operation: "model comparison \(storedRunId)/\(modelId)")
            let market = try requiredText(statement, column: 3, operation: operation, field: "market")
            let persistedStatus = optionalText(statement, column: 4)
            guard expectedRunIds.contains(storedRunId),
                  record.runId == storedRunId,
                  record.modelId == modelId,
                  record.market == market,
                  persistedStatus == nil || persistedStatus == record.status else {
                throw LedgerReadError.inconsistentData(operation: "model comparison \(storedRunId)/\(modelId)", detail: "comparison JSON disagrees with run_models")
            }
            let policyDigest = optionalText(statement, column: 11)
            let policyId = optionalText(statement, column: 8)
            let policyRevisionId = optionalText(statement, column: 9)
            guard (policyId == nil) == (policyRevisionId == nil),
                  (policyId == nil) == (policyDigest == nil) else {
                throw LedgerReadError.inconsistentData(operation: operation, detail: "incomplete policy revision reference")
            }
            if let policyDigest {
                guard policyDigest.count == 64,
                      policyDigest.allSatisfy({ "0123456789abcdef".contains($0) }),
                      policyDigest == optionalText(statement, column: 12) else {
                    throw LedgerReadError.inconsistentData(operation: operation, detail: "invalid policy definition digest")
                }
            }
            if record.status != "COMPLETED",
               record.initialEquity == nil,
               record.finalEquity == nil,
               record.netPnl == nil,
               record.cumulativeFees == nil,
               record.closedEpisodes == nil {
                step = sqlite3_step(statement)
                continue
            }
            guard let initialEquity = record.initialEquity,
                  let finalEquity = record.finalEquity,
                  let netPnl = record.netPnl,
                  let cumulativeFees = record.cumulativeFees,
                  let closedEpisodes = record.closedEpisodes else {
                throw LedgerReadError.inconsistentData(
                    operation: "model comparison \(storedRunId)/\(modelId)",
                    detail: "completed model comparison requires financial summary fields"
                )
            }
            results.append(RealModelComparison(
                runId: storedRunId,
                modelId: modelId,
                market: market,
                status: record.status,
                policyId: policyId,
                policyRevisionId: policyRevisionId,
                initialEquityDecimal: try decimal(initialEquity, operation: operation, field: "initial_equity"),
                finalEquityDecimal: try decimal(finalEquity, operation: operation, field: "final_equity"),
                netPnlDecimal: try decimal(netPnl, operation: operation, field: "net_pnl"),
                netReturnDecimal: record.netReturn.value,
                maxDrawdownDecimal: record.maxDrawdown.value,
                turnoverDecimal: record.turnover.value,
                netReturnNullReason: record.netReturn.nullReason,
                maxDrawdownNullReason: record.maxDrawdown.nullReason,
                turnoverNullReason: record.turnover.nullReason,
                cumulativeFeesDecimal: try decimal(cumulativeFees, operation: operation, field: "cumulative_fees"),
                fillCount: Int(sqlite3_column_int64(statement, 5)),
                accountMarkCount: Int(sqlite3_column_int64(statement, 6)),
                episodeCount: Int(sqlite3_column_int64(statement, 7)),
                closedEpisodes: closedEpisodes,
                f08Status: optionalText(statement, column: 10),
                policyDefinitionDigest: policyDigest
            ))
            step = sqlite3_step(statement)
        }
        try ensureDone(step, db: db, operation: operation)
        return results
    }

    func fetchAccountMarks(for runId: String, modelId: String, maxPoints: Int = 1000) throws -> [RealAccountMark] {
        guard maxPoints >= 2 else { throw LedgerReadError.invalidLimit(maxPoints) }
        let operation = "account marks"
        let db = try openDB()
        defer { sqlite3_close(db) }
        try beginReadTransaction(db, operation: operation)
        defer { endReadTransaction(db) }

        let contextSQL = """
        SELECT r.ledger_state
        FROM run_models AS rm
        JOIN runs AS r ON r.id = rm.run_id
        WHERE rm.run_id = ? AND rm.model_id = ?;
        """
        let contextStatement = try prepare(db, sql: contextSQL, operation: operation)
        defer { sqlite3_finalize(contextStatement) }
        bind(runId, at: 1, to: contextStatement)
        bind(modelId, at: 2, to: contextStatement)
        let contextStep = sqlite3_step(contextStatement)
        guard contextStep == SQLITE_ROW else {
            let detail = contextStep == SQLITE_DONE ? nil : String(cString: sqlite3_errmsg(db))
            if let detail { throw LedgerReadError.queryFailed(operation: operation, detail: detail) }
            throw LedgerReadError.modelNotFound(runId: runId, modelId: modelId)
        }
        let ledgerState = try requiredText(contextStatement, column: 0, operation: operation, field: "ledger_state")

        let countSQL = """
        SELECT COUNT(*)
        FROM account_marks AS am
        JOIN candle_observations AS c ON c.id = am.source_bar_id
        WHERE am.run_id = ? AND am.model_id = ?;
        """
        let countStatement = try prepare(db, sql: countSQL, operation: operation)
        defer { sqlite3_finalize(countStatement) }
        bind(runId, at: 1, to: countStatement)
        bind(modelId, at: 2, to: countStatement)
        let countStep = sqlite3_step(countStatement)
        guard countStep == SQLITE_ROW else {
            throw LedgerReadError.queryFailed(operation: operation, detail: String(cString: sqlite3_errmsg(db)))
        }
        let countValue = sqlite3_column_int64(countStatement, 0)
        try ensureDone(sqlite3_step(countStatement), db: db, operation: operation)
        guard let rowCount = Int(exactly: countValue), rowCount >= 0 else {
            throw LedgerReadError.inconsistentData(operation: operation, detail: "account mark count is outside Int range")
        }

        let sql = """
        SELECT am.model_id, am.event_seq, am.equity_decimal, am.cash_total_decimal,
               am.source_bar_id, am.record_json, c.close_time_ms
        FROM account_marks AS am
        JOIN candle_observations AS c ON c.id = am.source_bar_id
        WHERE am.run_id = ? AND am.model_id = ?
        ORDER BY am.event_seq ASC;
        """
        let statement = try prepare(db, sql: sql, operation: operation)
        defer { sqlite3_finalize(statement) }
        bind(runId, at: 1, to: statement)
        bind(modelId, at: 2, to: statement)

        var marks: [RealAccountMark] = []
        let sampleCount = min(rowCount, maxPoints)
        marks.reserveCapacity(sampleCount)
        let downsampling = rowCount > maxPoints
        let sampleDenominator = maxPoints - 1
        let sampleDistance = downsampling ? rowCount - 1 : 0
        let sampleStep = downsampling ? sampleDistance / sampleDenominator : 1
        let sampleRemainder = downsampling ? sampleDistance % sampleDenominator : 0
        var remainderCarry = 0
        var nextSampleIndex = 0
        var rowIndex = 0
        let timestampParser = ReadTimestampParser()
        let jsonDecoder = JSONDecoder()
        let jsonOperation = "account mark \(runId)/\(modelId)"
        let modelIdUTF8 = Array(modelId.utf8)
        var step = sqlite3_step(statement)
        while step == SQLITE_ROW {
            guard rowIndex < rowCount else {
                throw LedgerReadError.inconsistentData(operation: operation, detail: "account mark scan exceeded its snapshot count")
            }
            let modelBytes = try requiredColumnBytes(statement, column: 0, operation: operation, field: "model_id")
            guard modelBytes.elementsEqual(modelIdUTF8) else {
                throw LedgerReadError.inconsistentData(operation: operation, detail: "query returned a different model_id")
            }
            let equityBytes = try validateDecimalColumn(statement, column: 2, operation: operation, field: "equity_decimal")
            let cashBytes = try validateDecimalColumn(statement, column: 3, operation: operation, field: "cash_total_decimal")
            let sourceBarBytes = try requiredColumnBytes(statement, column: 4, operation: operation, field: "source_bar_id")
            let record: AccountMarkRecord = try decodeColumn(
                AccountMarkRecord.self,
                statement: statement,
                column: 5,
                field: "record_json",
                operation: operation,
                jsonOperation: jsonOperation,
                decoder: jsonDecoder
            )
            let eventSequence = sqlite3_column_int64(statement, 1)
            guard record.context.runId == runId,
                  record.context.modelId == modelId,
                  record.context.eventSeq == eventSequence,
                  sourceBarBytes.elementsEqual(record.sourceBarId.utf8) else {
                throw LedgerReadError.inconsistentData(
                    operation: operation,
                    detail: "record_json context/source identity differs from indexed columns"
                )
            }
            let closeTimeMilliseconds = sqlite3_column_int64(statement, 6)
            let timestamp = try utcDate(
                record.context.accountingEventTime,
                operation: operation,
                field: "context.accounting_event_time",
                parser: timestampParser
            )
            if let drawdownText = record.drawdown, !StrictDecimal.isValid(drawdownText) {
                throw LedgerReadError.invalidDecimal(
                    operation: operation,
                    field: "record_json.drawdown",
                    value: drawdownText
                )
            }
            if !downsampling || rowIndex == nextSampleIndex {
                let equity = try decimal(
                    String(decoding: equityBytes, as: UTF8.self),
                    operation: operation,
                    field: "equity_decimal"
                )
                let cash = try decimal(
                    String(decoding: cashBytes, as: UTF8.self),
                    operation: operation,
                    field: "cash_total_decimal"
                )
                let drawdown = try record.drawdown.map {
                    try decimal($0, operation: operation, field: "record_json.drawdown")
                }
                marks.append(RealAccountMark(
                    modelId: modelId,
                    seq: Int(eventSequence),
                    equityDecimal: equity,
                    cashDecimal: cash,
                    sourceBarId: record.sourceBarId,
                    timestamp: timestamp,
                    sourceBarCloseTime: Date(
                        timeIntervalSince1970: TimeInterval(closeTimeMilliseconds) / 1000.0
                    ),
                    kind: record.kind,
                    drawdownDecimal: drawdown
                ))
                if downsampling, marks.count < sampleCount {
                    nextSampleIndex += sampleStep
                    if sampleRemainder > 0 {
                        let carryThreshold = sampleDenominator - sampleRemainder
                        if remainderCarry >= carryThreshold {
                            nextSampleIndex += 1
                            remainderCarry -= carryThreshold
                        } else {
                            remainderCarry += sampleRemainder
                        }
                    }
                }
            }
            rowIndex += 1
            step = sqlite3_step(statement)
        }
        try ensureDone(step, db: db, operation: operation)
        guard rowIndex == rowCount else {
            throw LedgerReadError.inconsistentData(operation: operation, detail: "account mark scan differs from its snapshot count")
        }
        guard marks.count == sampleCount else {
            throw LedgerReadError.inconsistentData(operation: operation, detail: "account mark sample differs from its target count")
        }
        if marks.isEmpty, ledgerState == "COMPACTED" {
            throw LedgerReadError.accountMarksUnavailable(runId: runId, modelId: modelId, ledgerState: ledgerState)
        }
        return marks
    }

    func fetchAllPolicies() throws -> [RealPolicy] {
        let operation = "policy inventory"
        let db = try openDB()
        defer { sqlite3_close(db) }
        let sql = """
        SELECT p.policy_id, p.family, p.origin, p.name, p.head_revision_id,
               p.revision_count, COUNT(DISTINCT rm.run_id), COUNT(rm.model_id),
               datetime(MAX(r.created_at_ms) / 1000, 'unixepoch', '+9 hours')
        FROM policies AS p
        LEFT JOIN run_models AS rm ON rm.policy_id = p.policy_id
        LEFT JOIN runs AS r ON r.id = rm.run_id
        GROUP BY p.policy_id, p.family, p.origin, p.name, p.head_revision_id, p.revision_count
        ORDER BY p.origin DESC, p.policy_id ASC;
        """
        let statement = try prepare(db, sql: sql, operation: operation)
        defer { sqlite3_finalize(statement) }

        var results: [RealPolicy] = []
        var step = sqlite3_step(statement)
        while step == SQLITE_ROW {
            let runCount = Int(sqlite3_column_int64(statement, 6))
            results.append(RealPolicy(
                policyId: try requiredText(statement, column: 0, operation: operation, field: "policy_id"),
                family: try requiredText(statement, column: 1, operation: operation, field: "family"),
                origin: try requiredText(statement, column: 2, operation: operation, field: "origin"),
                name: try requiredText(statement, column: 3, operation: operation, field: "name"),
                headRevisionId: try requiredText(statement, column: 4, operation: operation, field: "head_revision_id"),
                revisionCount: Int(sqlite3_column_int64(statement, 5)),
                historicalRunCount: runCount,
                historicalModelCount: Int(sqlite3_column_int64(statement, 7)),
                lastHistoricalRunDateStr: optionalText(statement, column: 8),
                usageState: runCount > 0 ? .historicalOnly : .unused
            ))
            step = sqlite3_step(statement)
        }
        try ensureDone(step, db: db, operation: operation)
        return results
    }

    func fetchRecentJobs(limit: Int = 30) throws -> [RealJob] {
        guard (1...100).contains(limit) else { throw LedgerReadError.invalidLimit(limit) }
        let operation = "recent jobs"
        let db = try openDB()
        defer { sqlite3_close(db) }
        let sql = """
        SELECT id, request_id, payload_kind, current_status,
               datetime(created_at_ms / 1000, 'unixepoch', '+9 hours')
        FROM jobs
        ORDER BY created_at_ms DESC
        LIMIT ?;
        """
        let statement = try prepare(db, sql: sql, operation: operation)
        defer { sqlite3_finalize(statement) }
        sqlite3_bind_int(statement, 1, Int32(clamping: limit))

        var results: [RealJob] = []
        var step = sqlite3_step(statement)
        while step == SQLITE_ROW {
            results.append(RealJob(
                id: try requiredText(statement, column: 0, operation: operation, field: "job_id"),
                requestId: try requiredText(statement, column: 1, operation: operation, field: "request_id"),
                payloadKind: try requiredText(statement, column: 2, operation: operation, field: "payload_kind"),
                status: try requiredText(statement, column: 3, operation: operation, field: "current_status"),
                createdDateStr: try requiredText(statement, column: 4, operation: operation, field: "created_at")
            ))
            step = sqlite3_step(statement)
        }
        try ensureDone(step, db: db, operation: operation)
        return results
    }

    func fetchActiveJobs(limit: Int = 50) throws -> [RealJob] {
        guard (1...100).contains(limit) else { throw LedgerReadError.invalidLimit(limit) }
        let operation = "active jobs"
        let db = try openDB()
        defer { sqlite3_close(db) }
        let sql = """
        SELECT id, request_id, payload_kind, current_status,
               datetime(created_at_ms / 1000, 'unixepoch', '+9 hours')
        FROM jobs
        WHERE current_status IN ('QUEUED', 'RUNNING')
        ORDER BY created_at_ms DESC, id DESC
        LIMIT ?;
        """
        let statement = try prepare(db, sql: sql, operation: operation)
        defer { sqlite3_finalize(statement) }
        sqlite3_bind_int64(statement, 1, Int64(limit))

        var results: [RealJob] = []
        var step = sqlite3_step(statement)
        while step == SQLITE_ROW {
            results.append(RealJob(
                id: try requiredText(statement, column: 0, operation: operation, field: "job_id"),
                requestId: try requiredText(statement, column: 1, operation: operation, field: "request_id"),
                payloadKind: try requiredText(statement, column: 2, operation: operation, field: "payload_kind"),
                status: try requiredText(statement, column: 3, operation: operation, field: "current_status"),
                createdDateStr: try requiredText(statement, column: 4, operation: operation, field: "created_at")
            ))
            step = sqlite3_step(statement)
        }
        try ensureDone(step, db: db, operation: operation)
        return results
    }

    func fetchPausedCollectionSchedules(limit: Int = 50) throws -> [RealCollectionSchedule] {
        guard (1...100).contains(limit) else { throw LedgerReadError.invalidLimit(limit) }
        let operation = "paused collection schedules"
        let db = try openDB()
        defer { sqlite3_close(db) }
        let sql = """
        SELECT id, status, created_at_ms, next_action_at_ms,
               last_success_dataset_id, last_success_boundary_ms, failure_json
        FROM collection_schedules
        WHERE status = 'paused'
        ORDER BY created_at_ms DESC, id DESC
        LIMIT ?;
        """
        let statement = try prepare(db, sql: sql, operation: operation)
        defer { sqlite3_finalize(statement) }
        sqlite3_bind_int64(statement, 1, Int64(limit))

        var results: [RealCollectionSchedule] = []
        var step = sqlite3_step(statement)
        while step == SQLITE_ROW {
            let failureCode: String?
            if let failureJSON = optionalText(statement, column: 6) {
                let failure: ScheduleFailure = try decode(
                    ScheduleFailure.self,
                    text: failureJSON,
                    operation: operation
                )
                failureCode = failure.code
            } else {
                failureCode = nil
            }
            let lastBoundary = sqlite3_column_type(statement, 5) == SQLITE_NULL
                ? nil
                : Date(timeIntervalSince1970: TimeInterval(sqlite3_column_int64(statement, 5)) / 1000.0)
            results.append(RealCollectionSchedule(
                id: try requiredText(statement, column: 0, operation: operation, field: "schedule_id"),
                status: try requiredText(statement, column: 1, operation: operation, field: "status"),
                createdAt: Date(timeIntervalSince1970: TimeInterval(sqlite3_column_int64(statement, 2)) / 1000.0),
                nextActionAt: Date(timeIntervalSince1970: TimeInterval(sqlite3_column_int64(statement, 3)) / 1000.0),
                lastSuccessDatasetId: optionalText(statement, column: 4),
                lastSuccessBoundary: lastBoundary,
                failureCode: failureCode
            ))
            step = sqlite3_step(statement)
        }
        try ensureDone(step, db: db, operation: operation)
        return results
    }
}
