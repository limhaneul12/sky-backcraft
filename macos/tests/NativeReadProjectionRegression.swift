import Darwin
import Foundation
import SQLite3

private let fixtureSQLiteTransient = unsafeBitCast(-1, to: sqlite3_destructor_type.self)
private let fixtureRunId = "native-read-fixture-run"
private let fixtureModelId = "native-read-fixture-model"
private let fixtureEquity = "12345.67890123456789012345"
private let fixtureCash = "0.0000000000000000000000000001"
private let fixtureBaseEpoch = 1_704_067_200.0
private let fixtureDigest = String(repeating: "a", count: 64)

private enum FixtureFailure: Error, CustomStringConvertible {
    case assertion(String)
    case sqlite(String)

    var description: String {
        switch self {
        case .assertion(let message): return message
        case .sqlite(let message): return "SQLite fixture failed: \(message)"
        }
    }
}

/// Test-only stand-in for the real network/process boundary. Reader implementations stay real.
final class NativeMCPClient {
    static let shared = NativeMCPClient()
    private let responder: (String, [String: Any]) -> Result<Data, Error>

    init(responder: @escaping (String, [String: Any]) -> Result<Data, Error> = { _, _ in
        .failure(FixtureFailure.assertion("unexpected MCP fixture request"))
    }) {
        self.responder = responder
    }

    func call(_ tool: String, arguments: [String: Any], completion: @escaping (Result<Data, Error>) -> Void) {
        completion(responder(tool, arguments))
    }
}

@main
private struct NativeReadProjectionRegression {
    static func main() throws {
        let arguments = Array(CommandLine.arguments.dropFirst())
        if let command = arguments.first {
            switch command {
            case "create-marks-db":
                guard arguments.count == 3, let count = Int(arguments[2]), count >= 0 else {
                    throw FixtureFailure.assertion("usage: create-marks-db <support-directory> <row-count>")
                }
                try createMarkDatabase(at: URL(fileURLWithPath: arguments[1]), count: count)
                print("FIXTURE marks rows=\(count) support_directory=\(arguments[1])")
                return
            case "measure-marks":
                guard arguments.count == 3, let maxPoints = Int(arguments[2]), maxPoints >= 2 else {
                    throw FixtureFailure.assertion("usage: measure-marks <support-directory> <max-points>")
                }
                try measureMarks(supportDirectory: URL(fileURLWithPath: arguments[1]), maxPoints: maxPoints)
                return
            case "measure-ledger":
                guard arguments.count == 2, let iterations = Int(arguments[1]), iterations > 0 else {
                    throw FixtureFailure.assertion("usage: measure-ledger <iterations>")
                }
                try measureLedgerPages(iterations: iterations)
                return
            case "measure-policy":
                guard arguments.count == 2, let iterations = Int(arguments[1]), iterations > 0 else {
                    throw FixtureFailure.assertion("usage: measure-policy <iterations>")
                }
                try measurePolicyPages(iterations: iterations)
                return
            default:
                throw FixtureFailure.assertion("unknown fixture command: \(command)")
            }
        }

        try runRegressionSuite()
        print("PASS native read projection regression")
    }

    private static func runRegressionSuite() throws {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("sky-backcraft-native-read-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }

        try verifyMarks(root: root, name: "below", rowCount: 5, maxPoints: 10)
        try verifyMarks(root: root, name: "equal", rowCount: 5, maxPoints: 5)
        try verifyMarks(root: root, name: "above", rowCount: 11, maxPoints: 4)
        try verifyMarks(root: root, name: "large", rowCount: 25_000, maxPoints: 1_000)
        try verifyEmptyMarkBehavior(root: root)
        try verifyHiddenInvalidMarkRows(root: root)
        try verifyLedgerTimestampBehavior()
        try verifyPolicyTimestampBehavior()
    }

    private static func verifyMarks(root: URL, name: String, rowCount: Int, maxPoints: Int) throws {
        let supportDirectory = root.appendingPathComponent(name, isDirectory: true)
        try createMarkDatabase(at: supportDirectory, count: rowCount)
        let marks = try DatabaseReader(supportDirectory: supportDirectory).fetchAccountMarks(
            for: fixtureRunId, modelId: fixtureModelId, maxPoints: maxPoints
        )
        let expectedIndices = sampleIndices(totalCount: rowCount, maxPoints: maxPoints)
        try expect(marks.count == expectedIndices.count, "\(name): returned count differs from prior downsample result")
        try expect(marks.count <= maxPoints, "\(name): retained result exceeds maxPoints")

        for (mark, index) in zip(marks, expectedIndices) {
            try expect(mark.seq == index, "\(name): sample index changed at sequence \(mark.seq)")
            try expect(mark.modelId == fixtureModelId, "\(name): model identity changed")
            try expect(mark.sourceBarId == "bar-\(index)", "\(name): source bar identity changed")
            try expect(mark.timestamp.timeIntervalSince1970 == expectedTimestamp(index).timeIntervalSince1970,
                       "\(name): accounting timestamp changed at sequence \(index)")
            try expect(mark.sourceBarCloseTime?.timeIntervalSince1970 == fixtureBaseEpoch + Double(index),
                       "\(name): source close time changed at sequence \(index)")
            try expect(mark.equityDecimal == Decimal(string: fixtureEquity), "\(name): equity Decimal changed")
            try expect(mark.cashDecimal == Decimal(string: fixtureCash), "\(name): cash Decimal changed")
            if index.isMultiple(of: 2) {
                try expect(mark.drawdownDecimal == Decimal.zero, "\(name): explicit zero drawdown became nil")
                try expect(mark.kind == "MARK", "\(name): non-null kind changed")
            } else {
                try expect(mark.drawdownDecimal == nil, "\(name): nil drawdown became zero")
                try expect(mark.kind == nil, "\(name): nil kind changed")
            }
        }
        if !marks.isEmpty {
            try expect(marks.first?.seq == 0, "\(name): first endpoint changed")
            try expect(marks.last?.seq == rowCount - 1, "\(name): last endpoint changed")
        }
    }

    private static func verifyEmptyMarkBehavior(root: URL) throws {
        let canonicalDirectory = root.appendingPathComponent("empty-canonical", isDirectory: true)
        try createMarkDatabase(at: canonicalDirectory, count: 0, ledgerState: "CANONICAL")
        let canonical = try DatabaseReader(supportDirectory: canonicalDirectory).fetchAccountMarks(
            for: fixtureRunId, modelId: fixtureModelId, maxPoints: 2
        )
        try expect(canonical.isEmpty, "empty canonical ledger must return an empty array")

        let compactedDirectory = root.appendingPathComponent("empty-compacted", isDirectory: true)
        try createMarkDatabase(at: compactedDirectory, count: 0, ledgerState: "COMPACTED")
        do {
            _ = try DatabaseReader(supportDirectory: compactedDirectory).fetchAccountMarks(
                for: fixtureRunId, modelId: fixtureModelId, maxPoints: 2
            )
            throw FixtureFailure.assertion("empty compacted ledger must remain unavailable")
        } catch let error as LedgerReadError {
            guard case .accountMarksUnavailable(let runId, let modelId, let state) = error,
                  runId == fixtureRunId, modelId == fixtureModelId, state == "COMPACTED" else { throw error }
        }
    }

    private static func verifyHiddenInvalidMarkRows(root: URL) throws {
        let hiddenSequence = 5_000
        try expect(!sampleIndices(totalCount: 10_001, maxPoints: 1_000).contains(hiddenSequence),
                   "hidden-row fixture sequence must be sampled out")

        let timestampDirectory = root.appendingPathComponent("hidden-invalid-timestamp", isDirectory: true)
        try createMarkDatabase(at: timestampDirectory, count: 10_001, invalidTimestampSequence: hiddenSequence)
        do {
            _ = try DatabaseReader(supportDirectory: timestampDirectory).fetchAccountMarks(
                for: fixtureRunId, modelId: fixtureModelId, maxPoints: 1_000
            )
            throw FixtureFailure.assertion("sampled-out invalid timestamp must still fail")
        } catch let error as LedgerReadError {
            guard case .invalidTimestamp(let operation, let field, let value) = error,
                  operation == "account marks", field == "context.accounting_event_time", value == "invalid" else {
                throw error
            }
        }

        let jsonDirectory = root.appendingPathComponent("hidden-invalid-record", isDirectory: true)
        try createMarkDatabase(at: jsonDirectory, count: 10_001, invalidJSONSequence: hiddenSequence)
        do {
            _ = try DatabaseReader(supportDirectory: jsonDirectory).fetchAccountMarks(
                for: fixtureRunId, modelId: fixtureModelId, maxPoints: 1_000
            )
            throw FixtureFailure.assertion("sampled-out invalid record must still fail")
        } catch let error as LedgerReadError {
            guard case .invalidJSON(let operation, _) = error,
                  operation == "account mark \(fixtureRunId)/\(fixtureModelId)" else { throw error }
        }
    }

    private static func verifyLedgerTimestampBehavior() throws {
        let data = try makeLedgerSignalPage(count: 100)
        let page = try LedgerDetailMCPReader.decodePage(
            data: data, runId: fixtureRunId, modelId: fixtureModelId, section: .signals, requestedCursor: nil
        )
        try expect(page.records.count == 100, "ledger decoder changed page size")
        let signals = page.records.compactMap { record -> LedgerSignalDetail? in
            if case .signal(let signal) = record { return signal }
            return nil
        }
        try expect(signals.count == 100, "ledger decoder changed record kind")
        for (index, signal) in signals.enumerated() {
            try expect(signal.identity.sequence == UInt64(index), "ledger decoder changed event sequence")
            try expect(signal.identity.accountingTime.timeIntervalSince1970 == expectedTimestamp(index).timeIntervalSince1970,
                       "ledger accounting timestamp changed at sequence \(index)")
            try expect(signal.signalTime.timeIntervalSince1970 == expectedTimestamp(index + 1).timeIntervalSince1970,
                       "ledger signal timestamp changed at sequence \(index)")
            try expect(signal.signalTimeRaw == fixtureTimestamp(index + 1), "ledger raw timestamp changed")
            try expect(signal.rawTargetWeight == Decimal(string: "0.125"), "ledger Decimal changed")
        }

        do {
            _ = try LedgerDetailMCPReader.decodePage(
                data: makeLedgerSignalPage(count: 2, invalidTimestampIndex: 1),
                runId: fixtureRunId, modelId: fixtureModelId, section: .signals, requestedCursor: nil
            )
            throw FixtureFailure.assertion("ledger invalid timestamp must fail")
        } catch let error as LedgerDetailError {
            guard case .invalidResponse(let detail) = error, detail == "timestamp" else { throw error }
        }
    }

    private static func verifyPolicyTimestampBehavior() throws {
        let validBundle = try makePolicyBundle(count: 100)
        let valid = try fetchPolicy(validBundle)
        try expect(valid.header.createdAt == fixtureTimestamp(1), "policy header must preserve its raw fractional timestamp")
        try expect(valid.history.count == 100, "policy history count changed")
        try expect(valid.history[0].createdAt == fixtureTimestamp(100), "policy history must preserve its raw timestamp")
        try expect(valid.history[1].createdAt == fixtureTimestamp(99), "policy fractional history timestamp changed")
        try expect(valid.createdAt == valid.history[0].createdAt, "policy body/header history timestamp identity changed")

        let badHeader = try makePolicyBundle(count: 1, invalidHeaderIndex: 0)
        try expectPolicyTimestampFailure(try fetchPolicyResult(badHeader), field: "created_at")

        let badHistory = try makePolicyBundle(count: 2, invalidHistoryIndex: 1)
        try expectPolicyTimestampFailure(try fetchPolicyResult(badHistory), field: "created_at")
    }

    private static func expectPolicyTimestampFailure(
        _ result: Result<PolicyDetailSnapshot, Error>, field: String
    ) throws {
        switch result {
        case .success:
            throw FixtureFailure.assertion("invalid policy timestamp must fail")
        case .failure(let error):
            guard let policyError = error as? PolicyDetailReadError,
                  case .invalidResponse(let detail) = policyError, detail == field else { throw error }
        }
    }

    private static func fetchPolicy(_ bundle: PolicyFixtureBundle) throws -> PolicyDetailSnapshot {
        try fetchPolicyResult(bundle).get()
    }

    private static func fetchPolicyResult(
        _ bundle: PolicyFixtureBundle
    ) throws -> Result<PolicyDetailSnapshot, Error> {
        var result: Result<PolicyDetailSnapshot, Error>?
        PolicyDetailMCPReader(client: bundle.client()).fetchDetail(for: bundle.policy) { value in result = value }
        guard let result else { throw FixtureFailure.assertion("policy reader did not complete its synchronous fixture read") }
        return result
    }

    private static func measureMarks(supportDirectory: URL, maxPoints: Int) throws {
        let start = DispatchTime.now().uptimeNanoseconds
        let marks = try DatabaseReader(supportDirectory: supportDirectory).fetchAccountMarks(
            for: fixtureRunId, modelId: fixtureModelId, maxPoints: maxPoints
        )
        let elapsed = Double(DispatchTime.now().uptimeNanoseconds - start) / 1_000_000
        let rowCount = try markRowCount(at: supportDirectory)
        try expect(marks.count == sampleIndices(totalCount: rowCount, maxPoints: maxPoints).count,
                   "mark benchmark returned an unexpected sample size")
        print("METRIC reader=DatabaseReader.fetchAccountMarks rows=\(rowCount) max_points=\(maxPoints) returned=\(marks.count) elapsed_ms=\(String(format: "%.3f", elapsed)) peak_rss_bytes=\(peakRSSBytes())")
    }

    private static func measureLedgerPages(iterations: Int) throws {
        let data = try makeLedgerSignalPage(count: 100)
        _ = try LedgerDetailMCPReader.decodePage(
            data: data, runId: fixtureRunId, modelId: fixtureModelId, section: .signals, requestedCursor: nil
        )
        let start = DispatchTime.now().uptimeNanoseconds
        var decodedRecords = 0
        for _ in 0..<iterations {
            decodedRecords += try LedgerDetailMCPReader.decodePage(
                data: data, runId: fixtureRunId, modelId: fixtureModelId, section: .signals, requestedCursor: nil
            ).returnedCount
        }
        let elapsed = Double(DispatchTime.now().uptimeNanoseconds - start) / 1_000_000
        try expect(decodedRecords == iterations * 100, "ledger benchmark decoded an unexpected record count")
        print("METRIC reader=LedgerDetailMCPReader.decodePage rows_per_page=100 iterations=\(iterations) decoded=\(decodedRecords) elapsed_ms=\(String(format: "%.3f", elapsed)) peak_rss_bytes=\(peakRSSBytes())")
    }

    private static func measurePolicyPages(iterations: Int) throws {
        let bundle = try makePolicyBundle(count: 100)
        _ = try fetchPolicy(bundle)
        let start = DispatchTime.now().uptimeNanoseconds
        var decodedHistory = 0
        for _ in 0..<iterations {
            decodedHistory += try fetchPolicy(bundle).history.count
        }
        let elapsed = Double(DispatchTime.now().uptimeNanoseconds - start) / 1_000_000
        try expect(decodedHistory == iterations * 100, "policy benchmark decoded an unexpected history count")
        print("METRIC reader=PolicyDetailMCPReader.fetchDetail rows_per_page=100 iterations=\(iterations) decoded_history=\(decodedHistory) elapsed_ms=\(String(format: "%.3f", elapsed)) peak_rss_bytes=\(peakRSSBytes())")
    }

    private static func peakRSSBytes() -> Int64 {
        var usage = rusage()
        guard getrusage(RUSAGE_SELF, &usage) == 0 else { return -1 }
        return Int64(usage.ru_maxrss)
    }

    private static func createMarkDatabase(
        at supportDirectory: URL,
        count: Int,
        ledgerState: String = "CANONICAL",
        invalidTimestampSequence: Int? = nil,
        invalidJSONSequence: Int? = nil
    ) throws {
        try expect(count >= 0, "fixture mark count must not be negative")
        let researchDirectory = supportDirectory.appendingPathComponent("research", isDirectory: true)
        try FileManager.default.createDirectory(at: researchDirectory, withIntermediateDirectories: true)
        let databaseURL = researchDirectory.appendingPathComponent("lab.sqlite")
        var optionalDB: OpaquePointer?
        let openResult = sqlite3_open_v2(
            databaseURL.path, &optionalDB,
            SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, nil
        )
        guard openResult == SQLITE_OK, let db = optionalDB else {
            throw FixtureFailure.sqlite("open returned \(openResult)")
        }
        defer { sqlite3_close(db) }

        try execute(db, "CREATE TABLE runs (id TEXT PRIMARY KEY, ledger_state TEXT NOT NULL);")
        try execute(db, "CREATE TABLE run_models (run_id TEXT NOT NULL, model_id TEXT NOT NULL, PRIMARY KEY(run_id, model_id));")
        try execute(db, "CREATE TABLE candle_observations (id TEXT PRIMARY KEY, close_time_ms INTEGER NOT NULL);")
        try execute(db, "CREATE TABLE account_marks (run_id TEXT NOT NULL, model_id TEXT NOT NULL, event_seq INTEGER NOT NULL, equity_decimal TEXT NOT NULL, cash_total_decimal TEXT NOT NULL, source_bar_id TEXT NOT NULL, record_json TEXT NOT NULL, PRIMARY KEY(run_id, model_id, event_seq));")
        try execute(db, "BEGIN IMMEDIATE;")
        try execute(db, "INSERT INTO runs (id, ledger_state) VALUES ('\(fixtureRunId)', '\(ledgerState)');")
        try execute(db, "INSERT INTO run_models (run_id, model_id) VALUES ('\(fixtureRunId)', '\(fixtureModelId)');")

        let candleStatement = try prepare(db, "INSERT INTO candle_observations (id, close_time_ms) VALUES (?, ?);")
        defer { sqlite3_finalize(candleStatement) }
        let markStatement = try prepare(db, "INSERT INTO account_marks (run_id, model_id, event_seq, equity_decimal, cash_total_decimal, source_bar_id, record_json) VALUES (?, ?, ?, ?, ?, ?, ?);")
        defer { sqlite3_finalize(markStatement) }

        for sequence in 0..<count {
            let barId = "bar-\(sequence)"
            let invalidTimestamp = invalidTimestampSequence == sequence
            let recordText: String
            if invalidJSONSequence == sequence {
                recordText = "{"
            } else {
                let record: [String: Any] = [
                    "context": [
                        "run_id": fixtureRunId,
                        "model_id": fixtureModelId,
                        "event_seq": sequence,
                        "accounting_event_time": invalidTimestamp ? "invalid" : fixtureTimestamp(sequence)
                    ],
                    "source_bar_id": barId,
                    "kind": sequence.isMultiple(of: 2) ? "MARK" : NSNull(),
                    "drawdown": sequence.isMultiple(of: 2) ? "0" : NSNull()
                ]
                recordText = try jsonString(record)
            }

            try bindText(barId, to: candleStatement, at: 1)
            try bindInt64(Int64(fixtureBaseEpoch * 1_000) + Int64(sequence) * 1_000, to: candleStatement, at: 2)
            try stepAndReset(candleStatement, db: db)

            try bindText(fixtureRunId, to: markStatement, at: 1)
            try bindText(fixtureModelId, to: markStatement, at: 2)
            try bindInt64(Int64(sequence), to: markStatement, at: 3)
            try bindText(fixtureEquity, to: markStatement, at: 4)
            try bindText(fixtureCash, to: markStatement, at: 5)
            try bindText(barId, to: markStatement, at: 6)
            try bindText(recordText, to: markStatement, at: 7)
            try stepAndReset(markStatement, db: db)
        }
        try execute(db, "COMMIT;")
    }

    private static func markRowCount(at supportDirectory: URL) throws -> Int {
        let databaseURL = supportDirectory.appendingPathComponent("research/lab.sqlite")
        var optionalDB: OpaquePointer?
        let result = sqlite3_open_v2(databaseURL.path, &optionalDB, SQLITE_OPEN_READONLY | SQLITE_OPEN_FULLMUTEX, nil)
        guard result == SQLITE_OK, let db = optionalDB else { throw FixtureFailure.sqlite("read count DB open returned \(result)") }
        defer { sqlite3_close(db) }
        let statement = try prepare(db, "SELECT COUNT(*) FROM account_marks WHERE run_id=? AND model_id=?;")
        defer { sqlite3_finalize(statement) }
        try bindText(fixtureRunId, to: statement, at: 1)
        try bindText(fixtureModelId, to: statement, at: 2)
        guard sqlite3_step(statement) == SQLITE_ROW else { throw FixtureFailure.sqlite(String(cString: sqlite3_errmsg(db))) }
        let count = sqlite3_column_int64(statement, 0)
        guard count >= 0, count <= Int64(Int.max) else { throw FixtureFailure.sqlite("count outside Int range") }
        return Int(count)
    }

    private static func execute(_ db: OpaquePointer, _ sql: String) throws {
        var errorPointer: UnsafeMutablePointer<CChar>?
        let result = sqlite3_exec(db, sql, nil, nil, &errorPointer)
        guard result == SQLITE_OK else {
            let message = errorPointer.map { String(cString: $0) } ?? String(cString: sqlite3_errmsg(db))
            if let errorPointer { sqlite3_free(errorPointer) }
            throw FixtureFailure.sqlite(message)
        }
    }

    private static func prepare(_ db: OpaquePointer, _ sql: String) throws -> OpaquePointer {
        var statement: OpaquePointer?
        let result = sqlite3_prepare_v2(db, sql, -1, &statement, nil)
        guard result == SQLITE_OK, let statement else { throw FixtureFailure.sqlite(String(cString: sqlite3_errmsg(db))) }
        return statement
    }

    private static func bindText(_ value: String, to statement: OpaquePointer, at index: Int32) throws {
        let result = sqlite3_bind_text(statement, index, value, -1, fixtureSQLiteTransient)
        guard result == SQLITE_OK else { throw FixtureFailure.sqlite("text bind returned \(result)") }
    }

    private static func bindInt64(_ value: Int64, to statement: OpaquePointer, at index: Int32) throws {
        let result = sqlite3_bind_int64(statement, index, value)
        guard result == SQLITE_OK else { throw FixtureFailure.sqlite("integer bind returned \(result)") }
    }

    private static func stepAndReset(_ statement: OpaquePointer, db: OpaquePointer) throws {
        let result = sqlite3_step(statement)
        guard result == SQLITE_DONE else { throw FixtureFailure.sqlite(String(cString: sqlite3_errmsg(db))) }
        guard sqlite3_reset(statement) == SQLITE_OK, sqlite3_clear_bindings(statement) == SQLITE_OK else {
            throw FixtureFailure.sqlite(String(cString: sqlite3_errmsg(db)))
        }
    }

    private static func makeLedgerSignalPage(count: Int, invalidTimestampIndex: Int? = nil) throws -> Data {
        let records: [[String: Any]] = (0..<count).map { sequence in
            [
                "context": [
                    "run_id": fixtureRunId,
                    "model_id": fixtureModelId,
                    "market": "KRW-BTC",
                    "event_seq": sequence,
                    "accounting_event_time": fixtureTimestamp(sequence)
                ],
                "signal_id": "signal-\(sequence)",
                "strategy": "fixture-strategy",
                "intended_side": NSNull(),
                "raw_target_weight": "0.125",
                "constrained_target_weight": "0.125",
                "outcome": "HOLD",
                "reasons": ["fixture"],
                "signal_time": invalidTimestampIndex == sequence ? "invalid" : fixtureTimestamp(sequence + 1)
            ]
        }
        let page: [String: Any] = [
            "returned_count": count,
            "total_count": count,
            "next_cursor": NSNull(),
            "records": records
        ]
        return try JSONSerialization.data(withJSONObject: ["section": "signals", "page": page])
    }

    private struct PolicyFixtureBundle {
        let policy: RealPolicy
        let list: Data
        let revision: Data
        let history: Data

        func client() -> NativeMCPClient {
            NativeMCPClient { tool, arguments in
                guard tool == "policy_query", let action = arguments["action"] as? String else {
                    return .failure(FixtureFailure.assertion("unexpected policy tool/action"))
                }
                switch action {
                case "list": return .success(list)
                case "get": return .success(revision)
                case "history": return .success(history)
                default: return .failure(FixtureFailure.assertion("unexpected policy action \(action)"))
                }
            }
        }
    }

    private static func makePolicyBundle(
        count: Int,
        invalidHeaderIndex: Int? = nil,
        invalidHistoryIndex: Int? = nil
    ) throws -> PolicyFixtureBundle {
        try expect(count > 0, "policy fixture must contain a revision")
        let policyId = "fixture-policy"
        let headRevisionId = "rev-\(count)"
        let policyName = "Fixture Policy"
        let family = "RULES"
        let origin = "LOCAL"
        let headTimestamp = invalidHistoryIndex == 0 ? "invalid" : fixtureTimestamp(count)
        let headReference: [String: Any] = [
            "policy_id": policyId,
            "revision_id": headRevisionId,
            "definition_digest": fixtureDigest
        ]
        let header: [String: Any] = [
            "policy_id": policyId,
            "family": family,
            "origin": origin,
            "created_at": invalidHeaderIndex == 0 ? "invalid" : fixtureTimestamp(1),
            "name": policyName,
            "head": headReference,
            "revision_count": count
        ]
        let list = try jsonData([
            "items": [header], "returned_count": 1, "total_count": 1, "next_cursor": NSNull()
        ])

        var historyItems: [[String: Any]] = []
        for revisionNumber in stride(from: count, through: 1, by: -1) {
            let index = count - revisionNumber
            let revisionId = "rev-\(revisionNumber)"
            let reference: [String: Any] = [
                "policy_id": policyId,
                "revision_id": revisionId,
                "definition_digest": fixtureDigest
            ]
            historyItems.append([
                "reference": reference,
                "revision_number": revisionNumber,
                "parent_revision_id": revisionNumber > 1 ? "rev-\(revisionNumber - 1)" as Any : NSNull(),
                "name": policyName,
                "created_at": invalidHistoryIndex == index ? "invalid" : fixtureTimestamp(revisionNumber),
                "request_id": "request-\(revisionNumber)"
            ])
        }
        let history = try jsonData([
            "items": historyItems,
            "returned_count": count,
            "total_count": count,
            "next_cursor": NSNull()
        ])

        let definition: [String: Any] = [
            "schema_version": "1",
            "name": policyName,
            "description": "fixture",
            "program": ["kind": "BUILTIN", "strategy": ["kind": "FIXTURE"]]
        ]
        let snapshot: [String: Any] = [
            "reference": headReference,
            "family": family,
            "origin": origin,
            "definition": definition,
            "revision_number": count,
            "parent_revision_id": count > 1 ? "rev-\(count - 1)" as Any : NSNull()
        ]
        let revision = try jsonData([
            "snapshot": snapshot,
            "created_at": headTimestamp,
            "request_id": "request-\(count)"
        ])
        let policy = RealPolicy(
            policyId: policyId,
            family: family,
            origin: origin,
            name: policyName,
            headRevisionId: headRevisionId,
            revisionCount: count,
            historicalRunCount: 0,
            historicalModelCount: 0,
            lastHistoricalRunDateStr: nil,
            usageState: .unused
        )
        return PolicyFixtureBundle(policy: policy, list: list, revision: revision, history: history)
    }

    private static func jsonData(_ value: Any) throws -> Data {
        try JSONSerialization.data(withJSONObject: value, options: [.sortedKeys])
    }

    private static func jsonString(_ value: Any) throws -> String {
        guard let text = String(data: try jsonData(value), encoding: .utf8) else {
            throw FixtureFailure.assertion("fixture JSON was not UTF-8")
        }
        return text
    }

    private static func sampleIndices(totalCount: Int, maxPoints: Int) -> [Int] {
        guard totalCount > maxPoints else { return Array(0..<totalCount) }
        let lastIndex = totalCount - 1
        let denominator = maxPoints - 1
        return (0..<maxPoints).map { ($0 * lastIndex) / denominator }
    }

    private static func fixtureTimestamp(_ sequence: Int) -> String {
        let hour = (sequence / 3_600) % 24
        let minute = (sequence / 60) % 60
        let second = sequence % 60
        let time = String(format: "%02d:%02d:%02d", hour, minute, second)
        let fraction = sequence.isMultiple(of: 2) ? "" : ".125"
        return "2024-01-01T\(time)\(fraction)Z"
    }

    private static func expectedTimestamp(_ sequence: Int) -> Date {
        let fractional = sequence.isMultiple(of: 2) ? 0.0 : 0.125
        return Date(timeIntervalSince1970: fixtureBaseEpoch + Double(sequence) + fractional)
    }

    private static func expect(_ condition: @autoclosure () -> Bool, _ message: String) throws {
        guard condition() else { throw FixtureFailure.assertion(message) }
    }
}
