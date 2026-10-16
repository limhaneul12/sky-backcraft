import AppKit
import Foundation
import UniformTypeIdentifiers

struct LedgerCSVRecord {
    let run: RealRun
    let model: RealModelComparison?
    let policyName: String?
    var scopePolicyId: String? = nil
    var scopeMarket: String? = nil
    var scopeRunIds: [String]? = nil
    var scopePolicyRevisionId: String? = nil
}

enum LedgerCSVError: Error, LocalizedError {
    case mismatchedIdentity
    case invalidNumber
    case emptyScope
    case changedScope
    var errorDescription: String? {
        switch self {
        case .mismatchedIdentity: return "실행과 모델의 원장 식별자가 일치하지 않아 내보내기를 중단했습니다."
        case .invalidNumber: return "유효하지 않은 원장 숫자가 있어 내보내기를 중단했습니다."
        case .emptyScope: return "내보낼 원장 기록이 없습니다."
        case .changedScope: return "조회 중 실행 목록이 변경되거나 페이지 범위가 일치하지 않아 내보내기를 중단했습니다. 다시 조회하세요."
        }
    }
}

/// Exports immutable identities and unrounded economic values, never display-rounded KPIs.
enum LedgerCSVExport {
    enum Value {
        case text(String)
        case decimal(Decimal)
        case integer(Int)
        case unavailable
    }

    static func document(columns: [String], rows: [[Value]]) throws -> Data {
        guard !rows.isEmpty else { throw LedgerCSVError.emptyScope }
        var lines = [columns.map(quoted).joined(separator: ",")]
        for row in rows {
            guard row.count == columns.count else { throw LedgerCSVError.mismatchedIdentity }
            let fields = try row.map { value -> String in
                switch value {
                case .text(let text):
                    // Text fields must stay text when opened in spreadsheet applications.
                    let significant = text.drop(while: { $0.isWhitespace })
                    let protected = significant.first.map { "=+-@".contains($0) } == true ? "'" + text : text
                    return quoted(protected)
                case .decimal(let number):
                    guard !number.isNaN else { throw LedgerCSVError.invalidNumber }
                    return quoted(NSDecimalNumber(decimal: number).stringValue)
                case .integer(let number): return quoted(String(number))
                case .unavailable: return quoted("")
                }
            }
            lines.append(fields.joined(separator: ","))
        }
        // UTF-8 BOM makes Korean names interoperable with desktop spreadsheet importers.
        var data = Data([0xEF, 0xBB, 0xBF])
        data.append(Data((lines.joined(separator: "\r\n") + "\r\n").utf8))
        return data
    }

    static func models(_ records: [LedgerCSVRecord], exportedAt: Date = Date()) throws -> Data {
        let columns = ["record_kind", "run_id", "run_state", "plan_id", "dataset_ids", "model_id", "policy_id", "policy_name", "policy_revision_id", "policy_definition_digest", "market", "window_start_utc", "window_end_utc", "capital_mode", "cost_assumption", "f08_status", "f08_findings", "initial_equity_krw", "final_equity_krw", "net_pnl_krw", "net_return_fraction", "max_drawdown_fraction", "fees_krw", "fill_count", "closed_episodes", "model_status", "scope_policy_id", "scope_market", "scope_run_ids", "scope_policy_revision_id", "exported_at_utc"]
        let clock = ISO8601DateFormatter().string(from: exportedAt)
        let rows = try records.map { record -> [Value] in
            let run = record.run
            let model = record.model
            if let scope = record.scopeRunIds, !scope.contains(run.id) { throw LedgerCSVError.mismatchedIdentity }
            guard model == nil || model?.runId == run.id else { throw LedgerCSVError.mismatchedIdentity }
            if let model {
                if let markets = run.markets, !markets.contains(model.market) { throw LedgerCSVError.mismatchedIdentity }
                if let revision = model.policyRevisionId, let revisions = run.policyRevisionIds,
                   !revisions.contains(revision) { throw LedgerCSVError.mismatchedIdentity }
                guard (model.policyId == nil) == (model.policyRevisionId == nil) else { throw LedgerCSVError.mismatchedIdentity }
                if model.policyId != nil {
                    guard let digest = model.policyDefinitionDigest, digest.count == 64,
                          digest.allSatisfy({ "0123456789abcdef".contains($0) }) else { throw LedgerCSVError.mismatchedIdentity }
                } else if model.policyDefinitionDigest != nil { throw LedgerCSVError.mismatchedIdentity }
                if let scope = record.scopePolicyId, scope != model.policyId { throw LedgerCSVError.mismatchedIdentity }
                if let revision = record.scopePolicyRevisionId, revision != model.policyRevisionId { throw LedgerCSVError.mismatchedIdentity }
                if let market = record.scopeMarket, market != model.market { throw LedgerCSVError.mismatchedIdentity }
            }
            if let model, model.fillCount < 0 || model.closedEpisodes < 0 { throw LedgerCSVError.invalidNumber }
            func text(_ value: String?) -> Value { value.map(Value.text) ?? .unavailable }
            func decimal(_ value: Decimal?) -> Value { value.map(Value.decimal) ?? .unavailable }
            return [
                .text(model == nil ? "run_without_model_summary" : "model_result"),
                .text(run.id), .text(run.state), .text(run.planId), text(run.datasetIds?.joined(separator: ";")),
                text(model?.modelId), text(model?.policyId), text(record.policyName), text(model?.policyRevisionId), text(model?.policyDefinitionDigest),
                text(model?.market), text(run.windowStartUTC), text(run.windowEndUTC), text(run.capitalMode),
                text(run.costAssumptionLabel), text(model?.f08Status ?? run.f08Status),
                run.f08FindingsCount.map(Value.integer) ?? .unavailable,
                decimal(model?.initialEquityDecimal), decimal(model?.finalEquityDecimal), decimal(model?.netPnlDecimal),
                decimal(model?.netReturnDecimal), decimal(model?.maxDrawdownDecimal), decimal(model?.cumulativeFeesDecimal),
                model.map { .integer($0.fillCount) } ?? .unavailable,
                model.map { .integer($0.closedEpisodes) } ?? .unavailable,
                text(model?.status), text(record.scopePolicyId), text(record.scopeMarket), text(record.scopeRunIds?.joined(separator: ";")), text(record.scopePolicyRevisionId), .text(clock)
            ]
        }
        return try document(columns: columns, rows: rows)
    }

    static func history(_ runs: [RealRun], filter: RunHistoryFilter = RunHistoryFilter(), exportedAt: Date = Date()) throws -> Data {
        let clock = ISO8601DateFormatter().string(from: exportedAt)
        let columns = ["run_id", "state", "created_at_utc", "plan_id", "dataset_ids", "policy_revision_ids", "markets", "window_start_utc", "window_end_utc", "capital_mode", "cost_assumption", "ledger_state", "engine_version", "code_revision", "f08_status", "scope_search", "scope_status", "scope_market", "scope_policy_id", "scope_dataset_id", "scope_policy_revision_id", "scope_cost_assumption", "scope_created_from_ms", "scope_created_before_ms", "scope_pinned_run_ids", "exported_at_utc"]
        let rows: [[Value]] = runs.map { run in
            func text(_ value: String?) -> Value { value.map(Value.text) ?? .unavailable }
            return [.text(run.id), .text(run.state), text(run.createdAtUTC), .text(run.planId),
                    text(run.datasetIds?.joined(separator: ";")), text(run.policyRevisionIds?.joined(separator: ";")),
                    text(run.markets?.joined(separator: ";")), text(run.windowStartUTC), text(run.windowEndUTC),
                    text(run.capitalMode), text(run.costAssumptionLabel), .text(run.ledgerState),
                    text(run.engineVersion), text(run.codeRevision), text(run.f08Status),
                    text(filter.search), text(filter.status), text(filter.market), text(filter.policyId), text(filter.datasetId),
                    text(filter.policyRevisionId), text(filter.costAssumptionLabel), text(filter.createdFromMilliseconds.map(String.init)),
                    text(filter.createdBeforeMilliseconds.map(String.init)), text(filter.pinnedRunIds?.joined(separator: ";")), .text(clock)]
        }
        return try document(columns: columns, rows: rows)
    }

    static func save(_ data: Data, suggestedName: String, window: NSWindow?) {
        let panel = NSSavePanel()
        panel.allowedContentTypes = [.commaSeparatedText]
        panel.allowsOtherFileTypes = false
        panel.canCreateDirectories = true
        panel.nameFieldStringValue = suggestedName
        panel.title = "원장 결과 CSV 내보내기"
        panel.message = "내보내는 값은 원장의 정확한 숫자입니다. 금액 단위는 KRW, 수익률·낙폭은 비율(0.01 = 1%)이며 미확인 값은 빈칸입니다."
        let finish: (NSApplication.ModalResponse) -> Void = { response in
            guard response == .OK, let url = panel.url else { return }
            do { try data.write(to: url, options: .atomic) }
            catch { show(error, window: window) }
        }
        if let window { panel.beginSheetModal(for: window, completionHandler: finish) }
        else { panel.begin(completionHandler: finish) }
    }

    static func show(_ error: Error, window: NSWindow?) {
        let alert = NSAlert()
        alert.messageText = "CSV 내보내기 실패"
        alert.informativeText = error.localizedDescription
        if let window { alert.beginSheetModal(for: window) }
        else { alert.runModal() }
    }

    private static func quoted(_ text: String) -> String { "\"" + text.replacingOccurrences(of: "\"", with: "\"\"") + "\"" }
}
