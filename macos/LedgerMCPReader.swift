import Foundation

private struct EquityChartEnvelope: Decodable {
    let run_id: String
    let model_id: String
    let total_count: Int
    let records: [EquityRecord]
    let sample_count: Int
    let sampled: Bool
}
private struct EquityRecord: Decodable {
    struct Context: Decodable {
        let run_id: String
        let model_id: String
        let event_seq: Int
        let accounting_event_time: String
    }
    struct State: Decodable { let cash_total: String }
    let context: Context
    let state: State
    let equity: String
    let drawdown: String
    let source_bar_id: String
}

/// Reads one bounded verified Rust chart projection; no archive logic in Swift.
final class LedgerMCPReader {
    private let client: NativeMCPClient
    init(client: NativeMCPClient = .shared) { self.client = client }

    @discardableResult
    func marks(runId: String, modelId: String, expectedCount: Int,
               completion: @escaping (Result<[RealAccountMark], Error>) -> Void) -> NativeMCPRequest {
        guard expectedCount >= 0 && expectedCount <= 50_000 else {
            completion(.failure(NativeMCPError.protocolError))
            return .alreadyFinished()
        }
        return readProjection(runId: runId, modelId: modelId, expectedCount: expectedCount, completion: completion)
    }

    private func readProjection(runId: String, modelId: String, expectedCount: Int,
                                completion: @escaping (Result<[RealAccountMark], Error>) -> Void) -> NativeMCPRequest {
        return client.call("result_query", arguments: ["action": "equity_chart", "run_id": runId,
            "model_id": modelId, "max_points": 256]) { result in
            do {
                let page = try JSONDecoder().decode(EquityChartEnvelope.self, from: result.get())
                guard page.run_id == runId, page.model_id == modelId, page.total_count == expectedCount,
                      page.sample_count == page.records.count, page.sample_count <= 256,
                      page.sample_count <= expectedCount,
                      page.sampled == (page.sample_count < page.total_count) else { throw NativeMCPError.protocolError }
                let preciseClock = ISO8601DateFormatter()
                preciseClock.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
                let ordinaryClock = ISO8601DateFormatter()
                var marks: [RealAccountMark] = []
                for record in page.records {
                    guard record.context.run_id == runId, record.context.model_id == modelId,
                          record.context.event_seq > (marks.last?.seq ?? -1),
                          let equity = Decimal(string: record.equity),
                          let cash = Decimal(string: record.state.cash_total),
                          let drawdown = Decimal(string: record.drawdown),
                          let timestamp = preciseClock.date(from: record.context.accounting_event_time)
                            ?? ordinaryClock.date(from: record.context.accounting_event_time) else { throw NativeMCPError.protocolError }
                    marks.append(RealAccountMark(modelId: modelId, seq: record.context.event_seq,
                        equityDecimal: equity, cashDecimal: cash, sourceBarId: record.source_bar_id,
                        timestamp: timestamp, sourceBarCloseTime: nil, drawdownDecimal: drawdown))
                }
                completion(.success(marks))
            } catch { completion(.failure(error)) }
        }
    }
}
