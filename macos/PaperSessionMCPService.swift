import Foundation

enum PaperSessionServiceError: LocalizedError {
    case invalidResponse(String)
    case unknownMutationOutcome(requestId: String, message: String)

    var errorDescription: String? {
        switch self {
        case .invalidResponse(let detail):
            return "지속 모의 세션 응답 형식이 유효하지 않습니다: \(detail)"
        case .unknownMutationOutcome(let requestId, let message):
            return "요청(\(requestId)) 결과를 아직 확인하지 못했습니다 (\(message)). 새 세션을 중복 생성하지 말고 요청 상태 재조회를 실행하세요."
        }
    }
}

struct PaperPolicyRevisionReference: Codable, Equatable {
    let policy_id: String
    let revision_id: String
    let definition_digest: String

    var arguments: [String: Any] {
        [
            "policy_id": policy_id,
            "revision_id": revision_id,
            "definition_digest": definition_digest
        ]
    }
}

struct PaperPolicyOption: Codable, Equatable {
    let reference: PaperPolicyRevisionReference
    let family: String
    let name: String
    let supported: Bool
    let unavailable_reason: String?
    let decision_warmup_bars: UInt32
    let source_intervals: [String]
}

struct PaperDynamicCostParams: Codable, Equatable {
    let volatility_slippage_multiplier: String
    let liquidity_impact_factor: String

    var arguments: [String: Any] {
        [
            "volatility_slippage_multiplier": volatility_slippage_multiplier,
            "liquidity_impact_factor": liquidity_impact_factor
        ]
    }
}

struct PaperCostPolicy: Codable, Equatable {
    let buy_fee_bps: String
    let sell_fee_bps: String
    let maker_fee_bps: String
    let half_spread_bps: String
    let slippage_bps: String
    let impact_bps: String
    let assumption_label: String
    let dynamic: PaperDynamicCostParams?

    var arguments: [String: Any] {
        [
            "buy_fee_bps": buy_fee_bps,
            "sell_fee_bps": sell_fee_bps,
            "maker_fee_bps": maker_fee_bps,
            "half_spread_bps": half_spread_bps,
            "slippage_bps": slippage_bps,
            "impact_bps": impact_bps,
            "assumption_label": assumption_label,
            "dynamic": dynamic?.arguments ?? NSNull()
        ]
    }
}

struct PaperTickBand: Codable, Equatable {
    let lower_bound: String
    let tick: String

    var arguments: [String: Any] {
        [
            "lower_bound": lower_bound,
            "tick": tick
        ]
    }
}

struct PaperMarketRuleScenario: Codable, Equatable {
    let kind: String
    let min_notional: String
    let quantity_step: String
    let ticks: [PaperTickBand]
    let assumption_label: String

    var arguments: [String: Any] {
        [
            "kind": kind,
            "min_notional": min_notional,
            "quantity_step": quantity_step,
            "ticks": ticks.map(\.arguments),
            "assumption_label": assumption_label
        ]
    }
}

struct PaperExecutionPolicy: Codable, Equatable {
    let kind: String
    let participation_cap: String
    let offset_bps: String?
    let ttl_bars: UInt32?

    var arguments: [String: Any] {
        var dict: [String: Any] = [
            "kind": kind,
            "participation_cap": participation_cap
        ]
        if let offset_bps { dict["offset_bps"] = offset_bps }
        if let ttl_bars { dict["ttl_bars"] = ttl_bars }
        return dict
    }
}

struct PaperScenarioPreset: Codable, Equatable {
    let preset_id: String
    let name: String
    let initial_cash: String
    let costs: PaperCostPolicy
    let market_rules: PaperMarketRuleScenario
    let execution: PaperExecutionPolicy
    let latency_ms: UInt64
    let editable: Bool
    let source_note: String
}

struct PaperSessionLimits: Codable, Equatable {
    let active_sessions: UInt32
    let markets_per_session: UInt32
    let recovery_bars_per_market_interval: UInt32
}

struct PaperSessionStartOptions: Codable, Equatable {
    let policy_options: [PaperPolicyOption]
    let scenario_presets: [PaperScenarioPreset]
    let decision_intervals: [String]
    let execution_resolutions: [String]
    let limits: PaperSessionLimits
}

struct PaperBenchmarkSnapshot: Codable, Equatable {
    let kind: String
    let initial_cash: String
    let equity: String
    let pnl: String
    let return_fraction: String
    let base_observation_id: String
}

struct PaperVerification: Codable, Equatable {
    let status: String
    let reason: String
}

struct PaperMarketSnapshot: Codable, Equatable {
    let market: String
    let model_id: String
    let initial_cash: String
    let cash_total: String
    let cash_free: String
    let cash_reserved: String
    let quantity: String
    let mark_price: String?
    let equity: String?
    let realized_pnl: String
    let unrealized_pnl: String?
    let total_fees: String
    let drawdown_amount: String?
    let drawdown_fraction: String?
    let signal_count: UInt64
    let fill_count: UInt64
    let event_count: UInt64
    let last_source_event_at: String?
    let last_finalized_candle_at: String?
    let last_account_mark_at: String?
    let last_heartbeat_at: String?
    let freshness: String
    let benchmark: PaperBenchmarkSnapshot?
    let benchmark_null_reason: String?
    let verification: PaperVerification
    let costs: PaperCostPolicy
    let market_rules: PaperMarketRuleScenario

    var initialCashDecimal: Decimal? { Decimal(string: initial_cash, locale: Locale(identifier: "en_US_POSIX")) }
    var cashTotalDecimal: Decimal? { Decimal(string: cash_total, locale: Locale(identifier: "en_US_POSIX")) }
    var cashFreeDecimal: Decimal? { Decimal(string: cash_free, locale: Locale(identifier: "en_US_POSIX")) }
    var cashReservedDecimal: Decimal? { Decimal(string: cash_reserved, locale: Locale(identifier: "en_US_POSIX")) }
    var quantityDecimal: Decimal? { Decimal(string: quantity, locale: Locale(identifier: "en_US_POSIX")) }
    var markPriceDecimal: Decimal? { mark_price.flatMap { Decimal(string: $0, locale: Locale(identifier: "en_US_POSIX")) } }
    var equityDecimal: Decimal? { equity.flatMap { Decimal(string: $0, locale: Locale(identifier: "en_US_POSIX")) } }
    var realizedPnlDecimal: Decimal? { Decimal(string: realized_pnl, locale: Locale(identifier: "en_US_POSIX")) }
    var unrealizedPnlDecimal: Decimal? { unrealized_pnl.flatMap { Decimal(string: $0, locale: Locale(identifier: "en_US_POSIX")) } }
    var totalFeesDecimal: Decimal? { Decimal(string: total_fees, locale: Locale(identifier: "en_US_POSIX")) }
    var drawdownFractionDecimal: Decimal? { drawdown_fraction.flatMap { Decimal(string: $0, locale: Locale(identifier: "en_US_POSIX")) } }
}

struct PaperSessionView: Codable, Equatable {
    let session_id: String
    let origin_request_id: String
    let state: String
    let sequence: UInt64
    let policy_ref: PaperPolicyRevisionReference
    let decision_interval: String
    let execution_resolution: String
    let mode: String
    let created_at: String
    let started_at: String
    let paused_at: String?
    let stopped_at: String?
    let last_evaluated_at: String?
    let event_count: UInt64
    let markets: [PaperMarketSnapshot]
    let recovery_cause: String?

    var canOfferFeedGapRecoveryResume: Bool {
        state == "RECOVERY_REQUIRED" && recovery_cause == "FEED_GAP"
    }

    var stateDisplay: String {
        switch state {
        case "RUNNING": return "실행 중 (RUNNING)"
        case "PAUSED": return "일시정지 (PAUSED)"
        case "STOPPED": return "종료됨 (STOPPED)"
        case "RECOVERY_REQUIRED": return "복구 필요 (RECOVERY_REQUIRED)"
        default: return state
        }
    }
}

struct PaperSessionCursor: Codable, Equatable {
    let session_id: String
}

struct PaperSessionPage: Codable, Equatable {
    let items: [PaperSessionView]
    let returned_count: UInt64
    let total_count: UInt64
    let next_cursor: PaperSessionCursor?
}

struct PaperSessionGetResponse: Codable, Equatable {
    let session: PaperSessionView
}

struct PaperCandleReference: Codable, Equatable {
    let observation_id: String
    let interval: String
    let open_time: String
    let close_time: String
    let content_digest: String
}

struct PaperOrderRecord: Codable, Equatable {
    let order_id: String
    let parent_signal_id: String
    let episode_id: String?
    let side: String
    let order_type: String
    let requested_price: String?
    let requested_qty: String
    let created_at: String
    let effective_at: String
    let expires_at: String
    let rule_snapshot_id: String
    let policy_version: String
    let reserved_cash: String
    let cumulative_filled_qty: String
    let status: String
    let reason: String
    let order_origin: String
}

struct PaperSignalRecord: Codable, Equatable {
    let signal_id: String
    let strategy: String
    let intended_side: String?
    let state_before: String
    let state_after: String
    let raw_target_weight: String
    let constrained_target_weight: String
    let reasons: [String]
    let outcome: String
}

struct PaperFillRecord: Codable, Equatable {
    let fill_id: String
    let order_id: String
    let side: String
    let price: String
    let qty: String
    let notional: String
    let fee: String
    let fee_bps: String
}

struct PaperAccountState: Codable, Equatable {
    let cash_total: String
    let cash_free: String
    let cash_reserved: String
    let qty: String
    let price_basis: String
    let gross_realized: String
    let cumulative_fees: String
}

struct PaperAccountMarkRecord: Codable, Equatable {
    let kind: String
    let state: PaperAccountState
    let mark_price: String
    let position_value: String
    let gross_unrealized: String
    let equity: String
    let drawdown: String
}

struct PaperControlEvent: Codable, Equatable {
    let action: String
    let cancelled_pending_orders: [PaperOrderRecord]
}

struct PaperSessionEvent: Codable, Equatable {
    let sequence: UInt64
    let kind: String
    let market: String?
    let candle: PaperCandleReference?
    let source_event_at: String?
    let finalized_at: String?
    let control: PaperControlEvent?
    let signals: [PaperSignalRecord]
    let orders: [PaperOrderRecord]
    let order_events: [PaperOrderRecord]
    let fills: [PaperFillRecord]
    let account_marks: [PaperAccountMarkRecord]

    var kindDisplay: String {
        switch kind {
        case "CANDLE_PROCESSED": return "확정 봉 처리"
        case "PAUSE": return "일시정지"
        case "RESUME": return "재개"
        case "STOP": return "세션 종료"
        case "RECOVERY_REQUIRED": return "복구 필요"
        default: return kind
        }
    }

    var occurredAtDisplay: String {
        finalized_at ?? source_event_at ?? candle?.close_time ?? "—"
    }

    var primarySideDisplay: String {
        fills.first?.side
            ?? orders.first?.side
            ?? order_events.first?.side
            ?? signals.first?.intended_side
            ?? "—"
    }

    var primaryQtyDisplay: String {
        fills.first?.qty
            ?? orders.first?.requested_qty
            ?? order_events.first?.requested_qty
            ?? "—"
    }

    var primaryPriceDisplay: String {
        fills.first?.price
            ?? account_marks.last?.mark_price
            ?? orders.first?.requested_price
            ?? "—"
    }

    var primaryFeeDisplay: String {
        fills.first?.fee ?? "—"
    }

    var primaryEquityDisplay: String {
        account_marks.last?.equity ?? "—"
    }

    var summaryDetail: String {
        var parts: [String] = []
        if let candle {
            parts.append("봉 \(candle.close_time) [\(String(candle.content_digest.prefix(12)))…]")
        }
        if !signals.isEmpty {
            let sigText = signals.map { "\($0.strategy):\($0.state_before)→\($0.state_after)(\($0.outcome))" }.joined(separator: ",")
            parts.append("신호 \(sigText)")
        }
        if !fills.isEmpty {
            let fillText = fills.map { "\($0.side) \($0.qty)@\($0.price)" }.joined(separator: ",")
            parts.append("가상체결 \(fillText)")
        } else if !order_events.isEmpty {
            let ordText = order_events.map { "\($0.side) \($0.status)(\($0.reason))" }.joined(separator: ",")
            parts.append("가상주문 \(ordText)")
        }
        if let control {
            parts.append("제어 \(control.action) (취소 주문 \(control.cancelled_pending_orders.count)건)")
        }
        return parts.isEmpty ? kind : parts.joined(separator: " · ")
    }
}

struct PaperEventCursor: Codable, Equatable {
    let sequence: UInt64
}

struct PaperSessionEventPage: Codable, Equatable {
    let session_id: String
    let items: [PaperSessionEvent]
    let returned_count: UInt64
    let total_count: UInt64
    let next_cursor: PaperEventCursor?
}

struct PaperSessionReceipt: Codable, Equatable {
    let request_id: String
    let session_id: String
    let action: String
    let outcome: String
    let expected_sequence: UInt64?
    let sequence: UInt64
    let created_at: String
    let cancelled_pending_orders: [PaperOrderRecord]
}

struct PaperSessionCommandResponse: Codable, Equatable {
    let receipt: PaperSessionReceipt
    let session: PaperSessionView
}

struct PaperSessionRequestReadback: Codable, Equatable {
    let request_id: String
    let found: Bool
    let action: String?
    let session_id: String?
    let receipt: PaperSessionReceipt?
    let session: PaperSessionView?
}

struct PaperMarketStartInput: Equatable {
    let market: String
    let initialCash: String
    let costs: PaperCostPolicy
    let marketRules: PaperMarketRuleScenario

    var arguments: [String: Any] {
        [
            "market": market,
            "initial_cash": initialCash,
            "costs": costs.arguments,
            "market_rules": marketRules.arguments
        ]
    }
}

final class PaperSessionMCPService {
    private let client: NativeMCPClient
    private let decoder = JSONDecoder()

    init(client: NativeMCPClient = .shared) {
        self.client = client
    }

    @discardableResult
    func fetchStartOptions(
        completion: @escaping (Result<PaperSessionStartOptions, Error>) -> Void
    ) -> NativeMCPRequest {
        client.call("paper_session_query", arguments: ["action": "start_options"]) { [decoder] result in
            completion(result.flatMap { data in
                do {
                    let decoded = try decoder.decode(PaperSessionStartOptions.self, from: data)
                    guard !decoded.decision_intervals.isEmpty,
                          !decoded.scenario_presets.isEmpty else {
                        throw PaperSessionServiceError.invalidResponse("start_options 필수 항목이 비어 있습니다")
                    }
                    return .success(decoded)
                } catch {
                    return .failure(error)
                }
            })
        }
    }

    @discardableResult
    func listSessions(
        afterSessionId: String? = nil,
        limit: Int = 50,
        completion: @escaping (Result<PaperSessionPage, Error>) -> Void
    ) -> NativeMCPRequest {
        let args: [String: Any] = [
            "action": "list",
            "after_session_id": afterSessionId ?? NSNull(),
            "limit": limit
        ]
        return client.call("paper_session_query", arguments: args) { [decoder] result in
            completion(result.flatMap { data in
                do {
                    return .success(try decoder.decode(PaperSessionPage.self, from: data))
                } catch {
                    return .failure(error)
                }
            })
        }
    }

    @discardableResult
    func getSession(
        sessionId: String,
        completion: @escaping (Result<PaperSessionView, Error>) -> Void
    ) -> NativeMCPRequest {
        client.call(
            "paper_session_query",
            arguments: ["action": "get", "session_id": sessionId]
        ) { [decoder] result in
            completion(result.flatMap { data in
                do {
                    let response = try decoder.decode(PaperSessionGetResponse.self, from: data)
                    guard response.session.session_id == sessionId else {
                        throw PaperSessionServiceError.invalidResponse("요청한 session_id와 응답이 다릅니다")
                    }
                    return .success(response.session)
                } catch {
                    return .failure(error)
                }
            })
        }
    }

    @discardableResult
    func listEvents(
        sessionId: String,
        afterSequence: UInt64? = nil,
        limit: Int = 50,
        completion: @escaping (Result<PaperSessionEventPage, Error>) -> Void
    ) -> NativeMCPRequest {
        let args: [String: Any] = [
            "action": "events",
            "session_id": sessionId,
            "after_sequence": afterSequence ?? NSNull(),
            "limit": limit
        ]
        return client.call("paper_session_query", arguments: args) { [decoder] result in
            completion(result.flatMap { data in
                do {
                    let page = try decoder.decode(PaperSessionEventPage.self, from: data)
                    guard page.session_id == sessionId else {
                        throw PaperSessionServiceError.invalidResponse("이벤트 페이지의 session_id가 일치하지 않습니다")
                    }
                    return .success(page)
                } catch {
                    return .failure(error)
                }
            })
        }
    }

    @discardableResult
    func readbackRequest(
        requestId: String,
        completion: @escaping (Result<PaperSessionRequestReadback, Error>) -> Void
    ) -> NativeMCPRequest {
        client.call(
            "paper_session_query",
            arguments: ["action": "request", "request_id": requestId]
        ) { [decoder] result in
            completion(result.flatMap { data in
                do {
                    let readback = try decoder.decode(PaperSessionRequestReadback.self, from: data)
                    try Self.validateReadback(readback, requestId: requestId)
                    return .success(readback)
                } catch {
                    return .failure(error)
                }
            })
        }
    }

    @discardableResult
    func startSession(
        requestId: String,
        policyRef: PaperPolicyRevisionReference,
        decisionInterval: String,
        executionResolution: String,
        latencyMs: UInt64,
        execution: PaperExecutionPolicy,
        markets: [PaperMarketStartInput],
        completion: @escaping (Result<PaperSessionCommandResponse, Error>) -> Void
    ) -> NativeMCPRequest {
        let args: [String: Any] = [
            "action": "start",
            "request_id": requestId,
            "policy_ref": policyRef.arguments,
            "decision_interval": decisionInterval,
            "execution_resolution": executionResolution,
            "latency_ms": latencyMs,
            "execution": execution.arguments,
            "markets": markets.map(\.arguments)
        ]
        return executeWrite(requestId: requestId, arguments: args, completion: completion)
    }

    @discardableResult
    func pauseSession(
        requestId: String,
        sessionId: String,
        expectedSequence: UInt64,
        completion: @escaping (Result<PaperSessionCommandResponse, Error>) -> Void
    ) -> NativeMCPRequest {
        let args: [String: Any] = [
            "action": "pause",
            "request_id": requestId,
            "session_id": sessionId,
            "expected_sequence": expectedSequence
        ]
        return executeWrite(requestId: requestId, arguments: args, completion: completion)
    }

    @discardableResult
    func resumeSession(
        requestId: String,
        sessionId: String,
        expectedSequence: UInt64,
        completion: @escaping (Result<PaperSessionCommandResponse, Error>) -> Void
    ) -> NativeMCPRequest {
        let args: [String: Any] = [
            "action": "resume",
            "request_id": requestId,
            "session_id": sessionId,
            "expected_sequence": expectedSequence
        ]
        return executeWrite(requestId: requestId, arguments: args, completion: completion)
    }

    @discardableResult
    func stopSession(
        requestId: String,
        sessionId: String,
        expectedSequence: UInt64,
        completion: @escaping (Result<PaperSessionCommandResponse, Error>) -> Void
    ) -> NativeMCPRequest {
        let args: [String: Any] = [
            "action": "stop",
            "request_id": requestId,
            "session_id": sessionId,
            "expected_sequence": expectedSequence
        ]
        return executeWrite(requestId: requestId, arguments: args, completion: completion)
    }

    private func executeWrite(
        requestId: String,
        arguments: [String: Any],
        completion: @escaping (Result<PaperSessionCommandResponse, Error>) -> Void
    ) -> NativeMCPRequest {
        let composite = NativeMCPRequest()
        let writeReq = client.call("paper_session_write", arguments: arguments) { [weak self, decoder] result in
            guard let self else { return }
            switch result {
            case .success(let data):
                do {
                    let response = try decoder.decode(PaperSessionCommandResponse.self, from: data)
                    try self.validateWriteResponse(response, requestId: requestId, arguments: arguments)
                    guard composite.complete() else { return }
                    completion(.success(response))
                } catch {
                    self.reconcileUncertainWrite(
                        requestId: requestId,
                        arguments: arguments,
                        cause: error,
                        composite: composite,
                        completion: completion
                    )
                }
            case .failure(let error):
                let isUncertainTransport: Bool
                switch error as? NativeMCPError {
                case .transport?, .timeout?, .protocolError?: isUncertainTransport = true
                default: isUncertainTransport = false
                }
                if isUncertainTransport {
                    self.reconcileUncertainWrite(
                        requestId: requestId,
                        arguments: arguments,
                        cause: error,
                        composite: composite,
                        completion: completion
                    )
                } else {
                    guard composite.complete() else { return }
                    completion(.failure(error))
                }
            }
        }
        _ = composite.addChild(writeReq)
        return composite
    }

    private func reconcileUncertainWrite(
        requestId: String,
        arguments: [String: Any],
        cause: Error,
        composite: NativeMCPRequest,
        completion: @escaping (Result<PaperSessionCommandResponse, Error>) -> Void
    ) {
        let readbackRequest = readbackRequest(requestId: requestId) { [weak self] result in
            guard let self else { return }
            switch result {
            case .failure(let error):
                guard composite.complete() else { return }
                completion(.failure(PaperSessionServiceError.unknownMutationOutcome(
                    requestId: requestId,
                    message: "쓰기 응답을 검증하지 못했고 영수증 재조회도 실패했습니다: \(error.localizedDescription)"
                )))
            case .success(let readback):
                do {
                    try self.validateReadbackContext(readback, requestId: requestId, arguments: arguments)
                } catch {
                    guard composite.complete() else { return }
                    completion(.failure(PaperSessionServiceError.unknownMutationOutcome(
                        requestId: requestId,
                        message: "영수증 재조회가 쓰기 요청 정보와 일치하지 않습니다: \(error.localizedDescription)"
                    )))
                    return
                }
                guard readback.found, let receipt = readback.receipt,
                      let session = readback.session else {
                    guard composite.complete() else { return }
                    completion(.failure(PaperSessionServiceError.unknownMutationOutcome(
                        requestId: requestId,
                        message: "쓰기 결과를 확인할 영수증과 현재 세션 스냅샷이 없습니다: \(cause.localizedDescription)"
                    )))
                    return
                }
                let response = PaperSessionCommandResponse(receipt: receipt, session: session)
                do {
                    try self.validateWriteResponse(response, requestId: requestId, arguments: arguments)
                } catch {
                    guard composite.complete() else { return }
                    completion(.failure(PaperSessionServiceError.unknownMutationOutcome(
                        requestId: requestId,
                        message: "영수증은 조회했지만 쓰기 결과 검증에 실패했습니다: \(error.localizedDescription)"
                    )))
                    return
                }
                guard composite.complete() else { return }
                completion(.success(response))
            }
        }
        _ = composite.addChild(readbackRequest)
    }

    private static func validateReadback(_ readback: PaperSessionRequestReadback, requestId: String) throws {
        guard readback.request_id == requestId else {
            throw PaperSessionServiceError.invalidResponse("요청 readback의 request_id가 일치하지 않습니다")
        }
        if !readback.found {
            guard readback.action == nil, readback.session_id == nil,
                  readback.receipt == nil, readback.session == nil else {
                throw PaperSessionServiceError.invalidResponse("미발견 readback에 영수증 또는 세션 정보가 포함되었습니다")
            }
            return
        }
        guard let receipt = readback.receipt,
              readback.action == receipt.action,
              readback.session_id == receipt.session_id,
              receipt.request_id == requestId else {
            throw PaperSessionServiceError.invalidResponse("readback 영수증의 요청·동작·세션 식별자가 일치하지 않습니다")
        }
        if let session = readback.session {
            guard session.session_id == receipt.session_id,
                  session.sequence >= receipt.sequence else {
                throw PaperSessionServiceError.invalidResponse("readback 세션 스냅샷의 식별자 또는 sequence가 영수증과 일치하지 않습니다")
            }
        }
    }

    private func validateReadbackContext(
        _ readback: PaperSessionRequestReadback,
        requestId: String,
        arguments: [String: Any]
    ) throws {
        try Self.validateReadback(readback, requestId: requestId)
        guard readback.found else { return }
        guard let expectedAction = arguments["action"] as? String,
              readback.action == expectedAction,
              let receipt = readback.receipt else {
            throw PaperSessionServiceError.invalidResponse("readback 동작이 쓰기 요청과 일치하지 않습니다")
        }
        if let expectedSessionId = arguments["session_id"] as? String,
           receipt.session_id != expectedSessionId {
            throw PaperSessionServiceError.invalidResponse("readback session_id가 쓰기 요청과 일치하지 않습니다")
        }
        if let expectedSequence = arguments["expected_sequence"] as? UInt64 {
            guard receipt.expected_sequence == expectedSequence else {
                throw PaperSessionServiceError.invalidResponse("readback expected_sequence가 쓰기 요청과 일치하지 않습니다")
            }
        } else if expectedAction == "start", receipt.expected_sequence != nil {
            throw PaperSessionServiceError.invalidResponse("start readback에 expected_sequence가 포함되었습니다")
        }
    }

    private func validateWriteResponse(
        _ response: PaperSessionCommandResponse,
        requestId: String,
        arguments: [String: Any]
    ) throws {
        let receipt = response.receipt
        let session = response.session
        guard let expectedAction = arguments["action"] as? String,
              receipt.request_id == requestId,
              receipt.action == expectedAction,
              receipt.session_id == session.session_id,
              session.sequence >= receipt.sequence else {
            throw PaperSessionServiceError.invalidResponse("쓰기 영수증과 세션의 요청·동작·식별자·sequence가 일치하지 않습니다")
        }
        if let expectedSessionId = arguments["session_id"] as? String,
           receipt.session_id != expectedSessionId {
            throw PaperSessionServiceError.invalidResponse("응답 session_id가 쓰기 요청과 일치하지 않습니다")
        }
        if let expectedSequence = arguments["expected_sequence"] as? UInt64 {
            guard receipt.expected_sequence == expectedSequence else {
                throw PaperSessionServiceError.invalidResponse("응답 expected_sequence가 쓰기 요청과 일치하지 않습니다")
            }
        } else if expectedAction == "start" {
            guard receipt.expected_sequence == nil, session.origin_request_id == requestId else {
                throw PaperSessionServiceError.invalidResponse("start 응답의 원본 request_id 또는 expected_sequence가 일치하지 않습니다")
            }
        }
    }
}
