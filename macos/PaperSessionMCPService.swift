import Foundation

enum PaperSessionServiceError: LocalizedError {
    case invalidInput(String)
    case invalidResponse(String)
    case unknownMutationOutcome(requestId: String, message: String)

    var errorDescription: String? {
        switch self {
        case .invalidInput(let detail):
            return "지속 모의 세션 조회 요청 형식이 유효하지 않습니다: \(detail)"
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
    var recovery_detail: String? = nil

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

struct PaperCheckpointTransition: Codable, Equatable {
    let market: String
    let before_digest: String
    let after_digest: String
    let from_close: String?
    let to_close: String?
}

struct PaperProviderRecovery: Codable, Equatable {
    let phase: String
    let processed_at: String
    let target_completed_at: String
    let proof_ids: [String]
    let checkpoint_transitions: [PaperCheckpointTransition]
}

struct PaperSessionEvent: Codable, Equatable {
    let sequence: UInt64
    let kind: String
    let market: String?
    let candle: PaperCandleReference?
    let source_event_at: String?
    let finalized_at: String?
    let control: PaperControlEvent?
    let preparation: AutomationJSONValue?
    var provider_recovery: PaperProviderRecovery? = nil
    let signals: [PaperSignalRecord]
    let orders: [PaperOrderRecord]
    let order_events: [PaperOrderRecord]
    let fills: [PaperFillRecord]
    let account_marks: [PaperAccountMarkRecord]

    var kindDisplay: String {
        switch kind {
        case "CANDLE_PROCESSED":
            return provider_recovery?.phase == "CATCH_UP" ? "복구 상태 동기화 · 판단/주문 없음" : "확정 봉 처리"
        case "PROVIDER_RECOVERY":
            switch provider_recovery?.phase {
            case "STARTED": return "공백 복구 시작"
            case "PROGRESS": return "실제 봉 상태 동기화 중"
            case "COMPLETED": return "같은 세션 복구 완료"
            default: return "공급자 공백 복구"
            }
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
        if let recovery = provider_recovery {
            parts.append("복구 \(recovery.phase) · 공백 근거 \(recovery.proof_ids.count)개 · 처리 \(recovery.processed_at)")
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

enum PaperSessionReviewMode: String, Codable, Equatable {
    case simulated = "SIMULATED"
}

struct PaperSessionReviewConfiguration: Codable, Equatable {
    let request_id: String
    let policy_ref: PaperPolicyRevisionReference
    let decision_interval: String
    let execution_resolution: String
    let latency_ms: UInt64
    let execution: PaperExecutionPolicy
    let markets: [PaperMarketStartInput]
}

struct PaperSessionReviewResponse: Codable, Equatable {
    let schema_version: String
    let mode: PaperSessionReviewMode
    let configuration: PaperSessionReviewConfiguration
    let as_of_sequence: UInt64
    let after_sequence: UInt64
    let page: PaperSessionEventPage
}

struct PaperDecisionReviewSelection: Codable, Equatable {
    let session_id: String
    let event_sequence: UInt64
    let as_of_sequence: UInt64
    let signal_id: String?
}

struct PaperDecisionReviewConfigurationIdentity: Codable, Equatable {
    let policy_ref: PaperPolicyRevisionReference
}

struct PaperDecisionReviewPolicyIdentity: Codable, Equatable {
    let reference: PaperPolicyRevisionReference
}

struct PaperDecisionReviewResponse: Codable, Equatable {
    let schema_version: String
    let selection: PaperDecisionReviewSelection
    let configuration: PaperDecisionReviewConfigurationIdentity
    let event: PaperSessionEvent
    let original_event_digest: String
    let policy: PaperDecisionReviewPolicyIdentity
    let selected_signal_id: String?
    let recorded_decision: AutomationJSONValue?
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

struct PaperMarketStartInput: Codable, Equatable {
    let market: String
    let initialCash: String
    let costs: PaperCostPolicy
    let marketRules: PaperMarketRuleScenario

    private enum CodingKeys: String, CodingKey {
        case market
        case initialCash = "initial_cash"
        case costs
        case marketRules = "market_rules"
    }

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
    private static let maximumReviewPageAttempts = 7
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
    func reviewEvents(
        sessionId: String,
        asOfSequence: UInt64,
        afterSequence: UInt64? = nil,
        limit: Int = 50,
        completion: @escaping (Result<PaperSessionReviewResponse, Error>) -> Void
    ) -> NativeMCPRequest {
        do {
            try Self.validateReviewRequest(
                sessionId: sessionId,
                afterSequence: afterSequence,
                asOfSequence: asOfSequence,
                limit: limit
            )
        } catch {
            completion(.failure(error))
            return .alreadyFinished()
        }

        let requestOwner = NativeMCPRequest()
        let expectedTransportContext = client.currentTransportContext()
        let client = self.client
        let decoder = self.decoder
        var attempts = 0

        func requestPage(limit: Int) {
            guard requestOwner.isPending else { return }
            attempts += 1
            let attempt = attempts
            let arguments: [String: Any] = [
                "action": "review_events",
                "session_id": sessionId,
                "after_sequence": afterSequence ?? NSNull(),
                "as_of_sequence": asOfSequence,
                "limit": limit
            ]
            let request = client.call(
                "paper_session_query",
                arguments: arguments,
                expectedTransportContext: expectedTransportContext
            ) { result in
                guard requestOwner.isPending, attempts == attempt else { return }
                if case .failure(let error as NativeMCPError) = result,
                   case .paperEventPageByteBudgetExceeded = error {
                    guard limit > 1, attempt < Self.maximumReviewPageAttempts else {
                        guard requestOwner.complete() else { return }
                        completion(.failure(error))
                        return
                    }
                    guard let expectedTransportContext,
                          client.currentTransportContext() == expectedTransportContext else {
                        guard requestOwner.complete() else { return }
                        completion(.failure(NativeMCPError.transportContextChanged))
                        return
                    }
                    requestPage(limit: max(1, limit / 2))
                    return
                }

                let validatedResult = result.flatMap { data in
                    do {
                        let response = try decoder.decode(PaperSessionReviewResponse.self, from: data)
                        try Self.validateReviewResponse(
                            response,
                            sessionId: sessionId,
                            afterSequence: afterSequence,
                            asOfSequence: asOfSequence,
                            limit: limit
                        )
                        return .success(response)
                    } catch {
                        return .failure(error)
                    }
                }
                guard requestOwner.complete() else { return }
                completion(validatedResult)
            }

            guard requestOwner.isPending, attempts == attempt else {
                request.cancel()
                return
            }
            guard request.isPending else { return }
            guard requestOwner.replaceSingleChild(with: request) else {
                guard requestOwner.complete() else { return }
                completion(.failure(NativeMCPError.protocolError))
                return
            }
        }

        requestPage(limit: limit)
        return requestOwner
    }

    @discardableResult
    func reviewDecision(
        sessionId: String,
        eventSequence: UInt64,
        asOfSequence: UInt64,
        signalID: String?,
        expectedPolicyReference: PaperPolicyRevisionReference,
        completion: @escaping (Result<PaperDecisionReviewResponse, Error>) -> Void
    ) -> NativeMCPRequest {
        let arguments: [String: Any]
        do {
            arguments = try Self.decisionReviewArguments(
                sessionId: sessionId,
                eventSequence: eventSequence,
                asOfSequence: asOfSequence,
                signalID: signalID,
                expectedPolicyReference: expectedPolicyReference
            )
        } catch {
            completion(.failure(error))
            return .alreadyFinished()
        }

        return client.call("paper_review_query", arguments: arguments) { [decoder] result in
            completion(result.flatMap { data in
                do {
                    let response = try decoder.decode(PaperDecisionReviewResponse.self, from: data)
                    try Self.validateDecisionReviewResponse(
                        response,
                        sessionId: sessionId,
                        eventSequence: eventSequence,
                        asOfSequence: asOfSequence,
                        signalID: signalID,
                        expectedPolicyReference: expectedPolicyReference
                    )
                    return .success(response)
                } catch {
                    return .failure(error)
                }
            })
        }
    }

    static func decisionReviewArguments(
        sessionId: String,
        eventSequence: UInt64,
        asOfSequence: UInt64,
        signalID: String?,
        expectedPolicyReference: PaperPolicyRevisionReference
    ) throws -> [String: Any] {
        guard isReviewIdentifier(sessionId), eventSequence > 0, eventSequence <= asOfSequence,
              signalID.map(isReviewIdentifier) ?? true,
              isReviewIdentifier(expectedPolicyReference.policy_id),
              isReviewIdentifier(expectedPolicyReference.revision_id),
              isLowerHexSHA256(expectedPolicyReference.definition_digest) else {
            throw PaperSessionServiceError.invalidInput("saved decision identity, fixed cutoff, or policy reference")
        }
        var selection: [String: Any] = [
            "session_id": sessionId,
            "event_sequence": eventSequence,
            "as_of_sequence": asOfSequence
        ]
        if let signalID { selection["signal_id"] = signalID }
        return ["action": "decision", "selection": selection]
    }

    private static func validateDecisionReviewResponse(
        _ response: PaperDecisionReviewResponse,
        sessionId: String,
        eventSequence: UInt64,
        asOfSequence: UInt64,
        signalID: String?,
        expectedPolicyReference: PaperPolicyRevisionReference
    ) throws {
        guard response.schema_version == "1.0",
              response.selection.session_id == sessionId,
              response.selection.event_sequence == eventSequence,
              response.selection.as_of_sequence == asOfSequence,
              response.selection.signal_id == signalID,
              response.event.sequence == eventSequence,
              eventSequence <= asOfSequence,
              response.configuration.policy_ref == expectedPolicyReference,
              response.policy.reference == expectedPolicyReference,
              isLowerHexSHA256(response.original_event_digest),
              response.selected_signal_id == signalID else {
            throw PaperSessionServiceError.invalidResponse("saved decision, event, cutoff, signal, or policy identity")
        }

        let signalIDs = response.event.signals.map(\.signal_id)
        guard Set(signalIDs).count == signalIDs.count,
              signalID.map({ signalIDs.contains($0) }) ?? signalIDs.isEmpty else {
            throw PaperSessionServiceError.invalidResponse("selected signal does not match the saved event")
        }
        try validateRecordedDecisionPolicy(
            response.recorded_decision,
            expectedPolicyReference: expectedPolicyReference
        )
    }

    private static func validateRecordedDecisionPolicy(
        _ value: AutomationJSONValue?,
        expectedPolicyReference: PaperPolicyRevisionReference
    ) throws {
        guard let value, value != .null else { return }
        guard case .object(let fields) = value,
              let policyValue = fields["policy_ref"], policyValue != .null else { return }
        guard case .object(let reference) = policyValue,
              case .string(let policyID)? = reference["policy_id"],
              case .string(let revisionID)? = reference["revision_id"],
              case .string(let digest)? = reference["definition_digest"],
              policyID == expectedPolicyReference.policy_id,
              revisionID == expectedPolicyReference.revision_id,
              digest == expectedPolicyReference.definition_digest else {
            throw PaperSessionServiceError.invalidResponse("recorded decision policy reference mismatch")
        }
    }

    private static func validateReviewRequest(
        sessionId: String,
        afterSequence: UInt64?,
        asOfSequence: UInt64,
        limit: Int
    ) throws {
        guard isReviewIdentifier(sessionId), (1...100).contains(limit) else {
            throw PaperSessionServiceError.invalidInput("session_id 또는 limit")
        }
        if (afterSequence ?? 0) > asOfSequence {
            throw PaperSessionServiceError.invalidInput("after_sequence exceeds as_of_sequence")
        }
    }

    private static func validateReviewResponse(
        _ response: PaperSessionReviewResponse,
        sessionId: String,
        afterSequence: UInt64?,
        asOfSequence: UInt64,
        limit: Int
    ) throws {
        let expectedAfter = afterSequence ?? 0
        guard response.schema_version == "1.0",
              response.mode == .simulated,
              response.after_sequence == expectedAfter,
              response.as_of_sequence == asOfSequence,
              response.as_of_sequence >= expectedAfter,
              response.page.session_id == sessionId else {
            throw PaperSessionServiceError.invalidResponse("review schema/mode/session/cutoff identity")
        }

        let configuration = response.configuration
        let marketIDs = configuration.markets.map(\.market)
        guard isReviewIdentifier(configuration.request_id),
              isReviewIdentifier(configuration.policy_ref.policy_id),
              isReviewIdentifier(configuration.policy_ref.revision_id),
              isLowerHexSHA256(configuration.policy_ref.definition_digest),
              !configuration.decision_interval.isEmpty,
              !configuration.execution_resolution.isEmpty,
              !configuration.execution.kind.isEmpty,
              !configuration.execution.participation_cap.isEmpty,
              !configuration.markets.isEmpty,
              marketIDs.allSatisfy(isReviewIdentifier),
              Set(marketIDs).count == marketIDs.count,
              configuration.markets.allSatisfy({ !$0.initialCash.isEmpty }) else {
            throw PaperSessionServiceError.invalidResponse("review immutable configuration identity")
        }

        let items = response.page.items
        let sequences = items.map(\.sequence)
        guard response.page.returned_count == UInt64(items.count),
              items.count <= limit,
              response.page.total_count == response.as_of_sequence,
              sequences.allSatisfy({ $0 > expectedAfter && $0 <= response.as_of_sequence }) else {
            throw PaperSessionServiceError.invalidResponse("review page count or sequence bounds")
        }
        for pair in zip(sequences, sequences.dropFirst()) where pair.0 >= pair.1 {
            throw PaperSessionServiceError.invalidResponse("review event sequence ordering")
        }

        if let cursor = response.page.next_cursor {
            guard items.count == limit,
                  let lastSequence = sequences.last,
                  cursor.sequence == lastSequence,
                  cursor.sequence < response.as_of_sequence else {
                throw PaperSessionServiceError.invalidResponse("review next cursor does not match the returned page")
            }
        } else if expectedAfter < response.as_of_sequence,
                  sequences.last != response.as_of_sequence {
            throw PaperSessionServiceError.invalidResponse("review page omitted events before the fixed cutoff")
        }
    }

    private static func isReviewIdentifier(_ value: String) -> Bool {
        let bytes = Array(value.utf8)
        return (1...96).contains(bytes.count) && bytes.allSatisfy {
            (48...57).contains($0) || (65...90).contains($0)
                || (97...122).contains($0) || $0 == 45 || $0 == 95
        }
    }

    private static func isLowerHexSHA256(_ value: String) -> Bool {
        let bytes = Array(value.utf8)
        return bytes.count == 64 && bytes.allSatisfy {
            (48...57).contains($0) || (97...102).contains($0)
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
