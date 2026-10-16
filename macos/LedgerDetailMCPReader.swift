import Foundation

enum LedgerDetailError: LocalizedError {
    case invalidResponse(String)
    case identityMismatch(String)
    case countMismatch(String)
    case unsupportedUnit(String)

    var errorDescription: String? {
        switch self {
        case .invalidResponse(let detail): return "원장 응답 형식이 유효하지 않습니다: \(detail)"
        case .identityMismatch(let detail): return "선택한 실행·모델과 다른 원장 응답입니다: \(detail)"
        case .countMismatch(let detail): return "원장 건수가 실행 요약과 일치하지 않습니다: \(detail)"
        case .unsupportedUnit(let unit): return "지원하지 않는 비용 단위입니다: \(unit)"
        }
    }
}

enum LedgerDetailSection: String, CaseIterable {
    case fills, orders, orderEvents = "order_events", episodes, signals

    var title: String {
        switch self {
        case .fills: return "체결"
        case .orders: return "주문 최종 상태"
        case .orderEvents: return "주문 이벤트"
        case .episodes: return "거래 에피소드"
        case .signals: return "시그널"
        }
    }
}

struct LedgerDetailCursor: Equatable {
    let modelId: String
    let offset: UInt64
}

struct ModelLedgerCostDetail {
    let runId: String
    let modelId: String
    let policyId: String?
    let policyRevisionId: String?
    let policyDefinitionDigest: String?
    let status: String?
    let fillCount: Int
    let buyNotional: Decimal
    let sellNotional: Decimal
    let explicitFees: Decimal
    /// Signed KRW diagnostic already embedded in fill prices. Never debit this again.
    let embeddedPriceCost: Decimal
    let terminalEquity: Decimal?
    let terminalMarkSequence: UInt64?
}

struct LedgerEventIdentity {
    let runId: String
    let modelId: String
    let market: String
    let sequence: UInt64
    let accountingTime: Date
    let accountingTimeRaw: String
}

enum LedgerFillTimingDetail {
    case exact(at: Date, raw: String)
    case interval(start: Date, startRaw: String, end: Date, endRaw: String,
                  accountingAt: Date, accountingRaw: String)
}

struct LedgerFillDetail {
    let identity: LedgerEventIdentity
    let fillId: String
    let orderId: String
    let episodeId: String
    let side: String
    let price: Decimal
    let quantity: Decimal
    let notional: Decimal
    let fee: Decimal
    let feeBps: Decimal
    let feeCurrency: String?
    let adverseSlippageBps: Decimal
    /// Signed KRW-per-base-unit diagnostic; this is not a standalone cash debit.
    let priceDifferencePerUnit: Decimal
    let embeddedPriceCost: Decimal
    let decisionReference: Decimal
    let barOpenProxy: Decimal
    let arrivalMid: Decimal?
    let arrivalMidNullReason: String
    let executionOrigin: String
    let fillObserved: Bool
    let timing: LedgerFillTimingDetail
    let sourceBarId: String
    let liquiditySourceBarId: String
    let liquiditySourceCloseTime: Date
    let liquiditySourceCloseTimeRaw: String
    let liquidityRoleAssumption: String
    let modelVersion: String
    let accountingMarkSequence: UInt64
    let artificialTerminalExit: Bool
    let rawJSON: String
}

struct LedgerOrderDetail {
    let identity: LedgerEventIdentity
    let orderId: String
    let parentSignalId: String
    let episodeId: String?
    let side: String
    let orderType: String
    let requestedPrice: Decimal?
    let requestedQuantity: Decimal
    let reservedCash: Decimal
    let cumulativeFilledQuantity: Decimal
    let status: String
    let reason: String
    let createdAt: Date
    let createdAtRaw: String
    let rawJSON: String
}

struct LedgerEpisodeDetail {
    let runId: String
    let modelId: String
    let market: String
    let episodeId: String
    let status: String
    let openedAt: Date
    let openedAtRaw: String
    let closedAt: Date?
    let closedAtRaw: String?
    let fillCount: Int
    let orderCount: Int
    let buyVWAP: Decimal
    let sellVWAP: Decimal?
    let maxQuantity: Decimal
    let holdingSeconds: UInt64
    let realizedPricePnL: Decimal
    let fees: Decimal
    let netRealized: Decimal
    let residualQuantity: Decimal
    let residualBasis: Decimal
    let markedUnrealized: Decimal
    let exitReason: String?
    let rawJSON: String
}

struct LedgerSignalDetail {
    let identity: LedgerEventIdentity
    let signalId: String
    let strategy: String
    let intendedSide: String?
    let rawTargetWeight: Decimal
    let constrainedTargetWeight: Decimal
    let outcome: String
    let reasons: [String]
    let signalTime: Date
    let signalTimeRaw: String
    let rawJSON: String
}

enum LedgerDetailRecord {
    case fill(LedgerFillDetail)
    case order(LedgerOrderDetail)
    case episode(LedgerEpisodeDetail)
    case signal(LedgerSignalDetail)

    var rawJSON: String {
        switch self {
        case .fill(let value): return value.rawJSON
        case .order(let value): return value.rawJSON
        case .episode(let value): return value.rawJSON
        case .signal(let value): return value.rawJSON
        }
    }

    var eventSequence: UInt64? {
        switch self {
        case .fill(let value): return value.identity.sequence
        case .order(let value): return value.identity.sequence
        case .signal(let value): return value.identity.sequence
        case .episode: return nil
        }
    }

    var market: String {
        switch self {
        case .fill(let value): return value.identity.market
        case .order(let value): return value.identity.market
        case .episode(let value): return value.market
        case .signal(let value): return value.identity.market
        }
    }
}

struct LedgerDetailPage {
    let section: LedgerDetailSection
    let records: [LedgerDetailRecord]
    let returnedCount: Int
    let totalCount: Int
    let nextCursor: LedgerDetailCursor?
    let truncatedReason: String?

    var typedFills: [LedgerFillDetail] {
        records.compactMap { if case .fill(let fill) = $0 { return fill }; return nil }
    }
}

/// Read-only bounded adapter for immutable result_query projections.
final class LedgerDetailMCPReader {
    private let client: NativeMCPClient
    init(client: NativeMCPClient = .shared) { self.client = client }

    func costs(
        runId: String,
        modelId: String,
        expectedFillCount: Int,
        expectedTerminalEquity: Decimal?,
        expectedPolicyId: String?,
        expectedPolicyRevisionId: String?,
        expectedPolicyDefinitionDigest: String?,
        completion: @escaping (Result<ModelLedgerCostDetail, Error>) -> Void
    ) {
        guard let expectedPolicyId, let expectedPolicyRevisionId, let expectedPolicyDefinitionDigest,
              Self.isLowerHex64(expectedPolicyDefinitionDigest) else {
            completion(.failure(LedgerDetailError.identityMismatch("선택 모델의 정책 ID/리비전/정의 digest를 확인할 수 없음")))
            return
        }
        client.call("result_query", arguments: [
            "action": "costs", "run_id": runId, "model_id": modelId
        ]) { result in
            do {
                let value = try Self.decodeCosts(data: result.get(), runId: runId, modelId: modelId,
                                                 expectedPolicyId: expectedPolicyId,
                                                 expectedPolicyRevisionId: expectedPolicyRevisionId,
                                                 expectedPolicyDefinitionDigest: expectedPolicyDefinitionDigest)
                guard value.fillCount == expectedFillCount else {
                    throw LedgerDetailError.countMismatch("비용 체결 \(value.fillCount)건 / 실행 요약 \(expectedFillCount)건")
                }
                if let expected = expectedTerminalEquity {
                    guard value.terminalEquity == expected else {
                        throw LedgerDetailError.countMismatch("최종 평가액이 실행 요약과 다릅니다")
                    }
                }
                completion(.success(value))
            } catch { completion(.failure(error)) }
        }
    }

    func page(
        runId: String,
        modelId: String,
        section: LedgerDetailSection,
        cursor: LedgerDetailCursor? = nil,
        expectedTotalCount: Int? = nil,
        expectedMarket: String? = nil,
        completion: @escaping (Result<LedgerDetailPage, Error>) -> Void
    ) {
        guard cursor == nil || cursor?.modelId == modelId else {
            completion(.failure(LedgerDetailError.identityMismatch("cursor model_id"))); return
        }
        var query: [String: Any] = [
            "run_id": runId, "model_id": modelId, "section": section.rawValue, "limit": 100
        ]
        if let cursor {
            query["cursor"] = ["model_id": cursor.modelId, "offset": cursor.offset]
        }
        client.call("result_query", arguments: ["action": "ledger", "query": query]) { result in
            do {
                let page = try Self.decodePage(data: result.get(), runId: runId, modelId: modelId,
                                               section: section, requestedCursor: cursor)
                if let expectedTotalCount, page.totalCount != expectedTotalCount {
                    throw LedgerDetailError.countMismatch("\(section.title) \(page.totalCount)건 / 실행 요약 \(expectedTotalCount)건")
                }
                if let expectedMarket, page.records.contains(where: { $0.market != expectedMarket }) {
                    throw LedgerDetailError.identityMismatch("market")
                }
                completion(.success(page))
            } catch { completion(.failure(error)) }
        }
    }

    static func decodeCosts(data: Data, runId: String, modelId: String,
                            expectedPolicyId: String? = nil,
                            expectedPolicyRevisionId: String? = nil,
                            expectedPolicyDefinitionDigest: String? = nil) throws -> ModelLedgerCostDetail {
        let object = try dictionary(data, label: "costs")
        guard try string(object, "run_id") == runId, try string(object, "model_id") == modelId else {
            throw LedgerDetailError.identityMismatch("costs run_id/model_id")
        }
        let unit = try string(object, "price_cost_attribution_unit")
        guard unit == "KRW" else { throw LedgerDetailError.unsupportedUnit(unit) }
        let fillCount = try nonnegativeInt(object, "fill_count")
        let terminalEquity = try optionalDecimal(object, "terminal_equity")
        let terminalSequence = try optionalUInt64(object, "terminal_mark_seq")
        guard (terminalEquity == nil) == (terminalSequence == nil) else {
            throw LedgerDetailError.invalidResponse("terminal_equity/terminal_mark_seq 쌍")
        }
        let policy = object["policy_ref"] as? [String: Any]
        let definitionDigest = try policy.map { reference -> String in
            let digest = try string(reference, "definition_digest")
            guard isLowerHex64(digest) else {
                throw LedgerDetailError.invalidResponse("policy definition digest")
            }
            return digest
        }
        let result = ModelLedgerCostDetail(
            runId: runId, modelId: modelId,
            policyId: try policy.map { try string($0, "policy_id") },
            policyRevisionId: try policy.map { try string($0, "revision_id") },
            policyDefinitionDigest: definitionDigest,
            status: try optionalString(object, "status"), fillCount: fillCount,
            buyNotional: try decimal(object, "buy_notional"),
            sellNotional: try decimal(object, "sell_notional"),
            explicitFees: try decimal(object, "total_fees"),
            embeddedPriceCost: try decimal(object, "price_cost_attribution"),
            terminalEquity: terminalEquity, terminalMarkSequence: terminalSequence
        )
        if let expectedPolicyId, result.policyId != expectedPolicyId {
            throw LedgerDetailError.identityMismatch("정책 ID")
        }
        if let expectedPolicyRevisionId, result.policyRevisionId != expectedPolicyRevisionId {
            throw LedgerDetailError.identityMismatch("정책 리비전")
        }
        if let expectedPolicyDefinitionDigest,
           result.policyDefinitionDigest != expectedPolicyDefinitionDigest {
            throw LedgerDetailError.identityMismatch("정책 정의 digest")
        }
        return result
    }

    static func decodePage(
        data: Data, runId: String, modelId: String, section: LedgerDetailSection,
        requestedCursor: LedgerDetailCursor?
    ) throws -> LedgerDetailPage {
        let envelope = try dictionary(data, label: "ledger")
        guard try string(envelope, "section") == section.rawValue,
              let page = envelope["page"] as? [String: Any],
              let rawRecords = page["records"] as? [[String: Any]] else {
            throw LedgerDetailError.invalidResponse("section/page/records")
        }
        let returned = try nonnegativeInt(page, "returned_count")
        let total = try nonnegativeInt(page, "total_count")
        guard returned == rawRecords.count, returned <= 100, total >= returned else {
            throw LedgerDetailError.countMismatch("page returned_count/total_count")
        }
        let records = try rawRecords.map { record -> LedgerDetailRecord in
            switch section {
            case .fills: return .fill(try decodeFill(record, runId: runId, modelId: modelId))
            case .orders, .orderEvents: return .order(try decodeOrder(record, runId: runId, modelId: modelId))
            case .episodes: return .episode(try decodeEpisode(record, runId: runId, modelId: modelId))
            case .signals: return .signal(try decodeSignal(record, runId: runId, modelId: modelId))
            }
        }
        if section != .episodes {
            let sequences = records.compactMap(\.eventSequence)
            guard sequences.count == records.count,
                  zip(sequences, sequences.dropFirst()).allSatisfy({ $0 < $1 }) else {
                throw LedgerDetailError.invalidResponse("event_seq ordering")
            }
        }
        let next: LedgerDetailCursor?
        if page["next_cursor"] == nil || page["next_cursor"] is NSNull {
            next = nil
        } else {
            guard let cursor = page["next_cursor"] as? [String: Any] else {
                throw LedgerDetailError.invalidResponse("next_cursor")
            }
            let decoded = LedgerDetailCursor(modelId: try string(cursor, "model_id"),
                                             offset: try uint64(cursor, "offset"))
            let requestedOffset = requestedCursor?.offset ?? 0
            guard decoded.modelId == modelId,
                  decoded.offset == requestedOffset + UInt64(returned),
                  decoded.offset < UInt64(total) else {
                throw LedgerDetailError.identityMismatch("next_cursor")
            }
            next = decoded
        }
        if next == nil {
            let requestedOffset = requestedCursor?.offset ?? 0
            guard requestedOffset + UInt64(returned) == UInt64(total) else {
                throw LedgerDetailError.countMismatch("마지막 페이지 cursor")
            }
        }
        return LedgerDetailPage(section: section, records: records, returnedCount: returned,
                                totalCount: total, nextCursor: next,
                                truncatedReason: try optionalString(page, "truncated_reason"))
    }

    private static func decodeFill(_ value: [String: Any], runId: String, modelId: String) throws -> LedgerFillDetail {
        let unit = try string(value, "embedded_price_cost_quote_unit")
        guard unit == "KRW" else { throw LedgerDetailError.unsupportedUnit(unit) }
        guard try string(value, "price_cost_attribution_unit") == "KRW_PER_BASE_UNIT" else {
            throw LedgerDetailError.unsupportedUnit(try string(value, "price_cost_attribution_unit"))
        }
        let side = try string(value, "side")
        guard side == "BUY" || side == "SELL" else {
            throw LedgerDetailError.invalidResponse("fill side")
        }
        let feeCurrency = try optionalString(value, "fee_currency")
        guard feeCurrency == nil || feeCurrency == "KRW" else {
            throw LedgerDetailError.unsupportedUnit(feeCurrency ?? "fee_currency")
        }
        let liquidityCloseRaw = try string(value, "liquidity_source_close_time")
        return LedgerFillDetail(identity: try identity(value, runId: runId, modelId: modelId),
            fillId: try string(value, "fill_id"), orderId: try string(value, "order_id"),
            episodeId: try string(value, "episode_id"), side: side,
            price: try decimal(value, "price"), quantity: try decimal(value, "qty"),
            notional: try decimal(value, "notional"), fee: try decimal(value, "fee"),
            feeBps: try decimal(value, "fee_bps"), feeCurrency: feeCurrency,
            adverseSlippageBps: try decimal(value, "adverse_slippage_bps"),
            priceDifferencePerUnit: try decimal(value, "price_difference_per_unit"),
            embeddedPriceCost: try decimal(value, "embedded_price_cost_quote"),
            decisionReference: try decimal(value, "decision_reference"),
            barOpenProxy: try decimal(value, "bar_open_proxy"),
            arrivalMid: try optionalDecimal(value, "arrival_mid"),
            arrivalMidNullReason: try string(value, "arrival_mid_null_reason"),
            executionOrigin: try string(value, "execution_origin"),
            fillObserved: try bool(value, "fill_observed"),
            timing: try fillTiming(value),
            sourceBarId: try string(value, "source_bar_id"),
            liquiditySourceBarId: try string(value, "liquidity_source_bar_id"),
            liquiditySourceCloseTime: try timestamp(liquidityCloseRaw),
            liquiditySourceCloseTimeRaw: liquidityCloseRaw,
            liquidityRoleAssumption: try string(value, "liquidity_role_assumption"),
            modelVersion: try string(value, "model_version"),
            accountingMarkSequence: try uint64(value, "accounting_mark_seq"),
            artificialTerminalExit: try bool(value, "artificial_terminal_exit"), rawJSON: try prettyJSON(value))
    }

    private static func fillTiming(_ value: [String: Any]) throws -> LedgerFillTimingDetail {
        guard let timing = value["timing"] as? [String: Any] else {
            throw LedgerDetailError.invalidResponse("timing")
        }
        switch try string(timing, "kind") {
        case "EXACT":
            let raw = try string(timing, "at")
            return .exact(at: try timestamp(raw), raw: raw)
        case "INTERVAL":
            let start = try string(timing, "start"), end = try string(timing, "end")
            let accounting = try string(timing, "accounting_at")
            let startDate = try timestamp(start), endDate = try timestamp(end)
            let accountingDate = try timestamp(accounting)
            guard startDate <= accountingDate, accountingDate <= endDate else {
                throw LedgerDetailError.invalidResponse("timing interval ordering")
            }
            return .interval(start: startDate, startRaw: start, end: endDate, endRaw: end,
                             accountingAt: accountingDate, accountingRaw: accounting)
        default:
            throw LedgerDetailError.invalidResponse("timing kind")
        }
    }

    private static func decodeOrder(_ value: [String: Any], runId: String, modelId: String) throws -> LedgerOrderDetail {
        let createdRaw = try string(value, "created_at")
        return LedgerOrderDetail(identity: try identity(value, runId: runId, modelId: modelId),
            orderId: try string(value, "order_id"), parentSignalId: try string(value, "parent_signal_id"),
            episodeId: try optionalString(value, "episode_id"), side: try string(value, "side"),
            orderType: try string(value, "order_type"), requestedPrice: try optionalDecimal(value, "requested_price"),
            requestedQuantity: try decimal(value, "requested_qty"), reservedCash: try decimal(value, "reserved_cash"),
            cumulativeFilledQuantity: try decimal(value, "cumulative_filled_qty"),
            status: try string(value, "status"), reason: try string(value, "reason"),
            createdAt: try timestamp(createdRaw), createdAtRaw: createdRaw, rawJSON: try prettyJSON(value))
    }

    private static func decodeEpisode(_ value: [String: Any], runId: String, modelId: String) throws -> LedgerEpisodeDetail {
        guard try string(value, "run_id") == runId, try string(value, "model_id") == modelId else {
            throw LedgerDetailError.identityMismatch("episode run_id/model_id")
        }
        let opened = try string(value, "opened_at")
        let closed = try optionalString(value, "closed_at")
        guard let fills = value["fill_ids"] as? [String], let orders = value["order_ids"] as? [String] else {
            throw LedgerDetailError.invalidResponse("episode fill_ids/order_ids")
        }
        return LedgerEpisodeDetail(runId: runId, modelId: modelId, market: try string(value, "market"),
            episodeId: try string(value, "episode_id"), status: try string(value, "status"),
            openedAt: try timestamp(opened), openedAtRaw: opened,
            closedAt: try closed.map(timestamp), closedAtRaw: closed,
            fillCount: fills.count, orderCount: orders.count, buyVWAP: try decimal(value, "buy_vwap"),
            sellVWAP: try optionalDecimal(value, "sell_vwap"), maxQuantity: try decimal(value, "max_qty"),
            holdingSeconds: try uint64(value, "holding_seconds"),
            realizedPricePnL: try decimal(value, "realized_price_pnl"), fees: try decimal(value, "fees"),
            netRealized: try decimal(value, "net_realized"), residualQuantity: try decimal(value, "residual_qty"),
            residualBasis: try decimal(value, "residual_basis"), markedUnrealized: try decimal(value, "marked_unrealized"),
            exitReason: try optionalString(value, "exit_reason"), rawJSON: try prettyJSON(value))
    }

    private static func decodeSignal(_ value: [String: Any], runId: String, modelId: String) throws -> LedgerSignalDetail {
        let signalTime = try string(value, "signal_time")
        guard let reasons = value["reasons"] as? [String] else {
            throw LedgerDetailError.invalidResponse("signal reasons")
        }
        return LedgerSignalDetail(identity: try identity(value, runId: runId, modelId: modelId),
            signalId: try string(value, "signal_id"), strategy: try string(value, "strategy"),
            intendedSide: try optionalString(value, "intended_side"),
            rawTargetWeight: try decimal(value, "raw_target_weight"),
            constrainedTargetWeight: try decimal(value, "constrained_target_weight"),
            outcome: try string(value, "outcome"), reasons: reasons,
            signalTime: try timestamp(signalTime), signalTimeRaw: signalTime, rawJSON: try prettyJSON(value))
    }

    private static func identity(_ value: [String: Any], runId: String, modelId: String) throws -> LedgerEventIdentity {
        guard let context = value["context"] as? [String: Any] else {
            throw LedgerDetailError.invalidResponse("context")
        }
        let actualRun = try string(context, "run_id"), actualModel = try string(context, "model_id")
        guard actualRun == runId, actualModel == modelId else {
            throw LedgerDetailError.identityMismatch("record context")
        }
        let raw = try string(context, "accounting_event_time")
        return LedgerEventIdentity(runId: runId, modelId: modelId, market: try string(context, "market"),
                                   sequence: try uint64(context, "event_seq"),
                                   accountingTime: try timestamp(raw), accountingTimeRaw: raw)
    }

    private static func dictionary(_ data: Data, label: String) throws -> [String: Any] {
        do {
            guard let value = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
                throw LedgerDetailError.invalidResponse(label)
            }
            return value
        } catch let error as LedgerDetailError { throw error }
        catch { throw LedgerDetailError.invalidResponse("\(label) JSON") }
    }

    private static func string(_ value: [String: Any], _ key: String) throws -> String {
        guard let text = value[key] as? String, !text.isEmpty, text.count <= 4096 else {
            throw LedgerDetailError.invalidResponse(key)
        }
        return text
    }
    private static func optionalString(_ value: [String: Any], _ key: String) throws -> String? {
        guard let item = value[key], !(item is NSNull) else { return nil }
        guard let text = item as? String, !text.isEmpty, text.count <= 4096 else {
            throw LedgerDetailError.invalidResponse(key)
        }
        return text
    }
    private static func bool(_ value: [String: Any], _ key: String) throws -> Bool {
        guard let item = value[key] as? Bool else { throw LedgerDetailError.invalidResponse(key) }
        return item
    }
    private static func decimal(_ value: [String: Any], _ key: String) throws -> Decimal {
        guard let text = value[key] as? String else { throw LedgerDetailError.invalidResponse("\(key) Decimal") }
        return try strictDecimal(text, key: key)
    }
    private static func optionalDecimal(_ value: [String: Any], _ key: String) throws -> Decimal? {
        guard let item = value[key], !(item is NSNull) else { return nil }
        guard let text = item as? String else { throw LedgerDetailError.invalidResponse("\(key) Decimal") }
        return try strictDecimal(text, key: key)
    }
    private static func uint64(_ value: [String: Any], _ key: String) throws -> UInt64 {
        guard let number = value[key] as? NSNumber, CFGetTypeID(number) != CFBooleanGetTypeID() else {
            throw LedgerDetailError.invalidResponse(key)
        }
        let text = number.stringValue
        guard !text.isEmpty, text.allSatisfy(\.isNumber), let exact = UInt64(text) else {
            throw LedgerDetailError.invalidResponse(key)
        }
        return exact
    }
    private static func optionalUInt64(_ value: [String: Any], _ key: String) throws -> UInt64? {
        guard let item = value[key], !(item is NSNull) else { return nil }
        return try uint64([key: item], key)
    }
    private static func nonnegativeInt(_ value: [String: Any], _ key: String) throws -> Int {
        let number = try uint64(value, key)
        guard number <= UInt64(Int.max) else { throw LedgerDetailError.invalidResponse(key) }
        return Int(number)
    }
    private static func timestamp(_ raw: String) throws -> Date {
        let precise = ISO8601DateFormatter(); precise.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        guard let date = precise.date(from: raw) ?? ISO8601DateFormatter().date(from: raw) else {
            throw LedgerDetailError.invalidResponse("timestamp")
        }
        return date
    }
    private static func strictDecimal(_ text: String, key: String) throws -> Decimal {
        guard !text.isEmpty, text.count <= 64,
              text.range(of: #"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$"#,
                         options: .regularExpression) != nil else {
            throw LedgerDetailError.invalidResponse("\(key) Decimal")
        }
        let unsigned = text.first == "-" ? String(text.dropFirst()) : text
        let components = unsigned.split(separator: ".", omittingEmptySubsequences: false)
        let fractionalCount = components.count == 2 ? components[1].count : 0
        let coefficient = unsigned.replacingOccurrences(of: ".", with: "")
        let significant = String(coefficient.drop(while: { $0 == "0" }))
        let maxCoefficient = "79228162514264337593543950335"
        guard fractionalCount <= 28, significant.count <= 29,
              significant.count < 29 || significant <= maxCoefficient,
              let number = Decimal(string: text, locale: Locale(identifier: "en_US_POSIX")),
              !number.isNaN else {
            throw LedgerDetailError.invalidResponse("\(key) Decimal")
        }
        return number
    }
    private static func isLowerHex64(_ value: String) -> Bool {
        value.count == 64 && value.utf8.allSatisfy { byte in
            (48...57).contains(byte) || (97...102).contains(byte)
        }
    }
    private static func prettyJSON(_ value: [String: Any]) throws -> String {
        guard JSONSerialization.isValidJSONObject(value),
              let text = String(data: try JSONSerialization.data(withJSONObject: value, options: [.prettyPrinted, .sortedKeys]),
                                encoding: .utf8), text.count <= 262_144 else {
            throw LedgerDetailError.invalidResponse("record JSON")
        }
        return text
    }
}
