import Foundation

private enum UpbitReadValidation {
    static func decimal(_ value: String) -> Decimal? {
        let unsigned = value.first == "-" ? String(value.dropFirst()) : value
        let parts = unsigned.split(separator: ".", omittingEmptySubsequences: false)
        let integer = parts.first.map(String.init) ?? ""
        let fraction = parts.count == 2 ? String(parts[1]) : ""
        let asciiDigits: (String) -> Bool = { text in
            !text.isEmpty && text.utf8.allSatisfy { (48...57).contains($0) }
        }
        let significant = String((integer + fraction).drop(while: { $0 == "0" }))
        let maximum = "79228162514264337593543950335"
        guard !value.isEmpty, value.count <= 64,
              parts.count == 1 || parts.count == 2,
              asciiDigits(integer), parts.count == 1 || asciiDigits(fraction),
              integer == "0" || integer.first != "0", fraction.count <= 28,
              significant.count <= 29,
              significant.count < 29 || significant <= maximum,
              let parsed = Decimal(string: value, locale: Locale(identifier: "en_US_POSIX")),
              !parsed.isNaN else { return nil }
        return parsed
    }

    static func utcTimestamp(_ raw: String) -> Date? {
        guard raw.hasSuffix("Z") || raw.hasSuffix("+00:00") else { return nil }
        return rfc3339(raw)
    }

    static func rfc3339(_ raw: String) -> Date? {
        let precise = ISO8601DateFormatter()
        precise.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return precise.date(from: raw) ?? ISO8601DateFormatter().date(from: raw)
    }

    static func orderRangeTimestamp(_ raw: String) -> Date? {
        if let milliseconds = Int64(raw) {
            return Date(timeIntervalSince1970: Double(milliseconds) / 1_000)
        }
        return rfc3339(raw)
    }

    static func market(_ raw: String) -> Bool {
        let parts = raw.split(separator: "-", omittingEmptySubsequences: false)
        return parts.count == 2 && parts.allSatisfy { part in
            (2...16).contains(part.count) && part.utf8.allSatisfy {
                (48...57).contains($0) || (65...90).contains($0)
            }
        }
    }

    static func currency(_ raw: String) -> Bool {
        (2...16).contains(raw.count) && raw.utf8.allSatisfy {
            (48...57).contains($0) || (65...90).contains($0)
        }
    }

    static func token(_ raw: String) -> Bool {
        !raw.isEmpty && raw.count <= 128
    }
}

struct UpbitLiveAccount: Codable {
    let currency: String
    let balance: String
    let locked: String
    let avg_buy_price: String
    let avg_buy_price_modified: Bool
    let unit_currency: String

    var balanceDecimal: Decimal? { UpbitReadValidation.decimal(balance) }
    var lockedDecimal: Decimal? { UpbitReadValidation.decimal(locked) }
    var averageBuyPriceDecimal: Decimal? { UpbitReadValidation.decimal(avg_buy_price) }
    var balanceDouble: Double { balanceDecimal.map { NSDecimalNumber(decimal: $0).doubleValue } ?? .nan }
    var lockedDouble: Double { lockedDecimal.map { NSDecimalNumber(decimal: $0).doubleValue } ?? .nan }
}

struct UpbitLiveOrder: Codable {
    let uuid: String
    let side: String
    let ord_type: String
    let price: String?
    let state: String
    let market: String
    let created_at: String
    let volume: String?
    let remaining_volume: String?
    let executed_volume: String?
    let paid_fee: String?

    var priceDecimal: Decimal? { price.flatMap(UpbitReadValidation.decimal) }
    var volumeDecimal: Decimal? { volume.flatMap(UpbitReadValidation.decimal) }
    var remainingVolumeDecimal: Decimal? { remaining_volume.flatMap(UpbitReadValidation.decimal) }
    var executedVolumeDecimal: Decimal? { executed_volume.flatMap(UpbitReadValidation.decimal) }
    var paidFeeDecimal: Decimal? { paid_fee.flatMap(UpbitReadValidation.decimal) }
    var createdTimestamp: Date? { UpbitReadValidation.rfc3339(created_at) }

    var sideDisplay: String {
        switch side {
        case "bid": return "매수"
        case "ask": return "매도"
        default: return "미확인 (\(side))"
        }
    }
    var orderTypeDisplay: String {
        switch ord_type {
        case "limit": return "지정가"
        case "price": return "시장가 매수"
        case "market": return "시장가 매도"
        case "best": return "최유리"
        default: return "미확인 (\(ord_type))"
        }
    }
    var stateDisplay: String {
        if state == "wait", let executed = executedVolumeDecimal, executed > 0 { return "부분 체결" }
        switch state {
        case "wait", "watch": return "미체결"
        case "done": return "완료"
        case "cancel": return "취소"
        default: return "미확인 (\(state))"
        }
    }
}

struct UpbitOrdersSnapshot: Decodable {
    let exchange_read_mode: String
    let order_scope: String
    let observed_at_utc: String
    let page: Int?
    let limit: Int
    let returned_count: Int
    let records: [UpbitLiveOrder]
    let next_page: Int?
    let truncated_reason: String?
    let start_time: String?
    let end_time: String?

    var observedTimestamp: Date? { UpbitReadValidation.utcTimestamp(observed_at_utc) }
    var startTimestamp: Date? { start_time.flatMap(UpbitReadValidation.orderRangeTimestamp) }
    var endTimestamp: Date? { end_time.flatMap(UpbitReadValidation.orderRangeTimestamp) }
}

/// Read projection over Rust MCP. Swift never reads exchange secrets or signs JWTs.
final class UpbitLiveService {
    static let shared = UpbitLiveService()
    static let liveOrderEnableConfirmation = "ENABLE_LIVE_ORDER_SENDING"
    private let client: NativeMCPClient
    private(set) var lastAccountsObservedAt: String?
    private(set) var lastOpenOrders: UpbitOrdersSnapshot?
    private(set) var lastClosedOrders: UpbitOrdersSnapshot?
    init(client: NativeMCPClient = .shared) { self.client = client }

    @discardableResult
    func fetchControlStatus(completion: @escaping (Result<TradingControlSnapshot, Error>) -> Void) -> NativeMCPRequest? {
        fetchControlStatusRequest(completion: completion)
    }

    @discardableResult
    func fetchControlStatusRequest(
        completion: @escaping (Result<TradingControlSnapshot, Error>) -> Void) -> NativeMCPRequest {
        client.call("trading_control", arguments: ["action": "status"]) { result in
            completion(result.flatMap { data in
                Result { try JSONDecoder().decode(TradingControlSnapshot.self, from: data) }
            })
        }
    }

    func currentTransportContext() -> NativeMCPTransportContext? {
        client.currentTransportContext()
    }

    func isCurrentTransportContext(_ context: NativeMCPTransportContext) -> Bool {
        client.currentTransportContext() == context
    }

    @discardableResult
    func fetchLiveOrderControlStatus(
        expectedContext: NativeMCPTransportContext,
        completion: @escaping (Result<LiveOrderControlSnapshot, Error>) -> Void
    ) -> NativeMCPRequest {
        client.call(
            "live_order_control",
            arguments: ["action": "status"],
            expectedTransportContext: expectedContext
        ) { result in
            completion(result.flatMap { data in
                Result { try JSONDecoder().decode(LiveOrderControlSnapshot.self, from: data) }
            })
        }
    }

    @discardableResult
    func changeLiveOrderControl(
        action: String,
        requestID: String,
        expectedSequence: UInt64,
        expectedEpoch: String,
        confirmation: String? = nil,
        expectedContext: NativeMCPTransportContext,
        completion: @escaping (Result<LiveOrderControlSnapshot, Error>) -> Void
    ) -> NativeMCPRequest {
        guard ["enable", "disable"].contains(action), !requestID.isEmpty,
              !expectedEpoch.isEmpty,
              (action == "enable" ? confirmation == Self.liveOrderEnableConfirmation : confirmation == nil) else {
            completion(.failure(NativeMCPError.unauthorized))
            return .alreadyFinished()
        }
        var arguments: [String: Any] = [
            "action": action,
            "request_id": requestID,
            "expected_sequence": expectedSequence,
            "expected_epoch": expectedEpoch
        ]
        if let confirmation { arguments["confirmation"] = confirmation }
        return client.call(
            "live_order_control",
            arguments: arguments,
            expectedTransportContext: expectedContext
        ) { result in
            completion(result.flatMap { data in
                Result { try JSONDecoder().decode(LiveOrderControlSnapshot.self, from: data) }
            })
        }
    }

    @discardableResult
    func readLiveOrderControlRequest(
        requestID: String,
        expectedContext: NativeMCPTransportContext,
        completion: @escaping (Result<LiveOrderControlSnapshot, Error>) -> Void
    ) -> NativeMCPRequest {
        guard !requestID.isEmpty else {
            completion(.failure(NativeMCPError.unauthorized))
            return .alreadyFinished()
        }
        return client.call(
            "live_order_control",
            arguments: ["action": "request", "request_id": requestID],
            expectedTransportContext: expectedContext
        ) { result in
            completion(result.flatMap { data in
                Result { try JSONDecoder().decode(LiveOrderControlSnapshot.self, from: data) }
            })
        }
    }

    @discardableResult
    func fetchAccountSnapshot(completion: @escaping (Result<UpbitAccountSnapshot, Error>) -> Void) -> NativeMCPRequest {
        readLive("upbit_account_balances", arguments: [:]) { [weak self] (result: Result<UpbitAccountSnapshot, Error>) in
            guard let self else { completion(.failure(NativeMCPError.unavailable)); return }
            completion(result.flatMap { self.validateAccountSnapshot($0) })
        }
    }

    @discardableResult
    func fetchAccounts(completion: @escaping (Result<[UpbitLiveAccount], Error>) -> Void) -> NativeMCPRequest? {
        fetchAccountSnapshot { result in completion(result.map(\.balances)) }
    }

    func safeControl(action: String, requestId: String, sequence: UInt64,
                     expectedContext: NativeMCPTransportContext? = nil,
                     completion: @escaping (Result<TradingControlSnapshot, Error>) -> Void) {
        guard ["start", "resume", "pause", "emergency_stop"].contains(action), !requestId.isEmpty else {
            completion(.failure(NativeMCPError.unauthorized)); return
        }
        client.call("trading_control", arguments: ["action": action,
            "request_id": requestId, "expected_sequence": sequence], expectedTransportContext: expectedContext) { result in
            completion(result.flatMap { data in Result {
                let snapshot = try JSONDecoder().decode(TradingControlSnapshot.self, from: data)
                guard let receipt = snapshot.receipt, receipt.request_id == requestId,
                      receipt.action == action, receipt.sequence <= snapshot.sequence,
                      receipt.sequence >= sequence else {
                    throw NativeMCPError.protocolError
                }
                return snapshot
            } })
        }
    }

    @discardableResult
    func fetchOpenOrdersSnapshot(page: Int = 1,
        completion: @escaping (Result<UpbitOrdersSnapshot, Error>) -> Void) -> NativeMCPRequest {
        readOrdersSnapshot(scope: "OPEN", tool: "upbit_orders_open",
            arguments: ["limit": 100, "page": page], completion: completion)
    }

    @discardableResult
    func fetchClosedOrdersSnapshot(
        completion: @escaping (Result<UpbitOrdersSnapshot, Error>) -> Void) -> NativeMCPRequest {
        readOrdersSnapshot(scope: "CLOSED", tool: "upbit_orders_closed",
            arguments: ["limit": 100], completion: completion)
    }

    @discardableResult
    func fetchOpenOrders(page: Int = 1,
        completion: @escaping (Result<[UpbitLiveOrder], Error>) -> Void) -> NativeMCPRequest {
        fetchOpenOrdersSnapshot(page: page) { result in completion(result.map(\.records)) }
    }

    @discardableResult
    func fetchClosedOrders(completion: @escaping (Result<[UpbitLiveOrder], Error>) -> Void) -> NativeMCPRequest {
        fetchClosedOrdersSnapshot { result in completion(result.map(\.records)) }
    }

    private func validateAccountSnapshot(_ snapshot: UpbitAccountSnapshot) -> Result<UpbitAccountSnapshot, Error> {
        guard snapshot.mode == "LIVE_HTTP_READ" else { return .failure(NativeMCPError.mockData) }
        guard snapshot.accounts_count == snapshot.balances.count, snapshot.accounts_count <= 1000,
              snapshot.observedTimestamp != nil else { return .failure(NativeMCPError.protocolError) }
        var currencies = Set<String>()
        guard snapshot.balances.allSatisfy({ row in
            guard UpbitReadValidation.currency(row.currency), UpbitReadValidation.currency(row.unit_currency),
                  currencies.insert(row.currency).inserted,
                  let available = row.balanceDecimal, let locked = row.lockedDecimal,
                  let average = row.averageBuyPriceDecimal else { return false }
            return available >= 0 && locked >= 0 && average >= 0
        }) else { return .failure(NativeMCPError.protocolError) }
        lastAccountsObservedAt = snapshot.observed_at_utc
        return .success(snapshot)
    }

    private func readOrdersSnapshot(scope: String, tool: String, arguments: [String: Any],
        completion: @escaping (Result<UpbitOrdersSnapshot, Error>) -> Void) -> NativeMCPRequest {
        readLive(tool, arguments: arguments) { [weak self] (result: Result<UpbitOrdersSnapshot, Error>) in
            guard let self else { completion(.failure(NativeMCPError.unavailable)); return }
            completion(result.flatMap { self.validateOrdersSnapshot($0, scope: scope, arguments: arguments) })
        }
    }

    private func validateOrdersSnapshot(_ snapshot: UpbitOrdersSnapshot, scope: String,
        arguments: [String: Any]) -> Result<UpbitOrdersSnapshot, Error> {
        guard snapshot.exchange_read_mode == "LIVE_READ" else { return .failure(NativeMCPError.mockData) }
        guard snapshot.order_scope == scope, snapshot.returned_count == snapshot.records.count,
              snapshot.returned_count <= snapshot.limit, snapshot.limit >= 1,
              snapshot.limit <= (scope == "OPEN" ? 100 : 1000),
              snapshot.observedTimestamp != nil else { return .failure(NativeMCPError.protocolError) }
        var orderIds = Set<String>()
        guard snapshot.records.allSatisfy({ order in
            guard UpbitReadValidation.token(order.uuid), orderIds.insert(order.uuid).inserted,
                  UpbitReadValidation.market(order.market), UpbitReadValidation.token(order.side),
                  UpbitReadValidation.token(order.ord_type), UpbitReadValidation.token(order.state),
                  order.createdTimestamp != nil,
                  order.price == nil || order.priceDecimal != nil,
                  order.volume == nil || order.volumeDecimal != nil,
                  order.remaining_volume == nil || order.remainingVolumeDecimal != nil,
                  order.executed_volume == nil || order.executedVolumeDecimal != nil,
                  order.paid_fee == nil || order.paidFeeDecimal != nil else { return false }
            let values = [order.priceDecimal, order.volumeDecimal, order.remainingVolumeDecimal,
                          order.executedVolumeDecimal, order.paidFeeDecimal].compactMap { $0 }
            guard values.allSatisfy({ $0 >= 0 }) else { return false }
            if let volume = order.volumeDecimal, let remaining = order.remainingVolumeDecimal,
               let executed = order.executedVolumeDecimal {
                var left = remaining, right = executed, total = Decimal.zero
                guard NSDecimalAdd(&total, &left, &right, .plain) == .noError,
                      !total.isNaN, total <= volume else { return false }
            }
            return true
        }) else { return .failure(NativeMCPError.protocolError) }
        if scope == "OPEN" {
            guard snapshot.page == arguments["page"] as? Int,
                  snapshot.next_page == nil || snapshot.next_page == (snapshot.page ?? 0) + 1 else {
                return .failure(NativeMCPError.protocolError)
            }
            lastOpenOrders = snapshot
        } else {
            guard snapshot.page == nil, snapshot.next_page == nil,
                  snapshot.start_time == nil || snapshot.startTimestamp != nil,
                  snapshot.end_time == nil || snapshot.endTimestamp != nil else {
                return .failure(NativeMCPError.protocolError)
            }
            if let start = snapshot.startTimestamp, let end = snapshot.endTimestamp,
               start > end { return .failure(NativeMCPError.protocolError) }
            lastClosedOrders = snapshot
        }
        return .success(snapshot)
    }

    @discardableResult
    private func readLive<T: Decodable>(_ tool: String, arguments: [String: Any],
        completion: @escaping (Result<T, Error>) -> Void) -> NativeMCPRequest {
        let composite = NativeMCPRequest()
        let finish: (Result<T, Error>) -> Void = { result in
            guard composite.complete() else { return }
            completion(result)
        }
        composite.setCancellationHandler {
            completion(.failure(NativeMCPError.cancelled))
        }
        let preflight = fetchControlStatusRequest { [weak self] status in
            guard let self else { finish(.failure(NativeMCPError.unavailable)); return }
            switch status {
            case .failure(let error): finish(.failure(error))
            case .success(let value):
                guard value.exchange_read_mode == "LIVE_READ" else { finish(.failure(NativeMCPError.mockData)); return }
                guard composite.isPending else { return }
                let read = self.client.call(tool, arguments: arguments) { result in
                    finish(result.flatMap { data in Result { try JSONDecoder().decode(T.self, from: data) } })
                }
                _ = composite.addChild(read)
            }
        }
        _ = composite.addChild(preflight)
        return composite
    }
}

struct UpbitAccountSnapshot: Decodable {
    let mode: String
    let observed_at_utc: String
    let accounts_count: Int
    let balances: [UpbitLiveAccount]

    var observedTimestamp: Date? { UpbitReadValidation.utcTimestamp(observed_at_utc) }
}
