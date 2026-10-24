import CryptoKit
import Foundation

enum AutomationSessionServiceError: LocalizedError {
    case invalidResponse(String)
    case identityMismatch(String)
    case invalidInput(String)

    var errorDescription: String? {
        switch self {
        case .invalidResponse(let field):
            return "전략 자동화 응답을 해석할 수 없습니다: \(field)"
        case .identityMismatch(let field):
            return "전략 자동화 응답의 identity가 요청과 다릅니다: \(field)"
        case .invalidInput(let field):
            return "전략 자동화 입력이 유효하지 않습니다: \(field)"
        }
    }
}

struct AutomationPolicyRevisionReference: Codable, Equatable {
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

    func validate() throws {
        guard !policy_id.isEmpty, !revision_id.isEmpty,
              definition_digest.count == 64,
              definition_digest.allSatisfy({ $0.isHexDigit }) else {
            throw AutomationSessionServiceError.invalidInput("동결 policy_id/revision_id/definition_digest")
        }
    }
}

/// Decimal amounts remain strings from the text field through the frozen request and read model.
struct AutomationRiskLimits: Codable, Equatable {
    let max_order_notional_krw: String
    let max_daily_notional_krw: String
    let max_asset_exposure_krw: String
    let max_daily_loss_krw: String
    let max_open_orders: UInt32
    let max_orders_per_interval: UInt32
    let max_balance_age_seconds: UInt32
    let max_feed_age_seconds: UInt32
    let activation_loss_limit_krw: String?

    init(max_order_notional_krw: String, max_daily_notional_krw: String,
         max_asset_exposure_krw: String, max_daily_loss_krw: String,
         max_open_orders: UInt32, max_orders_per_interval: UInt32,
         max_balance_age_seconds: UInt32, max_feed_age_seconds: UInt32,
         activation_loss_limit_krw: String? = nil) {
        self.max_order_notional_krw = max_order_notional_krw
        self.max_daily_notional_krw = max_daily_notional_krw
        self.max_asset_exposure_krw = max_asset_exposure_krw
        self.max_daily_loss_krw = max_daily_loss_krw
        self.max_open_orders = max_open_orders
        self.max_orders_per_interval = max_orders_per_interval
        self.max_balance_age_seconds = max_balance_age_seconds
        self.max_feed_age_seconds = max_feed_age_seconds
        self.activation_loss_limit_krw = activation_loss_limit_krw
    }

    var arguments: [String: Any] {
        var result: [String: Any] = [
            "max_order_notional_krw": max_order_notional_krw,
            "max_daily_notional_krw": max_daily_notional_krw,
            "max_asset_exposure_krw": max_asset_exposure_krw,
            "max_daily_loss_krw": max_daily_loss_krw,
            "max_open_orders": max_open_orders,
            "max_orders_per_interval": max_orders_per_interval,
            "max_balance_age_seconds": max_balance_age_seconds,
            "max_feed_age_seconds": max_feed_age_seconds
        ]
        if let activation_loss_limit_krw { result["activation_loss_limit_krw"] = activation_loss_limit_krw }
        return result
    }

    func validate() throws {
        let order = try Self.positiveDecimal(max_order_notional_krw, field: "max_order_notional_krw")
        let daily = try Self.positiveDecimal(max_daily_notional_krw, field: "max_daily_notional_krw")
        _ = try Self.positiveDecimal(max_asset_exposure_krw, field: "max_asset_exposure_krw")
        _ = try Self.positiveDecimal(max_daily_loss_krw, field: "max_daily_loss_krw")
        if let activation_loss_limit_krw {
            _ = try Self.positiveDecimal(activation_loss_limit_krw, field: "activation_loss_limit_krw")
        }
        guard order <= daily else {
            throw AutomationSessionServiceError.invalidInput("max_order_notional_krw는 max_daily_notional_krw 이하여야 합니다")
        }
        guard max_open_orders > 0, max_orders_per_interval > 0,
              max_balance_age_seconds > 0, max_feed_age_seconds > 0 else {
            throw AutomationSessionServiceError.invalidInput("주문 수와 관측 신선도 한도는 0보다 커야 합니다")
        }
    }

    func validate(for mode: AutomationMode) throws {
        try validate()
        if mode == .mock, activation_loss_limit_krw != nil {
            throw AutomationSessionServiceError.invalidInput("시작 이후 손실 한도는 LIVE 세션에서만 선택할 수 있습니다")
        }
    }

    private static func positiveDecimal(_ rawValue: String, field: String) throws -> Decimal {
        let bytes = Array(rawValue.utf8)
        let dotCount = bytes.filter { $0 == 46 }.count
        guard rawValue == rawValue.trimmingCharacters(in: .whitespacesAndNewlines),
              !bytes.isEmpty, dotCount <= 1, bytes.first != 46, bytes.last != 46,
              bytes.allSatisfy({ ($0 >= 48 && $0 <= 57) || $0 == 46 }),
              let value = Decimal(string: rawValue, locale: Locale(identifier: "en_US_POSIX")),
              value > 0 else {
            throw AutomationSessionServiceError.invalidInput("\(field)는 0보다 큰 정확한 십진수여야 합니다")
        }
        return value
    }
}

enum AutomationMode: String, Codable, Equatable {
    case mock = "MOCK"
    case live = "LIVE"
}

enum AutomationAccountIdentityScope {
    static func validate(_ identity: String, mode: AutomationMode) throws {
        switch mode {
        case .mock:
            guard identity == "mock:upbit-default" else {
                throw AutomationSessionServiceError.identityMismatch("MOCK account scope")
            }
        case .live:
            let prefix = "live:upbit:"
            guard identity.hasPrefix(prefix) else {
                throw AutomationSessionServiceError.identityMismatch("LIVE account scope")
            }
            let fingerprint = identity.dropFirst(prefix.count)
            guard fingerprint.count == 64,
                  fingerprint.utf8.allSatisfy({ ($0 >= 48 && $0 <= 57) || ($0 >= 97 && $0 <= 102) }) else {
                throw AutomationSessionServiceError.identityMismatch("LIVE account fingerprint")
            }
        }
    }

    static func presentation(for mode: AutomationMode) -> String {
        switch mode {
        case .mock: return "mock:upbit-default"
        case .live: return "LIVE Upbit account · fingerprint hidden"
        }
    }
}

enum AutomationSessionState: String, Codable, Equatable {
    case running = "RUNNING"
    case paused = "PAUSED"
    case stopped = "STOPPED"
    case needsReconciliation = "NEEDS_RECONCILIATION"
    case blockedApproval = "BLOCKED_APPROVAL"
    case faulted = "FAULTED"
}

enum AutomationCommandAction: String, Codable, Equatable {
    case start = "START"
    case pause = "PAUSE"
    case resume = "RESUME"
    case stop = "STOP"
    case cancelOrder = "CANCEL_ORDER"
    case recordBaseline = "RECORD_BASELINE"
}

enum AutomationCommandOutcome: String, Codable, Equatable {
    case applied = "APPLIED"
    case replayed = "REPLAYED"
    case noOp = "NO_OP"
    case pending = "PENDING"
    case denied = "DENIED"
}

enum AutomationRequestReadbackState: String, Codable, Equatable {
    case pending = "PENDING"
    case applied = "APPLIED"
    case rejected = "REJECTED"
    case notRecorded = "NOT_RECORDED"
}

enum AutomationEventKind: String, Codable, Equatable {
    case started = "STARTED"
    case paused = "PAUSED"
    case resumed = "RESUMED"
    case stopped = "STOPPED"
    case candleObserved = "CANDLE_OBSERVED"
    case signalObserved = "SIGNAL_OBSERVED"
    case intentRejected = "INTENT_REJECTED"
    case orderAdmitted = "ORDER_ADMITTED"
    case orderSendClaimed = "ORDER_SEND_CLAIMED"
    case orderAcknowledged = "ORDER_ACKNOWLEDGED"
    case orderCancelRequested = "ORDER_CANCEL_REQUESTED"
    case orderReconciled = "ORDER_RECONCILED"
    case fillRecorded = "FILL_RECORDED"
    case needsReconciliation = "NEEDS_RECONCILIATION"
    case recoveryBlocked = "RECOVERY_BLOCKED"
    case baselineRecorded = "BASELINE_RECORDED"
    case baselineBlocked = "BASELINE_BLOCKED"
}

enum AutomationBaselineSource: String, Codable, Equatable {
    case operatorAttested = "OPERATOR_ATTESTED"
    case exchangeObservedDayStart = "EXCHANGE_OBSERVED_DAY_START"
    case priorDayVerifiedLedgerRollover = "PRIOR_DAY_VERIFIED_LEDGER_ROLLOVER"
    case durableMockVenueSeed = "DURABLE_MOCK_VENUE_SEED"

    static func selectableSources(for mode: AutomationMode) -> [AutomationBaselineSource] {
        switch mode {
        case .mock:
            return [.durableMockVenueSeed, .operatorAttested]
        case .live:
            return [.exchangeObservedDayStart, .priorDayVerifiedLedgerRollover, .operatorAttested]
        }
    }

    static func selectableSource(at index: Int, for mode: AutomationMode) throws -> AutomationBaselineSource {
        let sources = selectableSources(for: mode)
        guard sources.indices.contains(index) else {
            throw AutomationSessionServiceError.invalidInput("기준선 출처를 선택하세요")
        }
        return sources[index]
    }
}

enum AutomationBaselineVerificationState: String, Codable, Equatable {
    case verified = "VERIFIED"
    case blockedInsufficientEvidence = "BLOCKED_INSUFFICIENT_EVIDENCE"
}

enum AutomationAssetValuationBasis: String, Codable, Equatable {
    case quoteCurrencyParity = "QUOTE_CURRENCY_PARITY"
    case finalizedCandleClose = "FINALIZED_CANDLE_CLOSE"
    case operatorAttestedPrice = "OPERATOR_ATTESTED_PRICE"
    case zeroBalance = "ZERO_BALANCE"
}

struct AutomationBaselineAssetInput: Codable, Equatable {
    let currency: String
    let free: String
    let locked: String
    let valuation_basis: AutomationAssetValuationBasis
    let mark_price_krw: String?

    var arguments: [String: Any] {
        var dict: [String: Any] = [
            "currency": currency,
            "free": free,
            "locked": locked,
            "valuation_basis": valuation_basis.rawValue
        ]
        if let mark_price_krw {
            dict["mark_price_krw"] = mark_price_krw
        }
        return dict
    }

    func validate() throws {
        guard !currency.isEmpty, currency.count <= 16,
              currency.utf8.allSatisfy({ ($0 >= 65 && $0 <= 90) || ($0 >= 48 && $0 <= 57) }) else {
            throw AutomationSessionServiceError.invalidInput("자산 통화 코드(\(currency))는 대문자 영문/숫자여야 합니다")
        }
        let freeDec = try Self.nonNegativeDecimal(free, field: "\(currency) free")
        let lockedDec = try Self.nonNegativeDecimal(locked, field: "\(currency) locked")
        let total = freeDec + lockedDec
        if currency == "KRW" {
            guard valuation_basis == .quoteCurrencyParity, mark_price_krw == nil else {
                throw AutomationSessionServiceError.invalidInput("KRW는 QUOTE_CURRENCY_PARITY 기준이어야 하며 별도 가격을 입력하지 않습니다")
            }
        } else if total == 0 {
            if valuation_basis == .operatorAttestedPrice, mark_price_krw == nil {
                throw AutomationSessionServiceError.invalidInput("0 잔액 자산(\(currency))에 OPERATOR_ATTESTED_PRICE를 선택한 경우 mark_price_krw가 필요합니다")
            }
        } else {
            switch valuation_basis {
            case .zeroBalance, .quoteCurrencyParity:
                throw AutomationSessionServiceError.invalidInput("보유 수량이 있는 자산(\(currency))은 FINALIZED_CANDLE_CLOSE 또는 OPERATOR_ATTESTED_PRICE 기준을 사용해야 합니다")
            case .operatorAttestedPrice:
                guard let mark_price_krw, (try? Self.positiveDecimal(mark_price_krw, field: "\(currency) mark_price_krw")) != nil else {
                    throw AutomationSessionServiceError.invalidInput("OPERATOR_ATTESTED_PRICE 자산(\(currency))은 0보다 큰 mark_price_krw가 필요합니다")
                }
            case .finalizedCandleClose:
                if let mark_price_krw {
                    _ = try Self.positiveDecimal(mark_price_krw, field: "\(currency) mark_price_krw")
                }
            }
        }
    }

    private static func nonNegativeDecimal(_ rawValue: String, field: String) throws -> Decimal {
        let bytes = Array(rawValue.utf8)
        let dotCount = bytes.filter { $0 == 46 }.count
        guard rawValue == rawValue.trimmingCharacters(in: .whitespacesAndNewlines),
              !bytes.isEmpty, dotCount <= 1, bytes.first != 46, bytes.last != 46,
              bytes.allSatisfy({ ($0 >= 48 && $0 <= 57) || $0 == 46 }),
              let value = Decimal(string: rawValue, locale: Locale(identifier: "en_US_POSIX")),
              value >= 0 else {
            throw AutomationSessionServiceError.invalidInput("\(field)는 0 이상의 정확한 십진수여야 합니다")
        }
        return value
    }

    private static func positiveDecimal(_ rawValue: String, field: String) throws -> Decimal {
        let value = try nonNegativeDecimal(rawValue, field: field)
        guard value > 0 else {
            throw AutomationSessionServiceError.invalidInput("\(field)는 0보다 큰 정확한 십진수여야 합니다")
        }
        return value
    }
}

struct AutomationAccountDayBaselineInput: Codable, Equatable {
    let account_identity: String
    let day_utc: String
    let reference_time_utc: String
    let source: AutomationBaselineSource
    let assets: [AutomationBaselineAssetInput]
    let operator_note: String?

    var arguments: [String: Any] {
        var dict: [String: Any] = [
            "account_identity": account_identity,
            "day_utc": day_utc,
            "reference_time_utc": reference_time_utc,
            "source": source.rawValue,
            "assets": assets.map(\.arguments)
        ]
        if let operator_note {
            dict["operator_note"] = operator_note
        }
        return dict
    }

    func validate(for mode: AutomationMode) throws {
        try AutomationAccountIdentityScope.validate(account_identity, mode: mode)
        guard day_utc.count == 10,
              reference_time_utc == "\(day_utc)T00:00:00Z" else {
            throw AutomationSessionServiceError.invalidInput("기준일(day_utc)은 YYYY-MM-DD 형식이고 기준 시각(reference_time_utc)은 YYYY-MM-DDT00:00:00Z여야 합니다")
        }
        if mode == .live && source == .durableMockVenueSeed {
            throw AutomationSessionServiceError.invalidInput("실계좌(LIVE) 기준선에는 DURABLE_MOCK_VENUE_SEED 출처를 사용할 수 없습니다")
        }
        guard !assets.isEmpty else {
            throw AutomationSessionServiceError.invalidInput("기준선 자산 목록이 비어 있습니다")
        }
        var seen = Set<String>()
        var hasKRW = false
        for asset in assets {
            try asset.validate()
            guard seen.insert(asset.currency).inserted else {
                throw AutomationSessionServiceError.invalidInput("중복된 통화(\(asset.currency))가 있습니다")
            }
            if asset.currency == "KRW" { hasKRW = true }
        }
        guard hasKRW else {
            throw AutomationSessionServiceError.invalidInput("기준선 자산에는 KRW가 반드시 포함되어야 합니다")
        }
        if let operator_note, operator_note.utf8.count > 512 {
            throw AutomationSessionServiceError.invalidInput("메모(operator_note)는 512바이트 이하여야 합니다")
        }
    }
}

struct AutomationBaselineAssetEntry: Codable, Equatable {
    let currency: String
    let free: String
    let locked: String
    let total: String
    let valuation_basis: AutomationAssetValuationBasis
    let mark_price_krw: String?
    let valuation_krw: String?
    let candle_observation_id: String?
    let candle_content_digest: String?
}

struct AutomationBaselineExchangeEvidence: Codable, Equatable {
    let snapshot_sequence: UInt64?
    let snapshot_observed_at_utc: String?
    let snapshot_received_at_utc: String?
    let snapshot_balances_digest: String?
    let prior_day_baseline_digest: String?
}

struct AutomationAccountDayBaselineView: Codable, Equatable {
    let baseline_id: String
    let account_identity: String
    let mode: AutomationMode
    let day_utc: String
    let reference_time_utc: String
    let recorded_at_utc: String
    let source: AutomationBaselineSource
    let verification_state: AutomationBaselineVerificationState
    let sufficient_for_risk_admission: Bool
    let total_equity_krw: String?
    let assets: [AutomationBaselineAssetEntry]
    let exchange_evidence: AutomationBaselineExchangeEvidence?
    let block_reasons: [String]
    let baseline_digest: String
    let request_id: String?
    let operator_note: String?
}

/// Legacy local MOCK scalar only; it does not establish exchange or asset-level day-start evidence.
struct AutomationLegacyMockBaselineView: Codable, Equatable {
    static let scope = "LEGACY_LOCAL_MOCK_LEDGER"

    let account_identity: String
    let day_utc: String
    let baseline_equity_krw: String
    let balances_digest: String
    let recorded_at_utc: String
}

struct AutomationAccountDayBaselineStatus: Codable, Equatable {
    let session_id: String
    let account_identity: String
    let mode: AutomationMode
    let day_utc: String
    let admission_blocked: Bool
    let block_reasons: [String]
    let baseline: AutomationAccountDayBaselineView?
    let legacy_mock_baseline: AutomationLegacyMockBaselineView?

    init(
        session_id: String,
        account_identity: String,
        mode: AutomationMode,
        day_utc: String,
        admission_blocked: Bool,
        block_reasons: [String],
        baseline: AutomationAccountDayBaselineView?,
        legacy_mock_baseline: AutomationLegacyMockBaselineView? = nil
    ) {
        self.session_id = session_id
        self.account_identity = account_identity
        self.mode = mode
        self.day_utc = day_utc
        self.admission_blocked = admission_blocked
        self.block_reasons = block_reasons
        self.baseline = baseline
        self.legacy_mock_baseline = legacy_mock_baseline
    }
}

enum AutomationOrderState: String, Codable, Equatable {
    case reserved = "RESERVED"
    case submittedUnknown = "SUBMITTED_UNKNOWN"
    case open = "OPEN"
    case partiallyFilled = "PARTIALLY_FILLED"
    case filled = "FILLED"
    case cancelled = "CANCELLED"
    case rejected = "REJECTED"
    case needsReconciliation = "NEEDS_RECONCILIATION"
}

enum AutomationDataSource: String, Codable, Equatable {
    case upbitLiveRead = "UPBIT_LIVE_READ"
    case upbitPublicFeed = "UPBIT_PUBLIC_FEED"
    case durableMockVenue = "DURABLE_MOCK_VENUE"
}

enum AutomationFreshness: String, Codable, Equatable {
    case fresh = "FRESH"
    case delayed = "DELAYED"
    case stale = "STALE"
    case unavailable = "UNAVAILABLE"
}

struct AutomationAccountBalance: Codable, Equatable {
    let source: AutomationDataSource
    let observed_at_utc: String?
    let received_at_utc: String?
    let freshness: AutomationFreshness
    let quote_currency: String
    let quote_total: String?
    let quote_free: String?
    let quote_exchange_locked: String?
    let quote_local_reserved: String?
    let base_currency: String
    let base_total: String?
    let base_free: String?
    let base_exchange_locked: String?
    let base_local_reserved: String?
}

struct AutomationRiskUsage: Codable, Equatable {
    let daily_notional_used_krw: String?
    let daily_notional_limit_krw: String
    let daily_loss_used_krw: String?
    let daily_loss_limit_krw: String
    let asset_exposure_krw: String?
    let asset_exposure_limit_krw: String
    let open_order_count: UInt32
    let open_order_limit: UInt32
    let orders_in_interval: UInt32?
    let orders_per_interval_limit: UInt32
    let emergency_latched: Bool
    let live_grant_active: Bool
}

struct AutomationFeedStatus: Codable, Equatable {
    let source: AutomationDataSource
    let observation_close_at_utc: String?
    let received_at_utc: String?
    let freshness: AutomationFreshness
}

struct AutomationCapabilities: Codable, Equatable {
    let can_pause: Bool
    let can_resume: Bool
    let can_stop: Bool
    let can_cancel_order: Bool
    let can_submit_orders: Bool
    let live_effects_enabled: Bool
    let explicit_approval_required: Bool
}

struct AutomationMockAssumptions: Codable, Equatable {
    let initial_quote_balance_krw: String
    let initial_base_balance: String
    let fee_rate: String
    let fill_model: String
}

enum AutomationLossReferenceBasis: String, Codable, Equatable {
    case utcDayBaseline = "UTC_DAY_BASELINE"
    case liveActivationObserved = "LIVE_ACTIVATION_OBSERVED"
    case unavailable = "UNAVAILABLE"
}

struct AutomationEffectiveLossProjection: Codable, Equatable {
    let basis: AutomationLossReferenceBasis
    let limit_krw: String?
    let used_krw: String?
    let admission_blocked: Bool
    let block_reasons: [String]
}

enum AutomationActivationReferenceSource: String, Codable, Equatable {
    case currentLocalObserved = "CURRENT_LOCAL_OBSERVED"
}

enum AutomationActivationValuationState: String, Codable, Equatable {
    case pendingMarks = "PENDING_MARKS"
    case verified = "VERIFIED"
}

struct AutomationLiveActivationReferenceView: Codable, Equatable {
    let account_identity: String
    let mode: AutomationMode
    let source: AutomationActivationReferenceSource
    let reference_observed_at_utc: String
    let reference_received_at_utc: String
    let valuation_recorded_at_utc: String
    let snapshot_sequence: UInt64
    let control_sequence: UInt64
    let reference_max_feed_age_seconds: UInt32
    let balances_digest: String
    let valuation_state: AutomationActivationValuationState
    let valuation_digest: String
    let reference_equity_krw: String?
}

struct AutomationAccountRiskProjection: Codable, Equatable {
    let balance: AutomationAccountBalance
    let risk: AutomationRiskUsage
    let feed: AutomationFeedStatus
    let capabilities: AutomationCapabilities
    let mock_assumptions: AutomationMockAssumptions?
    let day_baseline: AutomationAccountDayBaselineView?
    let legacy_mock_baseline: AutomationLegacyMockBaselineView?
    let admission_block_reasons: [String]
    let effective_loss: AutomationEffectiveLossProjection?
    let activation_reference: AutomationLiveActivationReferenceView?

    enum CodingKeys: String, CodingKey {
        case balance, risk, feed, capabilities, mock_assumptions, day_baseline, legacy_mock_baseline, admission_block_reasons
        case effective_loss, activation_reference
    }

    init(
        balance: AutomationAccountBalance,
        risk: AutomationRiskUsage,
        feed: AutomationFeedStatus,
        capabilities: AutomationCapabilities,
        mock_assumptions: AutomationMockAssumptions?,
        day_baseline: AutomationAccountDayBaselineView? = nil,
        legacy_mock_baseline: AutomationLegacyMockBaselineView? = nil,
        admission_block_reasons: [String] = [],
        effective_loss: AutomationEffectiveLossProjection? = nil,
        activation_reference: AutomationLiveActivationReferenceView? = nil
    ) {
        self.balance = balance
        self.risk = risk
        self.feed = feed
        self.capabilities = capabilities
        self.mock_assumptions = mock_assumptions
        self.day_baseline = day_baseline
        self.legacy_mock_baseline = legacy_mock_baseline
        self.admission_block_reasons = admission_block_reasons
        self.effective_loss = effective_loss
        self.activation_reference = activation_reference
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.balance = try container.decode(AutomationAccountBalance.self, forKey: .balance)
        self.risk = try container.decode(AutomationRiskUsage.self, forKey: .risk)
        self.feed = try container.decode(AutomationFeedStatus.self, forKey: .feed)
        self.capabilities = try container.decode(AutomationCapabilities.self, forKey: .capabilities)
        self.mock_assumptions = try container.decodeIfPresent(AutomationMockAssumptions.self, forKey: .mock_assumptions)
        self.day_baseline = try container.decodeIfPresent(AutomationAccountDayBaselineView.self, forKey: .day_baseline)
        self.legacy_mock_baseline = try container.decodeIfPresent(AutomationLegacyMockBaselineView.self, forKey: .legacy_mock_baseline)
        self.admission_block_reasons = try container.decodeIfPresent([String].self, forKey: .admission_block_reasons) ?? []
        self.effective_loss = try container.decodeIfPresent(AutomationEffectiveLossProjection.self, forKey: .effective_loss)
        self.activation_reference = try container.decodeIfPresent(AutomationLiveActivationReferenceView.self, forKey: .activation_reference)
    }
}

struct AutomationSessionView: Codable, Equatable {
    let session_id: String
    let account_identity: String
    let mode: AutomationMode
    let state: AutomationSessionState
    let sequence: UInt64
    let policy_ref: AutomationPolicyRevisionReference
    let market: String
    let decision_interval: String
    let risk: AutomationRiskLimits
    let created_at_utc: String
    let updated_at_utc: String
    let last_finalized_candle_at: String?
    let last_balance_observed_at: String?
    let unresolved_order_count: UInt32
    let fill_count: UInt64
    let recovery_reason: String?
    let account_risk: AutomationAccountRiskProjection?
}

struct AutomationSessionReceipt: Codable, Equatable {
    let request_id: String
    let action: AutomationCommandAction
    let outcome: AutomationCommandOutcome
    let sequence: UInt64
    let observed_at_utc: String
    let reason: String?
}

struct AutomationSessionCommandResponse: Codable, Equatable {
    let session: AutomationSessionView
    let receipt: AutomationSessionReceipt
    let order: AutomationOrderView?
    let baseline: AutomationAccountDayBaselineView?

    init(
        session: AutomationSessionView,
        receipt: AutomationSessionReceipt,
        order: AutomationOrderView?,
        baseline: AutomationAccountDayBaselineView? = nil
    ) {
        self.session = session
        self.receipt = receipt
        self.order = order
        self.baseline = baseline
    }
}

struct AutomationSessionRequestReadback: Codable, Equatable {
    let request_id: String
    let state: AutomationRequestReadbackState
    let response: AutomationSessionCommandResponse?
    let reason: String?
}

struct AutomationSessionPage: Codable, Equatable {
    let items: [AutomationSessionView]
    let returned_count: UInt64
    let total_count: UInt64
    let next_offset: UInt32?
}

indirect enum AutomationJSONValue: Codable, Equatable {
    case object([String: AutomationJSONValue])
    case array([AutomationJSONValue])
    case string(String)
    case number(Decimal)
    case boolean(Bool)
    case null

    init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        if container.decodeNil() {
            self = .null
        } else if let value = try? container.decode(Bool.self) {
            self = .boolean(value)
        } else if let value = try? container.decode(String.self) {
            self = .string(value)
        } else if let value = try? container.decode(Decimal.self) {
            self = .number(value)
        } else if let value = try? container.decode([String: AutomationJSONValue].self) {
            self = .object(value)
        } else if let value = try? container.decode([AutomationJSONValue].self) {
            self = .array(value)
        } else {
            throw AutomationSessionServiceError.invalidResponse("event detail JSON value")
        }
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        switch self {
        case .object(let value): try container.encode(value)
        case .array(let value): try container.encode(value)
        case .string(let value): try container.encode(value)
        case .number(let value): try container.encode(value)
        case .boolean(let value): try container.encode(value)
        case .null: try container.encodeNil()
        }
    }

    var formattedJSON: String {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        guard let data = try? encoder.encode(self), let value = String(data: data, encoding: .utf8) else {
            return "미해석 JSON"
        }
        return value
    }

    var redactedForDisplay: AutomationJSONValue {
        redactedForDisplay(hidingMonetaryValues: false)
    }

    func redactedForDisplay(hidingMonetaryValues: Bool) -> AutomationJSONValue {
        switch self {
        case .object(let fields):
            var redacted: [String: AutomationJSONValue] = [:]
            for (key, value) in fields {
                let normalized = key.lowercased().filter { $0.isLetter || $0.isNumber }
                let sensitive = normalized.contains("accountidentity") || normalized.contains("accesskey")
                    || normalized.contains("secret") || normalized.contains("credential")
                    || normalized.contains("authorization") || normalized.hasSuffix("token")
                if sensitive {
                    redacted[key] = .string("[hidden]")
                } else if hidingMonetaryValues && Self.isMonetaryDisplayKey(normalized) {
                    redacted[key] = .string("[금액 숨김]")
                } else {
                    redacted[key] = value.redactedForDisplay(hidingMonetaryValues: hidingMonetaryValues)
                }
            }
            return .object(redacted)
        case .array(let values):
            return .array(values.map { $0.redactedForDisplay(hidingMonetaryValues: hidingMonetaryValues) })
        case .string, .number, .boolean, .null:
            return self
        }
    }

    private static func isMonetaryDisplayKey(_ normalized: String) -> Bool {
        if ["digest", "hash", "uuid", "id"].contains(where: normalized.hasSuffix) { return false }
        return [
            "amount", "notional", "price", "volume", "quantity", "qty", "fee", "commission",
            "balance", "cash", "funds", "equity", "pnl", "profit", "loss", "exposure",
            "cost", "capital", "budget", "proceeds", "locked", "reserved", "valuation",
            "turnover", "krw", "available", "total", "free"
        ].contains(where: normalized.contains)
    }
}

enum RecordedDecisionPresentation {
    private static let maximumRowsPerSection = 32
    private static let maximumEvidenceCharacters = 640

    static func lines(for value: AutomationJSONValue?, masked: Bool) -> [String] {
        guard !masked else { return ["개인정보 숨김 · 저장된 판단 근거를 표시하지 않습니다."] }
        guard let value else { return ["저장된 구조화 판단 근거 없음 · 당시 기록을 재실행하거나 재구성하지 않았습니다."] }
        guard let fields = object(value.redactedForDisplay) else {
            return ["저장된 판단 근거 형식을 해석할 수 없습니다."]
        }

        var result = [
            "엔진 결정 근거 · 스키마 \(string(fields["schema_version"]) ?? "미기록")",
            "결정 시각: \(display(fields["decision_time"]))",
            "전략 행동: \(action(fields["strategy_action"]))",
            "제약 적용 후 행동: \(action(fields["constrained_action"]))",
            "실행 상태: \(execution(fields["execution_state"]))",
            "결정 시점 실제 비중: \(display(fields["actual_weight_at_decision"]))",
            "원시 목표 비중: \(display(fields["raw_target_weight"]))",
            "제약 후 목표 비중: \(display(fields["constrained_target_weight"]))",
            "일치 규칙 ID: \(display(fields["matched_rule_id"]))",
            "조건 기록 상태: \(capture(fields["condition_trace_capture"]))"
        ]

        if let policyReference = fields["policy_ref"], policyReference != .null {
            result.append("정책 리비전 참조: \(compact(policyReference))")
        } else {
            result.append("정책 리비전 참조: 미기록")
        }

        appendReasons(fields["reasons"], to: &result)
        appendConditions(fields["condition_trace"], to: &result)
        appendOrders(fields["orders"], to: &result)
        appendFills(fields["fills"], to: &result)
        return result
    }

    static func preparationLines(for value: AutomationJSONValue?, masked: Bool) -> [String] {
        guard let value, value != .null else { return [] }
        guard !masked else { return ["개인정보 숨김 · 저장 준비 근거를 표시하지 않습니다."] }
        guard let fields = object(value.redactedForDisplay) else {
            return ["저장 준비 근거 형식을 해석할 수 없습니다."]
        }
        return [
            "엔진 저장 준비 상태: \(display(fields["status"])) · 처리 봉 수: \(display(fields["completed_bars_seen"])) · 필요 봉 수: \(display(fields["required_bars"]))",
            "준비 판단 시각: \(display(fields["decision_time"])) · 입력 관측 ID: \(display(fields["observation_id"]))"
        ]
    }

    private static func appendReasons(_ value: AutomationJSONValue?, to lines: inout [String]) {
        guard let values = array(value) else {
            lines.append("저장 사유: 미기록")
            return
        }
        if values.isEmpty {
            lines.append("저장된 사유 항목: 0건")
            return
        }
        lines.append("저장된 사유 항목: \(values.count)건")
        for reason in values.prefix(maximumRowsPerSection) {
            guard let fields = object(reason) else {
                lines.append("사유 원본: \(compact(reason))")
                continue
            }
            let stage = enumCode(fields["stage"]) ?? "미기록"
            let code = string(fields["code"]) ?? "미기록"
            let label = string(fields["label_ko"]) ?? "미기록"
            lines.append("사유 · \(stageLabel(stage)) · \(label) [\(code)]")
        }
        appendRemainder(values.count, to: &lines)
    }

    private static func appendConditions(_ value: AutomationJSONValue?, to lines: inout [String]) {
        guard let values = array(value) else {
            lines.append("조건 비교 기록: 미기록")
            return
        }
        lines.append("저장 조건 평가: \(values.count)개")
        for (index, condition) in values.prefix(maximumRowsPerSection).enumerated() {
            guard let fields = object(condition) else {
                lines.append("조건 \(index + 1): \(compact(condition))")
                continue
            }
            let path = string(fields["ast_path"]) ?? "경로 미기록"
            let role = enumCode(fields["role"]) ?? "미기록"
            let outcome = outcomeLabel(fields["outcome"])
            let engineResult = engineResultLabel(fields["engine_result"], outcome: fields["outcome"])
            lines.append("조건 \(index + 1) · \(path) · 역할 \(role) · \(outcome) · 엔진 반환 \(engineResult)")
            if let evidence = fields["evidence"], evidence != .null {
                lines.append("  실제 비교 입력: \(compact(evidence))")
            } else {
                lines.append(enumCode(fields["outcome"]) == "NOT_EVALUATED"
                    ? "  실제 비교 입력: 미평가" : "  실제 비교 입력: 미기록")
            }
        }
        appendRemainder(values.count, to: &lines)
    }

    private static func appendOrders(_ value: AutomationJSONValue?, to lines: inout [String]) {
        guard let values = array(value) else {
            lines.append("연결된 주문: 미기록")
            return
        }
        lines.append("연결된 주문: \(values.count)건")
        for order in values.prefix(maximumRowsPerSection) {
            guard let fields = object(order) else {
                lines.append("주문 원본: \(compact(order))")
                continue
            }
            let identifier = display(fields["order_id"])
            let state = display(fields["state"])
            let reason = display(fields["reason"])
            lines.append("주문 · \(identifier) · \(state) · 사유 \(reason)")
        }
        appendRemainder(values.count, to: &lines)
    }

    private static func appendFills(_ value: AutomationJSONValue?, to lines: inout [String]) {
        guard let values = array(value) else {
            lines.append("연결된 체결: 미기록")
            return
        }
        lines.append("연결된 체결: \(values.count)건")
        for fill in values.prefix(maximumRowsPerSection) {
            guard let fields = object(fill) else {
                lines.append("체결 원본: \(compact(fill))")
                continue
            }
            lines.append("체결 · \(display(fields["fill_id"])) · 주문 \(display(fields["order_id"]))")
        }
        appendRemainder(values.count, to: &lines)
    }

    private static func appendRemainder(_ count: Int, to lines: inout [String]) {
        guard count > maximumRowsPerSection else { return }
        lines.append("나머지 \(count - maximumRowsPerSection)개 항목은 표시를 생략했습니다.")
    }

    private static func action(_ value: AutomationJSONValue?) -> String {
        let code = enumCode(value) ?? "미기록"
        let label: String
        switch code {
        case "BUY": label = "매수"
        case "SELL": label = "매도"
        case "HOLD": label = "보유"
        case "NOT_EVALUATED": label = "평가 안 함"
        case "NOT_CAPTURED": label = "미기록"
        default: label = "확인 필요"
        }
        return "\(label) (\(code))"
    }

    private static func execution(_ value: AutomationJSONValue?) -> String {
        let code = enumCode(value) ?? "미기록"
        let label: String
        switch code {
        case "NOT_EVALUATED": label = "평가 안 함"
        case "NO_ORDER": label = "주문 없음"
        case "REJECTED": label = "거부"
        case "PENDING": label = "대기"
        case "FILLED": label = "체결"
        case "PARTIALLY_FILLED": label = "일부 체결"
        case "CANCELLED": label = "취소"
        case "EXPIRED": label = "만료"
        case "NOT_CAPTURED": label = "미기록"
        default: label = "확인 필요"
        }
        return "\(label) (\(code))"
    }

    private static func capture(_ value: AutomationJSONValue?) -> String {
        let code = enumCode(value) ?? "미기록"
        let label: String
        switch code {
        case "CAPTURED": label = "평가 결과 기록"
        case "PARTIAL_CAPTURE": label = "일부 기록"
        case "NOT_CAPTURED": label = "미기록"
        case "NOT_DECISION_EVENT": label = "판단 이벤트 아님"
        default: label = "확인 필요"
        }
        return "\(label) (\(code))"
    }

    private static func outcomeLabel(_ value: AutomationJSONValue?) -> String {
        let code = enumCode(value) ?? "미기록"
        let reason = enumCode(object(value)?["value"])
        switch code {
        case "TRUE": return "충족 (TRUE)"
        case "FALSE": return "불충족 (FALSE)"
        case "UNKNOWN": return "확인 불가 (UNKNOWN)" + (reason.map { " · \($0)" } ?? "")
        case "NOT_EVALUATED": return "미평가 (NOT_EVALUATED)" + (reason.map { " · \($0)" } ?? "")
        default: return "\(code) · 원본 \(value.map(compact) ?? "미기록")"
        }
    }

    private static func engineResultLabel(_ value: AutomationJSONValue?, outcome: AutomationJSONValue?) -> String {
        if let value {
            switch value {
            case .boolean(let result): return result ? "true" : "false"
            case .null: break
            default: return compact(value)
            }
        }
        return enumCode(outcome) == "NOT_EVALUATED" ? "미평가" : "미기록"
    }

    private static func stageLabel(_ stage: String) -> String {
        switch stage {
        case "DATA_READINESS": return "데이터 준비"
        case "STRATEGY": return "전략"
        case "CONSTRAINTS": return "제약"
        case "ADMISSION": return "주문 허용"
        case "EXECUTION": return "실행"
        case "LIFECYCLE": return "상태 전이"
        default: return stage
        }
    }

    private static func object(_ value: AutomationJSONValue?) -> [String: AutomationJSONValue]? {
        guard let value else { return nil }
        if case .object(let fields) = value { return fields }
        return nil
    }

    private static func array(_ value: AutomationJSONValue?) -> [AutomationJSONValue]? {
        guard let value else { return nil }
        if case .array(let values) = value { return values }
        return nil
    }

    private static func string(_ value: AutomationJSONValue?) -> String? {
        guard let value else { return nil }
        if case .string(let text) = value { return text }
        return nil
    }

    private static func enumCode(_ value: AutomationJSONValue?) -> String? {
        guard let value else { return nil }
        if case .string(let text) = value { return text }
        guard let fields = object(value) else { return nil }
        if let kind = string(fields["kind"]) { return kind }
        guard fields.count == 1, let key = fields.keys.first else { return nil }
        return key
    }

    private static func display(_ value: AutomationJSONValue?) -> String {
        guard let value else { return "미기록" }
        switch value {
        case .null: return "미기록"
        case .string(let text): return text.isEmpty ? "미기록" : text
        case .number(let number): return NSDecimalNumber(decimal: number).stringValue
        case .boolean(let flag): return flag ? "true" : "false"
        case .object, .array: return compact(value)
        }
    }

    private static func compact(_ value: AutomationJSONValue) -> String {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        guard let data = try? encoder.encode(value), let text = String(data: data, encoding: .utf8) else {
            return "미해석 JSON"
        }
        let flattened = text.replacingOccurrences(of: "\n", with: " ")
        guard flattened.count > maximumEvidenceCharacters else { return flattened }
        return String(flattened.prefix(maximumEvidenceCharacters)) + "…"
    }
}

struct AutomationEvent: Codable, Equatable {
    let session_id: String
    let sequence: UInt64
    let kind: AutomationEventKind
    let at_utc: String
    let detail: AutomationJSONValue
}

struct AutomationEventPage: Codable, Equatable {
    let items: [AutomationEvent]
    let next_sequence: UInt64?
}

enum AutomationSide: String, Codable, Equatable {
    case bid = "BID"
    case ask = "ASK"
}

struct AutomationOrderView: Codable, Equatable {
    let client_identifier: String
    let intent_id: String
    let market: String
    let side: AutomationSide
    let state: AutomationOrderState
    let venue_order_uuid: String?
    let requested_notional_krw: String
    let reserved_fee_krw: String
    let executed_volume: String
    let paid_fee_krw: String
    let locked_quote_krw: String?
    let locked_base: String?
    let trade_count: UInt64
    let created_at_utc: String
    let updated_at_utc: String
}

struct AutomationFillView: Codable, Equatable {
    let trade_uuid: String
    let client_identifier: String
    let market: String
    let side: AutomationSide
    let price_krw: String
    let volume: String
    let funds_krw: String
    let observed_at_utc: String
}

struct AutomationLedgerPage: Codable, Equatable {
    let orders: [AutomationOrderView]
    let fills: [AutomationFillView]
    let next_sequence: UInt64?
}

enum AutomationReviewAction: String, Codable, Equatable {
    case buy = "BUY"
    case sell = "SELL"
    case hold = "HOLD"
}

struct AutomationReviewCandleRecord: Codable, Equatable {
    let market: String
    let interval: String
    let open_time_utc: String
    let close_time_utc: String
    let open: String
    let high: String
    let low: String
    let close: String
    let volume: String
    let quote_turnover: String
    let completed: Bool
}

struct AutomationReviewCandleObservation: Codable, Equatable {
    let id: String
    let candle: AutomationReviewCandleRecord
    let content_digest: String
    let raw_object_ids: [String]
    let constituent_ids: [String]
}

struct AutomationReviewSession: Codable, Equatable {
    let session_id: String
    let mode: AutomationMode
    let state: AutomationSessionState
    let policy_ref: AutomationPolicyRevisionReference
    let market: String
    let decision_interval: String
    let risk: AutomationRiskLimits
    let created_at_utc: String
    let updated_at_utc: String
}

struct AutomationReviewPolicyDefinition: Codable, Equatable {
    let schema_version: String
    let name: String
    let description: String
    let program: AutomationJSONValue
}

struct AutomationReviewFrozenPolicy: Codable, Equatable {
    let reference: AutomationPolicyRevisionReference
    let revision_number: UInt32
    let parent_revision_id: String?
    let family: String
    let origin: AutomationJSONValue
    let definition: AutomationReviewPolicyDefinition
}

struct AutomationReviewDecisionSummary: Codable, Equatable {
    let intent_id: String
    let observation_id: String
    let decision_time_utc: String
    let action: AutomationReviewAction
    let state: String
}

struct AutomationReviewDecisionPage: Codable, Equatable {
    let session_id: String
    let mode: AutomationMode
    let policy_ref: AutomationPolicyRevisionReference
    let items: [AutomationReviewDecisionSummary]
    let returned_count: UInt64
    let total_count: UInt64
    let next_offset: UInt32?
}

struct AutomationReviewDecisionRecord: Codable, Equatable {
    let session_id: String
    let intent_id: String
    let observation_id: String
    let decision_time_utc: String
    let recorded_at_utc: String
    let action: AutomationReviewAction
    let state: String
    let candle: AutomationReviewCandleObservation
    let evaluation: AutomationJSONValue
    let evaluation_digest: String
}

/// Order metadata wrapper used by the full snapshot export; decision detail uses raw orders.
struct AutomationReviewSnapshotOrder: Codable, Equatable {
    let order: AutomationOrderView
    let decision_mark_krw: String
    let venue_created_at_utc: String?
}

struct AutomationReviewFill: Codable, Equatable {
    let fill: AutomationFillView
    let stored_trade_time_utc: String
    let recorded_at_utc: String
    let source_digest: String
}

struct AutomationReviewDecisionDetail: Codable, Equatable {
    let session: AutomationReviewSession
    let frozen_policy: AutomationReviewFrozenPolicy
    let decision: AutomationReviewDecisionRecord
    let context_candles: [AutomationReviewCandleObservation]
    let orders: [AutomationOrderView]
    let events: [AutomationEvent]
    let recorded_decision: AutomationJSONValue?
}

struct AutomationReviewMetric: Codable, Equatable {
    let value: String?
    let unit: String
    let unavailable_reason: String?
}

struct AutomationReviewOpeningInventory: Codable, Equatable {
    let snapshot_sequence: UInt64
    let source_mode: AutomationMode
    let observed_at_utc: String
    let base_quantity: String
    let balances_digest: String
}

struct AutomationPerformanceReport: Codable, Equatable {
    let schema_version: String
    let session: AutomationReviewSession
    let snapshot_digest: String
    let snapshot_at_utc: String
    let decision_count: UInt64
    let warmup_record_count: UInt64
    let buy_signal_count: UInt64
    let sell_signal_count: UInt64
    let hold_signal_count: UInt64
    let rejected_intent_count: UInt64
    let order_count: UInt64
    let fill_count: UInt64
    let buy_funds_krw: String
    let sell_funds_krw: String
    let paid_fees_krw: String
    let net_quote_cashflow_krw: String
    let realized_pnl_krw: AutomationReviewMetric
    let unrealized_pnl_krw: AutomationReviewMetric
    let total_pnl_krw: AutomationReviewMetric
    let return_ratio: AutomationReviewMetric
    let max_drawdown_ratio: AutomationReviewMetric
    let remaining_volume: String?
    let remaining_cost_krw: String?
    let mark_price_krw: String?
    let mark_at_utc: String?
    let mark_observation_id: String?
    let mark_content_digest: String?
    let opening_inventory: AutomationReviewOpeningInventory?
    let methodology: String
    let warnings: [String]
}

enum AutomationReviewFile: String, Codable, CaseIterable, Equatable, Hashable {
    case decisionsCSV = "decisions_csv"
    case ordersCSV = "orders_csv"
    case fillsCSV = "fills_csv"
    case reportJSON = "report_json"
    case reportMarkdown = "report_markdown"
    case snapshotJSON = "snapshot_json"

    var expectedFileName: String {
        switch self {
        case .decisionsCSV: return "decisions.csv"
        case .ordersCSV: return "orders.csv"
        case .fillsCSV: return "fills.csv"
        case .reportJSON: return "report.json"
        case .reportMarkdown: return "report.md"
        case .snapshotJSON: return "snapshot.json"
        }
    }
}

struct AutomationReviewExportFile: Codable, Equatable {
    let file: AutomationReviewFile
    let file_name: String
    let media_type: String
    let bytes: UInt64
    let sha256: String
}

struct AutomationReviewExportManifest: Codable, Equatable {
    let schema_version: String
    let session_id: String
    let mode: AutomationMode
    let snapshot_digest: String
    let files: [AutomationReviewExportFile]
}

struct AutomationReviewExportChunk: Codable, Equatable {
    let session_id: String
    let mode: AutomationMode
    let snapshot_digest: String
    let file: AutomationReviewFile
    let file_name: String
    let media_type: String
    let total_bytes: UInt64
    let sha256: String
    let offset: UInt64
    let bytes: UInt64
    let chunk_sha256: String
    let encoding: String
    let data_hex: String
    let next_offset: UInt64?
}

struct AutomationReviewRequestGeneration: Equatable {
    private(set) var value: UInt64 = 0

    mutating func advance() -> UInt64 {
        value &+= 1
        return value
    }

    func accepts(_ ticket: UInt64) -> Bool {
        value == ticket
    }
}

enum AutomationReviewExportCodec {
    static let maximumFileBytes: UInt64 = 32 * 1024 * 1024
    static let maximumChunkBytes: UInt64 = 65_536

    static func validate(
        _ manifest: AutomationReviewExportManifest,
        sessionID: String,
        mode: AutomationMode
    ) throws {
        guard manifest.schema_version == "1.0",
              manifest.session_id == sessionID,
              manifest.mode == mode,
              isSHA256Digest(manifest.snapshot_digest),
              manifest.files.count == AutomationReviewFile.allCases.count,
              Set(manifest.files.map(\.file)) == Set(AutomationReviewFile.allCases) else {
            throw AutomationSessionServiceError.identityMismatch("review export manifest identity/files")
        }
        for file in manifest.files {
            guard file.file_name == file.file.expectedFileName,
                  !file.media_type.isEmpty,
                  file.bytes <= maximumFileBytes,
                  isSHA256Digest(file.sha256) else {
                throw AutomationSessionServiceError.invalidResponse("review export manifest file bounds/hash")
            }
        }
    }

    static func decodeAndValidate(
        _ chunk: AutomationReviewExportChunk,
        file: AutomationReviewExportFile,
        sessionID: String,
        mode: AutomationMode,
        snapshotDigest: String,
        expectedOffset: UInt64
    ) throws -> Data {
        guard chunk.session_id == sessionID,
              chunk.mode == mode,
              chunk.snapshot_digest == snapshotDigest,
              chunk.file == file.file,
              chunk.file_name == file.file_name,
              chunk.media_type == file.media_type,
              chunk.total_bytes == file.bytes,
              chunk.sha256 == file.sha256,
              chunk.offset == expectedOffset,
              chunk.encoding == "HEX",
              chunk.bytes <= maximumChunkBytes,
              isSHA256Digest(chunk.sha256),
              isSHA256Digest(chunk.chunk_sha256) else {
            throw AutomationSessionServiceError.identityMismatch("review export chunk identity/offset/hash")
        }
        let decoded = try decodeHex(chunk.data_hex)
        guard UInt64(decoded.count) == chunk.bytes,
              hexDigest(decoded) == chunk.chunk_sha256,
              expectedOffset <= file.bytes,
              expectedOffset + chunk.bytes <= file.bytes else {
            throw AutomationSessionServiceError.invalidResponse("review export chunk byte count/hash")
        }
        let expectedNext = expectedOffset + chunk.bytes
        guard chunk.next_offset == (expectedNext < file.bytes ? expectedNext : nil),
              (chunk.bytes > 0 || file.bytes == 0) else {
            throw AutomationSessionServiceError.invalidResponse("review export chunk continuation")
        }
        return decoded
    }

    static func validateCompleteFile(_ data: Data, manifestFile: AutomationReviewExportFile) throws {
        guard UInt64(data.count) == manifestFile.bytes,
              hexDigest(data) == manifestFile.sha256 else {
            throw AutomationSessionServiceError.identityMismatch("review export whole-file SHA-256")
        }
    }

    static func decodeHex(_ text: String) throws -> Data {
        let bytes = Array(text.utf8)
        guard bytes.count.isMultiple(of: 2), bytes.count / 2 <= Int(maximumChunkBytes) else {
            throw AutomationSessionServiceError.invalidResponse("review export HEX length")
        }
        var output = Data(capacity: bytes.count / 2)
        for index in stride(from: 0, to: bytes.count, by: 2) {
            guard let high = hexNibble(bytes[index]), let low = hexNibble(bytes[index + 1]) else {
                throw AutomationSessionServiceError.invalidResponse("review export HEX alphabet")
            }
            output.append((high << 4) | low)
        }
        return output
    }

    static func hexDigest(_ data: Data) -> String {
        SHA256.hash(data: data).map { String(format: "%02x", $0) }.joined()
    }

    static func isSHA256Digest(_ value: String) -> Bool {
        value.count == 64 && value.utf8.allSatisfy { (48...57).contains($0) || (97...102).contains($0) }
    }

    private static func hexNibble(_ byte: UInt8) -> UInt8? {
        switch byte {
        case 48...57: return byte - 48
        case 97...102: return byte - 97 + 10
        case 65...70: return byte - 65 + 10
        default: return nil
        }
    }
}

enum AutomationReviewPrivacyPresentation {
    static let hiddenText = "개인정보 숨김"

    static func identity(_ value: String?, masked: Bool) -> String {
        guard !masked else { return hiddenText }
        guard let value, !value.isEmpty else { return "미확인" }
        return value
    }

    static func exactValue(_ value: String?, masked: Bool) -> String {
        guard !masked else { return hiddenText }
        return value ?? "미확인"
    }

    static func evaluation(_ value: AutomationJSONValue, masked: Bool) -> String {
        guard !masked else {
            return "개인정보 표시를 켜면 당시 지표·판단 조건·상태 전이를 확인할 수 있습니다."
        }
        return value.redactedForDisplay.formattedJSON
    }
}

enum AutomationReviewExportStaging {
    static let directoryPermissions: Int16 = 0o700
    static let filePermissions: Int16 = 0o600

    static func createDirectory(inside parent: URL) throws -> URL {
        var isDirectory: ObjCBool = false
        guard FileManager.default.fileExists(atPath: parent.path, isDirectory: &isDirectory),
              isDirectory.boolValue else {
            throw AutomationSessionServiceError.invalidInput("review export destination directory")
        }
        let staging = parent.appendingPathComponent(
            ".sky-backcraft-review-partial-\(UUID().uuidString.lowercased())",
            isDirectory: true
        )
        try FileManager.default.createDirectory(
            at: staging,
            withIntermediateDirectories: false,
            attributes: [.posixPermissions: directoryPermissions]
        )
        return staging
    }

    static func write(_ data: Data, file: AutomationReviewExportFile, inside staging: URL) throws {
        guard file.file_name == file.file.expectedFileName,
              UInt64(data.count) == file.bytes,
              data.count <= Int(AutomationReviewExportCodec.maximumFileBytes),
              AutomationReviewExportCodec.hexDigest(data) == file.sha256,
              URL(fileURLWithPath: file.file_name).lastPathComponent == file.file_name else {
            throw AutomationSessionServiceError.invalidResponse("review export staged file identity/size/hash")
        }
        var isDirectory: ObjCBool = false
        guard FileManager.default.fileExists(atPath: staging.path, isDirectory: &isDirectory),
              isDirectory.boolValue else {
            throw AutomationSessionServiceError.invalidInput("private review export staging directory")
        }
        let url = staging.appendingPathComponent(file.file_name)
        guard FileManager.default.createFile(
            atPath: url.path,
            contents: data,
            attributes: [.posixPermissions: filePermissions]
        ) else {
            throw AutomationSessionServiceError.invalidInput("review export staged file already exists or cannot be created")
        }
    }
}

enum AutomationLifecycleCommand: String {
    case pause
    case resume
    case stop

    var receiptAction: AutomationCommandAction {
        switch self {
        case .pause: return .pause
        case .resume: return .resume
        case .stop: return .stop
        }
    }
}

/// Credential-free command identity persisted locally while a write has no final receipt.
struct AutomationPendingWrite: Codable, Equatable {
    let request_id: String
    let action: AutomationCommandAction
    let session_id: String?
    let expected_sequence: UInt64?
    let policy_ref: AutomationPolicyRevisionReference?
    let market: String?
    let decision_interval: String?
    let risk: AutomationRiskLimits?
    let client_identifier: String?
    let baseline_day_utc: String?
    let baseline_source: AutomationBaselineSource?
    let mode: AutomationMode
    let account_identity: String?

    init(
        request_id: String,
        action: AutomationCommandAction,
        session_id: String? = nil,
        expected_sequence: UInt64? = nil,
        policy_ref: AutomationPolicyRevisionReference? = nil,
        market: String? = nil,
        decision_interval: String? = nil,
        risk: AutomationRiskLimits? = nil,
        client_identifier: String? = nil,
        baseline_day_utc: String? = nil,
        baseline_source: AutomationBaselineSource? = nil,
        mode: AutomationMode = .mock,
        account_identity: String? = nil
    ) {
        self.request_id = request_id
        self.action = action
        self.session_id = session_id
        self.expected_sequence = expected_sequence
        self.policy_ref = policy_ref
        self.market = market
        self.decision_interval = decision_interval
        self.risk = risk
        self.client_identifier = client_identifier
        self.baseline_day_utc = baseline_day_utc
        self.baseline_source = baseline_source
        self.mode = mode
        self.account_identity = account_identity
    }

    enum CodingKeys: String, CodingKey {
        case request_id, action, session_id, expected_sequence, policy_ref, market, decision_interval, risk
        case client_identifier, baseline_day_utc, baseline_source, mode, account_identity
    }

    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        self.init(
            request_id: try c.decode(String.self, forKey: .request_id),
            action: try c.decode(AutomationCommandAction.self, forKey: .action),
            session_id: try c.decodeIfPresent(String.self, forKey: .session_id),
            expected_sequence: try c.decodeIfPresent(UInt64.self, forKey: .expected_sequence),
            policy_ref: try c.decodeIfPresent(AutomationPolicyRevisionReference.self, forKey: .policy_ref),
            market: try c.decodeIfPresent(String.self, forKey: .market),
            decision_interval: try c.decodeIfPresent(String.self, forKey: .decision_interval),
            risk: try c.decodeIfPresent(AutomationRiskLimits.self, forKey: .risk),
            client_identifier: try c.decodeIfPresent(String.self, forKey: .client_identifier),
            baseline_day_utc: try c.decodeIfPresent(String.self, forKey: .baseline_day_utc),
            baseline_source: try c.decodeIfPresent(AutomationBaselineSource.self, forKey: .baseline_source),
            mode: try c.decodeIfPresent(AutomationMode.self, forKey: .mode) ?? .mock,
            account_identity: try c.decodeIfPresent(String.self, forKey: .account_identity)
        )
    }

    func validate(_ response: AutomationSessionCommandResponse) throws {
        guard response.receipt.request_id == request_id,
              response.receipt.action == action,
              response.receipt.sequence == response.session.sequence else {
            throw AutomationSessionServiceError.identityMismatch("durable request/action/session sequence")
        }
        if let expectedSessionID = session_id, response.session.session_id != expectedSessionID {
            throw AutomationSessionServiceError.identityMismatch("durable request session_id")
        }
        if let expectedSequence = expected_sequence, response.receipt.sequence < expectedSequence {
            throw AutomationSessionServiceError.identityMismatch("durable request expected_sequence")
        }
        guard response.session.mode == mode,
              account_identity.map({ $0 == response.session.account_identity }) ?? true else {
            throw AutomationSessionServiceError.identityMismatch("durable request mode/account")
        }
        if mode == .live, action != .start, account_identity == nil {
            throw AutomationSessionServiceError.invalidInput("durable LIVE lifecycle account identity is missing")
        }
        try AutomationAccountIdentityScope.validate(response.session.account_identity, mode: mode)

        switch action {
        case .start:
            guard let policy_ref, let market, let decision_interval, let risk,
                  session_id == nil, expected_sequence == nil,
                  response.session.mode == mode,
                  response.session.policy_ref == policy_ref,
                  response.session.market == market,
                  response.session.decision_interval == decision_interval,
                  response.session.risk == risk,
                  response.order == nil,
                  response.baseline == nil else {
                throw AutomationSessionServiceError.identityMismatch("durable frozen start identity/risk/mode")
            }
        case .pause, .resume, .stop:
            guard session_id != nil, expected_sequence != nil, response.order == nil, response.baseline == nil else {
                throw AutomationSessionServiceError.invalidResponse("durable lifecycle request identity")
            }
        case .cancelOrder:
            guard session_id != nil, expected_sequence != nil, let client_identifier,
                  response.session.mode == .mock,
                  let order = response.order,
                  order.client_identifier == client_identifier,
                  order.market == response.session.market,
                  response.baseline == nil else {
                throw AutomationSessionServiceError.identityMismatch("durable owned mock order identity")
            }
        case .recordBaseline:
            guard session_id != nil, expected_sequence != nil, response.order == nil,
                  let expectedDay = baseline_day_utc, let expectedSource = baseline_source,
                  let baseline = response.baseline,
                  baseline.account_identity == response.session.account_identity,
                  baseline.mode == response.session.mode,
                  baseline.day_utc == expectedDay,
                  baseline.source == expectedSource else {
                throw AutomationSessionServiceError.identityMismatch("durable baseline record identity")
            }
            try AutomationSessionMCPService.validateBaselineViewClaims(baseline, mode: response.session.mode)
        }
    }
}

enum AutomationSessionWire {
    static let sessionPageLimit: UInt32 = 20
    static let ledgerPageLimit: UInt32 = 100
    static let reviewDecisionPageLimit: UInt32 = 50

    static func validateSessionID(_ value: String) throws {
        let bytes = Array(value.utf8)
        guard !bytes.isEmpty, bytes.count <= 96,
              bytes.allSatisfy({ (48...57).contains($0) || (65...90).contains($0)
                  || (97...122).contains($0) || $0 == 45 || $0 == 95 }) else {
            throw AutomationSessionServiceError.invalidInput("전략 세션 식별 정보")
        }
    }

    static func startArguments(
        requestID: String,
        policyReference: AutomationPolicyRevisionReference,
        market: String,
        decisionInterval: String,
        risk: AutomationRiskLimits,
        mode: AutomationMode = .mock
    ) throws -> [String: Any] {
        try policyReference.validate()
        try risk.validate(for: mode)
        guard !requestID.isEmpty,
              market == market.trimmingCharacters(in: .whitespacesAndNewlines), !market.isEmpty,
              decisionInterval == decisionInterval.trimmingCharacters(in: .whitespacesAndNewlines),
              !decisionInterval.isEmpty else {
            throw AutomationSessionServiceError.invalidInput("request_id, market, decision_interval")
        }
        return [
            "action": "start",
            "request_id": requestID,
            "policy_ref": policyReference.arguments,
            "market": market,
            "decision_interval": decisionInterval,
            "mode": mode.rawValue,
            "risk": risk.arguments
        ]
    }

    static func lifecycleArguments(
        command: AutomationLifecycleCommand,
        requestID: String,
        sessionID: String,
        expectedSequence: UInt64
    ) throws -> [String: Any] {
        try validateSessionID(sessionID)
        guard !requestID.isEmpty, !sessionID.isEmpty else {
            throw AutomationSessionServiceError.invalidInput("request_id/session_id")
        }
        return [
            "action": command.rawValue,
            "request_id": requestID,
            "session_id": sessionID,
            "expected_sequence": expectedSequence
        ]
    }

    static func cancelOrderArguments(
        requestID: String,
        sessionID: String,
        expectedSequence: UInt64,
        clientIdentifier: String
    ) throws -> [String: Any] {
        guard !requestID.isEmpty, !sessionID.isEmpty, !clientIdentifier.isEmpty else {
            throw AutomationSessionServiceError.invalidInput("request_id/session_id/client_identifier")
        }
        return [
            "action": "cancel_order",
            "request_id": requestID,
            "session_id": sessionID,
            "expected_sequence": expectedSequence,
            "client_identifier": clientIdentifier
        ]
    }

    static func recordBaselineArguments(
        requestID: String,
        sessionID: String,
        expectedSequence: UInt64,
        mode: AutomationMode,
        baseline: AutomationAccountDayBaselineInput
    ) throws -> [String: Any] {
        guard !requestID.isEmpty, !sessionID.isEmpty else {
            throw AutomationSessionServiceError.invalidInput("request_id/session_id")
        }
        try baseline.validate(for: mode)
        return [
            "action": "record_baseline",
            "request_id": requestID,
            "session_id": sessionID,
            "expected_sequence": expectedSequence,
            "baseline": baseline.arguments
        ]
    }

    static func listArguments(offset: UInt32 = 0, limit: UInt32 = sessionPageLimit) -> [String: Any] {
        ["action": "list", "offset": offset, "limit": limit]
    }

    static func getArguments(sessionID: String) -> [String: Any] {
        ["action": "get", "session_id": sessionID]
    }

    static func requestArguments(requestID: String) -> [String: Any] {
        ["action": "request", "request_id": requestID]
    }

    static func baselineArguments(sessionID: String, dayUTC: String? = nil) -> [String: Any] {
        [
            "action": "baseline",
            "session_id": sessionID,
            "day_utc": dayUTC as Any? ?? NSNull()
        ]
    }

    static func eventsArguments(sessionID: String, afterSequence: UInt64? = nil) -> [String: Any] {
        [
            "action": "events",
            "session_id": sessionID,
            "after_sequence": afterSequence as Any? ?? NSNull(),
            "limit": ledgerPageLimit
        ]
    }

    static func ledgerArguments(sessionID: String, afterSequence: UInt64? = nil) -> [String: Any] {
        [
            "action": "ledger",
            "session_id": sessionID,
            "after_sequence": afterSequence as Any? ?? NSNull(),
            "limit": ledgerPageLimit
        ]
    }

    static func reviewDecisionsArguments(sessionID: String, offset: UInt32 = 0, limit: UInt32 = reviewDecisionPageLimit) -> [String: Any] {
        ["action": "decisions", "session_id": sessionID, "offset": offset, "limit": limit]
    }

    static func reviewDecisionArguments(sessionID: String, intentID: String) -> [String: Any] {
        ["action": "decision", "session_id": sessionID, "intent_id": intentID]
    }

    static func reviewPerformanceArguments(sessionID: String) -> [String: Any] {
        ["action": "performance", "session_id": sessionID]
    }

    static func reviewExportManifestArguments(sessionID: String) -> [String: Any] {
        ["action": "export_manifest", "session_id": sessionID]
    }

    static func reviewExportReadArguments(
        sessionID: String,
        file: AutomationReviewFile,
        offset: UInt64,
        expectedSHA256: String,
        expectedSnapshotDigest: String
    ) -> [String: Any] {
        [
            "action": "export_read",
            "session_id": sessionID,
            "file": file.rawValue,
            "offset": offset,
            "expected_sha256": expectedSHA256,
            "expected_snapshot_digest": expectedSnapshotDigest
        ]
    }
}

final class AutomationSessionMCPService {
    private let client: NativeMCPClient

    init(client: NativeMCPClient = .shared) {
        self.client = client
    }

    func currentTransportContext() -> NativeMCPTransportContext? { client.currentTransportContext() }
    func isCurrentTransportContext(_ context: NativeMCPTransportContext) -> Bool {
        client.currentTransportContext() == context
    }

    @discardableResult
    func listSessions(
        offset: UInt32 = 0,
        completion: @escaping (Result<AutomationSessionPage, Error>) -> Void
    ) -> NativeMCPRequest {
        let limit = AutomationSessionWire.sessionPageLimit
        return call(
            "automation_session_query",
            arguments: AutomationSessionWire.listArguments(offset: offset, limit: limit),
            validate: { page in
                guard page.returned_count == UInt64(page.items.count), page.items.count <= Int(limit),
                      Set(page.items.map(\.session_id)).count == page.items.count else {
                    throw AutomationSessionServiceError.invalidResponse("session page count/identity")
                }
                for session in page.items {
                    try Self.validateSessionProjection(session)
                }
            },
            completion: completion
        )
    }

    @discardableResult
    func getSession(
        sessionID: String,
        completion: @escaping (Result<AutomationSessionView, Error>) -> Void
    ) -> NativeMCPRequest {
        call(
            "automation_session_query",
            arguments: AutomationSessionWire.getArguments(sessionID: sessionID),
            validate: { session in
                guard session.session_id == sessionID else {
                    throw AutomationSessionServiceError.identityMismatch("session_id")
                }
                try Self.validateSessionProjection(session)
            },
            completion: completion
        )
    }

    @discardableResult
    func listReviewDecisions(
        sessionID: String,
        offset: UInt32 = 0,
        limit: UInt32 = AutomationSessionWire.reviewDecisionPageLimit,
        completion: @escaping (Result<AutomationReviewDecisionPage, Error>) -> Void
    ) -> NativeMCPRequest {
        guard !sessionID.isEmpty, (1...100).contains(limit) else {
            completion(.failure(AutomationSessionServiceError.invalidInput("review session_id/limit")))
            return .alreadyFinished()
        }
        return call(
            "automation_session_query",
            arguments: AutomationSessionWire.reviewDecisionsArguments(sessionID: sessionID, offset: offset, limit: limit),
            validate: { page in
                try Self.validateReviewDecisionPage(page, sessionID: sessionID, offset: offset, limit: limit)
            },
            completion: completion
        )
    }

    @discardableResult
    func getReviewDecision(
        sessionID: String,
        intentID: String,
        expectedMode: AutomationMode,
        expectedPolicyReference: AutomationPolicyRevisionReference,
        completion: @escaping (Result<AutomationReviewDecisionDetail, Error>) -> Void
    ) -> NativeMCPRequest {
        call(
            "automation_session_query",
            arguments: AutomationSessionWire.reviewDecisionArguments(sessionID: sessionID, intentID: intentID),
            validate: { detail in
                try Self.validateReviewDecisionDetail(
                    detail,
                    sessionID: sessionID,
                    intentID: intentID,
                    expectedMode: expectedMode,
                    expectedPolicyReference: expectedPolicyReference
                )
            },
            completion: completion
        )
    }

    @discardableResult
    func getAutomationPerformance(
        sessionID: String,
        expectedMode: AutomationMode,
        expectedPolicyReference: AutomationPolicyRevisionReference,
        completion: @escaping (Result<AutomationPerformanceReport, Error>) -> Void
    ) -> NativeMCPRequest {
        call(
            "automation_session_query",
            arguments: AutomationSessionWire.reviewPerformanceArguments(sessionID: sessionID),
            validate: { report in
                try Self.validateAutomationPerformance(
                    report,
                    sessionID: sessionID,
                    expectedMode: expectedMode,
                    expectedPolicyReference: expectedPolicyReference
                )
            },
            completion: completion
        )
    }

    @discardableResult
    func getAutomationReviewExportManifest(
        sessionID: String,
        expectedMode: AutomationMode,
        completion: @escaping (Result<AutomationReviewExportManifest, Error>) -> Void
    ) -> NativeMCPRequest {
        call(
            "automation_session_query",
            arguments: AutomationSessionWire.reviewExportManifestArguments(sessionID: sessionID),
            validate: { manifest in
                try AutomationReviewExportCodec.validate(manifest, sessionID: sessionID, mode: expectedMode)
            },
            completion: completion
        )
    }

    @discardableResult
    func readAutomationReviewExport(
        sessionID: String,
        mode: AutomationMode,
        snapshotDigest: String,
        file: AutomationReviewExportFile,
        offset: UInt64,
        completion: @escaping (Result<AutomationReviewExportChunk, Error>) -> Void
    ) -> NativeMCPRequest {
        call(
            "automation_session_query",
            arguments: AutomationSessionWire.reviewExportReadArguments(
                sessionID: sessionID,
                file: file.file,
                offset: offset,
                expectedSHA256: file.sha256,
                expectedSnapshotDigest: snapshotDigest
            ),
            validate: { chunk in
                _ = try AutomationReviewExportCodec.decodeAndValidate(
                    chunk,
                    file: file,
                    sessionID: sessionID,
                    mode: mode,
                    snapshotDigest: snapshotDigest,
                    expectedOffset: offset
                )
            },
            completion: completion
        )
    }

    static func validateReviewDecisionPage(
        _ page: AutomationReviewDecisionPage,
        sessionID: String,
        offset: UInt32,
        limit: UInt32
    ) throws {
        guard page.session_id == sessionID,
              page.items.count <= Int(limit),
              page.returned_count == UInt64(page.items.count),
              page.total_count >= page.returned_count,
              Set(page.items.map(\.intent_id)).count == page.items.count,
              page.items.allSatisfy({ !$0.intent_id.isEmpty && !$0.observation_id.isEmpty && !$0.state.isEmpty }) else {
            throw AutomationSessionServiceError.identityMismatch("review decision page session/count/identity")
        }
        let nextExpected = UInt64(offset) + page.returned_count
        if nextExpected < page.total_count {
            guard nextExpected <= UInt64(UInt32.max), page.next_offset == UInt32(nextExpected) else {
                throw AutomationSessionServiceError.identityMismatch("review decision page continuation")
            }
        } else if page.next_offset != nil {
            throw AutomationSessionServiceError.invalidResponse("review decision page final continuation")
        }
    }

    static func validateReviewDecisionDetail(
        _ detail: AutomationReviewDecisionDetail,
        sessionID: String,
        intentID: String,
        expectedMode: AutomationMode,
        expectedPolicyReference: AutomationPolicyRevisionReference
    ) throws {
        let session = detail.session
        let decision = detail.decision
        let candle = decision.candle
        let timestamp = candle.candle.close_time_utc
        guard session.session_id == sessionID,
              session.mode == expectedMode,
              session.policy_ref == expectedPolicyReference,
              detail.frozen_policy.reference == expectedPolicyReference,
              detail.frozen_policy.revision_number > 0,
              !detail.frozen_policy.family.isEmpty,
              !detail.frozen_policy.definition.name.isEmpty,
              decision.session_id == sessionID,
              decision.intent_id == intentID,
              decision.observation_id == candle.id,
              decision.candle.candle.market == session.market,
              decision.candle.candle.interval == session.decision_interval,
              timestamp == decision.decision_time_utc,
              decision.candle.candle.completed,
              isSHA256Digest(decision.evaluation_digest),
              Set(detail.context_candles.map(\.id)).count == detail.context_candles.count,
              detail.context_candles.count <= 64,
              detail.orders.allSatisfy({ $0.market == session.market && $0.intent_id == intentID }),
              detail.events.allSatisfy({ $0.session_id == sessionID }) else {
            throw AutomationSessionServiceError.identityMismatch("review decision/session/candle/policy/order/event link")
        }
        try validateRecordedDecisionPolicyReference(
            detail.recorded_decision,
            expected: expectedPolicyReference
        )
        guard let decisionTime = parseRFC3339(decision.decision_time_utc),
              let recordedAt = parseRFC3339(decision.recorded_at_utc),
              recordedAt >= decisionTime,
              parseRFC3339(session.created_at_utc) != nil,
              parseRFC3339(session.updated_at_utc) != nil else {
            throw AutomationSessionServiceError.invalidResponse("review decision UTC/candle evidence")
        }
        _ = try validateReviewCandle(
            candle,
            market: session.market,
            interval: session.decision_interval,
            latestAllowedClose: decisionTime
        )
        for context in detail.context_candles {
            _ = try validateReviewCandle(
                context,
                market: session.market,
                interval: session.decision_interval,
                latestAllowedClose: decisionTime
            )
        }
        let contextTimes = detail.context_candles.compactMap { parseRFC3339($0.candle.close_time_utc) }
        guard contextTimes.count == detail.context_candles.count,
              zip(contextTimes, contextTimes.dropFirst()).allSatisfy({ $0.0 < $0.1 }) else {
            throw AutomationSessionServiceError.invalidResponse("review candle context order")
        }
    }

    private static func validateRecordedDecisionPolicyReference(
        _ value: AutomationJSONValue?,
        expected: AutomationPolicyRevisionReference
    ) throws {
        guard let value, value != .null else { return }
        guard case .object(let fields) = value else {
            throw AutomationSessionServiceError.invalidResponse("recorded decision projection")
        }
        guard let referenceValue = fields["policy_ref"], referenceValue != .null else { return }
        do {
            let encoder = JSONEncoder()
            let data = try encoder.encode(referenceValue)
            let reference = try JSONDecoder().decode(AutomationPolicyRevisionReference.self, from: data)
            guard reference == expected else {
                throw AutomationSessionServiceError.identityMismatch("recorded decision policy reference")
            }
        } catch let error as AutomationSessionServiceError {
            throw error
        } catch {
            throw AutomationSessionServiceError.invalidResponse("recorded decision policy reference")
        }
    }

    private static func validateReviewCandle(
        _ observation: AutomationReviewCandleObservation,
        market: String,
        interval: String,
        latestAllowedClose: Date
    ) throws -> Date {
        let candle = observation.candle
        guard !observation.id.isEmpty,
              isSHA256Digest(observation.content_digest),
              candle.market == market,
              candle.interval == interval,
              candle.completed else {
            throw AutomationSessionServiceError.identityMismatch("review candle identity/completion")
        }
        guard let openedAt = parseRFC3339(candle.open_time_utc),
              let closedAt = parseRFC3339(candle.close_time_utc),
              openedAt < closedAt,
              closedAt <= latestAllowedClose else {
            throw AutomationSessionServiceError.invalidResponse("review candle UTC range/causal close")
        }
        guard [candle.open, candle.high, candle.low, candle.close].allSatisfy(isNonNegativeDecimal),
              [candle.volume, candle.quote_turnover].allSatisfy(isNonNegativeDecimal) else {
            throw AutomationSessionServiceError.invalidResponse("review candle exact decimal text")
        }
        guard let open = Decimal(string: candle.open, locale: Locale(identifier: "en_US_POSIX")),
              let high = Decimal(string: candle.high, locale: Locale(identifier: "en_US_POSIX")),
              let low = Decimal(string: candle.low, locale: Locale(identifier: "en_US_POSIX")),
              let close = Decimal(string: candle.close, locale: Locale(identifier: "en_US_POSIX")),
              open > 0, high > 0, low > 0, close > 0,
              high >= open, high >= close, low <= open, low <= close, high >= low else {
            throw AutomationSessionServiceError.invalidResponse("review candle OHLC relation")
        }
        return closedAt
    }

    static func validateAutomationPerformance(
        _ report: AutomationPerformanceReport,
        sessionID: String,
        expectedMode: AutomationMode,
        expectedPolicyReference: AutomationPolicyRevisionReference
    ) throws {
        let markFieldCount = [
            report.mark_price_krw,
            report.mark_at_utc,
            report.mark_observation_id,
            report.mark_content_digest
        ].filter { $0 != nil }.count
        let markTime = report.mark_at_utc.flatMap(parseRFC3339)
        let reportTime = parseRFC3339(report.snapshot_at_utc)
        let openingInventoryIsValid: Bool
        if let opening = report.opening_inventory {
            openingInventoryIsValid = opening.snapshot_sequence > 0
                && opening.source_mode == expectedMode
                && isNonNegativeDecimal(opening.base_quantity)
                && isSHA256Digest(opening.balances_digest)
                && parseRFC3339(opening.observed_at_utc).map { observedAt in
                    reportTime.map { observedAt <= $0 } ?? false
                } == true
        } else {
            openingInventoryIsValid = true
        }
        let sessionPnLMustBeUnavailable: Bool
        if let opening = report.opening_inventory {
            let baseQuantity = Decimal(string: opening.base_quantity, locale: Locale(identifier: "en_US_POSIX"))
            sessionPnLMustBeUnavailable = baseQuantity.map { $0 > 0 } ?? true
        } else {
            let paidFees = Decimal(string: report.paid_fees_krw, locale: Locale(identifier: "en_US_POSIX"))
            let feesExistWithoutOpening = paidFees.map { $0 != .zero } ?? true
            sessionPnLMustBeUnavailable = report.fill_count > 0 || feesExistWithoutOpening
        }
        let sessionPnlMetrics = [report.realized_pnl_krw, report.unrealized_pnl_krw, report.total_pnl_krw]
        let pnlAvailabilityIsValid = !sessionPnLMustBeUnavailable || sessionPnlMetrics.allSatisfy { $0.value == nil }
        guard report.schema_version == "1.0",
              report.session.session_id == sessionID,
              report.session.mode == expectedMode,
              report.session.policy_ref == expectedPolicyReference,
              isSHA256Digest(report.snapshot_digest),
              parseRFC3339(report.snapshot_at_utc) != nil,
              [report.buy_funds_krw, report.sell_funds_krw, report.paid_fees_krw]
                  .allSatisfy(isNonNegativeDecimal),
              [report.remaining_volume, report.remaining_cost_krw, report.mark_price_krw]
                  .allSatisfy({ $0.map(isNonNegativeDecimal) ?? true }),
              markFieldCount == 0 || markFieldCount == 4,
              report.mark_observation_id.map({ !$0.isEmpty }) ?? true,
              report.mark_content_digest.map(isSHA256Digest) ?? true,
              markTime.map({ time in reportTime.map({ time <= $0 }) ?? false }) ?? (markFieldCount == 0),
              openingInventoryIsValid,
              pnlAvailabilityIsValid,
              isSignedDecimal(report.net_quote_cashflow_krw),
              [report.realized_pnl_krw, report.unrealized_pnl_krw, report.total_pnl_krw,
               report.return_ratio, report.max_drawdown_ratio].allSatisfy(validMetric) else {
            throw AutomationSessionServiceError.identityMismatch("automation performance identity/exact decimal/report metrics")
        }
    }

    static func validateReviewDecisionMatchesSummary(
        _ detail: AutomationReviewDecisionDetail,
        summary: AutomationReviewDecisionSummary
    ) throws {
        let decision = detail.decision
        guard decision.intent_id == summary.intent_id,
              decision.observation_id == summary.observation_id,
              decision.decision_time_utc == summary.decision_time_utc,
              decision.action == summary.action,
              decision.state == summary.state else {
            throw AutomationSessionServiceError.identityMismatch("selected review decision differs from its timeline summary")
        }
    }

    private static func validMetric(_ metric: AutomationReviewMetric) -> Bool {
        guard !metric.unit.isEmpty else { return false }
        if let value = metric.value {
            return metric.unavailable_reason == nil && isSignedDecimal(value)
        }
        return metric.unavailable_reason.map { !$0.isEmpty } == true
    }

    private static func isSignedDecimal(_ value: String) -> Bool {
        let unsigned = value.first == "-" ? String(value.dropFirst()) : value
        return isNonNegativeDecimal(unsigned)
    }

    @discardableResult
    func getBaselineStatus(
        sessionID: String,
        dayUTC: String? = nil,
        completion: @escaping (Result<AutomationAccountDayBaselineStatus, Error>) -> Void
    ) -> NativeMCPRequest {
        call(
            "automation_session_query",
            arguments: AutomationSessionWire.baselineArguments(sessionID: sessionID, dayUTC: dayUTC),
            validate: { status in
                guard status.session_id == sessionID else {
                    throw AutomationSessionServiceError.identityMismatch("baseline status session_id")
                }
                if let dayUTC, status.day_utc != dayUTC {
                    throw AutomationSessionServiceError.identityMismatch("baseline status day_utc")
                }
                try AutomationAccountIdentityScope.validate(status.account_identity, mode: status.mode)
                if let baseline = status.baseline {
                    guard baseline.account_identity == status.account_identity,
                          baseline.mode == status.mode,
                          baseline.day_utc == status.day_utc else {
                        throw AutomationSessionServiceError.identityMismatch("baseline view identity/mode/day")
                    }
                }
                try Self.validateBaselineStatusClaims(status)
            },
            completion: completion
        )
    }

    static func validateSessionProjection(_ session: AutomationSessionView, now: Date = Date()) throws {
        try AutomationSessionWire.validateSessionID(session.session_id)
        try AutomationAccountIdentityScope.validate(session.account_identity, mode: session.mode)
        try session.risk.validate(for: session.mode)
        guard let projection = session.account_risk else { return }
        if let selectedLimit = session.risk.activation_loss_limit_krw {
            guard projection.effective_loss?.basis == .liveActivationObserved else {
                throw AutomationSessionServiceError.identityMismatch("activation policy loss basis")
            }
            if let limit = projection.effective_loss?.limit_krw,
               let effective = Decimal(string: limit, locale: Locale(identifier: "en_US_POSIX")),
               let selected = Decimal(string: selectedLimit, locale: Locale(identifier: "en_US_POSIX")),
               effective > selected {
                throw AutomationSessionServiceError.identityMismatch("effective activation limit exceeds frozen session limit")
            }
        } else if projection.effective_loss?.basis == .liveActivationObserved {
            throw AutomationSessionServiceError.identityMismatch("activation loss basis without an explicit policy limit")
        }
        try validateAccountRiskProjection(
            projection,
            accountIdentity: session.account_identity,
            mode: session.mode,
            now: now
        )
    }

    static func validateAccountRiskProjection(
        _ projection: AutomationAccountRiskProjection,
        accountIdentity: String,
        mode: AutomationMode,
        now: Date = Date()
    ) throws {
        let baseline = projection.day_baseline
        let legacy = projection.legacy_mock_baseline
        if mode == .live {
            guard projection.balance.source == .upbitLiveRead,
                  projection.mock_assumptions == nil, legacy == nil else {
                throw AutomationSessionServiceError.identityMismatch("MOCK account evidence in LIVE projection")
            }
        }
        try validateEffectiveLoss(projection, accountIdentity: accountIdentity, mode: mode, now: now)
        guard baseline == nil || legacy == nil else {
            throw AutomationSessionServiceError.invalidResponse("typed and legacy baseline projections overlap")
        }

        if !projection.admission_block_reasons.isEmpty && projection.capabilities.can_submit_orders {
            throw AutomationSessionServiceError.invalidResponse("order submission capability conflicts with admission block reasons")
        }
        if let baseline {
            guard baseline.account_identity == accountIdentity else {
                throw AutomationSessionServiceError.identityMismatch("account risk baseline account_identity")
            }
            try validateBaselineViewClaims(baseline, mode: mode)
            if !baseline.sufficient_for_risk_admission && projection.capabilities.can_submit_orders {
                throw AutomationSessionServiceError.invalidResponse("insufficient day baseline marked as order-submittable")
            }
        } else if let legacy {
            try validateLegacyMockBaseline(
                legacy,
                mode: mode,
                accountIdentity: accountIdentity,
                dayUTC: legacy.day_utc,
                now: now
            )
            if projection.admission_block_reasons.contains("DAILY_START_BASELINE_MISSING") {
                throw AutomationSessionServiceError.invalidResponse("legacy baseline conflicts with missing-baseline block")
            }
        } else if projection.capabilities.can_submit_orders,
                  projection.effective_loss?.basis != .liveActivationObserved {
            throw AutomationSessionServiceError.invalidResponse("order submission capability without either baseline projection")
        }
    }

    private static func validateEffectiveLoss(
        _ projection: AutomationAccountRiskProjection,
        accountIdentity: String,
        mode: AutomationMode,
        now: Date
    ) throws {
        if let reference = projection.activation_reference {
            guard mode == .live, reference.mode == mode, reference.account_identity == accountIdentity,
                  reference.snapshot_sequence > 0, reference.reference_max_feed_age_seconds > 0,
                  isSHA256Digest(reference.balances_digest), isSHA256Digest(reference.valuation_digest),
                  reference.reference_equity_krw.map(isNonNegativeDecimal) ?? true else {
                throw AutomationSessionServiceError.identityMismatch("LIVE activation reference account/mode/provenance")
            }
            // The persisted anchor does not expire; only impossible future local observations are rejected.
            let times = [reference.reference_observed_at_utc, reference.reference_received_at_utc,
                         reference.valuation_recorded_at_utc].compactMap(parseRFC3339)
            guard times.count == 3, times.allSatisfy({ $0 <= now }) else {
                throw AutomationSessionServiceError.invalidResponse("invalid/future local activation reference time")
            }
            if reference.valuation_state == .verified, reference.reference_equity_krw == nil {
                throw AutomationSessionServiceError.invalidResponse("verified activation reference omitted equity")
            }
        }
        guard let loss = projection.effective_loss else { return }
        if let limit = loss.limit_krw {
            guard isNonNegativeDecimal(limit), Decimal(string: limit, locale: Locale(identifier: "en_US_POSIX")).map({ $0 > 0 }) == true else {
                throw AutomationSessionServiceError.invalidResponse("effective loss limit")
            }
        }
        if let used = loss.used_krw, !isNonNegativeDecimal(used) {
            throw AutomationSessionServiceError.invalidResponse("effective loss used amount")
        }
        if !loss.admission_blocked {
            guard loss.basis != .unavailable, loss.block_reasons.isEmpty,
                  let limit = loss.limit_krw, let used = loss.used_krw,
                  let limitValue = Decimal(string: limit, locale: Locale(identifier: "en_US_POSIX")),
                  let usedValue = Decimal(string: used, locale: Locale(identifier: "en_US_POSIX")),
                  usedValue < limitValue else {
                throw AutomationSessionServiceError.invalidResponse("unblocked effective loss has insufficient evidence")
            }
        }
        if loss.basis == .liveActivationObserved {
            guard mode == .live else {
                throw AutomationSessionServiceError.identityMismatch("LIVE activation loss in MOCK mode")
            }
            if !loss.admission_blocked {
                guard projection.activation_reference?.valuation_state == .verified else {
                    throw AutomationSessionServiceError.invalidResponse("unblocked activation loss without verified reference")
                }
            }
        }
        if projection.capabilities.can_submit_orders, loss.admission_blocked {
            throw AutomationSessionServiceError.invalidResponse("order submission conflicts with effective loss block")
        }
    }

    static func validateBaselineStatusClaims(
        _ status: AutomationAccountDayBaselineStatus,
        now: Date = Date()
    ) throws {
        try AutomationAccountIdentityScope.validate(status.account_identity, mode: status.mode)
        guard status.baseline == nil || status.legacy_mock_baseline == nil else {
            throw AutomationSessionServiceError.invalidResponse("typed and legacy baseline status projections overlap")
        }
        if let baseline = status.baseline {
            try validateBaselineViewClaims(baseline, mode: status.mode)
            if !baseline.sufficient_for_risk_admission && !status.admission_blocked {
                throw AutomationSessionServiceError.invalidResponse("insufficient baseline marked as admission allowed")
            }
        } else if let legacy = status.legacy_mock_baseline {
            try validateLegacyMockBaseline(
                legacy,
                mode: status.mode,
                accountIdentity: status.account_identity,
                dayUTC: status.day_utc,
                now: now
            )
            if status.block_reasons.contains("DAILY_START_BASELINE_MISSING") {
                throw AutomationSessionServiceError.invalidResponse("legacy baseline conflicts with missing-baseline block")
            }
        } else if !status.admission_blocked {
            throw AutomationSessionServiceError.invalidResponse("missing baseline marked as admission allowed")
        }
        if !status.block_reasons.isEmpty && !status.admission_blocked {
            throw AutomationSessionServiceError.invalidResponse("baseline block reasons marked as admission allowed")
        }
    }

    static func validateLegacyMockBaseline(
        _ legacy: AutomationLegacyMockBaselineView,
        mode: AutomationMode,
        accountIdentity: String,
        dayUTC: String,
        now: Date = Date()
    ) throws {
        guard mode == .mock else {
            throw AutomationSessionServiceError.identityMismatch("legacy local MOCK baseline in LIVE mode")
        }
        guard legacy.account_identity == accountIdentity else {
            throw AutomationSessionServiceError.identityMismatch("legacy baseline account_identity")
        }
        guard legacy.day_utc == dayUTC, isCanonicalUTCDay(legacy.day_utc) else {
            throw AutomationSessionServiceError.identityMismatch("legacy baseline day_utc")
        }
        guard isSHA256Digest(legacy.balances_digest) else {
            throw AutomationSessionServiceError.invalidResponse("legacy baseline balances_digest")
        }
        guard isNonNegativeDecimal(legacy.baseline_equity_krw) else {
            throw AutomationSessionServiceError.invalidResponse("legacy baseline baseline_equity_krw")
        }
        guard let recordedAt = parseRFC3339(legacy.recorded_at_utc), recordedAt <= now else {
            throw AutomationSessionServiceError.invalidResponse("legacy baseline local recorded_at_utc")
        }
    }

    private static func isSHA256Digest(_ value: String) -> Bool {
        let bytes = Array(value.utf8)
        return bytes.count == 64 && bytes.allSatisfy {
            ($0 >= 48 && $0 <= 57) || ($0 >= 65 && $0 <= 70) || ($0 >= 97 && $0 <= 102)
        }
    }

    private static func isNonNegativeDecimal(_ value: String) -> Bool {
        let bytes = Array(value.utf8)
        let dotCount = bytes.filter { $0 == 46 }.count
        guard !bytes.isEmpty, dotCount <= 1, bytes.first != 46, bytes.last != 46,
              bytes.allSatisfy({ ($0 >= 48 && $0 <= 57) || $0 == 46 }),
              let decimal = Decimal(string: value, locale: Locale(identifier: "en_US_POSIX")) else {
            return false
        }
        return decimal >= 0
    }

    private static func isCanonicalUTCDay(_ value: String) -> Bool {
        guard value.count == 10 else { return false }
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.calendar = Calendar(identifier: .gregorian)
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = "yyyy-MM-dd"
        formatter.isLenient = false
        guard let date = formatter.date(from: value) else { return false }
        return formatter.string(from: date) == value
    }

    private static func parseRFC3339(_ value: String) -> Date? {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        if let date = formatter.date(from: value) { return date }
        formatter.formatOptions = [.withInternetDateTime]
        return formatter.date(from: value)
    }

    static func validateBaselineViewClaims(_ baseline: AutomationAccountDayBaselineView, mode: AutomationMode) throws {
        guard baseline.mode == mode else {
            throw AutomationSessionServiceError.identityMismatch("baseline mode")
        }
        if baseline.source == .durableMockVenueSeed, mode != .mock {
            throw AutomationSessionServiceError.identityMismatch("MOCK-only baseline source in LIVE status")
        }
        if baseline.verification_state == .blockedInsufficientEvidence,
           baseline.sufficient_for_risk_admission {
            throw AutomationSessionServiceError.invalidResponse("blocked baseline marked as risk sufficient")
        }
        if !baseline.block_reasons.isEmpty && baseline.sufficient_for_risk_admission {
            throw AutomationSessionServiceError.invalidResponse("baseline with block reasons marked as risk sufficient")
        }
    }

    @discardableResult
    func readRequest(
        requestID: String,
        completion: @escaping (Result<AutomationSessionRequestReadback, Error>) -> Void
    ) -> NativeMCPRequest {
        call(
            "automation_session_query",
            arguments: AutomationSessionWire.requestArguments(requestID: requestID),
            validate: { readback in try Self.validate(readback, requestID: requestID) },
            completion: completion
        )
    }

    @discardableResult
    func listEvents(
        sessionID: String,
        afterSequence: UInt64? = nil,
        completion: @escaping (Result<AutomationEventPage, Error>) -> Void
    ) -> NativeMCPRequest {
        call(
            "automation_session_query",
            arguments: AutomationSessionWire.eventsArguments(sessionID: sessionID, afterSequence: afterSequence),
            validate: { page in
                let sequences = page.items.map(\.sequence)
                guard page.items.allSatisfy({ $0.session_id == sessionID }),
                      Set(sequences).count == sequences.count,
                      sequences.allSatisfy({ sequence in
                          guard let afterSequence else { return true }
                          return sequence > afterSequence
                      }) else {
                    throw AutomationSessionServiceError.identityMismatch("events session_id/sequence")
                }
            },
            completion: completion
        )
    }

    @discardableResult
    func listLedger(
        sessionID: String,
        market: String,
        afterSequence: UInt64? = nil,
        completion: @escaping (Result<AutomationLedgerPage, Error>) -> Void
    ) -> NativeMCPRequest {
        call(
            "automation_session_query",
            arguments: AutomationSessionWire.ledgerArguments(sessionID: sessionID, afterSequence: afterSequence),
            validate: { page in
                let identifiers = page.orders.map(\.client_identifier)
                let tradeIDs = page.fills.map(\.trade_uuid)
                guard page.orders.allSatisfy({ $0.market == market }),
                      page.fills.allSatisfy({ $0.market == market }),
                      Set(identifiers).count == identifiers.count,
                      Set(tradeIDs).count == tradeIDs.count else {
                    throw AutomationSessionServiceError.identityMismatch("ledger market/order/fill identity")
                }
            },
            completion: completion
        )
    }

    @discardableResult
    func startSession(
        requestID: String,
        policyReference: AutomationPolicyRevisionReference,
        market: String,
        decisionInterval: String,
        risk: AutomationRiskLimits,
        mode: AutomationMode = .mock,
        expectedContext: NativeMCPTransportContext? = nil,
        completion: @escaping (Result<AutomationSessionCommandResponse, Error>) -> Void
    ) throws -> NativeMCPRequest {
        let arguments = try AutomationSessionWire.startArguments(
            requestID: requestID,
            policyReference: policyReference,
            market: market,
            decisionInterval: decisionInterval,
            risk: risk,
            mode: mode
        )
        return call(
            "automation_session_write",
            arguments: arguments,
            expectedContext: expectedContext,
            validate: { response in
                try Self.validate(response, requestID: requestID, action: .start)
                guard response.session.mode == mode,
                      response.session.policy_ref == policyReference,
                      response.session.market == market,
                      response.session.decision_interval == decisionInterval,
                      response.session.risk == risk,
                      response.order == nil,
                      response.baseline == nil else {
                    throw AutomationSessionServiceError.identityMismatch("frozen start identity/risk/mode")
                }
            },
            completion: completion
        )
    }

    @discardableResult
    func transition(
        command: AutomationLifecycleCommand,
        requestID: String,
        sessionID: String,
        expectedSequence: UInt64,
        expectedMode: AutomationMode = .mock,
        expectedAccountIdentity: String? = nil,
        expectedContext: NativeMCPTransportContext? = nil,
        completion: @escaping (Result<AutomationSessionCommandResponse, Error>) -> Void
    ) throws -> NativeMCPRequest {
        if expectedMode == .live, expectedAccountIdentity == nil {
            throw AutomationSessionServiceError.invalidInput("LIVE 세션의 계좌 식별 정보를 먼저 조회하세요")
        }
        if let expectedAccountIdentity {
            try AutomationAccountIdentityScope.validate(expectedAccountIdentity, mode: expectedMode)
        }
        let arguments = try AutomationSessionWire.lifecycleArguments(
            command: command,
            requestID: requestID,
            sessionID: sessionID,
            expectedSequence: expectedSequence
        )
        return call(
            "automation_session_write",
            arguments: arguments,
            expectedContext: expectedContext,
            validate: { response in
                try Self.validate(response, requestID: requestID, action: command.receiptAction)
                guard response.session.session_id == sessionID,
                      response.session.mode == expectedMode,
                      expectedAccountIdentity.map({ $0 == response.session.account_identity }) ?? true,
                      response.order == nil, response.baseline == nil else {
                    throw AutomationSessionServiceError.identityMismatch("lifecycle session_id")
                }
            },
            completion: completion
        )
    }

    @discardableResult
    func cancelOwnedMockOrder(
        requestID: String,
        sessionID: String,
        expectedSequence: UInt64,
        clientIdentifier: String,
        completion: @escaping (Result<AutomationSessionCommandResponse, Error>) -> Void
    ) throws -> NativeMCPRequest {
        let arguments = try AutomationSessionWire.cancelOrderArguments(
            requestID: requestID,
            sessionID: sessionID,
            expectedSequence: expectedSequence,
            clientIdentifier: clientIdentifier
        )
        return call(
            "automation_session_write",
            arguments: arguments,
            validate: { response in
                try Self.validate(response, requestID: requestID, action: .cancelOrder)
                guard response.session.session_id == sessionID,
                      response.session.mode == .mock,
                      let order = response.order,
                      order.client_identifier == clientIdentifier,
                      order.market == response.session.market,
                      response.baseline == nil else {
                    throw AutomationSessionServiceError.identityMismatch("mock cancellation session/mode")
                }
            },
            completion: completion
        )
    }

    @discardableResult
    func recordBaseline(
        requestID: String,
        sessionID: String,
        expectedSequence: UInt64,
        mode: AutomationMode,
        baseline: AutomationAccountDayBaselineInput,
        completion: @escaping (Result<AutomationSessionCommandResponse, Error>) -> Void
    ) throws -> NativeMCPRequest {
        let arguments = try AutomationSessionWire.recordBaselineArguments(
            requestID: requestID,
            sessionID: sessionID,
            expectedSequence: expectedSequence,
            mode: mode,
            baseline: baseline
        )
        return call(
            "automation_session_write",
            arguments: arguments,
            validate: { response in
                try Self.validate(response, requestID: requestID, action: .recordBaseline)
                guard response.session.session_id == sessionID,
                      response.order == nil,
                      let recorded = response.baseline,
                      recorded.account_identity == baseline.account_identity,
                      recorded.mode == mode,
                      recorded.day_utc == baseline.day_utc,
                      recorded.source == baseline.source else {
                    throw AutomationSessionServiceError.identityMismatch("baseline record session/account/day/source")
                }
            },
            completion: completion
        )
    }

    private func call<Response: Decodable>(
        _ tool: String,
        arguments: [String: Any],
        expectedContext: NativeMCPTransportContext? = nil,
        validate: @escaping (Response) throws -> Void,
        completion: @escaping (Result<Response, Error>) -> Void
    ) -> NativeMCPRequest {
        client.call(tool, arguments: arguments, expectedTransportContext: expectedContext) { result in
            do {
                let data = try result.get()
                let response = try JSONDecoder().decode(Response.self, from: data)
                try validate(response)
                completion(.success(response))
            } catch {
                completion(.failure(error))
            }
        }
    }

    private static func validate(
        _ response: AutomationSessionCommandResponse,
        requestID: String,
        action: AutomationCommandAction? = nil
    ) throws {
        guard response.receipt.request_id == requestID else {
            throw AutomationSessionServiceError.identityMismatch("receipt.request_id")
        }
        if let action, response.receipt.action != action {
            throw AutomationSessionServiceError.identityMismatch("receipt.action")
        }
        guard response.receipt.sequence == response.session.sequence else {
            throw AutomationSessionServiceError.identityMismatch("receipt/session sequence")
        }
        try validateSessionProjection(response.session)
        if let baseline = response.baseline {
            try AutomationAccountIdentityScope.validate(baseline.account_identity, mode: baseline.mode)
        }
    }

    static func validate(_ readback: AutomationSessionRequestReadback, requestID: String) throws {
        guard readback.request_id == requestID else {
            throw AutomationSessionServiceError.identityMismatch("request_id")
        }
        switch readback.state {
        case .applied:
            guard let response = readback.response else {
                throw AutomationSessionServiceError.invalidResponse("APPLIED request response")
            }
            try validate(response, requestID: requestID)
            guard response.receipt.outcome != .pending else {
                throw AutomationSessionServiceError.identityMismatch("applied request with pending receipt")
            }
            if response.receipt.action == .cancelOrder, response.order == nil {
                throw AutomationSessionServiceError.invalidResponse("cancel_order response.order")
            }
            if response.receipt.action == .recordBaseline, response.baseline == nil {
                throw AutomationSessionServiceError.invalidResponse("record_baseline response.baseline")
            }
        case .pending:
            if let response = readback.response {
                try validate(response, requestID: requestID)
                guard response.receipt.outcome == .pending else {
                    throw AutomationSessionServiceError.identityMismatch("pending request response.outcome")
                }
            }
        case .rejected, .notRecorded:
            guard readback.response == nil else {
                throw AutomationSessionServiceError.invalidResponse("rejected/not-recorded response")
            }
        }
    }
}
