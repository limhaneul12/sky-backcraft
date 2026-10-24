import Foundation

enum DecisionAnalysisServiceError: LocalizedError {
    case invalidInput(String)
    case invalidResponse(String)
    case identityMismatch(String)
    case responseTooLarge

    var errorDescription: String? {
        switch self {
        case .invalidInput(let field):
            return "판단 분석 입력이 유효하지 않습니다: \(field)"
        case .invalidResponse(let field):
            return "판단 분석 응답을 해석할 수 없습니다: \(field)"
        case .identityMismatch(let field):
            return "판단 분석 응답의 저장 snapshot identity가 요청과 다릅니다: \(field)"
        case .responseTooLarge:
            return "판단 분석 응답이 표시 한도를 넘었습니다. 기간이나 시장 범위를 좁혀 다시 조회하세요."
        }
    }
}

struct DecisionAnalysisPolicyReference: Codable, Equatable {
    let policy_id: String
    let revision_id: String
    let definition_digest: String

    init(policyID: String, revisionID: String, definitionDigest: String) {
        policy_id = policyID
        revision_id = revisionID
        definition_digest = definitionDigest
    }

    init(_ reference: PaperPolicyRevisionReference) {
        self.init(
            policyID: reference.policy_id,
            revisionID: reference.revision_id,
            definitionDigest: reference.definition_digest
        )
    }

    init(_ reference: AutomationPolicyRevisionReference) {
        self.init(
            policyID: reference.policy_id,
            revisionID: reference.revision_id,
            definitionDigest: reference.definition_digest
        )
    }

    var arguments: [String: Any] {
        [
            "policy_id": policy_id,
            "revision_id": revision_id,
            "definition_digest": definition_digest
        ]
    }
}

enum DecisionAnalysisSource: Codable, Equatable {
    case paper(sessionID: String)
    case automation(sessionID: String)

    private enum CodingKeys: String, CodingKey {
        case family
        case session_id
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let sessionID = try container.decode(String.self, forKey: .session_id)
        switch try container.decode(String.self, forKey: .family) {
        case "paper": self = .paper(sessionID: sessionID)
        case "automation": self = .automation(sessionID: sessionID)
        case let family:
            throw DecisionAnalysisServiceError.invalidResponse("source.family=\(family)")
        }
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        switch self {
        case .paper(let sessionID):
            try container.encode("paper", forKey: .family)
            try container.encode(sessionID, forKey: .session_id)
        case .automation(let sessionID):
            try container.encode("automation", forKey: .family)
            try container.encode(sessionID, forKey: .session_id)
        }
    }

    var family: String {
        switch self {
        case .paper: "paper"
        case .automation: "automation"
        }
    }

    var sessionID: String {
        switch self {
        case .paper(let sessionID), .automation(let sessionID): sessionID
        }
    }

    var arguments: [String: Any] {
        ["family": family, "session_id": sessionID]
    }
}

struct DecisionAnalysisPeriod: Codable, Equatable {
    let start: String
    let end: String

    func matches(_ other: DecisionAnalysisPeriod) -> Bool {
        guard let leftStart = DecisionAnalysisInstant.utc(start),
              let rightStart = DecisionAnalysisInstant.utc(other.start),
              let leftEnd = DecisionAnalysisInstant.utc(end),
              let rightEnd = DecisionAnalysisInstant.utc(other.end) else {
            return false
        }
        return leftStart == rightStart && leftEnd == rightEnd
    }

    var arguments: [String: Any] { ["start": start, "end": end] }
}

/// Exact UTC instant identity at RFC3339 nanosecond precision. Parsing is
/// deliberately restricted to `Z` timestamps so a displayed UTC filter never
/// silently normalizes a local offset or loses sub-millisecond precision.
struct DecisionAnalysisInstant: Comparable, Equatable {
    let secondsSince1970: Int64
    let nanoseconds: UInt32

    static func utc(_ value: String) -> DecisionAnalysisInstant? {
        guard value.hasSuffix("Z") else { return nil }
        let body = String(value.dropLast())
        let pieces = body.split(separator: ".", maxSplits: 1, omittingEmptySubsequences: false)
        guard let datePart = pieces.first, !datePart.isEmpty else { return nil }
        let fraction: String
        if pieces.count == 2 {
            fraction = String(pieces[1])
            guard !fraction.isEmpty, fraction.count <= 9,
                  fraction.utf8.allSatisfy({ $0 >= 48 && $0 <= 57 }) else { return nil }
        } else {
            fraction = ""
        }
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime]
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        guard let date = formatter.date(from: "\(datePart)Z") else { return nil }
        let epoch = date.timeIntervalSince1970
        guard epoch.isFinite, epoch >= Double(Int64.min), epoch <= Double(Int64.max) else { return nil }
        let seconds = Int64(epoch.rounded())
        let paddedFraction = fraction.padding(toLength: 9, withPad: "0", startingAt: 0)
        let nanos = paddedFraction.isEmpty ? 0 : (UInt32(paddedFraction) ?? 0)
        return DecisionAnalysisInstant(secondsSince1970: seconds, nanoseconds: nanos)
    }

    static func < (lhs: DecisionAnalysisInstant, rhs: DecisionAnalysisInstant) -> Bool {
        if lhs.secondsSince1970 != rhs.secondsSince1970 {
            return lhs.secondsSince1970 < rhs.secondsSince1970
        }
        return lhs.nanoseconds < rhs.nanoseconds
    }
}

struct DecisionAnalysisPopulation: Codable, Equatable {
    let source: DecisionAnalysisSource
    let period: DecisionAnalysisPeriod
    let market: String?
    let policy_ref: DecisionAnalysisPolicyReference?

    init(
        source: DecisionAnalysisSource,
        period: DecisionAnalysisPeriod,
        market: String?,
        policyReference: DecisionAnalysisPolicyReference?
    ) {
        self.source = source
        self.period = period
        self.market = market
        policy_ref = policyReference
    }

    var arguments: [String: Any] {
        var value: [String: Any] = [
            "source": source.arguments,
            "period": period.arguments
        ]
        if let market { value["market"] = market }
        if let policy_ref { value["policy_ref"] = policy_ref.arguments }
        return value
    }

    func matches(_ other: DecisionAnalysisPopulation) -> Bool {
        source == other.source
            && period.matches(other.period)
            && market == other.market
            && policy_ref == other.policy_ref
    }
}

enum DecisionAnalysisFence: Codable, Equatable {
    case paper(asOfSequence: UInt64)
    case automation(snapshotDigest: String)

    private enum CodingKeys: String, CodingKey {
        case family
        case as_of_sequence
        case snapshot_digest
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        switch try container.decode(String.self, forKey: .family) {
        case "paper":
            self = .paper(asOfSequence: try container.decode(UInt64.self, forKey: .as_of_sequence))
        case "automation":
            self = .automation(snapshotDigest: try container.decode(String.self, forKey: .snapshot_digest))
        case let family:
            throw DecisionAnalysisServiceError.invalidResponse("fence.family=\(family)")
        }
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        switch self {
        case .paper(let asOfSequence):
            try container.encode("paper", forKey: .family)
            try container.encode(asOfSequence, forKey: .as_of_sequence)
        case .automation(let snapshotDigest):
            try container.encode("automation", forKey: .family)
            try container.encode(snapshotDigest, forKey: .snapshot_digest)
        }
    }

    var arguments: [String: Any] {
        switch self {
        case .paper(let sequence):
            ["family": "paper", "as_of_sequence": sequence]
        case .automation(let digest):
            ["family": "automation", "snapshot_digest": digest]
        }
    }

    var family: String {
        switch self {
        case .paper: "paper"
        case .automation: "automation"
        }
    }
}

struct DecisionAnalysisSelection: Codable, Equatable {
    let population: DecisionAnalysisPopulation
    let fence: DecisionAnalysisFence

    var arguments: [String: Any] {
        ["population": population.arguments, "fence": fence.arguments]
    }
}

enum DecisionAnalysisDecisionID: Codable, Equatable, Hashable {
    case paper(eventSequence: UInt64, signalID: String)
    case automation(intentID: String)

    private enum CodingKeys: String, CodingKey {
        case family
        case event_sequence
        case signal_id
        case intent_id
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        switch try container.decode(String.self, forKey: .family) {
        case "paper":
            self = .paper(
                eventSequence: try container.decode(UInt64.self, forKey: .event_sequence),
                signalID: try container.decode(String.self, forKey: .signal_id)
            )
        case "automation":
            self = .automation(intentID: try container.decode(String.self, forKey: .intent_id))
        case let family:
            throw DecisionAnalysisServiceError.invalidResponse("decision_id.family=\(family)")
        }
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        switch self {
        case .paper(let sequence, let signalID):
            try container.encode("paper", forKey: .family)
            try container.encode(sequence, forKey: .event_sequence)
            try container.encode(signalID, forKey: .signal_id)
        case .automation(let intentID):
            try container.encode("automation", forKey: .family)
            try container.encode(intentID, forKey: .intent_id)
        }
    }

    var family: String {
        switch self {
        case .paper: "paper"
        case .automation: "automation"
        }
    }

    var displayIdentity: String {
        switch self {
        case .paper(let sequence, let signalID): "이벤트 #\(sequence) · 신호 \(signalID)"
        case .automation(let intentID): "intent \(intentID)"
        }
    }

    var arguments: [String: Any] {
        switch self {
        case .paper(let sequence, let signalID):
            ["family": "paper", "event_sequence": sequence, "signal_id": signalID]
        case .automation(let intentID):
            ["family": "automation", "intent_id": intentID]
        }
    }
}

struct DecisionAnalysisFilter: Codable, Equatable {
    let strategy_action: String?
    let constrained_action: String?
    let execution_state: String?
    let stage: String?
    let reason_code: String?

    init(
        strategyAction: String? = nil,
        constrainedAction: String? = nil,
        executionState: String? = nil,
        stage: String? = nil,
        reasonCode: String? = nil
    ) {
        strategy_action = strategyAction
        constrained_action = constrainedAction
        execution_state = executionState
        self.stage = stage
        reason_code = reasonCode
    }

    var arguments: [String: Any] {
        var value: [String: Any] = [:]
        if let strategy_action { value["strategy_action"] = strategy_action }
        if let constrained_action { value["constrained_action"] = constrained_action }
        if let execution_state { value["execution_state"] = execution_state }
        if let stage { value["stage"] = stage }
        if let reason_code { value["reason_code"] = reason_code }
        return value
    }
}

enum DecisionAnalysisComparisonTarget: Equatable {
    case previous
    case selected(selection: DecisionAnalysisSelection, decisionID: DecisionAnalysisDecisionID)

    var arguments: [String: Any] {
        switch self {
        case .previous:
            ["kind": "previous"]
        case .selected(let selection, let decisionID):
            [
                "kind": "selected",
                "selection": selection.arguments,
                "decision_id": decisionID.arguments
            ]
        }
    }
}

struct DecisionAnalysisSessionContext: Equatable {
    let source: DecisionAnalysisSource
    let mode: String
    let policyReference: DecisionAnalysisPolicyReference
    let markets: [String]

    static func paper(_ session: PaperSessionView) -> DecisionAnalysisSessionContext {
        DecisionAnalysisSessionContext(
            source: .paper(sessionID: session.session_id),
            mode: "SIMULATED",
            policyReference: DecisionAnalysisPolicyReference(session.policy_ref),
            markets: Array(Set(session.markets.map(\.market))).sorted()
        )
    }

    static func automation(_ session: AutomationSessionView) -> DecisionAnalysisSessionContext {
        DecisionAnalysisSessionContext(
            source: .automation(sessionID: session.session_id),
            mode: session.mode.rawValue,
            policyReference: DecisionAnalysisPolicyReference(session.policy_ref),
            markets: [session.market]
        )
    }

    func population(period: DecisionAnalysisPeriod, market: String?) -> DecisionAnalysisPopulation {
        DecisionAnalysisPopulation(
            source: source,
            period: period,
            market: market,
            policyReference: policyReference
        )
    }
}

struct DecisionAnalysisSourceSnapshot: Decodable, Equatable {
    let selection: DecisionAnalysisSelection
    let mode: String
    let policy_ref: DecisionAnalysisPolicyReference
}

struct DecisionAnalysisActionCounts: Decodable, Equatable {
    let buy: UInt64
    let sell: UInt64
    let hold: UInt64
    let not_evaluated: UInt64
    let not_captured: UInt64
}

struct DecisionAnalysisExecutionCounts: Decodable, Equatable {
    let not_evaluated: UInt64
    let no_order: UInt64
    let rejected: UInt64
    let pending: UInt64
    let filled: UInt64
    let partially_filled: UInt64
    let cancelled: UInt64
    let expired: UInt64
    let not_captured: UInt64
}

struct DecisionAnalysisCaptureCounts: Decodable, Equatable {
    let captured: UInt64
    let partial_capture: UInt64
    let not_captured: UInt64
    let not_decision_event: UInt64
}

struct DecisionAnalysisReasonCount: Decodable, Equatable {
    let stage: String
    let code: String
    let distinct_decisions: UInt64
    let occurrences: UInt64
}

struct DecisionAnalysisConditionCount: Decodable, Equatable {
    let policy_ref: DecisionAnalysisPolicyReference
    let scope: AutomationJSONValue
    let ast_path: String
    let role: String
    let outcome: DecisionAnalysisConditionOutcome
    let occurrences: UInt64
}

enum DecisionAnalysisConditionReason: String, Equatable {
    case crossHistoryUnavailable = "CROSS_HISTORY_UNAVAILABLE"
    case andShortCircuit = "AND_SHORT_CIRCUIT"
    case orShortCircuit = "OR_SHORT_CIRCUIT"
    case ifBranchNotSelected = "IF_BRANCH_NOT_SELECTED"
    case priorRuleMatched = "PRIOR_RULE_MATCHED"
    case ruleNotMatched = "RULE_NOT_MATCHED"
}

enum DecisionAnalysisConditionOutcome: Decodable, Equatable {
    case evaluatedTrue
    case evaluatedFalse
    case unknown(DecisionAnalysisConditionReason)
    case notEvaluated(DecisionAnalysisConditionReason)

    var state: String {
        switch self {
        case .evaluatedTrue: "TRUE"
        case .evaluatedFalse: "FALSE"
        case .unknown: "UNKNOWN"
        case .notEvaluated: "NOT_EVALUATED"
        }
    }

    var reason: String? {
        switch self {
        case .evaluatedTrue, .evaluatedFalse: nil
        case .unknown(let reason), .notEvaluated(let reason): reason.rawValue
        }
    }

    init(from decoder: Decoder) throws {
        guard case .object(let fields) = try AutomationJSONValue(from: decoder),
              case .string(let state)? = fields["status"] else {
            throw DecisionAnalysisServiceError.invalidResponse("condition outcome status")
        }

        switch state {
        case "TRUE":
            guard fields.count == 1 else {
                throw DecisionAnalysisServiceError.invalidResponse("TRUE condition outcome fields")
            }
            self = .evaluatedTrue
        case "FALSE":
            guard fields.count == 1 else {
                throw DecisionAnalysisServiceError.invalidResponse("FALSE condition outcome fields")
            }
            self = .evaluatedFalse
        case "UNKNOWN", "NOT_EVALUATED":
            guard fields.count == 2,
                  case .string(let rawReason)? = fields["reason"],
                  let reason = DecisionAnalysisConditionReason(rawValue: rawReason) else {
                throw DecisionAnalysisServiceError.invalidResponse("condition outcome reason")
            }
            self = state == "UNKNOWN" ? .unknown(reason) : .notEvaluated(reason)
        default:
            throw DecisionAnalysisServiceError.invalidResponse("condition outcome status=\(state)")
        }
    }
}

struct DecisionAnalysisObservationCount: Decodable, Equatable {
    let kind: String
    let basis_time: String
    let scope: DecisionAnalysisObservationScope
    let condition_trace_capture: String
    let observations: UInt64
    let unplaced: UInt64
}

enum DecisionAnalysisObservationScope: Decodable, Equatable {
    case market(String)
    case sessionWide
    case unknown

    private enum CodingKeys: String, CodingKey { case kind, market }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        switch try container.decode(String.self, forKey: .kind) {
        case "market":
            let market = try container.decode(String.self, forKey: .market)
            guard !market.isEmpty else {
                throw DecisionAnalysisServiceError.invalidResponse("observation scope market")
            }
            self = .market(market)
        case "session_wide": self = .sessionWide
        case "unknown": self = .unknown
        case let kind:
            throw DecisionAnalysisServiceError.invalidResponse("observation scope kind=\(kind)")
        }
    }
}

struct DecisionAnalysisTotals: Decodable, Equatable {
    let decisions: UInt64
    let decisions_with_time: UInt64
    let unplaced_decisions: UInt64
    let strategy: DecisionAnalysisActionCounts
    let constrained: DecisionAnalysisActionCounts
    let execution: DecisionAnalysisExecutionCounts
    let orders: UInt64
    let decisions_with_orders: UInt64
    let fills: UInt64
    let decisions_with_fills: UInt64
    let condition_trace_capture: DecisionAnalysisCaptureCounts
    let reasons: [DecisionAnalysisReasonCount]
    let conditions: [DecisionAnalysisConditionCount]
    let non_decision_observations: [DecisionAnalysisObservationCount]
}

enum DecisionAnalysisPeriodPlacement: String, Decodable, Equatable {
    case inPeriod = "in_period"
    case unplaced
}

struct DecisionAnalysisRecordedReason: Decodable, Equatable {
    let stage: String
    let code: String
    let label_ko: String
}

struct DecisionAnalysisDecisionRow: Decodable, Equatable {
    let decision_id: DecisionAnalysisDecisionID
    let market: String
    let decision_time: String?
    let period_placement: DecisionAnalysisPeriodPlacement
    let policy_ref: DecisionAnalysisPolicyReference
    let saved_fact_digest: String
    let strategy_action: String
    let constrained_action: String
    let execution_state: String
    let reasons: [DecisionAnalysisRecordedReason]
}

struct DecisionAnalysisDecisionDetail: Decodable, Equatable {
    let row: DecisionAnalysisDecisionRow
    let explanation: AutomationJSONValue
}

struct DecisionAnalysisPage: Decodable, Equatable {
    let offset: UInt32
    let limit: UInt32
    let returned_count: UInt32
    let total_count: UInt64
    let in_period_count: UInt64
    let unplaced_count: UInt64
    let next_offset: UInt32?
    let items: [DecisionAnalysisDecisionRow]
}

struct DecisionAnalysisMeasurement: Decodable, Equatable {
    let elapsed_millis: UInt64
    let response_bytes: UInt64
}

enum DecisionAnalysisUnavailableReason: String, Decodable, Equatable {
    case noPreviousDecision = "no_previous_decision"
    case missingDecisionTime = "missing_decision_time"
}

struct DecisionAnalysisComparison: Decodable, Equatable {
    let kind: String
    let selected: DecisionAnalysisDecisionDetail
    let compared: DecisionAnalysisDecisionDetail?
    let unavailable_reason: DecisionAnalysisUnavailableReason?
    let changed_fields: [String]
}

private struct DecisionAnalysisOverviewResponse: Decodable {
    let schema_version: String
    let source_snapshot: DecisionAnalysisSourceSnapshot
    let population_complete: Bool
    let totals: DecisionAnalysisTotals
    let measurement: DecisionAnalysisMeasurement
}

private struct DecisionAnalysisDecisionsResponse: Decodable {
    let schema_version: String
    let source_snapshot: DecisionAnalysisSourceSnapshot
    let filter: DecisionAnalysisFilter?
    let page: DecisionAnalysisPage
    let measurement: DecisionAnalysisMeasurement
}

private struct DecisionAnalysisDecisionResponse: Decodable {
    let schema_version: String
    let source_snapshot: DecisionAnalysisSourceSnapshot
    let decision: DecisionAnalysisDecisionDetail
    let measurement: DecisionAnalysisMeasurement
}

private struct DecisionAnalysisCompareResponse: Decodable {
    let schema_version: String
    let selected_source_snapshot: DecisionAnalysisSourceSnapshot
    let compared_source_snapshot: DecisionAnalysisSourceSnapshot?
    let comparison: DecisionAnalysisComparison
    let measurement: DecisionAnalysisMeasurement
}

private enum DecisionAnalysisResponse: Decodable {
    case overview(DecisionAnalysisOverviewResponse)
    case decisions(DecisionAnalysisDecisionsResponse)
    case decision(DecisionAnalysisDecisionResponse)
    case compare(DecisionAnalysisCompareResponse)

    private enum CodingKeys: String, CodingKey { case action }

    init(from decoder: Decoder) throws {
        let action = try decoder.container(keyedBy: CodingKeys.self).decode(String.self, forKey: .action)
        switch action {
        case "overview": self = .overview(try DecisionAnalysisOverviewResponse(from: decoder))
        case "decisions": self = .decisions(try DecisionAnalysisDecisionsResponse(from: decoder))
        case "decision": self = .decision(try DecisionAnalysisDecisionResponse(from: decoder))
        case "compare": self = .compare(try DecisionAnalysisCompareResponse(from: decoder))
        case let action:
            throw DecisionAnalysisServiceError.invalidResponse("action=\(action)")
        }
    }
}

final class DecisionAnalysisMCPService {
    static let maximumResponseBytes = 512 * 1024
    private static let toolName = "decision_analysis_query"
    private let client: NativeMCPClient

    init(client: NativeMCPClient = .shared) {
        self.client = client
    }

    func currentTransportContext() -> NativeMCPTransportContext? {
        client.currentTransportContext()
    }

    func isCurrentTransportContext(_ context: NativeMCPTransportContext) -> Bool {
        client.currentTransportContext() == context
    }

    @discardableResult
    func overview(
        population: DecisionAnalysisPopulation,
        expectedMode: String,
        expectedTransportContext: NativeMCPTransportContext,
        completion: @escaping (Result<(DecisionAnalysisSourceSnapshot, Bool, DecisionAnalysisTotals, DecisionAnalysisMeasurement), Error>) -> Void
    ) -> NativeMCPRequest {
        guard Self.validPopulation(population), Self.validMode(expectedMode, for: population.source) else {
            completion(.failure(DecisionAnalysisServiceError.invalidInput("source, policy_ref, UTC period")))
            return .alreadyFinished()
        }
        return call(
            action: "overview",
            arguments: ["action": "overview", "population": population.arguments],
            expectedTransportContext: expectedTransportContext,
            decode: { response in
                guard case .overview(let value) = response else {
                    throw DecisionAnalysisServiceError.invalidResponse("action overview")
                }
                try Self.validateSchema(value.schema_version)
                try Self.validate(
                    value.source_snapshot,
                    expectedPopulation: population,
                    expectedMode: expectedMode,
                    expectedSelection: nil
                )
                try Self.validateOverviewObservationScopes(
                    value.totals.non_decision_observations,
                    population: population
                )
                return (
                    value.source_snapshot,
                    value.population_complete,
                    value.totals,
                    value.measurement
                )
            },
            completion: completion
        )
    }

    @discardableResult
    func decisions(
        selection: DecisionAnalysisSelection,
        page: (offset: UInt32, limit: UInt32),
        filter: DecisionAnalysisFilter?,
        expectedMode: String,
        expectedTransportContext: NativeMCPTransportContext,
        completion: @escaping (Result<(DecisionAnalysisPage, DecisionAnalysisMeasurement), Error>) -> Void
    ) -> NativeMCPRequest {
        guard Self.validSelection(selection), (1...100).contains(page.limit),
              Self.validMode(expectedMode, for: selection.population.source) else {
            completion(.failure(DecisionAnalysisServiceError.invalidInput("selection, page, source mode")))
            return .alreadyFinished()
        }
        var arguments: [String: Any] = [
            "action": "decisions",
            "selection": selection.arguments,
            "page": ["offset": page.offset, "limit": page.limit]
        ]
        if let filter { arguments["filter"] = filter.arguments }
        return call(
            action: "decisions",
            arguments: arguments,
            expectedTransportContext: expectedTransportContext,
            decode: { response in
                guard case .decisions(let value) = response else {
                    throw DecisionAnalysisServiceError.invalidResponse("action decisions")
                }
                try Self.validateSchema(value.schema_version)
                try Self.validate(
                    value.source_snapshot,
                    expectedPopulation: selection.population,
                    expectedMode: expectedMode,
                    expectedSelection: selection
                )
                guard value.filter == filter else {
                    throw DecisionAnalysisServiceError.identityMismatch("filter")
                }
                try Self.validate(
                    value.page,
                    requestedOffset: page.offset,
                    requestedLimit: page.limit,
                    selection: selection,
                    filter: filter
                )
                return (value.page, value.measurement)
            },
            completion: completion
        )
    }

    @discardableResult
    func decision(
        selection: DecisionAnalysisSelection,
        decisionID: DecisionAnalysisDecisionID,
        expectedMode: String,
        expectedTransportContext: NativeMCPTransportContext,
        completion: @escaping (Result<(DecisionAnalysisDecisionDetail, DecisionAnalysisMeasurement), Error>) -> Void
    ) -> NativeMCPRequest {
        guard Self.validSelection(selection), Self.validDecisionID(decisionID, for: selection),
              Self.validMode(expectedMode, for: selection.population.source) else {
            completion(.failure(DecisionAnalysisServiceError.invalidInput("selection, decision identity, source mode")))
            return .alreadyFinished()
        }
        return call(
            action: "decision",
            arguments: [
                "action": "decision",
                "selection": selection.arguments,
                "decision_id": decisionID.arguments
            ],
            expectedTransportContext: expectedTransportContext,
            decode: { response in
                guard case .decision(let value) = response else {
                    throw DecisionAnalysisServiceError.invalidResponse("action decision")
                }
                try Self.validateSchema(value.schema_version)
                try Self.validate(
                    value.source_snapshot,
                    expectedPopulation: selection.population,
                    expectedMode: expectedMode,
                    expectedSelection: selection
                )
                try Self.validate(value.decision, expectedID: decisionID, selection: selection)
                return (value.decision, value.measurement)
            },
            completion: completion
        )
    }

    @discardableResult
    func compare(
        selection: DecisionAnalysisSelection,
        decisionID: DecisionAnalysisDecisionID,
        against: DecisionAnalysisComparisonTarget,
        expectedMode: String,
        expectedTransportContext: NativeMCPTransportContext,
        completion: @escaping (Result<(DecisionAnalysisComparison, DecisionAnalysisSourceSnapshot?, DecisionAnalysisMeasurement), Error>) -> Void
    ) -> NativeMCPRequest {
        guard Self.validSelection(selection), Self.validDecisionID(decisionID, for: selection),
              Self.validMode(expectedMode, for: selection.population.source),
              Self.validTarget(against, primary: selection) else {
            completion(.failure(DecisionAnalysisServiceError.invalidInput("comparison selection identity")))
            return .alreadyFinished()
        }
        return call(
            action: "compare",
            arguments: [
                "action": "compare",
                "selection": selection.arguments,
                "decision_id": decisionID.arguments,
                "against": against.arguments
            ],
            expectedTransportContext: expectedTransportContext,
            decode: { response in
                guard case .compare(let value) = response else {
                    throw DecisionAnalysisServiceError.invalidResponse("action compare")
                }
                try Self.validateSchema(value.schema_version)
                try Self.validate(
                    value.selected_source_snapshot,
                    expectedPopulation: selection.population,
                    expectedMode: expectedMode,
                    expectedSelection: selection
                )
                try Self.validate(value.comparison.selected, expectedID: decisionID, selection: selection)
                try Self.validate(
                    value.comparison,
                    target: against,
                    selectedID: decisionID,
                    primarySelection: selection,
                    primaryMode: expectedMode,
                    comparedSnapshot: value.compared_source_snapshot
                )
                return (value.comparison, value.compared_source_snapshot, value.measurement)
            },
            completion: completion
        )
    }

    private func call<Value>(
        action: String,
        arguments: [String: Any],
        expectedTransportContext: NativeMCPTransportContext,
        decode: @escaping (DecisionAnalysisResponse) throws -> Value,
        completion: @escaping (Result<Value, Error>) -> Void
    ) -> NativeMCPRequest {
        guard isCurrentTransportContext(expectedTransportContext) else {
            completion(.failure(NativeMCPError.transportContextChanged))
            return .alreadyFinished()
        }
        return client.call(
            Self.toolName,
            arguments: arguments,
            expectedTransportContext: expectedTransportContext
        ) { [weak self] result in
            guard let self else { return }
            do {
                let data = try result.get()
                guard data.count <= Self.maximumResponseBytes else {
                    throw DecisionAnalysisServiceError.responseTooLarge
                }
                let response = try JSONDecoder().decode(DecisionAnalysisResponse.self, from: data)
                guard self.isCurrentTransportContext(expectedTransportContext) else {
                    throw NativeMCPError.transportContextChanged
                }
                completion(.success(try decode(response)))
            } catch {
                completion(.failure(error))
            }
        }
    }

    private static func validateSchema(_ schemaVersion: String) throws {
        guard schemaVersion == "1.0" else {
            throw DecisionAnalysisServiceError.invalidResponse("schema_version=\(schemaVersion)")
        }
    }

    private static func validate(
        _ snapshot: DecisionAnalysisSourceSnapshot,
        expectedPopulation: DecisionAnalysisPopulation,
        expectedMode: String,
        expectedSelection: DecisionAnalysisSelection?
    ) throws {
        guard snapshot.selection.population.matches(expectedPopulation),
              snapshot.selection.fence.family == expectedPopulation.source.family,
              snapshot.mode == expectedMode,
              validMode(snapshot.mode, for: snapshot.selection.population.source),
              snapshot.policy_ref == expectedPopulation.policy_ref else {
            throw DecisionAnalysisServiceError.identityMismatch("source/mode/policy_ref")
        }
        if let expectedSelection, snapshot.selection != expectedSelection {
            throw DecisionAnalysisServiceError.identityMismatch("snapshot fence")
        }
        if case .paper(let sequence) = snapshot.selection.fence, sequence == 0 {
            throw DecisionAnalysisServiceError.invalidResponse("paper as_of_sequence")
        }
        if case .automation(let digest) = snapshot.selection.fence, !isDigest(digest) {
            throw DecisionAnalysisServiceError.invalidResponse("automation snapshot_digest")
        }
    }

    private static func validate(
        _ page: DecisionAnalysisPage,
        requestedOffset: UInt32,
        requestedLimit: UInt32,
        selection: DecisionAnalysisSelection,
        filter: DecisionAnalysisFilter?
    ) throws {
        let returned = UInt64(page.items.count)
        let endOffset = UInt64(requestedOffset) + returned
        guard page.offset == requestedOffset,
              page.limit == requestedLimit,
              page.returned_count == returned,
              returned <= UInt64(requestedLimit),
              page.total_count == page.in_period_count + page.unplaced_count,
              page.total_count >= endOffset,
              Set(page.items.map(\.decision_id)).count == page.items.count else {
            throw DecisionAnalysisServiceError.invalidResponse("page count/identity")
        }
        if endOffset < page.total_count {
            guard page.next_offset == UInt32(exactly: endOffset) else {
                throw DecisionAnalysisServiceError.invalidResponse("page continuation")
            }
        } else if page.next_offset != nil {
            throw DecisionAnalysisServiceError.invalidResponse("page final continuation")
        }

        var pageInPeriod: UInt64 = 0
        var pageUnplaced: UInt64 = 0
        for row in page.items {
            try validate(row, selection: selection)
            if row.period_placement == .inPeriod {
                pageInPeriod += 1
            } else {
                pageUnplaced += 1
            }
            if !matches(row, filter: filter) {
                throw DecisionAnalysisServiceError.identityMismatch("filtered decision row")
            }
        }
        let pageEnd = min(endOffset, page.total_count)
        guard pageInPeriod <= page.in_period_count,
              pageUnplaced <= page.unplaced_count,
              pageInPeriod + pageUnplaced == returned,
              pageEnd == endOffset || page.next_offset == nil else {
            throw DecisionAnalysisServiceError.invalidResponse("page period placement")
        }
    }

    private static func validate(
        _ detail: DecisionAnalysisDecisionDetail,
        expectedID: DecisionAnalysisDecisionID,
        selection: DecisionAnalysisSelection
    ) throws {
        try validate(detail.row, selection: selection)
        guard detail.row.decision_id == expectedID else {
            throw DecisionAnalysisServiceError.identityMismatch("decision_id")
        }
        guard case .object(let explanation) = detail.explanation,
              string(explanation["schema_version"]) == "1.0",
              string(explanation["strategy_action"]) == detail.row.strategy_action,
              string(explanation["constrained_action"]) == detail.row.constrained_action,
              string(explanation["execution_state"]) == detail.row.execution_state,
              explanationPolicyReference(explanation["policy_ref"], matches: detail.row.policy_ref),
              explanationTime(explanation["decision_time"], matches: detail.row.decision_time) else {
            throw DecisionAnalysisServiceError.identityMismatch("saved explanation/decision row")
        }
    }

    private static func validate(
        _ row: DecisionAnalysisDecisionRow,
        selection: DecisionAnalysisSelection
    ) throws {
        guard let periodStart = DecisionAnalysisInstant.utc(selection.population.period.start),
              let periodEnd = DecisionAnalysisInstant.utc(selection.population.period.end) else {
            throw DecisionAnalysisServiceError.invalidInput("UTC period")
        }
        guard row.decision_id.family == selection.population.source.family,
              row.policy_ref == selection.population.policy_ref,
              selection.population.market == nil || row.market == selection.population.market,
              !row.market.isEmpty,
              isDigest(row.saved_fact_digest) else {
            throw DecisionAnalysisServiceError.identityMismatch("decision row source/market/policy")
        }
        switch (row.decision_id, row.period_placement, row.decision_time, selection.fence) {
        case let (.paper(sequence, signalID), .inPeriod, .some(decisionTime), .paper(asOfSequence)):
            guard sequence > 0, sequence <= asOfSequence, !signalID.isEmpty,
                  let timestamp = DecisionAnalysisInstant.utc(decisionTime),
                  timestamp >= periodStart, timestamp < periodEnd else {
                throw DecisionAnalysisServiceError.identityMismatch("paper decision identity/time/cutoff")
            }
        case let (.paper(sequence, signalID), .unplaced, nil, .paper(asOfSequence)):
            guard sequence > 0, sequence <= asOfSequence, !signalID.isEmpty else {
                throw DecisionAnalysisServiceError.identityMismatch("paper unplaced decision time/cutoff")
            }
        case let (.automation(intentID), .inPeriod, .some(decisionTime), .automation(digest)):
            guard !intentID.isEmpty, isDigest(digest),
                  let timestamp = DecisionAnalysisInstant.utc(decisionTime),
                  timestamp >= periodStart, timestamp < periodEnd else {
                throw DecisionAnalysisServiceError.identityMismatch("automation decision identity/time/fence")
            }
        case let (.automation(intentID), .unplaced, nil, .automation(digest)):
            guard !intentID.isEmpty, isDigest(digest) else {
                throw DecisionAnalysisServiceError.identityMismatch("automation unplaced decision time/fence")
            }
        default:
            throw DecisionAnalysisServiceError.identityMismatch("decision placement/source fence")
        }
    }

    private static func validate(
        _ comparison: DecisionAnalysisComparison,
        target: DecisionAnalysisComparisonTarget,
        selectedID: DecisionAnalysisDecisionID,
        primarySelection: DecisionAnalysisSelection,
        primaryMode: String,
        comparedSnapshot: DecisionAnalysisSourceSnapshot?
    ) throws {
        guard comparison.selected.row.decision_id == selectedID else {
            throw DecisionAnalysisServiceError.identityMismatch("comparison selected identity")
        }
        switch (target, comparison.kind, comparison.compared, comparison.unavailable_reason) {
        case (.previous, "previous", let compared?, nil):
            guard let comparedSnapshot,
                  compared.row.market == comparison.selected.row.market,
                  isPrevious(compared.row, than: comparison.selected.row) else {
                throw DecisionAnalysisServiceError.identityMismatch("previous comparison fence/market/time")
            }
            try validatePreviousContext(
                comparedSnapshot,
                detail: compared,
                selectedSnapshot: primarySelection,
                selectedMode: primaryMode,
                selectedRow: comparison.selected.row
            )
        case (.previous, "previous", nil, .noPreviousDecision):
            guard comparedSnapshot == nil else {
                throw DecisionAnalysisServiceError.identityMismatch("unavailable previous snapshot")
            }
        case (.previous, "previous", nil, .missingDecisionTime):
            guard comparison.selected.row.decision_time == nil, comparedSnapshot == nil else {
                throw DecisionAnalysisServiceError.identityMismatch("missing-time comparison")
            }
        case let (.selected(expectedSelection, expectedID), "selected", compared?, nil):
            guard let comparedSnapshot,
                  comparedSnapshot.selection == expectedSelection,
                  comparedSnapshot.policy_ref == expectedSelection.population.policy_ref,
                  validMode(comparedSnapshot.mode, for: expectedSelection.population.source),
                  compared.row.decision_id == expectedID,
                  compared.row.policy_ref == expectedSelection.population.policy_ref,
                  expectedSelection.population.market == nil || compared.row.market == expectedSelection.population.market else {
                throw DecisionAnalysisServiceError.identityMismatch("selected comparison fence/identity")
            }
            try validate(compared, expectedID: expectedID, selection: expectedSelection)
        default:
            throw DecisionAnalysisServiceError.invalidResponse("comparison kind/availability")
        }
    }

    private static func validatePreviousContext(
        _ comparedSnapshot: DecisionAnalysisSourceSnapshot,
        detail: DecisionAnalysisDecisionDetail,
        selectedSnapshot: DecisionAnalysisSelection,
        selectedMode: String,
        selectedRow: DecisionAnalysisDecisionRow
    ) throws {
        let selectedPopulation = selectedSnapshot.population
        let comparedSelection = comparedSnapshot.selection
        let comparedPopulation = comparedSelection.population
        guard comparedPopulation.source == selectedPopulation.source,
              comparedSelection.fence == selectedSnapshot.fence,
              comparedSnapshot.mode == selectedMode,
              comparedSnapshot.policy_ref == selectedPopulation.policy_ref,
              comparedPopulation.policy_ref == selectedPopulation.policy_ref,
              comparedPopulation.market == selectedPopulation.market,
              detail.row.policy_ref == selectedPopulation.policy_ref,
              detail.row.period_placement == .inPeriod,
              let comparedTime = detail.row.decision_time,
              let selectedPeriodStart = DecisionAnalysisInstant.utc(selectedPopulation.period.start),
              let comparedPeriodStart = DecisionAnalysisInstant.utc(comparedPopulation.period.start),
              let comparedPeriodEnd = DecisionAnalysisInstant.utc(comparedPopulation.period.end) else {
            throw DecisionAnalysisServiceError.identityMismatch("previous source/session/mode/policy/market/fence")
        }
        if comparedSelection == selectedSnapshot {
            try validate(detail, expectedID: detail.row.decision_id, selection: selectedSnapshot)
        } else {
            guard let decisionTime = DecisionAnalysisInstant.utc(comparedTime),
                  decisionTime < selectedPeriodStart,
                  comparedPeriodStart == decisionTime,
                  comparedPeriodEnd == selectedPeriodStart,
                  validSelection(comparedSelection) else {
                throw DecisionAnalysisServiceError.identityMismatch("pre-period comparison bounds")
            }
            try validate(detail, expectedID: detail.row.decision_id, selection: comparedSelection)
        }
    }

    private static func isPrevious(
        _ candidate: DecisionAnalysisDecisionRow,
        than selected: DecisionAnalysisDecisionRow
    ) -> Bool {
        guard let candidateTime = candidate.decision_time.flatMap(DecisionAnalysisInstant.utc),
              let selectedTime = selected.decision_time.flatMap(DecisionAnalysisInstant.utc) else {
            return false
        }
        if candidateTime != selectedTime { return candidateTime < selectedTime }
        return identityPrecedes(candidate.decision_id, selected.decision_id)
    }

    private static func identityPrecedes(
        _ candidate: DecisionAnalysisDecisionID,
        _ selected: DecisionAnalysisDecisionID
    ) -> Bool {
        switch (candidate, selected) {
        case let (.paper(candidateSequence, candidateSignal), .paper(selectedSequence, selectedSignal)):
            if candidateSequence != selectedSequence { return candidateSequence < selectedSequence }
            return candidateSignal.lexicographicallyPrecedes(selectedSignal)
        case let (.automation(candidateIntent), .automation(selectedIntent)):
            return candidateIntent.lexicographicallyPrecedes(selectedIntent)
        default:
            return false
        }
    }

    private static func validPopulation(_ population: DecisionAnalysisPopulation) -> Bool {
        guard !population.source.sessionID.isEmpty,
              let start = DecisionAnalysisInstant.utc(population.period.start),
              let end = DecisionAnalysisInstant.utc(population.period.end), start < end,
              population.market.map({ !$0.isEmpty }) ?? true else { return false }
        return population.policy_ref.map(validPolicyReference) ?? false
    }

    private static func validateOverviewObservationScopes(
        _ observations: [DecisionAnalysisObservationCount],
        population: DecisionAnalysisPopulation
    ) throws {
        guard let selectedMarket = population.market else { return }
        guard !observations.contains(where: { observation in
            if case .market(let observedMarket) = observation.scope {
                return observedMarket != selectedMarket
            }
            return false
        }) else {
            throw DecisionAnalysisServiceError.identityMismatch("overview observation market scope")
        }
    }

    private static func validSelection(_ selection: DecisionAnalysisSelection) -> Bool {
        guard validPopulation(selection.population),
              selection.fence.family == selection.population.source.family else { return false }
        switch selection.fence {
        case .paper(let sequence): return sequence > 0
        case .automation(let digest): return isDigest(digest)
        }
    }

    private static func validPolicyReference(_ reference: DecisionAnalysisPolicyReference) -> Bool {
        !reference.policy_id.isEmpty && !reference.revision_id.isEmpty && isDigest(reference.definition_digest)
    }

    private static func validMode(_ mode: String, for source: DecisionAnalysisSource) -> Bool {
        switch source {
        case .paper: return mode == "SIMULATED"
        case .automation: return mode == "MOCK" || mode == "LIVE"
        }
    }

    private static func validDecisionID(_ decisionID: DecisionAnalysisDecisionID, for selection: DecisionAnalysisSelection) -> Bool {
        guard decisionID.family == selection.population.source.family else { return false }
        switch decisionID {
        case .paper(let sequence, let signalID):
            if case .paper(let asOfSequence) = selection.fence {
                return sequence > 0 && sequence <= asOfSequence && !signalID.isEmpty
            }
            return false
        case .automation(let intentID):
            if case .automation = selection.fence { return !intentID.isEmpty }
            return false
        }
    }

    private static func validTarget(_ target: DecisionAnalysisComparisonTarget, primary: DecisionAnalysisSelection) -> Bool {
        switch target {
        case .previous: return true
        case .selected(let selection, let decisionID):
            return selection.population.source.family == primary.population.source.family
                && validSelection(selection)
                && validDecisionID(decisionID, for: selection)
        }
    }

    private static func matches(_ row: DecisionAnalysisDecisionRow, filter: DecisionAnalysisFilter?) -> Bool {
        guard let filter else { return true }
        let reasonFilterMatches: Bool
        if filter.stage == nil && filter.reason_code == nil {
            reasonFilterMatches = true
        } else {
            reasonFilterMatches = row.reasons.contains { reason in
                (filter.stage == nil || filter.stage == reason.stage)
                    && (filter.reason_code == nil || filter.reason_code == reason.code)
            }
        }
        return (filter.strategy_action == nil || filter.strategy_action == row.strategy_action)
            && (filter.constrained_action == nil || filter.constrained_action == row.constrained_action)
            && (filter.execution_state == nil || filter.execution_state == row.execution_state)
            && reasonFilterMatches
    }

    private static func explanationPolicyReference(
        _ value: AutomationJSONValue?,
        matches expected: DecisionAnalysisPolicyReference
    ) -> Bool {
        guard let value else { return false }
        if case .null = value { return true }
        guard case .object(let fields) = value else { return false }
        return string(fields["policy_id"]) == expected.policy_id
            && string(fields["revision_id"]) == expected.revision_id
            && string(fields["definition_digest"]) == expected.definition_digest
    }

    private static func explanationTime(_ value: AutomationJSONValue?, matches expected: String?) -> Bool {
        guard let value else { return false }
        if case .null = value { return expected == nil }
        guard let value = string(value), let expected,
              let left = DecisionAnalysisInstant.utc(value),
              let right = DecisionAnalysisInstant.utc(expected) else { return false }
        return left == right
    }

    private static func string(_ value: AutomationJSONValue?) -> String? {
        guard let value, case .string(let string) = value else { return nil }
        return string
    }

    private static func isDigest(_ value: String) -> Bool {
        value.count == 64 && value.utf8.allSatisfy {
            ($0 >= 48 && $0 <= 57) || ($0 >= 97 && $0 <= 102)
        }
    }
}
