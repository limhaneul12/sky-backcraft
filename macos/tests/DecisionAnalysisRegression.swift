import AppKit
import Foundation

enum DecisionAnalysisFixtureFailure: Error, CustomStringConvertible {
    case assertion(String)

    var description: String {
        switch self {
        case .assertion(let message): message
        }
    }
}

struct DecisionAnalysisFixtureToolCall {
    let tool: String
    let action: String?
    let arguments: [String: Any]
}

final class DecisionAnalysisFixtureURLProtocol: URLProtocol {
    typealias Handler = (String, [String: Any]) -> [String: Any]?
    private static let lock = NSLock()
    private static var handler: Handler?
    private static var calls: [DecisionAnalysisFixtureToolCall] = []
    private static var cancellations = 0
    private let stateLock = NSLock()
    private var didFinish = false

    static func install(handler: @escaping Handler) {
        lock.lock()
        self.handler = handler
        calls = []
        cancellations = 0
        lock.unlock()
    }

    static func capturedToolCalls() -> [DecisionAnalysisFixtureToolCall] {
        lock.lock(); defer { lock.unlock() }
        return calls
    }

    static var cancellationCount: Int {
        lock.lock(); defer { lock.unlock() }
        return cancellations
    }

    override class func canInit(with request: URLRequest) -> Bool {
        request.url?.host == "127.0.0.1" && request.url?.path == "/desktop/mcp"
    }

    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        guard let body = Self.requestBody(request),
              let requestJSON = try? JSONSerialization.jsonObject(with: body) as? [String: Any],
              let method = requestJSON["method"] as? String else {
            fail("malformed JSON-RPC fixture request")
            return
        }
        let requestID = requestJSON["id"] ?? NSNull()
        if method == "initialize" {
            finish([
                "jsonrpc": "2.0",
                "id": requestID,
                "result": [
                    "protocolVersion": "2025-03-26",
                    "capabilities": ["tools": [:]],
                    "serverInfo": ["name": "decision-analysis-fixture", "version": "1"]
                ]
            ])
            return
        }
        guard method == "tools/call",
              let params = requestJSON["params"] as? [String: Any],
              let tool = params["name"] as? String,
              let arguments = params["arguments"] as? [String: Any] else {
            fail("unexpected JSON-RPC method or tool arguments")
            return
        }

        Self.lock.lock()
        let responder = Self.handler
        Self.calls.append(DecisionAnalysisFixtureToolCall(
            tool: tool,
            action: arguments["action"] as? String,
            arguments: arguments
        ))
        Self.lock.unlock()

        guard let responder, let result = responder(tool, arguments) else { return }
        finish(["jsonrpc": "2.0", "id": requestID, "result": result])
    }

    override func stopLoading() {
        stateLock.lock()
        let wasPending = !didFinish
        didFinish = true
        stateLock.unlock()
        if wasPending {
            Self.lock.lock()
            Self.cancellations += 1
            Self.lock.unlock()
        }
    }

    private func finish(_ payload: [String: Any]) {
        stateLock.lock()
        guard !didFinish else { stateLock.unlock(); return }
        didFinish = true
        stateLock.unlock()
        guard let data = try? JSONSerialization.data(withJSONObject: payload, options: [.fragmentsAllowed]),
              let url = request.url,
              let response = HTTPURLResponse(
                url: url,
                statusCode: 200,
                httpVersion: "HTTP/1.1",
                headerFields: ["Content-Type": "application/json"]
              ) else {
            fail("could not encode JSON-RPC fixture response")
            return
        }
        client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
        client?.urlProtocol(self, didLoad: data)
        client?.urlProtocolDidFinishLoading(self)
    }

    private func fail(_ message: String) {
        client?.urlProtocol(self, didFailWithError: DecisionAnalysisFixtureFailure.assertion(message))
    }

    private static func requestBody(_ request: URLRequest) -> Data? {
        if let body = request.httpBody { return body }
        guard let stream = request.httpBodyStream else { return nil }
        stream.open()
        defer { stream.close() }
        var data = Data()
        var buffer = [UInt8](repeating: 0, count: 4_096)
        while true {
            let count = buffer.withUnsafeMutableBufferPointer { bufferPointer -> Int in
                guard let baseAddress = bufferPointer.baseAddress else { return 0 }
                return stream.read(baseAddress, maxLength: bufferPointer.count)
            }
            if count < 0 { return nil }
            if count == 0 { break }
            data.append(contentsOf: buffer.prefix(count))
        }
        return data
    }
}

enum DecisionAnalysisFixtureWire {
    static let toolName = "decision_analysis_query"
    static let paperPolicy = DecisionAnalysisPolicyReference(
        policyID: "decision-analysis-policy",
        revisionID: "decision-analysis-revision",
        definitionDigest: String(repeating: "a", count: 64)
    )

    static func handler(
        mode: String,
        staleAction: String? = nil,
        outOfPeriodDecisionList: Bool = false,
        incompleteOverview: Bool = false,
        previousBeforePeriod: Bool = false,
        previousInPeriod: Bool = false,
        overviewPeriodStartOverride: String? = nil,
        conditionTraceOverride: [[String: Any]]? = nil,
        includeOtherMarketObservationForFilteredPopulation: Bool = false,
        heldAction: String? = nil
    ) -> DecisionAnalysisFixtureURLProtocol.Handler {
        { tool, arguments in
            guard tool == toolName, let action = arguments["action"] as? String else {
                return ["isError": true, "content": [["type": "text", "text": "unexpected fixture tool/action"]]]
            }
            if action == heldAction { return nil }
            let sourceAndPopulation: ([String: Any], [String: Any])
            if let population = arguments["population"] as? [String: Any],
               let source = population["source"] as? [String: Any] {
                sourceAndPopulation = (source, population)
            } else if let selection = arguments["selection"] as? [String: Any],
                      let population = selection["population"] as? [String: Any],
                      let source = population["source"] as? [String: Any] {
                sourceAndPopulation = (source, population)
            } else {
                return ["isError": true, "content": [["type": "text", "text": "missing population"]]]
            }

            let source = sourceAndPopulation.0
            let population = sourceAndPopulation.1
            let family = source["family"] as? String ?? "paper"
            let fence = family == "paper"
                ? ["family": "paper", "as_of_sequence": 10] as [String: Any]
                : ["family": "automation", "snapshot_digest": String(repeating: "b", count: 64)]
            var selection = (arguments["selection"] as? [String: Any]) ?? [
                "population": population,
                "fence": fence
            ]
            if staleAction == action {
                var changedFence = (selection["fence"] as? [String: Any]) ?? fence
                if family == "paper" {
                    changedFence["as_of_sequence"] = 11
                } else {
                    changedFence["snapshot_digest"] = String(repeating: "c", count: 64)
                }
                selection["fence"] = changedFence
            }

            let response: [String: Any]
            switch action {
            case "overview":
                var overviewSelection = selection
                if let overviewPeriodStartOverride,
                   var adjustedPopulation = overviewSelection["population"] as? [String: Any],
                   var adjustedPeriod = adjustedPopulation["period"] as? [String: Any] {
                    adjustedPeriod["start"] = overviewPeriodStartOverride
                    adjustedPopulation["period"] = adjustedPeriod
                    overviewSelection["population"] = adjustedPopulation
                }
                var value = overview(
                    selection: overviewSelection,
                    mode: mode,
                    family: family,
                    includeOtherMarketObservationForFilteredPopulation: includeOtherMarketObservationForFilteredPopulation
                )
                if incompleteOverview { value["population_complete"] = false }
                response = value
            case "decisions":
                response = decisions(
                    selection: selection,
                    mode: mode,
                    family: family,
                    arguments: arguments,
                    outOfPeriod: outOfPeriodDecisionList
                )
            case "decision":
                let decisionID = arguments["decision_id"] as? [String: Any] ?? baseDecisionID(family: family)
                response = decision(
                    selection: selection,
                    mode: mode,
                    family: family,
                    decisionID: decisionID,
                    conditionTraceOverride: conditionTraceOverride
                )
            case "compare":
                let decisionID = arguments["decision_id"] as? [String: Any] ?? baseDecisionID(family: family)
                let against = arguments["against"] as? [String: Any] ?? ["kind": "previous"]
                response = compare(
                    selection: selection,
                    mode: mode,
                    family: family,
                    decisionID: decisionID,
                    against: against,
                    previousBeforePeriod: previousBeforePeriod,
                    previousInPeriod: previousInPeriod
                )
            default:
                return ["isError": true, "content": [["type": "text", "text": "unexpected decision-analysis action"]]]
            }
            guard let encoded = try? JSONSerialization.data(withJSONObject: response, options: [.sortedKeys]),
                  let body = String(data: encoded, encoding: .utf8) else {
                return ["isError": true, "content": [["type": "text", "text": "could not encode fixture response"]]]
            }
            return ["isError": false, "content": [["type": "text", "text": body]]]
        }
    }

    static func fixtureEnvironment(
        mode: String,
        handler: @escaping DecisionAnalysisFixtureURLProtocol.Handler
    ) throws -> (NativeMCPClient, DecisionAnalysisMCPService, NativeMCPTransportContext) {
        DecisionAnalysisFixtureURLProtocol.install(handler: handler)
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [DecisionAnalysisFixtureURLProtocol.self]
        let client = NativeMCPClient(session: URLSession(configuration: configuration))
        guard let context = client.configure(port: 64_321, token: String(repeating: "a", count: 64)) else {
            throw DecisionAnalysisFixtureFailure.assertion("fixture MCP client did not configure")
        }
        _ = mode
        return (client, DecisionAnalysisMCPService(client: client), context)
    }

    static func population(
        family: String,
        sessionID: String,
        start: String = "2026-10-20T00:00:00Z",
        end: String = "2026-10-21T00:00:00Z",
        market: String? = nil
    ) -> DecisionAnalysisPopulation {
        let source: DecisionAnalysisSource = family == "automation"
            ? .automation(sessionID: sessionID)
            : .paper(sessionID: sessionID)
        return DecisionAnalysisPopulation(
            source: source,
            period: DecisionAnalysisPeriod(start: start, end: end),
            market: market,
            policyReference: paperPolicy
        )
    }

    static func context(family: String, sessionID: String, mode: String) -> DecisionAnalysisSessionContext {
        let source: DecisionAnalysisSource = family == "automation"
            ? .automation(sessionID: sessionID)
            : .paper(sessionID: sessionID)
        return DecisionAnalysisSessionContext(
            source: source,
            mode: mode,
            policyReference: paperPolicy,
            markets: ["KRW-BTC"]
        )
    }

    static func actualWireHandler(responseData: Data, expectedAction: String) -> DecisionAnalysisFixtureURLProtocol.Handler {
        let responseText = String(data: responseData, encoding: .utf8) ?? ""
        return { tool, arguments in
            guard tool == toolName, arguments["action"] as? String == expectedAction else {
                return ["isError": true, "content": [["type": "text", "text": "wire fixture action mismatch"]]]
            }
            return ["isError": false, "content": [["type": "text", "text": responseText]]]
        }
    }

    private static func overview(
        selection: [String: Any],
        mode: String,
        family: String,
        includeOtherMarketObservationForFilteredPopulation: Bool = false
    ) -> [String: Any] {
        let population = selection["population"] as? [String: Any] ?? [:]
        let policy = population["policy_ref"] as? [String: Any] ?? paperPolicy.arguments
        let observationsClock = family == "paper" ? "paper_source_or_finalized_time" : "automation_event_time"
        var observations: [[String: Any]] = [
            [
                "kind": "PREPARATION",
                "basis_time": "preparation_decision_time",
                "scope": ["kind": "market", "market": "KRW-BTC"],
                "condition_trace_capture": "PARTIAL_CAPTURE",
                "observations": 1,
                "unplaced": 0
            ],
            [
                "kind": "CANDLE_OBSERVED",
                "basis_time": observationsClock,
                "scope": ["kind": "market", "market": "KRW-ETH"],
                "condition_trace_capture": "NOT_DECISION_EVENT",
                "observations": 2,
                "unplaced": 0
            ],
            [
                "kind": "PAUSED",
                "basis_time": observationsClock,
                "scope": ["kind": "session_wide"],
                "condition_trace_capture": "NOT_DECISION_EVENT",
                "observations": 1,
                "unplaced": 1
            ],
            [
                "kind": "RECOVERY_BLOCKED",
                "basis_time": observationsClock,
                "scope": ["kind": "unknown"],
                "condition_trace_capture": "NOT_DECISION_EVENT",
                "observations": 1,
                "unplaced": 1
            ]
        ]
        if let selectedMarket = population["market"] as? String,
           !includeOtherMarketObservationForFilteredPopulation {
            observations = observations.filter { observation in
                guard let scope = observation["scope"] as? [String: Any],
                      scope["kind"] as? String == "market" else { return true }
                return scope["market"] as? String == selectedMarket
            }
        }
        return [
            "action": "overview",
            "schema_version": "1.0",
            "source_snapshot": snapshot(selection: selection, mode: mode, policy: policy),
            "population_complete": true,
            "totals": [
                "decisions": 1,
                "decisions_with_time": 1,
                "unplaced_decisions": 1,
                "strategy": ["buy": 1, "sell": 0, "hold": 0, "not_evaluated": 0, "not_captured": 0],
                "constrained": ["buy": 1, "sell": 0, "hold": 0, "not_evaluated": 0, "not_captured": 0],
                "execution": [
                    "not_evaluated": 0, "no_order": 0, "rejected": 0, "pending": 0,
                    "filled": 1, "partially_filled": 0, "cancelled": 0, "expired": 0, "not_captured": 0
                ],
                "orders": 1,
                "decisions_with_orders": 1,
                "fills": 1,
                "decisions_with_fills": 1,
                "condition_trace_capture": [
                    "captured": 1, "partial_capture": 0, "not_captured": 0, "not_decision_event": 1
                ],
                "reasons": [["stage": "STRATEGY", "code": "SAVED_REASON", "distinct_decisions": 1, "occurrences": 1]],
                "conditions": [[
                    "policy_ref": policy,
                    "scope": ["kind": "RULE_CONDITION", "rule_id": "entry"],
                    "ast_path": "rule:entry:when",
                    "role": "ENTRY",
                    "outcome": ["status": "FALSE"],
                    "occurrences": 2
                ]],
                "non_decision_observations": observations
            ],
            "measurement": ["elapsed_millis": 1, "response_bytes": 512]
        ]
    }

    private static func decisions(
        selection: [String: Any],
        mode: String,
        family: String,
        arguments: [String: Any],
        outOfPeriod: Bool
    ) -> [String: Any] {
        let population = selection["population"] as? [String: Any] ?? [:]
        let policy = population["policy_ref"] as? [String: Any] ?? paperPolicy.arguments
        let pageRequest = arguments["page"] as? [String: Any] ?? [:]
        let first = row(selection: selection, family: family, policy: policy, unplaced: false, outOfPeriod: outOfPeriod)
        let second = row(selection: selection, family: family, policy: policy, unplaced: true, outOfPeriod: false)
        return [
            "action": "decisions",
            "schema_version": "1.0",
            "source_snapshot": snapshot(selection: selection, mode: mode, policy: policy),
            "filter": arguments["filter"] ?? NSNull(),
            "page": [
                "offset": pageRequest["offset"] ?? 0,
                "limit": pageRequest["limit"] ?? 100,
                "returned_count": 2,
                "total_count": 2,
                "in_period_count": 1,
                "unplaced_count": 1,
                "next_offset": NSNull(),
                "items": [first, second]
            ],
            "measurement": ["elapsed_millis": 1, "response_bytes": 1024]
        ]
    }

    private static func decision(
        selection: [String: Any],
        mode: String,
        family: String,
        decisionID: [String: Any],
        conditionTraceOverride: [[String: Any]]? = nil
    ) -> [String: Any] {
        let population = selection["population"] as? [String: Any] ?? [:]
        let policy = population["policy_ref"] as? [String: Any] ?? paperPolicy.arguments
        let detail = detail(
            selection: selection,
            family: family,
            policy: policy,
            decisionID: decisionID,
            conditionTraceOverride: conditionTraceOverride
        )
        return [
            "action": "decision",
            "schema_version": "1.0",
            "source_snapshot": snapshot(selection: selection, mode: mode, policy: policy),
            "decision": detail,
            "measurement": ["elapsed_millis": 1, "response_bytes": 2048]
        ]
    }

    private static func compare(
        selection: [String: Any],
        mode: String,
        family: String,
        decisionID: [String: Any],
        against: [String: Any],
        previousBeforePeriod: Bool,
        previousInPeriod: Bool
    ) -> [String: Any] {
        let population = selection["population"] as? [String: Any] ?? [:]
        let policy = population["policy_ref"] as? [String: Any] ?? paperPolicy.arguments
        let isSelected = against["kind"] as? String == "selected"
        let selectedDetail = detail(
            selection: selection,
            family: family,
            policy: policy,
            decisionID: decisionID,
            decisionTimeOverride: previousInPeriod && !isSelected ? "2026-10-20T00:01:00Z" : nil
        )
        let targetSelection = against["selection"] as? [String: Any] ?? selection
        let targetPopulation = targetSelection["population"] as? [String: Any] ?? population
        let targetPolicy = targetPopulation["policy_ref"] as? [String: Any] ?? policy
        let targetID = against["decision_id"] as? [String: Any] ?? decisionID
        let comparedDetail: Any
        let comparedSnapshot: Any
        var unavailableReason: Any = NSNull()
        if isSelected {
            comparedDetail = detail(selection: targetSelection, family: family, policy: targetPolicy, decisionID: targetID)
            comparedSnapshot = snapshot(selection: targetSelection, mode: mode, policy: targetPolicy)
        } else if previousInPeriod {
            comparedDetail = detail(
                selection: selection,
                family: family,
                policy: policy,
                decisionID: baseDecisionID(family: family, previous: true),
                decisionTimeOverride: "2026-10-20T00:00:00Z"
            )
            comparedSnapshot = snapshot(selection: selection, mode: mode, policy: policy)
        } else if previousBeforePeriod,
                  let currentPeriod = population["period"] as? [String: Any],
                  let selectedStart = currentPeriod["start"] as? String,
                  let selectedStartDate = DecisionAnalysisInstant.utc(selectedStart) {
            let previousTime = ISO8601DateFormatter()
            previousTime.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
            previousTime.timeZone = TimeZone(secondsFromGMT: 0)
            let previousDate = Date(timeIntervalSince1970: Double(selectedStartDate.secondsSince1970 - 3_600))
            var previousPopulation = population
            previousPopulation["period"] = [
                "start": previousTime.string(from: previousDate),
                "end": selectedStart
            ]
            if let market = population["market"] as? String {
                previousPopulation["market"] = market
            } else {
                previousPopulation.removeValue(forKey: "market")
            }
            let previousSelection: [String: Any] = [
                "population": previousPopulation,
                "fence": selection["fence"] ?? [:]
            ]
            let previousID = baseDecisionID(family: family, unplaced: false, previous: true)
            comparedDetail = detail(
                selection: previousSelection,
                family: family,
                policy: targetPolicy,
                decisionID: previousID
            )
            comparedSnapshot = snapshot(selection: previousSelection, mode: mode, policy: targetPolicy)
        } else {
            comparedDetail = NSNull()
            comparedSnapshot = NSNull()
            unavailableReason = "no_previous_decision"
        }
        return [
            "action": "compare",
            "schema_version": "1.0",
            "selected_source_snapshot": snapshot(selection: selection, mode: mode, policy: policy),
            "compared_source_snapshot": comparedSnapshot,
            "comparison": [
                "kind": isSelected ? "selected" : "previous",
                "selected": selectedDetail,
                "compared": comparedDetail,
                "unavailable_reason": unavailableReason,
                "changed_fields": isSelected ? ["reasons"] : (previousBeforePeriod ? ["strategy_action"] : [])
            ],
            "measurement": ["elapsed_millis": 1, "response_bytes": 4096]
        ]
    }

    private static func detail(
        selection: [String: Any],
        family: String,
        policy: [String: Any],
        decisionID: [String: Any],
        conditionTraceOverride: [[String: Any]]? = nil,
        decisionTimeOverride: String? = nil
    ) -> [String: Any] {
        var decisionRow = row(
            selection: selection,
            family: family,
            policy: policy,
            unplaced: isUnplaced(decisionID, family: family),
            decisionID: decisionID,
            outOfPeriod: false
        )
        if let decisionTimeOverride { decisionRow["decision_time"] = decisionTimeOverride }
        let time = decisionRow["decision_time"] ?? NSNull()
        let explanation: [String: Any] = [
            "schema_version": "1.0",
            "policy_ref": policy,
            "decision_time": time,
            "strategy_action": decisionRow["strategy_action"] ?? "BUY",
            "constrained_action": decisionRow["constrained_action"] ?? "BUY",
            "execution_state": decisionRow["execution_state"] ?? "FILLED",
            "actual_weight_at_decision": 0.125,
            "raw_target_weight": 0.250,
            "constrained_target_weight": 0.0,
            "matched_rule_id": "entry",
            "condition_trace_capture": "CAPTURED",
            "condition_trace": conditionTraceOverride ?? (0..<32).map { index in
                [
                    "scope": ["kind": "RULE_CONDITION", "rule_id": "entry"],
                    "ast_path": "rule:entry:when/condition:\(index)",
                    "role": "ENTRY",
                    "outcome": index == 4
                        ? ["status": "UNKNOWN", "reason": "CROSS_HISTORY_UNAVAILABLE"]
                        : (index == 5
                            ? ["UNKNOWN": "CROSS_HISTORY_UNAVAILABLE"]
                            : (index % 3 == 0
                                ? ["status": "FALSE"]
                                : (index % 3 == 1
                                    ? ["status": "UNKNOWN", "reason": "CROSS_HISTORY_UNAVAILABLE"]
                                    : ["status": "NOT_EVALUATED", "reason": "PRIOR_RULE_MATCHED"]))),
                    "engine_result": index % 3 == 2 ? NSNull() as Any : false as Any,
                    "evidence": index == 4
                        ? [
                            "kind": "CROSSING",
                            "previous": NSNull(),
                            "current": ["left": 0.75, "right": 0.5]
                        ] as [String: Any]
                        : [
                            "kind": "COMPARISON",
                            "left": "101.2500000000000000000000001",
                            "comparison": "GT",
                            "right": "99.8750000000000000000000001"
                        ] as [String: Any]
                ] as [String: Any]
            },
            "reasons": decisionRow["reasons"] ?? [],
            "orders": [["order_id": "fixture-order", "state": "FILLED", "reason": NSNull()]],
            "fills": [["fill_id": "fixture-fill", "order_id": "fixture-order"]]
        ]
        return ["row": decisionRow, "explanation": explanation]
    }

    private static func row(
        selection: [String: Any],
        family: String,
        policy: [String: Any],
        unplaced: Bool,
        decisionID suppliedID: [String: Any]? = nil,
        outOfPeriod: Bool
    ) -> [String: Any] {
        let population = selection["population"] as? [String: Any] ?? [:]
        let market = population["market"] as? String ?? "KRW-BTC"
        let id = suppliedID ?? baseDecisionID(family: family, unplaced: unplaced)
        let time: Any
        if unplaced {
            time = NSNull()
        } else if outOfPeriod,
                  let period = population["period"] as? [String: Any], let end = period["end"] {
            time = end
        } else {
            time = (population["period"] as? [String: Any])?["start"] ?? "2026-10-20T00:00:00Z"
        }
        return [
            "decision_id": id,
            "market": market,
            "decision_time": time,
            "period_placement": unplaced ? "unplaced" : "in_period",
            "policy_ref": policy,
            "saved_fact_digest": String(repeating: "d", count: 64),
            "strategy_action": "BUY",
            "constrained_action": "BUY",
            "execution_state": "FILLED",
            "reasons": [["stage": "STRATEGY", "code": "SAVED_REASON", "label_ko": "저장 이유"]]
        ]
    }

    private static func baseDecisionID(family: String, unplaced: Bool = false, previous: Bool = false) -> [String: Any] {
        if family == "automation" {
            return ["family": "automation", "intent_id": unplaced ? "intent-unplaced" : (previous ? "intent-previous" : "intent-fixture")]
        }
        return ["family": "paper", "event_sequence": unplaced ? 10 : (previous ? 8 : 9), "signal_id": unplaced ? "signal-unplaced" : (previous ? "signal-previous" : "signal-fixture")]
    }

    private static func isUnplaced(_ id: [String: Any], family: String) -> Bool {
        if family == "automation" { return (id["intent_id"] as? String)?.contains("unplaced") == true }
        return (id["event_sequence"] as? UInt64) == 10 || (id["event_sequence"] as? Int) == 10
    }

    private static func snapshot(selection: [String: Any], mode: String, policy: [String: Any]) -> [String: Any] {
        ["selection": selection, "mode": mode, "policy_ref": policy]
    }
}

final class DecisionAnalysisFixtureResultBox<Value> {
    var value: Result<Value, Error>?
}

enum DecisionAnalysisFixtureAssertions {
    static func waitUntil(_ label: String, timeout: TimeInterval = 5, condition: () -> Bool) throws {
        let deadline = Date().addingTimeInterval(timeout)
        while !condition(), Date() < deadline {
            _ = RunLoop.main.run(mode: .default, before: Date().addingTimeInterval(0.01))
        }
        guard condition() else { throw DecisionAnalysisFixtureFailure.assertion("timed out waiting for \(label)") }
    }

    static func expect(_ condition: @autoclosure () -> Bool, _ message: String) throws {
        guard condition() else { throw DecisionAnalysisFixtureFailure.assertion(message) }
    }
}

@main
private enum DecisionAnalysisRegression {
    static func main() throws {
        _ = NSApplication.shared
        if CommandLine.arguments.contains("--gui-appearance") {
            try DecisionAnalysisGUIFixture.runAppearanceDemo()
            return
        }
        try verifyPaperWireJourney()
        try verifyObservationMarketScopeIdentity()
        try verifyAutomationSnapshotAndPeriodFences()
        try verifyTransportContextAndCancellation()
        try verifyPrePeriodPreviousComparisonContext()
        try verifyOrdinaryAllMarketsPreviousComparisonContext()
        try verifyExactTimestampIdentity()
        try verifyConditionOutcomeWireDecoding()
        guard let wireDirectory = ProcessInfo.processInfo.environment["SKY_BACKCRAFT_DECISION_ANALYSIS_WIRE_DIRECTORY"] else {
            throw DecisionAnalysisFixtureFailure.assertion("actual Rust decision-analysis wire directory is required")
        }
        try verifyRustWireFixtures(at: URL(fileURLWithPath: wireDirectory, isDirectory: true))
        print("PASS actual Rust decision-analysis wire fixtures through the native MCP service")
        try DecisionAnalysisGUIFixture.runLayoutRegressions()
        print("PASS native decision-analysis wire, identity, snapshot, period, and cancellation regressions")
        print("PASS native decision-analysis window layout, accessibility, glyph, resize, privacy, and stale-snapshot regressions")
    }

    private static func verifyPaperWireJourney() throws {
        let environment = try DecisionAnalysisFixtureWire.fixtureEnvironment(
            mode: "SIMULATED",
            handler: DecisionAnalysisFixtureWire.handler(mode: "SIMULATED")
        )
        let population = DecisionAnalysisFixtureWire.population(family: "paper", sessionID: "paper-fixture")

        let overviewBox = DecisionAnalysisFixtureResultBox<(DecisionAnalysisSourceSnapshot, Bool, DecisionAnalysisTotals, DecisionAnalysisMeasurement)>()
        let overviewRequest = environment.1.overview(
            population: population,
            expectedMode: "SIMULATED",
            expectedTransportContext: environment.2
        ) { overviewBox.value = $0 }
        try DecisionAnalysisFixtureAssertions.waitUntil("PAPER overview response") { overviewBox.value != nil }
        let overview = try overviewBox.value!.get()
        try DecisionAnalysisFixtureAssertions.expect(overviewRequest.isPending == false, "overview request should finish")
        try DecisionAnalysisFixtureAssertions.expect(overview.0.selection.population.source == population.source, "overview source identity must match the selected paper session")
        guard case .paper(let asOfSequence) = overview.0.selection.fence else {
            throw DecisionAnalysisFixtureFailure.assertion("PAPER overview must return its sequence fence")
        }
        try DecisionAnalysisFixtureAssertions.expect(asOfSequence == 10 && overview.1, "PAPER snapshot fence and complete period flag must be preserved")
        try DecisionAnalysisFixtureAssertions.expect(overview.2.decisions == 1 && overview.2.unplaced_decisions == 1, "period total and unplaced coverage must remain separate")
        try DecisionAnalysisFixtureAssertions.expect(overview.2.strategy.buy == 1 && overview.2.execution.filled == 1, "strategy BUY and filled counts must be distinct saved facts")
        try DecisionAnalysisFixtureAssertions.expect(overview.2.conditions.first?.outcome.state == "FALSE",
                                                     "tagged condition outcome state must preserve FALSE")

        let filter = DecisionAnalysisFilter(strategyAction: "BUY")
        let pageBox = DecisionAnalysisFixtureResultBox<(DecisionAnalysisPage, DecisionAnalysisMeasurement)>()
        _ = environment.1.decisions(
            selection: overview.0.selection,
            page: (offset: 0, limit: 100),
            filter: filter,
            expectedMode: "SIMULATED",
            expectedTransportContext: environment.2
        ) { pageBox.value = $0 }
        try DecisionAnalysisFixtureAssertions.waitUntil("PAPER decision list response") { pageBox.value != nil }
        let page = try pageBox.value!.get().0
        try DecisionAnalysisFixtureAssertions.expect(page.total_count == 2 && page.in_period_count == 1 && page.unplaced_count == 1,
                   "decision list must separate period rows from unplaced rows")
        try DecisionAnalysisFixtureAssertions.expect(page.items[1].period_placement == .unplaced && page.items[1].decision_time == nil,
                   "unplaced rows must not receive an invented decision time")
        let decisionCall = DecisionAnalysisFixtureURLProtocol.capturedToolCalls().last
        try DecisionAnalysisFixtureAssertions.expect(decisionCall?.tool == DecisionAnalysisFixtureWire.toolName
                    && decisionCall?.action == "decisions"
                    && (decisionCall?.arguments["filter"] as? [String: Any])?["strategy_action"] as? String == "BUY",
                   "summary action selection must use the typed exact strategy_action filter")

        let detailBox = DecisionAnalysisFixtureResultBox<(DecisionAnalysisDecisionDetail, DecisionAnalysisMeasurement)>()
        _ = environment.1.decision(
            selection: overview.0.selection,
            decisionID: page.items[0].decision_id,
            expectedMode: "SIMULATED",
            expectedTransportContext: environment.2
        ) { detailBox.value = $0 }
        try DecisionAnalysisFixtureAssertions.waitUntil("PAPER original decision detail") { detailBox.value != nil }
        let detail = try detailBox.value!.get().0
        try DecisionAnalysisFixtureAssertions.expect(detail.row.decision_id == page.items[0].decision_id, "detail must match the selected durable decision identity")
        try DecisionAnalysisFixtureAssertions.expect(detail.explanation.formattedJSON.contains("FALSE")
                    && detail.explanation.formattedJSON.contains("NOT_EVALUATED"),
                   "original explanation must preserve FALSE and NOT_EVALUATED condition evidence")
        try DecisionAnalysisFixtureAssertions.expect(detail.explanation.formattedJSON.contains("101.25"), "stored numeric operands must remain visible without manual rounding")

        let compareBox = DecisionAnalysisFixtureResultBox<(DecisionAnalysisComparison, DecisionAnalysisSourceSnapshot?, DecisionAnalysisMeasurement)>()
        _ = environment.1.compare(
            selection: overview.0.selection,
            decisionID: page.items[0].decision_id,
            against: .previous,
            expectedMode: "SIMULATED",
            expectedTransportContext: environment.2
        ) { compareBox.value = $0 }
        try DecisionAnalysisFixtureAssertions.waitUntil("PAPER previous comparison") { compareBox.value != nil }
        let comparison = try compareBox.value!.get().0
        try DecisionAnalysisFixtureAssertions.expect(comparison.kind == "previous" && comparison.compared == nil
                    && comparison.unavailable_reason == .noPreviousDecision,
                   "missing previous decision must remain explicitly unavailable")
        try DecisionAnalysisFixtureAssertions.expect(DecisionAnalysisFixtureURLProtocol.capturedToolCalls().allSatisfy {
            $0.tool == DecisionAnalysisFixtureWire.toolName
        }, "native analysis must use only the dedicated read projection tool")
        environment.0.disconnect()
    }

    private static func verifyObservationMarketScopeIdentity() throws {
        let environment = try DecisionAnalysisFixtureWire.fixtureEnvironment(
            mode: "SIMULATED",
            handler: DecisionAnalysisFixtureWire.handler(
                mode: "SIMULATED",
                includeOtherMarketObservationForFilteredPopulation: true
            )
        )
        let population = DecisionAnalysisFixtureWire.population(
            family: "paper",
            sessionID: "paper-observation-market-identity",
            market: "KRW-BTC"
        )
        let result = DecisionAnalysisFixtureResultBox<(DecisionAnalysisSourceSnapshot, Bool, DecisionAnalysisTotals, DecisionAnalysisMeasurement)>()
        _ = environment.1.overview(
            population: population,
            expectedMode: "SIMULATED",
            expectedTransportContext: environment.2
        ) { result.value = $0 }
        try DecisionAnalysisFixtureAssertions.waitUntil("cross-market overview observation response") {
            result.value != nil
        }
        guard case .failure(let error)? = result.value,
              case DecisionAnalysisServiceError.identityMismatch(let field) = error,
              field == "overview observation market scope" else {
            throw DecisionAnalysisFixtureFailure.assertion(
                "a KRW-BTC population containing KRW-ETH-scoped observations must fail with an identity mismatch"
            )
        }
        environment.0.disconnect()
    }

    private static func verifyAutomationSnapshotAndPeriodFences() throws {
        let staleEnvironment = try DecisionAnalysisFixtureWire.fixtureEnvironment(
            mode: "LIVE",
            handler: DecisionAnalysisFixtureWire.handler(mode: "LIVE", staleAction: "decisions")
        )
        let population = DecisionAnalysisFixtureWire.population(family: "automation", sessionID: "automation-live-fixture")
        let overviewBox = DecisionAnalysisFixtureResultBox<(DecisionAnalysisSourceSnapshot, Bool, DecisionAnalysisTotals, DecisionAnalysisMeasurement)>()
        _ = staleEnvironment.1.overview(
            population: population,
            expectedMode: "LIVE",
            expectedTransportContext: staleEnvironment.2
        ) { overviewBox.value = $0 }
        try DecisionAnalysisFixtureAssertions.waitUntil("LIVE automation overview") { overviewBox.value != nil }
        let snapshot = try overviewBox.value!.get().0
        guard case .automation(let digest) = snapshot.selection.fence else {
            throw DecisionAnalysisFixtureFailure.assertion("LIVE automation overview must return a digest fence")
        }
        try DecisionAnalysisFixtureAssertions.expect(digest == String(repeating: "b", count: 64), "automation digest fence must be retained exactly")

        let stalePageBox = DecisionAnalysisFixtureResultBox<(DecisionAnalysisPage, DecisionAnalysisMeasurement)>()
        _ = staleEnvironment.1.decisions(
            selection: snapshot.selection,
            page: (offset: 0, limit: 100),
            filter: nil,
            expectedMode: "LIVE",
            expectedTransportContext: staleEnvironment.2
        ) { stalePageBox.value = $0 }
        try DecisionAnalysisFixtureAssertions.waitUntil("stale LIVE list response") { stalePageBox.value != nil }
        guard case .failure(let staleError) = stalePageBox.value! else {
            throw DecisionAnalysisFixtureFailure.assertion("changed automation digest must reject follow-up rows")
        }
        guard case DecisionAnalysisServiceError.identityMismatch = staleError else {
            throw DecisionAnalysisFixtureFailure.assertion("changed automation digest must report identity mismatch")
        }
        staleEnvironment.0.disconnect()

        let periodEnvironment = try DecisionAnalysisFixtureWire.fixtureEnvironment(
            mode: "SIMULATED",
            handler: DecisionAnalysisFixtureWire.handler(mode: "SIMULATED", outOfPeriodDecisionList: true)
        )
        let paperPopulation = DecisionAnalysisFixtureWire.population(family: "paper", sessionID: "paper-period-fixture")
        let paperOverview = DecisionAnalysisFixtureResultBox<(DecisionAnalysisSourceSnapshot, Bool, DecisionAnalysisTotals, DecisionAnalysisMeasurement)>()
        _ = periodEnvironment.1.overview(
            population: paperPopulation,
            expectedMode: "SIMULATED",
            expectedTransportContext: periodEnvironment.2
        ) { paperOverview.value = $0 }
        try DecisionAnalysisFixtureAssertions.waitUntil("PAPER period overview") { paperOverview.value != nil }
        let paperSnapshot = try paperOverview.value!.get().0
        let outOfPeriodPage = DecisionAnalysisFixtureResultBox<(DecisionAnalysisPage, DecisionAnalysisMeasurement)>()
        _ = periodEnvironment.1.decisions(
            selection: paperSnapshot.selection,
            page: (offset: 0, limit: 100),
            filter: nil,
            expectedMode: "SIMULATED",
            expectedTransportContext: periodEnvironment.2
        ) { outOfPeriodPage.value = $0 }
        try DecisionAnalysisFixtureAssertions.waitUntil("out-of-period row response") { outOfPeriodPage.value != nil }
        guard case .failure(let periodError) = outOfPeriodPage.value! else {
            throw DecisionAnalysisFixtureFailure.assertion("period-end row must not be labeled in-period")
        }
        guard case DecisionAnalysisServiceError.identityMismatch = periodError else {
            throw DecisionAnalysisFixtureFailure.assertion("out-of-period placement must reject the response")
        }
        periodEnvironment.0.disconnect()
    }

    private static func verifyTransportContextAndCancellation() throws {
        let heldEnvironment = try DecisionAnalysisFixtureWire.fixtureEnvironment(
            mode: "SIMULATED",
            handler: DecisionAnalysisFixtureWire.handler(mode: "SIMULATED", heldAction: "overview")
        )
        let population = DecisionAnalysisFixtureWire.population(family: "paper", sessionID: "paper-cancel-fixture")
        let cancelledBox = DecisionAnalysisFixtureResultBox<(DecisionAnalysisSourceSnapshot, Bool, DecisionAnalysisTotals, DecisionAnalysisMeasurement)>()
        let request = heldEnvironment.1.overview(
            population: population,
            expectedMode: "SIMULATED",
            expectedTransportContext: heldEnvironment.2
        ) { cancelledBox.value = $0 }
        try DecisionAnalysisFixtureAssertions.waitUntil("held overview request") {
            DecisionAnalysisFixtureURLProtocol.capturedToolCalls().contains { $0.action == "overview" }
        }
        request.cancel()
        try DecisionAnalysisFixtureAssertions.waitUntil("cancelled overview callback") { cancelledBox.value != nil }
        try DecisionAnalysisFixtureAssertions.expect(!request.isPending, "canceled request must be terminal")
        guard case .failure(let cancellationError) = cancelledBox.value!,
              let nativeError = cancellationError as? NativeMCPError else {
            throw DecisionAnalysisFixtureFailure.assertion("cancellation callback must remain a read cancellation")
        }
        guard case .cancelled = nativeError else {
            throw DecisionAnalysisFixtureFailure.assertion("cancellation must not become a successful analysis")
        }
        try DecisionAnalysisFixtureAssertions.waitUntil("URLProtocol cancellation") { DecisionAnalysisFixtureURLProtocol.cancellationCount > 0 }

        let oldContext = heldEnvironment.2
        guard heldEnvironment.0.configure(port: 64_322, token: String(repeating: "b", count: 64)) != nil else {
            throw DecisionAnalysisFixtureFailure.assertion("replacement transport fixture did not configure")
        }
        let before = DecisionAnalysisFixtureURLProtocol.capturedToolCalls().count
        let staleContextBox = DecisionAnalysisFixtureResultBox<(DecisionAnalysisSourceSnapshot, Bool, DecisionAnalysisTotals, DecisionAnalysisMeasurement)>()
        _ = heldEnvironment.1.overview(
            population: population,
            expectedMode: "SIMULATED",
            expectedTransportContext: oldContext
        ) { staleContextBox.value = $0 }
        guard case .failure(let contextError) = staleContextBox.value!,
              let nativeContextError = contextError as? NativeMCPError else {
            throw DecisionAnalysisFixtureFailure.assertion("old transport context must fail closed")
        }
        guard case .transportContextChanged = nativeContextError else {
            throw DecisionAnalysisFixtureFailure.assertion("old transport context must report a context change")
        }
        try DecisionAnalysisFixtureAssertions.expect(DecisionAnalysisFixtureURLProtocol.capturedToolCalls().count == before,
                   "stale transport context must not dispatch another MCP query")
        heldEnvironment.0.disconnect()
    }

    private static func verifyPrePeriodPreviousComparisonContext() throws {
        let environment = try DecisionAnalysisFixtureWire.fixtureEnvironment(
            mode: "SIMULATED",
            handler: DecisionAnalysisFixtureWire.handler(mode: "SIMULATED", previousBeforePeriod: true)
        )
        let population = DecisionAnalysisFixtureWire.population(family: "paper", sessionID: "paper-pre-period-previous")
        let overviewBox = DecisionAnalysisFixtureResultBox<(DecisionAnalysisSourceSnapshot, Bool, DecisionAnalysisTotals, DecisionAnalysisMeasurement)>()
        _ = environment.1.overview(
            population: population,
            expectedMode: "SIMULATED",
            expectedTransportContext: environment.2
        ) { overviewBox.value = $0 }
        try DecisionAnalysisFixtureAssertions.waitUntil("pre-period previous overview") { overviewBox.value != nil }
        let selection = try overviewBox.value!.get().0.selection
        let selectedID = DecisionAnalysisDecisionID.paper(eventSequence: 9, signalID: "signal-fixture")
        let compareBox = DecisionAnalysisFixtureResultBox<(DecisionAnalysisComparison, DecisionAnalysisSourceSnapshot?, DecisionAnalysisMeasurement)>()
        _ = environment.1.compare(
            selection: selection,
            decisionID: selectedID,
            against: .previous,
            expectedMode: "SIMULATED",
            expectedTransportContext: environment.2
        ) { compareBox.value = $0 }
        try DecisionAnalysisFixtureAssertions.waitUntil("pre-period previous comparison") { compareBox.value != nil }
        let value = try compareBox.value!.get()
        guard let compared = value.0.compared, let comparedSnapshot = value.1,
              let comparedTime = compared.row.decision_time,
              let comparedDate = DecisionAnalysisInstant.utc(comparedTime),
              let currentPeriodStart = DecisionAnalysisInstant.utc(selection.population.period.start),
              let currentPeriodEnd = DecisionAnalysisInstant.utc(selection.population.period.end),
              let previousPeriodStart = DecisionAnalysisInstant.utc(comparedSnapshot.selection.population.period.start),
              let previousPeriodEnd = DecisionAnalysisInstant.utc(comparedSnapshot.selection.population.period.end) else {
            throw DecisionAnalysisFixtureFailure.assertion("pre-period previous comparison must return its independent fence context")
        }
        try DecisionAnalysisFixtureAssertions.expect(comparedDate < currentPeriodStart
                                                       && previousPeriodStart == comparedDate
                                                       && previousPeriodEnd == currentPeriodStart
                                                       && currentPeriodStart < currentPeriodEnd
                                                       && comparedSnapshot.selection.population.market == selection.population.market
                                                       && compared.row.market == value.0.selected.row.market
                                                       && comparedSnapshot.selection.fence == selection.fence,
                                                     "pre-period previous context must be bounded, preserve the nil market filter, and match the selected row market/fence")
        try DecisionAnalysisFixtureAssertions.expect(value.0.selected.row.decision_id == selectedID
                                                       && compared.row.period_placement == .inPeriod,
                                                     "pre-period predecessor must not replace the selected decision or its period context")
        environment.0.disconnect()
    }

    private static func verifyOrdinaryAllMarketsPreviousComparisonContext() throws {
        let environment = try DecisionAnalysisFixtureWire.fixtureEnvironment(
            mode: "SIMULATED",
            handler: DecisionAnalysisFixtureWire.handler(mode: "SIMULATED", previousInPeriod: true)
        )
        let population = DecisionAnalysisFixtureWire.population(
            family: "paper",
            sessionID: "paper-all-markets-previous"
        )
        let overviewBox = DecisionAnalysisFixtureResultBox<(DecisionAnalysisSourceSnapshot, Bool, DecisionAnalysisTotals, DecisionAnalysisMeasurement)>()
        _ = environment.1.overview(
            population: population,
            expectedMode: "SIMULATED",
            expectedTransportContext: environment.2
        ) { overviewBox.value = $0 }
        try DecisionAnalysisFixtureAssertions.waitUntil("all-markets previous overview") { overviewBox.value != nil }
        let selection = try overviewBox.value!.get().0.selection
        try DecisionAnalysisFixtureAssertions.expect(selection.population.market == nil,
                                                     "ordinary previous test must use the all-markets population")

        let selectedID = DecisionAnalysisDecisionID.paper(eventSequence: 9, signalID: "signal-fixture")
        let comparisonBox = DecisionAnalysisFixtureResultBox<(DecisionAnalysisComparison, DecisionAnalysisSourceSnapshot?, DecisionAnalysisMeasurement)>()
        _ = environment.1.compare(
            selection: selection,
            decisionID: selectedID,
            against: .previous,
            expectedMode: "SIMULATED",
            expectedTransportContext: environment.2
        ) { comparisonBox.value = $0 }
        try DecisionAnalysisFixtureAssertions.waitUntil("all-markets ordinary previous comparison") {
            comparisonBox.value != nil
        }
        let value = try comparisonBox.value!.get()
        guard let compared = value.0.compared, let comparedSnapshot = value.1,
              let comparedTime = compared.row.decision_time.flatMap(DecisionAnalysisInstant.utc),
              let selectedTime = value.0.selected.row.decision_time.flatMap(DecisionAnalysisInstant.utc) else {
            throw DecisionAnalysisFixtureFailure.assertion("all-markets previous comparison must include both saved rows")
        }
        try DecisionAnalysisFixtureAssertions.expect(
            value.0.kind == "previous"
                && value.0.unavailable_reason == nil
                && comparedSnapshot.selection == selection
                && comparedSnapshot.selection.population.market == nil
                && compared.row.period_placement == .inPeriod
                && compared.row.market == value.0.selected.row.market
                && comparedTime < selectedTime,
            "ordinary previous must preserve the nil market filter while matching row market and time ordering"
        )
        environment.0.disconnect()
    }

    private static func verifyConditionOutcomeWireDecoding() throws {
        let cases: [([String: Any], DecisionAnalysisConditionOutcome)] = [
            (["status": "TRUE"], .evaluatedTrue),
            (["status": "FALSE"], .evaluatedFalse),
            (["status": "UNKNOWN", "reason": "CROSS_HISTORY_UNAVAILABLE"], .unknown(.crossHistoryUnavailable)),
            (["status": "NOT_EVALUATED", "reason": "PRIOR_RULE_MATCHED"], .notEvaluated(.priorRuleMatched))
        ]
        for (wireValue, expected) in cases {
            let data = try JSONSerialization.data(withJSONObject: wireValue)
            let decoded = try JSONDecoder().decode(DecisionAnalysisConditionOutcome.self, from: data)
            try DecisionAnalysisFixtureAssertions.expect(decoded == expected,
                                                         "tagged Rust condition outcome must preserve \(wireValue)")
        }

        let invalidCases: [[String: Any]] = [
            ["status": "TRUE", "reason": "RULE_NOT_MATCHED"],
            ["status": "UNKNOWN"],
            ["status": "NOT_EVALUATED", "reason": "UNKNOWN_REASON"],
            ["state": "FALSE"],
            ["FALSE": NSNull()]
        ]
        for wireValue in invalidCases {
            let data = try JSONSerialization.data(withJSONObject: wireValue)
            try DecisionAnalysisFixtureAssertions.expect(
                (try? JSONDecoder().decode(DecisionAnalysisConditionOutcome.self, from: data)) == nil,
                "non-contract condition outcome must fail closed: \(wireValue)"
            )
        }

        let taggedCondition = [
            "policy_ref": [
                "policy_id": "condition-policy",
                "revision_id": "condition-revision",
                "definition_digest": String(repeating: "a", count: 64)
            ],
            "scope": ["kind": "RULE_CONDITION", "rule_id": "entry"],
            "ast_path": "rule:entry:when",
            "role": "ENTRY",
            "outcome": ["status": "UNKNOWN", "reason": "CROSS_HISTORY_UNAVAILABLE"],
            "occurrences": 3
        ] as [String: Any]
        let countData = try JSONSerialization.data(withJSONObject: taggedCondition)
        let count = try JSONDecoder().decode(DecisionAnalysisConditionCount.self, from: countData)
        try DecisionAnalysisFixtureAssertions.expect(count.outcome == .unknown(.crossHistoryUnavailable)
                                                       && count.outcome.reason == "CROSS_HISTORY_UNAVAILABLE",
                                                      "overview condition outcome must retain its tagged state and reason")
    }

    private static func verifyExactTimestampIdentity() throws {
        let expected = DecisionAnalysisPeriod(start: "2026-10-20T00:00:00Z", end: "2026-10-21T00:00:00Z")
        let zeroFraction = DecisionAnalysisPeriod(start: "2026-10-20T00:00:00.000Z", end: "2026-10-21T00:00:00.000000000Z")
        let oneNanosecondLater = DecisionAnalysisPeriod(start: "2026-10-20T00:00:00.000000001Z", end: "2026-10-21T00:00:00Z")
        try DecisionAnalysisFixtureAssertions.expect(expected.matches(zeroFraction),
                                                     "equivalent UTC instants with zero fractional seconds must share identity")
        try DecisionAnalysisFixtureAssertions.expect(!expected.matches(oneNanosecondLater),
                                                     "a one-nanosecond difference must remain distinct")

        let acceptedEnvironment = try DecisionAnalysisFixtureWire.fixtureEnvironment(
            mode: "SIMULATED",
            handler: DecisionAnalysisFixtureWire.handler(
                mode: "SIMULATED",
                overviewPeriodStartOverride: "2026-10-20T00:00:00.000Z"
            )
        )
        let population = DecisionAnalysisFixtureWire.population(family: "paper", sessionID: "paper-period-exactness")
        let accepted = DecisionAnalysisFixtureResultBox<(DecisionAnalysisSourceSnapshot, Bool, DecisionAnalysisTotals, DecisionAnalysisMeasurement)>()
        _ = acceptedEnvironment.1.overview(
            population: population,
            expectedMode: "SIMULATED",
            expectedTransportContext: acceptedEnvironment.2
        ) { accepted.value = $0 }
        try DecisionAnalysisFixtureAssertions.waitUntil("equivalent fractional timestamp response") { accepted.value != nil }
        _ = try accepted.value!.get()
        acceptedEnvironment.0.disconnect()

        let rejectedEnvironment = try DecisionAnalysisFixtureWire.fixtureEnvironment(
            mode: "SIMULATED",
            handler: DecisionAnalysisFixtureWire.handler(
                mode: "SIMULATED",
                overviewPeriodStartOverride: "2026-10-20T00:00:00.000000001Z"
            )
        )
        let rejected = DecisionAnalysisFixtureResultBox<(DecisionAnalysisSourceSnapshot, Bool, DecisionAnalysisTotals, DecisionAnalysisMeasurement)>()
        _ = rejectedEnvironment.1.overview(
            population: population,
            expectedMode: "SIMULATED",
            expectedTransportContext: rejectedEnvironment.2
        ) { rejected.value = $0 }
        try DecisionAnalysisFixtureAssertions.waitUntil("one-nanosecond timestamp mismatch") { rejected.value != nil }
        guard case .failure(let error) = rejected.value! else {
            throw DecisionAnalysisFixtureFailure.assertion("one-nanosecond period identity change must reject the response")
        }
        guard case DecisionAnalysisServiceError.identityMismatch = error else {
            throw DecisionAnalysisFixtureFailure.assertion("nanosecond population mismatch must be an identity mismatch")
        }
        rejectedEnvironment.0.disconnect()
    }

    private static func verifyRustWireFixtures(at directory: URL) throws {
        let names = [
            "paper-overview.json",
            "paper-decisions.json",
            "paper-buy-decisions.json",
            "paper-decision.json",
            "paper-compare.json",
            "paper-selected-compare.json",
            "automation-overview.json",
            "automation-decisions.json",
            "automation-buy-decisions.json",
            "automation-decision.json",
            "automation-compare.json",
            "automation-selected-compare.json",
            "paper-captured-overview.json",
            "paper-captured-decisions.json",
            "paper-captured-decision.json",
            "paper-captured-compare.json",
            "paper-all-markets-compare.json",
            "automation-all-markets-compare.json",
            "automation-preperiod-all-markets-compare.json",
            "paper-no-previous-compare.json",
            "automation-no-previous-compare.json",
            "paper-captured-compare-missing-time.json",
            "paper-captured-compare-no-previous.json"
        ]
        for name in names {
            let responseData = try Data(contentsOf: directory.appendingPathComponent(name))
            let responseObject = try JSONSerialization.jsonObject(with: responseData) as? [String: Any]
            guard let responseObject, let action = responseObject["action"] as? String else {
                throw DecisionAnalysisFixtureFailure.assertion("Rust fixture \(name) is not a typed response object")
            }
            let snapshotObject = (responseObject["source_snapshot"] as? [String: Any])
                ?? (responseObject["selected_source_snapshot"] as? [String: Any])
            guard let snapshotObject else {
                throw DecisionAnalysisFixtureFailure.assertion("Rust fixture \(name) is missing its source snapshot")
            }
            let snapshotData = try JSONSerialization.data(withJSONObject: snapshotObject)
            let snapshot = try JSONDecoder().decode(DecisionAnalysisSourceSnapshot.self, from: snapshotData)
            let environment = try DecisionAnalysisFixtureWire.fixtureEnvironment(
                mode: snapshot.mode,
                handler: DecisionAnalysisFixtureWire.actualWireHandler(responseData: responseData, expectedAction: action)
            )
            switch action {
            case "overview":
                let population = snapshot.selection.population
                let result = DecisionAnalysisFixtureResultBox<(DecisionAnalysisSourceSnapshot, Bool, DecisionAnalysisTotals, DecisionAnalysisMeasurement)>()
                _ = environment.1.overview(
                    population: population,
                    expectedMode: snapshot.mode,
                    expectedTransportContext: environment.2
                ) { result.value = $0 }
                try DecisionAnalysisFixtureAssertions.waitUntil("Rust \(name) overview decode") { result.value != nil }
                _ = try result.value!.get()
                if name == "paper-captured-overview.json" {
                    let conditions = try result.value!.get().2.conditions
                    let outcomesByPath = Dictionary(uniqueKeysWithValues: conditions.map {
                        ($0.ast_path, $0.outcome)
                    })
                    try DecisionAnalysisFixtureAssertions.expect(
                        outcomesByPath["rule:entry:comparison"] == .evaluatedTrue
                            && outcomesByPath["rule:entry:weight"] == .evaluatedFalse
                            && outcomesByPath["rule:entry:crossing"] == .unknown(.crossHistoryUnavailable)
                            && outcomesByPath["rule:entry:skipped"] == .notEvaluated(.priorRuleMatched),
                        "actual Rust captured overview must retain TRUE, FALSE, UNKNOWN and NOT_EVALUATED wire outcomes"
                    )
                }
            case "decisions":
                let pageObject = responseObject["page"] as? [String: Any] ?? [:]
                let pageData = try JSONSerialization.data(withJSONObject: pageObject)
                let page = try JSONDecoder().decode(DecisionAnalysisPage.self, from: pageData)
                let filterData = try JSONSerialization.data(
                    withJSONObject: responseObject["filter"] ?? NSNull(),
                    options: [.fragmentsAllowed]
                )
                let filter = try JSONDecoder().decode(DecisionAnalysisFilter?.self, from: filterData)
                let result = DecisionAnalysisFixtureResultBox<(DecisionAnalysisPage, DecisionAnalysisMeasurement)>()
                _ = environment.1.decisions(
                    selection: snapshot.selection,
                    page: (offset: page.offset, limit: page.limit),
                    filter: filter,
                    expectedMode: snapshot.mode,
                    expectedTransportContext: environment.2
                ) { result.value = $0 }
                try DecisionAnalysisFixtureAssertions.waitUntil("Rust \(name) decisions decode") { result.value != nil }
                _ = try result.value!.get()
            case "decision":
                let detailObject = responseObject["decision"] as? [String: Any] ?? [:]
                let detailData = try JSONSerialization.data(withJSONObject: detailObject)
                let detail = try JSONDecoder().decode(DecisionAnalysisDecisionDetail.self, from: detailData)
                let result = DecisionAnalysisFixtureResultBox<(DecisionAnalysisDecisionDetail, DecisionAnalysisMeasurement)>()
                _ = environment.1.decision(
                    selection: snapshot.selection,
                    decisionID: detail.row.decision_id,
                    expectedMode: snapshot.mode,
                    expectedTransportContext: environment.2
                ) { result.value = $0 }
                try DecisionAnalysisFixtureAssertions.waitUntil("Rust \(name) decision decode") { result.value != nil }
                _ = try result.value!.get()
            case "compare":
                let comparisonObject = responseObject["comparison"] as? [String: Any] ?? [:]
                let comparisonData = try JSONSerialization.data(withJSONObject: comparisonObject)
                let comparison = try JSONDecoder().decode(DecisionAnalysisComparison.self, from: comparisonData)
                switch name {
                case "paper-all-markets-compare.json", "automation-all-markets-compare.json",
                     "automation-preperiod-all-markets-compare.json":
                    guard let compared = comparison.compared,
                          let comparedSnapshotObject = responseObject["compared_source_snapshot"] as? [String: Any] else {
                        throw DecisionAnalysisFixtureFailure.assertion("all-markets previous fixture must carry the compared row and fence")
                    }
                    let comparedSnapshotData = try JSONSerialization.data(withJSONObject: comparedSnapshotObject)
                    let comparedSnapshot = try JSONDecoder().decode(DecisionAnalysisSourceSnapshot.self, from: comparedSnapshotData)
                    let isPrePeriod = name == "automation-preperiod-all-markets-compare.json"
                    let comparedTime = compared.row.decision_time.flatMap(DecisionAnalysisInstant.utc)
                    let selectedPeriodStart = DecisionAnalysisInstant.utc(snapshot.selection.population.period.start)
                    let comparedPeriodStart = DecisionAnalysisInstant.utc(comparedSnapshot.selection.population.period.start)
                    let comparedPeriodEnd = DecisionAnalysisInstant.utc(comparedSnapshot.selection.population.period.end)
                    try DecisionAnalysisFixtureAssertions.expect(
                        comparison.kind == "previous"
                            && comparison.unavailable_reason == nil
                            && snapshot.selection.population.market == nil
                            && comparedSnapshot.selection.population.market == nil
                            && compared.row.market == comparison.selected.row.market
                            && (isPrePeriod
                                ? (comparedTime != nil
                                    && selectedPeriodStart != nil
                                    && comparedPeriodStart == comparedTime
                                    && comparedPeriodEnd == selectedPeriodStart
                                    && comparedTime! < selectedPeriodStart!)
                                : comparedSnapshot.selection == snapshot.selection),
                        "actual all-markets previous fixture must preserve nil population filters, same-row-market identity and bounded prior context"
                    )
                case "paper-no-previous-compare.json", "automation-no-previous-compare.json",
                     "paper-captured-compare-no-previous.json":
                    try DecisionAnalysisFixtureAssertions.expect(
                        comparison.kind == "previous"
                            && comparison.compared == nil
                            && comparison.unavailable_reason == .noPreviousDecision
                            && (responseObject["compared_source_snapshot"] == nil
                                || responseObject["compared_source_snapshot"] is NSNull),
                        "actual no-previous fixture must carry no compared source snapshot"
                    )
                case "paper-captured-compare-missing-time.json":
                    try DecisionAnalysisFixtureAssertions.expect(
                        comparison.kind == "previous"
                            && comparison.selected.row.decision_time == nil
                            && comparison.compared == nil
                            && comparison.unavailable_reason == .missingDecisionTime
                            && (responseObject["compared_source_snapshot"] == nil
                                || responseObject["compared_source_snapshot"] is NSNull),
                        "actual missing-time fixture must carry no compared source snapshot"
                    )
                default:
                    break
                }
                let selectedID = comparison.selected.row.decision_id
                let target: DecisionAnalysisComparisonTarget
                if comparison.kind == "previous" {
                    target = .previous
                } else {
                    guard let compared = comparison.compared,
                          let comparedSnapshot = responseObject["compared_source_snapshot"] as? [String: Any] else {
                        throw DecisionAnalysisFixtureFailure.assertion("Rust fixture \(name) is missing selected comparison evidence")
                    }
                    let comparedData = try JSONSerialization.data(withJSONObject: comparedSnapshot)
                    let snapshot = try JSONDecoder().decode(DecisionAnalysisSourceSnapshot.self, from: comparedData)
                    target = .selected(selection: snapshot.selection, decisionID: compared.row.decision_id)
                }
                let result = DecisionAnalysisFixtureResultBox<(DecisionAnalysisComparison, DecisionAnalysisSourceSnapshot?, DecisionAnalysisMeasurement)>()
                _ = environment.1.compare(
                    selection: snapshot.selection,
                    decisionID: selectedID,
                    against: target,
                    expectedMode: snapshot.mode,
                    expectedTransportContext: environment.2
                ) { result.value = $0 }
                try DecisionAnalysisFixtureAssertions.waitUntil("Rust \(name) compare decode") { result.value != nil }
                _ = try result.value!.get()
            default:
                throw DecisionAnalysisFixtureFailure.assertion("Rust fixture \(name) has unsupported action \(action)")
            }
            let calls = DecisionAnalysisFixtureURLProtocol.capturedToolCalls()
            try DecisionAnalysisFixtureAssertions.expect(calls.count == 1 && calls[0].tool == DecisionAnalysisFixtureWire.toolName,
                                                         "Rust fixture \(name) must enter the native query-only transport")
            environment.0.disconnect()
        }
    }

}
