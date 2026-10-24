import Foundation

@main
struct RealtimeFeedMCPServiceRegression {
    static func main() throws {
        try parserHandlesFragmentedLatestStateAndKeepalive()
        try parserRejectsOversizedLines()
        try statusDecodePreservesZeroAndUnknown()
        reconciliationInvalidationsIgnoreRawReceiveChangesAndCoalesce()
        serviceRetainsLastKnownStatusAndFencesStaleCallbacks()
        serviceTreatsAuthExpiryAsTerminal()
        print("PASS native realtime status protocol and stream regression")
    }

    private static func parserHandlesFragmentedLatestStateAndKeepalive() throws {
        let frame = ": keepalive\r\nid: 18\r\nevent: snapshot\r\ndata: {\"observed_at_utc\":\"2026-10-17T03:30:00Z\",\r\ndata: \"market\":{\"state\":\"READY\"},\r\n\r\n"
        let bytes = Array(frame.utf8)
        var parser = RealtimeStatusSSEParser()
        let first = try parser.append(Data(bytes.prefix(28)))
        let second = try parser.append(Data(bytes.dropFirst(28)))
        let messages = first + second
        precondition(messages.count == 2)
        precondition(messages[0] == .keepalive)
        guard case .event(let name, let body) = messages[1] else {
            preconditionFailure("expected a framed snapshot event")
        }
        precondition(name == "snapshot")
        precondition(body.contains("\n\"market\""))
        precondition(!body.contains("18"))
    }

    private static func parserRejectsOversizedLines() throws {
        var parser = RealtimeStatusSSEParser()
        let oversized = Data(repeating: 120, count: RealtimeStatusSSEParser.maximumLineBytes + 1)
        do {
            _ = try parser.append(oversized)
            preconditionFailure("oversized SSE line was accepted")
        } catch RealtimeSSEParserError.lineTooLarge {
            // Expected bounded-parser rejection.
        }
    }

    private static func statusDecodePreservesZeroAndUnknown() throws {
        let status = try decodeStatus(statusJSON)
        precondition(status.market.connection_count == 0)
        precondition(status.market.subscription_count == 0)
        precondition(status.market.last_received_at_utc == nil)
        precondition(status.market.last_reconciled_at_utc == nil)
        precondition(!status.market.ready_for_decisions)
        precondition(status.private_feed.state == .unavailable)
        precondition(status.market.subscription_readiness == nil)

        var document = try JSONSerialization.jsonObject(with: Data(statusJSON.utf8)) as! [String: Any]
        var market = document["market"] as! [String: Any]
        market["readiness_blockers"] = ["PONG_MISSING"]
        market["subscription_readiness"] = [[
            "market": "KRW-BTC", "interval": "h1", "ready": false,
            "blockers": ["REST_BOOTSTRAP_REQUIRED"], "pending_rest_confirmation": true,
            "revision_snapshot_count": 1,
            "confirmed_provider_absence_spans": [[
                "proof_id": "fixture-proof", "start_utc": "2026-10-17T01:00:00Z",
                "end_utc": "2026-10-17T02:00:00Z"
            ]],
            "provider_recovery_progress": [
                "next_page_to_utc": "2026-10-17T01:00:00Z",
                "pages_scanned": 2, "rows_scanned": 399,
                "page_limit": 32, "row_limit": 6400,
                "pending_absence_confirmation": false
            ]
        ], [
            "market": "KRW-ETH", "interval": "m5", "ready": true
        ]]
        document["market"] = market
        let diagnosticData = try JSONSerialization.data(withJSONObject: document)
        let diagnostic = try JSONDecoder().decode(RealtimeStatus.self, from: diagnosticData)
        precondition(diagnostic.market.readiness_blockers == ["PONG_MISSING"])
        precondition(diagnostic.market.subscription_readiness?.first?.market == "KRW-BTC")
        precondition(diagnostic.market.subscription_readiness?.first?.ready == false)
        precondition(diagnostic.market.subscription_readiness?.first?.blockers == ["REST_BOOTSTRAP_REQUIRED"])
        precondition(diagnostic.market.subscription_readiness?.first?.pending_rest_confirmation == true)
        precondition(diagnostic.market.subscription_readiness?.first?.revision_snapshot_count == 1)
        precondition(diagnostic.market.subscription_readiness?.first?.confirmed_provider_absence_spans?.first?.proof_id == "fixture-proof")
        precondition(diagnostic.market.subscription_readiness?.first?.provider_recovery_progress?.pages_scanned == 2)
        precondition(diagnostic.market.subscription_readiness?.last?.ready == true)
        precondition(diagnostic.market.subscription_readiness?.last?.blockers == nil)
        precondition(diagnostic.market.subscription_readiness?.last?.confirmed_provider_absence_spans == nil)
        precondition(diagnostic.market.subscription_readiness?.last?.provider_recovery_progress == nil)

        let invalid = statusJSON.replacingOccurrences(of: "READY", with: "FUTURE_STATE")
        precondition((try? decodeStatus(invalid)) == nil)
    }

    private static func reconciliationInvalidationsIgnoreRawReceiveChangesAndCoalesce() {
        var invalidation = RealtimeReconciliationInvalidation()
        precondition(!invalidation.observe(nil, hasProjection: false, isVisible: true))
        precondition(!invalidation.observe(nil, isVisible: true))
        precondition(invalidation.observe("2026-10-17T03:30:00Z", isVisible: true))
        precondition(invalidation.takeIfReady(isVisible: true, readInFlight: false, writeInFlight: false))
        precondition(!invalidation.observe("2026-10-17T03:30:00Z", isVisible: true))

        precondition(!invalidation.observe("2026-10-17T03:31:00Z", isVisible: false))
        precondition(!invalidation.isPending)
        precondition(!invalidation.observe("2026-10-17T03:31:00Z", isVisible: true))

        precondition(invalidation.observe("2026-10-17T03:32:00Z", isVisible: true))
        precondition(invalidation.observe("2026-10-17T03:33:00Z", isVisible: true))
        precondition(invalidation.isPending)
        precondition(!invalidation.takeIfReady(isVisible: true, readInFlight: true, writeInFlight: false))
        precondition(!invalidation.takeIfReady(isVisible: true, readInFlight: false, writeInFlight: true))
        precondition(!invalidation.takeIfReady(isVisible: true, readInFlight: false, writeInFlight: false, budgetAvailable: false))
        precondition(invalidation.takeIfReady(isVisible: true, readInFlight: false, writeInFlight: false))
        precondition(!invalidation.isPending)
        precondition(!invalidation.observe("2026-10-17T03:33:00Z", isVisible: true))
    }

    private static func serviceRetainsLastKnownStatusAndFencesStaleCallbacks() {
        let transport = FakeRealtimeStatusTransport()
        let service = RealtimeFeedMCPService(transport: transport)
        service.setVisible(true)
        precondition(transport.statusReadCount == 1)
        precondition(transport.streamAttempts.count == 1)

        transport.completeStatusRead(.success(Data(statusJSON.utf8)))
        precondition(service.presentation.statusSource == .typedMCPQuery)
        transport.streamAttempts[0].onOpen()
        transport.streamAttempts[0].onData(Data("event: snapshot\ndata: \(statusJSON)\n\n".utf8))
        precondition(service.presentation.connectionState == .connected)
        precondition(service.presentation.statusSource == .sseLatestState)
        precondition(service.presentation.lastSSEStatusLocallyReceivedAt != nil)

        let lastKnown = service.presentation.status
        transport.streamAttempts[0].onEnd(NativeMCPError.transport)
        precondition(service.presentation.connectionState == .recovering)
        precondition(service.presentation.reconnectDelaySeconds == 1)
        precondition(service.presentation.status == lastKnown)
        precondition(service.presentation.statusSource == .sseLatestState)

        transport.streamAttempts[0].onData(Data("event: status\ndata: \(statusJSON.replacingOccurrences(of: "READY", with: "ERROR"))\n\n".utf8))
        precondition(service.presentation.status == lastKnown)

        service.setVisible(false)
        precondition(service.presentation.connectionState == .stopped)
        transport.streamAttempts[0].onData(Data("event: status\ndata: \(statusJSON.replacingOccurrences(of: "READY", with: "ERROR"))\n\n".utf8))
        precondition(service.presentation.status == lastKnown)

        service.setVisible(true)
        precondition(transport.streamAttempts.count == 2)
        transport.streamAttempts[0].onData(Data("event: status\ndata: \(statusJSON.replacingOccurrences(of: "READY", with: "ERROR"))\n\n".utf8))
        precondition(service.presentation.status == lastKnown)
        service.setVisible(false)
    }

    private static func serviceTreatsAuthExpiryAsTerminal() {
        let transport = FakeRealtimeStatusTransport()
        let service = RealtimeFeedMCPService(transport: transport)
        service.setVisible(true)
        transport.streamAttempts[0].onData(Data("event: auth_expired\n\n".utf8))
        precondition(service.presentation.connectionState == .authenticationExpired)
        precondition(service.presentation.reconnectDelaySeconds == nil)
        service.setVisible(false)
    }

    private static func decodeStatus(_ text: String) throws -> RealtimeStatus {
        let status = try JSONDecoder().decode(RealtimeStatus.self, from: Data(text.utf8))
        try status.validateTimestamps()
        return status
    }

    private static let statusJSON = """
    {"observed_at_utc":"2026-10-17T03:30:00Z","market":{"state":"READY","connection_count":0,"subscription_count":0,"last_received_at_utc":null,"last_reconciled_at_utc":null,"reconnect_count":0,"dropped_messages":0,"ready_for_decisions":false},"private":{"state":"UNAVAILABLE","connection_count":0,"subscription_count":0,"last_received_at_utc":null,"last_reconciled_at_utc":null,"reconnect_count":0,"dropped_messages":0,"ready_for_decisions":false}}
    """
}

private final class FakeRealtimeStatusTransport: RealtimeStatusTransport {
    struct StreamAttempt {
        let onOpen: () -> Void
        let onData: (Data) -> Void
        let onEnd: (Error?) -> Void
    }

    private(set) var statusReadCount = 0
    private(set) var streamAttempts: [StreamAttempt] = []
    private var statusCompletion: ((Result<Data, Error>) -> Void)?
    private let context = NativeMCPTransportContext(generation: 1, owned_helper: nil)

    func currentTransportContext() -> NativeMCPTransportContext? { context }

    func fetchRealtimeStatus(
        expectedTransportContext: NativeMCPTransportContext,
        completion: @escaping (Result<Data, Error>) -> Void
    ) -> NativeMCPRequest {
        precondition(expectedTransportContext == context)
        statusReadCount += 1
        statusCompletion = completion
        return .alreadyFinished()
    }

    func streamRealtimeStatusEvents(
        expectedTransportContext: NativeMCPTransportContext,
        onOpen: @escaping () -> Void,
        onData: @escaping (Data) -> Void,
        onEnd: @escaping (Error?) -> Void
    ) -> NativeMCPRequest {
        precondition(expectedTransportContext == context)
        streamAttempts.append(StreamAttempt(onOpen: onOpen, onData: onData, onEnd: onEnd))
        return .alreadyFinished()
    }

    func completeStatusRead(_ result: Result<Data, Error>) {
        let completion = statusCompletion
        statusCompletion = nil
        completion?(result)
    }
}
