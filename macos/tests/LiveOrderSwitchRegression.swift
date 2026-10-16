import Foundation

private enum RegressionFailure: Error, LocalizedError {
    case assertion(String)
    case timeout

    var errorDescription: String? {
        switch self {
        case .assertion(let message): return message
        case .timeout: return "Live-order control regression callback timed out."
        }
    }
}

private func require(_ condition: @autoclosure () -> Bool, _ message: String) throws {
    guard condition() else { throw RegressionFailure.assertion(message) }
}

private final class LiveOrderSwitchURLProtocol: URLProtocol {
    private static let lock = NSLock()
    private static var responses: [String: [String: Any]] = [:]
    private static var toolCalls: [(name: String, arguments: [String: Any])] = []
    private static var trace: [String] = []

    static func reset(responses: [String: [String: Any]]) {
        lock.lock(); defer { lock.unlock() }
        self.responses = responses
        toolCalls.removeAll(keepingCapacity: true)
        trace.removeAll(keepingCapacity: true)
    }

    static func capturedToolCalls() -> [(name: String, arguments: [String: Any])] {
        lock.lock(); defer { lock.unlock() }
        return toolCalls
    }

    static func diagnosticTrace() -> String {
        lock.lock(); defer { lock.unlock() }
        return trace.joined(separator: " | ")
    }

    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
    override func stopLoading() {}

    override func startLoading() {
        do {
            Self.recordTrace("start body=\(request.httpBody != nil) stream=\(request.httpBodyStream != nil)")
            guard let url = request.url,
                  let body = try Self.bodyData(for: request),
                  let rpc = try JSONSerialization.jsonObject(with: body) as? [String: Any],
                  let method = rpc["method"] as? String,
                  let params = rpc["params"] as? [String: Any] else {
                throw RegressionFailure.assertion("Malformed MCP request in URLProtocol fixture.")
            }
            Self.recordTrace("method=\(method)")
            let result: [String: Any]
            if method == "initialize" {
                result = ["protocolVersion": "2025-03-26"]
            } else if method == "tools/call" {
                guard let name = params["name"] as? String,
                      let arguments = params["arguments"] as? [String: Any] else {
                    throw RegressionFailure.assertion("Malformed MCP tools/call parameters.")
                }
                let responseText: String
                if name == "live_order_control", arguments["action"] as? String == "request",
                   arguments["request_id"] as? String == "unknown-request" {
                    responseText = "INVALID_CONFIG: unknown live-order control request unknown-request"
                    result = ["isError": true, "content": [["type": "text", "text": responseText]]]
                } else {
                    guard let data = Self.responseData(for: arguments, tool: name),
                          let text = String(data: data, encoding: .utf8) else {
                        throw RegressionFailure.assertion("No fixture response for MCP call \(name).")
                    }
                    responseText = text
                    result = ["content": [["type": "text", "text": responseText]]]
                }
                Self.record(name: name, arguments: arguments)
            } else {
                throw RegressionFailure.assertion("Unexpected MCP method \(method).")
            }
            let envelope: [String: Any] = [
                "jsonrpc": "2.0",
                "id": rpc["id"] ?? "fixture",
                "result": result
            ]
            let responseData = try JSONSerialization.data(withJSONObject: envelope)
            guard let response = HTTPURLResponse(
                url: url,
                statusCode: 200,
                httpVersion: "HTTP/1.1",
                headerFields: ["Content-Type": "application/json"]
            ) else {
                throw RegressionFailure.assertion("Could not create fixture HTTP response.")
            }
            client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
            client?.urlProtocol(self, didLoad: responseData)
            client?.urlProtocolDidFinishLoading(self)
        } catch {
            Self.recordTrace("fixture-error=\(error.localizedDescription)")
            client?.urlProtocol(self, didFailWithError: error)
        }
    }

    private static func bodyData(for request: URLRequest) throws -> Data? {
        if let body = request.httpBody { return body }
        guard let stream = request.httpBodyStream else { return nil }
        stream.open()
        defer { stream.close() }
        var data = Data()
        var buffer = [UInt8](repeating: 0, count: 16_384)
        while true {
            let count = buffer.withUnsafeMutableBufferPointer { bytes in
                stream.read(bytes.baseAddress!, maxLength: bytes.count)
            }
            if count < 0 { throw stream.streamError ?? RegressionFailure.assertion("MCP request body stream failed.") }
            if count == 0 { break }
            data.append(contentsOf: buffer.prefix(count))
        }
        return data
    }

    private static func responseData(for arguments: [String: Any], tool: String) -> Data? {
        lock.lock(); defer { lock.unlock() }
        guard tool == "live_order_control",
              let action = arguments["action"] as? String else { return nil }
        let key: String
        switch action {
        case "status": key = "status_off"
        case "enable": key = "enabled"
        case "disable": key = "disabled"
        case "request": key = "enabled"
        default: return nil
        }
        guard let response = responses[key] else { return nil }
        return try? JSONSerialization.data(withJSONObject: response)
    }

    private static func record(name: String, arguments: [String: Any]) {
        lock.lock(); defer { lock.unlock() }
        toolCalls.append((name, arguments))
    }

    private static func recordTrace(_ event: String) {
        lock.lock(); defer { lock.unlock() }
        trace.append(event)
    }
}

@main
private enum LiveOrderSwitchRegression {
    static func main() {
        do {
            let fixture = try loadRustWireFixture()
            try runtimeArgumentsStayOptIn(fixture: fixture)
            try decodeActualRustResponses(fixture: fixture)
            try serviceUsesFrozenToolAndMutationIdentity(fixture: fixture)
            print("Sky Backcraft native live-order switch regression: PASS")
        } catch {
            fputs("Sky Backcraft native live-order switch regression: FAIL: \(error.localizedDescription)\n", stderr)
            exit(1)
        }
    }

    private static func loadRustWireFixture() throws -> [String: [String: Any]] {
        guard let path = ProcessInfo.processInfo.environment["SKY_BACKCRAFT_LIVE_ORDER_WIRE_FIXTURE"] else {
            throw RegressionFailure.assertion("Set SKY_BACKCRAFT_LIVE_ORDER_WIRE_FIXTURE to Main's actual Rust JSON fixture.")
        }
        let data = try Data(contentsOf: URL(fileURLWithPath: path))
        guard let fixture = try JSONSerialization.jsonObject(with: data) as? [String: [String: Any]] else {
            throw RegressionFailure.assertion("Actual Rust live-order wire fixture must contain named response objects.")
        }
        for key in ["status_off", "enabled", "disabled", "prior_boot_receipt", "restarted_off"] {
            guard fixture[key] != nil else {
                throw RegressionFailure.assertion("Actual Rust live-order wire fixture is missing \(key).")
            }
        }
        return fixture
    }

    private static func runtimeArgumentsStayOptIn(fixture: [String: [String: Any]]) throws {
        let dataRoot = URL(fileURLWithPath: "/tmp/sky-backcraft-live-order-regression/research", isDirectory: true)
        let noAuth = try RuntimeController.backendArguments(
            configuration: .default,
            dataDirectory: dataRoot
        )
        try require(!noAuth.contains("--live-order-control"), "No-auth startup enabled the managed order gate.")
        try require(!noAuth.contains("--allow-live-orders"), "GUI startup restored legacy live-order authorization.")

        let oauth = AppConfiguration(domain: "oauth.example", port: 8130, authenticationMode: .oauth)
        let oauthArgs = try RuntimeController.backendArguments(configuration: oauth, dataDirectory: dataRoot)
        try require(!oauthArgs.contains("--live-order-control"), "OAuth without tradingMcpEnabled received the capability flag.")
        try require(!oauthArgs.contains("--allow-live-orders"), "OAuth startup restored legacy live-order authorization.")

        let managed = AppConfiguration(
            domain: "oauth.example",
            port: 8130,
            authenticationMode: .oauth,
            tradingMcpEnabled: true
        )
        let managedArgs = try RuntimeController.backendArguments(configuration: managed, dataDirectory: dataRoot)
        try require(managedArgs.contains("--oauth") && managedArgs.contains("--trading-mcp"), "Managed capability lost its existing OAuth/trading-MCP requirements.")
        try require(managedArgs.contains("--live-order-control"), "Managed OAuth trading-MCP startup omitted the order-control capability flag.")
        try require(!managedArgs.contains("--allow-live-orders"), "Managed startup enabled the legacy operator-live policy.")

        let missingHost = AppConfiguration(domain: "", port: 8130, authenticationMode: .oauth, tradingMcpEnabled: true)
        let missingHostArgs = try RuntimeController.backendArguments(configuration: missingHost, dataDirectory: dataRoot)
        try require(!missingHostArgs.contains("--oauth") && !missingHostArgs.contains("--live-order-control"), "Missing OAuth public host received the managed capability flag.")

        let persisted = String(data: try JSONEncoder().encode(managed), encoding: .utf8) ?? ""
        try require(!persisted.contains("live_order_control") && !persisted.contains("live_order_sending"), "The runtime ON/OFF gate was persisted in GUI configuration.")
        try require(fixture["status_off"] != nil, "Wire fixture was unexpectedly discarded.")
    }

    private static func decodeActualRustResponses(fixture: [String: [String: Any]]) throws {
        func decode(_ key: String) throws -> LiveOrderControlSnapshot {
            let data = try JSONSerialization.data(withJSONObject: fixture[key]!)
            return try JSONDecoder().decode(LiveOrderControlSnapshot.self, from: data)
        }
        func receipt(_ snapshot: LiveOrderControlSnapshot, action: String) throws -> LiveOrderControlReceipt {
            guard let receipt = snapshot.receipt,
                  receipt.action == action,
                  receipt.outcome == "ACKNOWLEDGED" else {
                throw RegressionFailure.assertion("Actual Rust \(action) response lacks its typed ACK receipt.")
            }
            return receipt
        }

        let off = try decode("status_off")
        let enabled = try decode("enabled")
        let disabled = try decode("disabled")
        let priorBoot = try decode("prior_boot_receipt")
        let restarted = try decode("restarted_off")
        try require(off.live_order_control_available && !off.live_trading_enabled, "Actual Rust initial status is not available/OFF.")
        let enableReceipt = try receipt(enabled, action: "enable")
        try require(enabled.live_order_control_available && enabled.live_trading_enabled, "Actual Rust enable response is not ON.")
        try require(enableReceipt.boot_epoch == enabled.boot_epoch && enableReceipt.sequence <= enabled.sequence, "Enable receipt does not match its current status identity.")
        let disableReceipt = try receipt(disabled, action: "disable")
        try require(disabled.live_order_control_available && !disabled.live_trading_enabled, "Actual Rust disable response is not OFF.")
        try require(disableReceipt.boot_epoch == disabled.boot_epoch && disableReceipt.sequence <= disabled.sequence, "Disable receipt does not match its current status identity.")
        guard let priorReceipt = priorBoot.receipt,
              priorReceipt.outcome == "ACKNOWLEDGED" else {
            throw RegressionFailure.assertion("Prior-boot fixture lacks its typed historical ACK receipt.")
        }
        try require(!priorBoot.live_trading_enabled && priorBoot.boot_epoch != priorReceipt.boot_epoch, "A prior-boot ACK was incorrectly represented as current ON.")
        try require(!restarted.live_trading_enabled && restarted.boot_epoch != disabled.boot_epoch, "Restarted server did not return to a fresh OFF epoch.")
    }

    private static func serviceUsesFrozenToolAndMutationIdentity(fixture: [String: [String: Any]]) throws {
        LiveOrderSwitchURLProtocol.reset(responses: fixture)
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [LiveOrderSwitchURLProtocol.self]
        let client = NativeMCPClient(session: URLSession(configuration: configuration))
        guard let context = client.configure(port: 65_431, token: String(repeating: "a", count: 64)) else {
            throw RegressionFailure.assertion("Could not configure the fake MCP transport.")
        }
        let service = UpbitLiveService(client: client)
        guard let enableReceipt = fixture["enabled"]?["receipt"] as? [String: Any],
              let requestID = enableReceipt["request_id"] as? String,
              let disableReceipt = fixture["disabled"]?["receipt"] as? [String: Any],
              let disableRequestID = disableReceipt["request_id"] as? String else {
            throw RegressionFailure.assertion("Actual Rust enable/disable fixture receipts must include their request IDs.")
        }

        let statusResult = try awaitResult { completion in
            service.fetchLiveOrderControlStatus(expectedContext: context, completion: completion)
        }
        let status: LiveOrderControlSnapshot
        switch statusResult {
        case .success(let value): status = value
        case .failure(let error):
            throw RegressionFailure.assertion("Allowed live_order_control status call failed: \(error.localizedDescription) [\(String(reflecting: error))]; URLProtocol: \(LiveOrderSwitchURLProtocol.diagnosticTrace())")
        }
        try require(status.live_order_control_available && !status.live_trading_enabled, "Service did not decode its status response.")

        let enableResult = try awaitResult { completion in
            service.changeLiveOrderControl(
                action: "enable",
                requestID: requestID,
                expectedSequence: status.sequence,
                expectedEpoch: status.boot_epoch,
                confirmation: UpbitLiveService.liveOrderEnableConfirmation,
                expectedContext: context,
                completion: completion
            )
        }
        guard case .success(let enabled) = enableResult,
              enabled.receipt?.request_id == requestID else {
            throw RegressionFailure.assertion("Service did not decode the typed enable receipt.")
        }

        let disableResult = try awaitResult { completion in
            service.changeLiveOrderControl(
                action: "disable",
                requestID: disableRequestID,
                expectedSequence: enabled.sequence,
                expectedEpoch: enabled.boot_epoch,
                expectedContext: context,
                completion: completion
            )
        }
        guard case .success(let disabled) = disableResult,
              disabled.receipt?.action == "disable",
              disabled.receipt?.request_id == disableRequestID else {
            throw RegressionFailure.assertion("Service did not decode the typed disable receipt.")
        }

        let callsBeforeInvalidConfirmation = LiveOrderSwitchURLProtocol.capturedToolCalls().count
        let invalidConfirmation = try awaitResult { completion in
            service.changeLiveOrderControl(
                action: "enable",
                requestID: "must-not-send",
                expectedSequence: disabled.sequence,
                expectedEpoch: disabled.boot_epoch,
                confirmation: "enable",
                expectedContext: context,
                completion: completion
            )
        }
        guard case .failure(let error) = invalidConfirmation,
              let nativeError = error as? NativeMCPError,
              case .unauthorized = nativeError else {
            throw RegressionFailure.assertion("A non-exact ON confirmation was not rejected locally.")
        }
        try require(LiveOrderSwitchURLProtocol.capturedToolCalls().count == callsBeforeInvalidConfirmation, "Invalid ON confirmation reached MCP.")

        let unknownResult = try awaitResult { completion in
            service.readLiveOrderControlRequest(requestID: "unknown-request", expectedContext: context, completion: completion)
        }
        guard case .failure(let error) = unknownResult,
              let requestError = error as? LiveOrderControlRequestError,
              case .unknownRequest = requestError else {
            throw RegressionFailure.assertion("Unknown request tool error was not kept distinct from an OFF status.")
        }

        let calls = LiveOrderSwitchURLProtocol.capturedToolCalls()
        let controls = calls.filter { $0.name == "live_order_control" }
        try require(controls.count == 4, "The native allowlist did not dispatch exactly the expected four control reads/writes.")
        let enableArgs = controls[1].arguments
        try require(enableArgs["action"] as? String == "enable", "Enable action was changed on the wire.")
        try require(enableArgs["request_id"] as? String == requestID, "Enable request ID was changed on the wire.")
        try require((enableArgs["expected_sequence"] as? NSNumber)?.uint64Value == status.sequence, "Enable CAS sequence was not sent.")
        try require(enableArgs["expected_epoch"] as? String == status.boot_epoch, "Enable boot epoch was not sent.")
        try require(enableArgs["confirmation"] as? String == "ENABLE_LIVE_ORDER_SENDING", "Exact ON confirmation field/value was not sent.")
        let disableArgs = controls[2].arguments
        try require(disableArgs["action"] as? String == "disable", "OFF action was changed on the wire.")
        try require(disableArgs["expected_sequence"] != nil && disableArgs["expected_epoch"] != nil, "OFF did not send its CAS identity.")
        try require(disableArgs["confirmation"] == nil, "OFF sent an ON confirmation string.")
        let requestArgs = controls[3].arguments
        try require(Set(requestArgs.keys) == Set(["action", "request_id"]), "Receipt lookup must carry request_id only.")

        let currentCount = controls.count
        _ = client.configure(port: 65_432, token: String(repeating: "b", count: 64))
        let staleResult = try awaitResult { completion in
            service.fetchLiveOrderControlStatus(expectedContext: context, completion: completion)
        }
        guard case .failure(let error) = staleResult,
              let nativeError = error as? NativeMCPError,
              case .transportContextChanged = nativeError else {
            throw RegressionFailure.assertion("Old helper generation was allowed to read after transport rotation.")
        }
        try require(LiveOrderSwitchURLProtocol.capturedToolCalls().count == currentCount, "Stale helper generation dispatched another MCP request.")
    }

    private static func awaitResult<Value>(
        timeout: TimeInterval = 5,
        start: (@escaping (Result<Value, Error>) -> Void) -> NativeMCPRequest
    ) throws -> Result<Value, Error> {
        var result: Result<Value, Error>?
        let request = start { result = $0 }
        let deadline = Date().addingTimeInterval(timeout)
        while result == nil && Date() < deadline {
            _ = RunLoop.main.run(mode: .default, before: Date().addingTimeInterval(0.02))
        }
        withExtendedLifetime(request) {}
        guard let result else { throw RegressionFailure.timeout }
        return result
    }
}
