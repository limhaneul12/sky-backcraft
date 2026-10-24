import Foundation
import Darwin

enum NativeMCPError: LocalizedError {
    case unavailable, busy, transport, cancelled, timeout, transportContextChanged
    case rateLimited(retryAfterSeconds: Int?)
    case unauthorized, protocolError, toolRejected, paperEventPageByteBudgetExceeded, mockData
    var errorDescription: String? {
        switch self {
        case .unavailable: return "로컬 Backend에 연결되지 않았습니다."
        case .busy: return "이전 조회가 진행 중입니다."
        case .transport: return "Backend 응답을 받지 못했습니다. 마지막 값은 최신으로 확인되지 않았습니다."
        case .cancelled: return "조회를 취소했습니다."
        case .timeout: return "Backend 응답 시간이 초과되었습니다. 마지막 값은 최신으로 확인되지 않았습니다."
        case .transportContextChanged: return "Backend 연결이 바뀌었습니다. 최신 상태와 요청 결과를 다시 확인하세요."
        case .rateLimited(let seconds):
            guard let seconds else { return "Backend 요청 한도에 도달했습니다. 잠시 후 다시 시도하세요." }
            return "Backend 요청 한도에 도달했습니다. \(seconds)초 후 다시 시도하세요."
        case .unauthorized: return "로컬 Backend 접근 권한을 확인할 수 없습니다."
        case .protocolError: return "Backend 응답 형식이 유효하지 않습니다."
        case .toolRejected: return "Backend가 요청을 거부했습니다. 상태를 다시 조회하세요."
        case .paperEventPageByteBudgetExceeded:
            return "단일 이벤트가 조회 크기 제한을 초과했습니다. 해당 이벤트를 표시할 수 없습니다."
        case .mockData: return "거래소 실계좌 연결이 없습니다. Mock 값을 실계좌로 표시하지 않습니다."
        }
    }
}

enum LiveOrderControlRequestError: LocalizedError {
    case unknownRequest

    var errorDescription: String? {
        "Backend has no durable receipt for this live-order control request. The result remains unknown."
    }
}

/// Per-read cancellation. A composite may own the two sequential requests used by
/// an exchange-mode preflight followed by its read tool.
final class NativeMCPRequest {
    private let lock = NSLock()
    private var currentTask: URLSessionDataTask?
    private var children: [NativeMCPRequest] = []
    private var cancellationHandler: (() -> Void)?
    private var cancelled = false
    private var finished = false

    var isPending: Bool {
        lock.lock(); defer { lock.unlock() }
        return !cancelled && !finished
    }

    func cancel() {
        let task: URLSessionDataTask?
        let childRequests: [NativeMCPRequest]
        let handler: (() -> Void)?
        lock.lock()
        guard !cancelled, !finished else { lock.unlock(); return }
        cancelled = true
        finished = true
        task = currentTask
        currentTask = nil
        childRequests = children
        children.removeAll(keepingCapacity: false)
        handler = cancellationHandler
        cancellationHandler = nil
        lock.unlock()
        task?.cancel()
        childRequests.forEach { $0.cancel() }
        handler?()
    }

    /// A composite read has at most a preflight and a tool request.
    @discardableResult
    func addChild(_ request: NativeMCPRequest) -> Bool {
        lock.lock()
        guard !cancelled, !finished, children.count < 2 else {
            lock.unlock()
            request.cancel()
            return false
        }
        children.append(request)
        lock.unlock()
        return true
    }

    /// A serialized read chain may replace its single active child without accumulating retries.
    @discardableResult
    func replaceSingleChild(with request: NativeMCPRequest) -> Bool {
        let previous: NativeMCPRequest?
        lock.lock()
        guard !cancelled, !finished, children.count <= 1 else {
            lock.unlock()
            request.cancel()
            return false
        }
        previous = children.first
        children = [request]
        lock.unlock()
        previous?.cancel()
        return true
    }

    /// Atomically claims a composite's final callback and releases its children.
    @discardableResult
    func complete() -> Bool {
        claimCompletion()
    }

    func setCancellationHandler(_ handler: @escaping () -> Void) {
        lock.lock()
        if cancelled {
            lock.unlock()
            handler()
            return
        }
        guard !finished else { lock.unlock(); return }
        cancellationHandler = handler
        lock.unlock()
    }

    fileprivate func install(_ task: URLSessionDataTask) -> Bool {
        lock.lock()
        guard !cancelled, !finished else {
            lock.unlock()
            task.cancel()
            return false
        }
        currentTask = task
        lock.unlock()
        return true
    }

    fileprivate func clearCurrentTask() {
        lock.lock()
        currentTask = nil
        lock.unlock()
    }

    @discardableResult
    fileprivate func claimCompletion() -> Bool {
        lock.lock()
        guard !finished else { lock.unlock(); return false }
        finished = true
        currentTask = nil
        children.removeAll(keepingCapacity: false)
        cancellationHandler = nil
        lock.unlock()
        return true
    }

    static func alreadyFinished() -> NativeMCPRequest {
        let request = NativeMCPRequest()
        _ = request.claimCompletion()
        return request
    }
}

private final class LocalSessionDelegate: NSObject, URLSessionTaskDelegate {
    func urlSession(_ session: URLSession, task: URLSessionTask,
                    willPerformHTTPRedirection response: HTTPURLResponse,
                    newRequest request: URLRequest,
                    completionHandler: @escaping (URLRequest?) -> Void) {
        completionHandler(nil)
    }
}

private final class NativeMCPRealtimeStatusStreamDelegate: NSObject, URLSessionDataDelegate, URLSessionTaskDelegate {
    private let expectedContext: NativeMCPTransportContext
    private let currentContext: () -> NativeMCPTransportContext?
    private let onOpen: () -> Void
    private let onData: (Data) -> Void
    private let onEnd: (Error?) -> Void
    private weak var owner: NativeMCPRequest?
    private let lock = NSLock()
    private var responseAccepted = false
    private var terminal = false

    init(
        expectedContext: NativeMCPTransportContext,
        currentContext: @escaping () -> NativeMCPTransportContext?,
        onOpen: @escaping () -> Void,
        onData: @escaping (Data) -> Void,
        onEnd: @escaping (Error?) -> Void
    ) {
        self.expectedContext = expectedContext
        self.currentContext = currentContext
        self.onOpen = onOpen
        self.onData = onData
        self.onEnd = onEnd
    }

    func attach(owner: NativeMCPRequest) {
        self.owner = owner
    }

    func urlSession(_ session: URLSession, dataTask: URLSessionDataTask,
                    didReceive response: URLResponse,
                    completionHandler: @escaping (URLSession.ResponseDisposition) -> Void) {
        guard currentContext() == expectedContext else {
            completionHandler(.cancel)
            finish(NativeMCPError.transportContextChanged, session: session)
            return
        }
        guard let response = response as? HTTPURLResponse else {
            completionHandler(.cancel)
            finish(NativeMCPError.protocolError, session: session)
            return
        }
        guard response.statusCode != 401 && response.statusCode != 403 else {
            completionHandler(.cancel)
            finish(NativeMCPError.unauthorized, session: session)
            return
        }
        guard response.statusCode == 200,
              response.mimeType?.lowercased() == "text/event-stream" else {
            completionHandler(.cancel)
            finish(NativeMCPError.protocolError, session: session)
            return
        }
        lock.lock()
        guard !terminal else {
            lock.unlock()
            completionHandler(.cancel)
            return
        }
        responseAccepted = true
        lock.unlock()
        completionHandler(.allow)
        onOpen()
    }

    func urlSession(_ session: URLSession, dataTask: URLSessionDataTask, didReceive data: Data) {
        guard currentContext() == expectedContext else {
            finish(NativeMCPError.transportContextChanged, session: session)
            return
        }
        lock.lock()
        let accepted = responseAccepted && !terminal
        lock.unlock()
        guard accepted else {
            finish(NativeMCPError.protocolError, session: session)
            return
        }
        onData(data)
    }

    func urlSession(_ session: URLSession, task: URLSessionTask,
                    willPerformHTTPRedirection response: HTTPURLResponse,
                    newRequest request: URLRequest,
                    completionHandler: @escaping (URLRequest?) -> Void) {
        completionHandler(nil)
    }

    func urlSession(_ session: URLSession, task: URLSessionTask, didCompleteWithError error: Error?) {
        guard currentContext() == expectedContext else {
            finish(NativeMCPError.transportContextChanged, session: session)
            return
        }
        if let urlError = error as? URLError {
            switch urlError.code {
            case .cancelled: finish(NativeMCPError.cancelled, session: session)
            case .timedOut: finish(NativeMCPError.timeout, session: session)
            default: finish(NativeMCPError.transport, session: session)
            }
        } else {
            finish(NativeMCPError.transport, session: session)
        }
    }

    private func finish(_ error: Error?, session: URLSession) {
        lock.lock()
        guard !terminal else {
            lock.unlock()
            return
        }
        terminal = true
        lock.unlock()
        if owner?.claimCompletion() == true { onEnd(error) }
        session.finishTasksAndInvalidate()
    }
}

struct NativeMCPOwnedHelperIdentity: Codable, Equatable {
    let instance_id: String
    let process_id: Int32
    let data_root_identity: String

    init(instanceID: UUID, processID: Int32, dataRoot: URL) {
        instance_id = instanceID.uuidString.lowercased()
        process_id = processID
        data_root_identity = dataRoot.resolvingSymlinksInPath().standardizedFileURL.path
    }
}

struct NativeMCPTransportContext: Codable, Equatable {
    let generation: UInt64
    let owned_helper: NativeMCPOwnedHelperIdentity?
}

extension Notification.Name {
    static let nativeMCPOwnedHelperExited = Notification.Name("NativeMCPOwnedHelperExited")
}

/// Native transport only: all policy, exchange credentials and effects stay in Rust.
final class NativeMCPClient {
    static let shared = NativeMCPClient()
    private let lock = NSLock()
    private var endpoint: URL?
    private var token: String?
    private var generation: UInt64 = 0
    private var transportContext: NativeMCPTransportContext?
    private var inFlight = 0
    private let session: URLSession
    private let delegate: LocalSessionDelegate?
    private static let allowedTools: Set<String> = [
        "lab_status", "result_query", "history_query", "policy_query",
        "realtime_status",
        "decision_analysis_query",
        "resource_delete_preview", "policy_delete_write", "policy_delete_query",
        "upbit_account_balances", "upbit_orders_open",
        "upbit_orders_closed", "trading_control",
        "live_order_control",
        "paper_review_query", "paper_session_query", "paper_session_write",
        "automation_session_query", "automation_session_write"
    ]
    private static let paperEventPageByteBudgetError =
        "RESOURCE_LIMIT: paper event page exceeds byte budget; request a smaller limit"

    init(session: URLSession? = nil) {
        if let session { self.session = session; delegate = nil }
        else {
            let delegate = LocalSessionDelegate()
            self.delegate = delegate
            let configuration = URLSessionConfiguration.ephemeral
            configuration.timeoutIntervalForRequest = 12
            configuration.timeoutIntervalForResource = 20
            configuration.httpMaximumConnectionsPerHost = 2
            configuration.httpCookieStorage = nil
            configuration.urlCredentialStorage = nil
            configuration.connectionProxyDictionary = [:]
            self.session = URLSession(configuration: configuration, delegate: delegate, delegateQueue: nil)
        }
    }

    @discardableResult
    func configure(
        port: Int,
        token: String,
        ownedHelper: NativeMCPOwnedHelperIdentity? = nil
    ) -> NativeMCPTransportContext? {
        lock.lock(); defer { lock.unlock() }
        generation &+= 1
        guard (1...65535).contains(port), token.count == 64,
              token.allSatisfy({ $0.isHexDigit }) else {
            endpoint = nil
            self.token = nil
            transportContext = nil
            return nil
        }
        endpoint = URL(string: "http://127.0.0.1:\(port)/desktop/mcp")
        self.token = token
        let context = NativeMCPTransportContext(generation: generation, owned_helper: ownedHelper)
        transportContext = context
        return context
    }

    func disconnect() {
        lock.lock(); defer { lock.unlock() }
        generation &+= 1; endpoint = nil; token = nil
        transportContext = nil
    }

    func currentTransportContext() -> NativeMCPTransportContext? {
        lock.lock(); defer { lock.unlock() }
        return transportContext
    }

    /// Opens the authenticated, latest-state status stream without exposing the endpoint or token.
    @discardableResult
    func streamRealtimeStatus(
        expectedTransportContext: NativeMCPTransportContext,
        onOpen: @escaping () -> Void,
        onData: @escaping (Data) -> Void,
        onEnd: @escaping (Error?) -> Void
    ) -> NativeMCPRequest {
        let endpoint: URL
        let token: String
        lock.lock()
        guard transportContext == expectedTransportContext,
              let configuredEndpoint = self.endpoint,
              let configuredToken = self.token else {
            let error: Error = transportContext == expectedTransportContext
                ? NativeMCPError.unavailable
                : NativeMCPError.transportContextChanged
            lock.unlock()
            DispatchQueue.main.async { onEnd(error) }
            return .alreadyFinished()
        }
        endpoint = configuredEndpoint
        token = configuredToken
        lock.unlock()

        guard var components = URLComponents(url: endpoint, resolvingAgainstBaseURL: false) else {
            DispatchQueue.main.async { onEnd(NativeMCPError.protocolError) }
            return .alreadyFinished()
        }
        components.path = "/desktop/realtime/events"
        components.query = nil
        components.fragment = nil
        guard let streamURL = components.url else {
            DispatchQueue.main.async { onEnd(NativeMCPError.protocolError) }
            return .alreadyFinished()
        }

        var urlRequest = URLRequest(url: streamURL, cachePolicy: .reloadIgnoringLocalCacheData)
        urlRequest.httpMethod = "GET"
        urlRequest.setValue("text/event-stream", forHTTPHeaderField: "Accept")
        urlRequest.setValue("no-cache", forHTTPHeaderField: "Cache-Control")
        urlRequest.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")

        let owner = NativeMCPRequest()
        let streamDelegate = NativeMCPRealtimeStatusStreamDelegate(
            expectedContext: expectedTransportContext,
            currentContext: { [weak self] in self?.currentTransportContext() },
            onOpen: onOpen,
            onData: onData,
            onEnd: onEnd
        )
        streamDelegate.attach(owner: owner)
        let configuration = URLSessionConfiguration.ephemeral
        configuration.timeoutIntervalForRequest = 45
        configuration.timeoutIntervalForResource = 3_600
        configuration.httpMaximumConnectionsPerHost = 1
        configuration.httpCookieStorage = nil
        configuration.urlCredentialStorage = nil
        configuration.connectionProxyDictionary = [:]
        let streamSession = URLSession(configuration: configuration, delegate: streamDelegate, delegateQueue: nil)
        let task = streamSession.dataTask(with: urlRequest)
        owner.setCancellationHandler { streamSession.invalidateAndCancel() }
        guard owner.install(task) else {
            streamSession.invalidateAndCancel()
            return owner
        }
        task.resume()
        return owner
    }

    func confirmOwnedHelperExit(_ context: NativeMCPTransportContext) {
        lock.lock()
        let matchesCurrentTransport = transportContext == context
        lock.unlock()
        guard matchesCurrentTransport, context.owned_helper != nil else { return }
        NotificationCenter.default.post(
            name: .nativeMCPOwnedHelperExited,
            object: self,
            userInfo: ["context": context]
        )
    }

    func ownedHelperIsAbsent(
        in context: NativeMCPTransportContext?,
        expectedDataRootIdentity: String
    ) -> Bool {
        guard let identity = context?.owned_helper,
              identity.data_root_identity == expectedDataRootIdentity,
              identity.process_id > 0,
              UUID(uuidString: identity.instance_id)?.uuidString.lowercased() == identity.instance_id else {
            return false
        }
        return kill(identity.process_id, 0) == -1 && errno == ESRCH
    }

    @discardableResult
    func call(_ tool: String, arguments: [String: Any],
              expectedTransportContext: NativeMCPTransportContext? = nil,
              completion: @escaping (Result<Data, Error>) -> Void) -> NativeMCPRequest {
        guard Self.allowedTools.contains(tool) else {
            completion(.failure(NativeMCPError.unauthorized))
            return .alreadyFinished()
        }
        lock.lock()
        guard expectedTransportContext == nil || expectedTransportContext == transportContext else {
            lock.unlock()
            completion(.failure(NativeMCPError.transportContextChanged))
            return .alreadyFinished()
        }
        guard let endpoint, let token else {
            lock.unlock()
            completion(.failure(NativeMCPError.unavailable))
            return .alreadyFinished()
        }
        guard inFlight < 4 else {
            lock.unlock()
            completion(.failure(NativeMCPError.busy))
            return .alreadyFinished()
        }
        inFlight += 1
        let startedGeneration = generation
        lock.unlock()
        let request = NativeMCPRequest()
        let deliver: (Result<Data, Error>) -> Void = { [self] result in
            self.lock.lock()
            self.inFlight -= 1
            let current = startedGeneration == self.generation
            self.lock.unlock()
            DispatchQueue.main.async { completion(current ? result : .failure(NativeMCPError.unavailable)) }
        }
        let finish: (Result<Data, Error>) -> Void = { result in
            guard request.claimCompletion() else { return }
            deliver(result)
        }
        request.setCancellationHandler { deliver(.failure(NativeMCPError.cancelled)) }
        rpc(endpoint: endpoint, token: token, request: request, method: "initialize", params: [
            "protocolVersion": "2025-03-26", "capabilities": [:],
            "clientInfo": ["name": "SkyBackcraft-Native", "version": "P13"]
        ]) { [self] initialization in
            switch initialization {
            case .failure(let error): finish(.failure(error))
            case .success(let result):
                guard let protocolVersion = result["protocolVersion"] as? String else {
                    finish(.failure(NativeMCPError.protocolError)); return
                }
                self.rpc(endpoint: endpoint, token: token, request: request, method: "tools/call", params: [
                    "name": tool, "arguments": arguments
                ], protocolVersion: protocolVersion) { result in
                    switch result {
                    case .failure(let error): finish(.failure(error))
                    case .success(let payload):
                        if payload["isError"] as? Bool == true {
                            let errorText = (payload["content"] as? [[String: Any]])?
                                .first(where: { $0["type"] as? String == "text" })?["text"] as? String ?? ""
                            if tool == "live_order_control",
                               arguments["action"] as? String == "request",
                               errorText.contains("unknown live-order control request") {
                                finish(.failure(LiveOrderControlRequestError.unknownRequest))
                            } else if tool == "paper_session_query",
                                      arguments["action"] as? String == "review_events",
                                      errorText == Self.paperEventPageByteBudgetError {
                                finish(.failure(NativeMCPError.paperEventPageByteBudgetExceeded))
                            } else {
                                finish(.failure(NativeMCPError.toolRejected))
                            }
                            return
                        }
                        guard let content = payload["content"] as? [[String: Any]],
                              let body = content.first(where: { $0["type"] as? String == "text" })?["text"] as? String,
                              let data = body.data(using: .utf8) else { finish(.failure(NativeMCPError.protocolError)); return }
                        finish(.success(data))
                    }
                }
            }
        }
        return request
    }

    private func rpc(endpoint: URL, token: String, request owner: NativeMCPRequest,
                     method: String, params: [String: Any],
                     protocolVersion: String? = nil,
                     completion: @escaping (Result<[String: Any], Error>) -> Void) {
        var request = URLRequest(url: endpoint)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue("application/json, text/event-stream", forHTTPHeaderField: "Accept")
        request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        if let protocolVersion { request.setValue(protocolVersion, forHTTPHeaderField: "MCP-Protocol-Version") }
        do { request.httpBody = try JSONSerialization.data(withJSONObject: ["jsonrpc": "2.0", "id": UUID().uuidString, "method": method, "params": params]) }
        catch { completion(.failure(NativeMCPError.protocolError)); return }
        let task = session.dataTask(with: request) { [weak owner] data, response, error in
            guard let owner else { return }
            owner.clearCurrentTask()
            if let urlError = error as? URLError {
                switch urlError.code {
                case .cancelled: completion(.failure(NativeMCPError.cancelled))
                case .timedOut: completion(.failure(NativeMCPError.timeout))
                default: completion(.failure(NativeMCPError.transport))
                }
                return
            }
            guard error == nil, let response = response as? HTTPURLResponse else {
                completion(.failure(NativeMCPError.transport)); return
            }
            guard response.statusCode != 401 && response.statusCode != 403 else { completion(.failure(NativeMCPError.unauthorized)); return }
            if response.statusCode == 429 {
                let retryAfter = response.value(forHTTPHeaderField: "Retry-After")
                    .flatMap { Int($0.trimmingCharacters(in: .whitespacesAndNewlines)) }
                    .flatMap { (0...3600).contains($0) ? $0 : nil }
                completion(.failure(NativeMCPError.rateLimited(retryAfterSeconds: retryAfter)))
                return
            }
            guard response.statusCode == 200, let data, data.count <= 4_194_304,
                  let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
                  json["error"] == nil, let result = json["result"] as? [String: Any] else { completion(.failure(NativeMCPError.protocolError)); return }
            completion(.success(result))
        }
        guard owner.install(task) else { return }
        task.resume()
    }
}

struct TradingControlSnapshot: Decodable {
    let exchange_read_mode: String
    let live_trading_enabled: Bool
    let state: String
    let sequence: UInt64
    let observed_at_utc: String
    let order_admission_blocked: Bool
    let emergency_latched: Bool
    let denied_code: String?
    let receipt: TradingControlReceipt?
}

struct TradingControlReceipt: Decodable {
    let request_id: String
    let action: String
    let outcome: String
    let sequence: UInt64
    let observed_at_utc: String
    let denied_code: String?
}

struct LiveOrderControlSnapshot: Decodable {
    let boot_epoch: String
    let live_order_control_available: Bool
    let live_trading_enabled: Bool
    let sequence: UInt64
    let observed_at_utc: String
    let exchange_read_mode: String
    let order_admission_blocked: Bool
    let emergency_latched: Bool
    let confirmation_required: Bool
    let receipt: LiveOrderControlReceipt?
}

struct LiveOrderControlReceipt: Decodable {
    let request_id: String
    let action: String
    let outcome: String
    let sequence: UInt64
    let boot_epoch: String
    let observed_at_utc: String
    let denied_code: String?
}
