import Foundation

enum RealtimeConnectionState: String, Decodable, Equatable {
    case unavailable = "UNAVAILABLE"
    case idle = "IDLE"
    case connecting = "CONNECTING"
    case recovering = "RECOVERING"
    case ready = "READY"
    case stale = "STALE"
    case stopped = "STOPPED"
    case error = "ERROR"
}

struct RealtimeSubscriptionReadiness: Decodable, Equatable {
    let market: String
    let interval: String
    let ready: Bool
    let blockers: [String]?
    let pending_rest_confirmation: Bool?
    let revision_snapshot_count: UInt64?
    let last_revision_open_time_utc: String?
    let confirmed_provider_absence_spans: [RealtimeProviderAbsenceSpan]?
    let provider_recovery_progress: RealtimeProviderRecoveryProgress?
}

struct RealtimeProviderAbsenceSpan: Decodable, Equatable {
    let proof_id: String
    let start_utc: String
    let end_utc: String
}

struct RealtimeProviderRecoveryProgress: Decodable, Equatable {
    let next_page_to_utc: String?
    let pages_scanned: UInt32
    let rows_scanned: UInt64
    let page_limit: UInt32
    let row_limit: UInt64
    let pending_absence_confirmation: Bool
}

struct RealtimeFeedStatus: Decodable, Equatable {
    let state: RealtimeConnectionState
    let connection_count: UInt32
    let subscription_count: UInt32
    let last_received_at_utc: String?
    let last_reconciled_at_utc: String?
    let reconnect_count: UInt64
    let dropped_messages: UInt64
    let ready_for_decisions: Bool
    let readiness_blockers: [String]?
    let subscription_readiness: [RealtimeSubscriptionReadiness]?
}

struct RealtimeStatus: Decodable, Equatable {
    let observed_at_utc: String
    let market: RealtimeFeedStatus
    let private_feed: RealtimeFeedStatus

    enum CodingKeys: String, CodingKey {
        case observed_at_utc
        case market
        case private_feed = "private"
    }

    func validateTimestamps() throws {
        let parser = ReadTimestampParser()
        guard parser.ordinaryThenFractional(observed_at_utc) != nil,
              [market.last_received_at_utc, market.last_reconciled_at_utc,
               private_feed.last_received_at_utc, private_feed.last_reconciled_at_utc]
                .allSatisfy({ timestamp in
                    guard let timestamp else { return true }
                    return parser.ordinaryThenFractional(timestamp) != nil
                }) else {
            throw RealtimeStatusDecodeError.invalidTimestamp
        }
    }
}

enum RealtimeStatusDecodeError: Error {
    case invalidTimestamp
}

protocol RealtimeStatusTransport {
    func currentTransportContext() -> NativeMCPTransportContext?
    func fetchRealtimeStatus(
        expectedTransportContext: NativeMCPTransportContext,
        completion: @escaping (Result<Data, Error>) -> Void
    ) -> NativeMCPRequest
    func streamRealtimeStatusEvents(
        expectedTransportContext: NativeMCPTransportContext,
        onOpen: @escaping () -> Void,
        onData: @escaping (Data) -> Void,
        onEnd: @escaping (Error?) -> Void
    ) -> NativeMCPRequest
}

extension NativeMCPClient: RealtimeStatusTransport {
    func fetchRealtimeStatus(
        expectedTransportContext: NativeMCPTransportContext,
        completion: @escaping (Result<Data, Error>) -> Void
    ) -> NativeMCPRequest {
        call("realtime_status", arguments: [:], expectedTransportContext: expectedTransportContext, completion: completion)
    }

    func streamRealtimeStatusEvents(
        expectedTransportContext: NativeMCPTransportContext,
        onOpen: @escaping () -> Void,
        onData: @escaping (Data) -> Void,
        onEnd: @escaping (Error?) -> Void
    ) -> NativeMCPRequest {
        streamRealtimeStatus(
            expectedTransportContext: expectedTransportContext,
            onOpen: onOpen,
            onData: onData,
            onEnd: onEnd
        )
    }
}

enum RealtimeSSEMessage: Equatable {
    case keepalive
    case event(name: String, data: String)
}

enum RealtimeSSEParserError: Error, Equatable {
    case chunkTooLarge
    case lineTooLarge
    case eventTooLarge
    case tooManyMessages
    case invalidUTF8
}

/// Incremental latest-state SSE framing. Resume IDs and server retry hints are ignored.
struct RealtimeStatusSSEParser {
    static let maximumChunkBytes = 512 * 1024
    static let maximumLineBytes = 64 * 1024
    static let maximumEventBytes = 256 * 1024
    static let maximumMessagesPerChunk = 64

    private var lineBytes: [UInt8] = []
    private var eventName: String?
    private var dataLines: [String] = []
    private var eventDataBytes = 0
    private var hasSeenFirstLine = false
    private var skipLineFeedAfterCarriageReturn = false

    mutating func append(_ data: Data) throws -> [RealtimeSSEMessage] {
        guard data.count <= Self.maximumChunkBytes else { throw RealtimeSSEParserError.chunkTooLarge }
        var messages: [RealtimeSSEMessage] = []
        for byte in data {
            if skipLineFeedAfterCarriageReturn {
                skipLineFeedAfterCarriageReturn = false
                if byte == 10 { continue }
            }
            if byte == 13 {
                try consumeLine(into: &messages)
                skipLineFeedAfterCarriageReturn = true
            } else if byte == 10 {
                try consumeLine(into: &messages)
            } else {
                lineBytes.append(byte)
                guard lineBytes.count <= Self.maximumLineBytes else { throw RealtimeSSEParserError.lineTooLarge }
            }
        }
        guard messages.count <= Self.maximumMessagesPerChunk else { throw RealtimeSSEParserError.tooManyMessages }
        return messages
    }

    mutating func finish() throws -> [RealtimeSSEMessage] {
        var messages: [RealtimeSSEMessage] = []
        if !lineBytes.isEmpty { try consumeLine(into: &messages) }
        if let event = dispatchEvent() { messages.append(event) }
        guard messages.count <= Self.maximumMessagesPerChunk else { throw RealtimeSSEParserError.tooManyMessages }
        return messages
    }

    private mutating func consumeLine(into messages: inout [RealtimeSSEMessage]) throws {
        defer { lineBytes.removeAll(keepingCapacity: true) }
        if !hasSeenFirstLine {
            hasSeenFirstLine = true
            if lineBytes.starts(with: [0xEF, 0xBB, 0xBF]) { lineBytes.removeFirst(3) }
        }
        guard !lineBytes.isEmpty else {
            if let event = dispatchEvent() { messages.append(event) }
            guard messages.count <= Self.maximumMessagesPerChunk else { throw RealtimeSSEParserError.tooManyMessages }
            return
        }
        if lineBytes.first == 58 {
            messages.append(.keepalive)
            guard messages.count <= Self.maximumMessagesPerChunk else { throw RealtimeSSEParserError.tooManyMessages }
            return
        }

        let separator = lineBytes.firstIndex(of: 58)
        let fieldBytes = separator.map { Array(lineBytes[..<$0]) } ?? lineBytes
        var valueBytes = separator.map { Array(lineBytes[lineBytes.index(after: $0)...]) } ?? []
        if valueBytes.first == 32 { valueBytes.removeFirst() }
        guard let field = String(bytes: fieldBytes, encoding: .utf8),
              let value = String(bytes: valueBytes, encoding: .utf8) else {
            throw RealtimeSSEParserError.invalidUTF8
        }
        switch field {
        case "event": eventName = value
        case "data":
            eventDataBytes += value.utf8.count + 1
            guard eventDataBytes <= Self.maximumEventBytes else { throw RealtimeSSEParserError.eventTooLarge }
            dataLines.append(value)
        case "id", "retry": break
        default: break
        }
    }

    private mutating func dispatchEvent() -> RealtimeSSEMessage? {
        defer {
            eventName = nil
            dataLines.removeAll(keepingCapacity: true)
            eventDataBytes = 0
        }
        guard eventName != nil || !dataLines.isEmpty else { return nil }
        return .event(name: eventName ?? "message", data: dataLines.joined(separator: "\n"))
    }
}

private final class RealtimeStatusStreamDecoder {
    private let lock = NSLock()
    private var parser = RealtimeStatusSSEParser()

    func append(_ data: Data) -> Result<[RealtimeSSEMessage], Error> {
        lock.lock()
        defer { lock.unlock() }
        do { return .success(try parser.append(data)) }
        catch { return .failure(error) }
    }
}

enum RealtimeFeedConnectionState: Equatable {
    case stopped
    case waitingForBackend
    case connecting
    case connected
    case recovering
    case authenticationExpired
}

enum RealtimeFeedStatusSource: Equatable {
    case typedMCPQuery
    case sseLatestState
}

/// Coalesces committed-feed invalidations while preserving one in-flight read owner per view.
struct RealtimeReconciliationInvalidation {
    private var hasBaseline = false
    private var lastObservedTimestamp: String?
    private(set) var isPending = false

    @discardableResult
    mutating func observe(_ timestamp: String?, hasProjection: Bool = true, isVisible: Bool) -> Bool {
        guard hasProjection else { return false }
        guard hasBaseline else {
            hasBaseline = true
            lastObservedTimestamp = timestamp
            return false
        }
        guard let timestamp, timestamp != lastObservedTimestamp else { return false }
        lastObservedTimestamp = timestamp
        guard isVisible else { return false }
        isPending = true
        return true
    }

    mutating func takeIfReady(
        isVisible: Bool,
        readInFlight: Bool,
        writeInFlight: Bool,
        budgetAvailable: Bool = true
    ) -> Bool {
        guard isPending, isVisible, !readInFlight, !writeInFlight, budgetAvailable else { return false }
        isPending = false
        return true
    }

    mutating func clearPending() {
        isPending = false
    }
}

struct RealtimeFeedPresentation: Equatable {
    var connectionState: RealtimeFeedConnectionState = .stopped
    var reconnectDelaySeconds: Int?
    var status: RealtimeStatus?
    var statusSource: RealtimeFeedStatusSource?
    var statusLocallyReceivedAt: Date?
    var streamOpenedAt: Date?
    var lastKeepaliveLocallyReceivedAt: Date?
    var lastSSEStatusLocallyReceivedAt: Date?
}

final class RealtimeFeedMCPService {
    static let shared = RealtimeFeedMCPService()

    private static let reconnectDelays = [1, 2, 4, 8, 15]
    private static let minimumFallbackInterval: TimeInterval = 15

    private let transport: RealtimeStatusTransport
    private var observers: [UUID: (RealtimeFeedPresentation) -> Void] = [:]
    private(set) var presentation = RealtimeFeedPresentation()
    private var visible = false
    private var generation: UInt64 = 0
    private var attemptGeneration: UInt64 = 0
    private var activeAttempt: UInt64?
    private var activeContext: NativeMCPTransportContext?
    private var statusRequest: NativeMCPRequest?
    private var streamRequest: NativeMCPRequest?
    private var reconnectWork: DispatchWorkItem?
    private var backoffIndex = 0
    private var lastFallbackStartedAt: Date?

    init(transport: RealtimeStatusTransport = NativeMCPClient.shared) {
        self.transport = transport
    }

    @discardableResult
    func observe(_ handler: @escaping (RealtimeFeedPresentation) -> Void) -> UUID {
        precondition(Thread.isMainThread)
        let identifier = UUID()
        observers[identifier] = handler
        handler(presentation)
        return identifier
    }

    func removeObserver(_ identifier: UUID) {
        performOnMain { [weak self] in
            self?.observers.removeValue(forKey: identifier)
        }
    }

    func setVisible(_ shouldBeVisible: Bool) {
        precondition(Thread.isMainThread)
        guard visible != shouldBeVisible else { return }
        visible = shouldBeVisible
        generation &+= 1
        reconnectWork?.cancel()
        reconnectWork = nil
        statusRequest?.cancel()
        statusRequest = nil
        streamRequest?.cancel()
        streamRequest = nil
        activeAttempt = nil
        activeContext = nil

        if shouldBeVisible {
            backoffIndex = 0
            updatePresentation {
                $0.connectionState = .connecting
                $0.reconnectDelaySeconds = nil
            }
            connect(generation: generation, forceFallback: true)
        } else {
            updatePresentation {
                $0.connectionState = .stopped
                $0.reconnectDelaySeconds = nil
                $0.streamOpenedAt = nil
            }
        }
    }

    private func connect(generation expectedGeneration: UInt64, forceFallback: Bool) {
        guard isCurrent(expectedGeneration) else { return }
        guard let context = transport.currentTransportContext() else {
            updatePresentation { $0.connectionState = .waitingForBackend }
            scheduleReconnect(generation: expectedGeneration)
            return
        }

        activeContext = context
        updatePresentation {
            $0.connectionState = .connecting
            $0.reconnectDelaySeconds = nil
        }
        fetchStatusIfDue(context: context, generation: expectedGeneration, force: forceFallback)

        attemptGeneration &+= 1
        let expectedAttempt = attemptGeneration
        activeAttempt = expectedAttempt
        let decoder = RealtimeStatusStreamDecoder()
        let request = transport.streamRealtimeStatusEvents(
            expectedTransportContext: context,
            onOpen: { [weak self] in
                self?.performOnMain {
                    guard let self, self.isCurrentAttempt(expectedGeneration, context: context, attempt: expectedAttempt) else { return }
                    guard self.transport.currentTransportContext() == context else {
                        self.handleTransportContextChanged(generation: expectedGeneration, attempt: expectedAttempt)
                        return
                    }
                    self.updatePresentation {
                        $0.connectionState = .connected
                        $0.streamOpenedAt = Date()
                        $0.reconnectDelaySeconds = nil
                    }
                }
            },
            onData: { [weak self] data in
                let result = decoder.append(data)
                self?.performOnMain {
                    guard let self, self.isCurrentAttempt(expectedGeneration, context: context, attempt: expectedAttempt) else { return }
                    guard self.transport.currentTransportContext() == context else {
                        self.handleTransportContextChanged(generation: expectedGeneration, attempt: expectedAttempt)
                        return
                    }
                    switch result {
                    case .failure:
                        self.handleStreamFailure(generation: expectedGeneration, context: context, attempt: expectedAttempt, error: NativeMCPError.protocolError)
                    case .success(let messages):
                        for message in messages {
                            guard self.isCurrentAttempt(expectedGeneration, context: context, attempt: expectedAttempt) else { return }
                            self.consume(message, generation: expectedGeneration, context: context, attempt: expectedAttempt)
                        }
                    }
                }
            },
            onEnd: { [weak self] error in
                self?.performOnMain {
                    guard let self, self.isCurrentGeneration(expectedGeneration), self.activeAttempt == expectedAttempt else { return }
                    self.handleStreamFailure(generation: expectedGeneration, context: context, attempt: expectedAttempt, error: error)
                }
            }
        )
        streamRequest = request.isPending ? request : nil
    }

    private func consume(
        _ message: RealtimeSSEMessage,
        generation expectedGeneration: UInt64,
        context: NativeMCPTransportContext,
        attempt: UInt64
    ) {
        guard isCurrentAttempt(expectedGeneration, context: context, attempt: attempt) else { return }
        switch message {
        case .keepalive:
            updatePresentation {
                $0.connectionState = .connected
                $0.lastKeepaliveLocallyReceivedAt = Date()
                $0.reconnectDelaySeconds = nil
            }
        case .event(let name, let body):
            if name == "auth_expired" {
                activeAttempt = nil
                streamRequest?.cancel()
                streamRequest = nil
                updatePresentation {
                    $0.connectionState = .authenticationExpired
                    $0.reconnectDelaySeconds = nil
                }
                return
            }
            guard name == "snapshot" || name == "status" else {
                handleStreamFailure(generation: expectedGeneration, context: context, attempt: attempt, error: NativeMCPError.protocolError)
                return
            }
            guard let data = body.data(using: .utf8),
                  let status = try? JSONDecoder().decode(RealtimeStatus.self, from: data),
                  (try? status.validateTimestamps()) != nil else {
                handleStreamFailure(generation: expectedGeneration, context: context, attempt: attempt, error: NativeMCPError.protocolError)
                return
            }
            let receivedAt = Date()
            updatePresentation {
                $0.connectionState = .connected
                $0.reconnectDelaySeconds = nil
                $0.status = status
                $0.statusSource = .sseLatestState
                $0.statusLocallyReceivedAt = receivedAt
                $0.lastSSEStatusLocallyReceivedAt = receivedAt
            }
        }
    }

    private func fetchStatusIfDue(
        context: NativeMCPTransportContext,
        generation expectedGeneration: UInt64,
        force: Bool
    ) {
        guard statusRequest == nil || !statusRequest!.isPending else { return }
        let now = Date()
        if !force, let lastFallbackStartedAt,
           now.timeIntervalSince(lastFallbackStartedAt) < Self.minimumFallbackInterval { return }
        lastFallbackStartedAt = now
        let request = transport.fetchRealtimeStatus(expectedTransportContext: context) { [weak self] result in
            self?.performOnMain {
                guard let self, self.isCurrentGeneration(expectedGeneration) else { return }
                self.statusRequest = nil
                guard self.transport.currentTransportContext() == context else { return }
                switch result {
                case .failure(let error):
                    if self.isUnauthorized(error) {
                        self.endForAuthenticationExpiry()
                    }
                case .success(let data):
                    guard let status = try? JSONDecoder().decode(RealtimeStatus.self, from: data),
                          (try? status.validateTimestamps()) != nil else { return }
                    let receivedAt = Date()
                    self.updatePresentation {
                        $0.status = status
                        $0.statusSource = .typedMCPQuery
                        $0.statusLocallyReceivedAt = receivedAt
                    }
                }
            }
        }
        statusRequest = request.isPending ? request : nil
    }

    private func handleStreamFailure(
        generation expectedGeneration: UInt64,
        context: NativeMCPTransportContext,
        attempt: UInt64,
        error: Error?
    ) {
        guard isCurrentGeneration(expectedGeneration), activeAttempt == attempt else { return }
        activeAttempt = nil
        let endedRequest = streamRequest
        streamRequest = nil
        if let error, isUnauthorized(error) {
            endedRequest?.cancel()
            endForAuthenticationExpiry()
            return
        }
        endedRequest?.cancel()
        if transport.currentTransportContext() == context {
            fetchStatusIfDue(context: context, generation: expectedGeneration, force: false)
        } else {
            statusRequest?.cancel()
            statusRequest = nil
            activeContext = nil
        }
        scheduleReconnect(generation: expectedGeneration)
    }

    private func handleTransportContextChanged(generation expectedGeneration: UInt64, attempt: UInt64) {
        guard isCurrentGeneration(expectedGeneration), activeAttempt == attempt else { return }
        activeAttempt = nil
        streamRequest?.cancel()
        streamRequest = nil
        statusRequest?.cancel()
        statusRequest = nil
        activeContext = nil
        scheduleReconnect(generation: expectedGeneration)
    }

    private func endForAuthenticationExpiry() {
        activeAttempt = nil
        reconnectWork?.cancel()
        reconnectWork = nil
        streamRequest?.cancel()
        streamRequest = nil
        statusRequest?.cancel()
        statusRequest = nil
        updatePresentation {
            $0.connectionState = .authenticationExpired
            $0.reconnectDelaySeconds = nil
        }
    }

    private func scheduleReconnect(generation expectedGeneration: UInt64) {
        guard isCurrent(expectedGeneration), reconnectWork == nil else { return }
        if let openedAt = presentation.streamOpenedAt,
           Date().timeIntervalSince(openedAt) >= 30 {
            backoffIndex = 0
        }
        let delay = Self.reconnectDelays[min(backoffIndex, Self.reconnectDelays.count - 1)]
        backoffIndex = min(backoffIndex + 1, Self.reconnectDelays.count - 1)
        updatePresentation {
            $0.connectionState = .recovering
            $0.reconnectDelaySeconds = delay
        }
        let work = DispatchWorkItem { [weak self] in
            guard let self else { return }
            self.reconnectWork = nil
            self.connect(generation: expectedGeneration, forceFallback: false)
        }
        reconnectWork = work
        DispatchQueue.main.asyncAfter(deadline: .now() + .seconds(delay), execute: work)
    }

    private func isUnauthorized(_ error: Error) -> Bool {
        guard let error = error as? NativeMCPError else { return false }
        if case .unauthorized = error { return true }
        return false
    }

    private func isCurrent(_ expectedGeneration: UInt64) -> Bool {
        isCurrentGeneration(expectedGeneration) && visible
    }

    private func isCurrentGeneration(_ expectedGeneration: UInt64) -> Bool {
        generation == expectedGeneration
    }

    private func isCurrentAttempt(
        _ expectedGeneration: UInt64,
        context: NativeMCPTransportContext,
        attempt: UInt64
    ) -> Bool {
        isCurrent(expectedGeneration) && activeAttempt == attempt && activeContext == context
    }

    private func updatePresentation(_ update: (inout RealtimeFeedPresentation) -> Void) {
        update(&presentation)
        for observer in observers.values { observer(presentation) }
    }

    private func performOnMain(_ work: @escaping () -> Void) {
        if Thread.isMainThread { work() }
        else { DispatchQueue.main.async(execute: work) }
    }
}
