import Foundation

enum ReadFreshnessState: Equatable {
    case notObserved
    case loading
    case live
    case delayed
    case stale
    case offline
    case unknown
}

enum ReadFreshnessContext: Equatable {
    case liveProvider
    case historicalImmutable
}

enum ReadFreshnessFailure: Equatable {
    case disconnected
    case timeout
    case noData
    case missingProviderTimestamp
    case missingLocalReceivedAt
    case futureTimestamp
    case regressedTimestamp
    case incompleteSnapshot
    case historicalContext
    case other(String)
}

struct ReadFreshnessClock {
    let wallNow: Date
    let monotonicNow: TimeInterval
}

struct ReadFreshnessSnapshot<Value> {
    let value: Value
    let providerObservedAt: Date
    let localReceivedAt: Date
    fileprivate let monotonicAcceptedAt: TimeInterval
    fileprivate let providerAgeAtAcceptance: TimeInterval
}

struct ReadFreshness<Value> {
    private static var liveLimit: TimeInterval { 30 }
    private static var delayedLimit: TimeInterval { 120 }
    private static var futureTolerance: TimeInterval { 5 }

    let context: ReadFreshnessContext
    private(set) var state: ReadFreshnessState = .notObserved
    private(set) var lastSuccess: ReadFreshnessSnapshot<Value>?
    private(set) var failureReason: ReadFreshnessFailure?
    private(set) var requestStartedAt: Date?

    init(context: ReadFreshnessContext = .liveProvider) {
        self.context = context
    }

    mutating func markStart(clock: ReadFreshnessClock) {
        requestStartedAt = clock.wallNow
        state = .loading
    }

    @discardableResult
    mutating func acceptSnapshot(
        _ value: Value,
        providerObservedAtUTC: String?,
        localReceivedAt: Date?,
        isComplete: Bool = true,
        clock: ReadFreshnessClock
    ) -> Bool {
        guard context == .liveProvider else { return reject(.historicalContext) }
        guard isComplete else { return reject(.incompleteSnapshot) }
        guard let providerObservedAtUTC,
              let providerObservedAt = Self.parseProviderTimestamp(providerObservedAtUTC) else {
            return reject(.missingProviderTimestamp)
        }
        guard let localReceivedAt else { return reject(.missingLocalReceivedAt) }
        guard providerObservedAt.timeIntervalSince(clock.wallNow) <= Self.futureTolerance,
              localReceivedAt.timeIntervalSince(clock.wallNow) <= Self.futureTolerance else {
            return reject(.futureTimestamp)
        }
        if let previous = lastSuccess,
           providerObservedAt < previous.providerObservedAt {
            return reject(.regressedTimestamp)
        }

        let providerAgeAtReceipt = max(0, localReceivedAt.timeIntervalSince(providerObservedAt))
        let receiptToAcceptance = max(0, clock.wallNow.timeIntervalSince(localReceivedAt))
        let snapshot = ReadFreshnessSnapshot(
            value: value,
            providerObservedAt: providerObservedAt,
            localReceivedAt: localReceivedAt,
            monotonicAcceptedAt: clock.monotonicNow,
            providerAgeAtAcceptance: providerAgeAtReceipt + receiptToAcceptance
        )
        lastSuccess = snapshot
        failureReason = nil
        requestStartedAt = nil
        state = Self.state(forAge: snapshot.providerAgeAtAcceptance)
        return true
    }

    mutating func acceptNoData(clock: ReadFreshnessClock) {
        _ = clock
        _ = reject(.noData)
    }

    mutating func fail(_ reason: ReadFreshnessFailure, clock: ReadFreshnessClock) {
        _ = clock
        failureReason = reason
        requestStartedAt = nil
        switch reason {
        case .disconnected, .timeout:
            state = lastSuccess == nil ? .offline : .stale
        default:
            state = .unknown
        }
    }

    func freshness(clock: ReadFreshnessClock) -> ReadFreshnessState {
        guard context == .liveProvider else { return .unknown }
        guard let lastSuccess else { return state }
        if failureReason != nil || state == .loading || state == .offline || state == .unknown {
            return state
        }
        let elapsed = max(0, clock.monotonicNow - lastSuccess.monotonicAcceptedAt)
        return Self.state(forAge: lastSuccess.providerAgeAtAcceptance + elapsed)
    }

    mutating func refresh(clock: ReadFreshnessClock) {
        state = freshness(clock: clock)
    }

    static func parseProviderTimestamp(_ raw: String) -> Date? {
        guard raw.hasSuffix("Z") || raw.hasSuffix("+00:00") else { return nil }
        let precise = ISO8601DateFormatter()
        precise.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        if let date = precise.date(from: raw) { return date }
        let ordinary = ISO8601DateFormatter()
        ordinary.formatOptions = [.withInternetDateTime]
        return ordinary.date(from: raw)
    }

    @discardableResult
    private mutating func reject(_ reason: ReadFreshnessFailure) -> Bool {
        failureReason = reason
        requestStartedAt = nil
        state = .unknown
        return false
    }

    private static func state(forAge age: TimeInterval) -> ReadFreshnessState {
        if age <= liveLimit { return .live }
        if age <= delayedLimit { return .delayed }
        return .stale
    }
}
