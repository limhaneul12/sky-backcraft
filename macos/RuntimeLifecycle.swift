import Foundation

enum RuntimeShutdownIntent: Equatable {
    case stop
    case restart(AppConfiguration)
    case quit
}

enum RuntimeShutdownAction: Equatable {
    case stopped
    case restart(AppConfiguration)
    case quit
}

enum RuntimeShutdownRequest: Equatable {
    case begin(UInt64)
    case coalesced
    case immediate(RuntimeShutdownAction)
}

final class RuntimeLifecycle {
    private enum Phase: Equatable {
        case stopped
        case starting(UInt64)
        case running(UInt64)
        case stopping(UInt64)
        case blocked(UInt64)
        case failed
    }

    private var phase: Phase = .stopped
    private var nextGeneration: UInt64 = 0
    private var pendingIntent: RuntimeShutdownIntent = .stop

    func beginStart() -> UInt64? {
        guard phase == .stopped || phase == .failed else { return nil }
        nextGeneration &+= 1
        phase = .starting(nextGeneration)
        pendingIntent = .stop
        return nextGeneration
    }

    func acceptsStartupCallback(_ generation: UInt64) -> Bool {
        phase == .starting(generation)
    }

    func owns(_ generation: UInt64) -> Bool {
        switch phase {
        case .starting(generation), .running(generation), .stopping(generation), .blocked(generation):
            return true
        default:
            return false
        }
    }

    func isStopping(_ generation: UInt64) -> Bool {
        phase == .stopping(generation)
    }

    func isBlocked(_ generation: UInt64) -> Bool {
        phase == .blocked(generation)
    }

    func markRunning(_ generation: UInt64) -> Bool {
        guard phase == .starting(generation) else { return false }
        phase = .running(generation)
        return true
    }

    func markFailed(_ generation: UInt64) -> Bool {
        guard owns(generation) else { return false }
        phase = .failed
        pendingIntent = .stop
        return true
    }

    func requestShutdown(_ intent: RuntimeShutdownIntent) -> RuntimeShutdownRequest {
        merge(intent)
        switch phase {
        case .stopped, .failed:
            return .immediate(takeAction())
        case .starting(let generation), .running(let generation):
            phase = .stopping(generation)
            return .begin(generation)
        case .stopping, .blocked:
            return .coalesced
        }
    }

    func finishShutdown(_ generation: UInt64) -> RuntimeShutdownAction? {
        guard phase == .stopping(generation) || phase == .blocked(generation) else { return nil }
        phase = .stopped
        return takeAction()
    }

    func blockShutdown(_ generation: UInt64) -> Bool {
        guard phase == .stopping(generation) else { return false }
        phase = .blocked(generation)
        return true
    }

    private func merge(_ intent: RuntimeShutdownIntent) {
        if pendingIntent == .quit { return }
        pendingIntent = intent
    }

    private func takeAction() -> RuntimeShutdownAction {
        let action: RuntimeShutdownAction
        switch pendingIntent {
        case .stop: action = .stopped
        case .restart(let configuration): action = .restart(configuration)
        case .quit: action = .quit
        }
        pendingIntent = .stop
        return action
    }
}

struct TunnelURLAccumulator {
    private let maximumBytes: Int
    private var bytes = Data()

    init(maximumBytes: Int = 8_192) {
        self.maximumBytes = max(1, maximumBytes)
    }

    var bufferedByteCount: Int { bytes.count }

    mutating func append(_ data: Data) -> String? {
        bytes.append(data)
        if bytes.count > maximumBytes { bytes = Data(bytes.suffix(maximumBytes)) }
        guard let text = String(data: bytes, encoding: .utf8),
              let range = text.range(
                of: #"[a-z0-9-]+\.trycloudflare\.com"#,
                options: .regularExpression
              ) else { return nil }
        let host = String(text[range])
        bytes.removeAll(keepingCapacity: true)
        return host
    }
}
