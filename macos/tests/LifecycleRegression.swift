import Foundation

private func require(_ condition: @autoclosure () -> Bool, _ message: String) throws {
    if !condition() { throw AppError.runtime(message) }
}

@main
private enum LifecycleRegression {
    static func main() {
        do {
            try staleTunnelCallbackIsRejectedAfterStop()
            try overlappingRestartKeepsOneSupervisorAndQuitWins()
            try splitTunnelURLIsRecoveredWithinBound()
            print("Sky Backcraft native lifecycle regression: PASS")
        } catch {
            fputs("Sky Backcraft native lifecycle regression: FAIL: \(error.localizedDescription)\n", stderr)
            exit(1)
        }
    }

    private static func staleTunnelCallbackIsRejectedAfterStop() throws {
        let lifecycle = RuntimeLifecycle()
        let generation = try requireValue(lifecycle.beginStart(), "start generation missing")
        let request = lifecycle.requestShutdown(.stop)
        try require(request == .begin(generation), "stop did not start exactly one supervisor")
        try require(!lifecycle.acceptsStartupCallback(generation), "stale tunnel callback remained authorized")
    }

    private static func overlappingRestartKeepsOneSupervisorAndQuitWins() throws {
        let lifecycle = RuntimeLifecycle()
        let generation = try requireValue(lifecycle.beginStart(), "start generation missing")
        let configB = AppConfiguration(domain: "b.example", port: 8130, authenticationMode: .none)
        let configC = AppConfiguration(domain: "c.example", port: 8130, authenticationMode: .oauth)
        try require(lifecycle.requestShutdown(.restart(configB)) == .begin(generation), "first restart did not own supervisor")
        try require(lifecycle.requestShutdown(.restart(configC)) == .coalesced, "second restart created another supervisor")
        try require(lifecycle.finishShutdown(generation) == .restart(configC), "latest restart configuration was not retained")
        try require(lifecycle.finishShutdown(generation) == nil, "restart shutdown completed more than once")

        let quitLifecycle = RuntimeLifecycle()
        let quitGeneration = try requireValue(quitLifecycle.beginStart(), "quit generation missing")
        try require(quitLifecycle.requestShutdown(.restart(configB)) == .begin(quitGeneration), "quit setup did not own supervisor")
        try require(quitLifecycle.requestShutdown(.restart(configC)) == .coalesced, "quit setup created another supervisor")
        try require(quitLifecycle.requestShutdown(.quit) == .coalesced, "quit created another supervisor")
        try require(quitLifecycle.finishShutdown(quitGeneration) == .quit, "quit did not cancel pending restart")
        try require(quitLifecycle.finishShutdown(quitGeneration) == nil, "quit shutdown completed more than once")
    }

    private static func splitTunnelURLIsRecoveredWithinBound() throws {
        var buffer = TunnelURLAccumulator(maximumBytes: 96)
        try require(buffer.append(Data("prefix https://split-host.trycloud".utf8)) == nil, "partial URL matched")
        try require(buffer.append(Data("flare.com suffix".utf8)) == "split-host.trycloudflare.com", "split URL was not recovered")
        _ = buffer.append(Data(repeating: 0x61, count: 256))
        try require(buffer.bufferedByteCount <= 96, "tunnel URL buffer exceeded its bound")
    }

    private static func requireValue<T>(_ value: T?, _ message: String) throws -> T {
        guard let value else { throw AppError.runtime(message) }
        return value
    }
}
