import Foundation
import Darwin

final class BoundedLogWriter {
    private let url: URL
    private let maximumBytes: UInt64
    private var handle: FileHandle?

    init(url: URL, maximumBytes: UInt64 = 2_000_000) {
        self.url = url
        self.maximumBytes = maximumBytes
    }

    func open() throws {
        try close()
        if let size = fileSize(), size > maximumBytes {
            try rotate()
            return
        }
        if !FileManager.default.fileExists(atPath: url.path),
           !FileManager.default.createFile(atPath: url.path, contents: nil) {
            throw AppError.runtime("로그 파일을 만들지 못했습니다.")
        }
        let opened = try FileHandle(forWritingTo: url)
        try opened.seekToEnd()
        handle = opened
    }

    func write(_ data: Data) throws {
        if handle == nil { try open() }
        let bounded = data.count > Int(maximumBytes) ? Data(data.suffix(Int(maximumBytes))) : data
        let offset = try handle?.offset() ?? 0
        if offset + UInt64(bounded.count) > maximumBytes { try rotate() }
        try handle?.write(contentsOf: bounded)
    }

    func close() throws {
        try handle?.close()
        handle = nil
    }

    private func rotate() throws {
        try close()
        let previous = url.appendingPathExtension("previous")
        try? FileManager.default.removeItem(at: previous)
        if FileManager.default.fileExists(atPath: url.path) {
            try FileManager.default.moveItem(at: url, to: previous)
        }
        guard FileManager.default.createFile(atPath: url.path, contents: nil) else {
            throw AppError.runtime("회전된 로그 파일을 만들지 못했습니다.")
        }
        handle = try FileHandle(forWritingTo: url)
    }

    private func fileSize() -> UInt64? {
        let attributes = try? FileManager.default.attributesOfItem(atPath: url.path)
        return (attributes?[.size] as? NSNumber)?.uint64Value
    }
}

enum RuntimeStatus: Equatable {
    case stopped, starting, stopping
    case running(host: String)
    case failed(String)
    case blocked(String)

    var menuTitle: String {
        switch self {
        case .stopped: return "MCP: 중지됨"
        case .starting: return "MCP: 시작 중…"
        case .stopping: return "MCP: 중지 중…"
        case .running(let host): return "MCP: 실행 중 (\(host))"
        case .failed(let message): return "MCP 오류: \(message)"
        case .blocked(let message): return "MCP 종료 오류: \(message)"
        }
    }
}

struct RuntimeCommand {
    let executableURL: URL
    let prefixArguments: [String]

    init(executableURL: URL, prefixArguments: [String] = []) {
        self.executableURL = executableURL
        self.prefixArguments = prefixArguments
    }
}

struct RuntimeExecutables {
    let backend: RuntimeCommand
    let tunnel: RuntimeCommand
}

private enum ChildRole {
    case backend
    case tunnel
}

final class RuntimeController {
    private let queue = DispatchQueue(label: "store.skybackcraft.runtime")
    private let queueKey = DispatchSpecificKey<UInt8>()
    private let store: ConfigurationStore
    private let executableOverrides: RuntimeExecutables?
    private let lifecycle = RuntimeLifecycle()
    private var backend: Process?
    private var tunnel: Process?
    private let logWriter: BoundedLogWriter
    private var currentConfiguration: AppConfiguration?
    private var currentHost: String?
    private var ownerCode: String?
    private var readinessBuffer = ""
    private var tunnelURLAccumulator = TunnelURLAccumulator()
    private var tunnelTimeout: DispatchWorkItem?
    private var backendReadinessTimeout: DispatchWorkItem?
    private var failureAfterShutdown: String?
    private var quitCompletion: (() -> Void)?
    private(set) var status: RuntimeStatus = .stopped
    var statusDidChange: ((RuntimeStatus) -> Void)?

    init(store: ConfigurationStore, executableOverrides: RuntimeExecutables? = nil) {
        self.store = store
        self.executableOverrides = executableOverrides
        logWriter = BoundedLogWriter(url: store.logURL)
        queue.setSpecific(key: queueKey, value: 1)
    }

    static func backendArguments(
        configuration raw: AppConfiguration,
        dataDirectory: URL,
        effectiveHost: String? = nil
    ) throws -> [String] {
        let configuration = try raw.validated()
        let host = configuration.domain.isEmpty ? effectiveHost : configuration.domain
        var arguments = ["mcp-serve", "--bind", "127.0.0.1", "--port", String(configuration.port), "--data-root", dataDirectory.path]
        if let host, !host.isEmpty { arguments += ["--allow-host", host] }
        switch configuration.authenticationMode {
        case .none:
            arguments.append("--public-no-auth")
        case .oauth:
            guard let host, !host.isEmpty else { return arguments }
            arguments += ["--oauth", "--public-url", "https://\(host)"]
        }
        return arguments
    }

    func start(_ configuration: AppConfiguration) { queue.async { [weak self] in self?.startLocked(configuration) } }
    func restart(_ configuration: AppConfiguration) { queue.async { [weak self] in self?.requestShutdownLocked(.restart(configuration)) } }
    func stop() { queue.async { [weak self] in self?.requestShutdownLocked(.stop) } }
    func terminate(completion: @escaping () -> Void) {
        queue.async { [weak self] in
            self?.quitCompletion = completion
            self?.requestShutdownLocked(.quit)
        }
    }
    func connectorURL() -> String? { queue.sync { currentHost.map { "https://\($0)/mcp" } } }
    func oauthLoginCode() -> String? { queue.sync { ownerCode } }
    func statusSnapshot() -> RuntimeStatus { queue.sync { status } }

    private func startLocked(_ raw: AppConfiguration) {
        guard backend == nil, tunnel == nil, let generation = lifecycle.beginStart() else { return }
        do {
            let configuration = try raw.validated()
            try ensurePortAvailable(configuration.port)
            try logWriter.open()
            currentConfiguration = configuration
            setStatus(.starting)
            if configuration.domain.isEmpty { try startQuickTunnel(configuration, generation: generation) }
            else { try startBackend(configuration, host: configuration.domain, generation: generation) }
        } catch { failLocked(error.localizedDescription, generation: generation) }
    }

    private func startQuickTunnel(_ configuration: AppConfiguration, generation: UInt64) throws {
        guard let command = executableCommand(named: "cloudflared", allowPathLookup: true) else {
            throw AppError.runtime("Quick Tunnel을 사용하려면 cloudflared가 필요합니다.")
        }
        let process = Process()
        let pipe = Pipe()
        process.executableURL = command.executableURL
        process.arguments = command.prefixArguments + [
            "tunnel", "--url", "http://127.0.0.1:\(configuration.port)", "--no-autoupdate"
        ]
        process.standardOutput = pipe
        process.standardError = pipe
        tunnel = process
        tunnelURLAccumulator = TunnelURLAccumulator()
        pipe.fileHandleForReading.readabilityHandler = { [weak self, weak process] handle in
            let data = handle.availableData
            guard !data.isEmpty else { return }
            self?.syncOnOwnerQueue { [weak self, weak process] in
                guard let self,
                      self.tunnel === process,
                      self.lifecycle.acceptsStartupCallback(generation) else { return }
                try? self.logWriter.write(data)
                if let host = self.tunnelURLAccumulator.append(data) {
                    self.quickTunnelReady(host, generation: generation, process: process)
                }
            }
        }
        process.terminationHandler = { [weak self] process in
            self?.queue.async {
                self?.childExited(
                    .tunnel,
                    process: process,
                    generation: generation,
                    "Quick Tunnel이 종료되었습니다. (코드 \(process.terminationStatus))",
                )
            }
        }
        try process.run()
        appendLogLine("cloudflared 시작")
        let timeout = DispatchWorkItem { [weak self, weak process] in
            guard let self,
                  self.tunnel === process,
                  self.backend == nil,
                  self.lifecycle.acceptsStartupCallback(generation) else { return }
            self.beginFailureShutdown("Quick Tunnel 주소를 20초 안에 받지 못했습니다.")
        }
        tunnelTimeout = timeout
        queue.asyncAfter(deadline: .now() + 20, execute: timeout)
    }

    private func quickTunnelReady(_ host: String, generation: UInt64, process: Process?) {
        guard lifecycle.acceptsStartupCallback(generation),
              tunnel === process,
              backend == nil,
              let configuration = currentConfiguration else { return }
        tunnelTimeout?.cancel()
        tunnelTimeout = nil
        do { try startBackend(configuration, host: host, generation: generation) }
        catch {
            beginFailureShutdown("MCP 시작 실패: \(error.localizedDescription)")
        }
    }

    private func startBackend(_ configuration: AppConfiguration, host: String, generation: UInt64) throws {
        guard let command = executableCommand(named: "spot-lab", allowPathLookup: false) else {
            throw AppError.runtime("앱 번들에서 spot-lab 실행 파일을 찾지 못했습니다.")
        }
        let arguments = try Self.backendArguments(
            configuration: configuration,
            dataDirectory: store.dataDirectory(for: configuration.authenticationMode),
            effectiveHost: host
        )
        let process = Process()
        let output = Pipe()
        process.executableURL = command.executableURL
        process.arguments = command.prefixArguments + arguments
        process.standardOutput = output
        process.standardError = output
        if configuration.authenticationMode == .oauth {
            let code = try OwnerCode.make()
            ownerCode = code
            var environment = ProcessInfo.processInfo.environment
            environment["SPOT_LAB_OAUTH_OWNER_CODE"] = code
            process.environment = environment
        } else { ownerCode = nil }
        backend = process
        currentHost = host
        readinessBuffer = ""
        output.fileHandleForReading.readabilityHandler = { [weak self, weak process] handle in
            let data = handle.availableData
            guard !data.isEmpty else { return }
            self?.syncOnOwnerQueue { [weak self, weak process] in
                guard let self, self.backend === process else { return }
                try? self.logWriter.write(data)
                guard self.lifecycle.acceptsStartupCallback(generation) else { return }
                self.readinessBuffer += String(data: data, encoding: .utf8) ?? ""
                if self.readinessBuffer.count > 8_192 {
                    self.readinessBuffer = String(self.readinessBuffer.suffix(4_096))
                }
                if self.readinessBuffer.contains("mcp_listening") {
                    self.readinessBuffer = ""
                    if self.lifecycle.markRunning(generation) {
                        self.backendReadinessTimeout?.cancel()
                        self.backendReadinessTimeout = nil
                        self.setStatus(.running(host: host))
                    }
                }
            }
        }
        process.terminationHandler = { [weak self] process in
            self?.queue.async {
                self?.childExited(
                    .backend,
                    process: process,
                    generation: generation,
                    "MCP가 종료되었습니다. (코드 \(process.terminationStatus))",
                )
            }
        }
        try process.run()
        appendLogLine("spot-lab 시작: 127.0.0.1:\(configuration.port), host=\(host)")
        let readinessTimeout = DispatchWorkItem { [weak self, weak process] in
            guard let self,
                  self.backend === process,
                  self.lifecycle.acceptsStartupCallback(generation) else { return }
            self.beginFailureShutdown("MCP가 15초 안에 준비되지 않았습니다.")
        }
        backendReadinessTimeout = readinessTimeout
        queue.asyncAfter(deadline: .now() + 15, execute: readinessTimeout)
    }

    private func requestShutdownLocked(_ intent: RuntimeShutdownIntent) {
        switch lifecycle.requestShutdown(intent) {
        case .begin(let generation):
            beginShutdownSupervisor(generation)
        case .coalesced:
            break
        case .immediate(let action):
            resetLocked()
            performShutdownAction(action)
        }
    }

    private func beginShutdownSupervisor(_ generation: UInt64) {
        tunnelTimeout?.cancel()
        tunnelTimeout = nil
        backendReadinessTimeout?.cancel()
        backendReadinessTimeout = nil
        let processes = [backend, tunnel].compactMap { $0 }
        setStatus(.stopping)
        guard !processes.isEmpty else { finishShutdown(generation); return }
        processes.filter(\.isRunning).forEach { $0.interrupt() }
        waitForGracefulExit(processes, generation: generation, deadline: Date().addingTimeInterval(35))
    }

    private func waitForGracefulExit(_ processes: [Process], generation: UInt64, deadline: Date) {
        let live = processes.filter(\.isRunning)
        if live.isEmpty { finishShutdown(generation); return }
        if Date() >= deadline {
            live.forEach { $0.terminate() }
            appendLogLine("정상 종료 대기 시간이 지나 실행 프로세스에 terminate를 보냈습니다.")
            waitAfterTerminate(processes, generation: generation, deadline: Date().addingTimeInterval(3))
            return
        }
        queue.asyncAfter(deadline: .now() + 0.1) { [weak self] in
            self?.waitForGracefulExit(processes, generation: generation, deadline: deadline)
        }
    }

    private func waitAfterTerminate(_ processes: [Process], generation: UInt64, deadline: Date) {
        let live = processes.filter(\.isRunning)
        if live.isEmpty { finishShutdown(generation); return }
        if Date() >= deadline {
            live.forEach { process in
                if process.isRunning { Darwin.kill(process.processIdentifier, SIGKILL) }
            }
            appendLogLine("terminate 대기 시간이 지나 실행 프로세스에 SIGKILL을 보냈습니다.")
            waitAfterKill(processes, generation: generation, deadline: Date().addingTimeInterval(2))
            return
        }
        queue.asyncAfter(deadline: .now() + 0.1) { [weak self] in
            self?.waitAfterTerminate(processes, generation: generation, deadline: deadline)
        }
    }

    private func waitAfterKill(_ processes: [Process], generation: UInt64, deadline: Date) {
        let live = processes.filter(\.isRunning)
        if live.isEmpty { finishShutdown(generation); return }
        if Date() >= deadline {
            let message = "소유한 프로세스 \(live.count)개가 종료되지 않아 재시작을 차단했습니다."
            appendLogLine("오류: \(message)")
            if lifecycle.blockShutdown(generation) { setStatus(.blocked(message)) }
            waitForBlockedExit(processes, generation: generation)
            return
        }
        queue.asyncAfter(deadline: .now() + 0.1) { [weak self] in
            self?.waitAfterKill(processes, generation: generation, deadline: deadline)
        }
    }

    private func waitForBlockedExit(_ processes: [Process], generation: UInt64) {
        guard lifecycle.isBlocked(generation) else { return }
        if processes.allSatisfy({ !$0.isRunning }) {
            finishShutdown(generation)
            return
        }
        queue.asyncAfter(deadline: .now() + 1) { [weak self] in
            self?.waitForBlockedExit(processes, generation: generation)
        }
    }

    private func finishShutdown(_ generation: UInt64) {
        guard let action = lifecycle.finishShutdown(generation) else { return }
        resetLocked()
        performShutdownAction(action)
    }

    private func performShutdownAction(_ action: RuntimeShutdownAction) {
        let failure = failureAfterShutdown
        failureAfterShutdown = nil
        switch action {
        case .stopped:
            setStatus(failure.map(RuntimeStatus.failed) ?? .stopped)
        case .restart(let configuration):
            setStatus(.stopped)
            startLocked(configuration)
        case .quit:
            setStatus(.stopped)
            let completion = quitCompletion
            quitCompletion = nil
            DispatchQueue.main.async { completion?() }
        }
    }

    private func failLocked(_ message: String, generation: UInt64) {
        appendLogLine("오류: \(message)")
        _ = lifecycle.markFailed(generation)
        resetLocked()
        setStatus(.failed(message))
    }

    private func childExited(
        _ role: ChildRole,
        process: Process,
        generation: UInt64,
        _ message: String
    ) {
        guard lifecycle.owns(generation) else { return }
        switch role {
        case .backend:
            guard backend === process else { return }
            backend = nil
            backendReadinessTimeout?.cancel()
            backendReadinessTimeout = nil
        case .tunnel:
            guard tunnel === process else { return }
            tunnel = nil
        }
        guard !lifecycle.isStopping(generation), !lifecycle.isBlocked(generation) else { return }
        beginFailureShutdown(message)
    }

    private func beginFailureShutdown(_ message: String) {
        failureAfterShutdown = message
        requestShutdownLocked(.stop)
    }

    private func resetLocked() {
        backend = nil
        tunnel = nil
        currentConfiguration = nil
        currentHost = nil
        ownerCode = nil
        readinessBuffer = ""
        tunnelURLAccumulator = TunnelURLAccumulator()
        tunnelTimeout?.cancel()
        tunnelTimeout = nil
        backendReadinessTimeout?.cancel()
        backendReadinessTimeout = nil
        try? logWriter.close()
    }

    private func setStatus(_ value: RuntimeStatus) {
        status = value
        DispatchQueue.main.async { [weak self] in self?.statusDidChange?(value) }
    }

    private func appendLogLine(_ line: String) {
        guard let data = "[\(ISO8601DateFormatter().string(from: Date()))] \(line)\n".data(using: .utf8) else { return }
        try? logWriter.write(data)
    }

    private func syncOnOwnerQueue(_ operation: () -> Void) {
        if DispatchQueue.getSpecific(key: queueKey) != nil { operation() }
        else { queue.sync(execute: operation) }
    }
    private func executableCommand(named name: String, allowPathLookup: Bool) -> RuntimeCommand? {
        if let executableOverrides {
            return name == "spot-lab" ? executableOverrides.backend : executableOverrides.tunnel
        }
        if let bundled = Bundle.main.executableURL?.deletingLastPathComponent().appendingPathComponent(name),
           FileManager.default.isExecutableFile(atPath: bundled.path) {
            return RuntimeCommand(executableURL: bundled)
        }
        guard allowPathLookup else { return nil }
        return ["/opt/homebrew/bin/\(name)", "/usr/local/bin/\(name)", "/usr/bin/\(name)"]
            .first(where: FileManager.default.isExecutableFile(atPath:))
            .map { RuntimeCommand(executableURL: URL(fileURLWithPath: $0)) }
    }

    private func ensurePortAvailable(_ port: Int) throws {
        let descriptor = socket(AF_INET, SOCK_STREAM, 0)
        guard descriptor >= 0 else { throw AppError.runtime("포트를 확인하지 못했습니다.") }
        defer { close(descriptor) }
        var reuseAddress: Int32 = 1
        guard setsockopt(
            descriptor,
            SOL_SOCKET,
            SO_REUSEADDR,
            &reuseAddress,
            socklen_t(MemoryLayout<Int32>.size)
        ) == 0 else {
            throw AppError.runtime("포트 재사용 가능 여부를 확인하지 못했습니다.")
        }
        var address = sockaddr_in()
        address.sin_len = UInt8(MemoryLayout<sockaddr_in>.size)
        address.sin_family = sa_family_t(AF_INET)
        address.sin_port = in_port_t(port).bigEndian
        address.sin_addr = in_addr(s_addr: inet_addr("127.0.0.1"))
        let result = withUnsafePointer(to: &address) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { Darwin.bind(descriptor, $0, socklen_t(MemoryLayout<sockaddr_in>.size)) }
        }
        guard result == 0 else { throw AppError.runtime("포트 \(port)을 이미 다른 프로세스가 사용 중입니다.") }
    }

}
