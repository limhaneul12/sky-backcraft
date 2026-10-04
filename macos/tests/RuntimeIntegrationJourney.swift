import Darwin
import Foundation

private func require(_ condition: @autoclosure () -> Bool, _ message: String) throws {
    if !condition() { throw AppError.runtime(message) }
}

@main
private enum RuntimeIntegrationJourney {
    static func main() {
        do {
            let python = try requiredEnvironmentURL("SKY_BACKCRAFT_TEST_PYTHON")
            let root = FileManager.default.temporaryDirectory
                .appendingPathComponent("sky-backcraft-runtime-journey-\(UUID().uuidString)", isDirectory: true)
            try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
            defer { try? FileManager.default.removeItem(at: root) }
            let childScript = root.appendingPathComponent("fake_child.py")
            try Data(fakeChildSource.utf8).write(to: childScript, options: .atomic)
            let executables = RuntimeExecutables(
                backend: RuntimeCommand(executableURL: python, prefixArguments: [childScript.path, "backend"]),
                tunnel: RuntimeCommand(executableURL: python, prefixArguments: [childScript.path, "tunnel"])
            )

            try stopRejectsLateTunnelURL(root: root, executables: executables)
            try overlappingRestartThenQuitJoinsOwnedChild(root: root, executables: executables)
            print("Sky Backcraft native runtime integration: PASS")
        } catch {
            fputs("Sky Backcraft native runtime integration: FAIL: \(error.localizedDescription)\n", stderr)
            exit(1)
        }
    }

    private static func stopRejectsLateTunnelURL(root: URL, executables: RuntimeExecutables) throws {
        let markers = root.appendingPathComponent("late-tunnel", isDirectory: true)
        try FileManager.default.createDirectory(at: markers, withIntermediateDirectories: true)
        setenv("SKY_BACKCRAFT_TEST_MARKERS", markers.path, 1)
        let store = try ConfigurationStore(supportDirectory: root.appendingPathComponent("late-store"))
        let runtime = RuntimeController(store: store, executableOverrides: executables)
        defer { drain(runtime) }
        let configuration = AppConfiguration(domain: "", port: try availablePort(), authenticationMode: .none)
        runtime.start(configuration)
        try waitUntil("fake tunnel start") { markerLines("tunnel_starts", in: markers).count == 1 }
        let tunnelPID = try markerPID("tunnel_starts", in: markers)
        runtime.stop()
        try waitUntil("late tunnel URL emission") { FileManager.default.fileExists(atPath: markers.appendingPathComponent("late_url").path) }
        try waitUntil("runtime stop") { runtime.statusSnapshot() == .stopped }
        try require(markerLines("backend_starts", in: markers).isEmpty, "stale tunnel URL started a backend")
        try require(!processExists(tunnelPID), "stopped tunnel child was not joined")
    }

    private static func overlappingRestartThenQuitJoinsOwnedChild(root: URL, executables: RuntimeExecutables) throws {
        let markers = root.appendingPathComponent("restart-quit", isDirectory: true)
        try FileManager.default.createDirectory(at: markers, withIntermediateDirectories: true)
        setenv("SKY_BACKCRAFT_TEST_MARKERS", markers.path, 1)
        let store = try ConfigurationStore(supportDirectory: root.appendingPathComponent("restart-store"))
        let runtime = RuntimeController(store: store, executableOverrides: executables)
        defer { drain(runtime) }
        let port = try availablePort()
        runtime.start(AppConfiguration(domain: "initial.example", port: port, authenticationMode: .none))
        try waitUntil("initial backend readiness") { runtime.statusSnapshot() == .running(host: "initial.example") }
        let backendPID = try markerPID("backend_starts", in: markers)

        runtime.restart(AppConfiguration(domain: "b.example", port: port, authenticationMode: .none))
        runtime.restart(AppConfiguration(domain: "c.example", port: port, authenticationMode: .oauth))
        var quitCompleted = false
        runtime.terminate { quitCompleted = true }
        try waitUntil("priority quit completion") { quitCompleted }
        try require(runtime.statusSnapshot() == .stopped, "runtime did not finish stopped after quit")
        try require(markerLines("backend_starts", in: markers).count == 1, "restart spawned a backend after priority quit")
        try require(!processExists(backendPID), "quit did not join the owned backend")
    }

    private static func waitUntil(
        _ description: String,
        timeout: TimeInterval = 8,
        condition: () -> Bool
    ) throws {
        let deadline = Date().addingTimeInterval(timeout)
        while Date() < deadline {
            if condition() { return }
            RunLoop.current.run(until: Date().addingTimeInterval(0.01))
        }
        throw AppError.runtime("timed out waiting for \(description)")
    }

    private static func drain(_ runtime: RuntimeController) {
        runtime.stop()
        let deadline = Date().addingTimeInterval(5)
        while Date() < deadline {
            if runtime.statusSnapshot() == .stopped { return }
            RunLoop.current.run(until: Date().addingTimeInterval(0.01))
        }
        fputs("native runtime integration cleanup did not reach stopped\n", stderr)
    }

    private static func markerLines(_ name: String, in directory: URL) -> [String] {
        let url = directory.appendingPathComponent(name)
        guard let text = try? String(contentsOf: url, encoding: .utf8) else { return [] }
        return text.split(separator: "\n").map(String.init)
    }

    private static func markerPID(_ name: String, in directory: URL) throws -> pid_t {
        guard let raw = markerLines(name, in: directory).last,
              let pid = pid_t(raw.split(separator: " ").first ?? "") else {
            throw AppError.runtime("missing PID marker \(name)")
        }
        return pid
    }

    private static func processExists(_ pid: pid_t) -> Bool {
        if kill(pid, 0) == 0 { return true }
        return errno != ESRCH
    }

    private static func requiredEnvironmentURL(_ name: String) throws -> URL {
        guard let value = ProcessInfo.processInfo.environment[name], !value.isEmpty else {
            throw AppError.runtime("missing \(name)")
        }
        return URL(fileURLWithPath: value)
    }

    private static func availablePort() throws -> Int {
        let descriptor = socket(AF_INET, SOCK_STREAM, 0)
        guard descriptor >= 0 else { throw AppError.runtime("test socket create failed") }
        defer { close(descriptor) }
        var address = sockaddr_in()
        address.sin_len = UInt8(MemoryLayout<sockaddr_in>.size)
        address.sin_family = sa_family_t(AF_INET)
        address.sin_addr = in_addr(s_addr: inet_addr("127.0.0.1"))
        let bound = withUnsafePointer(to: &address) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                Darwin.bind(descriptor, $0, socklen_t(MemoryLayout<sockaddr_in>.size))
            }
        }
        guard bound == 0 else { throw AppError.runtime("test socket bind failed") }
        var resolved = sockaddr_in()
        var length = socklen_t(MemoryLayout<sockaddr_in>.size)
        let read = withUnsafeMutablePointer(to: &resolved) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                getsockname(descriptor, $0, &length)
            }
        }
        guard read == 0 else { throw AppError.runtime("test socket name failed") }
        return Int(UInt16(bigEndian: resolved.sin_port))
    }

    private static let fakeChildSource = #"""
import os
import signal
import sys

markers = os.environ["SKY_BACKCRAFT_TEST_MARKERS"]
mode = sys.argv[1]

def append(name, value):
    with open(os.path.join(markers, name), "a", encoding="utf-8") as handle:
        handle.write(value + "\n")
        handle.flush()

def stop(signum, frame):
    if mode == "tunnel":
        print("https://late-output.trycloudflare.com", file=sys.stderr, flush=True)
        open(os.path.join(markers, "late_url"), "w", encoding="utf-8").close()
    append(mode + "_exits", str(os.getpid()))
    raise SystemExit(0)

signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)
append(mode + "_starts", str(os.getpid()) + " " + " ".join(sys.argv[2:]))
if mode == "backend":
    print('{"event":"mcp_listening"}', file=sys.stderr, flush=True)
while True:
    signal.pause()
"""#
}
