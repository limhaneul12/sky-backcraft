import AppKit

private struct RealtimeStatusFixtureCase {
    let state: String
    let connectionCount: UInt32
    let subscriptionCount: UInt32
    let lastReceived: String?
    let lastReconciled: String?
    let reconnectCount: UInt64
    let droppedMessages: UInt64
    let readyForDecisions: Bool
}

private final class RealtimeStatusFixtureTransport: RealtimeStatusTransport {
    private let context = NativeMCPTransportContext(generation: 1, owned_helper: nil)
    private let statusData: Data

    init(_ fixture: RealtimeStatusFixtureCase) {
        let feed: [String: Any] = [
            "state": fixture.state,
            "connection_count": fixture.connectionCount,
            "subscription_count": fixture.subscriptionCount,
            "last_received_at_utc": fixture.lastReceived as Any? ?? NSNull(),
            "last_reconciled_at_utc": fixture.lastReconciled as Any? ?? NSNull(),
            "reconnect_count": fixture.reconnectCount,
            "dropped_messages": fixture.droppedMessages,
            "ready_for_decisions": fixture.readyForDecisions
        ]
        let status: [String: Any] = [
            "observed_at_utc": "2026-10-17T04:00:00Z",
            "market": feed,
            "private": feed
        ]
        statusData = try! JSONSerialization.data(withJSONObject: status)
    }

    func currentTransportContext() -> NativeMCPTransportContext? { context }

    func fetchRealtimeStatus(
        expectedTransportContext: NativeMCPTransportContext,
        completion: @escaping (Result<Data, Error>) -> Void
    ) -> NativeMCPRequest {
        guard expectedTransportContext == context else {
            completion(.failure(NativeMCPError.transportContextChanged))
            return .alreadyFinished()
        }
        completion(.success(statusData))
        return .alreadyFinished()
    }

    func streamRealtimeStatusEvents(
        expectedTransportContext: NativeMCPTransportContext,
        onOpen: @escaping () -> Void,
        onData: @escaping (Data) -> Void,
        onEnd: @escaping (Error?) -> Void
    ) -> NativeMCPRequest {
        guard expectedTransportContext == context else {
            onEnd(NativeMCPError.transportContextChanged)
            return .alreadyFinished()
        }
        onOpen()
        let body = String(decoding: statusData, as: UTF8.self)
        onData(Data("event: snapshot\ndata: \(body)\n\n".utf8))
        return .alreadyFinished()
    }
}

private final class RealtimeStatusFixtureWindowController: NSWindowController, NSWindowDelegate {
    private let services: [RealtimeFeedMCPService]
    private let statusViews: [RealtimeConnectionStatusView]
    private let diagnosticsLabel = NSTextField(wrappingLabelWithString: "fixture services: not started")
    private var didStartFixtureStreams = false

    init() {
        let cases = [
            RealtimeStatusFixtureCase(
                state: "READY",
                connectionCount: 1,
                subscriptionCount: 3,
                lastReceived: "2026-10-17T03:59:58Z",
                lastReconciled: "2026-10-17T03:59:00Z",
                reconnectCount: 0,
                droppedMessages: 0,
                readyForDecisions: true
            ),
            RealtimeStatusFixtureCase(
                state: "RECOVERING",
                connectionCount: 1,
                subscriptionCount: 3,
                lastReceived: "2026-10-17T03:54:00Z",
                lastReconciled: "2026-10-17T03:53:00Z",
                reconnectCount: 2,
                droppedMessages: 4,
                readyForDecisions: false
            ),
            RealtimeStatusFixtureCase(
                state: "STALE",
                connectionCount: 1,
                subscriptionCount: 3,
                lastReceived: "2026-10-17T03:00:00Z",
                lastReconciled: "2026-10-17T02:59:00Z",
                reconnectCount: 3,
                droppedMessages: 7,
                readyForDecisions: false
            ),
            RealtimeStatusFixtureCase(
                state: "UNAVAILABLE",
                connectionCount: 0,
                subscriptionCount: 0,
                lastReceived: nil,
                lastReconciled: nil,
                reconnectCount: 0,
                droppedMessages: 0,
                readyForDecisions: false
            )
        ]
        services = cases.map { RealtimeFeedMCPService(transport: RealtimeStatusFixtureTransport($0)) }
        statusViews = services.map { RealtimeConnectionStatusView(service: $0) }

        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 1280, height: 980),
            styleMask: [.titled, .closable, .miniaturizable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.title = "Realtime Status · GUI Fixture"
        window.contentMinSize = NSSize(width: 960, height: 720)
        window.isReleasedWhenClosed = false
        window.appearance = nil
        window.backgroundColor = TerminalTheme.surface
        super.init(window: window)
        window.delegate = self

        buildFixture()
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) { nil }

    func startFixtureStreams() {
        guard let window, window.isVisible, !window.isMiniaturized,
              window.occlusionState.contains(.visible) else { return }
        guard !didStartFixtureStreams else { return }
        didStartFixtureStreams = true
        for service in services { service.setVisible(true) }
        diagnosticsLabel.stringValue = services.enumerated().map { index, service in
            let state = service.presentation.status?.market.state.rawValue ?? "NO_STATUS"
            return "fixture \(index + 1): \(service.presentation.connectionState) · \(state)"
        }.joined(separator: "\n")
    }

    func windowDidBecomeKey(_ notification: Notification) {
        startFixtureStreams()
    }

    func windowDidChangeOcclusionState(_ notification: Notification) {
        startFixtureStreams()
    }

    private func buildFixture() {
        guard let content = window?.contentView else { return }
        let scroll = NSScrollView()
        scroll.hasVerticalScroller = true
        scroll.hasHorizontalScroller = false
        scroll.drawsBackground = false
        scroll.translatesAutoresizingMaskIntoConstraints = false
        content.addSubview(scroll)

        let document = NSView()
        document.translatesAutoresizingMaskIntoConstraints = false
        scroll.documentView = document

        let heading = NSTextField(labelWithString: "Read-only fixture · injected in-memory MCP/SSE transport · no credentials or database")
        heading.font = TerminalTheme.systemFont(ofSize: 14, weight: .semibold)
        heading.textColor = TerminalTheme.secondaryText
        diagnosticsLabel.font = TerminalTheme.monospacedFont(ofSize: 10)

        let cases: [(String, RealtimeConnectionStatusView)] = [
            ("READY · recent market/private observations", statusViews[0]),
            ("RECOVERING · last-known clocks remain visible", statusViews[1]),
            ("STALE · old source clocks remain explicit", statusViews[2]),
            ("UNAVAILABLE · zero counts and unknown clocks", statusViews[3])
        ]
        let rows = cases.map { title, statusView -> NSView in
            let label = NSTextField(labelWithString: title)
            label.font = TerminalTheme.systemFont(ofSize: 12, weight: .semibold)
            label.textColor = TerminalTheme.accent
            let row = NSStackView(views: [label, statusView])
            row.orientation = .vertical
            row.alignment = .leading
            row.spacing = 5
            row.translatesAutoresizingMaskIntoConstraints = false
            NSLayoutConstraint.activate([
                label.widthAnchor.constraint(equalTo: row.widthAnchor),
                statusView.widthAnchor.constraint(equalTo: row.widthAnchor)
            ])
            return row
        }

        let stack = NSStackView(views: [heading, diagnosticsLabel] + rows)
        stack.orientation = .vertical
        stack.alignment = .leading
        stack.spacing = 12
        stack.translatesAutoresizingMaskIntoConstraints = false
        document.addSubview(stack)
        for row in rows {
            row.widthAnchor.constraint(equalTo: stack.widthAnchor).isActive = true
        }
        NSLayoutConstraint.activate([
            scroll.leadingAnchor.constraint(equalTo: content.leadingAnchor),
            scroll.trailingAnchor.constraint(equalTo: content.trailingAnchor),
            scroll.topAnchor.constraint(equalTo: content.topAnchor),
            scroll.bottomAnchor.constraint(equalTo: content.bottomAnchor),
            document.widthAnchor.constraint(equalTo: scroll.contentView.widthAnchor),
            stack.leadingAnchor.constraint(equalTo: document.leadingAnchor, constant: 18),
            stack.trailingAnchor.constraint(equalTo: document.trailingAnchor, constant: -18),
            stack.topAnchor.constraint(equalTo: document.topAnchor, constant: 16),
            stack.bottomAnchor.constraint(equalTo: document.bottomAnchor, constant: -16),
            heading.widthAnchor.constraint(equalTo: stack.widthAnchor),
            diagnosticsLabel.widthAnchor.constraint(equalTo: stack.widthAnchor)
        ])
    }
}

@main
private final class RealtimeStatusGUIFixtureApplicationDelegate: NSObject, NSApplicationDelegate {
    private var controller: RealtimeStatusFixtureWindowController?
    private var autoQuitTimer: Timer?

    func applicationDidFinishLaunching(_ notification: Notification) {
        let application = notification.object as? NSApplication ?? NSApplication.shared
        application.setActivationPolicy(.regular)

        let controller = RealtimeStatusFixtureWindowController()
        self.controller = controller
        application.activate(ignoringOtherApps: true)
        controller.showWindow(nil)
        controller.window?.center()
        controller.window?.makeKeyAndOrderFront(nil)

        autoQuitTimer = Timer.scheduledTimer(withTimeInterval: 120, repeats: false) { [weak application] _ in
            application?.terminate(nil)
        }
    }

    func applicationShouldTerminateAfterLastWindowClosed(_ sender: NSApplication) -> Bool {
        true
    }
}

private struct RealtimeStatusGUIFixture {
    static func main() {
        let application = NSApplication.shared
        application.setActivationPolicy(.regular)
        let delegate = RealtimeStatusGUIFixtureApplicationDelegate()
        application.delegate = delegate
        withExtendedLifetime(delegate) {
            application.run()
        }
    }
}
