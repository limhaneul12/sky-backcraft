import AppKit

final class RealtimeConnectionStatusView: NSView {
    private let service: RealtimeFeedMCPService
    private var observerID: UUID?

    private let titleLabel = NSTextField(labelWithString: "Backend WebSocket 피드 상태 · loopback SSE")
    private let streamLabel = NSTextField(wrappingLabelWithString: "상태 스트림: 조회 전")
    private let marketLabel = NSTextField(wrappingLabelWithString: "MARKET · 상태 미확인")
    private let privateLabel = NSTextField(wrappingLabelWithString: "PRIVATE · 상태 미확인")
    private let statusDot = TerminalStatusDot()

    init(service: RealtimeFeedMCPService = .shared) {
        self.service = service
        super.init(frame: .zero)
        buildView()
        observerID = service.observe { [weak self] presentation in
            self?.render(presentation)
        }
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) { nil }

    override var wantsDefaultClipping: Bool { true }

    override func draw(_ dirtyRect: NSRect) {
        super.draw(dirtyRect)
        let path = NSBezierPath(roundedRect: bounds.insetBy(dx: 0.5, dy: 0.5), xRadius: 8, yRadius: 8)
        TerminalTheme.panel.setFill()
        path.fill()
        TerminalTheme.border.setStroke()
        path.lineWidth = 1
        path.stroke()
    }

    override func viewDidChangeEffectiveAppearance() {
        super.viewDidChangeEffectiveAppearance()
        needsDisplay = true
    }

    override func viewDidMoveToWindow() {
        super.viewDidMoveToWindow()
        render(service.presentation)
    }

    deinit {
        if let observerID { service.removeObserver(observerID) }
    }

    private func buildView() {
        let titleFont = TerminalTheme.systemFont(ofSize: 12, weight: .semibold)
        titleLabel.font = titleFont
        titleLabel.textColor = .labelColor
        titleLabel.lineBreakMode = .byTruncatingTail
        let titleLineHeight = ceil(titleFont.ascender - titleFont.descender + titleFont.leading + 2)
        titleLabel.heightAnchor.constraint(greaterThanOrEqualToConstant: titleLineHeight).isActive = true
        titleLabel.setContentCompressionResistancePriority(.required, for: .vertical)
        titleLabel.setContentHuggingPriority(.required, for: .vertical)
        titleLabel.setAccessibilityLabel("실시간 피드 상태 제목")

        for label in [streamLabel, marketLabel, privateLabel] {
            label.font = TerminalTheme.monospacedDigitFont(ofSize: 11, weight: .regular)
            label.textColor = TerminalTheme.secondaryText
            label.maximumNumberOfLines = 3
            label.lineBreakMode = .byWordWrapping
            label.setContentCompressionResistancePriority(.required, for: .vertical)
            label.setContentHuggingPriority(.defaultHigh, for: .vertical)
        }
        streamLabel.setAccessibilityLabel("상태 스트림 연결, keepalive 및 로컬 수신 시각")
        marketLabel.setAccessibilityLabel("시장 WebSocket 상태, 연결 수, 구독 수, 마지막 수신 및 조정 시각, 재연결, 손실과 판단 capability")
        privateLabel.setAccessibilityLabel("비공개 WebSocket 상태, 연결 수, 구독 수, 마지막 수신 및 조정 시각, 재연결, 손실과 판단 capability")

        let stack = NSStackView(views: [titleLabel, streamLabel, marketLabel, privateLabel])
        stack.orientation = .vertical
        stack.alignment = .leading
        stack.spacing = 4
        stack.setHuggingPriority(.required, for: .vertical)
        stack.setContentCompressionResistancePriority(.required, for: .vertical)
        stack.translatesAutoresizingMaskIntoConstraints = false
        statusDot.translatesAutoresizingMaskIntoConstraints = false
        addSubview(stack)
        addSubview(statusDot)
        setContentCompressionResistancePriority(.required, for: .vertical)
        setContentHuggingPriority(.required, for: .vertical)
        NSLayoutConstraint.activate([
            stack.leadingAnchor.constraint(equalTo: leadingAnchor, constant: 12),
            stack.trailingAnchor.constraint(equalTo: trailingAnchor, constant: -12),
            stack.topAnchor.constraint(equalTo: topAnchor, constant: 8),
            stack.bottomAnchor.constraint(equalTo: bottomAnchor, constant: -8),
            statusDot.trailingAnchor.constraint(equalTo: stack.trailingAnchor, constant: -2),
            statusDot.centerYAnchor.constraint(equalTo: titleLabel.centerYAnchor),
            statusDot.widthAnchor.constraint(equalToConstant: 8),
            statusDot.heightAnchor.constraint(equalToConstant: 8),
            titleLabel.widthAnchor.constraint(equalTo: stack.widthAnchor),
            streamLabel.widthAnchor.constraint(equalTo: stack.widthAnchor),
            marketLabel.widthAnchor.constraint(equalTo: stack.widthAnchor),
            privateLabel.widthAnchor.constraint(equalTo: stack.widthAnchor)
        ])
        setAccessibilityElement(true)
        setAccessibilityRole(.group)
        setAccessibilityLabel("Backend WebSocket 피드의 loopback SSE 상태")
    }

    private func render(_ presentation: RealtimeFeedPresentation) {
        if !Thread.isMainThread {
            DispatchQueue.main.async { [weak self] in self?.render(presentation) }
            return
        }
        guard let window, window.isVisible, !window.isMiniaturized,
              window.occlusionState.contains(.visible), !isHiddenOrHasHiddenAncestor else { return }
        let streamState: String
        switch presentation.connectionState {
        case .stopped:
            streamState = "STOPPED"
            statusDot.color = TerminalTone.neutral.color
        case .waitingForBackend:
            streamState = "WAITING_FOR_BACKEND"
            statusDot.color = TerminalTone.warning.color
        case .connecting:
            streamState = "CONNECTING"
            statusDot.color = TerminalTone.warning.color
        case .connected:
            streamState = "CONNECTED"
            statusDot.color = TerminalTone.positive.color
        case .recovering:
            streamState = "RECOVERING"
            statusDot.color = TerminalTone.warning.color
        case .authenticationExpired:
            streamState = "AUTH_EXPIRED"
            statusDot.color = TerminalTone.critical.color
        }
        let retry = presentation.reconnectDelaySeconds.map { " · 다음 연결 시도 \($0)초 후" } ?? ""
        let source = presentation.statusSource.map {
            $0 == .sseLatestState ? "SSE_LATEST_STATE" : "TYPED_MCP_QUERY"
        } ?? "UNKNOWN"
        let statusReceived = presentation.statusLocallyReceivedAt.map(Self.localClock) ?? "미확인"
        let streamOpened = presentation.streamOpenedAt.map(Self.localClock) ?? "미확인"
        let keepaliveReceived = presentation.lastKeepaliveLocallyReceivedAt.map(Self.localClock) ?? "미확인"
        let sseStatusReceived = presentation.lastSSEStatusLocallyReceivedAt.map(Self.localClock) ?? "미확인"
        let observed = presentation.status?.observed_at_utc ?? "미확인"
        let hasCurrentStatus = presentation.connectionState == .connected
            && presentation.streamOpenedAt.map { openedAt in
                presentation.statusLocallyReceivedAt.map { $0 >= openedAt } ?? false
            } == true
        let statusReceiptLabel = hasCurrentStatus ? "최근 상태 로컬 수신" : "마지막 상태 로컬 수신"
        streamLabel.stringValue = "SSE 상태 연결: \(streamState)\(retry) · 마지막 연결 시작 로컬(KST): \(streamOpened) · comment keepalive 로컬 수신(KST): \(keepaliveReceived) · 상태 이벤트 로컬 수신(KST): \(sseStatusReceived) · \(statusReceiptLabel)(KST): \(statusReceived) · status source: \(source) · backend observed_at_utc: \(observed)"
        streamLabel.toolTip = streamLabel.stringValue
        streamLabel.setAccessibilityValue(streamLabel.stringValue)

        guard let status = presentation.status else {
            marketLabel.stringValue = "MARKET · state 미확인 · connection_count 미확인 · subscription_count 미확인 · last_received_at_utc 미확인 · last_reconciled_at_utc 미확인 · reconnect_count 미확인 · dropped_messages 미확인 · ready_for_decisions 미확인"
            privateLabel.stringValue = "PRIVATE · state 미확인 · connection_count 미확인 · subscription_count 미확인 · last_received_at_utc 미확인 · last_reconciled_at_utc 미확인 · reconnect_count 미확인 · dropped_messages 미확인 · ready_for_decisions 미확인"
            marketLabel.setAccessibilityValue(marketLabel.stringValue)
            privateLabel.setAccessibilityValue(privateLabel.stringValue)
            return
        }
        marketLabel.stringValue = Self.feedDescription("MARKET", status.market)
        privateLabel.stringValue = Self.feedDescription("PRIVATE", status.private_feed)
        marketLabel.toolTip = marketLabel.stringValue
        privateLabel.toolTip = privateLabel.stringValue
        marketLabel.setAccessibilityValue(marketLabel.stringValue)
        privateLabel.setAccessibilityValue(privateLabel.stringValue)
    }

    private static func feedDescription(_ name: String, _ feed: RealtimeFeedStatus) -> String {
        var diagnostic = ""
        if let subscriptions = feed.subscription_readiness, !subscriptions.isEmpty {
            let blocked = subscriptions.filter { !$0.ready }
            diagnostic = " · 구독 준비 \(subscriptions.count - blocked.count)/\(subscriptions.count)"
            let absentRanges = subscriptions.reduce(0) { $0 + ($1.confirmed_provider_absence_spans?.count ?? 0) }
            if absentRanges > 0 {
                diagnostic += " · 공급자 봉 부재 확인 \(absentRanges)구간 (전략 재개와 별개)"
            }
            let absencePending = subscriptions.filter { $0.provider_recovery_progress?.pending_absence_confirmation == true }.count
            if absencePending > 0 {
                diagnostic += " · 공급자 공백 추가 확인 대기 \(absencePending)개"
            }
            let recovering = subscriptions.filter { $0.provider_recovery_progress?.next_page_to_utc != nil }
            if !recovering.isEmpty {
                let progress = recovering.prefix(3).map { item in
                    "\(item.market)/\(item.interval.uppercased()): \(item.provider_recovery_progress?.pages_scanned ?? 0)페이지"
                }.joined(separator: " · ")
                diagnostic += " · 과거 입력 확인 \(progress)"
            }
            if !blocked.isEmpty {
                let summary = blocked.prefix(3).map { item in
                    "\(item.market)/\(item.interval.uppercased()): \((item.blockers ?? []).prefix(3).joined(separator: ", "))"
                }.joined(separator: " · ")
                diagnostic += " · 준비 대기 \(summary)"
                if blocked.count > 3 { diagnostic += " · 추가 \(blocked.count - 3)개" }
            }
            let pendingCount = subscriptions.filter { $0.pending_rest_confirmation == true }.count
            if pendingCount > 0 { diagnostic += " · REST 재확인 대기 \(pendingCount)개" }
            let revised = subscriptions.filter { ($0.revision_snapshot_count ?? 0) > 0 }
            if !revised.isEmpty {
                let markets = revised.prefix(3).map { "\($0.market)/\($0.interval.uppercased())" }.joined(separator: ", ")
                diagnostic += " · 과거 응답 수정 감지 \(markets)"
            }
        }
        if let blockers = feed.readiness_blockers, !blockers.isEmpty {
            diagnostic += " · 연결 대기 \(blockers.prefix(3).joined(separator: ", "))"
        }
        return "\(name) · state \(feed.state.rawValue)\(diagnostic) · connection_count \(feed.connection_count) · subscription_count \(feed.subscription_count) · last_received_at_utc \(feed.last_received_at_utc ?? "미확인") · last_reconciled_at_utc \(feed.last_reconciled_at_utc ?? "미확인") · reconnect_count \(feed.reconnect_count) · dropped_messages \(feed.dropped_messages) · ready_for_decisions \(feed.ready_for_decisions)"
    }

    private static let localClockFormatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "ko_KR")
        formatter.timeZone = TimeZone(identifier: "Asia/Seoul") ?? TimeZone(secondsFromGMT: 9 * 3_600) ?? .current
        formatter.dateFormat = "yyyy-MM-dd HH:mm:ss 'KST'"
        return formatter
    }()

    private static func localClock(_ date: Date) -> String {
        localClockFormatter.string(from: date)
    }
}
