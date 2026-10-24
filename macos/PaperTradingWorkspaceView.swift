import AppKit

struct PaperMarketTimingRow: Equatable {
    let market: String
    let sourceEvent: String
    let finalizedCandle: String
    let accountMark: String
    let engineHeartbeat: String
}

struct PaperMarketTimingFormatter {
    private let fractionalUTCParser: ISO8601DateFormatter
    private let wholeSecondUTCParser: ISO8601DateFormatter
    private let utcDisplayFormatter: ISO8601DateFormatter

    init() {
        let fractionalUTCParser = ISO8601DateFormatter()
        fractionalUTCParser.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        fractionalUTCParser.timeZone = TimeZone(secondsFromGMT: 0)
        self.fractionalUTCParser = fractionalUTCParser

        let wholeSecondUTCParser = ISO8601DateFormatter()
        wholeSecondUTCParser.formatOptions = [.withInternetDateTime]
        wholeSecondUTCParser.timeZone = TimeZone(secondsFromGMT: 0)
        self.wholeSecondUTCParser = wholeSecondUTCParser

        let utcDisplayFormatter = ISO8601DateFormatter()
        utcDisplayFormatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        utcDisplayFormatter.timeZone = TimeZone(secondsFromGMT: 0)
        self.utcDisplayFormatter = utcDisplayFormatter
    }

    func rows(for markets: [PaperMarketSnapshot], at now: Date) -> [PaperMarketTimingRow] {
        markets.map { market in
            PaperMarketTimingRow(
                market: market.market,
                sourceEvent: timestamp(market.last_source_event_at, at: now),
                finalizedCandle: timestamp(market.last_finalized_candle_at, at: now),
                accountMark: timestamp(market.last_account_mark_at, at: now),
                engineHeartbeat: timestamp(market.last_heartbeat_at, at: now)
            )
        }
    }

    func receivedLabel(_ receivedAt: Date?, at now: Date) -> String {
        guard let receivedAt else { return "미수신 · 경과 알 수 없음" }
        let timestamp = utcDisplayFormatter.string(from: receivedAt)
        guard receivedAt <= now else { return "\(timestamp) · 미래 시각 · 경과 알 수 없음" }
        return "\(timestamp) · 경과 \(elapsedSeconds(from: receivedAt, at: now))초"
    }

    func utcLabel(for date: Date) -> String {
        utcDisplayFormatter.string(from: date)
    }

    private func timestamp(_ raw: String?, at now: Date) -> String {
        guard let raw, !raw.isEmpty else { return "미관측 · 경과 알 수 없음" }
        guard let date = fractionalUTCParser.date(from: raw) ?? wholeSecondUTCParser.date(from: raw) else {
            return "\(raw) · 시각 해석 불가 · 경과 알 수 없음"
        }
        guard date <= now else { return "\(raw) · 미래 시각 · 경과 알 수 없음" }
        return "\(raw) · 경과 \(elapsedSeconds(from: date, at: now))초"
    }

    private func elapsedSeconds(from date: Date, at now: Date) -> Int {
        Int(now.timeIntervalSince(date).rounded(.down))
    }
}

final class PaperTradingWorkspaceView: NSView, NSTableViewDataSource, NSTableViewDelegate {
    override var wantsDefaultClipping: Bool { true }

    private let service: PaperSessionMCPService
    private let realtimeFeedService: RealtimeFeedMCPService
    private let realtimeStatusView: RealtimeConnectionStatusView
    private let jobsQueueView: NSView
    private let onSummaryChanged: (String) -> Void

    private var startOptions: PaperSessionStartOptions?
    private var supportedPolicyOptions: [PaperPolicyOption] = []
    private var sessions: [PaperSessionView] = []
    private var selectedSession: PaperSessionView?
    private var sessionSnapshotReceivedAt: Date?
    private var marketTimingRows: [PaperMarketTimingRow] = []
    private var events: [PaperSessionEvent] = []
    private var nextEventAfterSequence: UInt64?
    private var totalEventCount: UInt64 = 0
    private var eventsAsOfSequence: UInt64?
    private var eventsSnapshotSessionID: String?
    private var selectedDecisionEvent: PaperSessionEvent?

    private var activeReadRequest: NativeMCPRequest?
    private var activeEventsReadRequest: NativeMCPRequest?
    private var activeDecisionReadRequest: NativeMCPRequest?
    private var activeWriteRequest: NativeMCPRequest?
    private var pendingUnknownRequestId: String?
    private var pendingReadbackConfirmationRequestId: String?
    private var startOptionsAreFresh = false
    private var sessionSnapshotIsFresh = false
    private var readFailureMessage: String?
    private var readGeneration: UInt64 = 0
    private var eventsReadGeneration: UInt64 = 0
    private var decisionReadGeneration: UInt64 = 0
    private var realtimeFeedObserverID: UUID?
    private var marketFeedInvalidation = RealtimeReconciliationInvalidation()
    private var pollTimer: Timer?
    private var isVisibleWorkspace = false

    // Creation form controls
    private let policyPopup = NSPopUpButton()
    private let intervalPopup = NSPopUpButton()
    private let btcCheck = NSButton(checkboxWithTitle: "KRW-BTC", target: nil, action: nil)
    private let ethCheck = NSButton(checkboxWithTitle: "KRW-ETH", target: nil, action: nil)
    private let solCheck = NSButton(checkboxWithTitle: "KRW-SOL", target: nil, action: nil)
    private let initialCashField = NSTextField(string: "10000")
    private let feeBpsField = NSTextField(string: "10")
    private let slippageBpsField = NSTextField(string: "0")
    private let scenarioRuleLabel = NSTextField(
        wrappingLabelWithString: "시장 규칙: SIMULATED_SCENARIO (동결 가상 시나리오 · 실시간 거래소 호가/체결 규칙 검증 아님) · 자본 모드: 시장별 독립 가상 계좌 · 체결: NEXT_BAR_OPEN"
    )
    private let startButton = NSButton(title: "세션 생성 및 시작", target: nil, action: nil)
    private let readbackButton = NSButton(title: "미확인 요청 재조회", target: nil, action: nil)

    // Session selector & lifecycle controls
    private let sessionPopup = NSPopUpButton()
    private let pauseButton = NSButton(title: "일시정지 (Pause)", target: nil, action: nil)
    private let resumeButton = NSButton(title: "재개 (Resume)", target: nil, action: nil)
    private let stopButton = NSButton(title: "세션 종료 (Stop)", target: nil, action: nil)
    private let copyIdButton = NSButton(title: "세션·리비전 ID 복사", target: nil, action: nil)
    private let refreshButton = NSButton(title: "새로고침", target: nil, action: nil)

    // Status & summary banners
    private let statusBanner = NSTextField(
        wrappingLabelWithString: "지속 모의 세션을 불러오는 중입니다. 모든 주문·체결·잔고는 VIRTUAL / SIMULATED이며 실거래 주문을 전송하지 않습니다."
    )
    private let sessionSummaryLabel = NSTextField(
        wrappingLabelWithString: "선택된 지속 모의 세션이 없습니다. 위 생성 폼에서 정책 리비전과 시장을 선택해 새 세션을 시작하세요."
    )

    // Tables
    private let marketsTable = NSTableView()
    private let marketTimingTable = NSTableView()
    private let marketTimingStatusLabel = NSTextField(wrappingLabelWithString: "")
    private let eventsTable = NSTableView()
    private let eventsPageLabel = NSTextField(labelWithString: "이벤트 0건")
    private let eventsFirstPageButton = NSButton(title: "첫 페이지", target: nil, action: nil)
    private let eventsNextPageButton = NSButton(title: "다음 이벤트", target: nil, action: nil)
    private let decisionSignalPopup = NSPopUpButton()
    private let decisionReviewButton = NSButton(title: "선택 판단 근거 보기", target: nil, action: nil)
    private let decisionReviewStatusLabel = NSTextField(wrappingLabelWithString: "이벤트 행을 선택한 뒤 당시 저장 근거를 조회하세요.")
    private let decisionReviewText = NSTextView()

    init(
        service: PaperSessionMCPService = PaperSessionMCPService(),
        realtimeFeedService: RealtimeFeedMCPService = .shared,
        jobsQueueView: NSView,
        onSummaryChanged: @escaping (String) -> Void
    ) {
        self.service = service
        self.realtimeFeedService = realtimeFeedService
        self.realtimeStatusView = RealtimeConnectionStatusView(service: realtimeFeedService)
        self.jobsQueueView = jobsQueueView
        self.onSummaryChanged = onSummaryChanged
        super.init(frame: .zero)
        buildView()
        updateControlEnablement()
        realtimeFeedObserverID = realtimeFeedService.observe { [weak self] presentation in
            guard let self else { return }
            _ = self.marketFeedInvalidation.observe(
                presentation.status?.market.last_reconciled_at_utc,
                hasProjection: presentation.status != nil,
                isVisible: self.isVisibleWorkspace
            )
            self.drainPendingMarketFeedInvalidation()
        }
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        fatalError("init(coder:) has not been implemented")
    }

    deinit {
        if let realtimeFeedObserverID {
            realtimeFeedService.removeObserver(realtimeFeedObserverID)
        }
        pollTimer?.invalidate()
        activeReadRequest?.cancel()
        activeEventsReadRequest?.cancel()
        activeDecisionReadRequest?.cancel()
        activeWriteRequest?.cancel()
    }

    override func draw(_ dirtyRect: NSRect) {
        TerminalTheme.surface.setFill()
        bounds.fill()
    }

    override func viewDidChangeEffectiveAppearance() {
        super.viewDidChangeEffectiveAppearance()
        needsDisplay = true
    }

    func setWorkspaceVisible(_ visible: Bool) {
        guard visible != isVisibleWorkspace else { return }
        isVisibleWorkspace = visible
        if visible {
            reloadWorkspace(preserveSelection: true)
            if pollTimer == nil {
                pollTimer = Timer.scheduledTimer(withTimeInterval: 15.0, repeats: true) { [weak self] _ in
                    guard let self, self.isVisibleWorkspace else { return }
                    if self.activeReadRequest == nil && self.activeEventsReadRequest == nil
                        && self.activeDecisionReadRequest == nil
                        && self.activeWriteRequest == nil {
                        self.reloadSessionsAndDetail(preserveSelection: true, refreshEvents: false)
                    }
                }
            }
        } else {
            readGeneration &+= 1
            pollTimer?.invalidate()
            pollTimer = nil
            marketFeedInvalidation.clearPending()
            cancelEventsRead()
            activeReadRequest?.cancel()
            activeReadRequest = nil
            startOptionsAreFresh = false
            sessionSnapshotIsFresh = false
        }
        updateControlEnablement()
    }

    private func buildView() {
        let outerScroll = NSScrollView()
        outerScroll.hasVerticalScroller = true
        outerScroll.hasHorizontalScroller = false
        outerScroll.drawsBackground = false
        outerScroll.translatesAutoresizingMaskIntoConstraints = false
        addSubview(outerScroll)

        let documentView = TerminalFlippedDocumentView()
        documentView.translatesAutoresizingMaskIntoConstraints = false
        outerScroll.documentView = documentView

        let titleLabel = NSTextField(wrappingLabelWithString: "지속 모의투자 (Persistent Paper Sessions)")
        let titleFont = TerminalTheme.screenTitle
        titleLabel.font = titleFont
        let titleLineHeight = titleFont.ascender - titleFont.descender + titleFont.leading
        titleLabel.heightAnchor.constraint(greaterThanOrEqualToConstant: ceil(titleLineHeight + 4)).isActive = true
        titleLabel.maximumNumberOfLines = 0
        titleLabel.lineBreakMode = .byWordWrapping
        titleLabel.setAccessibilityLabel("지속 모의투자 세션")

        let virtualBadge = NSTextField(wrappingLabelWithString: "VIRTUAL / SIMULATED · 실주문 없음 · 확정 봉 폴링")
        let badgeFont = TerminalTheme.monospacedFont(ofSize: 11, weight: .bold)
        virtualBadge.font = badgeFont
        let badgeLineHeight = badgeFont.ascender - badgeFont.descender + badgeFont.leading
        virtualBadge.heightAnchor.constraint(greaterThanOrEqualToConstant: ceil(badgeLineHeight + 4)).isActive = true
        virtualBadge.maximumNumberOfLines = 0
        virtualBadge.lineBreakMode = .byWordWrapping
        virtualBadge.textColor = TerminalTheme.accent
        virtualBadge.wantsLayer = true
        virtualBadge.layer?.cornerRadius = 4
        virtualBadge.layer?.borderWidth = 1
        virtualBadge.layer?.borderColor = TerminalTheme.accent.cgColor
        virtualBadge.setAccessibilityLabel("VIRTUAL / SIMULATED · 실제 주문 없음 · 확정 봉 폴링")

        let headerRow = NSStackView(views: [titleLabel, virtualBadge])
        let usesStackedHeader = TerminalTheme.textScale >= 1.5
        headerRow.orientation = usesStackedHeader ? .vertical : .horizontal
        headerRow.alignment = usesStackedHeader ? .leading : .centerY
        headerRow.spacing = usesStackedHeader ? 6 : 12
        if usesStackedHeader {
            titleLabel.widthAnchor.constraint(equalTo: headerRow.widthAnchor).isActive = true
            virtualBadge.widthAnchor.constraint(equalTo: headerRow.widthAnchor).isActive = true
        }

        sessionSummaryLabel.font = TerminalTheme.monospacedFont(ofSize: 11)
        sessionSummaryLabel.textColor = TerminalTheme.secondaryText
        sessionSummaryLabel.maximumNumberOfLines = 0
        sessionSummaryLabel.lineBreakMode = .byWordWrapping
        sessionSummaryLabel.setAccessibilityLabel("선택한 지속 모의 세션과 동결 정책 identity")

        statusBanner.font = TerminalTheme.monospacedFont(ofSize: 11, weight: .medium)
        statusBanner.textColor = TerminalTheme.secondaryText
        statusBanner.maximumNumberOfLines = 4
        statusBanner.lineBreakMode = .byWordWrapping
        statusBanner.setAccessibilityLabel("지속 모의 작업 상태와 복구 안내")

        let summaryBox = makePanelBox()
        let summaryStack = NSStackView(views: [sessionSummaryLabel, statusBanner])
        summaryStack.orientation = .vertical
        summaryStack.alignment = .leading
        summaryStack.spacing = 4
        sessionSummaryLabel.widthAnchor.constraint(equalTo: summaryStack.widthAnchor).isActive = true
        statusBanner.widthAnchor.constraint(equalTo: summaryStack.widthAnchor).isActive = true
        embedInBox(summaryStack, into: summaryBox)

        let fixedHeader = NSStackView(views: [headerRow, summaryBox])
        fixedHeader.orientation = .vertical
        fixedHeader.alignment = .leading
        fixedHeader.spacing = 8
        fixedHeader.translatesAutoresizingMaskIntoConstraints = false
        addSubview(fixedHeader)

        // 1. Creation box
        let createBox = makePanelBox()
        let createTitle = NSTextField(labelWithString: "1. 새 지속 모의 세션 생성 및 시작 (동결 정책 리비전 · 시장별 독립 가상 계좌)")
        createTitle.font = .systemFont(ofSize: 13, weight: .semibold)

        policyPopup.setAccessibilityLabel("지속 모의 정책 리비전 선택")
        policyPopup.widthAnchor.constraint(greaterThanOrEqualToConstant: 320).isActive = true

        intervalPopup.setAccessibilityLabel("의사결정 봉 간격 선택")
        intervalPopup.addItems(withTitles: ["h1"])
        intervalPopup.selectItem(withTitle: "h1")
        intervalPopup.widthAnchor.constraint(equalToConstant: 90).isActive = true

        btcCheck.state = .on
        ethCheck.state = .on
        solCheck.state = .off

        initialCashField.font = TerminalTheme.table
        initialCashField.alignment = .right
        initialCashField.widthAnchor.constraint(equalToConstant: 130).isActive = true
        initialCashField.setAccessibilityLabel("시장별 초기 가상 자본 KRW")

        feeBpsField.font = TerminalTheme.table
        feeBpsField.alignment = .right
        feeBpsField.widthAnchor.constraint(equalToConstant: 64).isActive = true
        feeBpsField.setAccessibilityLabel("수수료 bps")

        slippageBpsField.font = TerminalTheme.table
        slippageBpsField.alignment = .right
        slippageBpsField.widthAnchor.constraint(equalToConstant: 64).isActive = true
        slippageBpsField.setAccessibilityLabel("슬리피지 bps")

        startButton.bezelStyle = .rounded
        startButton.target = self
        startButton.action = #selector(handleStartSession)
        startButton.setAccessibilityLabel("지속 모의 세션 생성 및 시작")

        readbackButton.bezelStyle = .rounded
        readbackButton.target = self
        readbackButton.action = #selector(handleReadbackUnknownRequest)
        readbackButton.isHidden = true
        readbackButton.setAccessibilityLabel("미확인 요청 상태 재조회")

        let formRow1 = NSStackView(views: [
            NSTextField(labelWithString: "정책 리비전:"), policyPopup,
            NSTextField(labelWithString: "봉 간격:"), intervalPopup,
            NSTextField(labelWithString: "시장:"), btcCheck, ethCheck, solCheck
        ])
        formRow1.orientation = .horizontal
        if TerminalTheme.textScale >= 1.5 {
            formRow1.orientation = .vertical
        }
        formRow1.alignment = TerminalTheme.textScale >= 1.5 ? .leading : .centerY
        formRow1.spacing = TerminalTheme.textScale >= 1.5 ? 6 : 10

        let formRow2 = NSStackView(views: [
            NSTextField(labelWithString: "시장별 초기 가상 자본(KRW):"), initialCashField,
            NSTextField(labelWithString: "수수료(bps):"), feeBpsField,
            NSTextField(labelWithString: "슬리피지(bps):"), slippageBpsField,
            startButton, readbackButton
        ])
        formRow2.orientation = .horizontal
        if TerminalTheme.textScale >= 1.5 {
            formRow2.orientation = .vertical
        }
        formRow2.alignment = TerminalTheme.textScale >= 1.5 ? .leading : .centerY
        formRow2.spacing = TerminalTheme.textScale >= 1.5 ? 6 : 10

        scenarioRuleLabel.font = .systemFont(ofSize: 11)
        scenarioRuleLabel.textColor = TerminalTheme.secondaryText

        let createStack = NSStackView(views: [createTitle, formRow1, formRow2, scenarioRuleLabel])
        createStack.orientation = .vertical
        createStack.alignment = .leading
        createStack.spacing = 8
        embedInBox(createStack, into: createBox)

        // 2. Session selector & lifecycle control bar
        let controlBox = makePanelBox()
        let controlTitle = NSTextField(labelWithString: "2. 세션 선택 및 상태 제어 (CAS Sequence · Idempotent Receipt)")
        controlTitle.font = .systemFont(ofSize: 13, weight: .semibold)

        sessionPopup.target = self
        sessionPopup.action = #selector(handleSessionSelectionChanged)
        sessionPopup.setAccessibilityLabel("지속 모의 세션 선택")
        sessionPopup.widthAnchor.constraint(greaterThanOrEqualToConstant: 360).isActive = true

        for (btn, action, label) in [
            (pauseButton, #selector(handlePauseSession), "세션 일시정지"),
            (resumeButton, #selector(handleResumeSession), "세션 재개"),
            (stopButton, #selector(handleStopSession), "세션 종료"),
            (copyIdButton, #selector(handleCopySessionIdentity), "세션 및 정책 리비전 ID 복사"),
            (refreshButton, #selector(handleManualRefresh), "세션 새로고침")
        ] {
            btn.bezelStyle = .rounded
            btn.target = self
            btn.action = action
            btn.setAccessibilityLabel(label)
        }

        let controlRow = NSStackView(views: [
            NSTextField(labelWithString: "세션:"), sessionPopup,
            pauseButton, resumeButton, stopButton, copyIdButton, refreshButton
        ])
        controlRow.orientation = .horizontal
        controlRow.alignment = .centerY
        controlRow.spacing = 8

        let sessionControlStack = NSStackView(views: [controlTitle, controlRow])
        sessionControlStack.orientation = .vertical
        sessionControlStack.alignment = .leading
        sessionControlStack.spacing = 8
        embedInBox(sessionControlStack, into: controlBox)

        // 3. Per-market independent virtual accounts table
        let marketsHeader = NSTextField(
            labelWithString: "3. 시장별 독립 가상 계좌 상태 (시장 간 합산 곡선 없음 · 각 시장 독립 원장)"
        )
        marketsHeader.font = .systemFont(ofSize: 13, weight: .semibold)

        setupMarketsTable()
        let marketsScroll = NSScrollView()
        marketsScroll.documentView = marketsTable
        marketsScroll.hasVerticalScroller = true
        marketsScroll.hasHorizontalScroller = true
        marketsScroll.heightAnchor.constraint(equalToConstant: 150).isActive = true

        let timingHeader = NSTextField(
            labelWithString: "4. 시장별 데이터 시각 (각 원천 UTC 시각과 화면 표시 시점 기준 경과)"
        )
        timingHeader.font = .systemFont(ofSize: 13, weight: .semibold)

        marketTimingStatusLabel.font = .monospacedSystemFont(ofSize: 11, weight: .regular)
        marketTimingStatusLabel.textColor = TerminalTheme.secondaryText
        marketTimingStatusLabel.lineBreakMode = .byWordWrapping

        setupMarketTimingTable()
        let marketTimingScroll = NSScrollView()
        marketTimingScroll.documentView = marketTimingTable
        marketTimingScroll.hasVerticalScroller = true
        marketTimingScroll.hasHorizontalScroller = true
        marketTimingScroll.heightAnchor.constraint(equalToConstant: 132).isActive = true

        // 5. Paged session events log
        let eventsTitle = NSTextField(labelWithString: "5. 지속 모의 세션 원장 이벤트 (확정 봉 처리 · 신호 · 가상 주문 예약/취소/체결 · 상태 전이)")
        eventsTitle.font = .systemFont(ofSize: 13, weight: .semibold)

        eventsPageLabel.font = .monospacedDigitSystemFont(ofSize: 12, weight: .regular)
        eventsPageLabel.textColor = TerminalTheme.secondaryText

        eventsFirstPageButton.bezelStyle = .rounded
        eventsFirstPageButton.target = self
        eventsFirstPageButton.action = #selector(handleLoadFirstEvents)

        eventsNextPageButton.bezelStyle = .rounded
        eventsNextPageButton.target = self
        eventsNextPageButton.action = #selector(handleLoadNextEvents)

        let eventsHeaderRow = NSStackView(views: [eventsTitle, eventsPageLabel, eventsFirstPageButton, eventsNextPageButton])
        eventsHeaderRow.orientation = .horizontal
        eventsHeaderRow.alignment = .centerY
        eventsHeaderRow.spacing = 10

        setupEventsTable()
        let eventsScroll = NSScrollView()
        eventsScroll.documentView = eventsTable
        eventsScroll.hasVerticalScroller = true
        eventsScroll.hasHorizontalScroller = true
        eventsScroll.heightAnchor.constraint(equalToConstant: 210).isActive = true

        decisionSignalPopup.isHidden = true
        decisionSignalPopup.setAccessibilityLabel("선택한 이벤트의 판단 신호")
        decisionSignalPopup.setAccessibilityHelp("이벤트에 여러 신호가 있으면 설명할 저장 신호를 선택합니다.")
        decisionSignalPopup.target = self
        decisionSignalPopup.action = #selector(handleDecisionSignalSelectionChanged)
        decisionReviewButton.bezelStyle = .rounded
        decisionReviewButton.target = self
        decisionReviewButton.action = #selector(handleReviewSelectedDecision)
        decisionReviewButton.isEnabled = false
        decisionReviewButton.setAccessibilityLabel("선택한 저장 이벤트의 판단 근거를 고정 시점으로 조회")
        decisionReviewButton.toolTip = "저장된 판단 근거만 조회합니다. 전략을 다시 실행하지 않습니다."
        let decisionControlsRow = NSStackView(views: [decisionSignalPopup, NSView(), decisionReviewButton])
        decisionControlsRow.orientation = .horizontal
        decisionControlsRow.alignment = .centerY
        decisionControlsRow.spacing = 8

        decisionReviewStatusLabel.font = TerminalTheme.table
        decisionReviewStatusLabel.textColor = TerminalTheme.secondaryText
        decisionReviewStatusLabel.maximumNumberOfLines = 0
        decisionReviewStatusLabel.lineBreakMode = .byWordWrapping
        decisionReviewStatusLabel.setAccessibilityLabel("선택 판단 기록의 session, 이벤트, cutoff identity와 조회 상태")
        decisionReviewText.isEditable = false
        decisionReviewText.isSelectable = true
        decisionReviewText.font = TerminalTheme.monospacedFont(ofSize: 11)
        decisionReviewText.textColor = .labelColor
        decisionReviewText.backgroundColor = TerminalTheme.chart
        decisionReviewText.setAccessibilityLabel("엔진이 저장한 결정 행동, 조건 평가, 이유, 주문과 체결")
        let decisionReviewScroll = TerminalWrappingTextScrollView()
        decisionReviewScroll.configureWrapping(textView: decisionReviewText)
        decisionReviewScroll.drawsBackground = true
        decisionReviewScroll.backgroundColor = TerminalTheme.chart
        decisionReviewScroll.heightAnchor.constraint(equalToConstant: 180).isActive = true
        let decisionReviewPanel = makePanelBox()
        let decisionReviewContent = NSStackView(views: [decisionReviewStatusLabel, decisionReviewScroll])
        decisionReviewContent.orientation = .vertical
        decisionReviewContent.alignment = .leading
        decisionReviewContent.spacing = 8
        decisionReviewStatusLabel.widthAnchor.constraint(equalTo: decisionReviewContent.widthAnchor).isActive = true
        decisionReviewScroll.widthAnchor.constraint(equalTo: decisionReviewContent.widthAnchor).isActive = true
        embedInBox(decisionReviewContent, into: decisionReviewPanel)

        let marketsBox = makePanelBox()
        let marketsStack = NSStackView(views: [marketsHeader, marketsScroll])
        marketsStack.orientation = .vertical
        marketsStack.alignment = .leading
        marketsStack.spacing = 8
        marketsHeader.widthAnchor.constraint(equalTo: marketsStack.widthAnchor).isActive = true
        marketsScroll.widthAnchor.constraint(equalTo: marketsStack.widthAnchor).isActive = true
        embedInBox(marketsStack, into: marketsBox)

        let timingBox = makePanelBox()
        let timingStack = NSStackView(views: [timingHeader, marketTimingStatusLabel, marketTimingScroll])
        timingStack.orientation = .vertical
        timingStack.alignment = .leading
        timingStack.spacing = 8
        timingHeader.widthAnchor.constraint(equalTo: timingStack.widthAnchor).isActive = true
        marketTimingStatusLabel.widthAnchor.constraint(equalTo: timingStack.widthAnchor).isActive = true
        marketTimingScroll.widthAnchor.constraint(equalTo: timingStack.widthAnchor).isActive = true
        embedInBox(timingStack, into: timingBox)

        let eventsBox = makePanelBox()
        let eventsStack = NSStackView(views: [eventsHeaderRow, eventsScroll, decisionControlsRow])
        eventsStack.orientation = .vertical
        eventsStack.alignment = .leading
        eventsStack.spacing = 8
        eventsHeaderRow.widthAnchor.constraint(equalTo: eventsStack.widthAnchor).isActive = true
        eventsScroll.widthAnchor.constraint(equalTo: eventsStack.widthAnchor).isActive = true
        decisionControlsRow.widthAnchor.constraint(equalTo: eventsStack.widthAnchor).isActive = true
        embedInBox(eventsStack, into: eventsBox)

        // The dashboard owns the embedded job queue and its table sizing.
        let mainStack = NSStackView(views: [
            jobsQueueView,
            realtimeStatusView,
            createBox,
            controlBox,
            marketsBox,
            timingBox,
            eventsBox,
            decisionReviewPanel
        ])
        mainStack.orientation = .vertical
        mainStack.alignment = .leading
        mainStack.spacing = 12
        mainStack.translatesAutoresizingMaskIntoConstraints = false
        documentView.addSubview(mainStack)

        NSLayoutConstraint.activate([
            outerScroll.leadingAnchor.constraint(equalTo: leadingAnchor),
            outerScroll.trailingAnchor.constraint(equalTo: trailingAnchor),
            outerScroll.topAnchor.constraint(equalTo: fixedHeader.bottomAnchor, constant: 10),
            outerScroll.bottomAnchor.constraint(equalTo: bottomAnchor),

            fixedHeader.leadingAnchor.constraint(equalTo: leadingAnchor, constant: 20),
            fixedHeader.trailingAnchor.constraint(equalTo: trailingAnchor, constant: -20),
            fixedHeader.topAnchor.constraint(equalTo: topAnchor, constant: 16),

            documentView.widthAnchor.constraint(equalTo: outerScroll.contentView.widthAnchor),
            mainStack.leadingAnchor.constraint(equalTo: documentView.leadingAnchor, constant: 20),
            mainStack.trailingAnchor.constraint(equalTo: documentView.trailingAnchor, constant: -20),
            mainStack.topAnchor.constraint(equalTo: documentView.topAnchor, constant: 16),
            mainStack.bottomAnchor.constraint(equalTo: documentView.bottomAnchor, constant: -16),

            headerRow.widthAnchor.constraint(equalTo: fixedHeader.widthAnchor),
            summaryBox.widthAnchor.constraint(equalTo: fixedHeader.widthAnchor),
            jobsQueueView.widthAnchor.constraint(equalTo: mainStack.widthAnchor),
            realtimeStatusView.widthAnchor.constraint(equalTo: mainStack.widthAnchor),
            createBox.widthAnchor.constraint(equalTo: mainStack.widthAnchor),
            controlBox.widthAnchor.constraint(equalTo: mainStack.widthAnchor),
            marketsBox.widthAnchor.constraint(equalTo: mainStack.widthAnchor),
            timingBox.widthAnchor.constraint(equalTo: mainStack.widthAnchor),
            eventsBox.widthAnchor.constraint(equalTo: mainStack.widthAnchor),
            decisionReviewPanel.widthAnchor.constraint(equalTo: mainStack.widthAnchor)
        ])
    }

    private func makePanelBox() -> NSBox {
        let box = NSBox()
        box.boxType = .custom
        box.cornerRadius = 8
        box.borderWidth = 1
        box.borderColor = TerminalTheme.border
        box.fillColor = TerminalTheme.panel
        return box
    }

    private func embedInBox(_ stack: NSStackView, into box: NSBox) {
        stack.translatesAutoresizingMaskIntoConstraints = false
        box.addSubview(stack)
        NSLayoutConstraint.activate([
            stack.leadingAnchor.constraint(equalTo: box.leadingAnchor, constant: 12),
            stack.trailingAnchor.constraint(equalTo: box.trailingAnchor, constant: -12),
            stack.topAnchor.constraint(equalTo: box.topAnchor, constant: 10),
            stack.bottomAnchor.constraint(equalTo: box.bottomAnchor, constant: -10)
        ])
    }

    private func setupMarketsTable() {
        marketsTable.removeAllColumns()
        let cols: [(String, String, CGFloat)] = [
            ("Market", "시장", 88),
            ("Freshness", "신선도", 88),
            ("Events", "이벤트/체결", 92),
            ("LastCandle", "마지막 확정 봉 (UTC)", 155),
            ("AvailCash", "가용 가상 현금", 125),
            ("ReservedCash", "예약 현금", 110),
            ("Qty", "보유 수량", 105),
            ("MarkPrice", "현재 평가가", 115),
            ("Equity", "가상 평가액(KRW)", 130),
            ("RealizedPnL", "실현 손익", 115),
            ("UnrealizedPnL", "미실현 손익", 115),
            ("Fees", "누적 수수료", 105),
            ("MDD", "현재 낙폭", 105),
            ("Benchmark", "벤치마크(B&H)", 145),
            ("F08", "독립 회계·이벤트 검산", 210)
        ]
        for (id, title, width) in cols {
            let col = NSTableColumn(identifier: NSUserInterfaceItemIdentifier(id))
            col.title = title
            col.width = width
            marketsTable.addTableColumn(col)
        }
        marketsTable.dataSource = self
        marketsTable.delegate = self
        TerminalTheme.prepareTable(marketsTable)
    }

    private func setupMarketTimingTable() {
        marketTimingTable.removeAllColumns()
        let cols: [(String, String, CGFloat)] = [
            ("TimingMarket", "시장", 95),
            ("SourceEventAt", "마지막 원천 이벤트 (UTC · 경과)", 390),
            ("FinalizedCandleAt", "마지막 확정 봉 (UTC · 지연)", 390),
            ("AccountMarkAt", "마지막 계정 평가 (UTC · 경과)", 390),
            ("EngineHeartbeatAt", "엔진 heartbeat (UTC · 경과)", 390)
        ]
        for (id, title, width) in cols {
            let column = NSTableColumn(identifier: NSUserInterfaceItemIdentifier(id))
            column.title = title
            let scaledWidth = Self.scaledMarketTimingColumnWidth(width)
            column.width = scaledWidth
            column.minWidth = scaledWidth
            marketTimingTable.addTableColumn(column)
        }
        marketTimingTable.dataSource = self
        marketTimingTable.delegate = self
        marketTimingTable.setAccessibilityLabel("시장별 독립 데이터 시각")
        TerminalTheme.prepareTable(marketTimingTable)
    }

    static func scaledMarketTimingColumnWidth(_ baseWidth: CGFloat) -> CGFloat {
        ceil(baseWidth * TerminalTheme.table.pointSize / 13.0)
    }

    private func setupEventsTable() {
        eventsTable.removeAllColumns()
        let cols: [(String, String, CGFloat)] = [
            ("Seq", "Seq", 58),
            ("OccurredAt", "확정/발생 시각 (UTC)", 155),
            ("EventMarket", "시장", 84),
            ("EventKind", "종류", 115),
            ("EventSide", "방향", 64),
            ("EventQty", "수량", 100),
            ("EventPrice", "가격(KRW)", 115),
            ("EventFee", "수수료(KRW)", 100),
            ("EventEquity", "반영 후 평가액", 130),
            ("EventDetail", "상세 내용 / Candle Digest", 360)
        ]
        for (id, title, width) in cols {
            let col = NSTableColumn(identifier: NSUserInterfaceItemIdentifier(id))
            col.title = title
            col.width = width
            eventsTable.addTableColumn(col)
        }
        eventsTable.dataSource = self
        eventsTable.delegate = self
        eventsTable.allowsEmptySelection = true
        eventsTable.allowsMultipleSelection = false
        eventsTable.setAccessibilityLabel("고정 cutoff에 대한 저장된 모의 세션 이벤트")
        TerminalTheme.prepareTable(eventsTable)
    }

    // MARK: - Data Loading

    func reloadWorkspace(preserveSelection: Bool) {
        guard isVisibleWorkspace else { return }
        marketFeedInvalidation.clearPending()
        cancelEventsRead()
        readGeneration &+= 1
        let generation = readGeneration
        activeReadRequest?.cancel()
        let composite = NativeMCPRequest()
        activeReadRequest = composite
        startOptionsAreFresh = false
        sessionSnapshotIsFresh = false
        readFailureMessage = nil
        if pendingUnknownRequestId == nil {
            statusBanner.stringValue = "시작 옵션과 세션 목록을 확인하는 중입니다. 확인이 끝날 때까지 쓰기 요청은 비활성화됩니다."
            statusBanner.textColor = TerminalTheme.secondaryText
        }
        updateControlEnablement()
        let optReq = service.fetchStartOptions { [weak self] optResult in
            guard let self, self.isVisibleWorkspace,
                  self.readGeneration == generation,
                  self.activeReadRequest === composite else { return }
            switch optResult {
            case .success(let options):
                self.startOptionsAreFresh = true
                self.startOptions = options
                self.populateStartOptions(options)
            case .failure(let error):
                self.startOptionsAreFresh = false
                self.readFailureMessage = "시작 옵션 조회 실패: \(error.localizedDescription) (마지막 확인된 옵션을 유지하며 쓰기는 차단합니다)"
            }
            let listReq = self.service.listSessions(limit: 50) { [weak self] listResult in
                guard let self, self.isVisibleWorkspace,
                      self.readGeneration == generation,
                      self.activeReadRequest === composite else { return }
                _ = composite.complete()
                self.activeReadRequest = nil
                self.applySessionListResult(listResult, preserveSelection: preserveSelection, refreshEvents: true)
                self.finishConfirmedWorkspaceReadIfReady()
                self.drainPendingMarketFeedInvalidation()
            }
            _ = composite.addChild(listReq)
        }
        _ = composite.addChild(optReq)
    }

    private func reloadSessionsAndDetail(preserveSelection: Bool, refreshEvents: Bool) {
        guard isVisibleWorkspace else {
            sessionSnapshotIsFresh = false
            updateControlEnablement()
            return
        }
        marketFeedInvalidation.clearPending()
        if refreshEvents { cancelEventsRead() }
        readGeneration &+= 1
        let generation = readGeneration
        activeReadRequest?.cancel()
        let composite = NativeMCPRequest()
        activeReadRequest = composite
        sessionSnapshotIsFresh = false
        updateControlEnablement()
        let listRequest = service.listSessions(limit: 50) { [weak self] listResult in
            guard let self, self.isVisibleWorkspace,
                  self.readGeneration == generation,
                  self.activeReadRequest === composite else { return }
            _ = composite.complete()
            self.activeReadRequest = nil
            self.applySessionListResult(
                listResult,
                preserveSelection: preserveSelection,
                refreshEvents: refreshEvents
            )
            self.updateControlEnablement()
            self.drainPendingMarketFeedInvalidation()
        }
        _ = composite.addChild(listRequest)
    }

    private func drainPendingMarketFeedInvalidation() {
        guard marketFeedInvalidation.takeIfReady(
            isVisible: isVisibleWorkspace,
            readInFlight: activeReadRequest != nil || activeEventsReadRequest != nil
                || activeDecisionReadRequest != nil,
            writeInFlight: activeWriteRequest != nil
        ) else { return }
        reloadSessionsAndDetail(preserveSelection: true, refreshEvents: false)
    }

    private func cancelEventsRead() {
        eventsReadGeneration &+= 1
        decisionReadGeneration &+= 1
        activeEventsReadRequest?.cancel()
        activeEventsReadRequest = nil
        activeDecisionReadRequest?.cancel()
        activeDecisionReadRequest = nil
        selectedDecisionEvent = nil
        eventsTable.deselectAll(nil)
        decisionSignalPopup.removeAllItems()
        decisionSignalPopup.isHidden = true
        decisionReviewStatusLabel.stringValue = "이벤트 행을 선택한 뒤 당시 저장 근거를 조회하세요."
        decisionReviewText.string = ""
        updateDecisionReviewEnablement()
    }

    private func populateStartOptions(_ options: PaperSessionStartOptions) {
        let previousTitle = policyPopup.selectedItem?.title
        policyPopup.removeAllItems()
        supportedPolicyOptions = options.policy_options.filter(\.supported)
        for opt in supportedPolicyOptions {
            let revShort = String(opt.reference.revision_id.prefix(12))
            let title = "\(opt.name) [\(opt.family) · H1 기준 예열 \(opt.decision_warmup_bars)봉 · \(revShort)]"
            policyPopup.addItem(withTitle: title)
            policyPopup.lastItem?.toolTip = "policy_id: \(opt.reference.policy_id)\nrevision_id: \(opt.reference.revision_id)\ndigest: \(opt.reference.definition_digest)"
        }
        if let previousTitle, policyPopup.itemTitles.contains(previousTitle) {
            policyPopup.selectItem(withTitle: previousTitle)
        } else if let s1Idx = supportedPolicyOptions.firstIndex(where: { $0.family == "S1" }) {
            policyPopup.selectItem(at: s1Idx)
        }

        let defaultInterval = options.decision_intervals.first ?? "h1"
        let currentInterval = intervalPopup.selectedItem?.title ?? defaultInterval
        intervalPopup.removeAllItems()
        intervalPopup.addItems(withTitles: options.decision_intervals)
        if options.decision_intervals.contains(currentInterval) {
            intervalPopup.selectItem(withTitle: currentInterval)
        } else {
            intervalPopup.selectItem(withTitle: defaultInterval)
        }

        if let preset = options.scenario_presets.first {
            if initialCashField.stringValue == "10000" {
                initialCashField.stringValue = preset.initial_cash
            }
            let rule = preset.market_rules
            scenarioRuleLabel.stringValue = "시장 규칙: \(rule.kind) (동결 가상 시나리오 · 최소 주문 금액 ₩\(rule.min_notional) · 수량 단위 \(rule.quantity_step)) · 한도: 최대 세션 \(options.limits.active_sessions)개 / 시장 \(options.limits.markets_per_session)개 · 체결: \(preset.execution.kind)"
        }
        updateControlEnablement()
    }

    private func applySessionListResult(
        _ result: Result<PaperSessionPage, Error>,
        preserveSelection: Bool,
        refreshEvents: Bool
    ) {
        switch result {
        case .failure(let error):
            sessionSnapshotIsFresh = false
            readFailureMessage = "세션 목록 조회 실패: \(error.localizedDescription) (마지막 확인된 세션 스냅샷을 유지하며 쓰기는 차단합니다)"
            statusBanner.stringValue = "지속 모의 세션 조회 실패: \(error.localizedDescription) (마지막 확인된 세션 스냅샷을 유지합니다)"
            statusBanner.textColor = TerminalTheme.negative
            renderSelectedSession()
            updateControlEnablement()
        case .success(let page):
            sessionSnapshotIsFresh = true
            sessionSnapshotReceivedAt = Date()
            if startOptionsAreFresh { readFailureMessage = nil }
            sessions = page.items
            let runningCount = sessions.filter { $0.state == "RUNNING" }.count
            let pausedCount = sessions.filter { $0.state == "PAUSED" }.count
            let recoveryCount = sessions.filter { $0.state == "RECOVERY_REQUIRED" }.count
            let summary: String
            if sessions.isEmpty {
                summary = "지속 모의: 세션 없음"
            } else {
                summary = "지속 모의: 총 \(sessions.count)건 (RUNNING \(runningCount) · PAUSED \(pausedCount)\(recoveryCount > 0 ? " · 복구필요 \(recoveryCount)" : "")) [VIRTUAL]"
            }
            onSummaryChanged(summary)

            let targetSessionId: String?
            if preserveSelection, let currentId = selectedSession?.session_id,
               sessions.contains(where: { $0.session_id == currentId }) {
                targetSessionId = currentId
            } else {
                targetSessionId = sessions.first?.session_id
            }

            sessionPopup.removeAllItems()
            if sessions.isEmpty {
                sessionPopup.addItem(withTitle: "등록된 지속 모의 세션 없음")
                selectedSession = nil
                events = []
                nextEventAfterSequence = nil
                totalEventCount = 0
                eventsAsOfSequence = nil
                eventsSnapshotSessionID = nil
                statusBanner.stringValue = "원장에 등록된 지속 모의 세션이 없습니다. 위 생성 폼에서 새 VIRTUAL 세션을 시작할 수 있습니다."
                statusBanner.textColor = TerminalTheme.secondaryText
                renderSelectedSession()
                updateControlEnablement()
                return
            }

            for session in sessions {
                let shortId = String(session.session_id.prefix(16))
                let itemTitle = "[\(session.state)] \(session.policy_ref.policy_id) · \(session.decision_interval) · seq \(session.sequence) · \(shortId)"
                sessionPopup.addItem(withTitle: itemTitle)
            }

            if let targetSessionId,
               let idx = sessions.firstIndex(where: { $0.session_id == targetSessionId }) {
                sessionPopup.selectItem(at: idx)
                selectedSession = sessions[idx]
            } else {
                sessionPopup.selectItem(at: 0)
                selectedSession = sessions.first
            }

            if let readFailureMessage {
                statusBanner.stringValue = readFailureMessage
                statusBanner.textColor = TerminalTheme.negative
            } else {
                statusBanner.stringValue = "VIRTUAL / SIMULATED 지속 모의 세션 \(sessions.count)건 로드 완료 · 실주문 전송 없음 · 모든 상태 전이는 sequence CAS로 보호됩니다."
                statusBanner.textColor = TerminalTheme.secondaryText
            }
            renderSelectedSession()
            updateControlEnablement()
            if refreshEvents || eventsAsOfSequence == nil
                || eventsSnapshotSessionID != selectedSession?.session_id {
                loadEventsForSelectedSession(afterSequence: nil)
            }
        }
    }

    private func renderSelectedSession() {
        marketsTable.reloadData()
        renderMarketTimingDetails()
        guard let session = selectedSession else {
            sessionSummaryLabel.stringValue = "선택된 지속 모의 세션이 없습니다."
            sessionSummaryLabel.toolTip = sessionSummaryLabel.stringValue
            sessionSummaryLabel.setAccessibilityHelp(sessionSummaryLabel.stringValue)
            return
        }
        let evalText = session.last_evaluated_at ?? "예열/대기 중 (확정 봉 미관측)"
        let pausedText = session.paused_at.map { " · 일시정지: \($0)" } ?? ""
        let stoppedText = session.stopped_at.map { " · 종료: \($0)" } ?? ""
        let recoveryText: String
        if session.state == "RECOVERY_REQUIRED" && session.recovery_cause == "PROVIDER_GAP" {
            recoveryText = "\n공급자 공백 복구 · 원문 근거를 검증하고 실제 봉만으로 기존 상태를 동기화합니다. 현재 경계까지 확인하면 같은 정책·세션으로 이어서 처리합니다. 미확인 공백은 보류합니다.\n근거: \(session.recovery_detail ?? "확인된 공급자 공백")"
        } else if session.canOfferFeedGapRecoveryResume {
            recoveryText = "\n복구 사유: FEED_GAP · 사용자가 ‘복구 후 재개’를 선택할 때만 확정 봉을 확인하고 재개합니다. 과거 판단은 다시 만들지 않습니다."
        } else if session.state == "RECOVERY_REQUIRED" {
            recoveryText = "\n복구 재개 차단 · 원인: \(session.recovery_cause ?? "미확인") · 안전한 재개 경로가 없습니다. Stop으로 안전 종료를 요청할 수 있습니다."
        } else {
            recoveryText = ""
        }
        sessionSummaryLabel.stringValue = """
        세션 ID: \(session.session_id) · 모드: \(session.mode) (VIRTUAL) · 상태: \(session.stateDisplay) · Sequence: \(session.sequence) · 이벤트: \(session.event_count)건
        동결 정책: \(session.policy_ref.policy_id) · 리비전: \(session.policy_ref.revision_id) · Digest: \(String(session.policy_ref.definition_digest.prefix(16)))…
        간격/체결: \(session.decision_interval) → \(session.execution_resolution) · 시장 수: \(session.markets.count)개 (시장별 독립 가상 계좌)
        시작 시각(UTC): \(session.started_at) · 마지막 세션 처리(UTC): \(evalText)\(pausedText)\(stoppedText)\(recoveryText)
        """
        sessionSummaryLabel.toolTip = sessionSummaryLabel.stringValue
        sessionSummaryLabel.setAccessibilityHelp(sessionSummaryLabel.stringValue)
    }

    private func renderMarketTimingDetails() {
        let now = Date()
        let formatter = PaperMarketTimingFormatter()
        marketTimingStatusLabel.stringValue = """
        화면 관측 시각(UTC): \(formatter.utcLabel(for: now)) · 마지막 응답 수신(UTC): \(formatter.receivedLabel(sessionSnapshotReceivedAt, at: now)) · 화면 재조회 설정: 15초(보이는 동안)
        세션 입력: 제한된 확정 봉 폴링 · WebSocket 상태 snapshot은 별도 연결 투영이며 확정 봉/세션 입력 신선도의 증거가 아닙니다.
        """
        marketTimingStatusLabel.toolTip = marketTimingStatusLabel.stringValue
        marketTimingStatusLabel.setAccessibilityLabel(marketTimingStatusLabel.stringValue)
        marketTimingRows = selectedSession.map { formatter.rows(for: $0.markets, at: now) } ?? []
        marketTimingTable.reloadData()
    }

    private func loadEventsForSelectedSession(afterSequence: UInt64?) {
        guard let session = selectedSession else {
            cancelEventsRead()
            events = []
            nextEventAfterSequence = nil
            totalEventCount = 0
            eventsAsOfSequence = nil
            eventsSnapshotSessionID = nil
            eventsTable.reloadData()
            eventsPageLabel.stringValue = "이벤트 0건 · 세션 없음"
            drainPendingMarketFeedInvalidation()
            return
        }
        let fixedCutoff: UInt64
        if afterSequence == nil {
            fixedCutoff = session.sequence
        } else if let eventsAsOfSequence {
            fixedCutoff = eventsAsOfSequence
        } else {
            eventsPageLabel.stringValue = "고정 cutoff가 없어 후속 페이지 조회를 중단했습니다. 첫 페이지를 다시 여세요."
            return
        }
        cancelEventsRead()
        if afterSequence == nil {
            eventsAsOfSequence = nil
            eventsSnapshotSessionID = nil
        }
        let targetId = session.session_id
        let ticket = eventsReadGeneration
        let request = service.reviewEvents(
            sessionId: targetId,
            asOfSequence: fixedCutoff,
            afterSequence: afterSequence,
            limit: 50
        ) { [weak self] result in
            guard let self, self.isVisibleWorkspace,
                  self.eventsReadGeneration == ticket,
                  self.selectedSession?.session_id == targetId else { return }
            self.activeEventsReadRequest = nil
            switch result {
            case .failure(let error):
                self.eventsPageLabel.stringValue = "이벤트 조회 오류: \(error.localizedDescription)"
            case .success(let page):
                self.events = page.page.items
                self.eventsAsOfSequence = page.as_of_sequence
                self.eventsSnapshotSessionID = targetId
                self.nextEventAfterSequence = page.page.next_cursor?.sequence
                self.totalEventCount = page.page.total_count
                self.eventsPageLabel.stringValue = "표시 \(page.page.items.count)건 / 총 \(page.page.total_count)건 · 고정 cutoff #\(page.as_of_sequence)"
                self.eventsTable.deselectAll(nil)
                self.eventsTable.reloadData()
                self.updateDecisionSelectionPresentation()
                self.updateControlEnablement()
            }
            self.drainPendingMarketFeedInvalidation()
        }
        if eventsReadGeneration == ticket && request.isPending {
            activeEventsReadRequest = request
        }
    }

    private var writeFenceReason: String? {
        guard isVisibleWorkspace else { return "지속 모의 화면이 활성화된 뒤 다시 확인하세요." }
        if let pendingUnknownRequestId {
            return "미확인 쓰기 요청 \(pendingUnknownRequestId)의 영수증 재조회와 새로고침이 끝날 때까지 쓰기를 차단합니다."
        }
        if activeWriteRequest != nil { return "이전 쓰기 요청의 결과를 확인하는 중입니다." }
        if activeReadRequest != nil { return "시작 옵션과 최신 세션 스냅샷을 확인하는 중입니다." }
        if !startOptionsAreFresh {
            return readFailureMessage ?? "시작 옵션을 최신 상태로 확인하지 못했습니다. 새로고침하세요."
        }
        if !sessionSnapshotIsFresh {
            return readFailureMessage ?? "세션 목록을 최신 상태로 확인하지 못했습니다. 새로고침하세요."
        }
        return nil
    }

    private func requireWriteReadiness(for action: String) -> Bool {
        guard let reason = writeFenceReason else { return true }
        statusBanner.stringValue = "\(action) 차단: \(reason)"
        statusBanner.textColor = TerminalTheme.negative
        updateControlEnablement()
        return false
    }

    private func finishConfirmedWorkspaceReadIfReady() {
        guard startOptionsAreFresh, sessionSnapshotIsFresh else {
            if let readFailureMessage {
                statusBanner.stringValue = readFailureMessage
                statusBanner.textColor = TerminalTheme.negative
            }
            updateControlEnablement()
            return
        }
        readFailureMessage = nil
        if let requestId = pendingUnknownRequestId,
           pendingReadbackConfirmationRequestId == requestId {
            pendingUnknownRequestId = nil
            pendingReadbackConfirmationRequestId = nil
            statusBanner.stringValue = "요청(\(requestId)) 재조회와 최신 시작 옵션·세션 스냅샷을 확인했습니다. 새 요청을 시작할 수 있습니다."
            statusBanner.textColor = TerminalTheme.positive
        }
        updateControlEnablement()
    }

    private func updateControlEnablement() {
        let writesReady = writeFenceReason == nil
        startButton.isEnabled = writesReady && !supportedPolicyOptions.isEmpty
        readbackButton.isHidden = (pendingUnknownRequestId == nil)
        readbackButton.isEnabled = isVisibleWorkspace && activeReadRequest == nil
            && activeWriteRequest == nil && pendingUnknownRequestId != nil

        let canOfferFeedGapRecovery = selectedSession?.canOfferFeedGapRecoveryResume == true
        resumeButton.title = canOfferFeedGapRecovery ? "복구 후 재개" : "세션 재개"
        resumeButton.setAccessibilityLabel(canOfferFeedGapRecovery
            ? "FEED_GAP 확정 봉 복구 후 지속 모의 세션 재개"
            : "일시정지된 지속 모의 세션 재개")
        resumeButton.toolTip = canOfferFeedGapRecovery
            ? "서버가 FEED_GAP으로 표시한 세션에서만 공개 확정 봉을 확인한 뒤 같은 Resume 요청으로 재개합니다."
            : (selectedSession?.state == "RECOVERY_REQUIRED"
                ? "복구 원인 \(selectedSession?.recovery_cause ?? "미확인")은 이 화면에서 재개할 수 없습니다."
                : nil)
        resumeButton.setAccessibilityHelp(resumeButton.toolTip)

        if let session = selectedSession, writesReady {
            pauseButton.isEnabled = (session.state == "RUNNING")
            resumeButton.isEnabled = session.state == "PAUSED" || session.canOfferFeedGapRecoveryResume
            stopButton.isEnabled = (session.state == "RUNNING"
                || session.state == "PAUSED"
                || session.state == "RECOVERY_REQUIRED")
            copyIdButton.isEnabled = true
            eventsFirstPageButton.isEnabled = true
            eventsNextPageButton.isEnabled = (nextEventAfterSequence != nil)
        } else {
            pauseButton.isEnabled = false
            resumeButton.isEnabled = false
            stopButton.isEnabled = false
            copyIdButton.isEnabled = (selectedSession != nil)
            eventsFirstPageButton.isEnabled = false
            eventsNextPageButton.isEnabled = false
        }
        updateDecisionReviewEnablement()
    }

    private func updateDecisionReviewEnablement() {
        guard let event = selectedDecisionEvent,
              let asOfSequence = eventsAsOfSequence,
              event.sequence <= asOfSequence else {
            decisionReviewButton.isEnabled = false
            return
        }
        let signalSelectionIsReady = event.signals.count <= 1
            || (decisionSignalPopup.selectedItem?.tag ?? -1) >= 0
        decisionReviewButton.isEnabled = isVisibleWorkspace
            && activeEventsReadRequest == nil
            && activeDecisionReadRequest == nil
            && activeReadRequest == nil
            && signalSelectionIsReady
    }

    private func updateDecisionSelectionPresentation() {
        decisionReviewText.string = ""
        decisionSignalPopup.removeAllItems()
        guard let event = selectedDecisionEvent else {
            decisionSignalPopup.isHidden = true
            decisionReviewStatusLabel.stringValue = "이벤트 행을 선택한 뒤 당시 저장 근거를 조회하세요."
            updateDecisionReviewEnablement()
            return
        }

        if event.signals.count > 1 {
            decisionSignalPopup.addItem(withTitle: "설명할 신호 선택…")
            decisionSignalPopup.lastItem?.tag = -1
            decisionSignalPopup.lastItem?.toolTip = "선택 신호를 고르기 전에는 조회하지 않습니다."
            for (index, signal) in event.signals.enumerated() {
                decisionSignalPopup.addItem(
                    withTitle: "\(signal.strategy) · \(signal.state_before) → \(signal.state_after) · \(String(signal.signal_id.prefix(8)))"
                )
                decisionSignalPopup.lastItem?.tag = index
                decisionSignalPopup.lastItem?.toolTip = "signal_id: \(signal.signal_id) · \(signal.outcome)"
            }
            decisionSignalPopup.selectItem(at: 0)
            decisionSignalPopup.isHidden = false
        } else {
            decisionSignalPopup.isHidden = true
        }

        let cutoff = eventsAsOfSequence.map(String.init) ?? "미확인"
        let market = event.market ?? "SESSION"
        let selection = event.signals.count > 1
            ? "신호 여러 개 · 조회할 신호 선택 필요"
            : (event.signals.first.map { "신호 \($0.signal_id)" } ?? "신호 없음 · 비판단 이벤트 여부 조회")
        decisionReviewStatusLabel.stringValue = "VIRTUAL / SIMULATED · \(selectedSession?.session_id ?? "세션 미확인") · \(market) · 이벤트 #\(event.sequence) · 고정 cutoff #\(cutoff) · \(selection)"
        decisionReviewStatusLabel.toolTip = decisionReviewStatusLabel.stringValue
        decisionReviewStatusLabel.setAccessibilityValue(decisionReviewStatusLabel.stringValue)
        updateDecisionReviewEnablement()
    }

    // MARK: - Actions

    @objc private func handleSessionSelectionChanged() {
        let idx = sessionPopup.indexOfSelectedItem
        guard sessions.indices.contains(idx) else { return }
        selectedSession = sessions[idx]
        renderSelectedSession()
        updateControlEnablement()
        loadEventsForSelectedSession(afterSequence: nil)
    }

    @objc private func handleManualRefresh() {
        reloadWorkspace(preserveSelection: true)
    }

    @objc private func handleLoadFirstEvents() {
        loadEventsForSelectedSession(afterSequence: nil)
    }

    @objc private func handleLoadNextEvents() {
        guard let after = nextEventAfterSequence else { return }
        loadEventsForSelectedSession(afterSequence: after)
    }

    @objc private func handleDecisionSignalSelectionChanged() {
        updateDecisionReviewEnablement()
    }

    @objc private func handleReviewSelectedDecision() {
        guard isVisibleWorkspace,
              let session = selectedSession,
              let event = selectedDecisionEvent,
              let cutoff = eventsAsOfSequence,
              event.sequence <= cutoff else { return }
        let signalID: String?
        if event.signals.isEmpty {
            signalID = nil
        } else if event.signals.count == 1 {
            signalID = event.signals.first?.signal_id
        } else if let item = decisionSignalPopup.selectedItem, item.tag >= 0,
                  event.signals.indices.contains(item.tag) {
            signalID = event.signals[item.tag].signal_id
        } else {
            decisionReviewStatusLabel.stringValue = "여러 신호가 있는 이벤트입니다. 설명할 신호를 선택하세요."
            decisionReviewStatusLabel.textColor = TerminalTheme.negative
            return
        }

        activeDecisionReadRequest?.cancel()
        decisionReadGeneration &+= 1
        let ticket = decisionReadGeneration
        let sessionID = session.session_id
        let eventSequence = event.sequence
        let policyReference = session.policy_ref
        decisionReviewStatusLabel.stringValue = "저장 판단 읽는 중 · 이벤트 #\(eventSequence) · 고정 cutoff #\(cutoff)"
        decisionReviewStatusLabel.textColor = TerminalTheme.secondaryText
        decisionReviewText.string = ""
        updateDecisionReviewEnablement()
        let request = service.reviewDecision(
            sessionId: sessionID,
            eventSequence: eventSequence,
            asOfSequence: cutoff,
            signalID: signalID,
            expectedPolicyReference: policyReference
        ) { [weak self] result in
            DispatchQueue.main.async {
                guard let self, self.isVisibleWorkspace,
                      self.decisionReadGeneration == ticket,
                      self.selectedSession?.session_id == sessionID,
                      self.selectedDecisionEvent?.sequence == eventSequence,
                      self.eventsAsOfSequence == cutoff else { return }
                self.activeDecisionReadRequest = nil
                switch result {
                case .failure(let error):
                    self.decisionReviewStatusLabel.stringValue = "저장 판단 조회 실패 · \(error.localizedDescription)"
                    self.decisionReviewStatusLabel.textColor = TerminalTheme.negative
                    self.decisionReviewText.string = ""
                case .success(let review):
                    let policy = review.policy.reference
                    let selectedSignal = review.selected_signal_id ?? "미기록"
                    self.decisionReviewStatusLabel.stringValue = "VIRTUAL / SIMULATED · \(review.selection.session_id) · 이벤트 #\(review.selection.event_sequence) · as_of #\(review.selection.as_of_sequence) · \(review.event.market ?? "SESSION") · 정책 \(policy.policy_id) / \(policy.revision_id) / \(policy.definition_digest) · 신호 \(selectedSignal) · 원장 시각 \(review.event.occurredAtDisplay)"
                    self.decisionReviewStatusLabel.textColor = TerminalTheme.secondaryText
                    self.decisionReviewStatusLabel.toolTip = self.decisionReviewStatusLabel.stringValue
                    self.decisionReviewStatusLabel.setAccessibilityValue(self.decisionReviewStatusLabel.stringValue)
                    let preparation = RecordedDecisionPresentation.preparationLines(
                        for: review.event.preparation,
                        masked: false
                    )
                    let decision = RecordedDecisionPresentation.lines(
                        for: review.recorded_decision,
                        masked: false
                    )
                    self.decisionReviewText.string = (preparation + decision).joined(separator: "\n")
                    self.decisionReviewText.setAccessibilityValue(self.decisionReviewText.string)
                }
                self.updateDecisionReviewEnablement()
                self.drainPendingMarketFeedInvalidation()
            }
        }
        if decisionReadGeneration == ticket && request.isPending {
            activeDecisionReadRequest = request
        }
        updateDecisionReviewEnablement()
    }

    @objc private func handleCopySessionIdentity() {
        guard let session = selectedSession else { return }
        let text = "session_id=\(session.session_id) policy_id=\(session.policy_ref.policy_id) revision_id=\(session.policy_ref.revision_id) digest=\(session.policy_ref.definition_digest)"
        NSPasteboard.general.clearContents()
        NSPasteboard.general.setString(text, forType: .string)
        statusBanner.stringValue = "클립보드에 세션 및 동결 정책 리비전 식별자를 복사했습니다: \(session.session_id)"
        statusBanner.textColor = TerminalTheme.positive
    }

    @objc private func handleStartSession() {
        guard requireWriteReadiness(for: "세션 생성 및 시작") else { return }
        guard let options = startOptions,
              let preset = options.scenario_presets.first,
              !supportedPolicyOptions.isEmpty else { return }
        let policyIdx = max(0, policyPopup.indexOfSelectedItem)
        guard supportedPolicyOptions.indices.contains(policyIdx) else { return }
        let selectedPolicy = supportedPolicyOptions[policyIdx]
        let interval = intervalPopup.selectedItem?.title ?? (options.decision_intervals.first ?? "h1")

        var selectedMarkets: [String] = []
        if btcCheck.state == .on { selectedMarkets.append("KRW-BTC") }
        if ethCheck.state == .on { selectedMarkets.append("KRW-ETH") }
        if solCheck.state == .on { selectedMarkets.append("KRW-SOL") }
        guard !selectedMarkets.isEmpty else {
            statusBanner.stringValue = "최소 1개 이상의 시장을 선택하세요 (예: KRW-BTC)."
            statusBanner.textColor = TerminalTheme.negative
            return
        }

        let cashText = initialCashField.stringValue.trimmingCharacters(in: .whitespacesAndNewlines)
        guard let cashDec = Decimal(string: cashText, locale: Locale(identifier: "en_US_POSIX")),
              cashDec > 0 else {
            statusBanner.stringValue = "시장별 초기 가상 자본은 0보다 큰 정확한 십진수(KRW)여야 합니다."
            statusBanner.textColor = TerminalTheme.negative
            return
        }

        let feeText = feeBpsField.stringValue.trimmingCharacters(in: .whitespacesAndNewlines)
        let slipText = slippageBpsField.stringValue.trimmingCharacters(in: .whitespacesAndNewlines)
        guard let feeDec = Decimal(string: feeText, locale: Locale(identifier: "en_US_POSIX")), feeDec >= 0,
              let slipDec = Decimal(string: slipText, locale: Locale(identifier: "en_US_POSIX")), slipDec >= 0 else {
            statusBanner.stringValue = "수수료 및 슬리피지(bps)는 0 이상의 십진수여야 합니다."
            statusBanner.textColor = TerminalTheme.negative
            return
        }

        let costs = PaperCostPolicy(
            buy_fee_bps: feeText,
            sell_fee_bps: feeText,
            maker_fee_bps: preset.costs.maker_fee_bps,
            half_spread_bps: preset.costs.half_spread_bps,
            slippage_bps: slipText,
            impact_bps: preset.costs.impact_bps,
            assumption_label: preset.costs.assumption_label,
            dynamic: preset.costs.dynamic
        )
        let marketInputs = selectedMarkets.map { mkt in
            PaperMarketStartInput(
                market: mkt,
                initialCash: cashText,
                costs: costs,
                marketRules: preset.market_rules
            )
        }

        let requestId = "paper-start-\(UUID().uuidString.lowercased())"
        statusBanner.stringValue = "지속 모의 세션 생성 및 시작 요청 중… (request_id: \(requestId))"
        statusBanner.textColor = TerminalTheme.secondaryText

        activeWriteRequest = service.startSession(
            requestId: requestId,
            policyRef: selectedPolicy.reference,
            decisionInterval: interval,
            executionResolution: interval,
            latencyMs: preset.latency_ms,
            execution: preset.execution,
            markets: marketInputs
        ) { [weak self] result in
            guard let self else { return }
            self.activeWriteRequest = nil
            self.handleWriteCommandResult(result, actionLabel: "세션 생성 및 시작")
        }
        updateControlEnablement()
    }

    @objc private func handlePauseSession() {
        guard requireWriteReadiness(for: "세션 일시정지"),
              let session = selectedSession, session.state == "RUNNING" else { return }
        let requestId = "paper-pause-\(session.session_id)-\(session.sequence)"
        statusBanner.stringValue = "세션 일시정지 요청 중… (expected_sequence: \(session.sequence))"
        activeWriteRequest = service.pauseSession(
            requestId: requestId,
            sessionId: session.session_id,
            expectedSequence: session.sequence
        ) { [weak self] result in
            guard let self else { return }
            self.activeWriteRequest = nil
            self.handleWriteCommandResult(result, actionLabel: "세션 일시정지")
        }
        updateControlEnablement()
    }

    @objc private func handleResumeSession() {
        guard let session = selectedSession else { return }
        let recoveryResume = session.canOfferFeedGapRecoveryResume
        let actionLabel = recoveryResume ? "복구 후 재개" : "세션 재개"
        guard requireWriteReadiness(for: actionLabel),
              session.state == "PAUSED" || recoveryResume else { return }
        let requestId = "paper-resume-\(session.session_id)-\(session.sequence)"
        statusBanner.stringValue = "\(actionLabel) 요청 중… (expected_sequence: \(session.sequence))"
        activeWriteRequest = service.resumeSession(
            requestId: requestId,
            sessionId: session.session_id,
            expectedSequence: session.sequence
        ) { [weak self] result in
            guard let self else { return }
            self.activeWriteRequest = nil
            self.handleWriteCommandResult(result, actionLabel: actionLabel)
        }
        updateControlEnablement()
    }

    @objc private func handleStopSession() {
        guard requireWriteReadiness(for: "세션 종료"),
              let session = selectedSession,
              session.state == "RUNNING" || session.state == "PAUSED" || session.state == "RECOVERY_REQUIRED" else { return }
        let requestId = "paper-stop-\(session.session_id)-\(session.sequence)"
        statusBanner.stringValue = "세션 종료 요청 중… (expected_sequence: \(session.sequence))"
        activeWriteRequest = service.stopSession(
            requestId: requestId,
            sessionId: session.session_id,
            expectedSequence: session.sequence
        ) { [weak self] result in
            guard let self else { return }
            self.activeWriteRequest = nil
            self.handleWriteCommandResult(result, actionLabel: "세션 종료")
        }
        updateControlEnablement()
    }

    @objc private func handleReadbackUnknownRequest() {
        guard isVisibleWorkspace, activeReadRequest == nil, activeWriteRequest == nil,
              let requestId = pendingUnknownRequestId else { return }
        statusBanner.stringValue = "미확인 요청(\(requestId)) 영속 영수증(receipt) 재조회 중…"
        activeWriteRequest = service.readbackRequest(requestId: requestId) { [weak self] result in
            guard let self else { return }
            self.activeWriteRequest = nil
            switch result {
            case .failure(let error):
                self.pendingReadbackConfirmationRequestId = nil
                self.statusBanner.stringValue = "요청(\(requestId)) 재조회 실패: \(error.localizedDescription)"
                self.statusBanner.textColor = TerminalTheme.negative
            case .success(let readback):
                if readback.found, let receipt = readback.receipt {
                    self.pendingReadbackConfirmationRequestId = requestId
                    if let session = readback.session {
                        self.selectedSession = session
                    }
                    self.statusBanner.stringValue = "요청(\(receipt.request_id)) 영수증 확인: \(receipt.action) [\(receipt.outcome)] · seq \(receipt.sequence) · 최신 옵션과 세션 목록을 다시 확인합니다."
                    self.statusBanner.textColor = TerminalTheme.secondaryText
                    self.reloadWorkspace(preserveSelection: true)
                } else if !readback.found {
                    self.pendingReadbackConfirmationRequestId = requestId
                    self.statusBanner.stringValue = "요청(\(requestId)) 영수증이 없습니다. 최신 옵션과 세션 목록을 확인할 때까지 새 쓰기를 차단합니다."
                    self.statusBanner.textColor = TerminalTheme.secondaryText
                    self.reloadWorkspace(preserveSelection: true)
                } else {
                    self.pendingReadbackConfirmationRequestId = nil
                    self.statusBanner.stringValue = "요청(\(requestId)) readback이 유효하지 않아 새 쓰기를 계속 차단합니다."
                    self.statusBanner.textColor = TerminalTheme.negative
                }
            }
            self.updateControlEnablement()
            self.drainPendingMarketFeedInvalidation()
        }
        updateControlEnablement()
    }

    private func handleWriteCommandResult(_ result: Result<PaperSessionCommandResponse, Error>, actionLabel: String) {
        switch result {
        case .failure(let error):
            if case PaperSessionServiceError.unknownMutationOutcome(let requestId, _) = error {
                pendingUnknownRequestId = requestId
                pendingReadbackConfirmationRequestId = nil
                sessionSnapshotIsFresh = false
                readFailureMessage = "요청 결과가 미확인 상태입니다. 영수증 readback과 최신 새로고침이 끝날 때까지 모든 쓰기를 차단합니다."
            }
            statusBanner.stringValue = "\(actionLabel) 실패: \(error.localizedDescription)"
            statusBanner.textColor = TerminalTheme.negative
            updateControlEnablement()
        case .success(let response):
            pendingUnknownRequestId = nil
            pendingReadbackConfirmationRequestId = nil
            selectedSession = response.session
            sessionSnapshotIsFresh = true
            readFailureMessage = nil
            statusBanner.stringValue = "\(actionLabel) 완료 [\(response.receipt.outcome)] · 세션 \(response.session.session_id) · 상태 \(response.session.state) · seq \(response.session.sequence)"
            statusBanner.textColor = TerminalTheme.positive
            reloadSessionsAndDetail(preserveSelection: true, refreshEvents: true)
        }
        drainPendingMarketFeedInvalidation()
    }

    // MARK: - NSTableViewDataSource & Delegate

    func tableViewSelectionDidChange(_ notification: Notification) {
        guard let table = notification.object as? NSTableView, table === eventsTable else { return }
        let row = table.selectedRow
        selectedDecisionEvent = events.indices.contains(row) ? events[row] : nil
        decisionReviewStatusLabel.textColor = TerminalTheme.secondaryText
        updateDecisionSelectionPresentation()
    }

    func numberOfRows(in tableView: NSTableView) -> Int {
        if tableView === marketTimingTable {
            return marketTimingRows.count
        }
        if tableView === marketsTable {
            return selectedSession?.markets.count ?? 0
        }
        if tableView === eventsTable {
            return events.count
        }
        return 0
    }

    func tableView(_ tableView: NSTableView, viewFor tableColumn: NSTableColumn?, row: Int) -> NSView? {
        guard let colId = tableColumn?.identifier.rawValue else { return nil }
        if tableView === marketTimingTable {
            guard marketTimingRows.indices.contains(row) else { return nil }
            return makeMarketTimingCell(marketTimingRows[row], columnId: colId)
        }
        if tableView === marketsTable {
            guard let markets = selectedSession?.markets, markets.indices.contains(row) else { return nil }
            return makeMarketCell(markets[row], columnId: colId)
        }
        if tableView === eventsTable {
            guard events.indices.contains(row) else { return nil }
            return makeEventCell(events[row], columnId: colId)
        }
        return nil
    }

    private func makeMarketTimingCell(_ row: PaperMarketTimingRow, columnId: String) -> NSView {
        let fieldLabel: String
        let text: String
        switch columnId {
        case "TimingMarket":
            fieldLabel = "시장"
            text = row.market
        case "SourceEventAt":
            fieldLabel = "마지막 원천 이벤트"
            text = row.sourceEvent
        case "FinalizedCandleAt":
            fieldLabel = "마지막 확정 봉"
            text = row.finalizedCandle
        case "AccountMarkAt":
            fieldLabel = "마지막 계정 평가"
            text = row.accountMark
        case "EngineHeartbeatAt":
            fieldLabel = "엔진 heartbeat"
            text = row.engineHeartbeat
        default:
            return NSTextField(labelWithString: "—")
        }

        let field = NSTextField(labelWithString: text)
        field.font = TerminalTheme.table
        field.textColor = text.contains("경과 알 수 없음") || text.contains("미관측")
            ? TerminalTheme.secondaryText
            : (text.contains("미래 시각") || text.contains("해석 불가") ? .systemOrange : .labelColor)
        field.lineBreakMode = .byTruncatingTail
        field.toolTip = "\(fieldLabel): \(text)"
        field.setAccessibilityLabel("\(fieldLabel): \(text)")
        return field
    }

    private func makeMarketCell(_ m: PaperMarketSnapshot, columnId: String) -> NSView {
        var text = "—"
        var color: NSColor = .labelColor
        var alignment: NSTextAlignment = .left

        switch columnId {
        case "Market":
            text = m.market
        case "Freshness":
            text = m.freshness
            switch m.freshness {
            case "LIVE": color = TerminalTheme.positive
            case "DELAYED": color = .systemOrange
            case "STALE", "OFFLINE": color = TerminalTheme.negative
            default: break
            }
        case "Events":
            text = "\(m.event_count) / 체결 \(m.fill_count)"
            alignment = .right
        case "LastCandle":
            text = m.last_finalized_candle_at ?? "미관측"
        case "AvailCash":
            text = m.cashFreeDecimal.map { Formatters.money($0) } ?? m.cash_free
            alignment = .right
        case "ReservedCash":
            text = m.cashReservedDecimal.map { Formatters.money($0) } ?? m.cash_reserved
            alignment = .right
        case "Qty":
            text = m.quantity
            alignment = .right
        case "MarkPrice":
            text = m.markPriceDecimal.map { Formatters.money($0) } ?? "미확정"
            alignment = .right
        case "Equity":
            text = m.equityDecimal.map { Formatters.money($0) } ?? "미산출(예열 중)"
            alignment = .right
        case "RealizedPnL":
            if let d = m.realizedPnlDecimal {
                text = Formatters.money(d, signed: true)
                color = d > 0 ? TerminalTheme.positive : (d < 0 ? TerminalTheme.negative : .labelColor)
            } else {
                text = m.realized_pnl
            }
            alignment = .right
        case "UnrealizedPnL":
            if let d = m.unrealizedPnlDecimal {
                text = Formatters.money(d, signed: true)
                color = d > 0 ? TerminalTheme.positive : (d < 0 ? TerminalTheme.negative : .labelColor)
            } else {
                text = "—"
            }
            alignment = .right
        case "Fees":
            text = m.totalFeesDecimal.map { Formatters.money($0) } ?? m.total_fees
            alignment = .right
        case "MDD":
            if let dd = m.drawdownFractionDecimal {
                text = Formatters.percent(-dd)
            } else {
                text = "—"
            }
            alignment = .right
        case "Benchmark":
            if let b = m.benchmark,
               let ret = Decimal(string: b.return_fraction, locale: Locale(identifier: "en_US_POSIX")) {
                text = "B&H \(Formatters.percent(ret, signed: true))"
            } else {
                text = m.benchmark_null_reason ?? "미산출"
                color = TerminalTheme.secondaryText
            }
        case "F08":
            text = "\(m.verification.status) (\(m.verification.reason))"
            color = TerminalTheme.secondaryText
        default:
            break
        }

        let tf = NSTextField(labelWithString: text)
        tf.font = TerminalTheme.table
        tf.textColor = color
        tf.alignment = alignment
        tf.lineBreakMode = .byTruncatingTail
        tf.toolTip = text
        return tf
    }

    private func makeEventCell(_ ev: PaperSessionEvent, columnId: String) -> NSView {
        var text = "—"
        var color: NSColor = .labelColor
        var alignment: NSTextAlignment = .left

        switch columnId {
        case "Seq":
            text = "\(ev.sequence)"
            alignment = .right
        case "OccurredAt":
            text = ev.occurredAtDisplay
        case "EventMarket":
            text = ev.market ?? "SESSION"
        case "EventKind":
            text = ev.kindDisplay
            if ev.kind == "RECOVERY_REQUIRED" {
                color = TerminalTheme.negative
            } else if !ev.fills.isEmpty {
                color = TerminalTheme.positive
            }
        case "EventSide":
            text = ev.primarySideDisplay
        case "EventQty":
            text = ev.primaryQtyDisplay
            alignment = .right
        case "EventPrice":
            let raw = ev.primaryPriceDisplay
            if let d = Decimal(string: raw, locale: Locale(identifier: "en_US_POSIX")) {
                text = Formatters.money(d)
            } else {
                text = raw
            }
            alignment = .right
        case "EventFee":
            let raw = ev.primaryFeeDisplay
            if let d = Decimal(string: raw, locale: Locale(identifier: "en_US_POSIX")) {
                text = Formatters.money(d)
            } else {
                text = raw
            }
            alignment = .right
        case "EventEquity":
            let raw = ev.primaryEquityDisplay
            if let d = Decimal(string: raw, locale: Locale(identifier: "en_US_POSIX")) {
                text = Formatters.money(d)
            } else {
                text = raw
            }
            alignment = .right
        case "EventDetail":
            text = ev.summaryDetail
        default:
            break
        }

        let tf = NSTextField(labelWithString: text)
        tf.font = TerminalTheme.table
        tf.textColor = color
        tf.alignment = alignment
        tf.lineBreakMode = .byTruncatingTail
        tf.toolTip = text
        return tf
    }
}
