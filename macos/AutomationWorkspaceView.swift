import AppKit

final class AutomationWorkspaceView: NSView, NSTableViewDataSource, NSTableViewDelegate, NSTextFieldDelegate {
    private static let pendingWriteDefaultsKey = "skyBackcraft.automation.pendingWrite.v2"
    private static let legacyPendingRequestDefaultsKey = "skyBackcraft.automation.pendingRequestID.v1"
    private static let decisionIntervals = ["m1", "m5", "m15", "h1", "h4", "d1"]

    private let service: AutomationSessionMCPService
    private let realtimeFeedService: RealtimeFeedMCPService
    private let realtimeStatusView: RealtimeConnectionStatusView
    private let policyReader: PolicyDetailMCPReader
    private var policies: [RealPolicy]
    private let privacyIsMasked: () -> Bool
    private let onTogglePrivacyMask: () -> Void
    private let onSummaryChanged: (String) -> Void

    private var sessions: [AutomationSessionView] = []
    private var sessionNextOffset: UInt32?
    private var selectedSessionID: String?
    private var selectedSession: AutomationSessionView?
    private var events: [AutomationEvent] = []
    private var eventNextSequence: UInt64?
    private var ledger: AutomationLedgerPage?
    private var ledgerNextSequence: UInt64?
    private var selectedOrderClientIdentifier: String?
    private var accountProjectionBySession: [String: AutomationAccountRiskProjection] = [:]
    private var accountReceivedAtBySession: [String: Date] = [:]
    private var projectionMissingForSelectedSession = false
    private var sessionListReadFailure: String?
    private var selectedProjectionReadAfterListFailure = false
    private var selectedProjectionReadSessionID: String?
    private var selectedProjectionReadStatus: String?
    private var lastSuccessfulReadAt: Date?
    private var lastReadFailure: String?
    private var pendingReadbackStatus: String?

    private var pendingRequestID: String?
    private var pendingWrite: AutomationPendingWrite?
    private var pendingMetadataError: String?
    private var activeWriteRequest: NativeMCPRequest?
    private var activeWriteTicket: UUID?
    private var activeReadRequest: NativeMCPRequest?
    private var readGeneration: UInt64 = 0
    private var realtimeFeedObserverID: UUID?
    private var marketFeedInvalidation = RealtimeReconciliationInvalidation()
    private var policyGeneration: UInt64 = 0
    private var policyReadInFlight = false
    private var pollTimer: Timer?
    private var isVisibleWorkspace = false
    private var reviewWindowController: AutomationReviewWindowController?
    private var reviewWindowSessionID: String?
    private var decisionAnalysisWindowController: DecisionAnalysisWindowController?
    private var decisionAnalysisWindowSessionID: String?

    private var currentPolicy: RealPolicy?
    private var currentPolicyDetail: PolicyDetailSnapshot?
    private var revisionItems: [PolicyRevisionHistoryItem] = []
    private var selectedPolicyReference: AutomationPolicyRevisionReference?
    private var queriedBaselineStatus: AutomationAccountDayBaselineStatus?
    private var baselineSourceMode: AutomationMode?
    private var baselineSourceChoices: [AutomationBaselineSource] = []

    private let statusBanner = NSTextField(wrappingLabelWithString: "전략 자동화: 조회 전")
    private let sessionSummaryLabel = NSTextField(wrappingLabelWithString: "세션을 선택하세요.")
    private let accountBalanceLabel = NSTextField(wrappingLabelWithString: "계좌 잔액: 미확인")
    private let baselineStatusLabel = NSTextField(wrappingLabelWithString: "일일 시작 기준선: 미확인")
    private let admissionBlockReasonLabel = NSTextField(wrappingLabelWithString: "주문 승인 차단 사유: 미확인")
    private let lifecycleAvailabilityLabel = NSTextField(wrappingLabelWithString: "세션 기능과 현재 차단 사유를 읽는 중입니다.")
    private let startReadinessLabel = NSTextField(wrappingLabelWithString: "")
    private let riskUsageLabel = NSTextField(wrappingLabelWithString: "위험 한도 사용량: 미확인")
    private let feedFreshnessLabel = NSTextField(wrappingLabelWithString: "시장 데이터: 미확인")
    private let capabilityLabel = NSTextField(wrappingLabelWithString: "서버가 허용한 기능: 미확인")
    private let mockAssumptionLabel = NSTextField(wrappingLabelWithString: "")
    private let riskIdentityLabel = NSTextField(wrappingLabelWithString: "동결 정책 및 위험 한도: 미확인")
    private let receivedAtLabel = NSTextField(labelWithString: "앱 수신 시각: 미확인")
    private let modeBadge = NSTextField(labelWithString: "세션 모드: 선택 전 · 실주문 전송: 미확인")
    private let privacyButton = NSButton(title: "계좌 금액 숨기기", target: nil, action: nil)
    private let refreshButton = NSButton(title: "새로고침", target: nil, action: nil)
    private let readbackButton = NSButton(title: "요청 결과 확인", target: nil, action: nil)
    private let nextSessionsButton = NSButton(title: "세션 더 보기", target: nil, action: nil)
    private let pauseButton = NSButton(title: "일시정지", target: nil, action: nil)
    private let resumeButton = NSButton(title: "재개", target: nil, action: nil)
    private let stopButton = NSButton(title: "중지", target: nil, action: nil)
    private let cancelOrderButton = NSButton(title: "선택한 모의 주문 취소", target: nil, action: nil)
    private let nextEventsButton = NSButton(title: "다음 이벤트", target: nil, action: nil)
    private let nextLedgerButton = NSButton(title: "다음 원장 페이지", target: nil, action: nil)

    private let baselineDayField = NSTextField(string: "")
    private let baselineSourcePopup = NSPopUpButton(frame: .zero, pullsDown: false)
    private let baselineQuoteFreeField = NSTextField(string: "")
    private let baselineQuoteLockedField = NSTextField(string: "0")
    private let baselineBaseCurrencyField = NSTextField(string: "")
    private let baselineBaseFreeField = NSTextField(string: "0")
    private let baselineBaseLockedField = NSTextField(string: "0")
    private let baselineValuationBasisPopup = NSPopUpButton(frame: .zero, pullsDown: false)
    private let baselineBasePriceField = NSTextField(string: "")
    private let baselineNoteField = NSTextField(string: "")
    private let recordBaselineButton = NSButton(title: "일일 시작 기준선 기록", target: nil, action: nil)
    private let queryBaselineButton = NSButton(title: "기준선 상태 조회", target: nil, action: nil)
    private let baselineSourceNoteLabel = NSTextField(wrappingLabelWithString: "세션을 선택하면 해당 모드의 기준선 출처를 표시합니다.")
    private let baselineQueryResultLabel = NSTextField(wrappingLabelWithString: "세션을 선택하면 계좌의 당일 시작 기준선 상태와 차단 사유를 조회하거나 기록할 수 있습니다.")

    private let policyPopup = NSPopUpButton(frame: .zero, pullsDown: false)
    private let revisionPopup = NSPopUpButton(frame: .zero, pullsDown: false)
    private let startModePopup = NSPopUpButton(frame: .zero, pullsDown: false)
    private let marketField = NSTextField(string: "")
    private let intervalPopup = NSPopUpButton(frame: .zero, pullsDown: false)
    private let activationLossLimitField = NSTextField(string: "")
    private let activationLossRow = NSStackView()
    private let riskLimitsDisclosureButton = NSButton(title: "필수 위험 한도 접기", target: nil, action: nil)
    private let startButton = NSButton(title: "전략 세션 생성 및 시작", target: nil, action: nil)
    private let reviewButton = NSButton(title: "복기 · 성과", target: nil, action: nil)
    private let decisionAnalysisButton = NSButton(title: "판단 분석", target: nil, action: nil)
    private var riskLimitsStack: NSStackView?
    private var riskFields: [String: NSTextField] = [:]

    private let sessionTable = NSTableView()
    private let orderTable = NSTableView()
    private let fillTable = NSTableView()
    private let eventTable = NSTableView()

    private let sessionLimit = 20

    init(
        policies: [RealPolicy],
        service: AutomationSessionMCPService = AutomationSessionMCPService(),
        realtimeFeedService: RealtimeFeedMCPService = .shared,
        policyReader: PolicyDetailMCPReader = PolicyDetailMCPReader(),
        privacyIsMasked: @escaping () -> Bool,
        onTogglePrivacyMask: @escaping () -> Void,
        onSummaryChanged: @escaping (String) -> Void
    ) {
        self.policies = policies
        self.service = service
        self.realtimeFeedService = realtimeFeedService
        self.realtimeStatusView = RealtimeConnectionStatusView(service: realtimeFeedService)
        self.policyReader = policyReader
        self.privacyIsMasked = privacyIsMasked
        self.onTogglePrivacyMask = onTogglePrivacyMask
        self.onSummaryChanged = onSummaryChanged
        let defaults = UserDefaults.standard
        if let data = defaults.data(forKey: Self.pendingWriteDefaultsKey) {
            do {
                let restored = try JSONDecoder().decode(AutomationPendingWrite.self, from: data)
                guard !restored.request_id.isEmpty else {
                    throw AutomationSessionServiceError.invalidResponse("pending request_id")
                }
                pendingWrite = restored
                pendingRequestID = restored.request_id
            } catch {
                pendingMetadataError = "이전 요청 정보를 읽을 수 없어 새 변경 요청을 막았습니다. \(error.localizedDescription)"
            }
        } else if let legacyID = defaults.string(forKey: Self.legacyPendingRequestDefaultsKey) {
            pendingRequestID = legacyID
            pendingMetadataError = "이전 요청의 작업 정보가 없어 결과를 다시 확인할 때까지 새 변경 요청을 막습니다."
        }
        super.init(frame: .zero)
        buildView()
        populatePolicies()
        renderWorkspace()
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

    required init?(coder: NSCoder) {
        return nil
    }

    deinit {
        if let realtimeFeedObserverID {
            realtimeFeedService.removeObserver(realtimeFeedObserverID)
        }
        pollTimer?.invalidate()
        activeReadRequest?.cancel()
        activeWriteRequest?.cancel()
        reviewWindowController?.close()
        closeDecisionAnalysisWindow()
    }

    override func viewDidChangeEffectiveAppearance() {
        super.viewDidChangeEffectiveAppearance()
        for box in subviews.compactMap({ $0 as? NSBox }) {
            box.fillColor = TerminalTheme.panel
            box.borderColor = TerminalTheme.border
        }
    }

    func updatePolicies(_ policies: [RealPolicy]) {
        self.policies = policies
        populatePolicies()
    }

    func refreshPrivacyPresentation() {
        renderWorkspace()
        reviewWindowController?.refreshPrivacyPresentation()
        decisionAnalysisWindowController?.refreshPrivacyPresentation()
    }

    func setWorkspaceVisible(_ visible: Bool) {
        guard visible != isVisibleWorkspace else { return }
        isVisibleWorkspace = visible
        if visible {
            reloadWorkspace()
            retryPolicyReadIfNeeded()
            if pollTimer == nil {
                pollTimer = Timer.scheduledTimer(withTimeInterval: 15.0, repeats: true) { [weak self] _ in
                    guard let self, self.isVisibleWorkspace,
                          self.activeReadRequest == nil, self.activeWriteTicket == nil else { return }
                    self.reloadWorkspace()
                }
            }
        } else {
            pollTimer?.invalidate()
            pollTimer = nil
            marketFeedInvalidation.clearPending()
            closeReviewWindow()
            closeDecisionAnalysisWindow()
            readGeneration &+= 1
            activeReadRequest?.cancel()
            activeReadRequest = nil
            policyGeneration &+= 1
            policyReadInFlight = false
        }
        updateControlEnablement()
    }

    private func buildView() {
        let title = NSTextField(labelWithString: "전략 자동화 세션")
        let titleFont = TerminalTheme.screenTitle
        title.font = titleFont
        let titleLineHeight = titleFont.ascender - titleFont.descender + titleFont.leading
        title.heightAnchor.constraint(greaterThanOrEqualToConstant: ceil(titleLineHeight + 4)).isActive = true
        title.setAccessibilityLabel("전략 자동화 세션")
        let header = NSStackView(views: [title, NSView(), privacyButton, refreshButton])
        header.orientation = .horizontal
        header.alignment = .centerY
        header.spacing = 10

        statusBanner.font = TerminalTheme.monospacedDigitFont(ofSize: 12, weight: .medium)
        statusBanner.textColor = TerminalTheme.secondaryText
        statusBanner.maximumNumberOfLines = 3
        statusBanner.lineBreakMode = .byWordWrapping
        statusBanner.setAccessibilityLabel("전략 자동화 작업 상태와 복구 안내")

        configureButton(privacyButton, action: #selector(togglePrivacyMask), label: "계좌 금액 개인정보 표시 전환")
        configureButton(refreshButton, action: #selector(handleManualRefresh), label: "전략 자동화 읽기 전용 새로고침")
        configureButton(readbackButton, action: #selector(handleReadbackPendingRequest), label: "결과가 확인되지 않은 요청의 상태 다시 읽기")
        configureButton(nextSessionsButton, action: #selector(handleLoadNextSessions), label: "다음 세션 20개 더 보기")
        configureButton(pauseButton, action: #selector(handlePause), label: "선택한 전략 자동화 세션 일시정지")
        configureButton(resumeButton, action: #selector(handleResume), label: "선택한 전략 자동화 세션 재개")
        configureButton(stopButton, action: #selector(handleStop), label: "선택한 전략 자동화 세션 중지")
        configureButton(cancelOrderButton, action: #selector(handleCancelSelectedOrder), label: "선택한 소유 모의 주문 취소")
        configureButton(nextEventsButton, action: #selector(handleLoadNextEvents), label: "다음 이벤트 더 보기")
        configureButton(nextLedgerButton, action: #selector(handleLoadNextLedger), label: "다음 주문·체결 내역 더 보기")
        configureButton(startButton, action: #selector(handleStartSession), label: "선택한 모드, 동결 전략과 위험 한도로 전략 세션 시작")
        configureButton(reviewButton, action: #selector(handleOpenReview), label: "선택한 전략 세션의 캔들·지표·판단 이유·성과 복기 열기")
        configureButton(decisionAnalysisButton, action: #selector(handleOpenDecisionAnalysis), label: "선택한 전략 세션의 저장 판단 분석 열기")
        decisionAnalysisButton.toolTip = "저장된 판단·조건·이유와 snapshot 비교를 조회합니다. 세션 제어, 원장 변경, 주문 전송을 하지 않습니다."
        configureButton(recordBaselineButton, action: #selector(handleRecordBaseline), label: "선택한 세션 계좌의 일일 시작 기준선 기록 및 검증")
        configureButton(queryBaselineButton, action: #selector(handleQueryBaseline), label: "선택한 세션 계좌의 일일 시작 기준선 상태 조회")

        let createBox = makePanelBox(title: "새 전략 세션 · MOCK 기본값 / LIVE 선택", content: makeStartForm())

        setupTable(sessionTable, label: "전략 자동화 세션 목록", columns: [
            ("mode_state", "모드 / 상태", 150),
            ("admission", "서버 주문 승인", 155),
            ("session_id", "세션 ID", 260),
            ("market_interval", "시장 / 판단 주기", 145),
            ("sequence", "순번", 88),
            ("orders", "미해결 주문", 112),
            ("fills", "체결", 66)
        ])
        let sessionScroll = tableScroll(sessionTable, height: 155)
        let sessionControls = NSStackView(views: [pauseButton, resumeButton, stopButton, reviewButton, decisionAnalysisButton, readbackButton])
        sessionControls.orientation = TerminalTheme.textScale >= 1.5 ? .vertical : .horizontal
        sessionControls.alignment = TerminalTheme.textScale >= 1.5 ? .leading : .centerY
        sessionControls.spacing = 8
        sessionSummaryLabel.font = TerminalTheme.monospacedDigitFont(ofSize: 12, weight: .regular)
        sessionSummaryLabel.textColor = .labelColor
        sessionSummaryLabel.maximumNumberOfLines = 0
        sessionSummaryLabel.lineBreakMode = .byWordWrapping
        sessionSummaryLabel.setAccessibilityLabel("선택한 전략 자동화 세션과 동결 정책 identity")
        let sessionListContent = verticalStack([sessionScroll, nextSessionsButton], spacing: 8)
        let sessionBox = makePanelBox(title: "세션 목록 · 모드, 서버 승인과 요청 순서", content: sessionListContent)

        modeBadge.font = TerminalTheme.systemFont(ofSize: 12, weight: .semibold)
        modeBadge.textColor = TerminalTheme.accent
        modeBadge.maximumNumberOfLines = 0
        modeBadge.lineBreakMode = .byWordWrapping
        modeBadge.setAccessibilityLabel("선택 세션의 실제 MOCK 또는 LIVE 모드, 계좌 identity와 주문 전송 기능")
        let modeBand = makePanelBox(
            title: "선택 세션 · 실제 모드와 서버 응답 기능",
            content: verticalStack([modeBadge], spacing: 4)
        )

        admissionBlockReasonLabel.font = TerminalTheme.monospacedDigitFont(ofSize: 11, weight: .medium)
        admissionBlockReasonLabel.maximumNumberOfLines = 0
        admissionBlockReasonLabel.setAccessibilityLabel("서버가 제공한 주문 승인 차단 사유")
        lifecycleAvailabilityLabel.font = TerminalTheme.systemFont(ofSize: 12, weight: .medium)
        lifecycleAvailabilityLabel.textColor = TerminalTheme.secondaryText
        lifecycleAvailabilityLabel.maximumNumberOfLines = 0
        lifecycleAvailabilityLabel.setAccessibilityLabel("선택 세션 제어 기능의 활성 또는 차단 사유")
        let selectedControls = verticalStack([
            sessionSummaryLabel,
            sessionControls,
            lifecycleAvailabilityLabel,
            admissionBlockReasonLabel
        ], spacing: 8)
        let selectedControlBox = makePanelBox(title: "선택 자동화 · 상태 제어와 identity", content: selectedControls)

        accountBalanceLabel.font = TerminalTheme.table
        accountBalanceLabel.maximumNumberOfLines = 8
        baselineStatusLabel.font = TerminalTheme.table
        baselineStatusLabel.maximumNumberOfLines = 12
        riskUsageLabel.font = TerminalTheme.table
        riskUsageLabel.maximumNumberOfLines = 7
        feedFreshnessLabel.font = TerminalTheme.table
        feedFreshnessLabel.maximumNumberOfLines = 4
        capabilityLabel.font = TerminalTheme.monospacedDigitFont(ofSize: 12, weight: .regular)
        capabilityLabel.maximumNumberOfLines = 5
        mockAssumptionLabel.font = TerminalTheme.systemFont(ofSize: 12)
        mockAssumptionLabel.maximumNumberOfLines = 4
        riskIdentityLabel.font = TerminalTheme.monospacedDigitFont(ofSize: 12, weight: .regular)
        riskIdentityLabel.maximumNumberOfLines = 8
        receivedAtLabel.font = TerminalTheme.monospacedDigitFont(ofSize: 10, weight: .regular)
        receivedAtLabel.textColor = TerminalTheme.secondaryText
        let accountContent = verticalStack([
            riskIdentityLabel, accountBalanceLabel, baselineStatusLabel,
            riskUsageLabel, feedFreshnessLabel, capabilityLabel, mockAssumptionLabel, receivedAtLabel
        ], spacing: 7)
        let accountBox = makePanelBox(title: "동결 정책 · 계좌 · 일일 시작 기준선 · 위험 사용량 · 데이터 신선도", content: accountContent)
        let baselineBox = makePanelBox(title: "일일 기준선 · 출처 증거 · 상태 조회", content: makeBaselineForm())

        setupTable(orderTable, label: "선택한 세션의 소유 주문 원장", columns: [
            ("client_identifier", "주문 식별자", 240),
            ("state", "상태", 150),
            ("side", "매매 구분", 62),
            ("notional", "주문 금액(KRW)", 120),
            ("reserved_fee", "예약 수수료", 110),
            ("locked_quote", "묶인 금액(KRW)", 110),
            ("locked_base", "묶인 수량", 105),
            ("executed", "체결 수량", 126),
            ("paid_fee", "지급 수수료", 100),
            ("trades", "체결 건수", 65),
            ("updated", "갱신 시각(UTC)", 180)
        ])
        let ordersScroll = tableScroll(orderTable, height: 170)
        cancelOrderButton.toolTip = "서버가 취소를 허용한 모의 세션에서 선택한 OPEN(미체결) 또는 PARTIALLY_FILLED(일부 체결) 주문만 취소할 수 있습니다. 일시정지·중지·화면 숨김 때는 주문을 취소하지 않습니다."
        let ordersHeader = NSStackView(views: [label("선택한 모의 주문만 직접 취소할 수 있습니다"), cancelOrderButton])
        ordersHeader.orientation = TerminalTheme.textScale >= 1.5 ? .vertical : .horizontal
        ordersHeader.alignment = TerminalTheme.textScale >= 1.5 ? .leading : .centerY
        ordersHeader.spacing = 12
        let orderBox = makePanelBox(title: "주문 내역 · 확인되지 않은 주문은 상태 재확인 필요", content: verticalStack([ordersHeader, ordersScroll], spacing: 8))

        setupTable(fillTable, label: "선택한 세션의 체결 원장", columns: [
            ("trade_uuid", "거래 ID", 220),
            ("client_identifier", "주문 식별자", 220),
            ("market", "시장", 110),
            ("side", "매매 구분", 62),
            ("price", "가격(KRW)", 120),
            ("volume", "수량", 120),
            ("funds", "거래 금액(KRW)", 120),
            ("observed", "관측 시각(UTC)", 190)
        ])
        let fillsScroll = tableScroll(fillTable, height: 150)
        let fillHeader = NSStackView(views: [label("거래별 고유 ID와 체결 사실"), nextLedgerButton])
        fillHeader.orientation = TerminalTheme.textScale >= 1.5 ? .vertical : .horizontal
        fillHeader.alignment = TerminalTheme.textScale >= 1.5 ? .leading : .centerY
        fillHeader.spacing = 12
        let fillBox = makePanelBox(title: "체결 내역 · 거래소 ID와 관측 시각", content: verticalStack([fillHeader, fillsScroll], spacing: 8))

        setupTable(eventTable, label: "선택한 세션의 저장된 이벤트 내역", columns: [
            ("sequence", "순번", 90),
            ("kind", "종류", 220),
            ("at", "시각(UTC)", 195),
            ("detail", "내용", 560)
        ])
        let eventsScroll = tableScroll(eventTable, height: 180)
        let eventsHeader = NSStackView(views: [label("저장된 순서대로 표시 · 시각과 복구 기록 포함"), nextEventsButton])
        eventsHeader.orientation = TerminalTheme.textScale >= 1.5 ? .vertical : .horizontal
        eventsHeader.alignment = TerminalTheme.textScale >= 1.5 ? .leading : .centerY
        eventsHeader.spacing = 12
        let eventBox = makePanelBox(title: "이벤트 내역 · 원본 시각과 복구 기록", content: verticalStack([eventsHeader, eventsScroll], spacing: 8))

        let lifecycleNote = label("일시정지하거나 중지하면 새 전략 주문만 막습니다. 기존 보유 자산·미체결 주문·예약 금액은 유지하며, 확인되지 않은 주문은 계속 확인합니다. 일시정지·중지·화면 숨김 때 주문을 자동 취소하거나 자산을 매도하지 않습니다.")
        lifecycleNote.font = .systemFont(ofSize: 12, weight: .medium)
        lifecycleNote.textColor = TerminalTheme.secondaryText
        lifecycleNote.maximumNumberOfLines = 0

        let leftColumn = verticalStack([sessionBox, createBox], spacing: 12)
        let rightColumn = verticalStack([
            modeBand, selectedControlBox, accountBox, baselineBox,
            orderBox, fillBox, eventBox, lifecycleNote
        ], spacing: 12)
        for view in leftColumn.arrangedSubviews {
            view.widthAnchor.constraint(equalTo: leftColumn.widthAnchor).isActive = true
        }
        for view in rightColumn.arrangedSubviews {
            view.widthAnchor.constraint(equalTo: rightColumn.widthAnchor).isActive = true
        }

        let columnScrollView: (NSView) -> NSScrollView = { content in
            let scroll = NSScrollView()
            scroll.hasVerticalScroller = true
            scroll.hasHorizontalScroller = false
            scroll.drawsBackground = false
            scroll.translatesAutoresizingMaskIntoConstraints = false
            let document = TerminalFlippedDocumentView()
            document.translatesAutoresizingMaskIntoConstraints = false
            scroll.documentView = document
            content.translatesAutoresizingMaskIntoConstraints = false
            document.addSubview(content)
            NSLayoutConstraint.activate([
                document.widthAnchor.constraint(equalTo: scroll.contentView.widthAnchor),
                content.leadingAnchor.constraint(equalTo: document.leadingAnchor, constant: 10),
                content.trailingAnchor.constraint(equalTo: document.trailingAnchor, constant: -10),
                content.topAnchor.constraint(equalTo: document.topAnchor, constant: 12),
                content.bottomAnchor.constraint(equalTo: document.bottomAnchor, constant: -12)
            ])
            return scroll
        }
        let leadingScroll = columnScrollView(leftColumn)
        let detailScroll = columnScrollView(rightColumn)
        let splitView = NSSplitView()
        splitView.isVertical = true
        splitView.dividerStyle = .thin
        splitView.addArrangedSubview(leadingScroll)
        splitView.addArrangedSubview(detailScroll)
        leadingScroll.widthAnchor.constraint(greaterThanOrEqualToConstant: max(280, 280 * min(TerminalTheme.textScale, 1.15))).isActive = true
        detailScroll.widthAnchor.constraint(greaterThanOrEqualToConstant: max(420, 420 * min(TerminalTheme.textScale, 1.15))).isActive = true
        let splitMinHeight = splitView.heightAnchor.constraint(greaterThanOrEqualToConstant: 240)
        splitMinHeight.priority = .defaultHigh
        splitMinHeight.isActive = true
        splitView.setContentHuggingPriority(.defaultLow, for: .vertical)
        splitView.setContentCompressionResistancePriority(.defaultLow, for: .vertical)
        header.setContentCompressionResistancePriority(.required, for: .vertical)
        realtimeStatusView.setContentCompressionResistancePriority(.required, for: .vertical)
        statusBanner.setContentCompressionResistancePriority(.required, for: .vertical)

        let mainStack = verticalStack([header, realtimeStatusView, statusBanner, splitView], spacing: 10)
        mainStack.translatesAutoresizingMaskIntoConstraints = false
        addSubview(mainStack)
        NSLayoutConstraint.activate([
            mainStack.leadingAnchor.constraint(equalTo: leadingAnchor, constant: 20),
            mainStack.trailingAnchor.constraint(equalTo: trailingAnchor, constant: -20),
            mainStack.topAnchor.constraint(equalTo: topAnchor, constant: 16),
            mainStack.bottomAnchor.constraint(equalTo: bottomAnchor, constant: -16),
            header.widthAnchor.constraint(equalTo: mainStack.widthAnchor),
            realtimeStatusView.widthAnchor.constraint(equalTo: mainStack.widthAnchor),
            statusBanner.widthAnchor.constraint(equalTo: mainStack.widthAnchor),
            splitView.widthAnchor.constraint(equalTo: mainStack.widthAnchor)
        ])
    }

    private func makeBaselineForm() -> NSView {
        baselineDayField.placeholderString = "예: 2026-10-14"
        baselineDayField.font = TerminalTheme.table
        baselineDayField.widthAnchor.constraint(equalToConstant: 125).isActive = true
        baselineDayField.setAccessibilityLabel("기준선 날짜(UTC YYYY-MM-DD)")
        baselineDayField.delegate = self

        baselineSourcePopup.removeAllItems()
        baselineSourcePopup.addItem(withTitle: "세션을 선택하세요")
        baselineSourcePopup.item(at: 0)?.isEnabled = false
        baselineSourcePopup.selectItem(at: 0)
        baselineSourcePopup.isEnabled = false
        baselineSourcePopup.widthAnchor.constraint(equalToConstant: 390).isActive = true
        baselineSourcePopup.setAccessibilityLabel("일일 시작 기준선 출처")

        baselineQuoteFreeField.placeholderString = "예: 1000000"
        baselineQuoteFreeField.alignment = .right
        baselineQuoteFreeField.font = TerminalTheme.table
        baselineQuoteFreeField.widthAnchor.constraint(equalToConstant: 135).isActive = true
        baselineQuoteFreeField.setAccessibilityLabel("KRW 사용 가능 금액")
        baselineQuoteFreeField.delegate = self

        baselineQuoteLockedField.placeholderString = "예: 0"
        baselineQuoteLockedField.alignment = .right
        baselineQuoteLockedField.font = TerminalTheme.table
        baselineQuoteLockedField.widthAnchor.constraint(equalToConstant: 115).isActive = true
        baselineQuoteLockedField.setAccessibilityLabel("KRW 거래소 잠금 금액")
        baselineQuoteLockedField.delegate = self

        baselineBaseCurrencyField.placeholderString = "예: BTC"
        baselineBaseCurrencyField.font = TerminalTheme.table
        baselineBaseCurrencyField.widthAnchor.constraint(equalToConstant: 95).isActive = true
        baselineBaseCurrencyField.setAccessibilityLabel("보유 기초 자산 코드(선택)")
        baselineBaseCurrencyField.delegate = self

        baselineBaseFreeField.placeholderString = "예: 0"
        baselineBaseFreeField.alignment = .right
        baselineBaseFreeField.font = TerminalTheme.table
        baselineBaseFreeField.widthAnchor.constraint(equalToConstant: 120).isActive = true
        baselineBaseFreeField.setAccessibilityLabel("기초 자산 사용 가능 수량")
        baselineBaseFreeField.delegate = self

        baselineBaseLockedField.placeholderString = "예: 0"
        baselineBaseLockedField.alignment = .right
        baselineBaseLockedField.font = TerminalTheme.table
        baselineBaseLockedField.widthAnchor.constraint(equalToConstant: 115).isActive = true
        baselineBaseLockedField.setAccessibilityLabel("기초 자산 잠금 수량")
        baselineBaseLockedField.delegate = self

        baselineValuationBasisPopup.removeAllItems()
        baselineValuationBasisPopup.addItems(withTitles: [
            "기준 시각에 맞는 확정 봉 종가 (FINALIZED_CANDLE_CLOSE)",
            "운영자 입력 단가 — 위험 승인 차단 (OPERATOR_ATTESTED_PRICE)"
        ])
        baselineValuationBasisPopup.selectItem(at: 0)
        baselineValuationBasisPopup.widthAnchor.constraint(equalToConstant: 330).isActive = true
        baselineValuationBasisPopup.setAccessibilityLabel("기초 자산 평가 근거")

        baselineBasePriceField.placeholderString = "예: 100000"
        baselineBasePriceField.alignment = .right
        baselineBasePriceField.font = TerminalTheme.table
        baselineBasePriceField.widthAnchor.constraint(equalToConstant: 130).isActive = true
        baselineBasePriceField.setAccessibilityLabel("기초 자산 기준 단가(KRW)")
        baselineBasePriceField.delegate = self

        baselineNoteField.placeholderString = "예: 선택 날짜의 기준선 출처 대조"
        baselineNoteField.font = TerminalTheme.table
        baselineNoteField.widthAnchor.constraint(equalToConstant: 240).isActive = true
        baselineNoteField.setAccessibilityLabel("기준선 기록 비고")

        baselineSourceNoteLabel.font = .systemFont(ofSize: 12, weight: .medium)
        baselineSourceNoteLabel.textColor = TerminalTheme.secondaryText
        baselineSourceNoteLabel.maximumNumberOfLines = 5

        let stacksVertically = TerminalTheme.textScale >= 1.5
        let row1a = NSStackView(views: [
            labeledControl("기준일(UTC)", baselineDayField),
            labeledControl("기준선 출처", baselineSourcePopup)
        ])
        row1a.orientation = stacksVertically ? .vertical : .horizontal
        row1a.alignment = .leading
        row1a.spacing = 12

        let row1b = NSStackView(views: [
            labeledControl("KRW 사용 가능", baselineQuoteFreeField),
            labeledControl("KRW 잠금", baselineQuoteLockedField)
        ])
        row1b.orientation = stacksVertically ? .vertical : .horizontal
        row1b.alignment = .leading
        row1b.spacing = 12

        let row1 = NSStackView(views: [row1a, row1b])
        row1.orientation = .vertical
        row1.alignment = .leading
        row1.spacing = 10

        let row2a = NSStackView(views: [
            labeledControl("기초 자산(선택)", baselineBaseCurrencyField),
            labeledControl("자산 사용 가능", baselineBaseFreeField),
            labeledControl("자산 잠금", baselineBaseLockedField)
        ])
        row2a.orientation = stacksVertically ? .vertical : .horizontal
        row2a.alignment = .leading
        row2a.spacing = 12

        let row2b = NSStackView(views: [
            labeledControl("자산 평가 근거", baselineValuationBasisPopup),
            labeledControl("기준 단가(KRW)", baselineBasePriceField)
        ])
        row2b.orientation = stacksVertically ? .vertical : .horizontal
        row2b.alignment = .leading
        row2b.spacing = 12

        let row2 = NSStackView(views: [row2a, row2b])
        row2.orientation = .vertical
        row2.alignment = .leading
        row2.spacing = 10

        let baselineButtons = NSStackView(views: [recordBaselineButton, queryBaselineButton])
        baselineButtons.orientation = stacksVertically ? .vertical : .horizontal
        baselineButtons.alignment = stacksVertically ? .leading : .centerY
        baselineButtons.spacing = 8

        let row3 = NSStackView(views: [
            labeledControl("비고(선택)", baselineNoteField),
            baselineButtons
        ])
        row3.orientation = stacksVertically ? .vertical : .horizontal
        row3.alignment = stacksVertically ? .leading : .bottom
        row3.spacing = 12

        baselineQueryResultLabel.font = TerminalTheme.monospacedDigitFont(ofSize: 11, weight: .regular)
        baselineQueryResultLabel.textColor = TerminalTheme.secondaryText
        baselineQueryResultLabel.maximumNumberOfLines = 5

        return verticalStack([baselineSourceNoteLabel, row1, row2, row3, baselineQueryResultLabel], spacing: 8)
    }

    private func makeStartForm() -> NSView {
        startModePopup.removeAllItems()
        startModePopup.addItems(withTitles: ["MOCK", "LIVE"])
        startModePopup.selectItem(at: 0)
        startModePopup.widthAnchor.constraint(equalToConstant: 100).isActive = true
        startModePopup.setAccessibilityLabel("새 전략 세션 모드 · 기본 선택 MOCK")
        startModePopup.target = self
        startModePopup.action = #selector(handleStartModeChanged)
        startButton.title = "MOCK 전략 세션 생성 및 시작"
        startButton.setAccessibilityLabel("선택한 동결 전략과 위험 한도로 MOCK 전략 세션 시작")

        policyPopup.addItem(withTitle: "정책 선택")
        policyPopup.selectItem(at: 0)
        policyPopup.widthAnchor.constraint(equalToConstant: 270).isActive = true
        policyPopup.setAccessibilityLabel("저장된 정책 중 원본 정보를 확인할 전략 선택")
        policyPopup.target = self
        policyPopup.action = #selector(handlePolicySelectionChanged)

        revisionPopup.addItem(withTitle: "동결된 정책 버전 선택")
        revisionPopup.selectItem(at: 0)
        revisionPopup.widthAnchor.constraint(equalToConstant: 280).isActive = true
        revisionPopup.setAccessibilityLabel("원본 정보를 확인한 정책 버전 선택")
        revisionPopup.target = self
        revisionPopup.action = #selector(handleRevisionSelectionChanged)

        marketField.placeholderString = "예: KRW-BTC"
        marketField.font = TerminalTheme.table
        marketField.widthAnchor.constraint(equalToConstant: 145).isActive = true
        marketField.setAccessibilityLabel("새 전략 세션의 시장 코드")
        marketField.delegate = self

        intervalPopup.addItem(withTitle: "판단 주기 선택")
        intervalPopup.addItems(withTitles: Self.decisionIntervals)
        intervalPopup.selectItem(at: 0)
        intervalPopup.widthAnchor.constraint(equalToConstant: 105).isActive = true
        intervalPopup.setAccessibilityLabel("새 전략 세션의 판단 주기")

        let modeRow = NSStackView(views: [
            labeledControl("세션 모드", startModePopup),
            label("MOCK가 기본 선택입니다. LIVE Start/Resume에는 Settings → 계좌 전략 주문 허용이 필요할 수 있습니다. 이 세션 화면은 계좌 권한이나 별도 주문 전송 게이트를 바꾸지 않습니다.")
        ])
        modeRow.orientation = .vertical
        modeRow.alignment = .leading
        modeRow.spacing = 12

        let marketIntervalRow = NSStackView(views: [
            labeledControl("시장", marketField),
            labeledControl("판단 주기", intervalPopup)
        ])
        marketIntervalRow.orientation = TerminalTheme.textScale >= 1.5 ? .vertical : .horizontal
        marketIntervalRow.alignment = TerminalTheme.textScale >= 1.5 ? .leading : .top
        marketIntervalRow.spacing = 12

        let identityRow = NSStackView(views: [
            labeledControl("동결 전략", policyPopup),
            labeledControl("정책 버전", revisionPopup),
            marketIntervalRow
        ])
        identityRow.orientation = .vertical
        identityRow.alignment = .leading
        identityRow.spacing = 10

        let samplePlaceholders: [(String, String, String)] = [
            ("max_order_notional_krw", "주문당 최대 금액(KRW)", "예: 100000"),
            ("max_daily_notional_krw", "하루 최대 주문액(KRW)", "예: 1000000"),
            ("max_asset_exposure_krw", "자산 최대 보유액(KRW)", "예: 500000"),
            ("max_daily_loss_krw", "하루 최대 손실(KRW)", "예: 50000"),
            ("max_open_orders", "동시 미체결 주문 수", "예: 2"),
            ("max_orders_per_interval", "주기당 주문 수", "예: 1"),
            ("max_balance_age_seconds", "잔액 정보 유효 시간(초)", "예: 30"),
            ("max_feed_age_seconds", "시세 정보 유효 시간(초)", "예: 90")
        ]
        for (key, title, placeholder) in samplePlaceholders {
            let field = NSTextField(string: "")
            field.placeholderString = placeholder
            field.alignment = .right
            field.font = TerminalTheme.table
            field.widthAnchor.constraint(equalToConstant: 148).isActive = true
            field.setAccessibilityLabel(title)
            field.delegate = self
            riskFields[key] = field
        }
        let riskRows: [[(String, String, String)]] = [
            Array(samplePlaceholders[0...1]), Array(samplePlaceholders[2...3]),
            Array(samplePlaceholders[4...5]), Array(samplePlaceholders[6...7])
        ]
        var rows: [NSView] = []
        for pair in riskRows {
            let row = NSStackView(views: pair.map { key, title, _ in
                labeledControl(title, riskFields[key] ?? NSTextField(string: ""))
            })
            row.orientation = TerminalTheme.textScale >= 1.5 ? .vertical : .horizontal
            row.alignment = TerminalTheme.textScale >= 1.5 ? .leading : .top
            row.spacing = 12
            rows.append(row)
        }

        activationLossLimitField.placeholderString = "선택 입력 · 기본값 없음"
        activationLossLimitField.alignment = .right
        activationLossLimitField.font = TerminalTheme.table
        activationLossLimitField.widthAnchor.constraint(equalToConstant: 190).isActive = true
        activationLossLimitField.setAccessibilityLabel("LIVE 계좌 활성화 기준 손실 한도 KRW · 선택 입력")
        activationLossLimitField.setAccessibilityHelp("LIVE에서만 사용합니다. MOCK를 선택하면 전송하지 않습니다.")
        activationLossLimitField.delegate = self
        activationLossRow.orientation = .vertical
        activationLossRow.alignment = .leading
        activationLossRow.spacing = 12
        activationLossRow.addArrangedSubview(labeledControl("LIVE 활성화 손실 한도(KRW · 선택)", activationLossLimitField))
        activationLossRow.addArrangedSubview(label("LIVE 전용 · 비워 두면 activation_loss_limit_krw를 전송하지 않습니다."))
        activationLossLimitField.isEnabled = false

        let riskTitle = label("위험 한도 · 금액은 0보다 큰 정확한 숫자로, 주문 수와 시간은 정수로 입력하세요. 예시와 빈 칸을 기본값으로 바꾸지 않습니다.")
        riskTitle.font = .systemFont(ofSize: 12, weight: .medium)
        riskTitle.textColor = TerminalTheme.secondaryText

        let startRow = NSStackView(views: [startButton])
        startRow.orientation = .horizontal
        startRow.alignment = .centerY
        startRow.spacing = 8

        riskLimitsDisclosureButton.bezelStyle = .rounded
        riskLimitsDisclosureButton.title = "필수 위험 한도 접기"
        riskLimitsDisclosureButton.setAccessibilityLabel("필수 위험 한도 입력 표시 전환")
        riskLimitsDisclosureButton.target = self
        riskLimitsDisclosureButton.action = #selector(toggleRiskLimitsDisclosure)
        let riskLimitsStack = verticalStack(rows + [activationLossRow], spacing: 8)
        self.riskLimitsStack = riskLimitsStack

        startReadinessLabel.font = TerminalTheme.systemFont(ofSize: 11, weight: .medium)
        startReadinessLabel.textColor = TerminalTheme.secondaryText
        startReadinessLabel.maximumNumberOfLines = 0
        startReadinessLabel.setAccessibilityLabel("새 전략 세션 시작 가능 여부와 입력 차단 사유")

        return verticalStack([
            modeRow, identityRow, riskTitle, riskLimitsDisclosureButton, riskLimitsStack,
            startRow, startReadinessLabel
        ], spacing: 8)
    }

    @objc private func toggleRiskLimitsDisclosure() {
        guard let riskLimitsStack else { return }
        riskLimitsStack.isHidden.toggle()
        riskLimitsDisclosureButton.title = riskLimitsStack.isHidden
            ? "필수 위험 한도 펼치기" : "필수 위험 한도 접기"
        riskLimitsDisclosureButton.setAccessibilityValue(riskLimitsStack.isHidden ? "접힘" : "펼침")
    }

    private func populatePolicies() {
        let previousPolicyID = currentPolicy?.policyId
        policyPopup.removeAllItems()
        policyPopup.addItem(withTitle: "정책 선택")
        for policy in policies {
            policyPopup.addItem(withTitle: "\(policy.name) · \(policy.family) · 기준 버전 \(String(policy.headRevisionId.prefix(12)))")
            policyPopup.lastItem?.toolTip = "policy_id: \(policy.policyId)\nhead revision_id: \(policy.headRevisionId)\nrevision count: \(policy.revisionCount)"
        }
        if let previousPolicyID,
           let index = policies.firstIndex(where: { $0.policyId == previousPolicyID }) {
            policyPopup.selectItem(at: index + 1)
        } else {
            policyPopup.selectItem(at: 0)
            currentPolicy = nil
            currentPolicyDetail = nil
            selectedPolicyReference = nil
            revisionItems = []
            revisionPopup.removeAllItems()
            revisionPopup.addItem(withTitle: "동결된 정책 버전 선택")
            revisionPopup.selectItem(at: 0)
        }
        updateControlEnablement()
    }

    private func setupTable(_ table: NSTableView, label accessibilityLabel: String, columns: [(String, String, CGFloat)]) {
        table.delegate = self
        table.dataSource = self
        table.headerView = NSTableHeaderView()
        table.rowHeight = 30
        table.allowsMultipleSelection = false
        table.setAccessibilityLabel(accessibilityLabel)
        for (identifier, title, width) in columns {
            let column = NSTableColumn(identifier: NSUserInterfaceItemIdentifier(identifier))
            column.title = title
            column.width = width
            table.addTableColumn(column)
        }
        TerminalTheme.prepareTable(table)
    }

    private func tableScroll(_ table: NSTableView, height: CGFloat) -> NSScrollView {
        let scroll = NSScrollView()
        scroll.documentView = table
        scroll.hasVerticalScroller = true
        scroll.hasHorizontalScroller = true
        scroll.drawsBackground = true
        scroll.backgroundColor = TerminalTheme.chart
        scroll.heightAnchor.constraint(equalToConstant: max(height, height * min(TerminalTheme.textScale, 1.5))).isActive = true
        return scroll
    }

    private func makePanelBox(title: String, content: NSView) -> NSBox {
        let box = TerminalTheme.makePanelBox()
        let heading = label(title)
        heading.font = TerminalTheme.sectionHeading
        let stack = verticalStack([heading, content], spacing: 8)
        stack.translatesAutoresizingMaskIntoConstraints = false
        box.addSubview(stack)
        NSLayoutConstraint.activate([
            stack.leadingAnchor.constraint(equalTo: box.leadingAnchor, constant: 12),
            stack.trailingAnchor.constraint(equalTo: box.trailingAnchor, constant: -12),
            stack.topAnchor.constraint(equalTo: box.topAnchor, constant: 10),
            stack.bottomAnchor.constraint(equalTo: box.bottomAnchor, constant: -10),
            content.widthAnchor.constraint(equalTo: stack.widthAnchor)
        ])
        if let contentStack = content as? NSStackView {
            for arrangedSubview in contentStack.arrangedSubviews {
                if let scrollView = arrangedSubview as? NSScrollView,
                   scrollView.documentView is NSTableView {
                    scrollView.widthAnchor.constraint(equalTo: contentStack.widthAnchor).isActive = true
                } else if arrangedSubview is NSTextField {
                    arrangedSubview.widthAnchor.constraint(equalTo: contentStack.widthAnchor).isActive = true
                }
            }
        }
        return box
    }

    private func labeledControl(_ title: String, _ control: NSView) -> NSStackView {
        let titleLabel = label(title)
        titleLabel.font = .systemFont(ofSize: 11, weight: .medium)
        titleLabel.textColor = TerminalTheme.secondaryText
        let stack = verticalStack([titleLabel, control], spacing: 4)
        stack.alignment = .leading
        return stack
    }

    private func verticalStack(_ views: [NSView], spacing: CGFloat) -> NSStackView {
        let stack = NSStackView(views: views)
        stack.orientation = .vertical
        stack.alignment = .leading
        stack.spacing = spacing
        stack.translatesAutoresizingMaskIntoConstraints = false
        return stack
    }

    private func label(_ text: String) -> NSTextField {
        NSTextField(wrappingLabelWithString: text)
    }

    private func configureButton(_ button: NSButton, action: Selector, label: String) {
        button.bezelStyle = .rounded
        button.target = self
        button.action = action
        button.setAccessibilityLabel(label)
    }

    private func reloadWorkspace() {
        guard isVisibleWorkspace, activeWriteTicket == nil else { return }
        marketFeedInvalidation.clearPending()
        readGeneration &+= 1
        activeReadRequest?.cancel()
        activeReadRequest = nil
        let ticket = readGeneration
        if let pendingRequestID {
            pendingReadbackStatus = "요청 결과 확인 중 · request_id: \(pendingRequestID) · 중복 전송 없음"
            loadRequestReadback(pendingRequestID, ticket: ticket)
        } else {
            loadSessionPage(offset: 0, reset: true, ticket: ticket)
        }
    }

    private func drainPendingMarketFeedInvalidation() {
        guard marketFeedInvalidation.takeIfReady(
            isVisible: isVisibleWorkspace,
            readInFlight: activeReadRequest != nil,
            writeInFlight: activeWriteTicket != nil
        ) else { return }
        reloadWorkspace()
    }

    private func loadSessionPage(offset: UInt32, reset: Bool, ticket: UInt64) {
        guard isCurrentRead(ticket) else { return }
        beginRead(ticket: ticket, operation: { completion in
            service.listSessions(offset: offset, completion: completion)
        }) { [weak self] (result: Result<AutomationSessionPage, Error>) in
            guard let self else { return }
            switch result {
            case .failure(let error):
                self.sessionListReadFailure = error.localizedDescription
                self.selectedProjectionReadAfterListFailure = false
                self.recordReadFailure(error)
            case .success(let page):
                let receivedAt = Date()
                self.sessionListReadFailure = nil
                self.selectedProjectionReadAfterListFailure = false
                if reset { self.sessions = page.items }
                else {
                    let existing = Set(self.sessions.map(\.session_id))
                    self.sessions.append(contentsOf: page.items.filter { !existing.contains($0.session_id) })
                }
                self.sessionNextOffset = page.next_offset
                self.lastSuccessfulReadAt = receivedAt
                self.lastReadFailure = nil
                self.updateStatusAfterSuccessfulRefresh()
                for session in page.items { self.cacheProjection(from: session, receivedAt: receivedAt) }
                if let selectedSessionID = self.selectedSessionID,
                   !self.sessions.contains(where: { $0.session_id == selectedSessionID }) {
                    self.selectedSessionID = nil
                }
                if self.selectedSessionID == nil { self.selectedSessionID = self.sessions.first?.session_id }
                self.sessionTable.reloadData()
                self.selectSessionRow()
                self.updateControlEnablement()
                if let sessionID = self.selectedSessionID {
                    self.loadSelectedSession(sessionID, ticket: ticket)
                } else {
                    self.selectedSession = nil
                    self.events = []
                    self.ledger = nil
                    self.renderWorkspace()
                }
            }
        }
    }

    private func loadSelectedSession(_ sessionID: String, ticket: UInt64) {
        guard isCurrentRead(ticket) else { return }
        if selectedSessionID == sessionID {
            selectedProjectionReadAfterListFailure = false
            selectedProjectionReadSessionID = sessionID
            selectedProjectionReadStatus = "선택 세션 조회 중 · 최신 계좌·위험 투영은 아직 확인되지 않았습니다."
            sessionTable.reloadData()
        }
        beginRead(ticket: ticket, operation: { completion in
            service.getSession(sessionID: sessionID, completion: completion)
        }) { [weak self] (result: Result<AutomationSessionView, Error>) in
            guard let self else { return }
            switch result {
            case .failure(let error):
                if self.selectedSessionID == sessionID {
                    self.selectedProjectionReadAfterListFailure = false
                    self.selectedProjectionReadSessionID = sessionID
                    self.selectedProjectionReadStatus = "선택 세션 조회 실패 · \(error.localizedDescription)"
                }
                self.recordReadFailure(error)
            case .success(let session):
                self.selectedSession = session
                self.projectionMissingForSelectedSession = session.account_risk == nil
                self.cacheProjection(from: session, receivedAt: Date())
                if self.selectedSessionID == sessionID {
                    self.selectedProjectionReadAfterListFailure = true
                    self.selectedProjectionReadSessionID = sessionID
                    self.selectedProjectionReadStatus = nil
                }
                self.lastSuccessfulReadAt = Date()
                self.lastReadFailure = nil
                self.renderWorkspace()
                self.updateControlEnablement()
                self.loadEvents(sessionID: sessionID, afterSequence: nil, append: false, ticket: ticket) { [weak self] in
                    self?.loadLedger(session: session, afterSequence: nil, append: false, ticket: ticket)
                }
            }
        }
    }

    private func loadEvents(
        sessionID: String,
        afterSequence: UInt64?,
        append: Bool,
        ticket: UInt64,
        then: (() -> Void)? = nil
    ) {
        guard isCurrentRead(ticket) else { return }
        beginRead(ticket: ticket, operation: { completion in
            service.listEvents(sessionID: sessionID, afterSequence: afterSequence, completion: completion)
        }) { [weak self] (result: Result<AutomationEventPage, Error>) in
            guard let self else { return }
            switch result {
            case .failure(let error): self.recordReadFailure(error)
            case .success(let page):
                if append {
                    let existing = Set(self.events.map(\.sequence))
                    self.events.append(contentsOf: page.items.filter { !existing.contains($0.sequence) })
                } else {
                    self.events = page.items
                }
                self.eventNextSequence = page.next_sequence
                self.lastSuccessfulReadAt = Date()
                self.eventTable.reloadData()
                self.updateControlEnablement()
            }
            then?()
        }
    }

    private func loadLedger(session: AutomationSessionView, afterSequence: UInt64?, append: Bool, ticket: UInt64) {
        guard isCurrentRead(ticket) else { return }
        beginRead(ticket: ticket, operation: { completion in
            service.listLedger(
                sessionID: session.session_id,
                market: session.market,
                afterSequence: afterSequence,
                completion: completion
            )
        }) { [weak self] (result: Result<AutomationLedgerPage, Error>) in
            guard let self else { return }
            switch result {
            case .failure(let error): self.recordReadFailure(error)
            case .success(let page):
                if append, let current = self.ledger {
                    let orderIDs = Set(current.orders.map(\.client_identifier))
                    let fillIDs = Set(current.fills.map(\.trade_uuid))
                    self.ledger = AutomationLedgerPage(
                        orders: current.orders + page.orders.filter { !orderIDs.contains($0.client_identifier) },
                        fills: current.fills + page.fills.filter { !fillIDs.contains($0.trade_uuid) },
                        next_sequence: page.next_sequence
                    )
                } else {
                    self.ledger = page
                }
                self.ledgerNextSequence = page.next_sequence
                self.lastSuccessfulReadAt = Date()
                self.lastReadFailure = nil
                self.updateControlEnablement()
            }
            self.renderWorkspace()
        }
    }

    private func loadRequestReadback(_ requestID: String, ticket: UInt64) {
        guard isCurrentRead(ticket) else { return }
        beginRead(ticket: ticket, operation: { completion in
            service.readRequest(requestID: requestID, completion: completion)
        }) { [weak self] (result: Result<AutomationSessionRequestReadback, Error>) in
            guard let self else { return }
            switch result {
            case .failure(let error):
                self.pendingReadbackStatus = "요청 결과를 확인하지 못했습니다 · request_id: \(requestID) · 중복 전송 없음 · \(error.localizedDescription)"
                self.recordReadFailure(error)
            case .success(let readback):
                self.lastSuccessfulReadAt = Date()
                self.lastReadFailure = nil
                switch readback.state {
                case .applied:
                    guard let response = readback.response else {
                        self.pendingReadbackStatus = "서버가 APPLIED(처리 완료)로 응답했지만 저장된 결과가 없습니다 · request_id: \(requestID) · 새 요청을 막은 상태입니다."
                        self.renderWorkspace()
                        return
                    }
                    guard let pendingWrite = self.pendingWrite else {
                        self.pendingReadbackStatus = "APPLIED(처리 완료) 응답을 원래 요청과 연결할 수 없습니다 · request_id: \(requestID) · 새 요청을 막은 상태입니다."
                        self.renderWorkspace()
                        return
                    }
                    do {
                        try pendingWrite.validate(response)
                    } catch {
                        self.pendingReadbackStatus = "APPLIED(처리 완료) 응답의 요청 정보가 일치하지 않습니다 · request_id: \(requestID) · 새 요청을 막았습니다. \(error.localizedDescription)"
                        self.renderWorkspace()
                        return
                    }
                    self.clearPendingRequest(requestID)
                    self.applyCommandResponse(response, source: "저장된 요청 결과 확인")
                    self.reloadWorkspace()
                case .rejected:
                    if let response = readback.response, let pendingWrite = self.pendingWrite {
                        do {
                            try pendingWrite.validate(response)
                        } catch {
                            self.pendingReadbackStatus = "REJECTED(거부) 응답의 요청 정보가 일치하지 않습니다 · request_id: \(requestID) · 새 요청을 막았습니다. \(error.localizedDescription)"
                            self.renderWorkspace()
                            return
                        }
                    }
                    self.clearPendingRequest(requestID)
                    if let response = readback.response {
                        self.applyCommandResponse(response, source: "저장된 거부 결과 확인")
                    } else {
                        self.statusBanner.stringValue = "서버가 요청을 REJECTED(거부)로 확정했습니다 · request_id: \(requestID) · 사유: \(readback.reason ?? "사유 미제공") · 새 요청은 보내지 않았습니다."
                        self.statusBanner.textColor = TerminalTheme.negative
                    }
                    self.loadSessionPage(offset: 0, reset: true, ticket: ticket)
                case .pending:
                    if let response = readback.response {
                        guard let pendingWrite = self.pendingWrite else {
                            self.pendingReadbackStatus = "PENDING(처리 중) 응답을 원래 요청과 연결할 수 없습니다 · request_id: \(requestID) · 새 요청을 막은 상태입니다."
                            self.renderWorkspace()
                            return
                        }
                        do {
                            try pendingWrite.validate(response)
                        } catch {
                            self.pendingReadbackStatus = "PENDING(처리 중) 응답의 요청 정보가 일치하지 않습니다 · request_id: \(requestID) · 새 요청을 막았습니다. \(error.localizedDescription)"
                            self.renderWorkspace()
                            return
                        }
                        self.applyCommandResponse(response, source: "처리 중인 요청 결과 확인")
                    }
                    self.pendingReadbackStatus = "서버가 PENDING(처리 중) 상태를 반환했습니다 · request_id: \(requestID) · 사유: \(readback.reason ?? "요청 처리 중") · 중복 전송 없음"
                    self.loadSessionPage(offset: 0, reset: true, ticket: ticket)
                case .notRecorded:
                    self.pendingReadbackStatus = "서버에서 NOT_RECORDED(기록 없음) 상태를 반환했습니다 · request_id: \(requestID) · 결과는 미확인입니다. 새 요청을 막고 있으며 중복 전송하지 않았습니다. 사유: \(readback.reason ?? "결과 미확인")"
                    self.loadSessionPage(offset: 0, reset: true, ticket: ticket)
                }
            }
            self.renderWorkspace()
        }
    }

    private func beginRead<Response>(
        ticket: UInt64,
        operation: (@escaping (Result<Response, Error>) -> Void) -> NativeMCPRequest,
        completion: @escaping (Result<Response, Error>) -> Void
    ) {
        guard isCurrentRead(ticket), activeReadRequest == nil else { return }
        let request = operation { [weak self] result in
            guard let self, self.isCurrentRead(ticket) else { return }
            self.activeReadRequest = nil
            completion(result)
            self.drainPendingMarketFeedInvalidation()
        }
        if request.isPending { activeReadRequest = request }
    }

    private func isCurrentRead(_ ticket: UInt64) -> Bool {
        isVisibleWorkspace && ticket == readGeneration
    }

    private func cacheProjection(from session: AutomationSessionView, receivedAt: Date) {
        guard let accountRisk = session.account_risk else { return }
        accountProjectionBySession[session.session_id] = accountRisk
        accountReceivedAtBySession[session.session_id] = receivedAt
        if accountProjectionBySession.count > sessionLimit {
            let retained = Set(sessions.map(\.session_id))
            accountProjectionBySession = accountProjectionBySession.filter { retained.contains($0.key) }
            accountReceivedAtBySession = accountReceivedAtBySession.filter { retained.contains($0.key) }
        }
    }

    private func recordReadFailure(_ error: Error) {
        lastReadFailure = error.localizedDescription
        statusBanner.stringValue = "읽기 실패: \(error.localizedDescription) · 마지막 성공값은 최신 확인으로 표시하지 않습니다."
        statusBanner.textColor = TerminalTheme.negative
        renderWorkspace()
    }

    private func updateStatusAfterSuccessfulRefresh() {
        guard pendingMetadataError == nil, pendingRequestID == nil,
              activeWriteTicket == nil else { return }
        if statusBanner.stringValue == "전략 자동화: 조회 전" {
            statusBanner.stringValue = "세션 목록 조회 완료 · 선택한 세션의 최신 내역을 확인하고 있습니다."
        } else if statusBanner.stringValue.hasPrefix("읽기 실패:") {
            statusBanner.stringValue = "세션 목록 연결이 복구되었습니다 · 선택한 세션의 최신 내역을 확인하고 있습니다."
        } else {
            return
        }
        statusBanner.textColor = TerminalTheme.secondaryText
    }

    private func clearPendingRequest(_ requestID: String) {
        guard pendingRequestID == requestID else { return }
        UserDefaults.standard.removeObject(forKey: Self.pendingWriteDefaultsKey)
        UserDefaults.standard.removeObject(forKey: Self.legacyPendingRequestDefaultsKey)
        pendingRequestID = nil
        pendingWrite = nil
        pendingMetadataError = nil
        pendingReadbackStatus = nil
    }

    private func beginWrite(
        pendingWrite: AutomationPendingWrite,
        actionName: String,
        operation: (String, @escaping (Result<AutomationSessionCommandResponse, Error>) -> Void) throws -> NativeMCPRequest
    ) {
        guard pendingRequestID == nil, pendingMetadataError == nil,
              activeWriteTicket == nil, isVisibleWorkspace else { return }
        let requestID = pendingWrite.request_id
        guard let metadata = try? JSONEncoder().encode(pendingWrite) else {
            statusBanner.stringValue = "요청 정보를 안전하게 저장하지 못해 전송하지 않았습니다."
            statusBanner.textColor = TerminalTheme.negative
            return
        }
        UserDefaults.standard.set(metadata, forKey: Self.pendingWriteDefaultsKey)
        UserDefaults.standard.removeObject(forKey: Self.legacyPendingRequestDefaultsKey)
        self.pendingWrite = pendingWrite
        pendingRequestID = requestID
        pendingReadbackStatus = "\(actionName) 처리 중 · request_id: \(requestID) · 화면을 닫거나 숨겨도 변경 요청은 취소하거나 다시 보내지 않습니다."
        let ticket = UUID()
        activeWriteTicket = ticket
        statusBanner.stringValue = "\(actionName) 요청 전송 중 · request_id: \(requestID)"
        statusBanner.textColor = TerminalTheme.secondaryText
        updateControlEnablement()

        do {
            let request = try operation(requestID) { [weak self] result in
                guard let self, self.activeWriteTicket == ticket else { return }
                self.activeWriteTicket = nil
                self.activeWriteRequest = nil
                defer { self.drainPendingMarketFeedInvalidation() }
                switch result {
                case .success(let response):
                    do {
                        try pendingWrite.validate(response)
                    } catch {
                        self.pendingReadbackStatus = "응답의 요청 정보가 일치하지 않습니다 · request_id: \(requestID) · 새 요청을 막고 저장된 결과를 다시 확인합니다. \(error.localizedDescription)"
                        self.statusBanner.textColor = TerminalTheme.negative
                        if self.isVisibleWorkspace { self.reloadWorkspace() }
                        return
                    }
                    if response.receipt.outcome == .pending {
                        self.pendingReadbackStatus = "서버가 PENDING(처리 중) 상태를 반환했습니다 · request_id: \(requestID) · 사유: \(response.receipt.reason ?? "주문 상태를 계속 확인 중") · 새 요청을 막은 상태입니다."
                        self.applyCommandResponse(response, source: "처리 중인 요청 결과")
                    } else {
                        self.clearPendingRequest(requestID)
                        self.applyCommandResponse(response, source: "요청 결과")
                    }
                    if self.isVisibleWorkspace { self.reloadWorkspace() }
                case .failure(let error):
                    self.statusBanner.stringValue = "요청 결과가 확인되지 않았습니다 · request_id: \(requestID) · 다시 보내지 않고 저장된 결과를 조회합니다. (\(error.localizedDescription))"
                    self.statusBanner.textColor = TerminalTheme.negative
                    if self.isVisibleWorkspace { self.reloadWorkspace() }
                }
                self.updateControlEnablement()
            }
            if activeWriteTicket == ticket { activeWriteRequest = request }
        } catch {
            activeWriteTicket = nil
            clearPendingRequest(requestID)
            statusBanner.stringValue = "요청을 보내기 전에 오류가 확인되어 전송하지 않았습니다: \(error.localizedDescription)"
            statusBanner.textColor = TerminalTheme.negative
            updateControlEnablement()
            drainPendingMarketFeedInvalidation()
        }
    }

    private func applyCommandResponse(_ response: AutomationSessionCommandResponse, source: String) {
        selectedSession = response.session
        selectedSessionID = response.session.session_id
        projectionMissingForSelectedSession = response.session.account_risk == nil
        selectedProjectionReadAfterListFailure = true
        selectedProjectionReadSessionID = response.session.session_id
        selectedProjectionReadStatus = nil
        cacheProjection(from: response.session, receivedAt: Date())
        if let baseline = response.baseline {
            queriedBaselineStatus = AutomationAccountDayBaselineStatus(
                session_id: response.session.session_id,
                account_identity: response.session.account_identity,
                mode: response.session.mode,
                day_utc: baseline.day_utc,
                admission_blocked: !baseline.sufficient_for_risk_admission
                    || !(response.session.account_risk?.admission_block_reasons.isEmpty ?? true),
                block_reasons: response.session.account_risk?.admission_block_reasons ?? baseline.block_reasons,
                baseline: baseline
            )
        }
        if let order = response.order, let currentLedger = ledger {
            var orders = currentLedger.orders
            if let index = orders.firstIndex(where: { $0.client_identifier == order.client_identifier }) {
                orders[index] = order
                ledger = AutomationLedgerPage(orders: orders, fills: currentLedger.fills, next_sequence: currentLedger.next_sequence)
            }
        }
        statusBanner.stringValue = "\(source) · 작업 \(actionLabel(response.receipt.action)) (\(response.receipt.action.rawValue)) · 결과 \(outcomeLabel(response.receipt.outcome)) (\(response.receipt.outcome.rawValue)) · 순번 \(response.receipt.sequence) · request_id: \(response.receipt.request_id)\(response.receipt.reason.map { " · 사유: \($0)" } ?? "")"
        statusBanner.textColor = response.receipt.outcome == .denied || response.receipt.outcome == .pending
            ? TerminalTheme.negative : TerminalTheme.secondaryText
        renderWorkspace()
        updateControlEnablement()
    }

    private func selectedPolicy() -> RealPolicy? {
        let index = policyPopup.indexOfSelectedItem - 1
        return policies.indices.contains(index) ? policies[index] : nil
    }

    private var selectedStartMode: AutomationMode {
        startModePopup.indexOfSelectedItem == 1 ? .live : .mock
    }

    @objc private func handleStartModeChanged() {
        let mode = selectedStartMode
        activationLossLimitField.isEnabled = mode == .live
        startButton.title = mode == .live ? "LIVE 전략 시작 검토…" : "MOCK 전략 세션 생성 및 시작"
        startButton.setAccessibilityLabel(mode == .live
            ? "선택한 동결 전략과 위험 한도를 검토한 뒤 LIVE 전략 시작 요청"
            : "선택한 동결 전략과 위험 한도로 MOCK 전략 세션 시작")
        updateControlEnablement()
        renderWorkspace()
    }

    private func selectedRiskLimits(for mode: AutomationMode) throws -> AutomationRiskLimits {
        func text(_ key: String) throws -> String {
            guard let field = riskFields[key] else {
                throw AutomationSessionServiceError.invalidInput(key)
            }
            let value = field.stringValue.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !value.isEmpty else { throw AutomationSessionServiceError.invalidInput("\(riskFieldLabel(for: key)): 입력하세요") }
            return value
        }
        func integer(_ key: String) throws -> UInt32 {
            guard let value = UInt32(try text(key)) else {
                throw AutomationSessionServiceError.invalidInput("\(riskFieldLabel(for: key)): 0보다 큰 정수로 입력하세요")
            }
            return value
        }
        let activationLossText = activationLossLimitField.stringValue.trimmingCharacters(in: .whitespacesAndNewlines)
        let activationLoss = mode == .live && !activationLossText.isEmpty ? activationLossText : nil
        let limits = AutomationRiskLimits(
            max_order_notional_krw: try text("max_order_notional_krw"),
            max_daily_notional_krw: try text("max_daily_notional_krw"),
            max_asset_exposure_krw: try text("max_asset_exposure_krw"),
            max_daily_loss_krw: try text("max_daily_loss_krw"),
            max_open_orders: try integer("max_open_orders"),
            max_orders_per_interval: try integer("max_orders_per_interval"),
            max_balance_age_seconds: try integer("max_balance_age_seconds"),
            max_feed_age_seconds: try integer("max_feed_age_seconds"),
            activation_loss_limit_krw: activationLoss
        )
        try limits.validate(for: mode)
        return limits
    }

    private func updateControlEnablement() {
        let hasPendingWrite = pendingRequestID != nil || activeWriteTicket != nil || pendingMetadataError != nil
        let riskValidationError: String?
        do {
            _ = try selectedRiskLimits(for: selectedStartMode)
            riskValidationError = nil
        } catch {
            riskValidationError = userFacingError(error)
        }
        let riskValid = riskValidationError == nil
        reviewButton.isEnabled = isVisibleWorkspace && selectedSession != nil
        decisionAnalysisButton.isEnabled = isVisibleWorkspace && selectedSession != nil
        let market = marketField.stringValue.trimmingCharacters(in: .whitespacesAndNewlines)
        let interval = intervalPopup.selectedItem?.title
        var startBlockReasons: [String] = []
        if pendingMetadataError != nil {
            startBlockReasons.append("이전 요청 작업 정보가 확인될 때까지 새 변경 요청을 차단합니다.")
        } else if pendingRequestID != nil || activeWriteTicket != nil {
            startBlockReasons.append("이전 요청 결과를 읽기 전용으로 확인하는 중입니다.")
        }
        if policyReadInFlight {
            startBlockReasons.append("정책 원본과 동결 버전 확인이 끝나지 않았습니다.")
        }
        if selectedPolicyReference == nil {
            startBlockReasons.append("확인된 정책과 동결 버전을 선택하세요.")
        }
        if market.isEmpty {
            startBlockReasons.append("시장 코드를 입력하세요.")
        }
        if interval.map({ Self.decisionIntervals.contains($0) }) != true {
            startBlockReasons.append("지원되는 판단 주기를 선택하세요.")
        }
        if let riskValidationError {
            startBlockReasons.append("위험 한도 입력 검증 실패 · \(riskValidationError). 예시와 빈 값을 기본값으로 바꾸지 않습니다.")
        }
        if startBlockReasons.isEmpty {
            startReadinessLabel.stringValue = "모든 필수 입력을 확인했습니다. \(modeLabel(selectedStartMode)) 모드가 표시되며 서버 admission은 요청 응답에서 확인합니다."
            startReadinessLabel.textColor = TerminalTheme.secondaryText
        } else {
            startReadinessLabel.stringValue = "세션 시작 차단 · \(startBlockReasons.joined(separator: " · "))"
            startReadinessLabel.textColor = TerminalTheme.negative
        }
        startReadinessLabel.toolTip = startReadinessLabel.stringValue
        startReadinessLabel.setAccessibilityValue(startReadinessLabel.stringValue)
        startButton.isEnabled = isVisibleWorkspace && !hasPendingWrite && !policyReadInFlight
            && selectedPolicyReference != nil && !market.isEmpty
            && interval.map { Self.decisionIntervals.contains($0) } == true && riskValid
        readbackButton.isHidden = pendingRequestID == nil
        readbackButton.isEnabled = isVisibleWorkspace && pendingRequestID != nil && activeWriteTicket == nil
        nextSessionsButton.isEnabled = isVisibleWorkspace && sessionNextOffset != nil && activeReadRequest == nil

        guard let session = selectedSession else {
            pauseButton.isEnabled = false
            resumeButton.isEnabled = false
            stopButton.isEnabled = false
            cancelOrderButton.isEnabled = false
            nextEventsButton.isEnabled = false
            nextLedgerButton.isEnabled = false
            recordBaselineButton.isEnabled = false
            queryBaselineButton.isEnabled = false
            baselineSourcePopup.isEnabled = false
            lifecycleAvailabilityLabel.stringValue = "세션을 선택하면 실제 계좌 identity, 서버가 허용한 기능과 제어 차단 사유를 표시합니다."
            lifecycleAvailabilityLabel.textColor = TerminalTheme.secondaryText
            return
        }
        let capabilities = session.account_risk?.capabilities
        let safeMock = session.mode == .mock
            && capabilities?.live_effects_enabled == false
            && session.account_risk?.risk.live_grant_active == false
        let sessionIdentityValid = (try? AutomationAccountIdentityScope.validate(session.account_identity, mode: session.mode)) != nil
        let lifecycleProjectionIsCurrent = hasFreshAccountProjection(for: session)
        let lifecycleSafe = session.mode == .live || safeMock
        let lifecycleEnabled = isVisibleWorkspace && !hasPendingWrite && sessionIdentityValid
            && lifecycleProjectionIsCurrent && lifecycleSafe
        pauseButton.isEnabled = lifecycleEnabled
            && session.state == .running && capabilities?.can_pause == true
        resumeButton.isEnabled = lifecycleEnabled
            && session.state == .paused && capabilities?.can_resume == true
        stopButton.isEnabled = lifecycleEnabled
            && session.state != .stopped && capabilities?.can_stop == true
        let lifecycleBlockReasons: [String]
        if hasPendingWrite {
            lifecycleBlockReasons = ["이전 변경 요청의 결과가 확인될 때까지 모든 변경 제어가 차단됩니다."]
        } else if !sessionIdentityValid {
            lifecycleBlockReasons = ["선택한 모드와 계좌 identity가 일치하지 않아 제어를 차단합니다."]
        } else if !lifecycleProjectionIsCurrent {
            lifecycleBlockReasons = ["현재 계좌·위험 정보가 확인되지 않았거나 30초보다 오래되어 제어를 차단합니다."]
        } else if !lifecycleSafe {
            lifecycleBlockReasons = ["모의 세션의 실주문 전송 기능이 켜졌다고 응답되어 안전 잠금으로 제어를 차단합니다."]
        } else {
            var reasons: [String] = []
            if session.state != .running { reasons.append("일시정지에는 RUNNING 상태가 필요합니다.") }
            else if capabilities?.can_pause != true { reasons.append("서버가 일시정지를 허용하지 않습니다.") }
            if session.state != .paused { reasons.append("재개에는 PAUSED 상태가 필요합니다.") }
            else if capabilities?.can_resume != true { reasons.append("서버가 재개를 허용하지 않습니다.") }
            if session.state == .stopped { reasons.append("세션이 이미 중지되었습니다.") }
            else if capabilities?.can_stop != true { reasons.append("서버가 중지를 허용하지 않습니다.") }
            lifecycleBlockReasons = reasons
        }
        let availableLifecycleActions = [
            pauseButton.isEnabled ? "일시정지" : nil,
            resumeButton.isEnabled ? "재개" : nil,
            stopButton.isEnabled ? "중지" : nil
        ].compactMap(\.self)
        if lifecycleBlockReasons.isEmpty {
            lifecycleAvailabilityLabel.stringValue = "현재 RUNNING / PAUSED 상태와 서버 기능 projection 기준 사용 가능 · \(availableLifecycleActions.joined(separator: " · "))"
            lifecycleAvailabilityLabel.textColor = TerminalTheme.secondaryText
        } else {
            lifecycleAvailabilityLabel.stringValue = "비활성 제어 사유 · \(lifecycleBlockReasons.joined(separator: " · "))"
            let capabilityBlocked = (session.state == .running && capabilities?.can_pause != true)
                || (session.state == .paused && capabilities?.can_resume != true)
                || (session.state != .stopped && capabilities?.can_stop != true)
            lifecycleAvailabilityLabel.textColor = hasPendingWrite || !sessionIdentityValid
                || !lifecycleProjectionIsCurrent || !lifecycleSafe || capabilityBlocked
                ? TerminalTheme.negative : TerminalTheme.secondaryText
        }
        lifecycleAvailabilityLabel.toolTip = lifecycleAvailabilityLabel.stringValue
        lifecycleAvailabilityLabel.setAccessibilityValue(lifecycleAvailabilityLabel.stringValue)
        nextEventsButton.isEnabled = isVisibleWorkspace && eventNextSequence != nil && activeReadRequest == nil
        nextLedgerButton.isEnabled = isVisibleWorkspace && ledgerNextSequence != nil && activeReadRequest == nil
        let selectedOrder = selectedOwnedOrder()
        let cancelableState = selectedOrder?.state == .open || selectedOrder?.state == .partiallyFilled
        cancelOrderButton.isEnabled = isVisibleWorkspace && !hasPendingWrite && safeMock
            && capabilities?.can_cancel_order == true && cancelableState
        let dayNonEmpty = !baselineDayField.stringValue.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
        let quoteFreeNonEmpty = !baselineQuoteFreeField.stringValue.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
        let baselineSourceSelected = baselineSourceMode == session.mode
            && (try? AutomationBaselineSource.selectableSource(at: baselineSourcePopup.indexOfSelectedItem, for: session.mode)) != nil
        baselineSourcePopup.isEnabled = isVisibleWorkspace && !hasPendingWrite && activeReadRequest == nil
            && baselineSourceMode == session.mode
        recordBaselineButton.isEnabled = isVisibleWorkspace && !hasPendingWrite
            && activeReadRequest == nil && session.state != .faulted
            && baselineSourceSelected && dayNonEmpty && quoteFreeNonEmpty
        queryBaselineButton.isEnabled = isVisibleWorkspace && activeReadRequest == nil
    }

    private func hasFreshAccountProjection(for session: AutomationSessionView) -> Bool {
        guard !projectionMissingForSelectedSession,
              accountProjectionBySession[session.session_id] != nil,
              let receivedAt = accountReceivedAtBySession[session.session_id] else { return false }
        let age = Date().timeIntervalSince(receivedAt)
        return age >= 0 && age <= 30
    }

    private func selectedOwnedOrder() -> AutomationOrderView? {
        guard let selectedOrderClientIdentifier else { return nil }
        return ledger?.orders.first(where: { $0.client_identifier == selectedOrderClientIdentifier })
    }

    private func restoreOrderSelection() {
        guard let selectedOrderClientIdentifier,
              let row = ledger?.orders.firstIndex(where: { $0.client_identifier == selectedOrderClientIdentifier }) else {
            selectedOrderClientIdentifier = nil
            orderTable.deselectAll(nil)
            return
        }
        orderTable.selectRowIndexes(IndexSet(integer: row), byExtendingSelection: false)
    }

    private func selectSessionRow() {
        guard let selectedSessionID,
              let row = sessions.firstIndex(where: { $0.session_id == selectedSessionID }) else { return }
        sessionTable.selectRowIndexes(IndexSet(integer: row), byExtendingSelection: false)
    }

    private func renderWorkspace() {
        if let decisionAnalysisWindowSessionID,
           selectedSession?.session_id != decisionAnalysisWindowSessionID {
            closeDecisionAnalysisWindow()
        }
        privacyButton.title = privacyIsMasked() ? "계좌 금액 표시" : "계좌 금액 숨기기"
        if let session = selectedSession {
            let effects = (session.account_risk?.capabilities.live_effects_enabled).map { $0 ? "ON" : "OFF" } ?? "미확인"
            modeBadge.stringValue = "\(modeLabel(session.mode)) · 계좌 \(accountIdentityLabel(session.account_identity)) · 실주문 전송 \(effects)"
            modeBadge.textColor = session.mode == .live ? TerminalTheme.negative : TerminalTheme.accent
        } else {
            modeBadge.stringValue = "새 세션 모드 \(modeLabel(selectedStartMode)) · 실주문 전송 상태 미확인"
            modeBadge.textColor = selectedStartMode == .live ? TerminalTheme.negative : TerminalTheme.accent
        }
        modeBadge.toolTip = modeBadge.stringValue
        modeBadge.setAccessibilityValue(modeBadge.stringValue)
        modeBadge.setAccessibilityHelp("선택한 session projection의 실제 MOCK/LIVE, account identity와 live_effects_enabled 값. 새 세션 전에는 주문 전송 상태가 미확인입니다.")
        sessionTable.reloadData()
        orderTable.reloadData()
        restoreOrderSelection()
        fillTable.reloadData()
        eventTable.reloadData()
        if let pendingMetadataError {
            statusBanner.stringValue = pendingMetadataError
            statusBanner.textColor = TerminalTheme.negative
        } else if let requestID = pendingRequestID {
            statusBanner.stringValue = pendingReadbackStatus
                ?? "이전 요청 결과 확인 중 · request_id: \(requestID) · 중복 전송하지 않고 저장된 기록만 다시 확인합니다."
            statusBanner.textColor = TerminalTheme.negative
        }
        updateBaselineSourceChoices(for: selectedSession?.mode)
        renderSessionSummary()
        renderAccountRisk()
        updateControlEnablement()
        publishSummary()
    }

    private func renderSessionSummary() {
        guard let session = selectedSession else {
            sessionSummaryLabel.stringValue = "선택된 세션이 없습니다. 새 세션은 기본 MOCK이며 LIVE를 직접 선택할 수 있습니다. 모든 위험 한도는 직접 입력해야 합니다."
            sessionSummaryLabel.toolTip = sessionSummaryLabel.stringValue
            sessionSummaryLabel.setAccessibilityHelp(sessionSummaryLabel.stringValue)
            return
        }
        let unresolved = session.unresolved_order_count
        let recovery = session.recovery_reason ?? "없음"
        sessionSummaryLabel.stringValue = [
            "세션 ID: \(session.session_id)",
            "계좌 구분: \(accountScopeLabel(for: session.mode))",
            "account_identity: \(accountIdentityLabel(session.account_identity))",
            "모드 / 상태: \(modeLabel(session.mode)) / \(stateLabel(session.state)) (\(session.state.rawValue))",
            "요청 순번: \(session.sequence)",
            "정책 ID: \(session.policy_ref.policy_id)",
            "정책 버전 ID: \(session.policy_ref.revision_id)",
            "정책 정의 digest: \(session.policy_ref.definition_digest)",
            "실행·모델·F08: 자동화 세션 정보에 포함되지 않음",
            "시장 / 판단 주기: \(session.market) / \(session.decision_interval)",
            "생성 시각(UTC): \(session.created_at_utc) · 갱신 시각(UTC): \(session.updated_at_utc)",
            "마지막 확정 봉(UTC): \(session.last_finalized_candle_at ?? "미확인") · 잔액 관측(UTC): \(session.last_balance_observed_at ?? "미확인")",
            "미해결 주문: \(unresolved) · 체결: \(session.fill_count) · 복구 사유: \(recovery)"
        ].joined(separator: "\n")
        sessionSummaryLabel.toolTip = sessionSummaryLabel.stringValue
        sessionSummaryLabel.setAccessibilityValue(sessionSummaryLabel.stringValue)
        sessionSummaryLabel.setAccessibilityHelp(sessionSummaryLabel.stringValue)
    }

    private func renderAccountRisk() {
        guard let session = selectedSession else {
            riskIdentityLabel.stringValue = "동결된 전략 버전과 위험 한도는 세션을 선택하면 표시됩니다."
            accountBalanceLabel.stringValue = "계좌 잔액: 미확인"
            baselineStatusLabel.stringValue = "일일 시작 기준선: 미확인"
            admissionBlockReasonLabel.stringValue = "주문 승인 차단 사유: 미확인"
            riskUsageLabel.stringValue = "위험 한도 사용량: 미확인"
            feedFreshnessLabel.stringValue = "시장 데이터: 미확인"
            capabilityLabel.stringValue = "서버가 허용한 기능: 미확인"
            mockAssumptionLabel.stringValue = ""
            mockAssumptionLabel.isHidden = true
            receivedAtLabel.stringValue = "앱 수신 시각: 미확인"
            return
        }
        let masked = privacyIsMasked()
        if baselineDayField.stringValue.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            let fallbackDay = session.account_risk?.day_baseline?.day_utc
                ?? session.account_risk?.legacy_mock_baseline?.day_utc
                ?? String(session.updated_at_utc.prefix(10))
            baselineDayField.stringValue = fallbackDay
        }
        let activationLossLimit = session.risk.activation_loss_limit_krw.map { amount($0, masked: masked) } ?? "미제공"
        riskIdentityLabel.stringValue = "동결된 위험 한도 · 주문당 최대 \(amount(session.risk.max_order_notional_krw, masked: masked)) KRW · 하루 주문액 최대 \(amount(session.risk.max_daily_notional_krw, masked: masked)) KRW · 자산 보유액 최대 \(amount(session.risk.max_asset_exposure_krw, masked: masked)) KRW · 하루 손실 최대 \(amount(session.risk.max_daily_loss_krw, masked: masked)) KRW · LIVE 활성화 손실 한도 \(activationLossLimit) KRW · 미체결 주문 최대 \(session.risk.max_open_orders)건 · 주기당 최대 \(session.risk.max_orders_per_interval)건 · 잔액 유효 시간 \(session.risk.max_balance_age_seconds)초 · 시세 유효 시간 \(session.risk.max_feed_age_seconds)초"
        guard let projection = accountProjectionBySession[session.session_id] else {
            let reason = projectionMissingForSelectedSession
                ? "이 세션에 계좌·위험 정보가 제공되지 않았습니다."
                : "계좌·위험 정보를 아직 받지 못했습니다."
            accountBalanceLabel.stringValue = "\(reason) 현금, 자산, 예약액, 노출액, 손실액은 알 수 없습니다. 0으로 간주하지 않습니다."
            baselineStatusLabel.stringValue = "일일 시작 기준선: 미확인 (서버 응답 대기 중)"
            admissionBlockReasonLabel.stringValue = "주문 승인 차단 사유: 미확인"
            riskUsageLabel.stringValue = "위험 한도 사용량: 미확인"
            feedFreshnessLabel.stringValue = "시세 출처·봉 종료·수신 시각·최신 여부: 미확인"
            capabilityLabel.stringValue = "서버가 허용한 기능: 정보 수신 전까지 미확인"
            mockAssumptionLabel.stringValue = ""
            mockAssumptionLabel.isHidden = true
            receivedAtLabel.stringValue = "앱 수신 시각: \(lastSuccessfulReadAt.map(utcString) ?? "미확인")"
            return
        }

        let balance = projection.balance
        let usage = projection.risk
        let mask = privacyIsMasked()
        let accountReceivedAt = accountReceivedAtBySession[session.session_id].map(utcString) ?? "미확인"
        let projectionState = projectionMissingForSelectedSession ? "현재 정보 없음 · 마지막으로 받은 값" : "현재 서버 응답"
        accountBalanceLabel.stringValue = [
            "\(projectionState) · 출처: \(sourceLabel(balance.source)) · 최신 여부: \(freshnessLabel(balance.freshness))",
            "\(accountObservationClockLabel(balance.source)): \(balance.observed_at_utc ?? "미확인") · 서버 수신 시각(UTC): \(balance.received_at_utc ?? "미확인")",
            "\(balance.quote_currency) 총액 / 사용 가능 / 거래소 잠금 / 세션 예약: \(amount(balance.quote_total, masked: mask)) / \(amount(balance.quote_free, masked: mask)) / \(amount(balance.quote_exchange_locked, masked: mask)) / \(amount(balance.quote_local_reserved, masked: mask))",
            "\(balance.base_currency) 총량 / 사용 가능 / 거래소 잠금 / 세션 예약: \(amount(balance.base_total, masked: mask)) / \(amount(balance.base_free, masked: mask)) / \(amount(balance.base_exchange_locked, masked: mask)) / \(amount(balance.base_local_reserved, masked: mask))"
        ].joined(separator: "\n")

        if let dayBaseline = projection.day_baseline {
            let evidenceSummary: String
            if let ev = dayBaseline.exchange_evidence {
                let snapDesc = ev.snapshot_observed_at_utc ?? "미확인"
                let receivedDesc = ev.snapshot_received_at_utc ?? "미확인"
                let priorDesc = ev.prior_day_baseline_digest.map { String($0.prefix(12)) } ?? "전일 기준선 없음"
                switch session.mode {
                case .mock:
                    evidenceSummary = "모의 원천 로컬 시각(UTC): \(snapDesc) · 서버 수신 시각(UTC): \(receivedDesc) · 전일 기준선 digest: \(priorDesc)"
                case .live:
                    evidenceSummary = "서버 로컬 잔액 조회 시각(UTC): \(snapDesc) · 서버 수신 시각(UTC): \(receivedDesc) · 거래소 provider as-of: 미제공 · 전일 기준선 digest: \(priorDesc)"
                }
            } else {
                evidenceSummary = session.mode == .live
                    ? "거래소 provider as-of 증거: 미제공 · 로컬 시각만으로 자정 잔액을 입증할 수 없습니다"
                    : "모의 원장 스냅샷 증거: 없음"
            }
            let baselineRiskSufficient = dayBaseline.verification_state == .verified
                && dayBaseline.sufficient_for_risk_admission
                && dayBaseline.block_reasons.isEmpty
            let storedBlockSummary = dayBaseline.block_reasons.isEmpty
                ? "저장 당시 차단 사유 기록 없음"
                : dayBaseline.block_reasons.map(admissionBlockReasonDescription).joined(separator: ", ")
            let currentBaselineBlockReasons = projection.admission_block_reasons.filter {
                $0.hasPrefix("BASELINE:")
                    || $0.hasPrefix("DAILY_START_BASELINE_")
                    || $0 == "DAILY_LOSS_UNKNOWN"
            }
            let currentBaselineUseSummary: String
            if !currentBaselineBlockReasons.isEmpty {
                currentBaselineUseSummary = "\(projectionState) · 현재 일일 손실 기준선 사용 차단: \(currentBaselineBlockReasons.map(admissionBlockReasonDescription).joined(separator: " · "))"
            } else if !projection.admission_block_reasons.isEmpty {
                currentBaselineUseSummary = "\(projectionState) · 저장된 기준선 판정과 별개로 현재 주문 승인은 다른 위험 조건으로 차단: \(projection.admission_block_reasons.map(admissionBlockReasonDescription).joined(separator: " · "))"
            } else if projection.capabilities.can_submit_orders {
                currentBaselineUseSummary = "\(projectionState) · 현재 서버가 주문 제출을 허용합니다"
            } else {
                currentBaselineUseSummary = "\(projectionState) · 서버 주문 제출은 불가하지만 차단 사유는 제공되지 않았습니다"
            }
            baselineStatusLabel.stringValue = [
                "저장된 기준선 · \(projectionState) · 계좌: \(modeLabel(dayBaseline.mode)) · \(accountIdentityLabel(dayBaseline.account_identity)) · UTC 날짜: \(dayBaseline.day_utc)",
                "저장된 검증 상태: \(baselineVerificationLabel(dayBaseline.verification_state)) · 저장 당시 위험 승인 충분성: \(baselineRiskSufficient ? "충족" : "차단됨") · \(baselineReferenceTimeLabel(dayBaseline)): \(dayBaseline.reference_time_utc) · 서버 기록 시각(UTC): \(dayBaseline.recorded_at_utc)",
                "\(baselineSourceLabel(dayBaseline.source)) · 시작 총자산(KRW): \(amount(dayBaseline.total_equity_krw, masked: mask)) · digest: \(String(dayBaseline.baseline_digest.prefix(16)))…",
                "자산: \(baselineAssetSummary(dayBaseline.assets, masked: mask))",
                "\(evidenceSummary) · 저장 당시 판정: \(storedBlockSummary)",
                currentBaselineUseSummary
            ].joined(separator: "\n")
            baselineStatusLabel.textColor = baselineRiskSufficient ? .labelColor : TerminalTheme.negative
        } else if let legacy = projection.legacy_mock_baseline {
            let legacyCanAdmit = projection.capabilities.can_submit_orders
                && projection.admission_block_reasons.isEmpty
            let blockText = projection.admission_block_reasons.isEmpty
                ? (legacyCanAdmit ? "현재 서버의 주문 제출 기능 허용" : "서버 차단 사유 미확인")
                : projection.admission_block_reasons.map(admissionBlockReasonDescription).joined(separator: " · ")
            baselineStatusLabel.stringValue = [
                "\(projectionState) · 계좌: \(modeLabel(.mock)) · \(accountIdentityLabel(legacy.account_identity)) · UTC 날짜: \(legacy.day_utc)",
                "기준선: 구형 모의 원장(모의 거래만) · 범위: \(AutomationLegacyMockBaselineView.scope)",
                "일일 손실 기준 총액(KRW): \(amount(legacy.baseline_equity_krw, masked: mask)) · balances digest: \(String(legacy.balances_digest.prefix(16)))…",
                "로컬 기록 시각(UTC): \(legacy.recorded_at_utc) · 거래소 as-of 증거 아님 · 자산별 평가 증거 없음",
                "주문 승인: \(legacyCanAdmit ? "서버 허용" : "차단") · \(blockText)"
            ].joined(separator: "\n")
            baselineStatusLabel.textColor = legacyCanAdmit ? .labelColor : TerminalTheme.negative
        } else if let effectiveLoss = projection.effective_loss,
                  effectiveLoss.basis == .liveActivationObserved {
            let referenceState = projection.activation_reference.map { $0.valuation_state.rawValue } ?? "미제공"
            let useText = effectiveLoss.admission_blocked
                ? "LIVE 활성화 손실 기준 사용 차단"
                : "LIVE_ACTIVATION_OBSERVED를 유효 손실 기준으로 사용"
            baselineStatusLabel.stringValue = "계좌: LIVE · \(accountIdentityLabel(session.account_identity)) · 일일 UTC 시작 기준선은 이 손실 기준으로 사용하지 않습니다. \(useText) · 활성화 평가 상태: \(referenceState)"
            baselineStatusLabel.textColor = effectiveLoss.admission_blocked ? TerminalTheme.negative : .labelColor
        } else if let effectiveLoss = projection.effective_loss,
                  effectiveLoss.basis == .unavailable {
            let reasons = effectiveLoss.block_reasons.isEmpty
                ? "차단 사유 미제공"
                : effectiveLoss.block_reasons.map(admissionBlockReasonDescription).joined(separator: " · ")
            baselineStatusLabel.stringValue = "유효 손실 기준: UNAVAILABLE · 일일 기준선 여부를 현재 응답에서 확인할 수 없습니다 · \(reasons)"
            baselineStatusLabel.textColor = TerminalTheme.negative
        } else {
            let missingEvidence = session.mode == .mock
                ? "신뢰 가능한 당일 모의 원장 기준선 증거"
                : "신뢰 가능한 당일 00:00:00Z 시작 기준선 증거"
            let effectiveLossReasons = projection.effective_loss?.block_reasons ?? []
            let reportedMissingBaseline = (effectiveLossReasons + projection.admission_block_reasons)
                .contains("DAILY_START_BASELINE_MISSING")
            let baselineState = reportedMissingBaseline
                ? "일일 시작 기준선 미등록 (DAILY_START_BASELINE_MISSING) · \(missingEvidence)가 없습니다."
                : "일일 시작 기준선 상태 미확인 · 현재 응답에 저장된 기준선이 없습니다."
            baselineStatusLabel.stringValue = "계좌: \(modeLabel(session.mode)) · \(accountIdentityLabel(session.account_identity)) · \(baselineState)"
            baselineStatusLabel.textColor = TerminalTheme.negative
        }

        let activationLossCanAdmit = session.mode == .live
            && projection.effective_loss?.basis == .liveActivationObserved
            && projection.effective_loss?.admission_blocked == false
            && projection.effective_loss?.block_reasons.isEmpty == true
            && projection.effective_loss?.limit_krw != nil
            && projection.effective_loss?.used_krw != nil
            && projection.activation_reference?.mode == .live
            && projection.activation_reference?.account_identity == session.account_identity
            && projection.activation_reference?.valuation_state == .verified
            && projection.activation_reference?.reference_equity_krw != nil
        let baselineBlocksAdmission = activationLossCanAdmit ? false : projection.day_baseline.map {
            $0.verification_state != .verified || !$0.sufficient_for_risk_admission || !$0.block_reasons.isEmpty
        } ?? (projection.legacy_mock_baseline == nil)
        if projection.admission_block_reasons.isEmpty && !projection.capabilities.can_submit_orders {
            admissionBlockReasonLabel.stringValue = "서버가 주문 제출을 허용하지 않습니다. 차단 사유는 응답에 포함되지 않았습니다."
            admissionBlockReasonLabel.textColor = TerminalTheme.negative
        } else if projection.admission_block_reasons.isEmpty && baselineBlocksAdmission {
            admissionBlockReasonLabel.stringValue = "주문 승인이 차단되어 있습니다. 서버 응답에 기준선 차단 사유가 포함되지 않아 사유는 미확인입니다."
            admissionBlockReasonLabel.textColor = TerminalTheme.negative
        } else if projection.admission_block_reasons.isEmpty {
            if projection.legacy_mock_baseline != nil {
                admissionBlockReasonLabel.stringValue = "서버 주문 제출 허용 · LEGACY_LOCAL_MOCK_LEDGER scalar만 모의 손실 계산 기준으로 사용 · 거래소 as-of 및 자산별 평가 증거 없음"
            } else if activationLossCanAdmit {
                admissionBlockReasonLabel.stringValue = "서버 주문 제출 허용 · LIVE 활성화 손실 사용 / 한도: \(amount(projection.effective_loss?.used_krw, masked: mask)) / \(amount(projection.effective_loss?.limit_krw, masked: mask)) KRW · 일일 UTC 기준선은 이 손실 기준으로 사용하지 않음"
            } else {
                admissionBlockReasonLabel.stringValue = "주문 승인 차단 사유: 없음 (위험 한도·일일 시작 기준선·데이터 신선도 충족)"
            }
            admissionBlockReasonLabel.textColor = TerminalTheme.secondaryText
        } else {
            let formattedReasons = projection.admission_block_reasons.map(admissionBlockReasonDescription).joined(separator: " · ")
            admissionBlockReasonLabel.stringValue = "주문 승인 차단 사유 (\(projection.admission_block_reasons.count)건): \(formattedReasons)"
            admissionBlockReasonLabel.textColor = TerminalTheme.negative
        }

        riskUsageLabel.stringValue = [
            "하루 주문액 사용 / 한도: \(amount(usage.daily_notional_used_krw, masked: mask)) / \(amount(usage.daily_notional_limit_krw, masked: mask)) KRW",
            "UTC 일일 손실 사용 / 한도: \(amount(usage.daily_loss_used_krw, masked: mask)) / \(amount(usage.daily_loss_limit_krw, masked: mask)) KRW",
            effectiveLossSummary(projection.effective_loss, masked: mask),
            "자산 보유액 / 한도: \(amount(usage.asset_exposure_krw, masked: mask)) / \(amount(usage.asset_exposure_limit_krw, masked: mask)) KRW",
            "미체결 주문 수 / 한도: \(usage.open_order_count) / \(usage.open_order_limit)건 · 이번 주기 주문 수 / 한도: \(usage.orders_in_interval.map(String.init) ?? "미확인") / \(usage.orders_per_interval_limit)건",
            "긴급 정지: \(usage.emergency_latched ? "켜짐" : "꺼짐") · 실거래 허가: \(usage.live_grant_active ? "활성" : "비활성")"
        ].joined(separator: "\n")
        let activationSummary = activationReferenceSummary(projection.activation_reference, masked: mask)
        if !activationSummary.isEmpty {
            baselineStatusLabel.stringValue += "\n\(activationSummary)"
        }
        let feed = projection.feed
        feedFreshnessLabel.stringValue = "시세 출처: \(sourceLabel(feed.source)) · 최신 여부: \(freshnessLabel(feed.freshness)) · 원천 봉 종료 시각(UTC): \(feed.observation_close_at_utc ?? "미확인") · 서버 수신 시각(UTC): \(feed.received_at_utc ?? "미확인")"
        let caps = projection.capabilities
        capabilityLabel.stringValue = [
            "서버가 허용한 기능 · 일시정지 \(yesNo(caps.can_pause)) · 재개 \(yesNo(caps.can_resume)) · 중지 \(yesNo(caps.can_stop)) · 주문 취소 \(yesNo(caps.can_cancel_order))",
            "전략 주문 제출 \(yesNo(caps.can_submit_orders)) · 실주문 전송 \(yesNo(caps.live_effects_enabled)) · 별도 승인 \(caps.explicit_approval_required ? "필요" : "불필요")",
            session.mode == .mock && caps.live_effects_enabled ? "안전 잠금 · 모의 세션인데 서버가 실주문 전송을 허용한다고 응답했습니다. 모든 제어를 비활성화했습니다." : ""
        ].filter { !$0.isEmpty }.joined(separator: "\n")
        if let assumptions = projection.mock_assumptions {
            mockAssumptionLabel.isHidden = false
            mockAssumptionLabel.stringValue = "서버가 반환한 모의 거래 가정 · 시작 KRW \(amount(assumptions.initial_quote_balance_krw, masked: mask)) · 시작 자산 수량 \(amount(assumptions.initial_base_balance, masked: mask)) · 수수료율 \(assumptions.fee_rate) · 체결 방식: \(assumptions.fill_model)"
        } else {
            mockAssumptionLabel.stringValue = ""
            mockAssumptionLabel.isHidden = true
        }
        receivedAtLabel.stringValue = "앱 수신 시각(UTC): \(accountReceivedAt) · 마지막 정상 조회(UTC): \(lastSuccessfulReadAt.map(utcString) ?? "미확인")"
        let warning = session.mode == .live || caps.live_effects_enabled || usage.live_grant_active
        capabilityLabel.textColor = warning ? TerminalTheme.negative : TerminalTheme.secondaryText

        if let queried = queriedBaselineStatus, queried.session_id == session.session_id {
            renderBaselineQueryStatus(queried, masked: mask)
        }
    }

    private func effectiveLossSummary(
        _ loss: AutomationEffectiveLossProjection?,
        masked: Bool
    ) -> String {
        guard let loss else { return "유효 손실 기준: 미확인 · projection 미제공" }
        let reasons = loss.block_reasons.isEmpty
            ? "차단 사유 없음"
            : loss.block_reasons.map(admissionBlockReasonDescription).joined(separator: " · ")
        return "유효 손실 기준 \(loss.basis.rawValue) · 사용 / 한도: \(amount(loss.used_krw, masked: masked)) / \(amount(loss.limit_krw, masked: masked)) KRW · 서버 차단: \(loss.admission_blocked ? "차단됨" : "없음") · \(reasons)"
    }

    private func activationReferenceSummary(
        _ reference: AutomationLiveActivationReferenceView?,
        masked: Bool
    ) -> String {
        guard let reference else { return "" }
        return "LIVE activation reference · 모드 \(modeLabel(reference.mode)) · account_identity \(accountIdentityLabel(reference.account_identity)) · 출처 \(reference.source.rawValue) · 관측 시각(UTC) \(reference.reference_observed_at_utc) · 수신 시각(UTC) \(reference.reference_received_at_utc) · 평가 기록 시각(UTC) \(reference.valuation_recorded_at_utc) · snapshot/control 순번 \(reference.snapshot_sequence)/\(reference.control_sequence) · 최대 시세 경과 \(reference.reference_max_feed_age_seconds)초 · 평가 상태 \(reference.valuation_state.rawValue) · 기준 equity(KRW) \(amount(reference.reference_equity_krw, masked: masked)) · balances digest \(reference.balances_digest) · valuation digest \(reference.valuation_digest)"
    }

    private func renderBaselineQueryStatus(_ status: AutomationAccountDayBaselineStatus, masked: Bool) {
        let scopeDesc = "\(modeLabel(status.mode)) · account_identity: \(accountIdentityLabel(status.account_identity))"
        if let record = status.baseline {
            let baselineRiskSufficient = record.verification_state == .verified
                && record.sufficient_for_risk_admission
                && record.block_reasons.isEmpty
            let storedBlockText = record.block_reasons.isEmpty
                ? "저장 당시 차단 사유 기록 없음"
                : record.block_reasons.map(admissionBlockReasonDescription).joined(separator: " · ")
            let currentBlockText = status.block_reasons.isEmpty
                ? (status.admission_blocked ? "서버가 현재 차단했으나 사유를 제공하지 않았습니다" : "현재 차단 사유 없음")
                : status.block_reasons.map(admissionBlockReasonDescription).joined(separator: " · ")
            baselineQueryResultLabel.stringValue = [
                "조회된 저장 기준선 · 세션: \(status.session_id) · \(scopeDesc) · ID: \(record.baseline_id) · UTC 날짜: \(record.day_utc) · \(baselineReferenceTimeLabel(record)): \(record.reference_time_utc)",
                "저장 당시 검증 상태: \(baselineVerificationLabel(record.verification_state)) · 저장 당시 위험 승인 충분성: \(baselineRiskSufficient ? "충족" : "차단") · 서버 기록 시각(UTC): \(record.recorded_at_utc)",
                "\(baselineSourceLabel(record.source)) · 시작 총자산(KRW): \(amount(record.total_equity_krw, masked: masked)) · digest: \(String(record.baseline_digest.prefix(16)))…",
                "자산: \(baselineAssetSummary(record.assets, masked: masked))",
                "저장 당시 판정: \(storedBlockText)",
                "현재 주문 승인: \(status.admission_blocked ? "차단됨" : "허용") · 현재 사유: \(currentBlockText)"
            ].joined(separator: "\n")
            baselineQueryResultLabel.textColor = status.admission_blocked ? TerminalTheme.negative : TerminalTheme.secondaryText
        } else if let legacy = status.legacy_mock_baseline {
            let blockText = status.block_reasons.isEmpty
                ? (status.admission_blocked ? "서버가 차단했으나 사유를 제공하지 않았습니다" : "차단 사유 없음")
                : status.block_reasons.map(admissionBlockReasonDescription).joined(separator: " · ")
            baselineQueryResultLabel.stringValue = [
                "조회된 구형 모의 원장 기준선 · 세션: \(status.session_id) · \(scopeDesc) · 날짜(UTC): \(legacy.day_utc)",
                "출처: 구형 모의 원장(모의 거래만) · 범위: \(AutomationLegacyMockBaselineView.scope)",
                "일일 손실 기준 총액(KRW): \(amount(legacy.baseline_equity_krw, masked: masked)) · balances digest: \(String(legacy.balances_digest.prefix(16)))…",
                "로컬 기록 시각(UTC): \(legacy.recorded_at_utc) · 거래소 as-of 증거 아님 · 자산별 평가 증거 없음",
                "주문 승인 차단 여부: \(status.admission_blocked ? "차단됨" : "허용") · 사유: \(blockText)"
            ].joined(separator: "\n")
            baselineQueryResultLabel.textColor = status.admission_blocked ? TerminalTheme.negative : TerminalTheme.secondaryText
        } else {
            let blockText = status.block_reasons.isEmpty
                ? "서버가 기준선 누락을 차단했으나 사유를 제공하지 않았습니다"
                : status.block_reasons.map(admissionBlockReasonDescription).joined(separator: " · ")
            baselineQueryResultLabel.stringValue = "조회된 기준선 · 세션: \(status.session_id) · \(scopeDesc) · 날짜: \(status.day_utc) · 미등록 · 주문 승인 차단됨 · 사유: \(blockText)"
            baselineQueryResultLabel.textColor = TerminalTheme.negative
        }
    }

    private func baselineAssetSummary(_ assets: [AutomationBaselineAssetEntry], masked: Bool) -> String {
        assets.map { asset in
            "\(asset.currency) free/locked/total \(amount(asset.free, masked: masked))/\(amount(asset.locked, masked: masked))/\(amount(asset.total, masked: masked)) · 기준 단가 \(amount(asset.mark_price_krw, masked: masked)) KRW · 평가 \(amount(asset.valuation_krw, masked: masked)) KRW · 기준 \(asset.valuation_basis.rawValue)"
        }.joined(separator: " · ")
    }

    private func amount(_ value: String?, masked: Bool) -> String {
        guard let value else { return "미확인" }
        return masked ? "••••" : value
    }

    private func riskFieldLabel(for key: String) -> String {
        switch key {
        case "max_order_notional_krw": return "주문당 최대 금액(KRW)"
        case "max_daily_notional_krw": return "하루 최대 주문액(KRW)"
        case "max_asset_exposure_krw": return "자산 최대 보유액(KRW)"
        case "max_daily_loss_krw": return "하루 최대 손실(KRW)"
        case "max_open_orders": return "동시 미체결 주문 수"
        case "max_orders_per_interval": return "주기당 주문 수"
        case "max_balance_age_seconds": return "잔액 정보 유효 시간(초)"
        case "max_feed_age_seconds": return "시세 정보 유효 시간(초)"
        default: return key
        }
    }

    private func userFacingError(_ error: Error) -> String {
        guard let serviceError = error as? AutomationSessionServiceError else {
            return error.localizedDescription
        }
        guard case .invalidInput(let detail) = serviceError else {
            return error.localizedDescription
        }
        if detail.contains("max_order_notional_krw는 max_daily_notional_krw") {
            return "주문당 최대 금액은 하루 최대 주문액 이하여야 합니다."
        }
        if detail.contains("는 0보다 큰 정확한 십진수여야 합니다") {
            let key = detail.components(separatedBy: "는 0보다 큰 정확한 십진수여야 합니다").first ?? detail
            return "\(riskFieldLabel(for: key))은 0보다 큰 숫자로 입력하세요."
        }
        let keys = [
            "max_order_notional_krw", "max_daily_notional_krw", "max_asset_exposure_krw",
            "max_daily_loss_krw", "max_open_orders", "max_orders_per_interval",
            "max_balance_age_seconds", "max_feed_age_seconds"
        ]
        return keys.reduce(detail) { result, key in
            result.replacingOccurrences(of: key, with: riskFieldLabel(for: key))
        }
    }

    private func yesNo(_ value: Bool) -> String { value ? "허용" : "불가" }

    private func sourceLabel(_ source: AutomationDataSource) -> String {
        let name: String
        switch source {
        case .upbitLiveRead: name = "Upbit 계좌 조회"
        case .upbitPublicFeed: name = "Upbit 공개 시세"
        case .durableMockVenue: name = "모의 거래소"
        }
        return "\(name) (\(source.rawValue))"
    }

    private func freshnessLabel(_ freshness: AutomationFreshness) -> String {
        let name: String
        switch freshness {
        case .fresh: name = "최신"
        case .delayed: name = "지연"
        case .stale: name = "오래됨"
        case .unavailable: name = "확인 불가"
        }
        return "\(name) (\(freshness.rawValue))"
    }

    private func accountScopeLabel(for mode: AutomationMode) -> String {
        switch mode {
        case .mock: return "모의 계좌"
        case .live: return "실계정 (식별 정보 숨김)"
        }
    }

    private func modeLabel(_ mode: AutomationMode) -> String {
        switch mode {
        case .mock: return "모의(MOCK)"
        case .live: return "실거래(LIVE)"
        }
    }

    private func stateLabel(_ state: AutomationSessionState) -> String {
        switch state {
        case .running: return "실행 중"
        case .paused: return "일시정지"
        case .stopped: return "중지됨"
        case .needsReconciliation: return "주문 확인 필요"
        case .blockedApproval: return "승인 대기"
        case .faulted: return "오류"
        }
    }

    private func orderStateLabel(_ state: AutomationOrderState) -> String {
        switch state {
        case .reserved: return "금액 예약됨"
        case .submittedUnknown: return "접수 여부 확인 중"
        case .open: return "미체결"
        case .partiallyFilled: return "일부 체결"
        case .filled: return "체결 완료"
        case .cancelled: return "취소됨"
        case .rejected: return "거부됨"
        case .needsReconciliation: return "주문 확인 필요"
        }
    }

    private func sideLabel(_ side: AutomationSide) -> String {
        switch side {
        case .bid: return "매수(BID)"
        case .ask: return "매도(ASK)"
        }
    }

    private func eventKindLabel(_ kind: AutomationEventKind) -> String {
        switch kind {
        case .started: return "세션 시작"
        case .paused: return "일시정지"
        case .resumed: return "세션 재개"
        case .stopped: return "세션 중지"
        case .candleObserved: return "봉 데이터 확인"
        case .signalObserved: return "전략 신호 확인"
        case .intentRejected: return "주문 조건 거부"
        case .orderAdmitted: return "주문 승인"
        case .orderSendClaimed: return "주문 전송 준비"
        case .orderAcknowledged: return "거래소 주문 접수"
        case .orderCancelRequested: return "주문 취소 요청"
        case .orderReconciled: return "주문 상태 확인"
        case .fillRecorded: return "체결 기록"
        case .needsReconciliation: return "주문 확인 필요"
        case .recoveryBlocked: return "복구 중단"
        case .baselineRecorded: return "일일 기준선 검증 완료"
        case .baselineBlocked: return "일일 기준선 차단 보존"
        }
    }

    private func actionLabel(_ action: AutomationCommandAction) -> String {
        switch action {
        case .start: return "시작"
        case .pause: return "일시정지"
        case .resume: return "재개"
        case .stop: return "중지"
        case .cancelOrder: return "주문 취소"
        case .recordBaseline: return "일일 기준선 기록"
        }
    }

    private func baselineSourceLabel(_ source: AutomationBaselineSource) -> String {
        switch source {
        case .operatorAttested: return "선언 출처: 운영자 증빙 입력 (OPERATOR_ATTESTED)"
        case .exchangeObservedDayStart: return "선언 출처: 거래소 당일 시작 스냅샷 (EXCHANGE_OBSERVED_DAY_START)"
        case .priorDayVerifiedLedgerRollover: return "선언 출처: 전일 검증 원장 롤오버 (PRIOR_DAY_VERIFIED_LEDGER_ROLLOVER)"
        case .durableMockVenueSeed: return "선언 출처: 모의 원장 시드 (DURABLE_MOCK_VENUE_SEED)"
        }
    }

    private func baselineReferenceTimeLabel(_ baseline: AutomationAccountDayBaselineView) -> String {
        baseline.mode == .mock && baseline.source == .durableMockVenueSeed
            ? "모의 시드 생성 시각(UTC)"
            : "기준 시각(UTC)"
    }

    private func updateBaselineSourceChoices(for mode: AutomationMode?) {
        let choices = mode.map { AutomationBaselineSource.selectableSources(for: $0) } ?? []
        guard baselineSourceMode != mode || baselineSourceChoices != choices else { return }
        baselineSourceMode = mode
        baselineSourceChoices = choices
        baselineSourcePopup.removeAllItems()

        if let mode {
            for source in choices {
                baselineSourcePopup.addItem(withTitle: baselineSourceOptionTitle(source, mode: mode))
            }
            baselineSourcePopup.selectItem(at: choices.isEmpty ? -1 : 0)
            baselineSourceNoteLabel.stringValue = baselineSourceNote(for: mode)
            baselineSourcePopup.isEnabled = isVisibleWorkspace && !choices.isEmpty
        } else {
            baselineSourcePopup.addItem(withTitle: "세션을 선택하세요")
            baselineSourcePopup.item(at: 0)?.isEnabled = false
            baselineSourcePopup.selectItem(at: 0)
            baselineSourceNoteLabel.stringValue = "세션을 선택하면 해당 모드의 기준선 출처를 표시합니다."
            baselineSourcePopup.isEnabled = false
        }
    }

    private func baselineSourceOptionTitle(_ source: AutomationBaselineSource, mode: AutomationMode) -> String {
        switch (mode, source) {
        case (.mock, .durableMockVenueSeed):
            return "모의 원장 시드 · 실제 생성 시각과 잔액 검증 (DURABLE_MOCK_VENUE_SEED)"
        case (.mock, .operatorAttested):
            return "운영자 수기 입력 · 검증 차단 (OPERATOR_ATTESTED)"
        case (.live, .exchangeObservedDayStart):
            return "거래소 자정 스냅샷 요청 · provider as-of 없으면 차단 (EXCHANGE_OBSERVED_DAY_START)"
        case (.live, .priorDayVerifiedLedgerRollover):
            return "전일 검증 롤오버 요청 · provider as-of 없으면 차단 (PRIOR_DAY_VERIFIED_LEDGER_ROLLOVER)"
        case (.live, .operatorAttested):
            return "운영자 수기 입력 · 서버 검증 차단 (OPERATOR_ATTESTED)"
        case (.mock, .exchangeObservedDayStart), (.mock, .priorDayVerifiedLedgerRollover), (.live, .durableMockVenueSeed):
            return "현재 계좌 모드에서 사용할 수 없는 출처"
        }
    }

    private func baselineSourceNote(for mode: AutomationMode) -> String {
        switch mode {
        case .mock:
            return "선택한 UTC 날짜는 모의 원장 확인 대상일입니다. 새 MOCK 시드의 기준 시각은 서버가 기록한 실제 생성 시각이며 자정 이후일 수 있습니다. 이 시각은 거래소 시각이 아닙니다. 잔액이나 당일 거래 활동이 시드와 맞지 않으면 차단 상태로 보존됩니다."
        case .live:
            return "LIVE 기준선의 현재 backend는 provider as-of 시각을 받지 않습니다. 로컬 잔액 조회·수신 시각은 거래소의 과거 기준 잔액 증거가 아니므로 providerless 입력은 BLOCKED_INSUFFICIENT_EVIDENCE로 보존되고 주문 승인이 차단됩니다."
        }
    }

    private func selectedBaselineSource(for mode: AutomationMode) throws -> AutomationBaselineSource {
        guard baselineSourceMode == mode else {
            throw AutomationSessionServiceError.invalidInput("선택한 세션 모드의 기준선 출처를 다시 불러오세요")
        }
        return try AutomationBaselineSource.selectableSource(at: baselineSourcePopup.indexOfSelectedItem, for: mode)
    }

    private func accountIdentityLabel(_ identity: String) -> String {
        privacyIsMasked() ? "••••" : identity
    }

    private func accountObservationClockLabel(_ source: AutomationDataSource) -> String {
        switch source {
        case .upbitLiveRead: return "서버 로컬 잔액 조회 시각(UTC)"
        case .upbitPublicFeed: return "시세 원천 시각(UTC)"
        case .durableMockVenue: return "모의 원천 로컬 시각(UTC)"
        }
    }

    private func baselineVerificationLabel(_ state: AutomationBaselineVerificationState) -> String {
        switch state {
        case .verified: return "검증 완료 (VERIFIED)"
        case .blockedInsufficientEvidence: return "증거 부족 차단 (BLOCKED_INSUFFICIENT_EVIDENCE)"
        }
    }

    private func admissionBlockReasonDescription(_ reason: String) -> String {
        switch reason {
        case "LIVE_TRADING_DISABLED": return "실거래 비활성화 (LIVE_TRADING_DISABLED)"
        case "LIVE_GRANT_INACTIVE": return "실거래 승인 비활성 (LIVE_GRANT_INACTIVE)"
        case "EMERGENCY_STOP_LATCHED": return "긴급 정지 작동 중 (EMERGENCY_STOP_LATCHED)"
        case "ORDER_ADMISSION_BLOCKED": return "전역 주문 승인 차단 (ORDER_ADMISSION_BLOCKED)"
        case "SESSION_NOT_RUNNING": return "세션 실행 중 아님 (SESSION_NOT_RUNNING)"
        case "UNRESOLVED_VENUE_ORDER_READBACK_REQUIRED": return "미확인 거래소 주문 재조회 필요 (UNRESOLVED_VENUE_ORDER_READBACK_REQUIRED)"
        case "DAILY_START_BASELINE_MISSING": return "일일 시작 기준선 미등록 (DAILY_START_BASELINE_MISSING)"
        case "DAILY_START_BASELINE_BLOCKED": return "일일 시작 기준선 증거 부족 차단 (DAILY_START_BASELINE_BLOCKED)"
        case "BASELINE:LIVE_PROVIDER_AS_OF_UNAVAILABLE":
            return "거래소가 과거 기준 시각 잔액을 제공하지 않아 LIVE 일일 손실 기준선을 현재 사용할 수 없음 (BASELINE:LIVE_PROVIDER_AS_OF_UNAVAILABLE)"
        case "DAILY_LOSS_UNKNOWN": return "일일 손실 미확인 (DAILY_LOSS_UNKNOWN)"
        case "DAILY_LOSS_LIMIT_REACHED": return "일일 최대 손실 한도 도달 (DAILY_LOSS_LIMIT_REACHED)"
        case "ASSET_EXPOSURE_UNKNOWN": return "자산 노출액 미확인 (ASSET_EXPOSURE_UNKNOWN)"
        case "OPERATOR_ATTESTED_BASELINE_REQUIRES_EXCHANGE_VERIFICATION":
            return "운영자 단독 입력은 거래소 자정 증거가 없어 차단됨 (OPERATOR_ATTESTED_BASELINE_REQUIRES_EXCHANGE_VERIFICATION)"
        case "LATE_LOCAL_BALANCE_RECEIPT_NOT_HISTORICAL_EVIDENCE":
            return "자정 이후 수신된 잔액 조회는 과거 00:00:00Z 증거로 불충분 (LATE_LOCAL_BALANCE_RECEIPT_NOT_HISTORICAL_EVIDENCE)"
        case "PRIOR_DAY_VERIFIED_BASELINE_MISSING":
            return "전일 검증 기준선 없음 (PRIOR_DAY_VERIFIED_BASELINE_MISSING)"
        default:
            return reason
        }
    }

    private func outcomeLabel(_ outcome: AutomationCommandOutcome) -> String {
        switch outcome {
        case .applied: return "적용됨"
        case .replayed: return "기존 결과 반환"
        case .noOp: return "변경 없음"
        case .pending: return "처리 중"
        case .denied: return "거부됨"
        }
    }

    private func lifecycleActionLabel(_ command: AutomationLifecycleCommand) -> String {
        switch command {
        case .pause: return "세션 일시정지"
        case .resume: return "세션 재개"
        case .stop: return "세션 중지"
        }
    }

    private static let utcFormatter: ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        return formatter
    }()

    private func utcString(_ date: Date) -> String {
        Self.utcFormatter.string(from: date)
    }

    private func publishSummary() {
        let summary: String
        if let selectedSession {
            summary = "\(modeLabel(selectedSession.mode)) · \(stateLabel(selectedSession.state)) · \(selectedSession.market) · 순번 \(selectedSession.sequence) · 체결 \(selectedSession.fill_count)"
        } else if let pendingRequestID {
            summary = "요청 결과 미확인 · \(pendingRequestID)"
        } else if pendingMetadataError != nil {
            summary = "이전 요청 정보 확인 필요"
        } else {
            summary = "조회 전"
        }
        onSummaryChanged("전략 자동화: \(summary)")
    }

    @objc private func handleManualRefresh() {
        reloadWorkspace()
    }

    @objc private func handleReadbackPendingRequest() {
        guard activeWriteTicket == nil, pendingRequestID != nil else { return }
        reloadWorkspace()
    }

    @objc private func handleLoadNextSessions() {
        guard let offset = sessionNextOffset, activeReadRequest == nil else { return }
        readGeneration &+= 1
        let ticket = readGeneration
        loadSessionPage(offset: offset, reset: false, ticket: ticket)
    }

    @objc private func handleLoadNextEvents() {
        guard let selectedSessionID, let eventNextSequence, activeReadRequest == nil else { return }
        readGeneration &+= 1
        let ticket = readGeneration
        loadEvents(sessionID: selectedSessionID, afterSequence: eventNextSequence, append: true, ticket: ticket)
    }

    @objc private func handleLoadNextLedger() {
        guard let selectedSession, let ledgerNextSequence, activeReadRequest == nil else { return }
        readGeneration &+= 1
        let ticket = readGeneration
        loadLedger(session: selectedSession, afterSequence: ledgerNextSequence, append: true, ticket: ticket)
    }

    @objc private func handlePolicySelectionChanged() {
        policyGeneration &+= 1
        let ticket = policyGeneration
        currentPolicy = selectedPolicy()
        currentPolicyDetail = nil
        selectedPolicyReference = nil
        revisionItems = []
        revisionPopup.removeAllItems()
        revisionPopup.addItem(withTitle: "동결된 정책 버전 선택")
        revisionPopup.selectItem(at: 0)
        guard let policy = currentPolicy else {
            policyReadInFlight = false
            updateControlEnablement()
            return
        }
        policyReadInFlight = true
        statusBanner.stringValue = "\(policy.name) 정책의 고정된 버전 정보를 확인하고 있습니다."
        statusBanner.textColor = TerminalTheme.secondaryText
        updateControlEnablement()
        policyReader.fetchDetail(for: policy) { [weak self] result in
            guard let self, ticket == self.policyGeneration, self.isVisibleWorkspace else { return }
            self.policyReadInFlight = false
            switch result {
            case .failure(let error):
                self.statusBanner.stringValue = "정책 버전 정보를 확인하지 못해 세션을 시작할 수 없습니다: \(error.localizedDescription)"
                self.statusBanner.textColor = TerminalTheme.negative
            case .success(let detail):
                self.currentPolicyDetail = detail
                self.revisionItems = detail.history
                self.revisionPopup.removeAllItems()
                self.revisionPopup.addItem(withTitle: "동결된 정책 버전 선택")
                for item in detail.history {
                    self.revisionPopup.addItem(withTitle: "r\(item.revisionNumber) · \(item.name) · \(String(item.reference.revisionId.prefix(12)))")
                    self.revisionPopup.lastItem?.toolTip = "policy_id: \(item.reference.policyId)\nrevision_id: \(item.reference.revisionId)\ndefinition_digest: \(item.reference.definitionDigest)\ncreated_at_utc: \(item.createdAt)"
                }
                self.revisionPopup.selectItem(at: 0)
                self.statusBanner.stringValue = "정책 버전 \(detail.history.count)개를 확인했습니다. 사용할 버전을 직접 선택하세요."
                self.statusBanner.textColor = TerminalTheme.secondaryText
            }
            self.updateControlEnablement()
        }
    }

    private func retryPolicyReadIfNeeded() {
        guard isVisibleWorkspace, !policyReadInFlight else { return }
        if currentPolicyDetail == nil {
            if currentPolicy == nil { currentPolicy = selectedPolicy() }
            guard currentPolicy != nil else { return }
            handlePolicySelectionChanged()
        } else if selectedPolicyReference == nil, revisionPopup.indexOfSelectedItem > 0 {
            handleRevisionSelectionChanged()
        }
    }

    @objc private func handleRevisionSelectionChanged() {
        guard let detail = currentPolicyDetail else { updateControlEnablement(); return }
        let index = revisionPopup.indexOfSelectedItem - 1
        guard revisionItems.indices.contains(index) else {
            policyGeneration &+= 1
            selectedPolicyReference = nil
            policyReadInFlight = false
            updateControlEnablement()
            return
        }
        policyGeneration &+= 1
        let ticket = policyGeneration
        selectedPolicyReference = nil
        let item = revisionItems[index]
        policyReadInFlight = true
        statusBanner.stringValue = "선택한 정책 버전의 정의 digest를 다시 확인하고 있습니다."
        statusBanner.textColor = TerminalTheme.secondaryText
        updateControlEnablement()
        policyReader.fetchRevision(item, within: detail) { [weak self] result in
            guard let self, ticket == self.policyGeneration, self.isVisibleWorkspace else { return }
            self.policyReadInFlight = false
            switch result {
            case .failure(let error):
                self.statusBanner.stringValue = "선택한 정책 버전을 확인하지 못했습니다: \(error.localizedDescription)"
                self.statusBanner.textColor = TerminalTheme.negative
            case .success(let verified):
                self.selectedPolicyReference = AutomationPolicyRevisionReference(
                    policy_id: verified.reference.policyId,
                    revision_id: verified.reference.revisionId,
                    definition_digest: verified.reference.definitionDigest
                )
                self.currentPolicyDetail = verified
                self.statusBanner.stringValue = "동결된 정책 버전을 확인했습니다 · policy_id \(verified.reference.policyId) · revision_id \(verified.reference.revisionId) · digest \(verified.reference.definitionDigest)"
                self.statusBanner.textColor = TerminalTheme.secondaryText
            }
            self.updateControlEnablement()
        }
    }

    @objc private func handleStartSession() {
        guard isVisibleWorkspace,
              pendingRequestID == nil,
              pendingMetadataError == nil,
              activeWriteTicket == nil,
              activeReadRequest == nil,
              !policyReadInFlight else { return }
        guard let policyReference = selectedPolicyReference,
              let interval = intervalPopup.selectedItem?.title,
              Self.decisionIntervals.contains(interval) else {
            statusBanner.stringValue = "확인된 정책 버전과 판단 주기를 선택하세요."
            statusBanner.textColor = TerminalTheme.negative
            return
        }
        let market = marketField.stringValue.trimmingCharacters(in: .whitespacesAndNewlines)
        let mode = selectedStartMode
        let context = service.currentTransportContext()
        do {
            let risk = try selectedRiskLimits(for: mode)
            guard !market.isEmpty else {
                throw AutomationSessionServiceError.invalidInput("market")
            }
            if mode == .live {
                guard let context,
                      confirmLiveStart(mode: mode, policyReference: policyReference, market: market, interval: interval, risk: risk),
                      service.isCurrentTransportContext(context),
                      isVisibleWorkspace,
                      selectedStartMode == mode,
                      selectedPolicyReference == policyReference,
                      marketField.stringValue.trimmingCharacters(in: .whitespacesAndNewlines) == market,
                      intervalPopup.selectedItem?.title == interval,
                      (try? selectedRiskLimits(for: mode)) == risk,
                      pendingRequestID == nil,
                      pendingMetadataError == nil,
                      activeWriteTicket == nil else { return }
            }
            let requestID = newRequestID()
            let pendingWrite = AutomationPendingWrite(
                request_id: requestID,
                action: .start,
                policy_ref: policyReference,
                market: market,
                decision_interval: interval,
                risk: risk,
                mode: mode
            )
            let actionName = mode == .live ? "LIVE 전략 세션 시작" : "MOCK 전략 세션 시작"
            beginWrite(pendingWrite: pendingWrite, actionName: actionName) { requestID, completion in
                try service.startSession(
                    requestID: requestID,
                    policyReference: policyReference,
                    market: market,
                    decisionInterval: interval,
                    risk: risk,
                    mode: mode,
                    expectedContext: context,
                    completion: completion
                )
            }
        } catch {
            statusBanner.stringValue = "위험 한도 입력을 확인하세요: \(userFacingError(error))"
            statusBanner.textColor = TerminalTheme.negative
        }
    }

    @objc private func handlePause() { performTransition(.pause) }
    @objc private func handleResume() { performTransition(.resume) }
    @objc private func handleStop() { performTransition(.stop) }

    private func performTransition(_ command: AutomationLifecycleCommand) {
        guard isVisibleWorkspace,
              pendingRequestID == nil,
              pendingMetadataError == nil,
              activeWriteTicket == nil,
              activeReadRequest == nil,
              let context = service.currentTransportContext(),
              let session = selectedSession,
              hasFreshAccountProjection(for: session),
              let capabilities = session.account_risk?.capabilities else { return }
        let permitted: Bool
        switch command {
        case .pause:
            permitted = session.state == .running && capabilities.can_pause
        case .resume:
            permitted = session.state == .paused && capabilities.can_resume
        case .stop:
            permitted = session.state != .stopped && capabilities.can_stop
        }
        guard permitted,
              session.mode == .live || (
                capabilities.live_effects_enabled == false
                    && session.account_risk?.risk.live_grant_active == false
              ) else { return }

        if command == .resume, session.mode == .live {
            guard confirmLiveResume(session: session),
                  service.isCurrentTransportContext(context),
                  isVisibleWorkspace,
                  selectedSession?.session_id == session.session_id,
                  selectedSession?.account_identity == session.account_identity,
                  selectedSession?.mode == session.mode,
                  selectedSession?.sequence == session.sequence,
                  selectedSession?.account_risk?.capabilities.can_resume == true,
                  hasFreshAccountProjection(for: session),
                  pendingRequestID == nil,
                  pendingMetadataError == nil,
                  activeWriteTicket == nil,
                  activeReadRequest == nil else { return }
        }
        let requestID = newRequestID()
        let pendingWrite = AutomationPendingWrite(
            request_id: requestID,
            action: command.receiptAction,
            session_id: session.session_id,
            expected_sequence: session.sequence,
            mode: session.mode,
            account_identity: session.account_identity
        )
        beginWrite(pendingWrite: pendingWrite, actionName: lifecycleActionLabel(command)) { requestID, completion in
            try service.transition(
                command: command,
                requestID: requestID,
                sessionID: session.session_id,
                expectedSequence: session.sequence,
                expectedMode: session.mode,
                expectedAccountIdentity: session.account_identity,
                expectedContext: context,
                completion: completion
            )
        }
    }

    private func confirmLiveStart(
        mode: AutomationMode,
        policyReference: AutomationPolicyRevisionReference,
        market: String,
        interval: String,
        risk: AutomationRiskLimits
    ) -> Bool {
        guard window != nil else {
            statusBanner.stringValue = "LIVE 시작 검토 창을 열 수 없어 요청을 보내지 않았습니다."
            statusBanner.textColor = TerminalTheme.negative
            return false
        }
        let alert = NSAlert()
        alert.alertStyle = .critical
        alert.messageText = "LIVE 전략 세션 시작 요청을 검토하세요"
        alert.informativeText = [
            "모드: \(mode.rawValue)",
            "계좌: 서버가 선택하는 LIVE Upbit 계좌 · 계좌 fingerprint는 durable 응답에서 검증합니다",
            "정책 ID: \(policyReference.policy_id)",
            "정책 revision ID: \(policyReference.revision_id)",
            "정책 definition digest: \(policyReference.definition_digest)",
            "시장 / 판단 주기: \(market) / \(interval)",
            "동결 위험 한도(KRW): 주문당 \(risk.max_order_notional_krw) · 하루 주문액 \(risk.max_daily_notional_krw) · 자산 노출 \(risk.max_asset_exposure_krw) · UTC 일일 손실 \(risk.max_daily_loss_krw)",
            "LIVE 활성화 손실 한도: \(risk.activation_loss_limit_krw ?? "미제공") KRW · 미체결 주문 최대 \(risk.max_open_orders) · 주기당 주문 최대 \(risk.max_orders_per_interval)",
            "최대 잔액 경과: \(risk.max_balance_age_seconds)초 · 최대 시세 경과: \(risk.max_feed_age_seconds)초",
            "주문 전송 게이트는 별도 설정입니다. 이 전략 요청은 주문 전송 게이트를 변경하지 않습니다. 서버가 최신 권한과 기능을 다시 확인합니다."
        ].joined(separator: "\n")
        alert.addButton(withTitle: "LIVE 전략 시작 요청")
        alert.addButton(withTitle: "취소")
        return alert.runModal() == .alertFirstButtonReturn
    }

    private func confirmLiveResume(session: AutomationSessionView) -> Bool {
        guard window != nil,
              let capabilities = session.account_risk?.capabilities else {
            statusBanner.stringValue = "LIVE 재개에 필요한 최신 계좌·기능 정보가 없어 요청을 보내지 않았습니다."
            statusBanner.textColor = TerminalTheme.negative
            return false
        }
        let alert = NSAlert()
        alert.alertStyle = .critical
        alert.messageText = "LIVE 전략 세션 재개 요청을 검토하세요"
        alert.informativeText = [
            "모드: LIVE · 계좌: \(accountIdentityLabel(session.account_identity))",
            "세션 ID: \(session.session_id) · 예상 순번: \(session.sequence)",
            "정책 ID: \(session.policy_ref.policy_id)",
            "정책 revision ID: \(session.policy_ref.revision_id)",
            "정책 definition digest: \(session.policy_ref.definition_digest)",
            "시장 / 판단 주기: \(session.market) / \(session.decision_interval)",
            "동결 위험 한도(KRW): 주문당 \(session.risk.max_order_notional_krw) · 하루 주문액 \(session.risk.max_daily_notional_krw) · 자산 노출 \(session.risk.max_asset_exposure_krw) · UTC 일일 손실 \(session.risk.max_daily_loss_krw)",
            "LIVE 활성화 손실 한도: \(session.risk.activation_loss_limit_krw ?? "미제공") KRW · 미체결 주문 최대 \(session.risk.max_open_orders) · 주기당 주문 최대 \(session.risk.max_orders_per_interval)",
            "최대 잔액 경과: \(session.risk.max_balance_age_seconds)초 · 최대 시세 경과: \(session.risk.max_feed_age_seconds)초",
            "최근 서버 기능: 재개 \(yesNo(capabilities.can_resume)) · 실주문 전송 \(yesNo(capabilities.live_effects_enabled)) · 별도 승인 \(capabilities.explicit_approval_required ? "필요" : "불필요")",
            "서버는 재개 요청을 다시 검증합니다. 주문 전송 게이트 상태는 이 요청으로 바뀌지 않습니다."
        ].joined(separator: "\n")
        alert.addButton(withTitle: "LIVE 세션 재개 요청")
        alert.addButton(withTitle: "취소")
        return alert.runModal() == .alertFirstButtonReturn
    }

    @objc private func handleCancelSelectedOrder() {
        guard let session = selectedSession, session.mode == .mock,
              session.account_risk?.capabilities.can_cancel_order == true,
              session.account_risk?.capabilities.live_effects_enabled == false,
              session.account_risk?.risk.live_grant_active == false,
              let order = selectedOwnedOrder(), order.market == session.market,
              order.state == .open || order.state == .partiallyFilled else {
            statusBanner.stringValue = "서버가 취소를 허용한 모의 세션에서 선택한 OPEN(미체결) 또는 PARTIALLY_FILLED(일부 체결) 주문만 취소할 수 있습니다."
            statusBanner.textColor = TerminalTheme.negative
            return
        }
        let requestID = newRequestID()
        let pendingWrite = AutomationPendingWrite(
            request_id: requestID,
            action: .cancelOrder,
            session_id: session.session_id,
            expected_sequence: session.sequence,
            client_identifier: order.client_identifier,
            mode: session.mode,
            account_identity: session.account_identity
        )
        beginWrite(pendingWrite: pendingWrite, actionName: "선택한 모의 주문 취소 · \(order.client_identifier)") { requestID, completion in
            try service.cancelOwnedMockOrder(
                requestID: requestID,
                sessionID: session.session_id,
                expectedSequence: session.sequence,
                clientIdentifier: order.client_identifier,
                completion: completion
            )
        }
    }

    @objc private func handleQueryBaseline() {
        guard isVisibleWorkspace, let session = selectedSession, activeReadRequest == nil else { return }
        let dayText = baselineDayField.stringValue.trimmingCharacters(in: .whitespacesAndNewlines)
        let dayUTC = dayText.isEmpty ? nil : dayText
        readGeneration &+= 1
        let ticket = readGeneration
        beginRead(ticket: ticket, operation: { completion in
            service.getBaselineStatus(sessionID: session.session_id, dayUTC: dayUTC, completion: completion)
        }) { [weak self] (result: Result<AutomationAccountDayBaselineStatus, Error>) in
            guard let self,
                  self.isVisibleWorkspace,
                  self.selectedSession?.session_id == session.session_id,
                  self.baselineDayField.stringValue.trimmingCharacters(in: .whitespacesAndNewlines) == dayText else { return }
            switch result {
            case .failure(let error):
                self.recordReadFailure(error)
            case .success(let status):
                self.queriedBaselineStatus = status
                self.lastSuccessfulReadAt = Date()
                self.lastReadFailure = nil
                self.renderBaselineQueryStatus(status, masked: self.privacyIsMasked())
            }
        }
    }

    @objc private func handleRecordBaseline() {
        guard isVisibleWorkspace, let session = selectedSession, session.state != .faulted else {
            statusBanner.stringValue = "오류(FAULTED) 상태가 아닌 세션을 선택하세요."
            statusBanner.textColor = TerminalTheme.negative
            return
        }
        let dayUTC = baselineDayField.stringValue.trimmingCharacters(in: .whitespacesAndNewlines)
        let quoteFree = baselineQuoteFreeField.stringValue.trimmingCharacters(in: .whitespacesAndNewlines)
        let quoteLocked = baselineQuoteLockedField.stringValue.trimmingCharacters(in: .whitespacesAndNewlines)
        let baseCurrency = baselineBaseCurrencyField.stringValue.trimmingCharacters(in: .whitespacesAndNewlines).uppercased()
        let baseFree = baselineBaseFreeField.stringValue.trimmingCharacters(in: .whitespacesAndNewlines)
        let baseLocked = baselineBaseLockedField.stringValue.trimmingCharacters(in: .whitespacesAndNewlines)
        let basePrice = baselineBasePriceField.stringValue.trimmingCharacters(in: .whitespacesAndNewlines)
        let noteText = baselineNoteField.stringValue.trimmingCharacters(in: .whitespacesAndNewlines)

        let source: AutomationBaselineSource
        do {
            source = try selectedBaselineSource(for: session.mode)
        } catch {
            statusBanner.stringValue = "일일 시작 기준선 출처 선택 오류: \(error.localizedDescription)"
            statusBanner.textColor = TerminalTheme.negative
            return
        }

        var assets: [AutomationBaselineAssetInput] = [
            AutomationBaselineAssetInput(
                currency: "KRW",
                free: quoteFree,
                locked: quoteLocked.isEmpty ? "0" : quoteLocked,
                valuation_basis: .quoteCurrencyParity,
                mark_price_krw: nil
            )
        ]
        if !baseCurrency.isEmpty && baseCurrency != "KRW" {
            let fVal = Decimal(string: baseFree.isEmpty ? "0" : baseFree) ?? .zero
            let lVal = Decimal(string: baseLocked.isEmpty ? "0" : baseLocked) ?? .zero
            let basis: AutomationAssetValuationBasis
            let markPrice: String?
            if fVal + lVal == .zero {
                basis = .zeroBalance
                markPrice = nil
            } else if baselineValuationBasisPopup.indexOfSelectedItem == 0 {
                basis = .finalizedCandleClose
                markPrice = basePrice.isEmpty ? nil : basePrice
            } else {
                basis = .operatorAttestedPrice
                markPrice = basePrice.isEmpty ? nil : basePrice
            }
            assets.append(
                AutomationBaselineAssetInput(
                    currency: baseCurrency,
                    free: baseFree.isEmpty ? "0" : baseFree,
                    locked: baseLocked.isEmpty ? "0" : baseLocked,
                    valuation_basis: basis,
                    mark_price_krw: markPrice
                )
            )
        }

        let input = AutomationAccountDayBaselineInput(
            account_identity: session.account_identity,
            day_utc: dayUTC,
            reference_time_utc: "\(dayUTC)T00:00:00Z",
            source: source,
            assets: assets,
            operator_note: noteText.isEmpty ? nil : noteText
        )
        do {
            try input.validate(for: session.mode)
            confirmBaselineWrite(session: session, input: input)
        } catch {
            statusBanner.stringValue = "일일 시작 기준선 입력 오류: \(error.localizedDescription)"
            statusBanner.textColor = TerminalTheme.negative
        }
    }

    private func confirmBaselineWrite(session: AutomationSessionView, input: AutomationAccountDayBaselineInput) {
        guard let window else {
            statusBanner.stringValue = "기준선 기록 전 확인 창을 열 수 없어 요청을 보내지 않았습니다."
            statusBanner.textColor = TerminalTheme.negative
            return
        }
        let evidenceWarning = session.mode == .live
            ? "LIVE의 선택 날짜에는 거래소가 보유 잔액을 과거 시각 기준으로 제공하지 않습니다. 로컬 조회·수신 시각은 이를 대신하지 않으며 증거가 없으면 서버가 차단 상태로 보존합니다."
            : (input.source == .durableMockVenueSeed
                ? "선택한 UTC 날짜는 모의 원장을 확인할 날짜입니다. 서버는 새 시드의 실제 생성 시각(자정 이후일 수 있음)을 기준으로 잔액과 당일 활동을 대조합니다. 이 시각은 UTC 자정이나 거래소 시각이 아니며 기록 시각은 요청을 저장한 시각입니다."
                : "운영자 입력은 독립 증거가 아니므로 서버가 BLOCKED_INSUFFICIENT_EVIDENCE로 보존합니다.")
        let alert = NSAlert()
        alert.alertStyle = .warning
        alert.messageText = "이 계좌와 UTC 날짜의 기준선 슬롯은 한 번만 기록할 수 있습니다."
        alert.informativeText = "계좌: \(accountIdentityLabel(session.account_identity)) · 모드: \(modeLabel(session.mode)) · 날짜(UTC): \(input.day_utc) · 출처: \(baselineSourceLabel(input.source))\n\n\(evidenceWarning)\n\n기록은 검증 차단 상태여도 유지되며 이후 주문 승인을 막습니다. 이 화면에는 덮어쓰기나 강제 초기화 기능이 없습니다. 입력한 정확한 잔액과 출처를 확인하세요."
        alert.addButton(withTitle: "확인하고 기준선 기록")
        alert.addButton(withTitle: "취소")
        alert.beginSheetModal(for: window) { [weak self] response in
            guard response == .alertFirstButtonReturn,
                  let self,
                  self.isVisibleWorkspace,
                  let currentSession = self.selectedSession,
                  currentSession.session_id == session.session_id,
                  currentSession.account_identity == session.account_identity,
                  currentSession.mode == session.mode,
                  currentSession.sequence == session.sequence,
                  self.baselineDayField.stringValue.trimmingCharacters(in: .whitespacesAndNewlines) == input.day_utc else { return }
            self.submitBaselineWrite(session: currentSession, input: input)
        }
    }

    private func submitBaselineWrite(session: AutomationSessionView, input: AutomationAccountDayBaselineInput) {
        let requestID = newRequestID()
        let pendingWrite = AutomationPendingWrite(
            request_id: requestID,
            action: .recordBaseline,
            session_id: session.session_id,
            expected_sequence: session.sequence,
            baseline_day_utc: input.day_utc,
            baseline_source: input.source,
            mode: session.mode,
            account_identity: session.account_identity
        )
        beginWrite(pendingWrite: pendingWrite, actionName: "일일 시작 기준선 기록 · \(input.day_utc)") { requestID, completion in
            try service.recordBaseline(
                requestID: requestID,
                sessionID: session.session_id,
                expectedSequence: session.sequence,
                mode: session.mode,
                baseline: input,
                completion: completion
            )
        }
    }

    @objc private func togglePrivacyMask() {
        onTogglePrivacyMask()
    }

    @objc private func handleSessionSelectionChanged() {
        let row = sessionTable.selectedRow
        guard sessions.indices.contains(row) else { return }
        let session = sessions[row]
        guard selectedSessionID != session.session_id else { return }
        closeReviewWindow()
        closeDecisionAnalysisWindow()
        selectedSessionID = session.session_id
        selectedSession = session
        projectionMissingForSelectedSession = session.account_risk == nil
        if sessionListReadFailure != nil {
            selectedProjectionReadAfterListFailure = false
            selectedProjectionReadSessionID = session.session_id
            selectedProjectionReadStatus = "선택 세션 조회 중 · 최신 계좌·위험 투영은 아직 확인되지 않았습니다."
        }
        queriedBaselineStatus = nil
        baselineQueryResultLabel.stringValue = "세션이 변경되어 기준선 상태를 다시 조회해야 합니다."
        baselineDayField.stringValue = ""
        baselineQuoteFreeField.stringValue = ""
        baselineQuoteLockedField.stringValue = "0"
        baselineBaseCurrencyField.stringValue = ""
        baselineBaseFreeField.stringValue = "0"
        baselineBaseLockedField.stringValue = "0"
        baselineBasePriceField.stringValue = ""
        baselineNoteField.stringValue = ""
        events = []
        eventNextSequence = nil
        ledger = nil
        ledgerNextSequence = nil
        selectedOrderClientIdentifier = nil
        renderWorkspace()
        guard isVisibleWorkspace else { return }
        readGeneration &+= 1
        activeReadRequest?.cancel()
        activeReadRequest = nil
        loadSelectedSession(session.session_id, ticket: readGeneration)
    }

    @objc private func handleOpenReview() {
        guard isVisibleWorkspace, let session = selectedSession else { return }
        if reviewWindowSessionID == session.session_id, let reviewWindowController {
            reviewWindowController.showReview()
            return
        }
        closeReviewWindow()
        let controller = AutomationReviewWindowController(
            session: session,
            service: service,
            privacyIsMasked: privacyIsMasked
        )
        controller.onClose = { [weak self, weak controller] in
            guard let self, self.reviewWindowController === controller else { return }
            self.reviewWindowController = nil
            self.reviewWindowSessionID = nil
        }
        reviewWindowController = controller
        reviewWindowSessionID = session.session_id
        controller.showReview()
    }

    @objc private func handleOpenDecisionAnalysis() {
        guard isVisibleWorkspace, let session = selectedSession else { return }
        if decisionAnalysisWindowSessionID == session.session_id,
           let decisionAnalysisWindowController {
            decisionAnalysisWindowController.showAnalysis(for: .automation(session))
            return
        }
        closeDecisionAnalysisWindow()
        let controller = DecisionAnalysisWindowController(privacyIsMasked: privacyIsMasked)
        controller.onClose = { [weak self, weak controller] in
            guard let self, self.decisionAnalysisWindowController === controller else { return }
            self.decisionAnalysisWindowController = nil
            self.decisionAnalysisWindowSessionID = nil
        }
        decisionAnalysisWindowController = controller
        decisionAnalysisWindowSessionID = session.session_id
        controller.showAnalysis(for: .automation(session))
    }

    private func closeReviewWindow() {
        let controller = reviewWindowController
        reviewWindowController = nil
        reviewWindowSessionID = nil
        controller?.close()
    }

    private func closeDecisionAnalysisWindow() {
        let controller = decisionAnalysisWindowController
        decisionAnalysisWindowController = nil
        decisionAnalysisWindowSessionID = nil
        controller?.close()
    }

    func controlTextDidChange(_ notification: Notification) {
        if let field = notification.object as? NSTextField, field === baselineDayField {
            queriedBaselineStatus = nil
            baselineQueryResultLabel.stringValue = "기준일이 변경되었습니다. 서버 상태를 다시 조회하세요."
        }
        updateControlEnablement()
    }

    func numberOfRows(in tableView: NSTableView) -> Int {
        if tableView === sessionTable { return sessions.count }
        if tableView === orderTable { return ledger?.orders.count ?? 0 }
        if tableView === fillTable { return ledger?.fills.count ?? 0 }
        if tableView === eventTable { return events.count }
        return 0
    }

    func tableView(_ tableView: NSTableView, viewFor tableColumn: NSTableColumn?, row: Int) -> NSView? {
        guard let identifier = tableColumn?.identifier.rawValue else { return nil }
        var value = ""
        var tooltip = ""
        if tableView === sessionTable, sessions.indices.contains(row) {
            let item = sessions[row]
            tooltip = "session_id: \(item.session_id)\naccount_identity scope: \(AutomationAccountIdentityScope.presentation(for: item.mode))\npolicy_id: \(item.policy_ref.policy_id)\nrevision_id: \(item.policy_ref.revision_id)\ndigest: \(item.policy_ref.definition_digest)\nmarket: \(item.market)\ninterval: \(item.decision_interval)\nsequence: \(item.sequence)"
            let selectedProjectionStatus = selectedProjectionReadSessionID == item.session_id
                ? selectedProjectionReadStatus : nil
            let currentRisk: AutomationAccountRiskProjection?
            let unavailableReason: String?
            if let selectedProjectionStatus {
                currentRisk = nil
                unavailableReason = selectedProjectionStatus
            } else if selectedSessionID == item.session_id,
                      let session = selectedSession, session.session_id == item.session_id {
                if sessionListReadFailure != nil && !selectedProjectionReadAfterListFailure {
                    currentRisk = nil
                    unavailableReason = "더 최근의 세션 목록 조회가 실패했습니다. 그 뒤 선택 세션을 성공적으로 다시 읽기 전까지 현재 projection은 미확인입니다."
                } else if projectionMissingForSelectedSession {
                    currentRisk = nil
                    unavailableReason = "최신 선택 세션 응답에 계좌·위험 projection이 없습니다."
                } else if let risk = session.account_risk {
                    currentRisk = risk
                    unavailableReason = nil
                } else {
                    currentRisk = nil
                    unavailableReason = "최신 선택 세션 응답의 계좌·위험 projection을 확인할 수 없습니다."
                }
            } else if let sessionListReadFailure {
                currentRisk = nil
                unavailableReason = "세션 목록 조회 실패 · \(sessionListReadFailure)"
            } else if let risk = item.account_risk {
                currentRisk = risk
                unavailableReason = nil
            } else {
                currentRisk = nil
                unavailableReason = "최신 세션 목록 응답에 계좌·위험 projection이 없습니다."
            }

            if let risk = currentRisk {
                let reasons = risk.admission_block_reasons
                let admission = reasons.isEmpty
                    ? (risk.capabilities.can_submit_orders ? "차단 사유 없음" : "서버 불허 · 사유 미제공")
                    : "차단 \(reasons.count)건"
                let receiveTime = accountReceivedAtBySession[item.session_id]
                let receiveAge = receiveTime.map { Date().timeIntervalSince($0) }
                let isSelectedSessionResponse = selectedSession?.session_id == item.session_id
                    && selectedProjectionStatus == nil
                    && !projectionMissingForSelectedSession
                let qualifier: String
                if let receiveAge, receiveAge >= 0, receiveAge <= 30 {
                    qualifier = isSelectedSessionResponse
                        ? "현재 선택 세션 응답 · 앱 수신 30초 이내"
                        : "목록 응답 · 앱 수신 30초 이내"
                } else if let receiveAge, receiveAge < 0 {
                    qualifier = "미래 앱 수신 시각 · 신선도 미확인"
                } else if receiveTime != nil {
                    qualifier = "마지막 응답"
                } else {
                    qualifier = "응답 시각 미확인"
                }
                value = identifier == "admission" ? "\(qualifier) · \(admission)" : value
                tooltip += "\nprojection_source: \(isSelectedSessionResponse ? "selected get_session" : "session list row")\nadmission_state: \(admission)\ncan_submit_orders: \(risk.capabilities.can_submit_orders)\nadmission_block_reasons: \(reasons.isEmpty ? "없음" : reasons.joined(separator: " · "))"
                tooltip += "\napp_received_at_utc: \(receiveTime.map(utcString) ?? "미확인")"
            } else {
                let lastKnownRisk = accountProjectionBySession[item.session_id]
                let visibleReason: String
                if selectedProjectionStatus?.contains("조회 중") == true {
                    visibleReason = "조회 중 · 최신값 미확인"
                } else if selectedProjectionStatus != nil {
                    visibleReason = "조회 실패 · 최신값 미확인"
                } else if selectedSessionID == item.session_id,
                          selectedSession?.session_id == item.session_id,
                          sessionListReadFailure != nil && !selectedProjectionReadAfterListFailure {
                    visibleReason = "목록 조회 실패 · 최신값 미확인"
                } else if selectedSessionID == item.session_id,
                          selectedSession?.session_id == item.session_id,
                          projectionMissingForSelectedSession {
                    visibleReason = "응답 projection 없음 · 미확인"
                } else if sessionListReadFailure != nil {
                    visibleReason = "목록 조회 실패 · 최신값 미확인"
                } else {
                    visibleReason = "응답 projection 없음 · 미확인"
                }
                if identifier == "admission" {
                    value = lastKnownRisk == nil ? visibleReason : "\(visibleReason) · 마지막값 보유"
                }
                tooltip += "\ncurrent_admission_state: 미확인\nprojection_unavailable_reason: \(unavailableReason ?? "최신 투영 없음")"
                if let lastKnownRisk {
                    let reasons = lastKnownRisk.admission_block_reasons
                    let lastKnownAdmission = reasons.isEmpty
                        ? (lastKnownRisk.capabilities.can_submit_orders ? "차단 사유 없음" : "서버 불허 · 사유 미제공")
                        : "차단 \(reasons.count)건"
                    tooltip += "\nlast_known_admission_state: \(lastKnownAdmission) · 역사적 값이며 현재 승인이 아닙니다."
                    tooltip += "\nlast_known_block_reasons: \(reasons.isEmpty ? "없음" : reasons.joined(separator: " · "))"
                    tooltip += "\nlast_known_app_received_at_utc: \(accountReceivedAtBySession[item.session_id].map(utcString) ?? "미확인")"
                } else {
                    tooltip += "\nlast_known_admission_state: 없음"
                }
            }
            switch identifier {
            case "session_id": value = shortened(item.session_id)
            case "mode_state": value = "\(modeLabel(item.mode)) / \(stateLabel(item.state)) (\(item.state.rawValue))"
            case "market_interval": value = "\(item.market) / \(item.decision_interval)"
            case "sequence": value = String(item.sequence)
            case "orders": value = String(item.unresolved_order_count)
            case "fills": value = String(item.fill_count)
            default: break
            }
        } else if tableView === orderTable, let orders = ledger?.orders, orders.indices.contains(row) {
            let item = orders[row]
            tooltip = "client_identifier: \(item.client_identifier)\nintent_id: \(item.intent_id)\nvenue_order_uuid: \(item.venue_order_uuid ?? "미확인")\nstate: \(item.state.rawValue)\nrequested_notional_krw: \(amount(item.requested_notional_krw, masked: privacyIsMasked()))\nreserved_fee_krw: \(amount(item.reserved_fee_krw, masked: privacyIsMasked()))\nlocked_quote_krw: \(amount(item.locked_quote_krw, masked: privacyIsMasked()))\nlocked_base: \(amount(item.locked_base, masked: privacyIsMasked()))\nexecuted_volume: \(amount(item.executed_volume, masked: privacyIsMasked()))\npaid_fee_krw: \(amount(item.paid_fee_krw, masked: privacyIsMasked()))"
            switch identifier {
            case "client_identifier": value = shortened(item.client_identifier)
            case "state": value = "\(orderStateLabel(item.state)) (\(item.state.rawValue))"
            case "side": value = sideLabel(item.side)
            case "notional": value = amount(item.requested_notional_krw, masked: privacyIsMasked())
            case "reserved_fee": value = amount(item.reserved_fee_krw, masked: privacyIsMasked())
            case "locked_quote": value = amount(item.locked_quote_krw, masked: privacyIsMasked())
            case "locked_base": value = amount(item.locked_base, masked: privacyIsMasked())
            case "executed": value = amount(item.executed_volume, masked: privacyIsMasked())
            case "paid_fee": value = amount(item.paid_fee_krw, masked: privacyIsMasked())
            case "trades": value = String(item.trade_count)
            case "updated": value = item.updated_at_utc
            default: break
            }
        } else if tableView === fillTable, let fills = ledger?.fills, fills.indices.contains(row) {
            let item = fills[row]
            tooltip = "trade_uuid: \(item.trade_uuid)\nclient_identifier: \(item.client_identifier)\nmarket: \(item.market)\nside: \(item.side.rawValue)\nprice_krw: \(amount(item.price_krw, masked: privacyIsMasked()))\nvolume: \(amount(item.volume, masked: privacyIsMasked()))\nfunds_krw: \(amount(item.funds_krw, masked: privacyIsMasked()))\nobserved_at_utc: \(item.observed_at_utc)"
            switch identifier {
            case "trade_uuid": value = shortened(item.trade_uuid)
            case "client_identifier": value = shortened(item.client_identifier)
            case "market": value = item.market
            case "side": value = sideLabel(item.side)
            case "price": value = amount(item.price_krw, masked: privacyIsMasked())
            case "volume": value = amount(item.volume, masked: privacyIsMasked())
            case "funds": value = amount(item.funds_krw, masked: privacyIsMasked())
            case "observed": value = item.observed_at_utc
            default: break
            }
        } else if tableView === eventTable, events.indices.contains(row) {
            let item = events[row]
            let detail = item.detail.redactedForDisplay(hidingMonetaryValues: privacyIsMasked()).formattedJSON
            tooltip = "session_id: \(item.session_id)\nsequence: \(item.sequence)\nkind: \(item.kind.rawValue)\nat_utc: \(item.at_utc)\n\(detail)"
            switch identifier {
            case "sequence": value = String(item.sequence)
            case "kind": value = eventKindLabel(item.kind)
            case "at": value = item.at_utc
            case "detail": value = detail.replacingOccurrences(of: "\n", with: " ")
            default: break
            }
        }
        let cell = NSTextField(labelWithString: value)
        cell.font = TerminalTheme.table
        cell.lineBreakMode = .byTruncatingMiddle
        cell.toolTip = tooltip
        cell.setAccessibilityLabel("\(tableColumn?.title ?? "") · \(value)")
        cell.setAccessibilityHelp(tooltip)
        if ["sequence", "orders", "fills", "notional", "reserved_fee", "locked_quote", "locked_base", "executed", "paid_fee", "trades", "price", "volume", "funds"].contains(identifier) {
            cell.alignment = .right
        }
        return cell
    }

    func tableViewSelectionDidChange(_ notification: Notification) {
        guard let table = notification.object as? NSTableView else { return }
        if table === sessionTable {
            handleSessionSelectionChanged()
        } else if table === orderTable {
            let row = table.selectedRow
            selectedOrderClientIdentifier = ledger?.orders.indices.contains(row) == true
                ? ledger?.orders[row].client_identifier
                : nil
            updateControlEnablement()
        }
    }

    private func shortened(_ value: String) -> String {
        value.count > 28 ? "\(value.prefix(12))…\(value.suffix(10))" : value
    }

    private func newRequestID() -> String {
        "automation-\(UUID().uuidString.lowercased())"
    }
}
