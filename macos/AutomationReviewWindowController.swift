import AppKit
import Foundation

private extension AutomationJSONValue {
    var objectValue: [String: AutomationJSONValue]? {
        guard case .object(let value) = self else { return nil }
        return value
    }

    var arrayValue: [AutomationJSONValue]? {
        guard case .array(let value) = self else { return nil }
        return value
    }

    var stringValue: String? {
        guard case .string(let value) = self else { return nil }
        return value
    }
}

private struct AutomationReviewChartCandle {
    let open: Decimal
    let high: Decimal
    let low: Decimal
    let close: Decimal
}

private final class AutomationReviewCandleChartView: NSView {
    var candles: [AutomationReviewChartCandle] = [] { didSet { needsDisplay = true } }
    var isMasked = true { didSet { needsDisplay = true } }

    override var wantsDefaultClipping: Bool { true }

    override func draw(_ dirtyRect: NSRect) {
        TerminalTheme.chart.setFill()
        bounds.fill()
        guard !isMasked else {
            drawCentered("개인정보 숨김")
            return
        }
        guard !candles.isEmpty else {
            drawCentered("표시할 당시 캔들 정보가 없습니다.")
            return
        }

        let lower = candles.map(\.low).min() ?? .zero
        let upper = candles.map(\.high).max() ?? .zero
        let span = upper - lower
        guard span > 0 else {
            drawCentered("캔들 범위를 계산할 수 없습니다.")
            return
        }

        let plot = bounds.insetBy(dx: 12, dy: 12)
        let widthPerCandle = plot.width / CGFloat(candles.count)
        let bodyWidth = max(2, min(12, widthPerCandle * 0.62))
        func y(_ value: Decimal) -> CGFloat {
            let ratio = NSDecimalNumber(decimal: (value - lower) / span).doubleValue
            return plot.minY + plot.height * CGFloat(ratio)
        }

        for (index, candle) in candles.enumerated() {
            let x = plot.minX + (CGFloat(index) + 0.5) * widthPerCandle
            let rising = candle.close >= candle.open
            let color = rising ? TerminalTheme.positive : TerminalTheme.negative
            color.setStroke()
            let wick = NSBezierPath()
            wick.lineWidth = 1
            wick.move(to: NSPoint(x: x, y: y(candle.low)))
            wick.line(to: NSPoint(x: x, y: y(candle.high)))
            wick.stroke()

            let openY = y(candle.open)
            let closeY = y(candle.close)
            let body = NSRect(
                x: x - bodyWidth / 2,
                y: min(openY, closeY),
                width: bodyWidth,
                height: max(2, abs(closeY - openY))
            )
            color.setFill()
            body.fill()
        }
    }

    override func viewDidChangeEffectiveAppearance() {
        super.viewDidChangeEffectiveAppearance()
        needsDisplay = true
    }

    private func drawCentered(_ text: String) {
        let attributes: [NSAttributedString.Key: Any] = [
            .font: TerminalTheme.systemFont(ofSize: 13),
            .foregroundColor: TerminalTheme.secondaryText
        ]
        let size = (text as NSString).size(withAttributes: attributes)
        (text as NSString).draw(
            at: NSPoint(x: max(8, (bounds.width - size.width) / 2), y: max(8, (bounds.height - size.height) / 2)),
            withAttributes: attributes
        )
    }
}

private final class AutomationReviewFlippedView: NSView {
    override var isFlipped: Bool { true }
}

/// Read-only AppKit review of the original persisted strategy decision and its report.
final class AutomationReviewWindowController: NSWindowController, NSWindowDelegate,
    NSTableViewDataSource, NSTableViewDelegate {
    private struct TableColumnSpec {
        let id: String
        let title: String
        let width: CGFloat
    }

    private let session: AutomationSessionView
    private let service: AutomationSessionMCPService
    private let privacyIsMasked: () -> Bool
    var onClose: (() -> Void)?

    private let modeBadge = NSTextField(labelWithString: "")
    private let identityLabel = NSTextField(wrappingLabelWithString: "")
    private let statusLabel = NSTextField(wrappingLabelWithString: "복기 자료를 읽는 중…")
    private let decisionTable = NSTableView()
    private let candleChart = AutomationReviewCandleChartView()
    private let candleLabel = NSTextField(wrappingLabelWithString: "선택한 판단의 캔들 값이 아직 없습니다.")
    private let indicatorTable = NSTableView()
    private let orderTable = NSTableView()
    private let eventTable = NSTableView()
    private let performanceLabel = NSTextField(wrappingLabelWithString: "성과 보고서를 읽는 중…")
    private let methodologyLabel = NSTextField(wrappingLabelWithString: "")
    private let warningLabel = NSTextField(wrappingLabelWithString: "")
    private let eventCoverageLabel = NSTextField(wrappingLabelWithString: "")
    private let reasonsLabel = NSTextField(wrappingLabelWithString: "저장된 판단 이유를 불러오는 중…")
    private let ruleTraceLabel = NSTextField(wrappingLabelWithString: "")
    private let recordedDecisionLabel = NSTextField(wrappingLabelWithString: "선택한 판단의 엔진 기록 근거를 불러오는 중…")
    private let rawEvaluationText = NSTextView()
    private let rawEvaluationScroll = NSScrollView()
    private let rawEvaluationButton = NSButton(title: "저장된 평가 원본 펼치기", target: nil, action: nil)
    private let previousPageButton = NSButton(title: "이전 50개", target: nil, action: nil)
    private let nextPageButton = NSButton(title: "다음 50개", target: nil, action: nil)
    private let refreshButton = NSButton(title: "새로고침", target: nil, action: nil)
    private let exportButton = NSButton(title: "복기 파일 내보내기", target: nil, action: nil)
    private let exportProgress = NSProgressIndicator()

    private var decisions: [AutomationReviewDecisionSummary] = []
    private var nextOffset: UInt32?
    private var currentOffset: UInt32 = 0
    private var selectedIntentID: String?
    private var detail: AutomationReviewDecisionDetail?
    private var report: AutomationPerformanceReport?
    private var indicatorRows: [(String, String)] = []
    private var eventRows: [AutomationEvent] = []
    private var isUpdatingDecisionSelection = false
    private var isShowingRawEvaluation = false
    private var readGeneration = AutomationReviewRequestGeneration()
    private var detailGeneration = AutomationReviewRequestGeneration()
    private var activeReadRequests: [UUID: NativeMCPRequest] = [:]
    private var activeDetailRequest: (UUID, NativeMCPRequest)?
    private var activeExportRequest: (UUID, NativeMCPRequest)?
    private var exportGeneration: UInt64 = 0
    private var exportDirectory: URL?
    private var exportStagingDirectory: URL?
    private var exportManifest: AutomationReviewExportManifest?
    private var exportFiles: [AutomationReviewExportFile] = []
    private var exportFileIndex = 0
    private var exportFileOffset: UInt64 = 0
    private var exportFileBuffer = Data()
    private var exportTotalBytes: UInt64 = 0
    private var exportReceivedBytes: UInt64 = 0
    private var isClosed = false

    init(
        session: AutomationSessionView,
        service: AutomationSessionMCPService,
        privacyIsMasked: @escaping () -> Bool
    ) {
        self.session = session
        self.service = service
        self.privacyIsMasked = privacyIsMasked
        super.init(window: nil)
        buildWindow()
        renderPrivacyPresentation()
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) { nil }

    deinit {
        cancelAllRequests()
        removeExportStaging()
    }

    func showReview() {
        guard !isClosed else { return }
        showWindow(nil)
        window?.makeKeyAndOrderFront(nil)
        loadReview()
    }

    func refreshPrivacyPresentation() {
        renderPrivacyPresentation()
    }

    func numberOfRows(in tableView: NSTableView) -> Int {
        if tableView == decisionTable { return decisions.count }
        if tableView == indicatorTable { return indicatorRows.count }
        if tableView == orderTable { return detail?.orders.count ?? 0 }
        if tableView == eventTable { return min(eventRows.count, 200) }
        return 0
    }

    func tableView(_ tableView: NSTableView, viewFor tableColumn: NSTableColumn?, row: Int) -> NSView? {
        guard let identifier = tableColumn?.identifier.rawValue else { return nil }
        let value: String
        let masked = privacyIsMasked()
        if tableView == decisionTable, decisions.indices.contains(row) {
            let decision = decisions[row]
            switch identifier {
            case "time": value = decision.decision_time_utc
            case "action": value = decision.action.rawValue
            case "state": value = decision.state
            case "intent": value = AutomationReviewPrivacyPresentation.identity(decision.intent_id, masked: masked)
            default: value = ""
            }
        } else if tableView == indicatorTable, indicatorRows.indices.contains(row) {
            let item = indicatorRows[row]
            value = identifier == "name"
                ? AutomationReviewPrivacyPresentation.identity(item.0, masked: masked)
                : AutomationReviewPrivacyPresentation.exactValue(item.1, masked: masked)
        } else if tableView == orderTable, let orders = detail?.orders, orders.indices.contains(row) {
            let order = orders[row]
            switch identifier {
            case "client": value = AutomationReviewPrivacyPresentation.identity(order.client_identifier, masked: masked)
            case "side": value = order.side.rawValue
            case "state": value = order.state.rawValue
            case "notional": value = AutomationReviewPrivacyPresentation.exactValue(order.requested_notional_krw, masked: masked)
            case "executed": value = AutomationReviewPrivacyPresentation.exactValue(order.executed_volume, masked: masked)
            case "fee": value = AutomationReviewPrivacyPresentation.exactValue(order.paid_fee_krw, masked: masked)
            default: value = ""
            }
        } else if tableView == eventTable, eventRows.indices.contains(row), row < 200 {
            let event = eventRows[row]
            switch identifier {
            case "kind": value = event.kind.rawValue
            case "time": value = event.at_utc
            case "detail": value = masked
                    ? AutomationReviewPrivacyPresentation.hiddenText
                    : event.detail.redactedForDisplay.formattedJSON
            default: value = ""
            }
        } else {
            return nil
        }

        let cell = NSTextField(labelWithString: value)
        cell.font = identifier == "detail" ? TerminalTheme.systemFont(ofSize: 11) : TerminalTheme.table
        cell.lineBreakMode = identifier == "detail" ? .byTruncatingTail : .byClipping
        cell.toolTip = value
        cell.setAccessibilityLabel(value)
        return cell
    }

    func tableViewSelectionDidChange(_ notification: Notification) {
        guard !isUpdatingDecisionSelection,
              notification.object as? NSTableView === decisionTable,
              decisions.indices.contains(decisionTable.selectedRow) else { return }
        let intentID = decisions[decisionTable.selectedRow].intent_id
        guard intentID != selectedIntentID else { return }
        loadDecision(intentID: intentID)
    }

    func windowWillClose(_ notification: Notification) {
        _ = notification
        guard !isClosed else { return }
        isClosed = true
        _ = readGeneration.advance()
        _ = detailGeneration.advance()
        exportGeneration &+= 1
        cancelAllRequests()
        removeExportStaging()
        onClose?()
        onClose = nil
    }

    @objc private func loadPreviousPage() {
        guard currentOffset > 0 else { return }
        let prior = currentOffset > AutomationSessionWire.reviewDecisionPageLimit
            ? currentOffset - AutomationSessionWire.reviewDecisionPageLimit : 0
        loadDecisionPage(offset: prior)
    }

    @objc private func loadNextPage() {
        guard let nextOffset else { return }
        loadDecisionPage(offset: nextOffset)
    }

    @objc private func toggleRawEvaluation() {
        isShowingRawEvaluation.toggle()
        rawEvaluationButton.title = isShowingRawEvaluation ? "저장된 평가 원본 접기" : "저장된 평가 원본 펼치기"
        rawEvaluationScroll.isHidden = !isShowingRawEvaluation
    }

    @objc private func exportReview() {
        guard let window, !isClosed, activeExportRequest == nil, report != nil else { return }
        let alert = NSAlert()
        alert.messageText = "복기 파일에 거래 정보가 포함됩니다"
        alert.informativeText = "선택한 폴더에 결정, 주문, 체결 CSV와 성과·스냅샷 JSON/Markdown을 저장합니다. LIVE 세션이면 실제 거래 금액과 주문 식별 정보가 포함됩니다."
        alert.addButton(withTitle: "폴더 선택")
        alert.addButton(withTitle: "취소")
        alert.beginSheetModal(for: window) { [weak self] response in
            guard response == .alertFirstButtonReturn, let self, !self.isClosed else { return }
            self.chooseExportDirectory()
        }
    }

    @objc private func refreshReview() {
        guard activeExportRequest == nil else { return }
        loadReview()
    }

    private func buildWindow() {
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 1160, height: 900),
            styleMask: [.titled, .closable, .resizable, .miniaturizable],
            backing: .buffered,
            defer: false
        )
        window.title = "전략 자동매매 복기 · 성과"
        window.minSize = NSSize(width: 960, height: 680)
        window.delegate = self
        window.center()
        self.window = window

        let root = TerminalSurfaceView()
        root.translatesAutoresizingMaskIntoConstraints = false
        window.contentView = root

        let title = NSTextField(labelWithString: "전략 자동매매 복기")
        let titleFont = TerminalTheme.screenTitle
        title.font = titleFont
        let titleLineHeight = titleFont.ascender - titleFont.descender + titleFont.leading
        title.heightAnchor.constraint(greaterThanOrEqualToConstant: ceil(titleLineHeight + 4)).isActive = true
        title.setAccessibilityLabel("전략 자동매매 복기와 성과 보고서")
        modeBadge.font = TerminalTheme.monospacedFont(ofSize: 12, weight: .bold)
        modeBadge.wantsLayer = true
        modeBadge.layer?.cornerRadius = 4
        modeBadge.layer?.borderWidth = 1
        modeBadge.maximumNumberOfLines = 0
        modeBadge.lineBreakMode = .byWordWrapping
        modeBadge.setAccessibilityLabel("선택한 세션의 MOCK 또는 LIVE 기록 모드")
        configure(refreshButton, title: "새로고침", selector: #selector(refreshReview), label: "선택한 자동매매 복기와 성과를 다시 조회")
        exportButton.bezelStyle = .rounded
        exportButton.target = self
        exportButton.action = #selector(exportReview)
        exportButton.setAccessibilityLabel("현재 표시된 MOCK 또는 LIVE 세션의 복기 CSV와 성과 보고서 내보내기")
        exportButton.toolTip = "명시적으로 선택한 폴더에 6개 파일을 저장합니다. 거래 금액과 식별 정보가 포함될 수 있습니다."
        let titleRow = NSStackView(views: [title, modeBadge, NSView()])
        titleRow.orientation = .horizontal
        titleRow.alignment = .centerY
        titleRow.spacing = 12
        let actionRow = NSStackView(views: [NSView(), refreshButton, exportButton])
        actionRow.orientation = .horizontal
        actionRow.alignment = .centerY
        actionRow.spacing = 8
        let header = verticalStack([titleRow, actionRow], spacing: 6)

        identityLabel.font = TerminalTheme.table
        identityLabel.maximumNumberOfLines = 0
        identityLabel.lineBreakMode = .byWordWrapping
        identityLabel.setAccessibilityLabel("복기 대상 세션, 정책 버전과 시장 identity")
        statusLabel.font = TerminalTheme.systemFont(ofSize: 12, weight: .medium)
        statusLabel.textColor = TerminalTheme.secondaryText
        statusLabel.maximumNumberOfLines = 0
        statusLabel.lineBreakMode = .byWordWrapping

        setupTable(decisionTable, label: "저장된 전략 판단 시간순 목록", columns: [
            TableColumnSpec(id: "time", title: "판단 시각(UTC)", width: 190),
            TableColumnSpec(id: "action", title: "판단", width: 80),
            TableColumnSpec(id: "state", title: "상태", width: 90),
            TableColumnSpec(id: "intent", title: "의도 ID", width: 190)
        ])
        decisionTable.delegate = self
        decisionTable.dataSource = self
        let pageControls = NSStackView(views: [previousPageButton, nextPageButton, NSView()])
        pageControls.orientation = .horizontal
        pageControls.spacing = 8
        configure(previousPageButton, title: "이전 50개", selector: #selector(loadPreviousPage), label: "이전 복기 판단 50개")
        configure(nextPageButton, title: "다음 50개", selector: #selector(loadNextPage), label: "다음 복기 판단 50개")
        let timelinePanel = makePanel(title: "저장된 판단 시간순 목록", content: verticalStack([
            tableScroll(decisionTable, height: 170), pageControls
        ], spacing: 8))

        candleLabel.font = TerminalTheme.table
        candleLabel.maximumNumberOfLines = 2
        candleChart.translatesAutoresizingMaskIntoConstraints = false
        candleChart.heightAnchor.constraint(equalToConstant: 190).isActive = true
        candleChart.setAccessibilityLabel("선택한 판단과 앞선 저장 캔들의 OHLC 차트")
        let candlePanel = makePanel(title: "당시 캔들 · 선택 판단과 앞선 최대 64개 봉", content: verticalStack([
            candleLabel, candleChart
        ], spacing: 8))

        reasonsLabel.font = TerminalTheme.systemFont(ofSize: 13, weight: .medium)
        reasonsLabel.maximumNumberOfLines = 0
        ruleTraceLabel.font = TerminalTheme.table
        ruleTraceLabel.maximumNumberOfLines = 0
        recordedDecisionLabel.font = TerminalTheme.table
        recordedDecisionLabel.maximumNumberOfLines = 0
        recordedDecisionLabel.lineBreakMode = .byWordWrapping
        recordedDecisionLabel.setAccessibilityLabel("저장된 결정 행동, 조건 평가, 차단 이유, 주문과 체결 근거")
        setupTable(indicatorTable, label: "저장된 전략 지표 값", columns: [
            TableColumnSpec(id: "name", title: "지표", width: 230),
            TableColumnSpec(id: "value", title: "저장 값", width: 220)
        ])
        indicatorTable.delegate = self
        indicatorTable.dataSource = self
        configure(rawEvaluationButton, title: "저장된 평가 원본 펼치기", selector: #selector(toggleRawEvaluation), label: "저장된 평가 원본 표시 전환")
        rawEvaluationText.isEditable = false
        rawEvaluationText.isSelectable = true
        rawEvaluationText.font = TerminalTheme.monospacedFont(ofSize: 11)
        rawEvaluationScroll.documentView = rawEvaluationText
        rawEvaluationScroll.hasVerticalScroller = true
        rawEvaluationScroll.hasHorizontalScroller = true
        rawEvaluationScroll.drawsBackground = true
        rawEvaluationScroll.backgroundColor = TerminalTheme.chart
        rawEvaluationScroll.heightAnchor.constraint(equalToConstant: 190).isActive = true
        rawEvaluationScroll.isHidden = true
        let evaluationPanel = makePanel(title: "당시 이유 · 지표 · 규칙 상태 전이", content: verticalStack([
            reasonsLabel,
            ruleTraceLabel,
            recordedDecisionLabel,
            labeledControl("당시 저장된 지표", tableScroll(indicatorTable, height: 130)),
            rawEvaluationButton,
            rawEvaluationScroll
        ], spacing: 8))

        setupTable(orderTable, label: "선택 판단에 연결된 주문", columns: [
            TableColumnSpec(id: "client", title: "주문 식별자", width: 210),
            TableColumnSpec(id: "side", title: "구분", width: 70),
            TableColumnSpec(id: "state", title: "상태", width: 110),
            TableColumnSpec(id: "notional", title: "요청 금액(KRW)", width: 150),
            TableColumnSpec(id: "executed", title: "체결 수량", width: 130),
            TableColumnSpec(id: "fee", title: "지급 수수료(KRW)", width: 140)
        ])
        orderTable.delegate = self
        orderTable.dataSource = self
        let orderPanel = makePanel(title: "선택 판단에 연결된 주문 원장", content: tableScroll(orderTable, height: 120))

        setupTable(eventTable, label: "선택 판단에 연결된 체결·위험 이벤트", columns: [
            TableColumnSpec(id: "kind", title: "이벤트", width: 210),
            TableColumnSpec(id: "time", title: "기록 시각(UTC)", width: 190),
            TableColumnSpec(id: "detail", title: "저장된 주문·체결·위험 내용", width: 560)
        ])
        eventTable.delegate = self
        eventTable.dataSource = self
        let eventPanel = makePanel(title: "체결·위험·복구 이벤트", content: verticalStack([
            tableScroll(eventTable, height: 150),
            eventCoverageLabel
        ], spacing: 7))
        eventCoverageLabel.font = TerminalTheme.systemFont(ofSize: 11)
        eventCoverageLabel.textColor = TerminalTheme.secondaryText
        eventCoverageLabel.maximumNumberOfLines = 2

        performanceLabel.font = TerminalTheme.systemFont(ofSize: 13)
        performanceLabel.maximumNumberOfLines = 0
        methodologyLabel.font = TerminalTheme.systemFont(ofSize: 12)
        methodologyLabel.textColor = TerminalTheme.secondaryText
        methodologyLabel.maximumNumberOfLines = 0
        warningLabel.font = TerminalTheme.systemFont(ofSize: 12, weight: .medium)
        warningLabel.textColor = TerminalTheme.negative
        warningLabel.maximumNumberOfLines = 0
        exportProgress.isIndeterminate = false
        exportProgress.minValue = 0
        exportProgress.maxValue = 1
        exportProgress.doubleValue = 0
        exportProgress.isHidden = true
        let reportPanel = makePanel(title: "선택 세션의 성과 보고서 · 저장된 거래 사실 기준", content: verticalStack([
            performanceLabel, methodologyLabel, warningLabel, exportProgress
        ], spacing: 8))

        let detailContent = verticalStack([
            candlePanel, evaluationPanel, orderPanel, eventPanel, reportPanel
        ], spacing: 12)
        for panel in detailContent.arrangedSubviews {
            panel.widthAnchor.constraint(equalTo: detailContent.widthAnchor).isActive = true
        }
        let detailScroll = NSScrollView()
        detailScroll.hasVerticalScroller = true
        detailScroll.hasHorizontalScroller = false
        detailScroll.drawsBackground = false
        detailScroll.translatesAutoresizingMaskIntoConstraints = false
        let detailDocument = AutomationReviewFlippedView()
        detailDocument.translatesAutoresizingMaskIntoConstraints = false
        detailScroll.documentView = detailDocument
        detailContent.translatesAutoresizingMaskIntoConstraints = false
        detailDocument.addSubview(detailContent)
        NSLayoutConstraint.activate([
            detailDocument.widthAnchor.constraint(equalTo: detailScroll.contentView.widthAnchor),
            detailContent.leadingAnchor.constraint(equalTo: detailDocument.leadingAnchor, constant: 12),
            detailContent.trailingAnchor.constraint(equalTo: detailDocument.trailingAnchor, constant: -12),
            detailContent.topAnchor.constraint(equalTo: detailDocument.topAnchor, constant: 12),
            detailContent.bottomAnchor.constraint(equalTo: detailDocument.bottomAnchor, constant: -12)
        ])

        let split = NSSplitView()
        split.isVertical = true
        split.dividerStyle = .thin
        split.addArrangedSubview(timelinePanel)
        split.addArrangedSubview(detailScroll)
        timelinePanel.widthAnchor.constraint(greaterThanOrEqualToConstant: 300).isActive = true
        detailScroll.widthAnchor.constraint(greaterThanOrEqualToConstant: 500).isActive = true
        split.heightAnchor.constraint(greaterThanOrEqualToConstant: 320).isActive = true
        split.setContentHuggingPriority(.defaultLow, for: .vertical)

        let contentStack = verticalStack([header, identityLabel, statusLabel, split], spacing: 10)
        contentStack.translatesAutoresizingMaskIntoConstraints = false
        root.addSubview(contentStack)
        NSLayoutConstraint.activate([
            contentStack.leadingAnchor.constraint(equalTo: root.leadingAnchor, constant: 16),
            contentStack.trailingAnchor.constraint(equalTo: root.trailingAnchor, constant: -16),
            contentStack.topAnchor.constraint(equalTo: root.topAnchor, constant: 16),
            contentStack.bottomAnchor.constraint(equalTo: root.bottomAnchor, constant: -16)
        ])
        for view in contentStack.arrangedSubviews {
            view.widthAnchor.constraint(equalTo: contentStack.widthAnchor).isActive = true
        }
        updateModeBadge()
    }

    private func loadReview() {
        guard !isClosed, window?.isVisible == true, activeExportRequest == nil else { return }
        _ = readGeneration.advance()
        let ticket = readGeneration.value
        cancelReadRequests()
        decisions = []
        nextOffset = nil
        selectedIntentID = nil
        detail = nil
        report = nil
        indicatorRows = []
        eventRows = []
        decisionTable.reloadData()
        renderDecisionDetail()
        renderPerformance()
        statusLabel.stringValue = "저장된 판단과 성과 보고서를 읽는 중…"
        exportButton.isEnabled = false
        loadDecisionPage(offset: 0, generation: ticket)
        loadPerformance(generation: ticket)
    }

    private func loadDecisionPage(offset: UInt32, generation: UInt64? = nil) {
        guard !isClosed, window?.isVisible == true else { return }
        let ticket: UInt64
        if let generation {
            ticket = generation
        } else {
            ticket = readGeneration.advance()
            cancelReadRequests()
            _ = detailGeneration.advance()
            decisions = []
            selectedIntentID = nil
            detail = nil
            indicatorRows = []
            eventRows = []
            eventCoverageLabel.stringValue = "선택한 판단에 연결된 저장 이벤트를 불러오는 중입니다."
            decisionTable.reloadData()
            renderDecisionDetail()
            loadPerformance(generation: ticket)
        }
        currentOffset = offset
        previousPageButton.isEnabled = false
        nextPageButton.isEnabled = false
        statusLabel.stringValue = "저장된 판단 목록 · \(offset)부터 조회 중…"
        let requestID = UUID()
        let request = service.listReviewDecisions(sessionID: session.session_id, offset: offset) { [weak self] result in
            DispatchQueue.main.async {
                guard let self, !self.isClosed, self.window?.isVisible == true,
                      self.readGeneration.accepts(ticket) else { return }
                self.activeReadRequests.removeValue(forKey: requestID)
                switch result {
                case .failure(let error):
                    self.statusLabel.stringValue = "판단 목록 조회 실패 · \(error.localizedDescription)"
                    self.statusLabel.textColor = TerminalTheme.negative
                    self.decisions = []
                    self.nextOffset = nil
                    self.decisionTable.reloadData()
                    self.renderDecisionDetail()
                case .success(let page):
                    guard page.mode == self.session.mode,
                          page.policy_ref == self.session.policy_ref else {
                        self.statusLabel.stringValue = "판단 목록의 LIVE/MOCK 또는 동결 전략 identity가 세션과 달라 표시를 중단했습니다."
                        self.statusLabel.textColor = TerminalTheme.negative
                        return
                    }
                    self.decisions = page.items
                    self.nextOffset = page.next_offset
                    self.currentOffset = offset
                    self.selectedIntentID = nil
                    self.isUpdatingDecisionSelection = true
                    self.decisionTable.reloadData()
                    if !page.items.isEmpty {
                        self.selectedIntentID = page.items[0].intent_id
                        self.decisionTable.selectRowIndexes(IndexSet(integer: 0), byExtendingSelection: false)
                    }
                    self.isUpdatingDecisionSelection = false
                    self.previousPageButton.isEnabled = offset > 0
                    self.nextPageButton.isEnabled = page.next_offset != nil
                    self.statusLabel.stringValue = "\(page.mode.rawValue) · 저장된 판단 \(UInt64(offset) + page.returned_count) / \(page.total_count) · 재평가 없음"
                    self.statusLabel.textColor = TerminalTheme.secondaryText
                    if let selected = self.selectedIntentID {
                        self.loadDecision(intentID: selected)
                    } else {
                        self.renderDecisionDetail()
                    }
                }
            }
        }
        if request.isPending { activeReadRequests[requestID] = request }
    }

    private func loadPerformance(generation: UInt64) {
        exportButton.isEnabled = false
        let requestID = UUID()
        let request = service.getAutomationPerformance(
            sessionID: session.session_id,
            expectedMode: session.mode,
            expectedPolicyReference: session.policy_ref
        ) { [weak self] result in
            DispatchQueue.main.async {
                guard let self, !self.isClosed, self.window?.isVisible == true,
                      self.readGeneration.accepts(generation) else { return }
                self.activeReadRequests.removeValue(forKey: requestID)
                switch result {
                case .failure(let error):
                    self.report = nil
                    self.performanceLabel.stringValue = "성과 보고서 조회 실패 · \(error.localizedDescription)"
                    self.performanceLabel.textColor = TerminalTheme.negative
                    self.exportButton.isEnabled = false
                case .success(let report):
                    self.report = report
                    self.renderPerformance()
                    self.exportButton.isEnabled = true
                }
            }
        }
        if request.isPending { activeReadRequests[requestID] = request }
    }

    private func loadDecision(intentID: String) {
        guard !isClosed, window?.isVisible == true else { return }
        selectedIntentID = intentID
        _ = detailGeneration.advance()
        let ticket = detailGeneration.value
        if let activeDetailRequest {
            activeDetailRequest.1.cancel()
            activeReadRequests.removeValue(forKey: activeDetailRequest.0)
            self.activeDetailRequest = nil
        }
        detail = nil
        indicatorRows = []
        eventRows = []
        renderDecisionDetail()
        statusLabel.stringValue = "선택한 판단의 캔들·지표·이벤트를 읽는 중…"
        let requestID = UUID()
        let request = service.getReviewDecision(
            sessionID: session.session_id,
            intentID: intentID,
            expectedMode: session.mode,
            expectedPolicyReference: session.policy_ref
        ) { [weak self] result in
            DispatchQueue.main.async {
                guard let self, !self.isClosed, self.window?.isVisible == true,
                      self.detailGeneration.accepts(ticket), self.selectedIntentID == intentID else { return }
                self.activeReadRequests.removeValue(forKey: requestID)
                self.activeDetailRequest = nil
                switch result {
                case .failure(let error):
                    self.statusLabel.stringValue = "당시 판단 상세 조회 실패 · \(error.localizedDescription)"
                    self.statusLabel.textColor = TerminalTheme.negative
                    self.renderDecisionDetail()
                case .success(let detail):
                    guard detail.session.mode == self.session.mode,
                          detail.session.policy_ref == self.session.policy_ref else {
                        self.statusLabel.stringValue = "상세 응답의 LIVE/MOCK 또는 동결 전략 identity가 달라 표시를 중단했습니다."
                        self.statusLabel.textColor = TerminalTheme.negative
                        return
                    }
                    guard let summary = self.decisions.first(where: { $0.intent_id == intentID }) else {
                        self.statusLabel.stringValue = "선택한 판단이 현재 시간순 페이지에 없어 상세 표시를 중단했습니다."
                        self.statusLabel.textColor = TerminalTheme.negative
                        return
                    }
                    do {
                        try AutomationSessionMCPService.validateReviewDecisionMatchesSummary(detail, summary: summary)
                    } catch {
                        self.statusLabel.stringValue = "상세 판단과 선택한 시간순 기록의 identity가 달라 표시를 중단했습니다."
                        self.statusLabel.textColor = TerminalTheme.negative
                        return
                    }
                    self.detail = detail
                    self.eventRows = detail.events
                    self.indicatorRows = Self.indicators(from: detail.decision.evaluation)
                    self.statusLabel.stringValue = "저장된 평가 원본 · \(detail.decision.recorded_at_utc) 기록 · \(detail.decision.decision_time_utc) 판단 · 현재 데이터로 재평가하지 않음"
                    self.statusLabel.textColor = TerminalTheme.secondaryText
                    self.renderPrivacyPresentation()
                }
            }
        }
        activeDetailRequest = (requestID, request)
        if request.isPending { activeReadRequests[requestID] = request }
    }

    private func renderPrivacyPresentation() {
        updateModeBadge()
        let masked = privacyIsMasked()
        identityLabel.stringValue = [
            "전략 세션: \(AutomationReviewPrivacyPresentation.identity(session.session_id, masked: masked))",
            "전략 이름: \(AutomationReviewPrivacyPresentation.identity(detail?.frozen_policy.definition.name, masked: masked))",
            "전략 버전: \(AutomationReviewPrivacyPresentation.identity(session.policy_ref.policy_id, masked: masked)) · \(AutomationReviewPrivacyPresentation.identity(session.policy_ref.revision_id, masked: masked))",
            "시장: \(AutomationReviewPrivacyPresentation.identity(session.market, masked: masked)) · 주기: \(AutomationReviewPrivacyPresentation.identity(session.decision_interval, masked: masked))"
        ].joined(separator: "   ·   ")
        identityLabel.toolTip = masked ? AutomationReviewPrivacyPresentation.hiddenText : identityLabel.stringValue
        identityLabel.setAccessibilityValue(identityLabel.stringValue)
        identityLabel.setAccessibilityHelp(identityLabel.toolTip)
        renderDecisionDetail()
        renderPerformance()
        decisionTable.reloadData()
        indicatorTable.reloadData()
        orderTable.reloadData()
        eventTable.reloadData()
    }

    private func updateModeBadge() {
        switch session.mode {
        case .mock:
            modeBadge.stringValue = "  MOCK · 모의 기록  "
            modeBadge.textColor = TerminalTheme.positive
            modeBadge.layer?.borderColor = TerminalTheme.positive.cgColor
        case .live:
            modeBadge.stringValue = "  LIVE · 실제 계좌 기록  "
            modeBadge.textColor = TerminalTheme.negative
            modeBadge.layer?.borderColor = TerminalTheme.negative.cgColor
        }
        modeBadge.toolTip = modeBadge.stringValue
        modeBadge.setAccessibilityValue(modeBadge.stringValue)
    }

    private func renderDecisionDetail() {
        guard let detail else {
            candleLabel.stringValue = "선택한 판단의 당시 캔들 값이 아직 없습니다."
            candleChart.candles = []
            candleChart.isMasked = privacyIsMasked()
            reasonsLabel.stringValue = decisions.isEmpty ? "저장된 전략 판단이 없습니다." : "시간순 목록에서 판단을 선택하세요."
            ruleTraceLabel.stringValue = ""
            recordedDecisionLabel.stringValue = decisions.isEmpty
                ? "저장된 결정 근거가 없습니다."
                : "시간순 목록에서 판단을 선택하세요."
            indicatorRows = []
            indicatorTable.reloadData()
            orderTable.reloadData()
            eventTable.reloadData()
            rawEvaluationText.string = ""
            return
        }
        let masked = privacyIsMasked()
        recordedDecisionLabel.stringValue = RecordedDecisionPresentation.lines(
            for: detail.recorded_decision,
            masked: masked
        ).joined(separator: "\n")
        recordedDecisionLabel.toolTip = recordedDecisionLabel.stringValue
        recordedDecisionLabel.setAccessibilityValue(recordedDecisionLabel.stringValue)
        let evaluation = Self.evaluationObject(from: detail.decision.evaluation)
        let isWarmup = detail.decision.action == .hold
            && ["phase", "kind"].contains(where: { evaluation[$0]?.stringValue == "POLICY_WARMUP" })
        let reasonValues = evaluation["reasons"]?.arrayValue?.compactMap(\.stringValue) ?? []
        let recovery = detail.decision.evaluation.objectValue?["recovery_disposition"]?.stringValue
        let reasons = isWarmup ? "지표 초기화를 위한 저장 기록 · 정책 신호 없음"
            : (reasonValues.isEmpty ? "저장된 reason code 없음" : reasonValues.joined(separator: " · "))
        let actionLabel = isWarmup ? "워밍업" : detail.decision.action.rawValue
        reasonsLabel.stringValue = masked
            ? "\(actionLabel) · 개인정보 표시를 켜면 당시 지표와 판단 이유를 볼 수 있습니다."
            : "\(actionLabel) · \(detail.decision.state) · 이유: \(reasons)\(recovery.map { " · 복구 상태: \($0)" } ?? "")"
        reasonsLabel.textColor = detail.decision.action == .buy ? TerminalTheme.positive
            : (detail.decision.action == .sell ? TerminalTheme.negative : TerminalTheme.secondaryText)
        let trace = evaluation["policy_trace"]?.objectValue
        let matchedRule = trace?["matched_rule_id"]?.stringValue ?? "해당 없음 / 미제공"
        let stateBefore = evaluation["state_before"]?.stringValue ?? "미제공"
        let stateAfter = evaluation["state_after"]?.stringValue ?? "미제공"
        let baseBefore = evaluation["base_state_before"]?.stringValue
        let baseAfter = evaluation["base_state_after"]?.stringValue
        let policyBefore = Self.namedStates(trace?["state_before"])
        let policyAfter = Self.namedStates(trace?["state_after"])
        ruleTraceLabel.stringValue = masked
            ? "정책 규칙·상태 전이: 개인정보 숨김"
            : "정책 규칙: \(matchedRule) · 상태 전이: \(stateBefore) → \(stateAfter)"
                + (baseBefore != nil || baseAfter != nil ? " · 기준 신호: \(baseBefore ?? "미제공") → \(baseAfter ?? "미제공")" : "")
                + (!policyBefore.isEmpty || !policyAfter.isEmpty
                    ? " · 정책 상태 값: \(policyBefore.isEmpty ? "미제공" : policyBefore) → \(policyAfter.isEmpty ? "미제공" : policyAfter)"
                    : "")

        let candle = detail.decision.candle.candle
        candleLabel.stringValue = "\(candle.close_time_utc) · 시가 \(AutomationReviewPrivacyPresentation.exactValue(candle.open, masked: masked)) · 고가 \(AutomationReviewPrivacyPresentation.exactValue(candle.high, masked: masked)) · 저가 \(AutomationReviewPrivacyPresentation.exactValue(candle.low, masked: masked)) · 종가 \(AutomationReviewPrivacyPresentation.exactValue(candle.close, masked: masked)) · 거래량 \(AutomationReviewPrivacyPresentation.exactValue(candle.volume, masked: masked)) · 거래대금 \(AutomationReviewPrivacyPresentation.exactValue(candle.quote_turnover, masked: masked))"
        let chartObservations = (detail.context_candles + [detail.decision.candle])
            .reduce(into: [String: AutomationReviewCandleObservation]()) { result, item in result[item.id] = item }
            .values.sorted { $0.candle.close_time_utc < $1.candle.close_time_utc }
        candleChart.candles = chartObservations.compactMap { item in
            guard let open = Decimal(string: item.candle.open, locale: Locale(identifier: "en_US_POSIX")),
                  let high = Decimal(string: item.candle.high, locale: Locale(identifier: "en_US_POSIX")),
                  let low = Decimal(string: item.candle.low, locale: Locale(identifier: "en_US_POSIX")),
                  let close = Decimal(string: item.candle.close, locale: Locale(identifier: "en_US_POSIX")) else { return nil }
            return AutomationReviewChartCandle(open: open, high: high, low: low, close: close)
        }
        candleChart.isMasked = masked
        candleChart.setAccessibilityLabel(masked
            ? "개인정보 숨김"
            : "\(chartObservations.count)개 저장 캔들 OHLC 차트 · 마지막 봉이 선택 판단입니다.")

        indicatorRows = Self.indicators(from: detail.decision.evaluation)
        indicatorTable.reloadData()
        orderTable.reloadData()
        eventRows = detail.events
        eventTable.reloadData()
        eventCoverageLabel.stringValue = eventRows.count > 200
            ? "저장 이벤트 \(eventRows.count)건 중 이 표에는 앞의 200건을 표시합니다. 내보내기 스냅샷에는 manifest가 지정한 전체 기록이 포함됩니다."
            : "저장 이벤트 \(eventRows.count)건 모두 표시 · 원본 시각과 유형 · 전략 재실행 없음"
        rawEvaluationText.string = AutomationReviewPrivacyPresentation.evaluation(detail.decision.evaluation, masked: masked)
        rawEvaluationButton.isEnabled = !masked
        if masked {
            isShowingRawEvaluation = false
            rawEvaluationButton.title = "저장된 평가 원본 펼치기"
            rawEvaluationScroll.isHidden = true
        }
    }

    private func renderPerformance() {
        guard let report else {
            if performanceLabel.stringValue.isEmpty { performanceLabel.stringValue = "성과 보고서를 불러오는 중…" }
            return
        }
        let masked = privacyIsMasked()
        let value: (String?) -> String = { AutomationReviewPrivacyPresentation.exactValue($0, masked: masked) }
        let openingInventory: String
        if let opening = report.opening_inventory {
            openingInventory = "기준 시작 보유 수량 \(value(opening.base_quantity)) · 출처 \(opening.source_mode.rawValue) · 관측 \(opening.observed_at_utc) · snapshot \(AutomationReviewPrivacyPresentation.identity(String(opening.snapshot_sequence), masked: masked)) · balances digest \(AutomationReviewPrivacyPresentation.identity(opening.balances_digest, masked: masked))"
        } else {
            openingInventory = "기준 시작 보유 수량·잔고 snapshot 증거: 미확인 (저장된 opening inventory 없음)"
        }
        performanceLabel.stringValue = [
            "기록 모드: \(report.session.mode.rawValue) · 판단 \(report.decision_count)건 (워밍업 \(report.warmup_record_count) / 매수 \(report.buy_signal_count) / 매도 \(report.sell_signal_count) / 보유 \(report.hold_signal_count)) · 거절 \(report.rejected_intent_count)건",
            "주문 \(report.order_count)건 · 체결 \(report.fill_count)건 · 매수 금액 \(value(report.buy_funds_krw)) KRW · 매도 금액 \(value(report.sell_funds_krw)) KRW · 수수료 \(value(report.paid_fees_krw)) KRW",
            "순 현금 흐름 \(value(report.net_quote_cashflow_krw)) KRW · 실현 손익 \(metric(report.realized_pnl_krw, masked: masked)) · 미실현 손익 \(metric(report.unrealized_pnl_krw, masked: masked))",
            "총 손익 \(metric(report.total_pnl_krw, masked: masked)) · 수익률 \(metric(report.return_ratio, masked: masked)) · 최대 낙폭 \(metric(report.max_drawdown_ratio, masked: masked))",
            "남은 전략 보유량 \(value(report.remaining_volume)) · 보유 원가 \(value(report.remaining_cost_krw)) KRW · 마크 가격 \(value(report.mark_price_krw)) KRW · 마크 시각 \(report.mark_at_utc ?? "미확인")",
            "마크 봉 관측 ID \(AutomationReviewPrivacyPresentation.identity(report.mark_observation_id, masked: masked)) · 봉 content digest \(AutomationReviewPrivacyPresentation.identity(report.mark_content_digest, masked: masked))",
            openingInventory,
            "스냅샷 시각 \(report.snapshot_at_utc) · digest \(AutomationReviewPrivacyPresentation.identity(report.snapshot_digest, masked: masked))"
        ].joined(separator: "\n")
        methodologyLabel.stringValue = "계산 방법: \(report.methodology)\n현금 흐름은 수익으로 표시하지 않습니다. 공유 계좌 전체 자본·수익률은 세션 성과로 추정하지 않습니다."
        warningLabel.stringValue = masked
            ? "검토 참고: 개인정보 표시를 켜면 저장된 성과 경고를 확인할 수 있습니다."
            : (report.warnings.isEmpty ? "성과 산출 경고 없음" : "검토 참고: " + report.warnings.joined(separator: " · "))
        performanceLabel.textColor = .labelColor
    }

    private func metric(_ metric: AutomationReviewMetric, masked: Bool) -> String {
        guard let raw = metric.value else {
            return "산출 불가 · \(metric.unavailable_reason ?? "사유 미제공")"
        }
        return "\(AutomationReviewPrivacyPresentation.exactValue(raw, masked: masked)) \(metric.unit)"
    }

    private static func evaluationObject(from value: AutomationJSONValue) -> [String: AutomationJSONValue] {
        guard let root = value.objectValue else { return [:] }
        if let nested = root["policy_evaluation"]?.objectValue { return nested }
        return root
    }

    private static func indicators(from value: AutomationJSONValue) -> [(String, String)] {
        let root = evaluationObject(from: value)
        guard let fields = root["indicators"]?.objectValue else { return [] }
        var rows: [(String, String)] = []
        if let version = fields["version"]?.stringValue {
            rows.append(("지표 버전", version))
        }
        for (key, title) in [
            ("ema", "EMA"), ("bar_sigma", "봉 변동성"), ("annual_vol", "연환산 변동성"),
            ("rsi", "RSI"), ("entry_high", "진입 고점"), ("exit_low", "이탈 저점")
        ] {
            guard let item = fields[key] else { continue }
            rows.append((title, exactJSONValue(item)))
        }
        if let bars = fields["completed_bars_seen"] {
            rows.append(("완료 봉 수", exactJSONValue(bars)))
        }
        for item in fields["policy_values"]?.arrayValue ?? [] {
            guard let fields = item.objectValue,
                  let id = fields["id"]?.stringValue,
                  let value = fields["value"] else { continue }
            rows.append(("규칙 지표 · \(id)", exactJSONValue(value)))
        }
        return rows
    }

    private static func namedStates(_ value: AutomationJSONValue?) -> String {
        (value?.arrayValue ?? []).compactMap { item -> String? in
            guard let fields = item.objectValue,
                  let name = fields["id"]?.stringValue,
                  let state = fields["value"] else { return nil }
            return "\(name)=\(exactJSONValue(state))"
        }.joined(separator: ", ")
    }

    private static func exactJSONValue(_ value: AutomationJSONValue) -> String {
        switch value {
        case .number(let decimal): return NSDecimalNumber(decimal: decimal).stringValue
        case .string(let string): return string
        case .boolean(let boolean): return boolean ? "true" : "false"
        case .null: return "미제공"
        case .object, .array: return value.formattedJSON
        }
    }

    private func chooseExportDirectory() {
        guard let window, !isClosed else { return }
        let panel = NSOpenPanel()
        panel.canChooseFiles = false
        panel.canChooseDirectories = true
        panel.allowsMultipleSelection = false
        panel.canCreateDirectories = true
        panel.prompt = "이 폴더에 저장"
        panel.message = "모든 파일을 검증한 뒤 이 폴더 안에 새 복기 폴더 하나로 저장합니다."
        panel.beginSheetModal(for: window) { [weak self] response in
            guard response == .OK, let self, let url = panel.url, !self.isClosed else { return }
            self.startExport(to: url)
        }
    }

    private func startExport(to directory: URL) {
        guard activeExportRequest == nil else { return }
        exportGeneration &+= 1
        let ticket = exportGeneration
        exportDirectory = directory
        exportStagingDirectory = nil
        exportManifest = nil
        exportFiles = []
        exportFileIndex = 0
        exportFileOffset = 0
        exportFileBuffer = Data()
        exportTotalBytes = 0
        exportReceivedBytes = 0
        exportProgress.isHidden = false
        exportProgress.doubleValue = 0
        exportButton.isEnabled = false
        refreshButton.isEnabled = false
        statusLabel.stringValue = "내보내기 manifest를 조회하고 스냅샷을 고정하는 중…"
        let requestID = UUID()
        let request = service.getAutomationReviewExportManifest(
            sessionID: session.session_id,
            expectedMode: session.mode
        ) { [weak self] result in
            DispatchQueue.main.async {
                guard let self, !self.isClosed, self.exportGeneration == ticket,
                      self.window?.isVisible == true else { return }
                self.activeExportRequest = nil
                switch result {
                case .failure(let error):
                    self.finishExportFailure("내보내기 manifest 검증 실패 · \(error.localizedDescription)")
                case .success(let manifest):
                    guard manifest.mode == self.session.mode,
                          manifest.session_id == self.session.session_id else {
                        self.finishExportFailure("manifest 세션 또는 LIVE/MOCK identity가 선택과 다릅니다.")
                        return
                    }
                    guard let displayedReport = self.report,
                          displayedReport.snapshot_digest == manifest.snapshot_digest else {
                        self.finishExportFailure("화면의 성과 보고서와 내보내기 manifest가 다른 snapshot입니다. 새로고침한 뒤 다시 내보내세요.")
                        return
                    }
                    self.exportManifest = manifest
                    self.exportFiles = manifest.files.sorted {
                        (AutomationReviewFile.allCases.firstIndex(of: $0.file) ?? Int.max)
                            < (AutomationReviewFile.allCases.firstIndex(of: $1.file) ?? Int.max)
                    }
                    self.exportTotalBytes = self.exportFiles.reduce(0) { $0 + $1.bytes }
                    self.exportProgress.maxValue = max(1, Double(self.exportTotalBytes))
                    do {
                        try self.prepareExportStagingDirectory()
                        self.statusLabel.stringValue = "6개 파일 · \(self.exportTotalBytes) bytes · 청크와 SHA-256 검증 중…"
                        self.downloadNextExportFile(ticket: ticket)
                    } catch {
                        self.finishExportFailure("복기 파일 준비 실패 · \(error.localizedDescription)")
                    }
                }
            }
        }
        activeExportRequest = (requestID, request)
    }

    private func downloadNextExportFile(ticket: UInt64) {
        guard !isClosed, exportGeneration == ticket else { return }
        guard exportFileIndex < exportFiles.count else {
            finishExportSuccess()
            return
        }
        exportFileOffset = 0
        exportFileBuffer = Data()
        readExportChunk(ticket: ticket)
    }

    private func readExportChunk(ticket: UInt64) {
        guard !isClosed, exportGeneration == ticket,
              exportFiles.indices.contains(exportFileIndex),
              let manifest = exportManifest else { return }
        let file = exportFiles[exportFileIndex]
        let requestID = UUID()
        let request = service.readAutomationReviewExport(
            sessionID: session.session_id,
            mode: session.mode,
            snapshotDigest: manifest.snapshot_digest,
            file: file,
            offset: exportFileOffset
        ) { [weak self] result in
            DispatchQueue.main.async {
                guard let self, !self.isClosed, self.exportGeneration == ticket,
                      self.window?.isVisible == true else { return }
                self.activeExportRequest = nil
                switch result {
                case .failure(let error):
                    self.finishExportFailure("\(file.file_name) 다운로드 중 스냅샷 검증 실패 · \(error.localizedDescription)")
                case .success(let chunk):
                    do {
                        let data = try AutomationReviewExportCodec.decodeAndValidate(
                            chunk,
                            file: file,
                            sessionID: self.session.session_id,
                            mode: self.session.mode,
                            snapshotDigest: manifest.snapshot_digest,
                            expectedOffset: self.exportFileOffset
                        )
                        self.exportFileBuffer.append(data)
                        self.exportReceivedBytes += UInt64(data.count)
                        self.exportProgress.doubleValue = Double(self.exportReceivedBytes)
                        self.exportFileOffset += UInt64(data.count)
                        if chunk.next_offset != nil {
                            self.readExportChunk(ticket: ticket)
                        } else {
                            try AutomationReviewExportCodec.validateCompleteFile(self.exportFileBuffer, manifestFile: file)
                            try self.writeStagedFile(file.file_name, data: self.exportFileBuffer)
                            self.exportFileIndex += 1
                            self.downloadNextExportFile(ticket: ticket)
                        }
                    } catch {
                        self.finishExportFailure("\(file.file_name) 청크/전체 SHA-256 검증 실패 · \(error.localizedDescription)")
                    }
                }
            }
        }
        activeExportRequest = (requestID, request)
    }

    private func prepareExportStagingDirectory() throws {
        guard let exportDirectory else { throw ExportFailure.missingDirectory }
        var isDirectory: ObjCBool = false
        guard FileManager.default.fileExists(atPath: exportDirectory.path, isDirectory: &isDirectory),
              isDirectory.boolValue else { throw ExportFailure.missingDirectory }
        exportStagingDirectory = try AutomationReviewExportStaging.createDirectory(inside: exportDirectory)
    }

    private func writeStagedFile(_ name: String, data: Data) throws {
        guard let exportStagingDirectory,
              let manifestFile = exportFiles.first(where: { $0.file_name == name }) else {
            throw ExportFailure.invalidFileName
        }
        try AutomationReviewExportStaging.write(data, file: manifestFile, inside: exportStagingDirectory)
    }

    private func finishExportSuccess() {
        guard let staging = exportStagingDirectory, let directory = exportDirectory else {
            finishExportFailure("임시 내보내기 폴더를 찾을 수 없습니다.")
            return
        }
        do {
            let destination = try unusedReviewDirectory(inside: directory)
            try FileManager.default.moveItem(at: staging, to: destination)
            exportStagingDirectory = nil
            exportButton.isEnabled = true
            refreshButton.isEnabled = true
            exportProgress.doubleValue = exportProgress.maxValue
            statusLabel.stringValue = "복기 파일 6개 저장·검증 완료 · \(destination.lastPathComponent)"
            statusLabel.textColor = TerminalTheme.positive
        } catch {
            finishExportFailure("복기 파일을 한 번에 반영하지 못했습니다 · \(error.localizedDescription)")
        }
        activeExportRequest = nil
    }

    private func unusedReviewDirectory(inside parent: URL) throws -> URL {
        let fileManager = FileManager.default
        let baseName = "SkyBackcraft-review"
        for suffix in 1...100 {
            let name = suffix == 1 ? baseName : "\(baseName)-\(suffix)"
            let candidate = parent.appendingPathComponent(name, isDirectory: true)
            if !fileManager.fileExists(atPath: candidate.path) { return candidate }
        }
        throw ExportFailure.destinationBusy
    }

    private func finishExportFailure(_ message: String) {
        if let activeExportRequest {
            activeExportRequest.1.cancel()
            self.activeExportRequest = nil
        }
        exportGeneration &+= 1
        removeExportStaging()
        exportButton.isEnabled = true
        refreshButton.isEnabled = true
        exportProgress.isHidden = true
        statusLabel.stringValue = message
        statusLabel.textColor = TerminalTheme.negative
    }

    private func removeExportStaging() {
        if let exportStagingDirectory {
            try? FileManager.default.removeItem(at: exportStagingDirectory)
        }
        exportStagingDirectory = nil
        exportFileBuffer.removeAll(keepingCapacity: false)
    }

    private func cancelReadRequests() {
        activeReadRequests.values.forEach { $0.cancel() }
        activeReadRequests.removeAll(keepingCapacity: false)
        activeDetailRequest?.1.cancel()
        activeDetailRequest = nil
    }

    private func cancelAllRequests() {
        cancelReadRequests()
        activeExportRequest?.1.cancel()
        activeExportRequest = nil
    }

    private func setupTable(_ table: NSTableView, label accessibilityLabel: String, columns: [TableColumnSpec]) {
        table.headerView = NSTableHeaderView()
        table.rowHeight = max(30, TerminalTheme.tableRowHeight)
        table.setAccessibilityLabel(accessibilityLabel)
        for spec in columns {
            let column = NSTableColumn(identifier: NSUserInterfaceItemIdentifier(spec.id))
            column.title = spec.title
            column.width = spec.width
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

    private func makePanel(title: String, content: NSView) -> NSBox {
        let box = NSBox()
        box.boxType = .custom
        box.cornerRadius = 6
        box.borderWidth = 1
        box.borderColor = TerminalTheme.border
        box.fillColor = TerminalTheme.panel
        let heading = label(title)
        heading.font = TerminalTheme.systemFont(ofSize: 14, weight: .semibold)
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
        return box
    }

    private func labeledControl(_ title: String, _ view: NSView) -> NSStackView {
        let heading = label(title)
        heading.font = TerminalTheme.systemFont(ofSize: 11, weight: .medium)
        heading.textColor = TerminalTheme.secondaryText
        let stack = verticalStack([heading, view], spacing: 4)
        stack.alignment = .leading
        return stack
    }

    private func verticalStack(_ views: [NSView], spacing: CGFloat) -> NSStackView {
        let stack = NSStackView(views: views)
        stack.orientation = .vertical
        stack.alignment = .leading
        stack.spacing = spacing
        stack.translatesAutoresizingMaskIntoConstraints = false
        for view in views where view is NSScrollView || view is NSStackView
            || view is NSTextField || view is AutomationReviewCandleChartView {
            view.widthAnchor.constraint(equalTo: stack.widthAnchor).isActive = true
        }
        return stack
    }

    private func label(_ text: String) -> NSTextField {
        let field = NSTextField(wrappingLabelWithString: text)
        field.maximumNumberOfLines = 0
        return field
    }

    private func configure(_ button: NSButton, title: String, selector: Selector, label: String) {
        button.title = title
        button.bezelStyle = .rounded
        button.target = self
        button.action = selector
        button.setAccessibilityLabel(label)
    }

    private enum ExportFailure: LocalizedError {
        case missingDirectory
        case invalidFileName
        case destinationBusy

        var errorDescription: String? {
            switch self {
            case .missingDirectory: return "선택한 폴더를 찾을 수 없습니다."
            case .invalidFileName: return "서버 manifest에 허용되지 않은 파일 이름이 있습니다."
            case .destinationBusy: return "새 복기 폴더 이름을 확보하지 못했습니다."
            }
        }
    }
}
