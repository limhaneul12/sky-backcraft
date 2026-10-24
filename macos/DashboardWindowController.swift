import AppKit

struct DashboardDatabaseStatusSummary: Equatable {
    let historyRunCount: Int
    let policyCount: Int

    init(historyRunCount: Int, policies: [RealPolicy]) {
        self.historyRunCount = historyRunCount
        policyCount = policies.count
    }

    var label: String {
        "원장 · \(historyRunCount) 실행 · \(policyCount) 정책"
    }
}

extension NSTableView {
    func removeAllColumns() {
        while let col = tableColumns.first {
            removeTableColumn(col)
        }
    }
}

private final class DashboardSidebarSymbolView: NSImageView {
    func updateTint(isSelected: Bool, isEmphasized: Bool) {
        if isSelected {
            contentTintColor = isEmphasized ? .alternateSelectedControlTextColor : .labelColor
        } else {
            contentTintColor = TerminalTheme.secondaryText
        }
    }
}

private final class HistoryRunsTableView: NSTableView {
    var openSelectedRun: (() -> Void)?
    var clearRunSelection: (() -> Void)?

    override func keyDown(with event: NSEvent) {
        switch event.keyCode {
        case 36, 76:
            openSelectedRun?()
        case 53:
            clearRunSelection?()
        default:
            super.keyDown(with: event)
        }
    }

    override func menu(for event: NSEvent) -> NSMenu? {
        let point = convert(event.locationInWindow, from: nil)
        let row = row(at: point)
        guard row >= 0 else { return nil }
        if !selectedRowIndexes.contains(row) {
            selectRowIndexes(IndexSet(integer: row), byExtendingSelection: false)
        }
        return super.menu(for: event)
    }
}

private final class HistoryWorkbenchSplitView: NSSplitView {
    var orientationDidChange: ((Bool) -> Void)?
    private var restoredDividerForCurrentOrientation = false

    override func layout() {
        let shouldBeVertical = bounds.width + 40 >= 900
        if isVertical != shouldBeVertical {
            isVertical = shouldBeVertical
            restoredDividerForCurrentOrientation = false
        }
        super.layout()
        guard !restoredDividerForCurrentOrientation, bounds.width > 0, bounds.height > 0 else { return }
        restoredDividerForCurrentOrientation = true
        orientationDidChange?(isVertical)
    }
}

enum Formatters {
    static func percent(_ value: Decimal, signed: Bool = false) -> String {
        let magnitude = value < 0 ? -value : value
        let digits = feeFormatter.string(from: NSDecimalNumber(decimal: magnitude * 100)) ?? "—"
        return (value < 0 ? "−" : (signed ? "+" : "")) + digits + "%"
    }
    static func money(_ value: Decimal, signed: Bool = false) -> String {
        let magnitude = value < 0 ? -value : value
        let digits = feeFormatter.string(from: NSDecimalNumber(decimal: magnitude)) ?? "—"
        return (value < 0 ? "−" : (signed ? "+" : "")) + "₩" + digits
    }
    static let pnlFormatter: NumberFormatter = {
        let f = NumberFormatter()
        f.numberStyle = .decimal
        f.locale = Locale(identifier: "ko_KR")
        f.minimumFractionDigits = 2
        f.maximumFractionDigits = 2
        f.positivePrefix = "+"
        return f
    }()

    static let feeFormatter: NumberFormatter = {
        let f = NumberFormatter()
        f.numberStyle = .decimal
        f.locale = Locale(identifier: "ko_KR")
        f.minimumFractionDigits = 2
        f.maximumFractionDigits = 2
        return f
    }()

    static let integerFormatter: NumberFormatter = {
        let f = NumberFormatter()
        f.numberStyle = .decimal
        f.maximumFractionDigits = 0
        return f
    }()

    static func pnl(_ val: Double) -> String {
        return pnlFormatter.string(from: NSNumber(value: val)) ?? String(format: "%+.2f", val)
    }

    static func fee(_ val: Double) -> String {
        return feeFormatter.string(from: NSNumber(value: val)) ?? String(format: "%.2f", val)
    }

    static func intVal(_ val: Double) -> String {
        return integerFormatter.string(from: NSNumber(value: val)) ?? String(format: "%.0f", val)
    }
}

// MARK: - Native Equity & Drawdown Chart View (CoreGraphics & NSBezierPath)

enum ChartMode: Int {
    case equity = 0
    case drawdown = 1
}

struct EquityFillMarker {
    let timestamp: Date
    let side: String
    let fillId: String
    let sequence: UInt64
    let simulated: Bool
}

final class EquityChartView: NSView {
    // AppKit can pass an expanded dirty rect; chart drawing must stay in its pane.
    override var wantsDefaultClipping: Bool { true }
    var title: String = "자산 평가 곡선 (Real Equity Curve)"
    var emptyMessage = "실행과 모델을 선택하세요. 임의의 수익 곡선은 표시하지 않습니다."
    private(set) var points: [RealAccountMark] = []
    private var fullPoints: [RealAccountMark] = []
    private var fullDrawdowns: [Double] = []
    private var visibleBounds: Range<Int> = 0..<0
    private var fillMarkers: [EquityFillMarker] = []
    private var fillTotal = 0
    private var fillMessage: String?

    private var initialEquity: Double = 300000.0
    private(set) var mode: ChartMode = .equity
    private var trackingArea: NSTrackingArea?
    private var focusedPoint: Int?
    private var displayTitle: String {
        let base = mode == .equity ? title : title.replacingOccurrences(of: "계좌 평가액 KRW", with: "낙폭 (%)")
        return base + (points.count < fullPoints.count ? " · 확대 · 지표는 전체 구간" : "")
    }
    private func pointValue(at index: Int) -> String {
        if mode == .equity { return "평가액 \(Formatters.money(points[index].equityDecimal))" }
        let drawdown = fullDrawdowns[visibleBounds.lowerBound + index]
        return "낙폭 \(String(format: "%.2f%%", -drawdown * 100))"
    }

    override var acceptsFirstResponder: Bool { true }

    override func updateTrackingAreas() {
        super.updateTrackingAreas()
        if let trackingArea { removeTrackingArea(trackingArea) }
        let area = NSTrackingArea(rect: .zero, options: [.mouseMoved, .mouseEnteredAndExited, .activeInKeyWindow, .inVisibleRect], owner: self)
        trackingArea = area
        addTrackingArea(area)
    }

    override func mouseDown(with event: NSEvent) {
        window?.makeFirstResponder(self)
        mouseMoved(with: event)
    }

    override func mouseMoved(with event: NSEvent) {
        guard !points.isEmpty else { return }
        let x = convert(event.locationInWindow, from: nil).x
        let fraction = min(max((x - 54) / max(bounds.width - 108, 1), 0), 1)
        if let first = points.first, let last = points.last, last.timestamp > first.timestamp {
            let requested = first.timestamp.addingTimeInterval(Double(fraction) * last.timestamp.timeIntervalSince(first.timestamp))
            focusedPoint = points.indices.min { abs(points[$0].timestamp.timeIntervalSince(requested)) < abs(points[$1].timestamp.timeIntervalSince(requested)) }
        } else { focusedPoint = Int((fraction * CGFloat(points.count - 1)).rounded()) }
        updatePointSummary()
    }

    override func keyDown(with event: NSEvent) {
        guard !points.isEmpty else { super.keyDown(with: event); return }
        if event.keyCode == 123 || event.keyCode == 124 {
            let delta = event.keyCode == 123 ? -1 : 1
            focusedPoint = min(max((focusedPoint ?? 0) + delta, 0), points.count - 1)
            updatePointSummary()
        } else if event.keyCode == 115 {
            focusedPoint = 0
            updatePointSummary()
        } else if event.keyCode == 119 {
            focusedPoint = points.count - 1
            updatePointSummary()
        } else if ["+", "="].contains(event.charactersIgnoringModifiers ?? "") { zoom(by: 2) }
        else if event.charactersIgnoringModifiers == "-" { zoom(by: 0.5) }
        else if event.charactersIgnoringModifiers == "0" { resetZoom() }
        else { super.keyDown(with: event) }
    }

    override func magnify(with event: NSEvent) { zoom(by: max(0.1, 1 + Double(event.magnification))) }

    func zoom(by factor: Double) {
        guard fullPoints.count > 2, factor > 0 else { return }
        let center = visibleBounds.lowerBound + (focusedPoint ?? (points.count / 2))
        let count = min(fullPoints.count, max(2, Int(Double(max(points.count, 2)) / factor)))
        let lower = min(max(center - count / 2, 0), fullPoints.count - count)
        visibleBounds = lower..<(lower + count)
        points = Array(fullPoints[visibleBounds])
        focusedPoint = min(max(center - lower, 0), points.count - 1)
        updatePointSummary()
    }

    func resetZoom() {
        visibleBounds = 0..<fullPoints.count
        points = fullPoints
        focusedPoint = nil
        setAccessibilityLabel(displayTitle)
        setAccessibilityValue("전체 원장 구간 · 화면용 \(points.count) 포인트")
        needsDisplay = true
    }

    func setFillMarkers(_ markers: [EquityFillMarker], total: Int, message: String? = nil) {
        fillMarkers = markers
        fillTotal = total
        fillMessage = message
        needsDisplay = true
    }

    private func updatePointSummary() {
        guard let focusedPoint, points.indices.contains(focusedPoint) else { return }
        let point = points[focusedPoint]
        let clock = DateFormatter()
        clock.timeZone = TimeZone(identifier: "Asia/Seoul")
        clock.dateFormat = "yyyy-MM-dd HH:mm"
        let nearby = fillMarkers.filter { abs($0.timestamp.timeIntervalSince(point.timestamp)) <= 60 }
        let event = nearby.first.map { " · \($0.simulated ? "가상" : "관측") \($0.side == "BUY" ? "매수" : "매도") 체결 \($0.fillId)" } ?? ""
        let value = "\(clock.string(from: point.timestamp)) KST · \(pointValue(at: focusedPoint)) · 원장 순번 \(point.seq)" + event
        toolTip = value
        setAccessibilityElement(true)
        setAccessibilityRole(.image)
        setAccessibilityLabel(displayTitle)
        setAccessibilityValue(value)
        NSAccessibility.post(element: self, notification: .valueChanged)
        needsDisplay = true
    }

    private func timeRatio(at index: Int) -> CGFloat {
        guard let first = points.first, let last = points.last else { return 0 }
        let span = last.timestamp.timeIntervalSince(first.timestamp)
        if span > 0 { return CGFloat(points[index].timestamp.timeIntervalSince(first.timestamp) / span) }
        return CGFloat(index) / CGFloat(max(points.count - 1, 1))
    }

    func setPoints(_ newPoints: [RealAccountMark], initialEquity: Double = 300000.0) {
        self.fullPoints = newPoints
        self.points = newPoints
        visibleBounds = 0..<newPoints.count
        self.initialEquity = initialEquity
        var peak = initialEquity
        fullDrawdowns = newPoints.map { point in
            peak = max(peak, point.equity)
            return point.drawdownDecimal.map { NSDecimalNumber(decimal: $0).doubleValue }
                ?? (peak > 0 ? (peak - point.equity) / peak : 0)
        }
        focusedPoint = nil
        setAccessibilityElement(true)
        setAccessibilityRole(.image)
        setAccessibilityLabel(displayTitle)
        setAccessibilityValue(newPoints.isEmpty ? emptyMessage : "원장 화면용 \(newPoints.count) 포인트. 좌우 키로 시각과 평가액을 확인합니다.")
        needsDisplay = true
    }

    func setMode(_ newMode: ChartMode) {
        self.mode = newMode
        setAccessibilityLabel(displayTitle)
        updatePointSummary()
        needsDisplay = true
    }

    override func draw(_ dirtyRect: NSRect) {
        super.draw(dirtyRect)
        guard let context = NSGraphicsContext.current?.cgContext else { return }

        // Dark slate card canvas background
        let bgColor = TerminalTheme.chart
        bgColor.setFill()
        bounds.fill()

        let insetRect = bounds.insetBy(dx: 54, dy: 36)
        guard insetRect.width > 20, insetRect.height > 20 else { return }

        if points.count < 2 {
            let noDataAttrs: [NSAttributedString.Key: Any] = [
                .font: NSFont.systemFont(ofSize: 13, weight: .medium),
                .foregroundColor: TerminalTheme.secondaryText
            ]
            let msg = emptyMessage
            let strSize = (msg as NSString).size(withAttributes: noDataAttrs)
            (msg as NSString).draw(
                at: NSPoint(x: bounds.midX - strSize.width / 2, y: bounds.midY - strSize.height / 2),
                withAttributes: noDataAttrs
            )
            return
        }

        switch mode {
        case .equity:
            drawEquityCurve(in: insetRect, context: context)
        case .drawdown:
            drawDrawdownCurve(in: insetRect, context: context)
        }

        drawFillMarkers(in: insetRect)
        if window?.firstResponder === self {
            NSColor.keyboardFocusIndicatorColor.setStroke()
            let focus = NSBezierPath(rect: bounds.insetBy(dx: 2, dy: 2))
            focus.lineWidth = 2
            focus.stroke()
        }

        // Draw title in top-left
        let titleAttrs: [NSAttributedString.Key: Any] = [
            .font: NSFont.systemFont(ofSize: 11, weight: .semibold),
            .foregroundColor: TerminalTheme.secondaryText
        ]
        (displayTitle as NSString).draw(at: NSPoint(x: insetRect.minX, y: bounds.height - 24), withAttributes: titleAttrs)
        if let focusedPoint, points.indices.contains(focusedPoint) {
            let point = points[focusedPoint]
            let clock = DateFormatter()
            clock.timeZone = TimeZone(identifier: "Asia/Seoul")
            clock.dateFormat = "MM-dd HH:mm"
            let info = "\(clock.string(from: point.timestamp)) KST · \(pointValue(at: focusedPoint))"
            (info as NSString).draw(at: NSPoint(x: insetRect.minX, y: 10), withAttributes: titleAttrs)
        } else if let first = points.first, let last = points.last {
            let clock = DateFormatter()
            clock.timeZone = TimeZone(identifier: "Asia/Seoul")
            clock.dateFormat = "MM-dd HH:mm"
            let range = "\(clock.string(from: first.timestamp)) → \(clock.string(from: last.timestamp)) KST"
            (range as NSString).draw(at: NSPoint(x: insetRect.minX, y: 10), withAttributes: titleAttrs)
        }
    }

    private func drawFillMarkers(in rect: NSRect) {
        guard let first = points.first, let last = points.last else { return }
        let span = last.timestamp.timeIntervalSince(first.timestamp)
        guard span > 0 else { return }
        let visible = fillMarkers.filter { $0.timestamp >= first.timestamp && $0.timestamp <= last.timestamp }
        for marker in visible {
            let ratio = marker.timestamp.timeIntervalSince(first.timestamp) / span
            let x = rect.minX + CGFloat(ratio) * rect.width
            let y = rect.minY - 10
            let buy = marker.side == "BUY"
            let path = NSBezierPath()
            path.move(to: NSPoint(x: x, y: y + (buy ? 4 : -4)))
            path.line(to: NSPoint(x: x - 4, y: y + (buy ? -4 : 4)))
            path.line(to: NSPoint(x: x + 4, y: y + (buy ? -4 : 4)))
            path.close()
            (buy ? TerminalTheme.positive : TerminalTheme.negative).setFill()
            path.fill()
        }
        if let fillMessage {
            let text = fillMessage + (fillMarkers.isEmpty ? "" : " · ▲ 매수 ▼ 매도 · 표시 \(visible.count)/원장 \(fillTotal) 체결")
            (text as NSString).draw(at: NSPoint(x: rect.minX, y: rect.maxY - 16), withAttributes: [
                .font: NSFont.systemFont(ofSize: 10), .foregroundColor: TerminalTheme.secondaryText
            ])
        }
    }

    private func drawEquityCurve(in insetRect: NSRect, context: CGContext) {
        var minVal = initialEquity
        var maxVal = initialEquity
        for p in points {
            if p.equity < minVal { minVal = p.equity }
            if p.equity > maxVal { maxVal = p.equity }
        }
        if minVal == maxVal {
            let span = max(1, abs(initialEquity) * 0.01)
            minVal -= span
            maxVal += span
        }
        let padding = (maxVal - minVal) * 0.1
        minVal -= padding
        maxVal += padding
        let valRange = maxVal - minVal

        // Draw horizontal grid lines & price labels
        let gridLines = 5
        let gridColor = TerminalTheme.border
        let labelAttrs: [NSAttributedString.Key: Any] = [
            .font: NSFont.monospacedDigitSystemFont(ofSize: 10, weight: .regular),
            .foregroundColor: TerminalTheme.secondaryText
        ]

        for i in 0...gridLines {
            let ratio = CGFloat(i) / CGFloat(gridLines)
            let y = insetRect.minY + ratio * insetRect.height
            let line = NSBezierPath()
            line.move(to: NSPoint(x: insetRect.minX, y: y))
            line.line(to: NSPoint(x: insetRect.maxX, y: y))
            gridColor.setStroke()
            line.lineWidth = 1.0
            line.stroke()

            let val = minVal + Double(ratio) * valRange
            let formatted = Formatters.intVal(val)
            (formatted as NSString).draw(at: NSPoint(x: 6, y: y - 6), withAttributes: labelAttrs)
        }

        // Baseline (Initial Equity)
        let baseRatio = CGFloat((initialEquity - minVal) / valRange)
        let baseY = insetRect.minY + baseRatio * insetRect.height
        let baseLine = NSBezierPath()
        baseLine.move(to: NSPoint(x: insetRect.minX, y: baseY))
        baseLine.line(to: NSPoint(x: insetRect.maxX, y: baseY))
        let baseDash: [CGFloat] = [4, 4]
        baseLine.setLineDash(baseDash, count: 2, phase: 0)
        NSColor(calibratedRed: 0.5, green: 0.6, blue: 0.7, alpha: 0.35).setStroke()
        baseLine.lineWidth = 1.0
        baseLine.stroke()

        // Build curve path
        let path = NSBezierPath()
        for (i, p) in points.enumerated() {
            let xRatio = timeRatio(at: i)
            let yRatio = CGFloat((p.equity - minVal) / valRange)
            let pt = NSPoint(x: insetRect.minX + xRatio * insetRect.width, y: insetRect.minY + yRatio * insetRect.height)
            if i == 0 { path.move(to: pt) } else { path.line(to: pt) }
        }

        // A neutral canvas and explicit line preserve readability in both appearances.
        let terminal = points.last?.equity ?? initialEquity
        let curveColor = terminal > initialEquity ? TerminalTheme.positive
            : (terminal < initialEquity ? TerminalTheme.negative : TerminalTheme.secondaryText)
        curveColor.setStroke()
        path.lineWidth = 2.0
        path.stroke()
    }

    private func drawDrawdownCurve(in insetRect: NSRect, context: CGContext) {
        var drawdowns: [Double] = []
        var maxDd = 0.0
        for index in points.indices {
            let dd = -fullDrawdowns[visibleBounds.lowerBound + index]
            drawdowns.append(dd)
            maxDd = max(maxDd, -dd)
        }
        maxDd = max(maxDd * 1.1, 0.001)

        let gridLines = 4
        let gridColor = TerminalTheme.border
        let labelAttrs: [NSAttributedString.Key: Any] = [
            .font: NSFont.monospacedDigitSystemFont(ofSize: 10, weight: .regular),
            .foregroundColor: TerminalTheme.secondaryText
        ]

        for i in 0...gridLines {
            let ratio = CGFloat(i) / CGFloat(gridLines)
            let y = insetRect.minY + ratio * insetRect.height
            let line = NSBezierPath()
            line.move(to: NSPoint(x: insetRect.minX, y: y))
            line.line(to: NSPoint(x: insetRect.maxX, y: y))
            gridColor.setStroke()
            line.lineWidth = 1.0
            line.stroke()

            let val = -maxDd * (1.0 - Double(ratio))
            let formatted = String(format: "%.1f%%", val * 100)
            (formatted as NSString).draw(at: NSPoint(x: 6, y: y - 6), withAttributes: labelAttrs)
        }

        // 0% Top Baseline
        let zeroY = insetRect.maxY
        let zeroLine = NSBezierPath()
        zeroLine.move(to: NSPoint(x: insetRect.minX, y: zeroY))
        zeroLine.line(to: NSPoint(x: insetRect.maxX, y: zeroY))
        NSColor(calibratedRed: 0.5, green: 0.6, blue: 0.7, alpha: 0.4).setStroke()
        zeroLine.lineWidth = 1.0
        zeroLine.stroke()

        // Build drawdown underwater path
        let path = NSBezierPath()
        for (i, dd) in drawdowns.enumerated() {
            let xRatio = timeRatio(at: i)
            let yRatio = CGFloat(1.0 + (dd / maxDd))
            let pt = NSPoint(x: insetRect.minX + xRatio * insetRect.width, y: insetRect.minY + yRatio * insetRect.height)
            if i == 0 { path.move(to: pt) } else { path.line(to: pt) }
        }

        // Fill underwater area up to 0% line
        if let fillPath = path.copy() as? NSBezierPath {
            fillPath.line(to: NSPoint(x: insetRect.maxX, y: insetRect.maxY))
            fillPath.line(to: NSPoint(x: insetRect.minX, y: insetRect.maxY))
            fillPath.close()

            context.saveGState()
            fillPath.addClip()
            let gradient = NSGradient(
                starting: NSColor(calibratedRed: 0.95, green: 0.25, blue: 0.25, alpha: 0.0),
                ending: NSColor(calibratedRed: 0.95, green: 0.25, blue: 0.25, alpha: 0.35)
            )
            gradient?.draw(in: insetRect, angle: 90)
            context.restoreGState()
        }

        // Stroke drawdown line
        TerminalTheme.negative.setStroke()
        path.lineWidth = 2.0
        path.stroke()
    }
}

// MARK: - Native Dashboard Window Controller

final class DashboardWindowController: NSWindowController, NSWindowDelegate, NSTableViewDataSource, NSTableViewDelegate, NSSplitViewDelegate, NSToolbarDelegate {
    private let store: ConfigurationStore
    private let dbReader: DatabaseReader
    private let liveService: UpbitLiveService
    private let liveWallNow: () -> Date
    private let liveMonotonicNow: () -> TimeInterval
    private let historyWorkspace: HistoryWorkspaceStore
    private var restoredHistoryState: HistoryPresentationState?
    private var historyWorkspaceError: String?
    private var applyingHistoryState = false
    private var lastOpenedHistoryRunId: String?
    private let ledgerClient = LedgerMCPReader()

    // Layout
    private let splitView = NSSplitView()
    private let sidebarTable = NSTableView()
    private let detailContainer = TerminalSurfaceView()
    private var databaseStatusFooter: NSTextField?
    private var databaseStatusFooterIcon: NSImageView?
    private var paperSummaryText = "지속 모의: 조회 전"
    private var automationSummaryText = "전략 자동화: 조회 전"
    private struct GlobalContextFact: Equatable {
        let domain: String
        let value: String
    }
    private var globalContextFacts: [GlobalContextFact] = []
    private var globalContextStrip: NSView?
    private var globalContextContent: NSView?
    private let globalContextPopover = NSPopover()
    private let realtimeStatusDisclosure = NSButton(title: "실시간 상태 접기", target: nil, action: nil)
    private var realtimeStatusCollapsed = true
    private var isSidebarCollapsed = false
    private var sidebarWidthBeforeCollapse: CGFloat = TerminalTheme.textScale >= 1.5 ? 280 : 230
    private static let sidebarToolbarIdentifier = NSToolbarItem.Identifier("SkyBackcraft.ToggleSidebar")
    private static let sidebarSeparatorIdentifier = NSToolbarItem.Identifier("SkyBackcraft.SidebarSeparator")
    private static let historyDividerDefaultsKey = "dev.skybackcraft.run-history-divider-ratio.v1"

    // Navigation sections
    private enum Section: Int, CaseIterable {
        case backtestAnalysis = 0
        case allRunsHistory
        case strategyCatalog
        case paperTradingTelemetry
        case strategyAutomation
        case liveTradingMonitor
        case policyManagement

        var sidebarRow: Int {
            switch self {
            case .backtestAnalysis: return 1
            case .allRunsHistory: return 2
            case .strategyCatalog: return 3
            case .paperTradingTelemetry: return 5
            case .strategyAutomation: return 6
            case .liveTradingMonitor: return 8
            case .policyManagement: return 10
            }
        }

        var symbolName: String {
            switch self {
            case .backtestAnalysis: return "chart.xyaxis.line"
            case .allRunsHistory: return "clock.arrow.circlepath"
            case .strategyCatalog: return "list.bullet.rectangle"
            case .paperTradingTelemetry: return "waveform.path.ecg"
            case .strategyAutomation: return "gearshape.2"
            case .liveTradingMonitor: return "eye"
            case .policyManagement: return "slider.horizontal.3"
            }
        }

        var title: String {
            switch self {
            case .backtestAnalysis: return "성과 분석"
            case .allRunsHistory: return "실행 이력"
            case .strategyCatalog: return "정책 카탈로그"
            case .paperTradingTelemetry: return "지속 모의 · 작업"
            case .strategyAutomation: return "전략 자동화"
            case .liveTradingMonitor: return "실계좌 및 주문 조회"
            case .policyManagement: return "정책 삭제"
            }
        }
    }

    private enum SidebarRow {
        case group(String)
        case route(Section)

        static let ordered: [SidebarRow] = [
            .group("연구"), .route(.backtestAnalysis), .route(.allRunsHistory), .route(.strategyCatalog),
            .group("운영"), .route(.paperTradingTelemetry), .route(.strategyAutomation),
            .group("계좌 관찰"), .route(.liveTradingMonitor),
            .group("관리"), .route(.policyManagement)
        ]

        var isGroup: Bool {
            if case .group = self { return true }
            return false
        }

        var section: Section? {
            if case .route(let section) = self { return section }
            return nil
        }
    }

    private enum HistoryFilterTokenKind: Int {
        case search
        case status
        case market
        case policy
        case revision
        case cost
        case date
        case pinned
    }
    private var currentSection: Section = .backtestAnalysis
    private var synchronizingSidebarSelection = false

    // Real Data Caches
    private var allRuns: [RealRun] = []
    private var filteredRuns: [RealRun] = []
    private var selectedRun: RealRun?
    private var selectedStrategyId: String?
    private var showingRunDetails = false
    private var strategyOverview: StrategyPerformanceView?
    private var policyInventory: PolicyInventoryView?
    private var paperWorkspaceView: PaperTradingWorkspaceView?
    private var automationWorkspaceView: AutomationWorkspaceView?
    private let realtimeFeedService = RealtimeFeedMCPService.shared
    private lazy var realtimeStatusView = RealtimeConnectionStatusView(service: realtimeFeedService)
    private var realtimeStatusHeightConstraint: NSLayoutConstraint?
    private var realtimeFeedObserverID: UUID?
    private var privateFeedInvalidation = RealtimeReconciliationInvalidation()
    private var ledgerDetailController: ModelLedgerDetailWindowController?
    private let fillReader = LedgerDetailMCPReader()
    private var markerGeneration: UInt64 = 0
    private let exportQueue = DispatchQueue(label: "dev.skybackcraft.history-export")
    private var selectedModelId: String?
    private weak var runDetailLedgerButton: NSButton?
    private weak var runDetailExportButton: NSButton?
    private weak var runDetailMarkerButton: NSButton?
    private let contextLabel = NSTextField(wrappingLabelWithString: "실행을 선택하세요.")
    private let modelSelector = NSPopUpButton()
    private var dataError: String?
    private var currentComparisons: [RealModelComparison] = []
    private var allPolicies: [RealPolicy] = []
    private var recentJobs: [RealJob] = []

    // Section 1 UI elements
    private let runSelector = NSPopUpButton()
    private let chartModeControl = NSSegmentedControl(labels: ["자산 곡선", "낙폭"], trackingMode: .selectOne, target: nil, action: nil)
    private let chartView = EquityChartView()
    private let returnMetricLabel = NSTextField(labelWithString: "0.00%")
    private let mddMetricLabel = NSTextField(labelWithString: "0.00%")
    private let pnlMetricLabel = NSTextField(labelWithString: "0 KRW")
    private let feesMetricLabel = NSTextField(labelWithString: "0 KRW")
    private let tradesMetricLabel = NSTextField(labelWithString: "0건")
    private let capitalMetricLabel = NSTextField(labelWithString: "300,000 KRW")
    private let comparisonsTable = NSTableView()
    private var synchronizingComparisonSelection = false
    private let inspectorText = NSTextView()
    private var inspectorScroll: NSScrollView?
    private var inspectorWidth: NSLayoutConstraint?
    private var inspectorGap: NSLayoutConstraint?
    private var inspectorExpanded: Bool?


    // Section 2 UI elements
    private let runsSearchField = NSSearchField()
    private let runsTable = HistoryRunsTableView()
    private let historyStatus = NSPopUpButton()
    private let historyMarket = NSPopUpButton()
    private let historyPolicy = NSPopUpButton()
    private let historyRevision = NSPopUpButton()
    private let historyCost = NSPopUpButton()
    private let historyDateEnabled = NSButton(checkboxWithTitle: "기록일(KST)", target: nil, action: nil)
    private let historyFromDate = NSDatePicker()
    private let historyThroughDate = NSDatePicker()
    private let historyPinnedOnly = NSButton(checkboxWithTitle: "고정만 보기", target: nil, action: nil)
    private let historyClearFilters = NSButton(title: "필터 모두 지우기", target: nil, action: nil)
    private let historyDisplayMode = NSSegmentedControl(labels: ["컴팩트", "상세 열"], trackingMode: .selectOne, target: nil, action: nil)
    private let historyCompare = NSButton(title: "선택 비교", target: nil, action: nil)
    private let historyRecent = NSButton(title: "최근 실행", target: nil, action: nil)
    private let historyMetricSort = NSPopUpButton()
    private let historyWorkspaceLabel = NSTextField(wrappingLabelWithString: "")
    private var historyMetricValues: [String: Decimal] = [:]
    private var historyMetricAvailability: [String: HistoryMetricAvailability] = [:]
    private var historyFilterOptions: RunHistoryFilterOptions?
    private var historyFiltersConfigured = false
    private var historyPinnedIds = Set<String>()
    private var historySelection: [String: RealRun] = [:]
    private var restoringHistorySelection = false
    private var runComparisonController: RunComparisonWindowController?
    private let historyCount = NSTextField(labelWithString: "")
    private let historyMore = NSButton(title: "다음 기록 50건", target: nil, action: nil)
    private let historyDetail = NSTextField(wrappingLabelWithString: "행을 선택하면 실행의 정책·데이터셋 근거를 확인할 수 있습니다.")
    private let historyIdentityRows = NSStackView()
    private let historyAppliedFiltersSummary = NSTextField(wrappingLabelWithString: "활성 필터 없음")
    private let historyOpen = NSButton(title: "성과에서 열기", target: nil, action: nil)
    private let historyAdvancedFiltersDisclosure = NSButton(title: "고급 필터", target: nil, action: nil)
    private var historyAdvancedFiltersPanel: NSStackView?
    private var historyTableScroll: NSScrollView?
    private var historyWorkbenchSplitView: HistoryWorkbenchSplitView?
    private var historyCopyMenuItem: NSMenuItem?
    private var historyPinMenuItem: NSMenuItem?
    private var historyClearMenuItem: NSMenuItem?
    private var historyContextOpenMenuItem: NSMenuItem?
    private var historyContextCompareMenuItem: NSMenuItem?
    private var historyContextPinMenuItem: NSMenuItem?
    private var historyContextCopyMenuItem: NSMenuItem?
    private let historyActionsPullDown = NSPopUpButton(frame: .zero, pullsDown: true)
    private var historySplitView: HistoryWorkbenchSplitView? { historyWorkbenchSplitView }
    private var historyCursor: RunHistoryCursor?
    private var historyTotal = 0
    private var pausedSchedules: [RealCollectionSchedule] = []
    private var activeJobs: [RealJob] = []

    private enum JobsListItem {
        case job(RealJob)
        case schedule(RealCollectionSchedule)
    }

    private var visibleJobsListItems: [JobsListItem] {
        switch jobsListScope.selectedSegment {
        case 1: return activeJobs.map(JobsListItem.job)
        case 2: return pausedSchedules.map(JobsListItem.schedule)
        default: return recentJobs.map(JobsListItem.job)
        }
    }

    // Section 4 UI elements
    private let jobsTable = NSTableView()
    private let jobsListScope = NSSegmentedControl(labels: ["최근 30건", "대기·실행", "일시중지 수집"], trackingMode: .selectOne, target: nil, action: nil)
    private var jobsQueueHeader: NSTextField?

    // Section 5 UI elements
    private let upbitBalanceLabel = NSTextField(wrappingLabelWithString: "Upbit 실계좌 잔고 조회 전")
    private let balanceReadStatus = NSTextField(wrappingLabelWithString: "잔고 · UNKNOWN · 아직 관측하지 않음")
    private let privacyMaskButton = NSButton()
    private var isPrivacyMasked: Bool = true
    private let openOrdersTable = NSTableView()
    private let liveExchangeLabel = NSTextField(labelWithString: "EXCHANGE READ UNKNOWN")
    private let liveTradeLockLabel = NSTextField(labelWithString: "LIVE ORDERS DISABLED · 확인 전")
    private let liveStateLabel = NSTextField(labelWithString: "EXECUTION STATUS UNKNOWN · 거래 허용 근거 없음")
    private let liveAuditLabel = NSTextField(wrappingLabelWithString: "계좌와 주문은 Upbit 읽기 응답만 표시합니다. 과거·모의 체결과 혼합하지 않습니다.")
    private let liveRefreshButton = NSButton(title: "전체 새로고침", target: nil, action: nil)
    private let automaticLiveReadButton = NSButton(checkboxWithTitle: "자동 조회 (30초)", target: nil, action: nil)
    private let orderDetailLabel = NSTextField(wrappingLabelWithString: "주문을 선택하면 전체 주문 ID와 가격·수량·수수료 근거를 표시합니다.")
    private let copyOrderIdButton = NSButton(title: "주문 ID 복사", target: nil, action: nil)
    private var balanceFreshness = ReadFreshness<UpbitAccountSnapshot>(context: .liveProvider)
    private var openOrdersFreshness = ReadFreshness<UpbitOrdersSnapshot>(context: .liveProvider)
    private var closedOrdersFreshness = ReadFreshness<UpbitOrdersSnapshot>(context: .liveProvider)
    private var controlFreshness = ReadFreshness<TradingControlSnapshot>(context: .liveProvider)
    private var balanceRequest: NativeMCPRequest?
    private var openOrdersRequest: NativeMCPRequest?
    private var closedOrdersRequest: NativeMCPRequest?
    private var controlRequest: NativeMCPRequest?
    private var balancePending = false
    private var openOrdersPending = false
    private var closedOrdersPending = false
    private var controlPending = false
    private var balanceTicket: UInt64 = 0
    private var openOrdersTicket: UInt64 = 0
    private var closedOrdersTicket: UInt64 = 0
    private var controlTicket: UInt64 = 0
    private var liveContextGeneration: UInt64 = 0
    private var liveTickTimer: Timer?
    private var nextAutomaticLiveRead: TimeInterval?
    private var liveRetryAllowedAt: TimeInterval?
    private var selectedOrderId: String?

    // Section 6 read projection and deletion workspace
    private var userPolicies: [RealPolicy] = []
    private var policyDeletionWorkspace: PolicyDeletionWorkspace?

    var onStateChange: ((LiveTradingState) -> Void)?
    var onOpenSettings: (() -> Void)?
    private let orderScope = NSSegmentedControl(labels: ["미체결", "완료"], trackingMode: .selectOne, target: nil, action: nil)
    private let orderReadStatus = NSTextField(wrappingLabelWithString: "주문 조회 전 · 0건과 조회 실패는 다른 상태입니다.")
    private let nextOrderPageButton = NSButton(title: "다음 미체결 페이지", target: nil, action: nil)
    private var currentOrderPage = 1

    init(
        store: ConfigurationStore,
        liveService: UpbitLiveService = .shared,
        wallNow: @escaping () -> Date = { Date() },
        monotonicNow: @escaping () -> TimeInterval = { ProcessInfo.processInfo.systemUptime }
    ) {
        self.store = store
        self.dbReader = DatabaseReader(supportDirectory: store.supportDirectory)
        self.liveService = liveService
        self.liveWallNow = wallNow
        self.liveMonotonicNow = monotonicNow
        let review = CommandLine.arguments.contains("--gui-review")
        let temporaryRoot = FileManager.default.temporaryDirectory.standardizedFileURL.path
        let storePath = store.supportDirectory.standardizedFileURL.path
        let isTemporaryStore = storePath.hasPrefix(temporaryRoot) || storePath.hasPrefix("/tmp/") || storePath.hasPrefix("/private/tmp/")
        let uiRoot = review && !isTemporaryStore
            ? URL(fileURLWithPath: "/tmp/sky-backcraft-p13-review-ui", isDirectory: true)
            : store.supportDirectory
        historyWorkspace = HistoryWorkspaceStore(rootDirectory: uiRoot)
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 1200, height: 820),
            styleMask: [.titled, .closable, .miniaturizable, .resizable, .fullSizeContentView],
            backing: .buffered,
            defer: false
        )
        window.title = CommandLine.arguments.contains("--gui-review")
            ? "Sky Backcraft — 검증 세션 · 원장 읽기 / Backend 복사본"
            : "Sky Backcraft"
        window.contentMinSize = NSSize(width: 1200, height: 800)
        window.isReleasedWhenClosed = false
        window.appearance = nil
        window.backgroundColor = TerminalTheme.surface
        window.titlebarAppearsTransparent = true
        window.toolbarStyle = .unified
        window.titleVisibility = .hidden
        super.init(window: window)
        window.delegate = self
        realtimeFeedObserverID = realtimeFeedService.observe { [weak self] presentation in
            guard let self else { return }
            _ = self.privateFeedInvalidation.observe(
                presentation.status?.private_feed.last_reconciled_at_utc,
                hasProjection: presentation.status != nil,
                isVisible: self.isLiveMonitorVisible
            )
        }

        do { restoredHistoryState = try historyWorkspace.loadHistoryState() }
        catch { historyWorkspaceError = "저장된 보기 상태 확인 실패 · 기존 파일은 유지됩니다. \(error.localizedDescription)" }
        loadData()
        setupUI()
        configureWindowToolbar()
        selectSection(.backtestAnalysis)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) { nil }

    deinit {
        if let realtimeFeedObserverID {
            realtimeFeedService.removeObserver(realtimeFeedObserverID)
        }
    }

    func showDashboard() {
        loadData()
        refreshCurrentSectionView()
        showWindow(nil)
        window?.center()
        NSApp.activate(ignoringOtherApps: true)
        updateStrategyOverviewVisibility()
        updateAutomationWorkspaceVisibility()
        updatePolicyDeletionWorkspaceVisibility()
        updateRealtimeFeedVisibility()
        if currentSection == .liveTradingMonitor { resumeLiveMonitoringIfVisible() }
    }

    func showAutomationWorkspace() {
        loadData()
        selectSection(.strategyAutomation)
        showWindow(nil)
        window?.center()
        NSApp.activate(ignoringOtherApps: true)
        updateAutomationWorkspaceVisibility()
        updateRealtimeFeedVisibility()
    }

    func updateRuntimeStatus(_ status: RuntimeStatus) {
        guard case .running = status else { return }
        loadData()
        if window?.isVisible == true {
            refreshCurrentSectionView()
        }
    }

    private func loadData() {
        do {
            let page = try dbReader.fetchRunHistoryPage(limit: 50)
            allRuns = page.items
            historyTotal = page.totalCount
            historyCursor = page.nextCursor
            let loadedPolicies = try dbReader.fetchAllPolicies()
            let policiesChanged = loadedPolicies.map {
                "\($0.policyId):\($0.headRevisionId):\($0.name):\($0.historicalRunCount):\($0.historicalModelCount)"
            } != allPolicies.map {
                "\($0.policyId):\($0.headRevisionId):\($0.name):\($0.historicalRunCount):\($0.historicalModelCount)"
            }
            allPolicies = loadedPolicies
            updateDatabaseStatusFooter()
            if policiesChanged {
                strategyOverview?.updatePolicies(allPolicies)
                policyInventory = nil
            }
            historyFilterOptions = try dbReader.fetchHistoryFilterOptions()
            userPolicies = allPolicies.filter { $0.origin == "USER" }
            recentJobs = try dbReader.fetchRecentJobs(limit: 30)
            pausedSchedules = try dbReader.fetchPausedCollectionSchedules(limit: 100)
            activeJobs = try dbReader.fetchActiveJobs(limit: 100)
            filteredRuns = allRuns
            policyDeletionWorkspace?.updatePolicies(userPolicies: userPolicies, allPolicies: allPolicies)
            automationWorkspaceView?.updatePolicies(allPolicies)
            logFixtureDatabaseReadResult(policyCount: allPolicies.count, error: nil)
            dataError = nil
            updateJobsQueuePresentation()
            if jobsTable.dataSource != nil { jobsTable.reloadData() }
            // An explicit selection is required; a completed replay is not live paper.
            if let current = selectedRun { loadRunDetails(current) }
            else { clearRunMetrics(message: "실행을 선택하면 해당 모델의 원장과 성과를 표시합니다.") }
        } catch {
            logFixtureDatabaseReadResult(policyCount: allPolicies.count, error: error)
            dataError = "원장 조회 실패 · 저장소 연결 및 스키마를 확인하세요."
            clearRunMetrics(message: dataError!)
        }
    }

    private func logFixtureDatabaseReadResult(policyCount: Int, error: Error?) {
        guard CommandLine.arguments.contains("--gui-review") else { return }
        let root = store.supportDirectory.resolvingSymlinksInPath().standardizedFileURL
        guard root.path.hasPrefix("/tmp/") || root.path.hasPrefix("/private/tmp/") else { return }

        let database = root.appendingPathComponent("research/lab.sqlite").path
        let log = root.appendingPathComponent("automation-gui-database-read.log")
        let timestamp = ISO8601DateFormatter().string(from: Date())
        let errorText = error?.localizedDescription ?? "none"
        let entry = "\(timestamp)\nsupport_root=\(root.path)\nresearch_database=\(database)\npolicy_count=\(policyCount)\nerror=\(errorText)\n\n"
        do {
            let bytes = Data(entry.utf8)
            if FileManager.default.fileExists(atPath: log.path) {
                let handle = try FileHandle(forWritingTo: log)
                defer { try? handle.close() }
                try handle.seekToEnd()
                try handle.write(contentsOf: bytes)
            } else {
                try bytes.write(to: log, options: .atomic)
            }
        } catch {
            // Fixture diagnostics must never change normal UI error handling.
        }
    }

    private func clearRunMetrics(message: String) {
        for label in [returnMetricLabel, mddMetricLabel, pnlMetricLabel, feesMetricLabel, tradesMetricLabel, capitalMetricLabel] {
            label.stringValue = "—"
            label.textColor = TerminalTheme.secondaryText
            if let box = label.superview?.superview as? NSBox {
                box.setAccessibilityValue("—")
            }
        }
        contextLabel.stringValue = message
        updateInspector()
        currentComparisons = []
        runDetailLedgerButton?.isEnabled = false
        runDetailExportButton?.isEnabled = selectedRun != nil
        runDetailMarkerButton?.isEnabled = false
        chartView.title = message
        chartView.emptyMessage = message
        chartView.setPoints([], initialEquity: 0)
        comparisonsTable.reloadData()
    }

    private func loadRunDetails(_ run: RealRun, requiredPolicyId: String? = nil, requiredRevisionId: String? = nil, requiredMarket: String? = nil) {
        selectedRun = run
        do {
            currentComparisons = try dbReader.fetchModelComparisons(for: run.id)
            if comparisonsTable.dataSource != nil { comparisonsTable.reloadData() }
            if requiredPolicyId != nil || requiredRevisionId != nil || requiredMarket != nil {
                let matches = currentComparisons.filter {
                    (requiredPolicyId == nil || $0.policyId == requiredPolicyId)
                        && (requiredRevisionId == nil || $0.policyRevisionId == requiredRevisionId)
                        && (requiredMarket == nil || $0.market == requiredMarket)
                }
                guard let matching = matches.first, requiredMarket == nil || matches.count == 1 else {
                    selectedModelId = nil
                    rebuildModelSelector()
                    clearRunMetrics(message: matches.isEmpty ? "선택 정책·리비전·시장의 성과 요약이 없습니다. 실행 근거를 확인하세요." : "선택 범위에 여러 모델이 있습니다. 정확한 모델을 선택하세요.")
                    return
                }
                selectedModelId = matching.modelId
            } else if !currentComparisons.contains(where: { $0.modelId == selectedModelId }) {
                selectedModelId = currentComparisons.first?.modelId
            }
            rebuildModelSelector()
            applySelectedModel(run)
        } catch {
            clearRunMetrics(message: "선택 실행의 모델 원장 조회 실패 · 다른 실행을 선택하거나 다시 조회하세요.")
        }
    }

    private func rebuildModelSelector() {
        modelSelector.removeAllItems()
        for model in currentComparisons {
            modelSelector.addItem(withTitle: "\(model.market) · \(policyName(model.policyId)) · \(model.modelId.prefix(12))")
        }
        if let index = currentComparisons.firstIndex(where: { $0.modelId == selectedModelId }) {
            modelSelector.selectItem(at: index)
        }
        modelSelector.isEnabled = !currentComparisons.isEmpty
        modelSelector.target = self
        modelSelector.action = #selector(modelSelectorChanged)
    }

    private func applySelectedModel(_ run: RealRun) {
        guard let model = currentComparisons.first(where: { $0.modelId == selectedModelId }) else {
            clearRunMetrics(message: "선택된 실행에 비교 가능한 모델 원장이 없습니다. 수익률과 거래 수는 미확인입니다.")
            return
        }
        runDetailLedgerButton?.isEnabled = true
        runDetailExportButton?.isEnabled = true
        runDetailMarkerButton?.isEnabled = true
        do {
            returnMetricLabel.stringValue = model.netReturn.map { String(format: "%+.2f%%", $0 * 100) }
                ?? (model.netReturnNullReason.map { "산출 불가 · \($0)" } ?? "산출 불가")
            returnMetricLabel.textColor = model.netReturn.map { $0 > 0 ? TerminalTheme.positive : ($0 < 0 ? TerminalTheme.negative : TerminalTheme.secondaryText) } ?? .secondaryLabelColor
            for label in [mddMetricLabel, pnlMetricLabel, feesMetricLabel, tradesMetricLabel, capitalMetricLabel] { label.textColor = .labelColor }
            mddMetricLabel.stringValue = model.maxDrawdown.map { String(format: "%.2f%%", $0 * 100) }
                ?? (model.maxDrawdownNullReason.map { "산출 불가 · \($0)" } ?? "산출 불가")
            pnlMetricLabel.stringValue = Formatters.money(model.netPnlDecimal, signed: true)
            feesMetricLabel.stringValue = Formatters.money(model.cumulativeFeesDecimal)
            tradesMetricLabel.stringValue = model.fillCount == 0 ? "무거래 · 0 체결" : "\(model.fillCount) 체결 · \(model.closedEpisodes) 청산"
            capitalMetricLabel.stringValue = "\(Formatters.money(model.initialEquityDecimal)) → \(Formatters.money(model.finalEquityDecimal))"
            for label in [returnMetricLabel, mddMetricLabel, pnlMetricLabel, feesMetricLabel, tradesMetricLabel, capitalMetricLabel] {
                label.toolTip = label.stringValue
                if let box = label.superview?.superview as? NSBox {
                    box.setAccessibilityValue(label.stringValue)
                }
            }
            NSAccessibility.post(element: returnMetricLabel, notification: .valueChanged)
            contextLabel.stringValue = "\(run.id)\n\(model.market) · \(run.mode ?? "유형 미확인") · 정책 revision \(model.policyRevisionId ?? "미확인")\n\(run.windowStartUTC ?? "시작 미확인") → \(run.windowEndUTC ?? "종료 미확인") UTC · \(run.capitalMode ?? "자본 모드 미확인") · \(run.costAssumptionLabel ?? "비용 미확인") · F08 \(model.f08Status ?? run.f08Status ?? "미검증")"
            let fullContext = contextLabel.stringValue
            contextLabel.stringValue = "\(run.createdDateStr) KST · \(model.market) · \(policyName(model.policyId)) · \(TerminalPresentation.state(run.mode ?? ""))\n\(displayKST(run.windowStartUTC)) → \(displayKST(run.windowEndUTC)) KST · \(TerminalPresentation.state(run.capitalMode ?? ""))\n\(run.costAssumptionLabel ?? "비용 미확인") · F08 \(model.f08Status ?? run.f08Status ?? "미검증") (정합성 검증)"
            contextLabel.toolTip = fullContext
            updateInspector()
            markerGeneration &+= 1
            chartView.setFillMarkers([], total: model.fillCount)
            chartView.title = "\(model.market) · \(policyName(model.policyId)) · 계좌 평가액 KRW"
            selectComparisonRow()
            let marks = try dbReader.fetchAccountMarks(for: run.id, modelId: model.modelId)
            chartView.emptyMessage = "선택된 모델의 계좌 마크가 없습니다. 원장 상태를 확인하세요."
            chartView.setPoints(marks, initialEquity: model.initialEquity)
            chartView.setAccessibilityLabel("선택 모델 \(model.modelId), 원장 \(marks.count) 계좌 마크, 최종 평가액 \(Formatters.money(model.finalEquityDecimal))")
            selectComparisonRow()
        } catch {
            chartView.setPoints([], initialEquity: 0)
            chartView.emptyMessage = run.ledgerState == "COMPACTED"
                ? "압축된 원장 · 차트 읽기 경로 연결 필요. 성과 표는 보존된 모델 요약입니다."
                : "계좌 마크 조회 실패 · 차트 데이터는 미확인입니다. 다시 조회하세요."
            if run.ledgerState == "COMPACTED" {
                chartView.emptyMessage = "압축 원장 차트 조회 중 · Rust 검증 경로를 사용합니다."
                ledgerClient.marks(runId: run.id, modelId: model.modelId, expectedCount: model.accountMarkCount) { [weak self] result in
                    guard let self, self.selectedRun?.id == run.id, self.selectedModelId == model.modelId else { return }
                    switch result {
                    case .success(let marks): self.chartView.setPoints(marks, initialEquity: model.initialEquity)
                        self.chartView.title += " · 화면용 \(marks.count) / 원장 \(model.accountMarkCount) 마크"
                    case .failure(let error):
                        self.chartView.emptyMessage = (error as? NativeMCPError)?.errorDescription
                            ?? "차트 응답 형식이 유효하지 않습니다. 요약 지표는 보존된 원장입니다."
                    }
                    self.chartView.needsDisplay = true
                }
            }
        }
    }

    @objc private func modelSelectorChanged() {
        let index = modelSelector.indexOfSelectedItem
        guard currentComparisons.indices.contains(index), let run = selectedRun else { return }
        selectedModelId = currentComparisons[index].modelId
        applySelectedModel(run)
    }

    private func policyName(_ id: String?) -> String {
        guard let id else { return "정책 미확인" }
        return allPolicies.first(where: { $0.policyId == id })?.name ?? id
    }

    private func displayKST(_ value: String?) -> String {
        guard let value else { return "시각 미확인" }
        let parser = ISO8601DateFormatter()
        parser.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        guard let date = parser.date(from: value) ?? ISO8601DateFormatter().date(from: value) else { return value }
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "ko_KR")
        formatter.timeZone = TimeZone(identifier: "Asia/Seoul")
        formatter.dateFormat = "yyyy-MM-dd HH:mm"
        return formatter.string(from: date)
    }

    private func displayKST(_ date: Date) -> String {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "ko_KR")
        formatter.timeZone = TimeZone(identifier: "Asia/Seoul")
        formatter.dateFormat = "yyyy-MM-dd HH:mm:ss"
        return formatter.string(from: date)
    }

    @objc private func showStrategyOverview() {
        markerGeneration &+= 1
        showingRunDetails = false
        refreshCurrentSectionView()
    }

    private func runChoiceLabel(_ run: RealRun) -> String {
        let markets = run.markets?.map { $0.replacingOccurrences(of: "KRW-", with: "") }.joined(separator: "/") ?? "시장 미확인"
        let start = String(displayKST(run.windowStartUTC).prefix(10))
        let end = String(displayKST(run.windowEndUTC).prefix(10))
        let label = run.costAssumptionLabel ?? "비용 미확인"
        let cost = label.range(of: "[0-9]+(?:\\.[0-9]+)?\\s*bp", options: .regularExpression).map { String(label[$0]) } ?? String(label.prefix(28))
        return "\(markets) · \(start)–\(end) · \(cost) · \(run.createdDateStr)"
    }

    @objc private func showRunProvenance() {
        let visible = !(inspectorScroll?.isHidden ?? true)
        inspectorExpanded = !visible
        layoutInspector()
    }

    func windowDidResize(_ notification: Notification) { layoutInspector() }

    func windowDidMiniaturize(_ notification: Notification) {
        suspendLiveMonitoring()
        realtimeFeedService.setVisible(false)
        updateStrategyOverviewVisibility()
        automationWorkspaceView?.setWorkspaceVisible(false)
        policyDeletionWorkspace?.setWorkspaceVisible(false)
    }

    func windowDidDeminiaturize(_ notification: Notification) {
        resumeLiveMonitoringIfVisible()
        updateStrategyOverviewVisibility()
        updateAutomationWorkspaceVisibility()
        updateRealtimeFeedVisibility()
        updatePolicyDeletionWorkspaceVisibility()
    }

    func windowDidBecomeKey(_ notification: Notification) {
        refreshSidebarSymbolAppearance()
        resumeLiveMonitoringIfVisible()
        updateStrategyOverviewVisibility()
        updateAutomationWorkspaceVisibility()
        updateRealtimeFeedVisibility()
        updatePolicyDeletionWorkspaceVisibility()
    }

    func windowDidResignKey(_ notification: Notification) {
        refreshSidebarSymbolAppearance()
    }

    func windowDidChangeOcclusionState(_ notification: Notification) {
        if isLiveMonitorVisible { resumeLiveMonitoringIfVisible() }
        else { suspendLiveMonitoring() }
        updateStrategyOverviewVisibility()
        updateAutomationWorkspaceVisibility()
        updateRealtimeFeedVisibility()
        updatePolicyDeletionWorkspaceVisibility()
    }

    func windowWillClose(_ notification: Notification) {
        suspendLiveMonitoring()
        realtimeFeedService.setVisible(false)
        strategyOverview?.setWorkspaceVisible(false)
        automationWorkspaceView?.setWorkspaceVisible(false)
        policyDeletionWorkspace?.setWorkspaceVisible(false)
    }

    private func layoutInspector() {
        let expanded = inspectorExpanded ?? ((window?.contentView?.bounds.width ?? 0) >= 1440)
        inspectorScroll?.isHidden = !expanded
        inspectorWidth?.constant = expanded ? 270 : 0
        inspectorGap?.constant = expanded ? -16 : 0
    }

    private func updateInspector() {
        guard let run = selectedRun else {
            inspectorText.string = "선택 근거\n\n실행과 모델을 선택하세요.\n\n표·차트와 함께 원장의 검증 결과 및 재현성 정보를 확인할 수 있습니다."
            return
        }
        let model = currentComparisons.first { $0.modelId == selectedModelId }
        inspectorText.string = "선택 근거\n\n시장 · \(model?.market ?? "미확인")\n정책 · \(policyName(model?.policyId))\n상태 · \(TerminalPresentation.state(run.state))\n유형 · \(TerminalPresentation.state(run.mode ?? ""))\n\nF08 정합성\n결과 · \(model?.f08Status ?? run.f08Status ?? "미검증")\n발견 사항 · \(run.f08FindingsCount.map(String.init) ?? "미확인")\n원장 · \(run.ledgerState)\n계좌 마크 · \(model.map { String($0.accountMarkCount) } ?? "미확인")\n\n실행 구간 · KST\n\(displayKST(run.windowStartUTC))\n→ \(displayKST(run.windowEndUTC))\n\n비용 가정\n\(run.costAssumptionLabel ?? "미확인")\n\n정확한 원장 값 · KRW\n순손익 · \(model.map { NSDecimalNumber(decimal: $0.netPnlDecimal).stringValue } ?? "미확인")\n수수료 · \(model.map { NSDecimalNumber(decimal: $0.cumulativeFeesDecimal).stringValue } ?? "미확인")\n\n실행 ID\n\(run.id)\n\n모델 ID\n\(model?.modelId ?? "미확인")\n\n계획 ID\n\(run.planId)\n\n데이터셋\n\(run.datasetIds?.joined(separator: "\n") ?? "미확인")\n\n정책 리비전\n\(model?.policyRevisionId ?? "미확인")\n\n엔진 · \(run.engineVersion ?? "미확인")\n코드 · \(run.codeRevision ?? "미확인")\n\n원본 상태\n\(run.state) · \(run.mode ?? "미확인")"
    }

    // MARK: - UI Construction

    private func setupUI() {
        guard let content = window?.contentView else { return }

        splitView.isVertical = true
        splitView.delegate = self
        splitView.dividerStyle = .thin
        splitView.translatesAutoresizingMaskIntoConstraints = false
        content.addSubview(splitView)

        NSLayoutConstraint.activate([
            splitView.leadingAnchor.constraint(equalTo: content.leadingAnchor),
            splitView.trailingAnchor.constraint(equalTo: content.trailingAnchor),
            splitView.topAnchor.constraint(equalTo: content.topAnchor),
            splitView.bottomAnchor.constraint(equalTo: content.bottomAnchor)
        ])

        // Sidebar
        let initialSidebarWidth: CGFloat = TerminalTheme.textScale >= 1.5 ? 280 : 230
        let sidebarView = buildSidebarView()
        splitView.addSubview(sidebarView)

        // Detail View
        // NSSplitView owns its pane frames; pane autoresizing must remain enabled.
        detailContainer.translatesAutoresizingMaskIntoConstraints = true
        detailContainer.autoresizingMask = [.width, .height]
        detailContainer.frame = NSRect(x: initialSidebarWidth, y: 0, width: 1200 - initialSidebarWidth, height: 820)
        detailContainer.wantsLayer = true
        detailContainer.layer?.masksToBounds = true
        splitView.addSubview(detailContainer)

        content.layoutSubtreeIfNeeded()
        splitView.adjustSubviews()
        splitView.setPosition(initialSidebarWidth, ofDividerAt: 0)
    }

    private func buildSidebarView() -> NSView {
        let initialSidebarWidth: CGFloat = TerminalTheme.textScale >= 1.5 ? 280 : 230
        let sidebar = NSVisualEffectView()
        sidebar.material = .sidebar
        sidebar.blendingMode = .behindWindow
        sidebar.state = .followsWindowActiveState
        sidebar.translatesAutoresizingMaskIntoConstraints = true
        sidebar.autoresizingMask = [.height]
        sidebar.frame = NSRect(x: 0, y: 0, width: initialSidebarWidth, height: 820)

        let brandScale = min(TerminalTheme.textScale, 1.5)
        let brandIcon = NSImageView(image: NSImage(systemSymbolName: "chart.line.uptrend.xyaxis", accessibilityDescription: nil) ?? NSImage())
        brandIcon.symbolConfiguration = NSImage.SymbolConfiguration(pointSize: round(13 * brandScale), weight: .semibold)
        brandIcon.contentTintColor = .white
        let brandBadge = NSView()
        brandBadge.wantsLayer = true
        brandBadge.layer?.cornerRadius = round(7 * brandScale)
        brandBadge.layer?.backgroundColor = NSColor.systemBlue.cgColor
        brandBadge.translatesAutoresizingMaskIntoConstraints = false
        brandIcon.translatesAutoresizingMaskIntoConstraints = false
        brandBadge.addSubview(brandIcon)
        let badgeSize = round(28 * brandScale)
        NSLayoutConstraint.activate([
            brandBadge.widthAnchor.constraint(equalToConstant: badgeSize),
            brandBadge.heightAnchor.constraint(equalToConstant: badgeSize),
            brandIcon.centerXAnchor.constraint(equalTo: brandBadge.centerXAnchor),
            brandIcon.centerYAnchor.constraint(equalTo: brandBadge.centerYAnchor)
        ])

        let brandTitle = NSTextField(labelWithString: "Sky Backcraft")
        brandTitle.font = NSFont.systemFont(ofSize: round(13 * brandScale), weight: .semibold)
        let brandSub = NSTextField(labelWithString: "퀀트 리서치 스튜디오")
        brandSub.font = NSFont.systemFont(ofSize: round(11 * brandScale))
        brandSub.textColor = .secondaryLabelColor
        let brandTextStack = NSStackView(views: [brandTitle, brandSub])
        brandTextStack.orientation = .vertical
        brandTextStack.alignment = .leading
        brandTextStack.spacing = 1
        let brandHeader = NSStackView(views: [brandBadge, brandTextStack])
        brandHeader.orientation = .horizontal
        brandHeader.spacing = 10
        brandHeader.alignment = .centerY

        // Table
        let col = NSTableColumn(identifier: NSUserInterfaceItemIdentifier("NavCol"))
        col.title = "네비게이션"
        sidebarTable.addTableColumn(col)
        sidebarTable.headerView = nil
        sidebarTable.dataSource = self
        sidebarTable.delegate = self
        sidebarTable.identifier = NSUserInterfaceItemIdentifier("DashboardNavigationTable")
        sidebarTable.setAccessibilityLabel("대시보드 기본 화면")
        sidebarTable.style = .sourceList
        sidebarTable.rowHeight = TerminalTheme.navigationRowHeight
        sidebarTable.backgroundColor = .clear
        sidebarTable.focusRingType = .none

        let scroll = NSScrollView()
        scroll.documentView = sidebarTable
        scroll.hasVerticalScroller = true
        scroll.hasHorizontalScroller = false
        scroll.autohidesScrollers = true
        scroll.drawsBackground = false
        scroll.contentView.drawsBackground = false

        // DB Status Footer
        let footer = NSTextField(labelWithString: DashboardDatabaseStatusSummary(
            historyRunCount: historyTotal,
            policies: allPolicies
        ).label)
        footer.font = TerminalTheme.monospacedDigitFont(ofSize: 11)
        footer.textColor = .secondaryLabelColor
        footer.setAccessibilityLabel("로컬 실행 및 정책 저장 수")
        footer.toolTip = footer.stringValue
        databaseStatusFooter = footer
        updateDatabaseStatusFooter()
        let footerIcon = NSImageView(image: NSImage(systemSymbolName: "externaldrive", accessibilityDescription: "로컬 데이터베이스") ?? NSImage())
        footerIcon.contentTintColor = .tertiaryLabelColor
        footerIcon.setAccessibilityElement(false)
        footerIcon.translatesAutoresizingMaskIntoConstraints = false
        NSLayoutConstraint.activate([
            footerIcon.widthAnchor.constraint(equalToConstant: TerminalTheme.navigationIconSize),
            footerIcon.heightAnchor.constraint(equalToConstant: TerminalTheme.navigationIconSize)
        ])
        databaseStatusFooterIcon = footerIcon
        let footerRow = NSStackView(views: [footerIcon, footer])
        footerRow.orientation = .horizontal
        footerRow.alignment = .centerY
        footerRow.spacing = 8

        let stack = NSStackView(views: [brandHeader, scroll, footerRow])
        stack.orientation = .vertical
        stack.alignment = .leading
        stack.spacing = 14
        stack.setCustomSpacing(18, after: brandHeader)
        stack.translatesAutoresizingMaskIntoConstraints = false
        sidebar.addSubview(stack)

        NSLayoutConstraint.activate([
            stack.leadingAnchor.constraint(equalTo: sidebar.leadingAnchor, constant: 12),
            stack.trailingAnchor.constraint(equalTo: sidebar.trailingAnchor, constant: -12),
            stack.topAnchor.constraint(equalTo: sidebar.topAnchor, constant: 54),
            stack.bottomAnchor.constraint(equalTo: sidebar.bottomAnchor, constant: -14),
            scroll.widthAnchor.constraint(equalTo: stack.widthAnchor),
            brandHeader.widthAnchor.constraint(lessThanOrEqualTo: stack.widthAnchor),
            footerRow.widthAnchor.constraint(lessThanOrEqualTo: stack.widthAnchor)
        ])
        brandHeader.edgeInsets = NSEdgeInsets(top: 0, left: 6, bottom: 0, right: 0)
        footerRow.edgeInsets = NSEdgeInsets(top: 0, left: 8, bottom: 0, right: 0)

        return sidebar
    }

    private func configureWindowToolbar() {
        guard let window else { return }
        let toolbar = NSToolbar(identifier: "SkyBackcraft.MainToolbar")
        toolbar.delegate = self
        toolbar.displayMode = .iconOnly
        toolbar.allowsUserCustomization = false
        window.toolbar = toolbar
    }

    func toolbarDefaultItemIdentifiers(_ toolbar: NSToolbar) -> [NSToolbarItem.Identifier] {
        _ = toolbar
        return [Self.sidebarToolbarIdentifier, Self.sidebarSeparatorIdentifier, .flexibleSpace]
    }

    func toolbarAllowedItemIdentifiers(_ toolbar: NSToolbar) -> [NSToolbarItem.Identifier] {
        _ = toolbar
        return [Self.sidebarToolbarIdentifier, Self.sidebarSeparatorIdentifier, .space, .flexibleSpace]
    }

    func toolbar(
        _ toolbar: NSToolbar,
        itemForItemIdentifier itemIdentifier: NSToolbarItem.Identifier,
        willBeInsertedIntoToolbar flag: Bool
    ) -> NSToolbarItem? {
        _ = toolbar
        _ = flag
        if itemIdentifier == Self.sidebarSeparatorIdentifier {
            return NSTrackingSeparatorToolbarItem(identifier: itemIdentifier, splitView: splitView, dividerIndex: 0)
        }
        guard itemIdentifier == Self.sidebarToolbarIdentifier else { return nil }
        let item = NSToolbarItem(itemIdentifier: itemIdentifier)
        item.label = "사이드바"
        item.paletteLabel = "사이드바 표시/가리기"
        item.toolTip = "사이드바 표시 또는 가리기"
        item.image = NSImage(systemSymbolName: "sidebar.left", accessibilityDescription: "사이드바")
        item.target = self
        item.action = #selector(toggleSidebar(_:))
        return item
    }

    @objc func toggleSidebar(_ sender: Any?) {
        guard let sidebar = splitView.subviews.first else { return }
        if isSidebarCollapsed {
            isSidebarCollapsed = false
            sidebar.isHidden = false
            splitView.setPosition(sidebarWidthBeforeCollapse, ofDividerAt: 0)
        } else {
            let maxSidebarWidth: CGFloat = TerminalTheme.textScale >= 1.5 ? 300 : 280
            sidebarWidthBeforeCollapse = min(max(sidebar.frame.width, 180), maxSidebarWidth)
            isSidebarCollapsed = true
            splitView.setPosition(0, ofDividerAt: 0)
        }
        window?.contentView?.layoutSubtreeIfNeeded()
        if let item = sender as? NSToolbarItem { item.isEnabled = true }
    }

    func splitView(_ splitView: NSSplitView, constrainMinCoordinate proposedMinimumPosition: CGFloat, ofSubviewAt dividerIndex: Int) -> CGFloat {
        if splitView === self.splitView { return isSidebarCollapsed ? 0 : 180 }
        if splitView === historySplitView { return splitView.isVertical ? 420 : 220 }
        return proposedMinimumPosition
    }

    func splitView(_ splitView: NSSplitView, constrainMaxCoordinate proposedMaximumPosition: CGFloat, ofSubviewAt dividerIndex: Int) -> CGFloat {
        if splitView === self.splitView { return TerminalTheme.textScale >= 1.5 ? 300 : 280 }
        if splitView === historySplitView {
            return splitView.isVertical ? max(420, splitView.bounds.width - 250) : max(220, splitView.bounds.height - 150)
        }
        return proposedMaximumPosition
    }

    func splitView(_ splitView: NSSplitView, shouldAdjustSizeOfSubview view: NSView) -> Bool {
        if splitView === historySplitView { return true }
        return view !== splitView.subviews.first
    }

    func splitView(_ splitView: NSSplitView, canCollapseSubview subview: NSView) -> Bool {
        guard splitView === self.splitView, subview === splitView.subviews.first else { return false }
        return isSidebarCollapsed
    }

    func splitViewDidResizeSubviews(_ notification: Notification) {
        guard let resized = notification.object as? NSSplitView else { return }
        if resized === self.splitView, !isSidebarCollapsed, let sidebar = resized.subviews.first {
            let maxSidebarWidth: CGFloat = TerminalTheme.textScale >= 1.5 ? 300 : 280
            sidebarWidthBeforeCollapse = min(max(sidebar.frame.width, 180), maxSidebarWidth)
        } else if resized === historySplitView, let split = historySplitView {
            let extent = split.isVertical ? split.bounds.width : split.bounds.height
            guard extent > 0, let leading = split.subviews.first else { return }
            let leadingExtent = split.isVertical ? leading.frame.width : leading.frame.height
            let ratio = min(max(leadingExtent / extent, 0.15), 0.85)
            UserDefaults.standard.set(Double(ratio), forKey: Self.historyDividerDefaultsKey + (split.isVertical ? ".columns" : ".rows"))
        }
    }

    private func restoreHistoryDividerPosition(vertical: Bool) {
        guard let split = historySplitView else { return }
        let extent = vertical ? split.bounds.width : split.bounds.height
        guard extent > 0 else { return }
        let key = Self.historyDividerDefaultsKey + (vertical ? ".columns" : ".rows")
        let saved = UserDefaults.standard.double(forKey: key)
        let ratio = (0.15...0.85).contains(saved) ? CGFloat(saved) : (vertical ? 0.62 : 0.55)
        split.setPosition(extent * ratio, ofDividerAt: 0)
    }

    private func selectSection(_ section: Section) {
        if currentSection == .liveTradingMonitor, section != .liveTradingMonitor {
            suspendLiveMonitoring()
        }
        if currentSection == .paperTradingTelemetry, section != .paperTradingTelemetry {
            paperWorkspaceView?.setWorkspaceVisible(false)
        }
        if currentSection == .strategyAutomation, section != .strategyAutomation {
            automationWorkspaceView?.setWorkspaceVisible(false)
        }
        if currentSection == .policyManagement, section != .policyManagement {
            policyDeletionWorkspace?.setWorkspaceVisible(false)
        }
        currentSection = section
        updateGlobalContextText()
        if section != .backtestAnalysis { markerGeneration &+= 1 }
        synchronizingSidebarSelection = true
        sidebarTable.selectRowIndexes(IndexSet(integer: section.sidebarRow), byExtendingSelection: false)
        synchronizingSidebarSelection = false
        refreshCurrentSectionView()
        refreshSidebarSymbolAppearance()
    }

    private func refreshCurrentSectionView() {
        let reduceMotion = NSWorkspace.shared.accessibilityDisplayShouldReduceMotion
        if reduceMotion {
            CATransaction.begin()
            CATransaction.setDisableActions(true)
        }
        defer {
            if reduceMotion {
                CATransaction.commit()
            }
        }
        detailContainer.subviews.forEach { $0.removeFromSuperview() }
        let view: NSView
        switch currentSection {
        case .backtestAnalysis:
            if showingRunDetails { view = buildBacktestAnalysisView() }
            else if let overview = strategyOverview { view = overview }
            else {
                let overview = StrategyPerformanceView(reader: dbReader, policies: allPolicies,
                    initialPolicyId: selectedStrategyId,
                    onOpenRun: { [weak self] run, modelId in
                        guard let self else { return }
                        self.lastOpenedHistoryRunId = run.id
                        self.persistHistoryState()
                        self.showingRunDetails = true
                        self.selectedModelId = modelId
                        self.loadRunDetails(run)
                        self.refreshCurrentSectionView()
                    }, onPolicyChange: { [weak self] id in self?.selectedStrategyId = id },
                    onRefreshCatalog: { [weak self] in self?.loadData() },
                    initiallyVisible: isStrategyOverviewVisible)
                strategyOverview = overview
                view = overview
            }
        case .allRunsHistory: view = buildRunsHistoryView()
        case .strategyCatalog:
            if let inventory = policyInventory { view = inventory }
            else {
                let inventory = PolicyInventoryView(reader: dbReader, policies: allPolicies,
                    onOpenRun: { [weak self] run, policyId, revisionId in
                        guard let self else { return }
                        self.lastOpenedHistoryRunId = run.id
                        self.persistHistoryState()
                        self.showingRunDetails = true
                        self.selectedModelId = nil
                        self.selectedStrategyId = policyId
                        self.strategyOverview = nil
                        self.loadRunDetails(run, requiredPolicyId: policyId, requiredRevisionId: revisionId)
                        self.selectSection(.backtestAnalysis)
                    }, onShowStrategy: { [weak self] policyId in
                        guard let self else { return }
                        self.selectedStrategyId = policyId
                        self.strategyOverview = nil
                        self.showingRunDetails = false
                        self.selectSection(.backtestAnalysis)
                    })
                policyInventory = inventory
                view = inventory
            }
        case .paperTradingTelemetry: view = buildPaperTradingView()
        case .strategyAutomation: view = buildAutomationWorkspace()
        case .liveTradingMonitor: view = buildLiveTradingView()
        case .policyManagement: view = buildPolicyManagementView()
        }

        view.translatesAutoresizingMaskIntoConstraints = false
        configureSectionScrollViews(in: view)
        detailContainer.addSubview(view)
        mountGlobalContextStrip()
        realtimeStatusView.translatesAutoresizingMaskIntoConstraints = false
        let showRealtimeStatus = currentSection == .backtestAnalysis
        let showRealtimeDetails = showRealtimeStatus && !realtimeStatusCollapsed
        realtimeStatusDisclosure.setButtonType(.pushOnPushOff)
        realtimeStatusDisclosure.bezelStyle = .recessed
        realtimeStatusDisclosure.controlSize = .small
        realtimeStatusDisclosure.image = NSImage(systemSymbolName: "dot.radiowaves.left.and.right", accessibilityDescription: nil)
        realtimeStatusDisclosure.imagePosition = .imageLeading
        realtimeStatusDisclosure.font = TerminalTheme.systemFont(ofSize: 11, weight: .medium)
        realtimeStatusDisclosure.target = self
        realtimeStatusDisclosure.action = #selector(toggleRealtimeStatusDisclosure(_:))
        realtimeStatusDisclosure.title = realtimeStatusCollapsed ? "실시간 상태 펼치기" : "실시간 상태 접기"
        realtimeStatusDisclosure.state = realtimeStatusCollapsed ? .off : .on
        realtimeStatusDisclosure.setAccessibilityLabel("실시간 피드 상세 상태")
        realtimeStatusDisclosure.setAccessibilityValue(realtimeStatusCollapsed ? "접힘" : "펼쳐짐")
        realtimeStatusDisclosure.isHidden = !showRealtimeStatus
        realtimeStatusDisclosure.translatesAutoresizingMaskIntoConstraints = false
        detailContainer.addSubview(realtimeStatusDisclosure)
        realtimeStatusView.isHidden = !showRealtimeDetails
        let statusHeight = showRealtimeDetails
            ? realtimeStatusView.heightAnchor.constraint(greaterThanOrEqualToConstant: max(132, 132 * TerminalTheme.textScale))
            : realtimeStatusView.heightAnchor.constraint(equalToConstant: 0)
        statusHeight.isActive = true
        realtimeStatusHeightConstraint?.isActive = false
        realtimeStatusHeightConstraint = statusHeight
        detailContainer.addSubview(realtimeStatusView)
        NSLayoutConstraint.activate([
            realtimeStatusDisclosure.leadingAnchor.constraint(greaterThanOrEqualTo: detailContainer.leadingAnchor, constant: 20),
            realtimeStatusDisclosure.trailingAnchor.constraint(equalTo: detailContainer.trailingAnchor, constant: -20),
            realtimeStatusDisclosure.topAnchor.constraint(equalTo: globalContextStrip?.bottomAnchor ?? detailContainer.topAnchor, constant: showRealtimeStatus ? 8 : 4),
            realtimeStatusDisclosure.heightAnchor.constraint(equalToConstant: showRealtimeStatus ? ceil(22 * TerminalTheme.textScale) : 0),
            realtimeStatusView.leadingAnchor.constraint(equalTo: detailContainer.leadingAnchor, constant: 20),
            realtimeStatusView.trailingAnchor.constraint(equalTo: detailContainer.trailingAnchor, constant: -20),
            realtimeStatusView.topAnchor.constraint(equalTo: realtimeStatusDisclosure.bottomAnchor, constant: showRealtimeDetails ? 4 : 0),
            view.leadingAnchor.constraint(equalTo: detailContainer.leadingAnchor),
            view.trailingAnchor.constraint(equalTo: detailContainer.trailingAnchor),
            view.topAnchor.constraint(equalTo: realtimeStatusView.bottomAnchor, constant: showRealtimeDetails ? 4 : 0),
            view.bottomAnchor.constraint(equalTo: detailContainer.bottomAnchor)
        ])
        NSAccessibility.post(element: detailContainer, notification: .layoutChanged)
        if currentSection == .liveTradingMonitor { resumeLiveMonitoringIfVisible() }
        if currentSection == .paperTradingTelemetry { paperWorkspaceView?.setWorkspaceVisible(true) }
        if currentSection == .strategyAutomation { updateAutomationWorkspaceVisibility() }
        updatePolicyDeletionWorkspaceVisibility()
        updateRealtimeFeedVisibility()
        updateStrategyOverviewVisibility()
    }

    private func updatePolicyDeletionWorkspaceVisibility() {
        let shouldBeVisible = currentSection == .policyManagement
            && window?.isVisible == true
            && window?.isMiniaturized == false
            && window?.occlusionState.contains(.visible) == true
        policyDeletionWorkspace?.setWorkspaceVisible(shouldBeVisible)
    }

    private func configureSectionScrollViews(in root: NSView) {
        if let scroll = root as? NSScrollView, scroll.documentView is NSTableView {
            scroll.drawsBackground = false
            scroll.contentView.drawsBackground = false
            for sub in scroll.subviews {
                (sub as? NSClipView)?.drawsBackground = false
            }
        }
        for subview in root.subviews {
            configureSectionScrollViews(in: subview)
        }
    }

    // MARK: - Section 1: Backtest Analysis & Real Chart

    private func buildBacktestAnalysisView() -> NSView {
        let container = NSView()

        let headerLabel = NSTextField(labelWithString: "실행 상세")
        headerLabel.font = TerminalTheme.systemFont(ofSize: 18, weight: .bold)

        let refreshBtn = NSButton(title: "새로고침", target: self, action: #selector(refreshClicked))
        refreshBtn.bezelStyle = .rounded

        let spacer1 = NSView()
        spacer1.setContentHuggingPriority(.defaultLow, for: .horizontal)
        let copyBtn = NSButton(title: "실행 ID 복사", target: self, action: #selector(copySelectedRunID))
        copyBtn.bezelStyle = .rounded
        let detailBtn = NSButton(title: "근거 패널", target: self, action: #selector(showRunProvenance))
        detailBtn.bezelStyle = .rounded
        let backBtn = NSButton(title: "전략별 그래프", target: self, action: #selector(showStrategyOverview))
        let ledgerButton = NSButton(title: "비용·원장", target: self, action: #selector(openSelectedLedger))
        ledgerButton.isEnabled = currentComparisons.contains { $0.modelId == selectedModelId }
        runDetailLedgerButton = ledgerButton
        let exportButton = NSButton(title: "CSV", target: self, action: #selector(exportRunResults))
        exportButton.isEnabled = selectedRun != nil
        runDetailExportButton = exportButton
        let headerRow = NSStackView(views: [headerLabel, spacer1, backBtn, ledgerButton, exportButton, detailBtn, copyBtn, refreshBtn])
        headerRow.orientation = .horizontal
        headerRow.alignment = .centerY

        let runLabel = NSTextField(labelWithString: "실행 선택:")
        runLabel.font = TerminalTheme.systemFont(ofSize: 12, weight: .semibold)
        runLabel.textColor = TerminalTheme.secondaryText

        runSelector.removeAllItems()
        runSelector.addItem(withTitle: "실행을 선택하세요")
        runSelector.lastItem?.representedObject = nil
        var selectorRuns = allRuns
        if let current = selectedRun, !selectorRuns.contains(where: { $0.id == current.id }) {
            selectorRuns.append(current)
        }
        for run in selectorRuns {
            runSelector.addItem(withTitle: runChoiceLabel(run))
            runSelector.lastItem?.representedObject = run
            runSelector.lastItem?.toolTip = run.id
        }
        if let current = selectedRun, let idx = selectorRuns.firstIndex(where: { $0.id == current.id }) {
            runSelector.selectItem(at: idx + 1)
        }
        runSelector.target = self
        runSelector.action = #selector(runSelectorChanged)

        chartModeControl.selectedSegment = chartView.mode.rawValue
        chartModeControl.target = self
        chartModeControl.action = #selector(chartModeChanged)

        let spacer2 = NSView()
        spacer2.setContentHuggingPriority(.defaultLow, for: .horizontal)
        let controlsRow = NSStackView(views: [runLabel, runSelector, spacer2, chartModeControl])
        controlsRow.orientation = .horizontal
        controlsRow.spacing = 10
        controlsRow.alignment = .centerY

        // Metric Tiles
        let tile1 = createMetricTile(title: "누적 수익률", label: returnMetricLabel)
        let tile2 = createMetricTile(title: "최대 낙폭", label: mddMetricLabel)
        let tile3 = createMetricTile(title: "순손익", label: pnlMetricLabel)
        let metricRow1 = NSStackView(views: [tile1, tile2, tile3])
        metricRow1.orientation = .horizontal
        metricRow1.distribution = .fillEqually
        metricRow1.spacing = 8
        let metricsGrid = NSStackView(views: [metricRow1])
        metricsGrid.orientation = .vertical
        metricsGrid.alignment = .leading
        metricRow1.widthAnchor.constraint(equalTo: metricsGrid.widthAnchor).isActive = true
        for label in [feesMetricLabel, tradesMetricLabel, capitalMetricLabel] {
            label.font = TerminalTheme.table
            label.lineBreakMode = .byTruncatingTail
            label.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        }
        feesMetricLabel.setAccessibilityLabel("수수료")
        tradesMetricLabel.setAccessibilityLabel("체결·청산")
        capitalMetricLabel.setAccessibilityLabel("기초→최종 자본")
        let secondaryMetrics = NSStackView(views: [NSTextField(labelWithString: "수수료"), feesMetricLabel, NSView(), NSTextField(labelWithString: "체결·청산"), tradesMetricLabel, NSView(), NSTextField(labelWithString: "기초→최종"), capitalMetricLabel])
        secondaryMetrics.orientation = .horizontal
        secondaryMetrics.spacing = 8
        secondaryMetrics.alignment = .centerY

        // Chart View
        chartView.translatesAutoresizingMaskIntoConstraints = false
        let chartHeight = chartView.heightAnchor.constraint(equalToConstant: 240)
        chartHeight.priority = .defaultHigh
        chartHeight.isActive = true

        // Model Comparisons Table
        setupComparisonsTable()
        let tableScroll = NSScrollView()
        tableScroll.documentView = comparisonsTable
        tableScroll.hasVerticalScroller = true
        tableScroll.hasHorizontalScroller = true
        tableScroll.translatesAutoresizingMaskIntoConstraints = false
        tableScroll.heightAnchor.constraint(greaterThanOrEqualToConstant: 140).isActive = true

        let tableHeader = NSTextField(labelWithString: "모델 비교 · 행을 선택하면 차트와 근거가 함께 바뀝니다")
        tableHeader.font = TerminalTheme.systemFont(ofSize: 13, weight: .semibold)

        contextLabel.font = TerminalTheme.systemFont(ofSize: 12)
        contextLabel.textColor = TerminalTheme.secondaryText
        contextLabel.maximumNumberOfLines = 3
        contextLabel.lineBreakMode = .byWordWrapping
        contextLabel.cell?.wraps = true
        contextLabel.setContentCompressionResistancePriority(.required, for: .vertical)
        let contextFont = contextLabel.font ?? TerminalTheme.systemFont(ofSize: 12)
        let contextLineHeight = ceil(contextFont.ascender - contextFont.descender + contextFont.leading)
        let contextMinimumHeight = max(48, contextLineHeight * CGFloat(contextLabel.maximumNumberOfLines))
        let zoomOut = NSButton(title: "−", target: self, action: #selector(zoomChartOut))
        zoomOut.toolTip = "그래프 축소 · 키보드 -"
        let zoomIn = NSButton(title: "+", target: self, action: #selector(zoomChartIn))
        zoomIn.toolTip = "그래프 확대 · 키보드 + · 지표는 전체 원장 구간입니다."
        let zoomReset = NSButton(title: "전체", target: self, action: #selector(resetChartZoom))
        let markerButton = NSButton(title: "체결 마커", target: self, action: #selector(loadChartFillMarkers))
        markerButton.isEnabled = selectedModelId != nil
        runDetailMarkerButton = markerButton
        let modelRow = NSStackView(views: [NSTextField(labelWithString: "표시 모델"), modelSelector, NSView(), markerButton, zoomOut, zoomIn, zoomReset])
        modelRow.orientation = .horizontal
        modelRow.spacing = 10
        modelRow.alignment = .centerY
        let mainPane = NSView()
        mainPane.translatesAutoresizingMaskIntoConstraints = false
        container.addSubview(mainPane)
        let inspector = NSScrollView()
        inspector.translatesAutoresizingMaskIntoConstraints = false
        inspector.hasVerticalScroller = true
        inspector.drawsBackground = true
        inspector.backgroundColor = TerminalTheme.panel
        inspectorText.isEditable = false
        inspectorText.isSelectable = true
        inspectorText.font = TerminalTheme.systemFont(ofSize: 12)
        inspectorText.textColor = .labelColor
        inspectorText.backgroundColor = TerminalTheme.panel
        inspectorText.textContainerInset = NSSize(width: 12, height: 14)
        inspectorText.isVerticallyResizable = true
        inspectorText.isHorizontallyResizable = false
        inspectorText.autoresizingMask = [.width]
        inspectorText.textContainer?.widthTracksTextView = true
        inspectorText.setAccessibilityLabel("선택 실행 및 모델의 재현성 근거")
        inspector.documentView = inspectorText
        container.addSubview(inspector)
        inspectorScroll = inspector
        let width = inspector.widthAnchor.constraint(equalToConstant: 0)
        let gap = mainPane.trailingAnchor.constraint(equalTo: inspector.leadingAnchor)
        inspectorWidth = width
        inspectorGap = gap
        NSLayoutConstraint.activate([
            mainPane.leadingAnchor.constraint(equalTo: container.leadingAnchor),
            mainPane.topAnchor.constraint(equalTo: container.topAnchor),
            mainPane.bottomAnchor.constraint(equalTo: container.bottomAnchor),
            gap, width,
            inspector.trailingAnchor.constraint(equalTo: container.trailingAnchor, constant: -20),
            inspector.topAnchor.constraint(equalTo: container.topAnchor, constant: 54),
            inspector.bottomAnchor.constraint(equalTo: container.bottomAnchor, constant: -16)
        ])
        updateInspector()
        layoutInspector()
        let regions: [NSView] = [headerRow, controlsRow, modelRow, contextLabel, metricsGrid, secondaryMetrics, chartView, tableHeader, tableScroll]
        for region in regions {
            region.translatesAutoresizingMaskIntoConstraints = false
            mainPane.addSubview(region)
            region.leadingAnchor.constraint(equalTo: mainPane.leadingAnchor, constant: 20).isActive = true
            region.trailingAnchor.constraint(equalTo: mainPane.trailingAnchor, constant: -20).isActive = true
        }
        headerRow.topAnchor.constraint(equalTo: container.topAnchor, constant: 16).isActive = true
        for pair in zip(regions, regions.dropFirst()) {
            pair.1.topAnchor.constraint(equalTo: pair.0.bottomAnchor, constant: 10).isActive = true
        }
        NSLayoutConstraint.activate([
            headerRow.heightAnchor.constraint(greaterThanOrEqualToConstant: 28),
            controlsRow.heightAnchor.constraint(greaterThanOrEqualToConstant: 28),
            modelRow.heightAnchor.constraint(greaterThanOrEqualToConstant: 28),
            contextLabel.heightAnchor.constraint(greaterThanOrEqualToConstant: contextMinimumHeight),
            metricsGrid.heightAnchor.constraint(greaterThanOrEqualToConstant: 58),
            secondaryMetrics.heightAnchor.constraint(greaterThanOrEqualToConstant: 28),
            chartView.heightAnchor.constraint(greaterThanOrEqualToConstant: 128),
            tableHeader.heightAnchor.constraint(greaterThanOrEqualToConstant: 20),
            tableScroll.bottomAnchor.constraint(equalTo: container.bottomAnchor, constant: -16)
        ])

        return container
    }

    private func createMetricTile(title: String, label: NSTextField) -> NSBox {
        let box = NSBox()
        box.boxType = .custom
        box.cornerRadius = 6
        box.borderWidth = 1
        box.borderColor = TerminalTheme.border
        box.fillColor = TerminalTheme.panel
        box.translatesAutoresizingMaskIntoConstraints = false
        box.heightAnchor.constraint(greaterThanOrEqualToConstant: 58).isActive = true
        box.setAccessibilityElement(true)
        box.setAccessibilityRole(.group)
        box.setAccessibilityLabel(title)
        box.setAccessibilityValue(label.stringValue)

        let titleLabel = NSTextField(labelWithString: title)
        titleLabel.font = TerminalTheme.systemFont(ofSize: 12, weight: .medium)
        titleLabel.textColor = TerminalTheme.secondaryText
        titleLabel.setContentCompressionResistancePriority(.required, for: .vertical)

        label.font = TerminalTheme.monospacedDigitFont(ofSize: 15, weight: .semibold)
        label.lineBreakMode = .byTruncatingTail
        label.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        label.setContentCompressionResistancePriority(.required, for: .vertical)
        label.setAccessibilityLabel(title)

        let stack = NSStackView(views: [titleLabel, label])
        stack.orientation = .vertical
        stack.alignment = .leading
        stack.spacing = 4
        stack.translatesAutoresizingMaskIntoConstraints = false
        box.addSubview(stack)

        NSLayoutConstraint.activate([
            stack.leadingAnchor.constraint(equalTo: box.leadingAnchor, constant: 10),
            stack.trailingAnchor.constraint(equalTo: box.trailingAnchor, constant: -10),
            stack.topAnchor.constraint(equalTo: box.topAnchor, constant: 8),
            stack.bottomAnchor.constraint(equalTo: box.bottomAnchor, constant: -8)
        ])
        return box
    }

    private func setupComparisonsTable() {
        comparisonsTable.columnAutoresizingStyle = .noColumnAutoresizing
        comparisonsTable.removeAllColumns()
        let cols = [
            ("Market", "시장", 80),
            ("Policy", "정책", 170),
            ("Return", "수익률", 80),
            ("MDD", "최대 낙폭", 80),
            ("PnL", "순손익", 110),
            ("Fees", "수수료", 100),
            ("Trades", "체결 / 청산", 90)
        ]
        for (id, title, w) in cols {
            let tc = NSTableColumn(identifier: NSUserInterfaceItemIdentifier(id))
            tc.title = title
            tc.width = CGFloat(w)
            tc.minWidth = CGFloat(w)
            tc.resizingMask = .userResizingMask
            tc.sortDescriptorPrototype = NSSortDescriptor(key: id, ascending: true)
            comparisonsTable.addTableColumn(tc)
        }
        comparisonsTable.dataSource = self
        comparisonsTable.delegate = self
        TerminalTheme.prepareTable(comparisonsTable)
        comparisonsTable.columnAutoresizingStyle = .noColumnAutoresizing
        for (column, specification) in zip(comparisonsTable.tableColumns, cols) {
            column.width = CGFloat(specification.2)
        }
        comparisonsTable.reloadData()
        selectComparisonRow()
    }

    @objc private func zoomChartIn() { chartView.zoom(by: 2) }
    @objc private func zoomChartOut() { chartView.zoom(by: 0.5) }
    @objc private func resetChartZoom() { chartView.resetZoom() }

    @objc private func loadChartFillMarkers() {
        guard let run = selectedRun,
              let model = currentComparisons.first(where: { $0.modelId == selectedModelId }) else { return }
        markerGeneration &+= 1
        let generation = markerGeneration
        chartView.setFillMarkers([], total: model.fillCount, message: "체결 시각 조회 중")
        loadMarkerPage(run: run, model: model, cursor: nil, accumulated: [], generation: generation)
    }

    private func loadMarkerPage(run: RealRun, model: RealModelComparison, cursor: LedgerDetailCursor?,
                                accumulated: [EquityFillMarker], generation: UInt64) {
        guard generation == markerGeneration, selectedRun?.id == run.id, selectedModelId == model.modelId else { return }
        fillReader.page(runId: run.id, modelId: model.modelId, section: .fills, cursor: cursor,
                        expectedTotalCount: model.fillCount, expectedMarket: model.market) { [weak self] result in
            guard let self, generation == self.markerGeneration,
                  self.selectedRun?.id == run.id, self.selectedModelId == model.modelId else { return }
            switch result {
            case .success(let page):
                let added = page.typedFills.map { EquityFillMarker(timestamp: $0.identity.accountingTime, side: $0.side,
                                                                  fillId: $0.fillId, sequence: $0.identity.sequence,
                                                                  simulated: $0.fillObserved != true) }
                let markers = accumulated + added
                self.chartView.setFillMarkers(markers, total: page.totalCount,
                                              message: page.nextCursor == nil ? (markers.isEmpty ? "무거래 · 0 체결" : "원장 체결 시각 · 가격곡선과 별도") : "체결 시각 \(markers.count)/\(page.totalCount) 조회 중")
                if let next = page.nextCursor { self.loadMarkerPage(run: run, model: model, cursor: next, accumulated: markers, generation: generation) }
            case .failure:
                self.chartView.setFillMarkers([], total: model.fillCount, message: "체결 마커 조회 실패 · 체결 없음과 구분됩니다")
            }
        }
    }

    @objc private func chartModeChanged() {
        if let mode = ChartMode(rawValue: chartModeControl.selectedSegment) {
            chartView.setMode(mode)
        }
    }

    @objc private func runSelectorChanged() {
        if let run = runSelector.selectedItem?.representedObject as? RealRun {
            loadRunDetails(run)
            return
        }
        let idx = runSelector.indexOfSelectedItem - 1
        if idx >= 0 && idx < allRuns.count {
            loadRunDetails(allRuns[idx])
        } else {
            selectedRun = nil
            selectedModelId = nil
            modelSelector.removeAllItems()
            clearRunMetrics(message: "실행을 선택하면 해당 모델의 원장과 성과를 표시합니다.")
        }
    }

    @objc private func refreshClicked() {
        loadData()
        refreshCurrentSectionView()
    }

    @objc private func openSelectedLedger() {
        guard let run = selectedRun,
              let model = currentComparisons.first(where: { $0.modelId == selectedModelId }) else { return }
        let controller = ModelLedgerDetailWindowController(run: run, model: model, policyName: policyName(model.policyId))
        markerGeneration &+= 1
        let generation = markerGeneration
        controller.onFillsLoaded = { [weak self] fills, total in
            guard let self, generation == self.markerGeneration,
                  self.currentSection == .backtestAnalysis, self.showingRunDetails,
                  self.selectedRun?.id == run.id, self.selectedModelId == model.modelId else { return }
            self.chartView.setFillMarkers(fills.map { EquityFillMarker(timestamp: $0.identity.accountingTime, side: $0.side,
                                                                      fillId: $0.fillId, sequence: $0.identity.sequence,
                                                                      simulated: $0.fillObserved != true) },
                                          total: total, message: "원장 체결 시각 · 가격곡선과 별도")
        }
        ledgerDetailController = controller
        controller.showLedgerDetails()
    }

    @objc private func exportRunResults() {
        guard let run = selectedRun else { return }
        let records = currentComparisons.isEmpty
            ? [LedgerCSVRecord(run: run, model: nil, policyName: nil)]
            : currentComparisons.map { LedgerCSVRecord(run: run, model: $0, policyName: policyName($0.policyId)) }
        do { LedgerCSVExport.save(try LedgerCSVExport.models(records), suggestedName: "backcraft-run-results.csv", window: window) }
        catch { LedgerCSVExport.show(error, window: window) }
    }

    @objc private func exportHistory() {
        let filter = currentHistoryFilter()
        let reader = dbReader
        exportQueue.async { [weak self] in
            do {
                var runs: [RealRun] = []
                var cursor: RunHistoryCursor?
                var expectedCount: Int?
                var seenRunIds = Set<String>()
                repeat {
                    let page = try reader.fetchRunHistoryPage(filter: filter, cursor: cursor, limit: 100)
                    guard page.returnedCount == page.items.count,
                          expectedCount == nil || expectedCount == page.totalCount,
                          page.nextCursor == nil || page.nextCursor != cursor,
                          page.items.allSatisfy({ seenRunIds.insert($0.id).inserted }) else {
                        throw LedgerCSVError.changedScope
                    }
                    expectedCount = page.totalCount
                    runs.append(contentsOf: page.items)
                    cursor = page.nextCursor
                } while cursor != nil
                guard runs.count == expectedCount else { throw LedgerCSVError.changedScope }
                let data = try LedgerCSVExport.history(runs, filter: filter)
                DispatchQueue.main.async { [weak self] in
                    guard let self else { return }
                    LedgerCSVExport.save(data, suggestedName: "backcraft-history-\(runs.count).csv", window: self.window)
                }
            } catch {
                DispatchQueue.main.async { [weak self] in LedgerCSVExport.show(error, window: self?.window) }
            }
        }
    }

    @objc private func copySelectedRunID() {
        guard let id = selectedRun?.id else { return }
        NSPasteboard.general.clearContents()
        NSPasteboard.general.setString(id, forType: .string)
    }

    // MARK: - Section 2: All Runs History

    private var hasAdvancedHistoryFilters: Bool {
        historyPolicy.selectedItem?.representedObject as? String != nil
            || historyRevision.selectedItem?.representedObject as? String != nil
            || historyCost.selectedItem?.representedObject as? String != nil
            || historyDateEnabled.state == .on
            || historyPinnedOnly.state == .on
    }

    @objc private func toggleHistoryAdvancedFilters(_ sender: NSButton) {
        let expanded = sender.state == .on
        historyAdvancedFiltersPanel?.isHidden = !expanded
        updateHistoryFilterPresentation()
        NSAccessibility.post(element: sender, notification: .valueChanged)
    }

    private func buildRunsHistoryView() -> NSView {
        let container = NSView()
        container.setAccessibilityIdentifier("DashboardRunHistoryWorkspace")

        let titleLabel = NSTextField(labelWithString: "실행 이력")
        titleLabel.font = TerminalTheme.screenTitle

        runsSearchField.placeholderString = "실행·시장·정책·데이터셋 검색"
        runsSearchField.target = self
        runsSearchField.action = #selector(runsSearchChanged)
        runsSearchField.font = TerminalTheme.systemFont(ofSize: 12)
        runsSearchField.setAccessibilityLabel("실행 이력 검색")
        runsSearchField.widthAnchor.constraint(greaterThanOrEqualToConstant: 220 * TerminalTheme.textScale).isActive = true

        let exportButton = NSButton(title: "검색 결과 CSV", target: self, action: #selector(exportHistory))
        let searchRow = NSStackView(views: [runsSearchField, exportButton])
        searchRow.orientation = .horizontal
        searchRow.alignment = .centerY
        searchRow.spacing = 8
        runsSearchField.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        let header = NSStackView(views: [titleLabel, searchRow])
        header.orientation = .vertical
        header.alignment = .leading
        header.spacing = 8

        configureHistoryFilters()
        historyMore.target = self
        historyMore.action = #selector(loadMoreHistory)
        historyMore.isEnabled = historyCursor != nil
        historyCount.stringValue = "표시 \(filteredRuns.count) / \(historyTotal) · KST"
        historyCount.textColor = TerminalTheme.secondaryText
        historyCount.font = TerminalTheme.systemFont(ofSize: 12, weight: .medium)

        let scopeTitle = NSTextField(labelWithString: "기본 범위")
        scopeTitle.font = TerminalTheme.sectionHeading
        let scopeFilters = NSStackView(views: [historyStatus, historyMarket])
        scopeFilters.orientation = .horizontal
        scopeFilters.alignment = .centerY
        scopeFilters.spacing = 8
        let primaryScope = NSStackView(views: [scopeTitle, scopeFilters])
        primaryScope.orientation = .horizontal
        primaryScope.alignment = .centerY
        primaryScope.spacing = 8
        historyCount.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)

        let displayTitle = NSTextField(labelWithString: "표시")
        displayTitle.font = TerminalTheme.sectionHeading
        let displayControls = NSStackView(views: [displayTitle, historyDisplayMode, historyMetricSort])
        displayControls.orientation = .horizontal
        displayControls.alignment = .centerY
        displayControls.spacing = 8

        historyClearFilters.target = self
        historyClearFilters.action = #selector(resetHistoryFilters)
        historyClearFilters.bezelStyle = .rounded
        historyClearFilters.setAccessibilityLabel("실행 이력 필터 모두 지우기")
        let identityFilters = NSStackView(views: [historyPolicy, historyRevision])
        identityFilters.orientation = TerminalTheme.textScale >= 1.5 ? .vertical : .horizontal
        identityFilters.alignment = TerminalTheme.textScale >= 1.5 ? .leading : .centerY
        identityFilters.spacing = 8
        let filterRows = NSStackView(views: [identityFilters, historyCost])
        filterRows.orientation = .vertical
        filterRows.alignment = .leading
        filterRows.spacing = 8

        let dateRows = NSStackView()
        dateRows.orientation = .vertical
        dateRows.alignment = .leading
        dateRows.spacing = 8
        dateRows.addArrangedSubview(historyDateEnabled)
        if TerminalTheme.textScale >= 1.5 {
            dateRows.addArrangedSubview(historyFromDate)
            dateRows.addArrangedSubview(historyThroughDate)
        } else {
            let dateArrow = NSTextField(labelWithString: "→")
            dateArrow.setAccessibilityElement(false)
            let dateRange = NSStackView(views: [historyFromDate, dateArrow, historyThroughDate])
            dateRange.orientation = .horizontal
            dateRange.alignment = .centerY
            dateRange.spacing = 8
            dateRows.addArrangedSubview(dateRange)
        }
        let savedFilters = NSStackView(views: [historyPinnedOnly])
        savedFilters.orientation = .vertical
        savedFilters.alignment = .leading
        savedFilters.spacing = 8

        let advancedFilters = NSStackView(views: [filterRows, dateRows, savedFilters])
        advancedFilters.orientation = .vertical
        advancedFilters.alignment = .leading
        advancedFilters.spacing = 10
        advancedFilters.isHidden = !hasAdvancedHistoryFilters
        historyAdvancedFiltersPanel = advancedFilters
        historyAdvancedFiltersDisclosure.setButtonType(.toggle)
        historyAdvancedFiltersDisclosure.bezelStyle = .rounded
        historyAdvancedFiltersDisclosure.target = self
        historyAdvancedFiltersDisclosure.action = #selector(toggleHistoryAdvancedFilters(_:))
        historyAdvancedFiltersDisclosure.state = advancedFilters.isHidden ? .off : .on
        historyAdvancedFiltersDisclosure.setAccessibilityLabel("고급 실행 필터")
        historyAdvancedFiltersDisclosure.setAccessibilityValue(advancedFilters.isHidden ? "접힘" : "펼쳐짐")
        historyAdvancedFiltersDisclosure.setAccessibilityHelp("정책, 정책 리비전, 비용 조건, 기간, 고정 실행 필터")

        historyAppliedFiltersSummary.font = TerminalTheme.systemFont(ofSize: 11, weight: .medium)
        historyAppliedFiltersSummary.textColor = TerminalTheme.secondaryText
        historyAppliedFiltersSummary.maximumNumberOfLines = 2
        historyAppliedFiltersSummary.lineBreakMode = .byTruncatingTail
        historyAppliedFiltersSummary.setAccessibilityIdentifier("DashboardRunHistoryAppliedFiltersSummary")
        historyAppliedFiltersSummary.setAccessibilityLabel("현재 실행 이력 필터")
        historyAppliedFiltersSummary.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        let appliedFilters = NSStackView(views: [historyAppliedFiltersSummary, historyClearFilters, historyAdvancedFiltersDisclosure])
        appliedFilters.orientation = .horizontal
        appliedFilters.alignment = .centerY
        appliedFilters.spacing = 8
        updateHistoryFilterPresentation()

        setupRunsTable()
        let tableScroll = NSScrollView()
        tableScroll.documentView = runsTable
        tableScroll.hasVerticalScroller = true
        tableScroll.hasHorizontalScroller = true
        tableScroll.autohidesScrollers = true
        tableScroll.drawsBackground = false
        tableScroll.translatesAutoresizingMaskIntoConstraints = false
        tableScroll.identifier = NSUserInterfaceItemIdentifier("DashboardRunHistoryTableScroll")
        tableScroll.setAccessibilityLabel("실행 이력 표")
        historyTableScroll = tableScroll

        let leftPane = NSView()
        leftPane.setAccessibilityIdentifier("DashboardRunHistoryTablePane")
        leftPane.addSubview(tableScroll)
        historyMore.bezelStyle = .rounded
        historyMore.setContentCompressionResistancePriority(.required, for: .horizontal)
        historyMore.setContentHuggingPriority(.required, for: .horizontal)
        historyCount.lineBreakMode = .byTruncatingTail
        let footerSpacer = NSView()
        footerSpacer.setContentHuggingPriority(.defaultLow, for: .horizontal)
        let tableFooter = NSStackView(views: TerminalTheme.textScale >= 1.5
            ? [historyCount, historyMore]
            : [historyCount, footerSpacer, historyMore])
        tableFooter.orientation = TerminalTheme.textScale >= 1.5 ? .vertical : .horizontal
        tableFooter.alignment = TerminalTheme.textScale >= 1.5 ? .leading : .centerY
        tableFooter.spacing = 8
        tableFooter.translatesAutoresizingMaskIntoConstraints = false
        leftPane.addSubview(tableFooter)
        NSLayoutConstraint.activate([
            tableScroll.leadingAnchor.constraint(equalTo: leftPane.leadingAnchor),
            tableScroll.trailingAnchor.constraint(equalTo: leftPane.trailingAnchor, constant: -6),
            tableScroll.topAnchor.constraint(equalTo: leftPane.topAnchor),
            tableScroll.bottomAnchor.constraint(equalTo: tableFooter.topAnchor, constant: -6),
            tableFooter.leadingAnchor.constraint(equalTo: leftPane.leadingAnchor),
            tableFooter.trailingAnchor.constraint(equalTo: leftPane.trailingAnchor, constant: -6),
            tableFooter.bottomAnchor.constraint(equalTo: leftPane.bottomAnchor)
        ])

        historyDetail.font = TerminalTheme.body
        historyDetail.textColor = .labelColor
        historyDetail.maximumNumberOfLines = 0
        historyDetail.lineBreakMode = .byWordWrapping
        historyDetail.setAccessibilityLabel("선택된 실행 상세")
        historyIdentityRows.orientation = .vertical
        historyIdentityRows.alignment = .leading
        historyIdentityRows.spacing = 6
        historyIdentityRows.setAccessibilityIdentifier("DashboardRunHistoryIdentity")
        historyIdentityRows.setAccessibilityLabel("실행 식별 및 데이터 근거")
        historyOpen.target = self
        historyOpen.action = #selector(openHistorySelection)
        historyOpen.isEnabled = historySelection.count == 1
        for button in [historyOpen, historyCompare, historyRecent] {
            button.bezelStyle = .rounded
        }
        let primaryActions = NSStackView(views: [historyOpen, historyCompare, historyRecent, historyActionsPullDown])
        primaryActions.orientation = TerminalTheme.textScale >= 1.5 ? .vertical : .horizontal
        primaryActions.alignment = TerminalTheme.textScale >= 1.5 ? .leading : .centerY
        primaryActions.spacing = 8
        let selectedContent = NSStackView(views: [historyDetail, historyIdentityRows, primaryActions])
        selectedContent.orientation = .vertical
        selectedContent.alignment = .leading
        selectedContent.spacing = 8
        selectedContent.translatesAutoresizingMaskIntoConstraints = false
        historyDetail.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        let selectionPanel = NSBox()
        selectionPanel.boxType = .custom
        selectionPanel.cornerRadius = 8
        selectionPanel.title = "선택 상세"
        selectionPanel.titleFont = TerminalTheme.sectionHeading
        selectionPanel.borderWidth = 1
        selectionPanel.borderColor = TerminalTheme.border
        selectionPanel.fillColor = TerminalTheme.panel
        selectionPanel.contentViewMargins = NSSize(width: 12, height: 8)
        let selectionContent = NSView(frame: .zero)
        selectionPanel.setAccessibilityIdentifier("DashboardRunHistorySelectionInspector")
        selectionPanel.contentView = selectionContent
        selectionContent.addSubview(selectedContent)
        NSLayoutConstraint.activate([
            selectedContent.leadingAnchor.constraint(equalTo: selectionContent.leadingAnchor),
            selectedContent.trailingAnchor.constraint(equalTo: selectionContent.trailingAnchor),
            selectedContent.topAnchor.constraint(equalTo: selectionContent.topAnchor),
            selectedContent.bottomAnchor.constraint(equalTo: selectionContent.bottomAnchor)
        ])

        historyActionsPullDown.removeAllItems()
        historyActionsPullDown.pullsDown = true
        historyActionsPullDown.addItem(withTitle: "추가 작업")
        historyActionsPullDown.setAccessibilityLabel("선택 실행 추가 작업")
        historyActionsPullDown.toolTip = "선택한 실행 ID 복사, 고정 또는 선택 해제"
        let actionsMenu = historyActionsPullDown.menu ?? NSMenu(title: "추가 작업")
        historyCopyMenuItem = NSMenuItem(title: "실행 ID 복사", action: #selector(copyHistoryRunIDs), keyEquivalent: "")
        historyPinMenuItem = NSMenuItem(title: "선택 고정", action: #selector(pinSelectedHistory), keyEquivalent: "")
        historyClearMenuItem = NSMenuItem(title: "선택 해제", action: #selector(clearHistorySelection), keyEquivalent: "")
        for item in [historyCopyMenuItem, historyPinMenuItem, historyClearMenuItem].compactMap({ $0 }) {
            item.target = self
            actionsMenu.addItem(item)
        }
        historyActionsPullDown.menu = actionsMenu

        let contextMenu = NSMenu(title: "실행 이력")
        historyContextOpenMenuItem = NSMenuItem(title: "성과에서 열기", action: #selector(openHistorySelection), keyEquivalent: "")
        historyContextCompareMenuItem = NSMenuItem(title: "선택 비교", action: #selector(compareSelectedHistory), keyEquivalent: "")
        historyContextPinMenuItem = NSMenuItem(title: "선택 고정", action: #selector(pinSelectedHistory), keyEquivalent: "")
        historyContextCopyMenuItem = NSMenuItem(title: "실행 ID 복사", action: #selector(copyHistoryRunIDs), keyEquivalent: "")
        for item in [historyContextOpenMenuItem, historyContextCompareMenuItem,
                     historyContextPinMenuItem, historyContextCopyMenuItem].compactMap({ $0 }) {
            item.target = self
            contextMenu.addItem(item)
        }
        runsTable.menu = contextMenu
        runsTable.openSelectedRun = { [weak self] in self?.openHistorySelection() }
        runsTable.clearRunSelection = { [weak self] in self?.clearHistorySelection() }

        historyWorkspaceLabel.font = TerminalTheme.systemFont(ofSize: 11)
        historyWorkspaceLabel.textColor = TerminalTheme.secondaryText
        historyWorkspaceLabel.maximumNumberOfLines = 0
        historyWorkspaceLabel.lineBreakMode = .byWordWrapping
        historyWorkspaceLabel.setAccessibilityLabel("실행 이력 보기 상태 및 피드백")
        showHistoryWorkspaceFeedback(historyWorkspaceError ?? "필터·고정·선택은 이 기기에 저장됩니다.")
        let inspectorDocument = TerminalFlippedDocumentView()
        inspectorDocument.translatesAutoresizingMaskIntoConstraints = false
        let inspectorStack = NSStackView(views: [selectionPanel])
        inspectorStack.orientation = .vertical
        inspectorStack.alignment = .leading
        inspectorStack.spacing = 10
        inspectorStack.translatesAutoresizingMaskIntoConstraints = false
        inspectorDocument.addSubview(inspectorStack)
        let inspectorScroll = NSScrollView()
        inspectorScroll.documentView = inspectorDocument
        inspectorScroll.hasVerticalScroller = true
        inspectorScroll.hasHorizontalScroller = false
        inspectorScroll.autohidesScrollers = true
        inspectorScroll.drawsBackground = false
        inspectorScroll.setAccessibilityLabel("선택 실행 상세 및 보기 상태")
        inspectorScroll.translatesAutoresizingMaskIntoConstraints = false
        NSLayoutConstraint.activate([
            inspectorDocument.widthAnchor.constraint(equalTo: inspectorScroll.contentView.widthAnchor),
            inspectorStack.leadingAnchor.constraint(equalTo: inspectorDocument.leadingAnchor),
            inspectorStack.trailingAnchor.constraint(equalTo: inspectorDocument.trailingAnchor),
            inspectorStack.topAnchor.constraint(equalTo: inspectorDocument.topAnchor),
            inspectorStack.bottomAnchor.constraint(equalTo: inspectorDocument.bottomAnchor),
            selectionPanel.widthAnchor.constraint(equalTo: inspectorStack.widthAnchor)
        ])
        let inspectorPane = NSView()
        inspectorPane.setAccessibilityIdentifier("DashboardRunHistoryInspectorPane")
        inspectorPane.addSubview(inspectorScroll)
        historyWorkspaceLabel.translatesAutoresizingMaskIntoConstraints = false
        historyWorkspaceLabel.maximumNumberOfLines = 0
        inspectorPane.addSubview(historyWorkspaceLabel)
        NSLayoutConstraint.activate([
            inspectorScroll.leadingAnchor.constraint(equalTo: inspectorPane.leadingAnchor, constant: 8),
            inspectorScroll.trailingAnchor.constraint(equalTo: inspectorPane.trailingAnchor),
            inspectorScroll.topAnchor.constraint(equalTo: inspectorPane.topAnchor),
            inspectorScroll.bottomAnchor.constraint(equalTo: historyWorkspaceLabel.topAnchor, constant: -8),
            historyWorkspaceLabel.leadingAnchor.constraint(equalTo: inspectorPane.leadingAnchor, constant: 8),
            historyWorkspaceLabel.trailingAnchor.constraint(equalTo: inspectorPane.trailingAnchor),
            historyWorkspaceLabel.bottomAnchor.constraint(equalTo: inspectorPane.bottomAnchor)
        ])

        let workbench = HistoryWorkbenchSplitView()
        workbench.isVertical = true
        workbench.dividerStyle = .thin
        workbench.delegate = self
        workbench.identifier = NSUserInterfaceItemIdentifier("DashboardRunHistoryWorkbench")
        workbench.translatesAutoresizingMaskIntoConstraints = false
        workbench.addArrangedSubview(leftPane)
        workbench.addArrangedSubview(inspectorPane)
        workbench.orientationDidChange = { [weak self] vertical in self?.restoreHistoryDividerPosition(vertical: vertical) }
        historyWorkbenchSplitView = workbench

        let stack = NSStackView(views: [header, primaryScope, appliedFilters, advancedFilters,
                                        displayControls, workbench])
        stack.orientation = .vertical
        stack.alignment = .leading
        stack.spacing = 8
        stack.detachesHiddenViews = true
        stack.translatesAutoresizingMaskIntoConstraints = false
        workbench.setContentCompressionResistancePriority(.defaultLow, for: .vertical)
        container.addSubview(stack)

        NSLayoutConstraint.activate([
            stack.leadingAnchor.constraint(equalTo: container.leadingAnchor, constant: 20),
            stack.trailingAnchor.constraint(equalTo: container.trailingAnchor, constant: -20),
            stack.topAnchor.constraint(equalTo: container.topAnchor, constant: 16),
            stack.bottomAnchor.constraint(equalTo: container.bottomAnchor, constant: -16),
            header.widthAnchor.constraint(equalTo: stack.widthAnchor),
            searchRow.widthAnchor.constraint(equalTo: header.widthAnchor),
            primaryScope.widthAnchor.constraint(equalTo: stack.widthAnchor),
            displayControls.widthAnchor.constraint(equalTo: stack.widthAnchor),
            appliedFilters.widthAnchor.constraint(equalTo: stack.widthAnchor),
            historyAppliedFiltersSummary.widthAnchor.constraint(lessThanOrEqualTo: appliedFilters.widthAnchor),
            advancedFilters.widthAnchor.constraint(equalTo: stack.widthAnchor),
            workbench.widthAnchor.constraint(equalTo: stack.widthAnchor),
            workbench.heightAnchor.constraint(greaterThanOrEqualToConstant: 260)
        ])

        fetchHistoryPage(append: false)
        return container
    }

    private func showHistoryWorkspaceFeedback(_ message: String) {
        guard historyWorkspaceLabel.stringValue != message else { return }
        historyWorkspaceLabel.stringValue = message
        historyWorkspaceLabel.toolTip = message
        historyWorkspaceLabel.setAccessibilityValue(message)
        if historyWorkspaceLabel.superview != nil {
            NSAccessibility.post(element: historyWorkspaceLabel, notification: .valueChanged)
        }
    }

    private func updateHistoryFilterPresentation() {
        var filters: [String] = []
        if historyStatus.indexOfSelectedItem > 0, let title = historyStatus.titleOfSelectedItem { filters.append("상태 \(title)") }
        if historyMarket.indexOfSelectedItem > 0, let title = historyMarket.titleOfSelectedItem { filters.append("시장 \(title)") }
        if let policyId = historyPolicy.selectedItem?.representedObject as? String {
            filters.append("정책 \(historyPolicy.titleOfSelectedItem ?? String(policyId.suffix(8)))")
        }
        if let revisionId = historyRevision.selectedItem?.representedObject as? String {
            filters.append("리비전 \(historyRevision.titleOfSelectedItem ?? String(revisionId.suffix(8)))")
        }
        if let cost = historyCost.selectedItem?.representedObject as? String { filters.append("비용 \(historyCostTitle(cost))") }
        if historyDateEnabled.state == .on {
            let formatter = Self.historyDayFormatter()
            filters.append("기록일 \(formatter.string(from: historyFromDate.dateValue))–\(formatter.string(from: historyThroughDate.dateValue)) KST")
        }
        if historyPinnedOnly.state == .on { filters.append("고정만 \(historyPinnedIds.count)개") }
        let search = runsSearchField.stringValue.trimmingCharacters(in: .whitespacesAndNewlines)
        if !search.isEmpty { filters.insert("검색 \(search)", at: 0) }

        let summary = filters.isEmpty ? "활성 필터 없음" : "적용 중: " + filters.joined(separator: " · ")
        historyAppliedFiltersSummary.stringValue = summary
        historyAppliedFiltersSummary.toolTip = summary
        historyAppliedFiltersSummary.setAccessibilityValue(summary)
        historyClearFilters.isEnabled = !filters.isEmpty
        historyClearFilters.toolTip = filters.isEmpty ? "지울 실행 이력 필터가 없습니다." : summary
        let advancedCount = (historyPolicy.selectedItem?.representedObject as? String != nil ? 1 : 0)
            + (historyRevision.selectedItem?.representedObject as? String != nil ? 1 : 0)
            + (historyCost.selectedItem?.representedObject as? String != nil ? 1 : 0)
            + (historyDateEnabled.state == .on ? 1 : 0)
            + (historyPinnedOnly.state == .on ? 1 : 0)
        historyAdvancedFiltersDisclosure.title = advancedCount > 0 ? "고급 필터 · \(advancedCount)개 적용" : "고급 필터"
        historyAdvancedFiltersDisclosure.setAccessibilityValue("\(historyAdvancedFiltersPanel?.isHidden == false ? "펼쳐짐" : "접힘") · 고급 필터 \(advancedCount)개 적용")
        historyAdvancedFiltersDisclosure.setAccessibilityHelp(summary)
    }

    private func configureHistoryFilters() {
        guard !historyFiltersConfigured else { return }
        historyFiltersConfigured = true
        historyStatus.addItems(withTitles: ["모든 상태", "완료", "실패", "차단", "중단", "실행 중"])
        historyMarket.addItems(withTitles: ["모든 시장", "KRW-BTC", "KRW-ETH", "KRW-XRP"])
        historyPolicy.addItem(withTitle: "모든 정책")
        for policy in allPolicies.sorted(by: { $0.name < $1.name }) {
            historyPolicy.addItem(withTitle: policy.name)
            historyPolicy.lastItem?.representedObject = policy.policyId
            historyPolicy.lastItem?.toolTip = policy.policyId
        }
        historyCost.addItem(withTitle: "모든 비용 조건")
        for label in historyFilterOptions?.costAssumptionLabels ?? [] {
            historyCost.addItem(withTitle: label)
            historyCost.lastItem?.representedObject = label
            historyCost.lastItem?.toolTip = label
        }
        for popup in [historyStatus, historyMarket, historyRevision, historyCost] {
            popup.target = self; popup.action = #selector(runsSearchChanged)
        }
        historyPolicy.target = self; historyPolicy.action = #selector(historyPolicyChanged)
        rebuildHistoryRevisions()
        for (popup, label, width) in [(historyStatus, "실행 상태", 102.0), (historyMarket, "실행 시장", 108.0),
                                      (historyPolicy, "실행 정책", 270.0), (historyRevision, "정책 리비전", 230.0)] {
            popup.setAccessibilityLabel(label)
            popup.font = TerminalTheme.systemFont(ofSize: 12)
            popup.widthAnchor.constraint(equalToConstant: width * TerminalTheme.textScale).isActive = true
        }
        historyCost.setAccessibilityLabel("비용 시나리오")
        historyCost.font = TerminalTheme.systemFont(ofSize: 12)
        historyCost.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        historyDateEnabled.target = self; historyDateEnabled.action = #selector(runsSearchChanged)
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = TimeZone(identifier: "Asia/Seoul")!
        for (picker, label) in [(historyFromDate, "기록 시작일 KST"), (historyThroughDate, "기록 마지막 날 KST")] {
            picker.datePickerStyle = .textFieldAndStepper
            picker.datePickerElements = .yearMonthDay
            picker.timeZone = calendar.timeZone
            picker.calendar = calendar
            picker.dateValue = Date()
            picker.target = self; picker.action = #selector(runsSearchChanged)
            picker.setAccessibilityLabel(label)
            picker.widthAnchor.constraint(equalToConstant: 125 * TerminalTheme.textScale).isActive = true
            picker.isEnabled = false
        }
        if let latest = allRuns.first { historyFromDate.dateValue = Date(timeIntervalSince1970: Double(latest.createdAtMilliseconds) / 1000) }
        historyPinnedOnly.target = self; historyPinnedOnly.action = #selector(runsSearchChanged)
        historyDisplayMode.selectedSegment = 0
        historyDisplayMode.target = self; historyDisplayMode.action = #selector(historyDisplayChanged)
        historyCompare.target = self; historyCompare.action = #selector(compareSelectedHistory)
        historyRecent.target = self; historyRecent.action = #selector(openRecentHistoryRun)
        historyMetricSort.addItems(withTitles: ["표시된 행 정렬", "선택 모델 수익률 ↓", "선택 모델 낙폭 ↑"])
        historyMetricSort.target = self; historyMetricSort.action = #selector(historyMetricSortChanged)
        historyMetricSort.setAccessibilityLabel("표시된 실행의 정확한 선택 모델 정렬")
        applyRestoredHistoryState()
    }

    private func rebuildHistoryRevisions() {
        let selectedRevision = historyRevision.selectedItem?.representedObject as? String
        let policyId = historyPolicy.selectedItem?.representedObject as? String
        historyRevision.removeAllItems(); historyRevision.addItem(withTitle: "모든 리비전")
        for revision in historyFilterOptions?.policyRevisions ?? [] where policyId == nil || revision.policyId == policyId {
            historyRevision.addItem(withTitle: "r\(revision.revisionNumber) · \(revision.name)")
            historyRevision.lastItem?.representedObject = revision.id
            historyRevision.lastItem?.toolTip = "\(revision.policyId)\n\(revision.id)\n\(revision.definitionDigest)"
        }
        if let selectedRevision, let index = historyRevision.itemArray.firstIndex(where: { $0.representedObject as? String == selectedRevision }) {
            historyRevision.selectItem(at: index)
        }
    }

    @objc private func historyPolicyChanged() { rebuildHistoryRevisions(); runsSearchChanged() }
    @objc private func historyDisplayChanged() { setupRunsTable(); restoreHistorySelection(); updateHistorySelection(); persistHistoryState() }
    @objc private func resetHistoryFilters() {
        runsSearchField.stringValue = ""
        for popup in [historyStatus, historyMarket, historyPolicy, historyCost] { popup.selectItem(at: 0) }
        rebuildHistoryRevisions(); historyRevision.selectItem(at: 0)
        historyDateEnabled.state = .off; historyPinnedOnly.state = .off
        historyAdvancedFiltersDisclosure.state = .off
        historyAdvancedFiltersPanel?.isHidden = true
        updateHistoryFilterPresentation()
        runsSearchChanged()
    }

    private static func historyDayFormatter() -> DateFormatter {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(identifier: "Asia/Seoul")
        formatter.dateFormat = "yyyy-MM-dd"
        formatter.isLenient = false
        return formatter
    }

    private func applyRestoredHistoryState() {
        guard let state = restoredHistoryState else { return }
        applyingHistoryState = true
        defer { applyingHistoryState = false; restoredHistoryState = nil }
        runsSearchField.stringValue = state.search
        func selectTitle(_ popup: NSPopUpButton, value: String?) {
            guard let value else { return }
            if let item = popup.itemArray.first(where: { $0.title == value }) { popup.select(item) }
            else { popup.addItem(withTitle: value); popup.selectItem(at: popup.numberOfItems - 1) }
        }
        let statusNames = ["COMPLETED": "완료", "FAILED": "실패", "BLOCKED": "차단", "CANCELLED": "중단", "RUNNING": "실행 중"]
        selectTitle(historyStatus, value: state.status.map { statusNames[$0] ?? $0 })
        selectTitle(historyMarket, value: state.market)
        func selectIdentity(_ popup: NSPopUpButton, value: String?, missingTitle: String) {
            guard let value else { return }
            if let item = popup.itemArray.first(where: { $0.representedObject as? String == value }) { popup.select(item) }
            else {
                popup.addItem(withTitle: missingTitle)
                popup.lastItem?.representedObject = value
                popup.lastItem?.toolTip = value
                popup.selectItem(at: popup.numberOfItems - 1)
            }
        }
        selectIdentity(historyPolicy, value: state.policyId, missingTitle: "미확인 정책 · \(state.policyId?.suffix(8) ?? "")")
        rebuildHistoryRevisions()
        selectIdentity(historyRevision, value: state.policyRevisionId, missingTitle: "미확인 리비전 · \(state.policyRevisionId?.suffix(8) ?? "")")
        selectIdentity(historyCost, value: state.costAssumptionLabel, missingTitle: state.costAssumptionLabel ?? "미확인 비용")
        historyDateEnabled.state = state.dateEnabled ? .on : .off
        let formatter = Self.historyDayFormatter()
        if let value = state.fromDayKST, let date = formatter.date(from: value) { historyFromDate.dateValue = date }
        if let value = state.throughDayKST, let date = formatter.date(from: value) { historyThroughDate.dateValue = date }
        historyFromDate.isEnabled = state.dateEnabled; historyThroughDate.isEnabled = state.dateEnabled
        historyPinnedIds = Set(state.pinnedRunIds)
        historyPinnedOnly.state = state.pinnedOnly ? .on : .off
        historyDisplayMode.selectedSegment = state.detailedColumns ? 1 : 0
        historyMetricSort.selectItem(at: state.metricSort == "return_desc" ? 1 : state.metricSort == "drawdown_asc" ? 2 : 0)
        lastOpenedHistoryRunId = state.lastOpenedRunId
        do {
            let runs = try dbReader.fetchRunHistoryPage(filter: RunHistoryFilter(pinnedRunIds: state.selectedRunIds), limit: 4).items
            historySelection = Dictionary(uniqueKeysWithValues: runs.map { ($0.id, $0) })
            if runs.count != state.selectedRunIds.count { historyWorkspaceError = "일부 최근 선택 실행이 현재 원장에 없습니다. 다른 실행으로 대체하지 않았습니다." }
        } catch { historyWorkspaceError = "최근 실행 선택 복원 실패 · 다른 실행으로 대체하지 않았습니다." }
    }

    private func persistHistoryState() {
        guard !applyingHistoryState else { return }
        guard historyFiltersConfigured else {
            var state = restoredHistoryState ?? .empty
            state.lastOpenedRunId = lastOpenedHistoryRunId
            restoredHistoryState = state
            try? historyWorkspace.saveHistoryState(state)
            return
        }
        let filter = currentHistoryFilter()
        let formatter = Self.historyDayFormatter()
        let state = HistoryPresentationState(search: runsSearchField.stringValue, status: filter.status,
            market: filter.market, policyId: filter.policyId, policyRevisionId: filter.policyRevisionId,
            costAssumptionLabel: filter.costAssumptionLabel, dateEnabled: historyDateEnabled.state == .on,
            fromDayKST: formatter.string(from: historyFromDate.dateValue), throughDayKST: formatter.string(from: historyThroughDate.dateValue),
            pinnedRunIds: historyPinnedIds.sorted(), pinnedOnly: historyPinnedOnly.state == .on,
            selectedRunIds: historySelection.keys.sorted(),
            detailedColumns: historyDisplayMode.selectedSegment == 1,
            metricSort: historyMetricSort.indexOfSelectedItem == 1 ? "return_desc" : historyMetricSort.indexOfSelectedItem == 2 ? "drawdown_asc" : nil,
            lastOpenedRunId: lastOpenedHistoryRunId)
        do {
            try historyWorkspace.saveHistoryState(state)
            showHistoryWorkspaceFeedback("필터·고정·선택 보기 상태 저장됨")
        } catch { showHistoryWorkspaceFeedback("보기 상태 저장 실패 · 기존 파일을 유지합니다. \(error.localizedDescription)") }
    }

    @objc private func openRecentHistoryRun() {
        guard let id = lastOpenedHistoryRunId else { return }
        do {
            guard let run = try dbReader.fetchRunHistoryPage(filter: RunHistoryFilter(pinnedRunIds: [id]), limit: 1).items.first else {
                showHistoryWorkspaceFeedback("최근 실행이 현재 원장에 없습니다. 다른 실행으로 대체하지 않았습니다.")
                return
            }
            showingRunDetails = true; selectedModelId = nil
            loadRunDetails(run); selectSection(.backtestAnalysis)
        } catch { showHistoryWorkspaceFeedback("최근 실행 조회 실패 · 원장을 확인하세요.") }
    }

    @objc private func historyMetricSortChanged() {
        persistHistoryState()
        if historyMetricSort.indexOfSelectedItem == 0 {
            resetHistoryMetricOrdering()
            historyCount.stringValue = "표시 \(filteredRuns.count) / \(historyTotal) · KST"
            showHistoryWorkspaceFeedback("지표 정렬 해제 · 표시된 실행을 최신순으로 정렬합니다.")
        } else { applyHistoryMetricOrdering() }
    }

    private func resetHistoryMetricOrdering() {
        historyMetricSort.selectItem(at: 0)
        historyMetricValues = [:]; historyMetricAvailability = [:]
        runsTable.sortDescriptors = []
        filteredRuns.sort {
            $0.createdAtMilliseconds == $1.createdAtMilliseconds
                ? $0.id > $1.id : $0.createdAtMilliseconds > $1.createdAtMilliseconds
        }
        restoringHistorySelection = true; setupRunsTable(); runsTable.reloadData(); restoringHistorySelection = false
        restoreHistorySelection(); updateHistorySelection()
    }

    private func applyHistoryMetricOrdering() {
        guard historyMetricSort.indexOfSelectedItem > 0 else { return }
        persistHistoryState()
        guard let policyId = historyPolicy.selectedItem?.representedObject as? String,
              historyMarket.indexOfSelectedItem > 0, let market = historyMarket.titleOfSelectedItem else {
            resetHistoryMetricOrdering()
            persistHistoryState()
            showHistoryWorkspaceFeedback("지표 정렬은 정책과 시장을 정확히 선택한 뒤 사용할 수 있습니다. 독립 모델을 합산하지 않습니다.")
            return
        }
        do {
            let models = try dbReader.fetchModelComparisons(for: filteredRuns.map(\.id))
            let scope = HistoryMetricScope(policyId: policyId, market: market,
                policyRevisionId: historyRevision.selectedItem?.representedObject as? String)
            let metric: HistoryMetricSort = historyMetricSort.indexOfSelectedItem == 1 ? .netReturn : .maxDrawdown
            let result = try HistoryMetricOrdering.order(runs: filteredRuns, models: models, scope: scope,
                metric: metric, ascending: historyMetricSort.indexOfSelectedItem == 2)
            filteredRuns = result.orderedRuns
            historyMetricValues = result.valuesByRunId
            historyMetricAvailability = result.availabilityByRunId
            runsTable.sortDescriptors = []
            restoringHistorySelection = true; setupRunsTable(); runsTable.reloadData(); restoringHistorySelection = false
            restoreHistorySelection(); updateHistorySelection()
            historyCount.stringValue = "표시 \(filteredRuns.count) / \(historyTotal) · 선택 모델 정렬 · 미확인 \(result.unavailableCount) · 모호 \(result.ambiguousCount)"
            showHistoryWorkspaceFeedback("정책·시장·리비전의 단일 모델로 표시된 행만 정렬합니다. 기간·비용·자본 조건을 함께 확인하세요.")
        } catch {
            resetHistoryMetricOrdering()
            persistHistoryState()
            showHistoryWorkspaceFeedback("지표 정렬 불가 · 정확한 모델 또는 표시된 행 범위를 확인하세요(최대 100행).")
        }
    }

    private func historyCostTitle(_ label: String?) -> String {
        guard let label else { return "미확인" }
        for (prefix, name) in [("Frozen baseline scenario: one-way ", "기준"),
                               ("Frozen stress scenario: one-way ", "스트레스"),
                               ("Frozen common scenario: one-way ", "공통")] {
            if label.hasPrefix(prefix), let bp = label.dropFirst(prefix.count).split(separator: " ").first {
                return "편도 \(bp) · \(name)"
            }
        }
        return label
    }

    private func setupRunsTable() {
        runsTable.removeAllColumns()
        var columns: [(String, String, CGFloat)] = [
            ("Pinned", "고정", 48), ("Date", "기록 KST", 150), ("Markets", "시장", 110),
            ("State", "상태", 70), ("Costs", "비용 시나리오", 235), ("Refs", "정책·데이터", 90), ("F08", "F08", 70)
        ]
        if historyMetricSort.indexOfSelectedItem > 0 {
            let metricColWidth = Self.scaledHistoryColumnWidth(260)
            columns.insert(("SelectedMetric", historyMetricSort.indexOfSelectedItem == 1 ? "모델 수익률" : "모델 최대 낙폭", metricColWidth), at: 2)
        }
        if historyDisplayMode.selectedSegment == 1 {
            columns += [("Window", "평가 기간 KST", 280), ("RunId", "실행 ID", 220), ("Mode", "유형", 150), ("Engine", "엔진 버전", 180)]
        }
        for (id, title, width) in columns {
            let column = NSTableColumn(identifier: NSUserInterfaceItemIdentifier(id))
            let scaledWidth = id == "SelectedMetric" ? width : Self.scaledHistoryColumnWidth(width)
            column.title = title; column.width = scaledWidth; column.minWidth = scaledWidth
            column.resizingMask = .userResizingMask
            if id != "SelectedMetric" { column.sortDescriptorPrototype = NSSortDescriptor(key: id, ascending: true) }
            runsTable.addTableColumn(column)
        }
        runsTable.dataSource = self; runsTable.delegate = self
        TerminalTheme.prepareTable(runsTable)
        runsTable.columnAutoresizingStyle = .noColumnAutoresizing
        runsTable.allowsMultipleSelection = true
        runsTable.doubleAction = #selector(runTableDoubleClicked); runsTable.target = self
    }

    static func scaledHistoryColumnWidth(_ baseWidth: CGFloat) -> CGFloat {
        ceil(baseWidth * TerminalTheme.textScale)
    }

    private func restoreHistorySelection() {
        restoringHistorySelection = true
        runsTable.selectRowIndexes(IndexSet(filteredRuns.indices.filter { historySelection[filteredRuns[$0].id] != nil }), byExtendingSelection: false)
        restoringHistorySelection = false
    }

    private func selectedMetricSummary(for runId: String) -> String? {
        guard historyMetricSort.indexOfSelectedItem > 0 else { return nil }
        let label = historyMetricSort.indexOfSelectedItem == 1 ? "모델 수익률" : "모델 최대 낙폭"
        if let value = historyMetricValues[runId] {
            return "\(label) \(Formatters.percent(value, signed: historyMetricSort.indexOfSelectedItem == 1))"
        }
        switch historyMetricAvailability[runId] {
        case .ambiguousModels(let count):
            return "\(label) 모호 · \(count) 모델"
        case .missingModel:
            return "\(label) 모델 없음"
        case .missingMetric(let reason):
            if let reason, !reason.isEmpty {
                return "\(label) 지표 미확인 · \(reason)"
            }
            return "\(label) 지표 미확인"
        default:
            return "\(label) 미확인"
        }
    }

    private func updateHistorySelection() {
        historyDetail.toolTip = nil
        let count = historySelection.count
        let visibleIds = Set(filteredRuns.map(\.id))
        let hidden = historySelection.keys.filter { !visibleIds.contains($0) }.count
        historyRecent.isEnabled = lastOpenedHistoryRunId != nil
        historyOpen.isEnabled = count == 1
        historyCompare.isEnabled = (2...4).contains(count)
        let allSelectedPinned = count > 0 && historySelection.keys.allSatisfy({ historyPinnedIds.contains($0) })
        let pinTitle = allSelectedPinned ? "선택 고정 해제" : "선택 고정"
        historyPinMenuItem?.title = pinTitle
        historyContextPinMenuItem?.title = pinTitle
        historyCopyMenuItem?.isEnabled = count > 0
        historyPinMenuItem?.isEnabled = count > 0
        historyClearMenuItem?.isEnabled = count > 0
        historyContextOpenMenuItem?.isEnabled = count == 1
        historyContextCompareMenuItem?.isEnabled = (2...4).contains(count)
        historyContextPinMenuItem?.isEnabled = count > 0
        historyContextCopyMenuItem?.isEnabled = count > 0
        historyActionsPullDown.isEnabled = count > 0

        for row in historyIdentityRows.arrangedSubviews {
            historyIdentityRows.removeArrangedSubview(row)
            row.removeFromSuperview()
        }
        if count == 1, let run = historySelection.values.first {
            let metricSuffix = selectedMetricSummary(for: run.id).map { " · \($0)" } ?? ""
            historyDetail.stringValue = "\(TerminalPresentation.state(run.state)) · F08 \(run.f08Status ?? "미검증")\(metricSuffix) · \(hidden > 0 ? "현재 필터에서 숨김" : "현재 목록")"
            historyDetail.toolTip = historyDetail.stringValue
            appendHistoryIdentityFields(for: run)
        } else {
            historyDetail.stringValue = count == 0
                ? "실행을 선택하세요. ⌘ 클릭으로 최대 4개를 비교할 수 있습니다."
                : "\(count)개 선택 · 현재 필터에서 숨김 \(hidden)개 · 고정 \(historyPinnedIds.count)개\n기간·비용·자본이 다른 결과는 독립 그래프로 비교합니다."
            if count > 1 {
                for run in historySelection.values.sorted(by: { $0.createdAtMilliseconds > $1.createdAtMilliseconds }) {
                    appendHistoryIdentityFields(for: run, includeHeading: true)
                }
            }
        }
        historyDetail.setAccessibilityValue(historyDetail.stringValue)
        NSAccessibility.post(element: historyDetail, notification: .valueChanged)
        refreshSelectionAppearance(runsTable)
    }

    private func appendHistoryIdentityFields(for run: RealRun, includeHeading: Bool = false) {
        if includeHeading {
            let heading = NSTextField(labelWithString: "\(TerminalPresentation.state(run.state)) · \(run.createdDateStr)")
            heading.font = TerminalTheme.sectionHeading
            heading.setAccessibilityLabel("선택 실행 · \(run.id)")
            historyIdentityRows.addArrangedSubview(heading)
        }

        let revisionIds = run.policyRevisionIds ?? []
        let policyIds = Set(historyFilterOptions?.policyRevisions
            .filter { revisionIds.contains($0.id) }
            .map(\.policyId) ?? []).sorted()
        func valueOrDash(_ value: String?) -> String {
            guard let value, !value.isEmpty else { return "—" }
            return value
        }
        let marketValue = run.markets?.isEmpty == false ? run.markets?.joined(separator: ", ") ?? "—" : "—"
        let datasetValue = run.datasetIds?.isEmpty == false ? run.datasetIds?.joined(separator: "\n") ?? "—" : "—"
        let values: [(String, String)] = [
            ("실행 ID", run.id),
            ("정책 ID", policyIds.isEmpty ? "—" : policyIds.joined(separator: "\n")),
            ("리비전 ID", revisionIds.isEmpty ? "—" : revisionIds.joined(separator: "\n")),
            ("시장", marketValue),
            ("데이터셋 ID", datasetValue),
            ("실행 유형", valueOrDash(run.mode)),
            ("기록 시각 KST", valueOrDash(run.createdDateStr)),
            ("평가 기간 UTC", "\(valueOrDash(run.windowStartUTC)) → \(valueOrDash(run.windowEndUTC))")
        ]
        for (labelText, valueText) in values {
            let label = NSTextField(labelWithString: labelText)
            label.font = TerminalTheme.systemFont(ofSize: 10, weight: .semibold)
            label.textColor = TerminalTheme.secondaryText
            label.setAccessibilityElement(false)
            let value = NSTextField(wrappingLabelWithString: valueText)
            value.font = TerminalTheme.monospacedFont(ofSize: 10)
            value.textColor = .labelColor
            value.isSelectable = true
            value.lineBreakMode = .byTruncatingMiddle
            value.maximumNumberOfLines = 3
            value.toolTip = valueText
            value.setAccessibilityLabel(labelText)
            value.setAccessibilityValue(valueText)
            value.setAccessibilityHelp(valueText == "—" ? "이 실행에 대해 확인된 값이 없습니다." : valueText)
            value.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
            let row = NSStackView(views: [label, value])
            row.orientation = TerminalTheme.textScale >= 1.5 ? .vertical : .horizontal
            row.alignment = TerminalTheme.textScale >= 1.5 ? .leading : .firstBaseline
            row.spacing = 4
            if TerminalTheme.textScale < 1.5 {
                label.widthAnchor.constraint(equalToConstant: 82).isActive = true
            }
            historyIdentityRows.addArrangedSubview(row)
            row.widthAnchor.constraint(equalTo: historyIdentityRows.widthAnchor).isActive = true
        }
    }

    @objc private func copyHistoryRunIDs() {
        NSPasteboard.general.clearContents()
        NSPasteboard.general.setString(historySelection.keys.sorted().joined(separator: "\n"), forType: .string)
    }

    @objc private func clearHistorySelection() {
        historySelection.removeAll()
        restoreHistorySelection(); updateHistorySelection(); persistHistoryState()
    }

    @objc private func pinSelectedHistory() {
        let ids = Set(historySelection.keys)
        if ids.isSubset(of: historyPinnedIds) { historyPinnedIds.subtract(ids) }
        else {
            guard historyPinnedIds.union(ids).count <= 100 else { historyDetail.stringValue = "고정 실행은 최대 100개입니다."; return }
            historyPinnedIds.formUnion(ids)
        }
        persistHistoryState(); fetchHistoryPage(append: false)
    }

    @objc private func compareSelectedHistory() {
        guard (2...4).contains(historySelection.count) else { return }
        let runs = historySelection.values.sorted { $0.createdAtMilliseconds > $1.createdAtMilliseconds }
        let revisionId = historyRevision.selectedItem?.representedObject as? String
        let policyId = historyPolicy.selectedItem?.representedObject as? String
            ?? historyFilterOptions?.policyRevisions.first(where: { $0.id == revisionId })?.policyId
        let controller = RunComparisonWindowController(reader: dbReader, runs: runs, policies: allPolicies,
            initialPolicyId: policyId, policyRevisionScopeId: revisionId,
            onOpenRun: { [weak self] run, modelId in
                guard let self else { return }
                self.runComparisonController?.close()
                self.lastOpenedHistoryRunId = run.id
                self.persistHistoryState()
                self.showingRunDetails = true; self.selectedModelId = modelId
                self.loadRunDetails(run); self.selectSection(.backtestAnalysis)
            })
        runComparisonController = controller; controller.showComparison()
    }

    @objc private func runsSearchChanged() {
        historyFromDate.isEnabled = historyDateEnabled.state == .on
        historyThroughDate.isEnabled = historyDateEnabled.state == .on
        historyCost.toolTip = historyCost.selectedItem?.representedObject as? String
        updateHistoryFilterPresentation()
        persistHistoryState(); fetchHistoryPage(append: false)
    }

    @objc private func loadMoreHistory() { fetchHistoryPage(append: true) }

    private func currentHistoryFilter() -> RunHistoryFilter {
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = TimeZone(identifier: "Asia/Seoul")!
        let dateActive = historyDateEnabled.state == .on
        let from = calendar.startOfDay(for: historyFromDate.dateValue)
        let through = calendar.date(byAdding: .day, value: 1, to: calendar.startOfDay(for: historyThroughDate.dateValue))!
        return RunHistoryFilter(search: runsSearchField.stringValue,
            status: historyStatus.indexOfSelectedItem > 0 ? ["완료": "COMPLETED", "실패": "FAILED", "차단": "BLOCKED", "중단": "CANCELLED", "실행 중": "RUNNING"][historyStatus.titleOfSelectedItem ?? ""] ?? historyStatus.titleOfSelectedItem : nil,
            market: historyMarket.indexOfSelectedItem > 0 ? historyMarket.titleOfSelectedItem : nil,
            policyId: historyPolicy.selectedItem?.representedObject as? String,
            createdFromMilliseconds: dateActive ? Int64(from.timeIntervalSince1970 * 1000) : nil,
            createdBeforeMilliseconds: dateActive ? Int64(through.timeIntervalSince1970 * 1000) : nil,
            policyRevisionId: historyRevision.selectedItem?.representedObject as? String,
            costAssumptionLabel: historyCost.selectedItem?.representedObject as? String,
            pinnedRunIds: historyPinnedOnly.state == .on ? historyPinnedIds.sorted() : nil)
    }

    private func fetchHistoryPage(append: Bool) {
        let filter = currentHistoryFilter()
        do {
            let page = try dbReader.fetchRunHistoryPage(filter: filter, cursor: append ? historyCursor : nil, limit: 50)
            filteredRuns = append ? filteredRuns + page.items : page.items
            historyTotal = page.totalCount
            historyCursor = page.nextCursor
            historyMore.isEnabled = page.nextCursor != nil
            historyCount.stringValue = "표시 \(filteredRuns.count) / \(page.totalCount) · KST"
            if !append { resetHistoryTableScroll() }
            restoringHistorySelection = true
            runsTable.reloadData()
            restoringHistorySelection = false
            restoreHistorySelection(); updateHistorySelection()
            if historyMetricSort.indexOfSelectedItem > 0 { applyHistoryMetricOrdering() }
            else if !runsTable.sortDescriptors.isEmpty { tableView(runsTable, sortDescriptorsDidChange: []) }
        } catch {
            filteredRuns = []; historyCursor = nil
            restoringHistorySelection = true; runsTable.reloadData(); restoringHistorySelection = false
            updateHistorySelection()
            historyCount.stringValue = "이력 조회 실패 · 다시 검색하세요."
            if case LedgerReadError.invalidFilter(let reason) = error { historyCount.stringValue = reason }
            historyMore.isEnabled = false
        }
        updateHistoryFilterPresentation()
    }

    private func resetHistoryTableScroll() {
        guard let historyTableScroll else { return }
        historyTableScroll.contentView.scroll(to: .zero)
        if runsTable.numberOfRows > 0 { runsTable.scrollRowToVisible(0) }
        historyTableScroll.reflectScrolledClipView(historyTableScroll.contentView)
    }

    @objc private func runTableDoubleClicked() {
        let clickedRow = runsTable.clickedRow
        guard filteredRuns.indices.contains(clickedRow) else { return }
        openHistoryRun(filteredRuns[clickedRow])
    }

    @objc private func openHistorySelection() {
        guard historySelection.count == 1, let run = historySelection.values.first else { return }
        openHistoryRun(run)
    }

    private func openHistoryRun(_ run: RealRun) {
        lastOpenedHistoryRunId = run.id
        persistHistoryState()
        showingRunDetails = true
        loadRunDetails(run, requiredPolicyId: historyPolicy.selectedItem?.representedObject as? String,
                       requiredRevisionId: historyRevision.selectedItem?.representedObject as? String,
                       requiredMarket: currentHistoryFilter().market)
        selectSection(.backtestAnalysis)
    }

    // MARK: - Section 4: Real-time Paper Trading Telemetry

    private func buildPaperTradingView() -> NSView {
        if let existing = paperWorkspaceView {
            updateJobsQueuePresentation()
            jobsTable.reloadData()
            return existing
        }

        let queueContainer = NSView()
        queueContainer.setAccessibilityIdentifier("DashboardPaperJobsQueue")
        let queueHeader = NSTextField(labelWithString: "백그라운드 작업")
        queueHeader.font = TerminalTheme.sectionHeading
        queueHeader.setAccessibilityLabel("백그라운드 작업 상태")
        jobsQueueHeader = queueHeader

        setupJobsTable()
        jobsListScope.target = self
        jobsListScope.action = #selector(jobsListScopeChanged)
        jobsListScope.selectedSegment = 0
        jobsListScope.setAccessibilityLabel("백그라운드 작업 범위")
        let scroll = NSScrollView()
        scroll.documentView = jobsTable
        scroll.hasVerticalScroller = true
        scroll.hasHorizontalScroller = true
        scroll.autohidesScrollers = true
        scroll.drawsBackground = false
        scroll.translatesAutoresizingMaskIntoConstraints = false
        scroll.heightAnchor.constraint(equalToConstant: min(360, max(160, TerminalTheme.tableRowHeight * 5))).isActive = true

        let stack = NSStackView(views: [queueHeader, jobsListScope, scroll])
        stack.orientation = .vertical
        stack.alignment = .leading
        stack.spacing = 8
        stack.translatesAutoresizingMaskIntoConstraints = false
        queueContainer.addSubview(stack)

        NSLayoutConstraint.activate([
            stack.leadingAnchor.constraint(equalTo: queueContainer.leadingAnchor),
            stack.trailingAnchor.constraint(equalTo: queueContainer.trailingAnchor),
            stack.topAnchor.constraint(equalTo: queueContainer.topAnchor),
            stack.bottomAnchor.constraint(equalTo: queueContainer.bottomAnchor),
            queueHeader.widthAnchor.constraint(equalTo: stack.widthAnchor),
            jobsListScope.widthAnchor.constraint(lessThanOrEqualTo: stack.widthAnchor),
            scroll.widthAnchor.constraint(equalTo: stack.widthAnchor)
        ])
        updateJobsQueuePresentation()

        let workspace = PaperTradingWorkspaceView(
            realtimeFeedService: realtimeFeedService,
            jobsQueueView: queueContainer,
            onSummaryChanged: { [weak self] summary in
                guard let self else { return }
                self.paperSummaryText = summary
                self.updateGlobalContextText()
            }
        )
        paperWorkspaceView = workspace
        return workspace
    }

    private func buildAutomationWorkspace() -> NSView {
        if let existing = automationWorkspaceView {
            existing.updatePolicies(allPolicies)
            return existing
        }
        let workspace = AutomationWorkspaceView(
            policies: allPolicies,
            realtimeFeedService: realtimeFeedService,
            privacyIsMasked: { [weak self] in self?.isPrivacyMasked ?? false },
            onTogglePrivacyMask: { [weak self] in self?.togglePrivacyMask() },
            onSummaryChanged: { [weak self] summary in
                guard let self else { return }
                self.automationSummaryText = summary
                self.updateGlobalContextText()
            }
        )
        automationWorkspaceView = workspace
        return workspace
    }

    private func updateGlobalContextText() {
        let clock = liveClock()
        let orderFreshness = orderScope.selectedSegment == 1 ? closedOrdersFreshness : openOrdersFreshness
        let accountReadState = balanceFreshness.freshness(clock: clock)
        let orderReadState = orderFreshness.freshness(clock: clock)
        let hasAccountRead = balanceFreshness.lastSuccess != nil
        let hasOrderRead = orderFreshness.lastSuccess != nil
        let readStatus: String
        if hasAccountRead && hasOrderRead && accountReadState == .live && orderReadState == .live {
            readStatus = "잔고·주문 조회: 연결됨"
        } else if !hasAccountRead && !hasOrderRead && (accountReadState == .offline || orderReadState == .offline) {
            readStatus = "잔고·주문 조회: 연결 안 됨"
        } else if !hasAccountRead && !hasOrderRead {
            readStatus = "잔고·주문 조회: 상태 미확인"
        } else {
            readStatus = "잔고·주문 조회: 일부 상태 확인 필요"
        }
        let automationStatus: String
        if let snapshot = controlFreshness.lastSuccess?.value {
            if controlFreshness.freshness(clock: clock) != .live {
                let lastKnown = !snapshot.live_trading_enabled ? "꺼짐"
                    : (snapshot.order_admission_blocked ? "차단" : "설정 켜짐")
                automationStatus = "실주문 자동매매 상태 미확인 · 마지막 확인 \(lastKnown) · 주문 기능 사용 불가"
            } else if !snapshot.live_trading_enabled {
                automationStatus = "실주문 자동매매: 꺼짐 — 실제 주문 전송 안 함"
            } else if snapshot.emergency_latched || snapshot.order_admission_blocked {
                automationStatus = snapshot.emergency_latched
                    ? "실주문 자동매매 설정: 켜짐 · 긴급 차단 확인"
                    : "실주문 자동매매 설정: 켜짐 · 신규 주문 차단"
            } else {
                automationStatus = "실주문 자동매매 설정: 켜짐"
            }
        } else {
            automationStatus = "실주문 자동매매 상태 미확인 · 주문 기능 사용 불가"
        }

        func removingPrefix(_ value: String, _ prefix: String) -> String {
            value.hasPrefix(prefix) ? String(value.dropFirst(prefix.count)) : value
        }

        var facts = [
            GlobalContextFact(domain: "연구", value: "과거 원장"),
            GlobalContextFact(domain: "지속 모의", value: removingPrefix(paperSummaryText, "지속 모의: "))
        ]
        if currentSection == .strategyAutomation {
            facts.append(GlobalContextFact(domain: "전략 자동화", value: removingPrefix(automationSummaryText, "전략 자동화: ")))
        }
        facts.append(GlobalContextFact(domain: "계좌 관찰", value: readStatus))
        facts.append(GlobalContextFact(domain: "실거래", value: automationStatus))
        guard facts != globalContextFacts else { return }

        let wasMounted = globalContextStrip?.superview === detailContainer
        globalContextPopover.performClose(nil)
        globalContextFacts = facts
        if globalContextStrip == nil {
            let host = NSView(frame: .zero)
            host.identifier = NSUserInterfaceItemIdentifier("DashboardGlobalContextStrip")
            host.setAccessibilityElement(true)
            host.setAccessibilityRole(.group)
            host.setAccessibilityLabel("도메인별 상태")
            globalContextStrip = host
        }
        guard let host = globalContextStrip else { return }
        globalContextContent?.removeFromSuperview()
        let content = makeGlobalContextStrip(facts)
        content.translatesAutoresizingMaskIntoConstraints = false
        host.addSubview(content)
        NSLayoutConstraint.activate([
            content.leadingAnchor.constraint(equalTo: host.leadingAnchor),
            content.trailingAnchor.constraint(equalTo: host.trailingAnchor),
            content.topAnchor.constraint(equalTo: host.topAnchor),
            content.bottomAnchor.constraint(equalTo: host.bottomAnchor)
        ])
        globalContextContent = content
        if wasMounted {
            NSAccessibility.post(element: host, notification: .layoutChanged)
        }
    }

    private func makeGlobalContextStrip(_ facts: [GlobalContextFact]) -> NSView {
        let root = NSView(frame: .zero)

        func row(_ items: [NSView]) -> NSStackView {
            let spacer = NSView()
            spacer.setContentHuggingPriority(.init(1), for: .horizontal)
            spacer.setContentCompressionResistancePriority(.init(1), for: .horizontal)
            let stack = NSStackView(views: items + [spacer])
            stack.orientation = .horizontal
            stack.alignment = .centerY
            stack.distribution = .fill
            stack.spacing = 8
            return stack
        }

        let rows = NSStackView()
        rows.orientation = .vertical
        rows.alignment = .leading
        rows.spacing = 6
        rows.translatesAutoresizingMaskIntoConstraints = false

        let liveFact = facts.last.map(makeGlobalContextFactView)
        if TerminalTheme.textScale >= 1.5 {
            let leadingFacts = Array(facts.prefix(2)).map(makeGlobalContextFactView)
            rows.addArrangedSubview(row(leadingFacts))
            let middleFacts = Array(facts.dropFirst(min(2, facts.count)).dropLast())
            var trailingItems: [NSView] = []
            if !middleFacts.isEmpty {
                trailingItems.append(makeGlobalContextOverflowButton(omittedCount: middleFacts.count))
            }
            if let liveFact, facts.count > leadingFacts.count {
                trailingItems.append(liveFact)
            }
            if !trailingItems.isEmpty { rows.addArrangedSubview(row(trailingItems)) }
        } else if facts.count > 4, let liveFact {
            let leadingFacts = Array(facts.prefix(3)).map(makeGlobalContextFactView)
            let omittedCount = facts.count - leadingFacts.count - 1
            rows.addArrangedSubview(row(leadingFacts + [makeGlobalContextOverflowButton(omittedCount: omittedCount), liveFact]))
        } else {
            rows.addArrangedSubview(row(facts.map(makeGlobalContextFactView)))
        }

        root.addSubview(rows)
        NSLayoutConstraint.activate([
            rows.leadingAnchor.constraint(equalTo: root.leadingAnchor),
            rows.trailingAnchor.constraint(equalTo: root.trailingAnchor),
            rows.topAnchor.constraint(equalTo: root.topAnchor),
            root.bottomAnchor.constraint(equalTo: rows.bottomAnchor)
        ])
        for row in rows.arrangedSubviews {
            row.widthAnchor.constraint(equalTo: root.widthAnchor).isActive = true
        }
        return root
    }

    /// Unknown is amber, confirmed-off is neutral, enabled real orders and emergency stops are red.
    private static func globalContextTone(_ value: String) -> TerminalTone {
        if value.contains("긴급 차단") { return .critical }
        if value.contains("미확인") || value.contains("확인 필요") || value.contains("연결 안 됨")
            || value.contains("실패") || value.contains("신규 주문 차단") { return .warning }
        if value.contains("자동매매 설정: 켜짐") { return .critical }
        if value.contains("연결됨") || value.contains("실행 중") { return .positive }
        if value.contains("과거 원장") { return .accent }
        return .neutral
    }

    private func makeGlobalContextFactView(_ fact: GlobalContextFact) -> NSView {
        TerminalStatusPill(domain: fact.domain, value: fact.value, tone: Self.globalContextTone(fact.value))
    }

    private func makeGlobalContextOverflowButton(omittedCount: Int) -> NSButton {
        let button = NSButton(title: "+\(omittedCount)", target: self, action: #selector(showGlobalContextPopover(_:)))
        button.bezelStyle = .recessed
        button.controlSize = .small
        button.font = TerminalTheme.systemFont(ofSize: 11, weight: .semibold)
        button.toolTip = "모든 도메인 상태 보기"
        button.setAccessibilityLabel("모든 도메인 상태 보기")
        button.setAccessibilityValue("추가 상태 \(omittedCount)개")
        return button
    }

    /// Top of the usable content area: below the unified toolbar when content runs under the title bar.
    private var contentTopAnchor: NSLayoutYAxisAnchor {
        (window?.contentLayoutGuide as? NSLayoutGuide)?.topAnchor ?? detailContainer.topAnchor
    }

    private func mountGlobalContextStrip() {
        guard let strip = globalContextStrip else { return }
        strip.translatesAutoresizingMaskIntoConstraints = false
        detailContainer.addSubview(strip)
        NSLayoutConstraint.activate([
            strip.leadingAnchor.constraint(equalTo: detailContainer.leadingAnchor, constant: 20),
            strip.trailingAnchor.constraint(equalTo: detailContainer.trailingAnchor, constant: -20),
            strip.topAnchor.constraint(equalTo: contentTopAnchor, constant: 10)
        ])
    }

    @objc private func showGlobalContextPopover(_ sender: NSButton) {
        let rowHeight: CGFloat = TerminalTheme.textScale >= 1.5 ? 84 : 52
        let content = NSView(frame: NSRect(x: 0, y: 0, width: 420, height: max(120, CGFloat(globalContextFacts.count) * rowHeight + 24)))
        let stack = NSStackView()
        stack.orientation = .vertical
        stack.alignment = .leading
        stack.spacing = 10
        stack.translatesAutoresizingMaskIntoConstraints = false
        for fact in globalContextFacts {
            let item = makeGlobalContextFactView(fact)
            item.widthAnchor.constraint(equalTo: stack.widthAnchor).isActive = true
            stack.addArrangedSubview(item)
        }
        content.addSubview(stack)
        NSLayoutConstraint.activate([
            stack.leadingAnchor.constraint(equalTo: content.leadingAnchor, constant: 12),
            stack.trailingAnchor.constraint(equalTo: content.trailingAnchor, constant: -12),
            stack.topAnchor.constraint(equalTo: content.topAnchor, constant: 12),
            stack.bottomAnchor.constraint(equalTo: content.bottomAnchor, constant: -12)
        ])
        let controller = NSViewController()
        controller.view = content
        globalContextPopover.contentViewController = controller
        globalContextPopover.behavior = .transient
        globalContextPopover.show(relativeTo: sender.bounds, of: sender, preferredEdge: .maxY)
    }

    @objc private func toggleRealtimeStatusDisclosure(_ sender: NSButton) {
        realtimeStatusCollapsed = sender.state != .on
        sender.title = realtimeStatusCollapsed ? "실시간 상태 펼치기" : "실시간 상태 접기"
        sender.setAccessibilityValue(realtimeStatusCollapsed ? "접힘" : "펼쳐짐")
        let showDetails = currentSection == .backtestAnalysis && !realtimeStatusCollapsed
        realtimeStatusView.isHidden = !showDetails
        realtimeStatusHeightConstraint?.isActive = false
        realtimeStatusHeightConstraint = showDetails
            ? realtimeStatusView.heightAnchor.constraint(greaterThanOrEqualToConstant: max(132, 132 * TerminalTheme.textScale))
            : realtimeStatusView.heightAnchor.constraint(equalToConstant: 0)
        realtimeStatusHeightConstraint?.isActive = true
        detailContainer.layoutSubtreeIfNeeded()
    }

    private func scheduleTooltip(_ schedule: RealCollectionSchedule) -> String {
        var details = [
            "스케줄 ID \(schedule.id)",
            "상태 원문 \(schedule.status)",
            "등록 시각 KST \(displayKST(schedule.createdAt))",
            "다음 동작 KST \(displayKST(schedule.nextActionAt))"
        ]
        if let datasetId = schedule.lastSuccessDatasetId {
            details.append("최근 성공 데이터셋 ID \(datasetId)")
        }
        if let boundary = schedule.lastSuccessBoundary {
            details.append("최근 성공 경계 KST \(displayKST(boundary))")
        }
        if let failureCode = schedule.failureCode {
            details.append("최근 실패 코드 \(failureCode)")
        }
        return details.joined(separator: "\n")
    }

    private func setupJobsTable() {
        jobsTable.removeAllColumns()
        jobsTable.identifier = NSUserInterfaceItemIdentifier("DashboardPaperJobsTable")
        jobsTable.setAccessibilityLabel("백그라운드 작업 및 일시중지 수집 목록")
        let cols = [
            ("JobId", "작업·스케줄 ID", 260),
            ("ReqId", "요청 ID", 200),
            ("Kind", "종류", 130),
            ("JobStatus", "상태", 115),
            ("JobDate", "등록 시각 (KST)", 180)
        ]
        for (id, title, w) in cols {
            let tc = NSTableColumn(identifier: NSUserInterfaceItemIdentifier(id))
            tc.title = title
            tc.width = CGFloat(w) * TerminalTheme.textScale
            jobsTable.addTableColumn(tc)
        }
        jobsTable.dataSource = self
        jobsTable.delegate = self
        TerminalTheme.prepareTable(jobsTable)
    }

    @objc private func jobsListScopeChanged() {
        updateJobsQueuePresentation()
        jobsTable.reloadData()
    }

    private func updateJobsQueuePresentation() {
        jobsListScope.setLabel("최근 30건 · \(recentJobs.count)", forSegment: 0)
        jobsListScope.setLabel("대기·실행 · \(activeJobs.count)", forSegment: 1)
        jobsListScope.setLabel("일시중지 · \(pausedSchedules.count)", forSegment: 2)
        let scopeTitle: String
        switch jobsListScope.selectedSegment {
        case 1: scopeTitle = "대기 및 실행 작업"
        case 2: scopeTitle = "일시중지 수집 스케줄"
        default: scopeTitle = "최근 작업"
        }
        jobsQueueHeader?.stringValue = "백그라운드 작업 · \(scopeTitle) · 조회 전용"
        jobsQueueHeader?.toolTip = "최근 작업 \(recentJobs.count)건 · 대기/실행 작업 \(activeJobs.count)건 · 일시중지 수집 스케줄 \(pausedSchedules.count)개"
        jobsListScope.setAccessibilityValue("\(scopeTitle) · 최근 \(recentJobs.count)건 · 대기/실행 \(activeJobs.count)건 · 일시중지 수집 \(pausedSchedules.count)개")
    }

    // MARK: - Section 5: Real-time Live Trading & Balances

    private func buildLiveTradingView() -> NSView {
        let container = NSView()
        let titleLabel = NSTextField(labelWithString: "실계좌 및 주문 조회")
        titleLabel.font = TerminalTheme.screenTitle

        let safetyBox = NSBox()
        safetyBox.boxType = .custom
        safetyBox.cornerRadius = 8
        safetyBox.borderWidth = 1.5
        safetyBox.borderColor = NSColor.systemRed.withAlphaComponent(0.6)
        safetyBox.fillColor = TerminalTheme.panel

        let reviewQualifier = CommandLine.arguments.contains("--gui-review")
            ? "\n검증 세션 응답은 실제 계좌 관측 증거가 아닙니다."
            : ""
        let safetyText = NSTextField(wrappingLabelWithString: "조회 전용 모니터\n잔고·주문 조회 상태와 실주문 자동매매 허용은 서로 다릅니다. 이 화면은 조회만 하며 주문 제출·취소를 하지 않습니다.\(reviewQualifier)")
        safetyText.font = TerminalTheme.systemFont(ofSize: 12, weight: .semibold)
        safetyText.textColor = TerminalTheme.secondaryText

        safetyBox.addSubview(safetyText)
        safetyText.translatesAutoresizingMaskIntoConstraints = false
        NSLayoutConstraint.activate([
            safetyText.leadingAnchor.constraint(equalTo: safetyBox.leadingAnchor, constant: 14),
            safetyText.trailingAnchor.constraint(equalTo: safetyBox.trailingAnchor, constant: -14),
            safetyText.topAnchor.constraint(equalTo: safetyBox.topAnchor, constant: 12),
            safetyText.bottomAnchor.constraint(equalTo: safetyBox.bottomAnchor, constant: -12)
        ])

        for label in [liveExchangeLabel, liveTradeLockLabel, liveStateLabel] {
            label.font = TerminalTheme.systemFont(ofSize: 12, weight: .semibold)
            label.textColor = TerminalTheme.secondaryText
        }
        liveExchangeLabel.setAccessibilityLabel("잔고·주문 조회 상태")
        liveTradeLockLabel.setAccessibilityLabel("실주문 자동매매 상태")
        liveStateLabel.setAccessibilityLabel("서버 실행 제어 상태")
        let stateRow = NSStackView(views: [liveExchangeLabel, liveTradeLockLabel, liveStateLabel])
        stateRow.orientation = TerminalTheme.textScale >= 1.5 ? .vertical : .horizontal
        stateRow.alignment = TerminalTheme.textScale >= 1.5 ? .leading : .centerY
        stateRow.distribution = TerminalTheme.textScale >= 1.5 ? .fill : .fillEqually
        stateRow.spacing = 12

        liveRefreshButton.target = self
        liveRefreshButton.action = #selector(refreshLivePressed)
        liveRefreshButton.bezelStyle = .rounded
        automaticLiveReadButton.target = self
        automaticLiveReadButton.action = #selector(automaticLiveReadChanged)
        let settingsButton = NSButton(title: "설정에서 거래 제어 확인", target: self, action: #selector(openTradingSettings))
        settingsButton.bezelStyle = .rounded
        let actionRow = NSStackView(views: [liveRefreshButton, automaticLiveReadButton, settingsButton])
        actionRow.orientation = .horizontal
        actionRow.alignment = .centerY
        actionRow.spacing = 8

        // Upbit Balance Card
        let balanceBox = NSBox()
        balanceBox.boxType = .custom
        balanceBox.cornerRadius = 8
        balanceBox.borderWidth = 1
        balanceBox.borderColor = TerminalTheme.border
        balanceBox.fillColor = TerminalTheme.panel

        let balanceHeading = NSTextField(labelWithString: "Upbit 실계좌 · 읽기 전용")
        balanceHeading.font = TerminalTheme.sectionHeading

        upbitBalanceLabel.font = TerminalTheme.monospacedFont(ofSize: 13, weight: .medium)
        upbitBalanceLabel.textColor = .labelColor
        balanceReadStatus.font = TerminalTheme.systemFont(ofSize: 11)
        balanceReadStatus.textColor = TerminalTheme.secondaryText
        balanceReadStatus.setAccessibilityLabel("잔고 조회 출처와 신선도")

        privacyMaskButton.title = isPrivacyMasked ? "공유 보호 해제 · 금액 표시" : "화면 공유 보호 · 금액 숨기기"
        privacyMaskButton.setAccessibilityLabel(privacyMaskButton.title)
        privacyMaskButton.target = self
        privacyMaskButton.action = #selector(togglePrivacyMask)
        privacyMaskButton.bezelStyle = .rounded

        let balanceActionRow = NSStackView(views: [privacyMaskButton])
        balanceActionRow.orientation = .horizontal
        balanceActionRow.spacing = 8

        let balanceStack = NSStackView(views: [balanceHeading, upbitBalanceLabel, balanceReadStatus, balanceActionRow])
        balanceStack.orientation = .vertical
        balanceStack.alignment = .leading
        balanceStack.spacing = 8
        balanceStack.translatesAutoresizingMaskIntoConstraints = false
        balanceBox.addSubview(balanceStack)

        NSLayoutConstraint.activate([
            balanceStack.leadingAnchor.constraint(equalTo: balanceBox.leadingAnchor, constant: 14),
            balanceStack.trailingAnchor.constraint(equalTo: balanceBox.trailingAnchor, constant: -14),
            balanceStack.topAnchor.constraint(equalTo: balanceBox.topAnchor, constant: 12),
            balanceStack.bottomAnchor.constraint(equalTo: balanceBox.bottomAnchor, constant: -12)
        ])

        let ordersBox = NSBox()
        ordersBox.boxType = .custom
        ordersBox.cornerRadius = 8
        ordersBox.borderWidth = 1
        ordersBox.borderColor = TerminalTheme.border
        ordersBox.fillColor = TerminalTheme.panel

        let ordersHeading = NSTextField(labelWithString: "Upbit 주문 내역 · 읽기 전용")
        ordersHeading.font = TerminalTheme.sectionHeading

        setupOpenOrdersTable()
        let ordersScroll = NSScrollView()
        ordersScroll.documentView = openOrdersTable
        ordersScroll.hasVerticalScroller = true
        ordersScroll.hasHorizontalScroller = true
        ordersScroll.translatesAutoresizingMaskIntoConstraints = false
        ordersScroll.heightAnchor.constraint(equalToConstant: max(132, 132 * TerminalTheme.textScale)).isActive = true

        orderScope.selectedSegment = max(orderScope.selectedSegment, 0)
        orderScope.target = self
        orderScope.action = #selector(orderScopeChanged)
        orderReadStatus.font = TerminalTheme.systemFont(ofSize: 12)
        orderReadStatus.textColor = TerminalTheme.secondaryText
        orderReadStatus.setAccessibilityLabel("주문 조회 범위와 신선도")
        nextOrderPageButton.target = self
        nextOrderPageButton.action = #selector(nextOrderPage)
        nextOrderPageButton.isEnabled = false
        copyOrderIdButton.target = self
        copyOrderIdButton.action = #selector(copySelectedOrderId)
        copyOrderIdButton.isEnabled = false
        orderDetailLabel.font = TerminalTheme.monospacedFont(ofSize: 11, weight: .regular)
        orderDetailLabel.textColor = TerminalTheme.secondaryText
        let orderButtons = NSStackView(views: [nextOrderPageButton, copyOrderIdButton])
        orderButtons.orientation = .horizontal
        orderButtons.spacing = 8
        let ordersStack = NSStackView(views: [ordersHeading, orderScope, orderReadStatus, orderButtons, ordersScroll, orderDetailLabel])
        ordersStack.orientation = .vertical
        ordersStack.alignment = .leading
        ordersStack.spacing = 8
        ordersStack.translatesAutoresizingMaskIntoConstraints = false
        ordersBox.addSubview(ordersStack)

        NSLayoutConstraint.activate([
            ordersStack.leadingAnchor.constraint(equalTo: ordersBox.leadingAnchor, constant: 14),
            ordersStack.trailingAnchor.constraint(equalTo: ordersBox.trailingAnchor, constant: -14),
            ordersStack.topAnchor.constraint(equalTo: ordersBox.topAnchor, constant: 12),
            ordersStack.bottomAnchor.constraint(equalTo: ordersBox.bottomAnchor, constant: -12),
            ordersScroll.widthAnchor.constraint(equalTo: ordersStack.widthAnchor)
        ])

        let auditorBox = NSBox()
        auditorBox.boxType = .custom
        auditorBox.cornerRadius = 8
        auditorBox.borderWidth = 1
        auditorBox.borderColor = TerminalTheme.border
        auditorBox.fillColor = TerminalTheme.panel

        let auditorTitle = NSTextField(labelWithString: "계좌·주문 데이터 근거")
        auditorTitle.font = TerminalTheme.sectionHeading

        liveAuditLabel.font = TerminalTheme.monospacedFont(ofSize: 11, weight: .regular)
        liveAuditLabel.textColor = TerminalTheme.secondaryText

        let auditorStack = NSStackView(views: [auditorTitle, liveAuditLabel])
        auditorStack.orientation = .vertical
        auditorStack.alignment = .leading
        auditorStack.spacing = 6
        auditorStack.translatesAutoresizingMaskIntoConstraints = false
        auditorBox.addSubview(auditorStack)

        NSLayoutConstraint.activate([
            auditorStack.leadingAnchor.constraint(equalTo: auditorBox.leadingAnchor, constant: 14),
            auditorStack.trailingAnchor.constraint(equalTo: auditorBox.trailingAnchor, constant: -14),
            auditorStack.topAnchor.constraint(equalTo: auditorBox.topAnchor, constant: 12),
            auditorStack.bottomAnchor.constraint(equalTo: auditorBox.bottomAnchor, constant: -12)
        ])

        let accountCards = NSStackView(views: [balanceBox, ordersBox])
        accountCards.orientation = TerminalTheme.textScale >= 1.5 ? .vertical : .horizontal
        accountCards.alignment = TerminalTheme.textScale >= 1.5 ? .leading : .top
        accountCards.distribution = .fill
        accountCards.spacing = 12

        let stack = NSStackView(views: [titleLabel, safetyBox, stateRow, actionRow, accountCards, auditorBox])
        stack.orientation = .vertical
        stack.alignment = .leading
        stack.spacing = 12
        stack.translatesAutoresizingMaskIntoConstraints = false
        let document = TerminalFlippedDocumentView()
        document.translatesAutoresizingMaskIntoConstraints = false
        document.addSubview(stack)
        let scroll = NSScrollView()
        scroll.drawsBackground = false
        scroll.hasVerticalScroller = true
        scroll.documentView = document
        scroll.translatesAutoresizingMaskIntoConstraints = false
        container.addSubview(scroll)

        NSLayoutConstraint.activate([
            scroll.leadingAnchor.constraint(equalTo: container.leadingAnchor),
            scroll.trailingAnchor.constraint(equalTo: container.trailingAnchor),
            scroll.topAnchor.constraint(equalTo: container.topAnchor),
            scroll.bottomAnchor.constraint(equalTo: container.bottomAnchor),
            document.widthAnchor.constraint(equalTo: scroll.contentView.widthAnchor),
            stack.leadingAnchor.constraint(equalTo: document.leadingAnchor, constant: 20),
            stack.trailingAnchor.constraint(equalTo: document.trailingAnchor, constant: -20),
            stack.topAnchor.constraint(equalTo: document.topAnchor, constant: 18),
            stack.bottomAnchor.constraint(equalTo: document.bottomAnchor, constant: -18),
            titleLabel.widthAnchor.constraint(equalTo: stack.widthAnchor),
            safetyBox.widthAnchor.constraint(equalTo: stack.widthAnchor),
            stateRow.widthAnchor.constraint(equalTo: stack.widthAnchor),
            accountCards.widthAnchor.constraint(equalTo: stack.widthAnchor),
            auditorBox.widthAnchor.constraint(equalTo: stack.widthAnchor)
        ])
        if TerminalTheme.textScale >= 1.5 {
            NSLayoutConstraint.activate([
                balanceBox.widthAnchor.constraint(equalTo: accountCards.widthAnchor),
                ordersBox.widthAnchor.constraint(equalTo: accountCards.widthAnchor)
            ])
        } else {
            NSLayoutConstraint.activate([
                balanceBox.widthAnchor.constraint(equalTo: ordersBox.widthAnchor, multiplier: 0.6)
            ])
        }

        renderLiveMonitor(reloadOrders: true)
        return container
    }

    private func setupOpenOrdersTable() {
        openOrdersTable.removeAllColumns()
        let cols = [
            ("OrderId", "주문 ID", 190),
            ("OrderMarket", "마켓", 100),
            ("OrderSide", "구분", 90),
            ("OrderState", "상태", 100),
            ("OrderFilled", "체결 / 잔여 수량", 160),
            ("OrderVolume", "주문 수량", 120),
            ("OrderPrice", "가격 / 예산", 140),
            ("OrderFee", "지급 수수료", 120),
            ("OrderDate", "접수 시각 (KST)", 160)
        ]
        for (id, title, w) in cols {
            let tc = NSTableColumn(identifier: NSUserInterfaceItemIdentifier(id))
            tc.title = title
            tc.width = CGFloat(w)
            openOrdersTable.addTableColumn(tc)
        }
        openOrdersTable.dataSource = self
        openOrdersTable.delegate = self
        TerminalTheme.prepareTable(openOrdersTable)
    }

    @objc private func togglePrivacyMask() {
        isPrivacyMasked.toggle()
        privacyMaskButton.title = isPrivacyMasked ? "공유 보호 해제 · 금액 표시" : "화면 공유 보호 · 금액 숨기기"
        privacyMaskButton.setAccessibilityLabel(privacyMaskButton.title)
        renderBalances()
        automationWorkspaceView?.refreshPrivacyPresentation()
    }

    @objc private func refreshLivePressed() {
        guard !hasPendingLiveRead else {
            liveAuditLabel.stringValue = "조회가 이미 진행 중입니다. 중복 요청은 시작하지 않습니다."
            return
        }
        guard liveReadBudgetAvailable else {
            renderLiveMonitor()
            return
        }
        refreshLiveCycle()
    }

    @objc private func automaticLiveReadChanged() {
        nextAutomaticLiveRead = automaticLiveReadButton.state == .on
            ? liveMonotonicNow() + 30
            : nil
        renderLiveMonitor()
    }

    @objc private func orderScopeChanged() {
        currentOrderPage = 1
        selectedOrderId = nil
        cancelLiveRequests()
        renderOrders(reloadTable: true)
        if isLiveMonitorVisible { refreshLiveCycle() }
    }

    @objc private func nextOrderPage() {
        guard orderScope.selectedSegment == 0,
              let page = openOrdersFreshness.lastSuccess?.value.next_page else { return }
        currentOrderPage = page
        selectedOrderId = nil
        cancelLiveRequests()
        renderOrders(reloadTable: true)
        if isLiveMonitorVisible { refreshLiveCycle() }
    }

    private var hasPendingLiveRead: Bool {
        balancePending || openOrdersPending || closedOrdersPending || controlPending
    }

    private var liveReadBudgetAvailable: Bool {
        guard let liveRetryAllowedAt else { return true }
        return liveMonotonicNow() >= liveRetryAllowedAt
    }

    private var isLiveMonitorVisible: Bool {
        guard currentSection == .liveTradingMonitor, let window,
              window.isVisible, !window.isMiniaturized else { return false }
        return window.occlusionState.contains(.visible)
    }

    private var isAutomationWorkspaceVisible: Bool {
        guard currentSection == .strategyAutomation, let window,
              window.isVisible, !window.isMiniaturized else { return false }
        return window.occlusionState.contains(.visible)
    }

    private func updateAutomationWorkspaceVisibility() {
        automationWorkspaceView?.setWorkspaceVisible(isAutomationWorkspaceVisible)
    }

    private var isStrategyOverviewVisible: Bool {
        currentSection == .backtestAnalysis && !showingRunDetails
            && window?.isVisible == true
            && window?.isMiniaturized == false
            && window?.occlusionState.contains(.visible) == true
    }

    private func updateStrategyOverviewVisibility() {
        strategyOverview?.setWorkspaceVisible(isStrategyOverviewVisible)
    }

    private func updateRealtimeFeedVisibility() {
        let sectionShowsRealtimeStatus = currentSection == .backtestAnalysis
            || currentSection == .paperTradingTelemetry
            || currentSection == .strategyAutomation
            || currentSection == .liveTradingMonitor
        let windowIsVisible = window.map {
            $0.isVisible && !$0.isMiniaturized && $0.occlusionState.contains(.visible)
        } ?? false
        realtimeFeedService.setVisible(sectionShowsRealtimeStatus && windowIsVisible)
    }

    private var visibleOrderSnapshot: UpbitOrdersSnapshot? {
        if orderScope.selectedSegment == 1 {
            return closedOrdersFreshness.lastSuccess?.value
        }
        guard let snapshot = openOrdersFreshness.lastSuccess?.value,
              snapshot.page == currentOrderPage else { return nil }
        return snapshot
    }

    private var visibleOrders: [UpbitLiveOrder] {
        visibleOrderSnapshot?.records ?? []
    }

    private func liveClock(receivedAt: Date? = nil) -> ReadFreshnessClock {
        ReadFreshnessClock(wallNow: receivedAt ?? liveWallNow(), monotonicNow: liveMonotonicNow())
    }

    private func resumeLiveMonitoringIfVisible() {
        guard isLiveMonitorVisible, liveTickTimer == nil else { return }
        let timer = Timer(timeInterval: 1, repeats: true) { [weak self] _ in
            self?.liveTick()
        }
        liveTickTimer = timer
        RunLoop.main.add(timer, forMode: .common)
        refreshLiveCycle()
    }

    private func suspendLiveMonitoring() {
        liveTickTimer?.invalidate()
        liveTickTimer = nil
        cancelLiveRequests()
    }

    private func cancelLiveRequests() {
        liveContextGeneration &+= 1
        privateFeedInvalidation.clearPending()
        let clock = liveClock()
        let reason = ReadFreshnessFailure.other("화면 또는 조회 범위 변경으로 요청 취소")
        if balancePending { balanceFreshness.fail(reason, clock: clock) }
        if openOrdersPending { openOrdersFreshness.fail(reason, clock: clock) }
        if closedOrdersPending { closedOrdersFreshness.fail(reason, clock: clock) }
        if controlPending { controlFreshness.fail(reason, clock: clock) }
        let requests = [balanceRequest, openOrdersRequest, closedOrdersRequest, controlRequest]
        balanceRequest = nil
        openOrdersRequest = nil
        closedOrdersRequest = nil
        controlRequest = nil
        balancePending = false
        openOrdersPending = false
        closedOrdersPending = false
        controlPending = false
        requests.compactMap { $0 }.forEach { $0.cancel() }
    }

    private func liveTick() {
        guard isLiveMonitorVisible else {
            suspendLiveMonitoring()
            return
        }
        let clock = liveClock()
        balanceFreshness.refresh(clock: clock)
        openOrdersFreshness.refresh(clock: clock)
        closedOrdersFreshness.refresh(clock: clock)
        controlFreshness.refresh(clock: clock)
        renderBalanceFreshness()
        renderOrders()
        renderControlState()
        renderLiveStatusSummary()
        if privateFeedInvalidation.takeIfReady(
            isVisible: isLiveMonitorVisible,
            readInFlight: hasPendingLiveRead,
            writeInFlight: false,
            budgetAvailable: liveReadBudgetAvailable
        ) {
            refreshLiveCycle(includeControlRead: false)
            return
        }
        guard automaticLiveReadButton.state == .on,
              let due = nextAutomaticLiveRead,
              liveMonotonicNow() >= due,
              !hasPendingLiveRead,
              liveReadBudgetAvailable else { return }
        refreshLiveCycle()
    }

    private func refreshLiveCycle(includeControlRead: Bool = true) {
        guard isLiveMonitorVisible, !hasPendingLiveRead else { return }
        guard !CommandLine.arguments.contains("--gui-review") || CommandLine.arguments.contains("--gui-review-read-only") else {
            liveAuditLabel.stringValue = "GUI 검토 모드 · 실계좌 네트워크 조회를 수행하지 않습니다."
            return
        }
        guard liveReadBudgetAvailable else {
            renderLiveMonitor()
            return
        }
        privateFeedInvalidation.clearPending()
        let generation = liveContextGeneration
        startBalanceRead(generation: generation)
        startOrderRead(generation: generation)
        if includeControlRead { startControlRead(generation: generation) }
        if automaticLiveReadButton.state == .on {
            nextAutomaticLiveRead = liveMonotonicNow() + 30
        }
        renderLiveMonitor()
    }

    private func startBalanceRead(generation: UInt64) {
        guard !balancePending else { return }
        balancePending = true
        balanceTicket &+= 1
        let ticket = balanceTicket
        balanceFreshness.markStart(clock: liveClock())
        let request = liveService.fetchAccountSnapshot { [weak self] result in
            guard let self, generation == self.liveContextGeneration,
                  ticket == self.balanceTicket else { return }
            self.balancePending = false
            self.balanceRequest = nil
            let received = self.liveWallNow()
            let clock = self.liveClock(receivedAt: received)
            switch result {
            case .success(let snapshot):
                _ = self.balanceFreshness.acceptSnapshot(snapshot,
                    providerObservedAtUTC: snapshot.observed_at_utc,
                    localReceivedAt: received,
                    clock: clock)
            case .failure(let error):
                self.recordLiveFailure(error, into: &self.balanceFreshness, clock: clock)
            }
            self.renderBalances()
            self.renderControlState()
            self.renderLiveStatusSummary()
        }
        if balancePending, generation == liveContextGeneration, ticket == balanceTicket {
            balanceRequest = request
        }
    }

    private func startOrderRead(generation: UInt64) {
        if orderScope.selectedSegment == 1 {
            guard !closedOrdersPending else { return }
            closedOrdersPending = true
            closedOrdersTicket &+= 1
            let ticket = closedOrdersTicket
            closedOrdersFreshness.markStart(clock: liveClock())
            let request = liveService.fetchClosedOrdersSnapshot { [weak self] result in
                guard let self, generation == self.liveContextGeneration,
                      ticket == self.closedOrdersTicket else { return }
                self.closedOrdersPending = false
                self.closedOrdersRequest = nil
                self.finishOrderRead(result, closed: true)
            }
            if closedOrdersPending, generation == liveContextGeneration, ticket == closedOrdersTicket {
                closedOrdersRequest = request
            }
        } else {
            guard !openOrdersPending else { return }
            openOrdersPending = true
            openOrdersTicket &+= 1
            let ticket = openOrdersTicket
            openOrdersFreshness.markStart(clock: liveClock())
            let request = liveService.fetchOpenOrdersSnapshot(page: currentOrderPage) { [weak self] result in
                guard let self, generation == self.liveContextGeneration,
                      ticket == self.openOrdersTicket else { return }
                self.openOrdersPending = false
                self.openOrdersRequest = nil
                self.finishOrderRead(result, closed: false)
            }
            if openOrdersPending, generation == liveContextGeneration, ticket == openOrdersTicket {
                openOrdersRequest = request
            }
        }
    }

    private func finishOrderRead(_ result: Result<UpbitOrdersSnapshot, Error>, closed: Bool) {
        let received = liveWallNow()
        let clock = liveClock(receivedAt: received)
        switch result {
        case .success(let snapshot):
            if closed {
                _ = closedOrdersFreshness.acceptSnapshot(snapshot,
                    providerObservedAtUTC: snapshot.observed_at_utc,
                    localReceivedAt: received,
                    clock: clock)
            } else {
                _ = openOrdersFreshness.acceptSnapshot(snapshot,
                    providerObservedAtUTC: snapshot.observed_at_utc,
                    localReceivedAt: received,
                    clock: clock)
            }
        case .failure(let error):
            if closed { recordLiveFailure(error, into: &closedOrdersFreshness, clock: clock) }
            else { recordLiveFailure(error, into: &openOrdersFreshness, clock: clock) }
        }
        renderOrders(reloadTable: true)
        renderControlState()
        renderLiveStatusSummary()
    }

    private func startControlRead(generation: UInt64) {
        guard !controlPending else { return }
        controlPending = true
        controlTicket &+= 1
        let ticket = controlTicket
        controlFreshness.markStart(clock: liveClock())
        let request = liveService.fetchControlStatusRequest { [weak self] result in
            guard let self, generation == self.liveContextGeneration,
                  ticket == self.controlTicket else { return }
            self.controlPending = false
            self.controlRequest = nil
            let received = self.liveWallNow()
            let clock = self.liveClock(receivedAt: received)
            switch result {
            case .success(let snapshot):
                _ = self.controlFreshness.acceptSnapshot(snapshot,
                    providerObservedAtUTC: snapshot.observed_at_utc,
                    localReceivedAt: received,
                    isComplete: snapshot.exchange_read_mode == "LIVE_READ" && !snapshot.state.isEmpty,
                    clock: clock)
            case .failure(let error):
                self.recordLiveFailure(error, into: &self.controlFreshness, clock: clock)
            }
            self.renderControlState()
            self.renderLiveStatusSummary()
        }
        if controlPending, generation == liveContextGeneration, ticket == controlTicket {
            controlRequest = request
        }
    }

    private func recordLiveFailure<Value>(_ error: Error, into freshness: inout ReadFreshness<Value>,
                                          clock: ReadFreshnessClock) {
        let reason: ReadFreshnessFailure
        switch error as? NativeMCPError {
        case .unavailable?, .transport?: reason = .disconnected
        case .transportContextChanged?: reason = .other("Backend 연결 세대 변경")
        case .timeout?: reason = .timeout
        case .protocolError?: reason = .incompleteSnapshot
        case .rateLimited(let seconds)?:
            let delay = TimeInterval(max(30, seconds ?? 30))
            liveRetryAllowedAt = max(liveRetryAllowedAt ?? 0, liveMonotonicNow() + delay)
            reason = .other("429 요청 제한 · \(Int(delay))초 후 재시도")
        case .unauthorized?: reason = .other("Backend 인증 확인 불가")
        case .mockData?: reason = .other("실계좌 연결 아님 · Mock 표시 거부")
        case .cancelled?: reason = .other("조회 취소")
        case .busy?: reason = .other("동시 조회 한도")
        case .toolRejected?, .paperEventPageByteBudgetExceeded?: reason = .other("Backend 요청 거부")
        case nil: reason = .other(error.localizedDescription)
        }
        freshness.fail(reason, clock: clock)
    }

    private func renderLiveMonitor(reloadOrders: Bool = false) {
        renderBalances()
        renderOrders(reloadTable: reloadOrders)
        renderControlState()
        renderLiveStatusSummary()
    }

    private func renderLiveStatusSummary() {
        liveRefreshButton.isEnabled = !hasPendingLiveRead && liveReadBudgetAvailable
        let balanceState = freshnessStateText(balanceFreshness.freshness(clock: liveClock()))
        let orderState = freshnessStateText((orderScope.selectedSegment == 1
            ? closedOrdersFreshness : openOrdersFreshness).freshness(clock: liveClock()))
        let controlState = freshnessStateText(controlFreshness.freshness(clock: liveClock()))
        var lines = [
            "출처 · Upbit provider read / 로컬 Backend · 과거·모의 주문 미포함",
            "신선도 · 잔고 \(balanceState) · 선택 주문 \(orderState) · 제어 \(controlState)",
            freshnessDescription("제어", freshness: controlFreshness, source: "Rust trading_control")
        ]
        if let allowed = liveRetryAllowedAt, liveMonotonicNow() < allowed {
            lines.append("요청 제한 · \(Int(ceil(allowed - liveMonotonicNow())))초 후 조회 가능")
        } else if automaticLiveReadButton.state == .on, let nextAutomaticLiveRead {
            lines.append("자동 조회 · 화면이 보이는 동안만 · 다음 조회 약 \(Int(max(0, ceil(nextAutomaticLiveRead - liveMonotonicNow()))))초")
        } else {
            lines.append("자동 조회 · 꺼짐")
        }
        liveAuditLabel.stringValue = lines.joined(separator: "\n")
    }

    private func renderBalances() {
        renderBalanceFreshness()
        guard let snapshot = balanceFreshness.lastSuccess?.value else {
            upbitBalanceLabel.stringValue = "확정된 실계좌 잔고가 없습니다. 모름을 0으로 표시하지 않습니다."
            upbitBalanceLabel.toolTip = nil
            return
        }
        var rows: [String] = []
        var rawRows: [String] = []
        for account in snapshot.balances {
            guard let available = account.balanceDecimal, let locked = account.lockedDecimal else {
                rows.append("\(account.currency): 금액 해석 불가")
                continue
            }
            if account.currency != "KRW" && available == 0 && locked == 0 { continue }
            if isPrivacyMasked {
                rows.append("\(account.currency): 금액 숨김")
            } else {
                var row = "\(account.currency): 사용 가능 \(liveAmount(available, currency: account.currency)) · 잠김 \(liveAmount(locked, currency: account.currency))"
                if account.currency != "KRW", let average = account.averageBuyPriceDecimal {
                    row += " · 평균 매수가 \(liveAmount(average, currency: account.unit_currency))"
                    if account.avg_buy_price_modified { row += " (거래소 수정값)" }
                }
                rows.append(row)
                rawRows.append("\(account.currency) · available 원문 \(account.balance) · locked 원문 \(account.locked) · avg_buy_price 원문 \(account.avg_buy_price) \(account.unit_currency)")
            }
        }
        upbitBalanceLabel.stringValue = rows.isEmpty
            ? "유효한 Upbit 응답 · 표시할 잔고 0건"
            : rows.joined(separator: "\n")
        upbitBalanceLabel.toolTip = isPrivacyMasked ? nil : rawRows.joined(separator: "\n")
    }

    private func renderBalanceFreshness() {
        balanceReadStatus.stringValue = freshnessDescription("잔고", freshness: balanceFreshness,
            source: "Upbit LIVE_HTTP_READ")
    }

    private func renderOrders(reloadTable: Bool = false) {
        let closed = orderScope.selectedSegment == 1
        let freshness = closed ? closedOrdersFreshness : openOrdersFreshness
        let scope = closed ? "완료·취소 주문" : "미체결 주문 · 페이지 \(currentOrderPage)"
        orderReadStatus.stringValue = freshnessDescription(scope, freshness: freshness,
            source: "Upbit OBSERVED")
        if let snapshot = visibleOrderSnapshot {
            let count = snapshot.returned_count
            let qualifier = freshness.failureReason == nil
                && freshness.freshness(clock: liveClock()) == .live
                ? "유효 응답"
                : "마지막 확정 응답"
            orderReadStatus.stringValue += " · \(qualifier) · 주문 \(count)건"
            if count == 0 { orderReadStatus.stringValue += " · 현재 범위 주문 0건" }
            if let reason = snapshot.truncated_reason {
                orderReadStatus.stringValue += " · 제한 \(reason) · 전체 이력 아님"
            }
        }
        if !closed, let priorPage = openOrdersFreshness.lastSuccess?.value.page,
           priorPage != currentOrderPage {
            orderReadStatus.stringValue += " · 확정된 이전 페이지 \(priorPage)는 현재 범위에 표시하지 않음"
        }
        if reloadTable {
            let priorId = selectedOrderId
            openOrdersTable.reloadData()
            if let priorId, let row = visibleOrders.firstIndex(where: { $0.uuid == priorId }) {
                openOrdersTable.selectRowIndexes(IndexSet(integer: row), byExtendingSelection: false)
            } else {
                selectedOrderId = nil
                openOrdersTable.deselectAll(nil)
            }
        }
        nextOrderPageButton.isEnabled = !closed && !openOrdersPending
            && visibleOrderSnapshot?.next_page != nil
        renderSelectedOrder()
    }

    private func renderControlState() {
        let clock = liveClock()
        let balanceState = balanceFreshness.freshness(clock: clock)
        let selectedOrderFreshness = orderScope.selectedSegment == 1 ? closedOrdersFreshness : openOrdersFreshness
        let orderState = selectedOrderFreshness.freshness(clock: clock)
        let hasAccountRead = balanceFreshness.lastSuccess != nil
        let hasOrderRead = selectedOrderFreshness.lastSuccess != nil
        if !hasAccountRead && !hasOrderRead {
            let states = [balanceState, orderState]
            let offline = states.contains(.offline)
            liveExchangeLabel.stringValue = offline
                ? "잔고·주문 조회: 연결 안 됨"
                : (states.contains(.loading) ? "잔고·주문 조회: 조회 중" : "잔고·주문 조회: 상태 미확인")
            liveExchangeLabel.textColor = offline ? TerminalTheme.negative : TerminalTheme.secondaryText
        } else if hasAccountRead && hasOrderRead && balanceState == .live && orderState == .live {
            liveExchangeLabel.stringValue = "잔고·주문 조회: 연결됨"
            liveExchangeLabel.textColor = TerminalTheme.positive
        } else {
            let hasReadFailure = [balanceState, orderState].contains {
                [.stale, .offline, .unknown].contains($0)
            }
            liveExchangeLabel.stringValue = "잔고·주문 조회: 일부 상태 확인 필요"
            liveExchangeLabel.textColor = hasReadFailure ? TerminalTheme.negative : TerminalTheme.secondaryText
        }
        guard let snapshot = controlFreshness.lastSuccess?.value else {
            liveTradeLockLabel.stringValue = "실주문 자동매매 상태 미확인 · 주문 기능 사용 불가"
            liveTradeLockLabel.textColor = TerminalTheme.negative
            liveStateLabel.stringValue = "서버 제어 상태 미확인"
            liveStateLabel.textColor = TerminalTheme.secondaryText
            updateGlobalContextText()
            return
        }
        let controlState = controlFreshness.freshness(clock: clock)
        guard controlState == .live else {
            let lastKnown = !snapshot.live_trading_enabled ? "꺼짐"
                : (snapshot.order_admission_blocked ? "차단" : "설정 켜짐")
            liveTradeLockLabel.stringValue = "실주문 자동매매 상태 미확인 · 마지막 확인 \(lastKnown) · 주문 기능 사용 불가"
            liveTradeLockLabel.textColor = TerminalTheme.negative
            liveStateLabel.stringValue = "서버 상태 마지막 확인 \(TerminalPresentation.state(snapshot.state)) (\(snapshot.state)) · 순번 \(snapshot.sequence) · \(freshnessStateText(controlState))"
            liveStateLabel.textColor = TerminalTheme.negative
            updateGlobalContextText()
            return
        }
        if !snapshot.live_trading_enabled {
            let emergency = snapshot.emergency_latched ? " · 긴급 차단 확인" : ""
            liveTradeLockLabel.stringValue = "실주문 자동매매: 꺼짐 — 실제 주문 전송 안 함\(emergency)"
            liveTradeLockLabel.textColor = snapshot.emergency_latched ? TerminalTheme.negative : TerminalTheme.secondaryText
        } else if snapshot.order_admission_blocked {
            liveTradeLockLabel.stringValue = snapshot.emergency_latched
                ? "실주문 자동매매 설정: 켜짐 · 긴급 차단 확인"
                : "실주문 자동매매 설정: 켜짐 · 신규 주문 차단"
            liveTradeLockLabel.textColor = snapshot.emergency_latched ? TerminalTheme.negative : TerminalTheme.secondaryText
        } else {
            liveTradeLockLabel.stringValue = "실주문 자동매매 설정: 켜짐 · 서버 상태 확인 필요"
            liveTradeLockLabel.textColor = TerminalTheme.negative
        }
        liveStateLabel.stringValue = "서버 상태 \(TerminalPresentation.state(snapshot.state)) (\(snapshot.state)) · 순번 \(snapshot.sequence) · 최신"
        liveStateLabel.textColor = snapshot.emergency_latched ? TerminalTheme.negative : TerminalTheme.secondaryText
        updateGlobalContextText()
    }

    private func renderSelectedOrder() {
        guard let selectedOrderId,
              let order = visibleOrders.first(where: { $0.uuid == selectedOrderId }) else {
            copyOrderIdButton.isEnabled = false
            orderDetailLabel.stringValue = "선택 범위 · \(orderScope.selectedSegment == 1 ? "완료·취소" : "미체결") · Upbit OBSERVED\n주문을 선택하면 전체 주문 ID와 가격·수량·수수료 근거를 표시합니다."
            return
        }
        copyOrderIdButton.isEnabled = true
        orderDetailLabel.stringValue = [
            "선택 범위 · \(orderScope.selectedSegment == 1 ? "완료·취소" : "미체결") · Upbit OBSERVED",
            "주문 ID · \(order.uuid)",
            "원본 상태 · \(order.state) · 표시 \(liveOrderStateDescription(order))",
            "유형 · \(order.orderTypeDisplay) · \(orderPriceDescription(order))",
            "가격/예산 원문 · \(order.price ?? "미제공") \(orderQuoteCurrency(order))",
            "수량 · \(orderVolumeDescription(order))",
            "지급 수수료 · \(orderFeeDescription(order)) · 원문 \(order.paid_fee ?? "미제공") \(orderQuoteCurrency(order))",
            "접수 · \(displayKST(order.created_at)) KST"
        ].joined(separator: "\n")
    }

    private func freshnessDescription<Value>(_ title: String, freshness: ReadFreshness<Value>,
                                              source: String) -> String {
        let state = freshness.freshness(clock: liveClock())
        var parts = ["\(title) · \(freshnessStateText(state))", "출처 \(source)"]
        if let snapshot = freshness.lastSuccess {
            parts.append("observed \(displayKST(snapshot.providerObservedAt)) KST")
            parts.append("received \(displayKST(snapshot.localReceivedAt)) KST")
            if state != .live && state != .delayed { parts.append("마지막 확정값 표시") }
        } else {
            parts.append("확정값 없음")
        }
        if let failure = freshness.failureReason { parts.append(freshnessFailureText(failure)) }
        parts.append("finalized 미지원")
        return parts.joined(separator: " · ")
    }

    private func freshnessStateText(_ state: ReadFreshnessState) -> String {
        switch state {
        case .notObserved: return "UNKNOWN"
        case .loading: return "LOADING"
        case .live: return "FRESH"
        case .delayed: return "DELAYED"
        case .stale: return "STALE"
        case .offline: return "OFFLINE"
        case .unknown: return "UNKNOWN"
        }
    }

    private func freshnessFailureText(_ failure: ReadFreshnessFailure) -> String {
        switch failure {
        case .disconnected: return "연결 끊김"
        case .timeout: return "응답 시간 초과"
        case .noData: return "유효 응답 없음"
        case .missingProviderTimestamp: return "provider 시각 없음"
        case .missingLocalReceivedAt: return "수신 시각 없음"
        case .futureTimestamp: return "미래 시각 거부"
        case .regressedTimestamp: return "관측 시각 역행 거부"
        case .incompleteSnapshot: return "불완전 응답 거부"
        case .historicalContext: return "과거 데이터를 실시간으로 표시하지 않음"
        case .other(let text): return text
        }
    }

    private func liveAmount(_ amount: Decimal, currency: String) -> String {
        let magnitude = amount < 0 ? -amount : amount
        let threshold = currency == "KRW" ? Decimal(string: "0.01")! : Decimal(string: "0.00000001")!
        if magnitude > 0, magnitude < threshold {
            let boundary = currency == "KRW" ? "₩0.01" : "0.00000001 \(currency)"
            return amount < 0 ? "−<\(boundary)" : "<\(boundary)"
        }
        let formatter = NumberFormatter()
        formatter.locale = Locale(identifier: "ko_KR")
        formatter.numberStyle = .decimal
        formatter.minimumFractionDigits = currency == "KRW" ? 2 : 0
        formatter.maximumFractionDigits = currency == "KRW" ? 2 : 8
        let value = formatter.string(from: NSDecimalNumber(decimal: amount))
            ?? NSDecimalNumber(decimal: amount).stringValue
        return currency == "KRW" ? "₩\(value)" : "\(value) \(currency)"
    }

    private func orderPriceDescription(_ order: UpbitLiveOrder) -> String {
        let quote = orderQuoteCurrency(order)
        let amount = order.priceDecimal.map { liveAmount($0, currency: quote) } ?? "미제공"
        switch order.ord_type {
        case "price": return "총 주문 예산 \(amount) · 단가 아님"
        case "market": return "시장가 매도 · 주문 단가 미제공"
        default: return "주문 가격 \(amount)"
        }
    }

    private func orderVolumeDescription(_ order: UpbitLiveOrder) -> String {
        if order.ord_type == "price", order.volume == nil {
            return "미지정 · 총액 예산 주문"
        }
        return "주문 \(order.volume ?? "미제공") · 체결 \(order.executed_volume ?? "미제공") · 잔여 \(order.remaining_volume ?? "미제공")"
    }

    private func orderFeeDescription(_ order: UpbitLiveOrder) -> String {
        guard let fee = order.paidFeeDecimal else { return "미제공 · 0으로 간주하지 않음" }
        return liveAmount(fee, currency: orderQuoteCurrency(order))
    }

    private func orderQuoteCurrency(_ order: UpbitLiveOrder) -> String {
        order.market.split(separator: "-").first.map(String.init) ?? "통화 미확인"
    }

    private func liveOrderStateDescription(_ order: UpbitLiveOrder) -> String {
        if order.state == "wait", let executed = order.executedVolumeDecimal, executed > 0 {
            return "부분 체결"
        }
        switch order.state {
        case "wait", "watch": return "미체결"
        case "done": return "완료"
        case "cancel", "cancelled": return "취소"
        case "reject", "rejected": return "거절"
        default: return "미확인 (\(order.state))"
        }
    }

    @objc private func copySelectedOrderId() {
        guard let selectedOrderId else { return }
        NSPasteboard.general.clearContents()
        NSPasteboard.general.setString(selectedOrderId, forType: .string)
        renderSelectedOrder()
        orderDetailLabel.stringValue += "\n주문 ID를 복사했습니다."
    }

    func updateControlSnapshot(_ snapshot: TradingControlSnapshot) {
        let received = liveWallNow()
        let clock = liveClock(receivedAt: received)
        _ = controlFreshness.acceptSnapshot(snapshot,
            providerObservedAtUTC: snapshot.observed_at_utc,
            localReceivedAt: received,
            isComplete: snapshot.exchange_read_mode == "LIVE_READ" && !snapshot.state.isEmpty,
            clock: clock)
        renderControlState()
        renderLiveStatusSummary()
    }

    func updateTradingState(_ _: LiveTradingState) {
        renderControlState()
    }

    @objc private func openTradingSettings() { onOpenSettings?() }

    // MARK: - Section 6: Policy Management & Safe Delete

    private func buildPolicyManagementView() -> NSView {
        if let existing = policyDeletionWorkspace {
            existing.updatePolicies(userPolicies: userPolicies, allPolicies: allPolicies)
            return existing
        }
        let workspace = PolicyDeletionWorkspace(
            service: PolicyDeletionService(),
            intentStore: PolicyDeletionIntentStore(
                supportDirectory: store.supportDirectory,
                dataRoot: store.dataDirectory()
            ),
            userPolicies: userPolicies,
            allPolicies: allPolicies,
            onPoliciesReloaded: { [weak self] in self?.reloadPolicyProjection() }
        )
        policyDeletionWorkspace = workspace
        return workspace
    }

    private func reloadPolicyProjection() {
        do {
            allPolicies = try dbReader.fetchAllPolicies()
            updateDatabaseStatusFooter()
            userPolicies = allPolicies.filter { $0.origin == "USER" }
            policyDeletionWorkspace?.updatePolicies(userPolicies: userPolicies, allPolicies: allPolicies)
            automationWorkspaceView?.updatePolicies(allPolicies)
            policyInventory = nil
            strategyOverview = nil
        } catch {
            policyDeletionWorkspace?.showPolicyRefreshError(error)
        }
    }

    private func updateDatabaseStatusFooter() {
        let summary = DashboardDatabaseStatusSummary(
            historyRunCount: historyTotal,
            policies: allPolicies
        ).label
        databaseStatusFooter?.stringValue = TerminalTheme.textScale >= 1.5
            ? "\(historyTotal) 실행 · \(allPolicies.count) 정책"
            : summary
        databaseStatusFooter?.toolTip = summary
        databaseStatusFooter?.setAccessibilityValue(summary)
    }

    // MARK: - NSTableViewDataSource & NSTableViewDelegate

    func numberOfRows(in tableView: NSTableView) -> Int {
        if tableView == sidebarTable {
            return SidebarRow.ordered.count
        } else if tableView == comparisonsTable {
            return currentComparisons.count
        } else if tableView == runsTable {
            return filteredRuns.count
        } else if tableView == jobsTable {
            return visibleJobsListItems.count
        } else if tableView == openOrdersTable {
            return visibleOrders.count
        }
        return 0
    }

    func tableView(_ tableView: NSTableView, heightOfRow row: Int) -> CGFloat {
        guard tableView == sidebarTable else { return tableView.rowHeight }
        return TerminalTheme.navigationRowHeight
    }

    func tableView(_ tableView: NSTableView, isGroupRow row: Int) -> Bool {
        guard tableView == sidebarTable, SidebarRow.ordered.indices.contains(row) else { return false }
        return SidebarRow.ordered[row].isGroup
    }

    func tableView(_ tableView: NSTableView, shouldSelectRow row: Int) -> Bool {
        guard tableView == sidebarTable else { return true }
        return SidebarRow.ordered.indices.contains(row) && !SidebarRow.ordered[row].isGroup
    }

    func tableView(_ tableView: NSTableView, viewFor tableColumn: NSTableColumn?, row: Int) -> NSView? {
        let identifier = tableColumn?.identifier.rawValue ?? ""

        if tableView == sidebarTable {
            guard SidebarRow.ordered.indices.contains(row) else { return nil }
            switch SidebarRow.ordered[row] {
            case .group(let title):
                let label = NSTextField(labelWithString: title)
                label.font = TerminalTheme.navigationGroup
                label.textColor = .secondaryLabelColor
                label.setAccessibilityLabel(title)
                return label
            case .route(let section):
                let cell = NSTableCellView()
                cell.identifier = NSUserInterfaceItemIdentifier("DashboardNavigationRouteCell")

                let icon = DashboardSidebarSymbolView()
                icon.image = NSImage(systemSymbolName: section.symbolName, accessibilityDescription: nil)
                icon.symbolConfiguration = NSImage.SymbolConfiguration(
                    pointSize: TerminalTheme.navigationIconSize,
                    weight: .medium
                )
                icon.updateTint(isSelected: sidebarTable.selectedRow == row, isEmphasized: window?.isKeyWindow == true)
                icon.translatesAutoresizingMaskIntoConstraints = false
                NSLayoutConstraint.activate([
                    icon.widthAnchor.constraint(equalToConstant: TerminalTheme.navigationIconSize),
                    icon.heightAnchor.constraint(equalToConstant: TerminalTheme.navigationIconSize)
                ])

                let title = NSTextField(labelWithString: section.title)
                title.font = TerminalTheme.navigationItem
                title.lineBreakMode = .byTruncatingTail
                title.toolTip = section.title
                title.textColor = sidebarTable.selectedRow == row && window?.isKeyWindow == true
                    ? .alternateSelectedControlTextColor
                    : .labelColor
                title.setAccessibilityIdentifier("dashboard.navigation.\(section.rawValue)")
                title.setAccessibilityLabel(section.title)
                title.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
                cell.textField = title

                let route = NSStackView(views: [icon, title])
                route.orientation = .horizontal
                route.alignment = .centerY
                route.spacing = 10
                route.translatesAutoresizingMaskIntoConstraints = false
                cell.addSubview(route)
                NSLayoutConstraint.activate([
                    route.leadingAnchor.constraint(equalTo: cell.leadingAnchor, constant: 12),
                    route.trailingAnchor.constraint(equalTo: cell.trailingAnchor, constant: -8),
                    route.centerYAnchor.constraint(equalTo: cell.centerYAnchor)
                ])
                return cell
            }
        }

        if tableView == comparisonsTable {
            guard row < currentComparisons.count else { return nil }
            let item = currentComparisons[row]
            let str: String
            var color: NSColor = .labelColor
            switch identifier {
            case "Market": str = item.market
            case "Policy": str = policyName(item.policyId)
            case "ModelId": str = item.modelId
            case "Return":
                str = item.netReturn.map { String(format: "%+.2f%%", $0 * 100) }
                    ?? (item.netReturnNullReason.map { "산출 불가 · \($0)" } ?? "산출 불가")
                color = item.netReturn.map { $0 > 0 ? TerminalTheme.positive : ($0 < 0 ? TerminalTheme.negative : TerminalTheme.secondaryText) } ?? .secondaryLabelColor
            case "MDD":
                str = item.maxDrawdown.map { String(format: "%.2f%%", $0 * 100) }
                    ?? (item.maxDrawdownNullReason.map { "산출 불가 · \($0)" } ?? "산출 불가")
            case "PnL":
                str = Formatters.money(item.netPnlDecimal, signed: true)
                color = item.netPnl > 0 ? TerminalTheme.positive : (item.netPnl < 0 ? TerminalTheme.negative : TerminalTheme.secondaryText)
            case "Fees": str = Formatters.money(item.cumulativeFeesDecimal)
            case "Trades": str = "\(item.fillCount) / \(item.closedEpisodes)"
            case "F08": str = item.f08Status ?? "미검증"
            case "Status": str = item.fillCount == 0 ? "\(item.status) · 무거래" : item.status
            default: str = ""
            }
            let cell = NSTextField(labelWithString: str)
            cell.lineBreakMode = .byTruncatingMiddle
            cell.toolTip = str
            cell.font = TerminalTheme.table
            cell.textColor = color
            if ["Return", "MDD", "PnL", "Fees", "Trades"].contains(identifier) { cell.alignment = .right }
            if tableView.selectedRowIndexes.contains(row) {
                let emphasized = tableView.rowView(atRow: row, makeIfNecessary: false)?.isEmphasized ?? (tableView.window?.isKeyWindow == true)
                cell.textColor = emphasized ? .alternateSelectedControlTextColor : .labelColor
            }
            return cell
        }

        if tableView == runsTable {
            guard row < filteredRuns.count else { return nil }
            let item = filteredRuns[row]
            let str: String
            switch identifier {
            case "Pinned": str = historyPinnedIds.contains(item.id) ? "●" : ""
            case "Markets": str = item.markets?.map { $0.replacingOccurrences(of: "KRW-", with: "") }.joined(separator: "/") ?? "미확인"
            case "Costs": str = historyCostTitle(item.costAssumptionLabel)
            case "Refs": str = "\(item.policyRevisionIds.map { String($0.count) } ?? "?") / \(item.datasetIds.map { String($0.count) } ?? "?")"
            case "F08": str = item.f08Status ?? "미검증"
            case "Window": str = "\(displayKST(item.windowStartUTC)) → \(displayKST(item.windowEndUTC))"
            case "RunId": str = item.id
            case "State": str = TerminalPresentation.state(item.state)
            case "Date": str = item.createdDateStr
            case "SelectedMetric":
                if let value = historyMetricValues[item.id] {
                    str = Formatters.percent(value, signed: historyMetricSort.indexOfSelectedItem == 1)
                } else {
                    switch historyMetricAvailability[item.id] {
                    case .ambiguousModels(let count): str = "모호 · \(count) 모델"
                    case .missingModel: str = "모델 없음"
                    case .missingMetric(let reason):
                        if let reason, !reason.isEmpty {
                            str = "지표 미확인 · \(reason)"
                        } else {
                            str = "지표 미확인"
                        }
                    default: str = "미확인"
                    }
                }
            case "Mode": str = TerminalPresentation.state(item.mode ?? "")
            case "Engine": str = item.engineVersion ?? "UNKNOWN"
            default: str = ""
            }
            let cell = NSTextField(labelWithString: str)
            cell.lineBreakMode = .byTruncatingMiddle
            cell.toolTip = "\(item.id) · \(item.state) · \(item.mode ?? "미확인")\n평가 \(item.windowStartUTC ?? "미확인") → \(item.windowEndUTC ?? "미확인") UTC\n비용 \(item.costAssumptionLabel ?? "미확인")\n자본 \(item.capitalMode ?? "미확인")"
            cell.font = TerminalTheme.table
            if identifier == "SelectedMetric" {
                cell.alignment = .right
                cell.toolTip = "\(historyMarket.titleOfSelectedItem ?? "미확인 시장") · \(historyPolicy.titleOfSelectedItem ?? "미확인 정책")\n원장 비율 \(historyMetricValues[item.id].map { NSDecimalNumber(decimal: $0).stringValue } ?? str)\n" + (cell.toolTip ?? "")
                let metricColumnTitle = tableColumn?.title ?? (historyMetricSort.indexOfSelectedItem == 1 ? "모델 수익률" : "모델 최대 낙폭")
                cell.setAccessibilityLabel("\(metricColumnTitle) · \(item.id)")
                cell.setAccessibilityValue(str)
                cell.setAccessibilityHelp(cell.toolTip)
            }
            if identifier == "State" && item.state == "COMPLETED" {
                cell.textColor = TerminalTheme.positive
            }
            if tableView.selectedRowIndexes.contains(row) {
                let emphasized = tableView.rowView(atRow: row, makeIfNecessary: false)?.isEmphasized ?? (tableView.window?.isKeyWindow == true)
                cell.textColor = emphasized ? .alternateSelectedControlTextColor : .labelColor
            }
            return cell
        }

        if tableView == jobsTable {
            let items = visibleJobsListItems
            guard items.indices.contains(row) else { return nil }
            let str: String
            let toolTip: String
            let rawStatus: String?
            switch identifier {
            case "JobId":
                switch items[row] {
                case .job(let job): str = job.id; toolTip = "작업 ID \(job.id)"
                case .schedule(let schedule): str = schedule.id; toolTip = "수집 스케줄 ID \(schedule.id)"
                }
                rawStatus = nil
            case "ReqId":
                switch items[row] {
                case .job(let job): str = job.requestId; toolTip = "요청 ID \(job.requestId)"
                case .schedule: str = "—"; toolTip = "요청 ID는 이 스케줄 목록에 제공되지 않습니다."
                }
                rawStatus = nil
            case "Kind":
                switch items[row] {
                case .job(let job): str = job.payloadKind; toolTip = "작업 종류 \(job.payloadKind)"
                case .schedule: str = "수집 스케줄"; toolTip = "일시중지 상태로 조회된 수집 스케줄"
                }
                rawStatus = nil
            case "JobStatus":
                switch items[row] {
                case .job(let job): str = TerminalPresentation.state(job.status); toolTip = "작업 상태 원문 \(job.status)"; rawStatus = job.status
                case .schedule(let schedule): str = TerminalPresentation.state(schedule.status.uppercased()); toolTip = "스케줄 상태 원문 \(schedule.status)"; rawStatus = schedule.status
                }
            case "JobDate":
                switch items[row] {
                case .job(let job): str = job.createdDateStr; toolTip = "작업 등록 시각 KST \(job.createdDateStr)"
                case .schedule(let schedule): str = displayKST(schedule.createdAt); toolTip = scheduleTooltip(schedule)
                }
                rawStatus = nil
            default: str = ""
                toolTip = ""
                rawStatus = nil
            }
            let cell = NSTextField(labelWithString: str)
            cell.lineBreakMode = .byTruncatingMiddle
            cell.toolTip = toolTip
            cell.font = TerminalTheme.table
            if identifier == "JobStatus", rawStatus == "COMPLETED" {
                cell.textColor = TerminalTheme.positive
            }
            if tableView.selectedRowIndexes.contains(row) {
                let emphasized = tableView.rowView(atRow: row, makeIfNecessary: false)?.isEmphasized ?? (tableView.window?.isKeyWindow == true)
                cell.textColor = emphasized ? .alternateSelectedControlTextColor : .labelColor
            }
            return cell
        }

        if tableView == openOrdersTable {
            guard row < visibleOrders.count else { return nil }
            let item = visibleOrders[row]
            let str: String
            switch identifier {
            case "OrderId": str = item.uuid
            case "OrderMarket": str = item.market
            case "OrderSide": str = item.sideDisplay
            case "OrderState": str = liveOrderStateDescription(item)
            case "OrderFilled": str = "\(item.executed_volume ?? "미확인") / \(item.remaining_volume ?? "미확인")"
            case "OrderVolume": str = item.ord_type == "price" && item.volume == nil ? "미지정 · 예산 주문" : (item.volume ?? "미제공")
            case "OrderPrice": str = orderPriceDescription(item)
            case "OrderFee": str = orderFeeDescription(item)
            case "OrderDate": str = displayKST(item.created_at)
            default: str = ""
            }
            let cell = NSTextField(labelWithString: str)
            cell.lineBreakMode = .byTruncatingMiddle
            cell.toolTip = "Upbit OBSERVED · \(orderScope.selectedSegment == 1 ? "완료·취소" : "미체결")\n주문 ID \(item.uuid)\n\(orderPriceDescription(item))\n가격/예산 원문 \(item.price ?? "미제공") \(orderQuoteCurrency(item))\n\(orderVolumeDescription(item))\n지급 수수료 \(orderFeeDescription(item))\npaid_fee 원문 \(item.paid_fee ?? "미제공") \(orderQuoteCurrency(item))"
            cell.font = TerminalTheme.table
            if tableView.selectedRowIndexes.contains(row) {
                let emphasized = tableView.rowView(atRow: row, makeIfNecessary: false)?.isEmphasized ?? (tableView.window?.isKeyWindow == true)
                cell.textColor = emphasized ? .alternateSelectedControlTextColor : .labelColor
            }
            return cell
        }

        return nil
    }

    private func selectComparisonRow() {
        guard comparisonsTable.numberOfRows == currentComparisons.count,
              let row = currentComparisons.firstIndex(where: { $0.modelId == selectedModelId }) else { return }
        synchronizingComparisonSelection = true
        if comparisonsTable.selectedRow != row {
            comparisonsTable.selectRowIndexes(IndexSet(integer: row), byExtendingSelection: false)
        }
        synchronizingComparisonSelection = false
        refreshSelectionAppearance(comparisonsTable)
    }

    /// Update visible cells without reloading the table and losing the selected identity.
    private func refreshSelectionAppearance(_ table: NSTableView) {
        for row in 0..<table.numberOfRows {
            for column in table.tableColumns.indices {
                guard let cell = table.view(atColumn: column, row: row, makeIfNecessary: false) as? NSTextField else { continue }
                let key = table.tableColumns[column].identifier.rawValue
                if table.selectedRowIndexes.contains(row) {
                    let active = table.rowView(atRow: row, makeIfNecessary: false)?.isEmphasized ?? (table.window?.isKeyWindow == true)
                    cell.textColor = active ? .alternateSelectedControlTextColor : .labelColor
                } else if table == comparisonsTable && currentComparisons.indices.contains(row) && ["Return", "PnL"].contains(key) {
                    let model = currentComparisons[row]
                    let value = key == "Return" ? model.netReturn : model.netPnl
                    cell.textColor = value.map { $0 > 0 ? TerminalTheme.positive : ($0 < 0 ? TerminalTheme.negative : .labelColor) } ?? .labelColor
                } else { cell.textColor = .labelColor }
            }
        }
    }

    private func refreshSidebarSymbolAppearance() {
        for row in 0..<sidebarTable.numberOfRows {
            guard let cell = sidebarTable.view(atColumn: 0, row: row, makeIfNecessary: false) as? NSTableCellView,
                  let route = cell.subviews.first as? NSStackView,
                  let icon = route.views.first as? DashboardSidebarSymbolView else { continue }
            let selected = sidebarTable.selectedRow == row
            let emphasized = window?.isKeyWindow == true
            icon.updateTint(isSelected: selected, isEmphasized: emphasized)
            if let title = route.views.compactMap({ $0 as? NSTextField }).first {
                title.textColor = selected && emphasized ? .alternateSelectedControlTextColor : .labelColor
            }
        }
    }

    func tableViewSelectionDidChange(_ notification: Notification) {
        guard let table = notification.object as? NSTableView else { return }
        if table == sidebarTable {
            guard !synchronizingSidebarSelection else { return }
            let row = table.selectedRow
            if row >= 0, SidebarRow.ordered.indices.contains(row), let sec = SidebarRow.ordered[row].section {
                if sec == .backtestAnalysis { showingRunDetails = false }
                selectSection(sec)
            }
            refreshSidebarSymbolAppearance()
        } else if table == comparisonsTable {
            guard !synchronizingComparisonSelection else { return }
            let row = table.selectedRow
            if currentComparisons.indices.contains(row), let run = selectedRun {
                selectedModelId = currentComparisons[row].modelId
                modelSelector.selectItem(at: row)
                applySelectedModel(run)
            }
        } else if table == runsTable {
            guard !restoringHistorySelection else { return }
            for run in filteredRuns { historySelection.removeValue(forKey: run.id) }
            for row in table.selectedRowIndexes where filteredRuns.indices.contains(row) {
                historySelection[filteredRuns[row].id] = filteredRuns[row]
            }
            updateHistorySelection()
            persistHistoryState()
        } else if table == openOrdersTable {
            let row = table.selectedRow
            selectedOrderId = visibleOrders.indices.contains(row) ? visibleOrders[row].uuid : nil
            renderSelectedOrder()
        }
    }

    func tableView(_ tableView: NSTableView, selectionIndexesForProposedSelection proposed: IndexSet) -> IndexSet {
        if tableView == sidebarTable {
            return IndexSet(proposed.filter { SidebarRow.ordered.indices.contains($0) && !SidebarRow.ordered[$0].isGroup })
        }
        guard tableView == runsTable else { return proposed }
        let visibleIds = Set(filteredRuns.map(\.id))
        let hiddenCount = historySelection.keys.filter { !visibleIds.contains($0) }.count
        return IndexSet(proposed.prefix(max(0, 4 - hiddenCount)))
    }

    func tableView(_ tableView: NSTableView, sortDescriptorsDidChange oldDescriptors: [NSSortDescriptor]) {
        guard let descriptor = tableView.sortDescriptors.first, let key = descriptor.key else { return }
        if tableView == comparisonsTable {
            currentComparisons.sort { lhs, rhs in
                let comparison: ComparisonResult
                switch key {
                case "Return", "MDD":
                    let left = key == "Return" ? lhs.netReturnDecimal : lhs.maxDrawdownDecimal
                    let right = key == "Return" ? rhs.netReturnDecimal : rhs.maxDrawdownDecimal
                    if left == nil { return false }
                    if right == nil { return true }
                    comparison = NSDecimalNumber(decimal: left!).compare(NSDecimalNumber(decimal: right!))
                case "PnL": comparison = NSDecimalNumber(decimal: lhs.netPnlDecimal).compare(NSDecimalNumber(decimal: rhs.netPnlDecimal))
                case "Fees": comparison = NSDecimalNumber(decimal: lhs.cumulativeFeesDecimal).compare(NSDecimalNumber(decimal: rhs.cumulativeFeesDecimal))
                case "Trades": comparison = NSNumber(value: lhs.fillCount).compare(NSNumber(value: rhs.fillCount))
                case "Market": comparison = lhs.market.compare(rhs.market)
                case "Policy": comparison = policyName(lhs.policyId).compare(policyName(rhs.policyId))
                case "F08": comparison = (lhs.f08Status ?? "미검증").compare(rhs.f08Status ?? "미검증")
                case "Status": comparison = lhs.status.compare(rhs.status)
                default: comparison = lhs.modelId.compare(rhs.modelId)
                }
                if comparison == .orderedSame { return lhs.modelId < rhs.modelId }
                return descriptor.ascending ? comparison == .orderedAscending : comparison == .orderedDescending
            }
            rebuildModelSelector()
        } else if tableView == runsTable {
            historyMetricSort.selectItem(at: 0)
            historyMetricValues = [:]
            historyMetricAvailability = [:]
            if let column = runsTable.tableColumn(withIdentifier: NSUserInterfaceItemIdentifier("SelectedMetric")) { runsTable.removeTableColumn(column) }
            persistHistoryState()
            showHistoryWorkspaceFeedback("표시된 행을 선택한 열로 정렬합니다. 지표 정렬은 해제되었습니다.")
            filteredRuns.sort { lhs, rhs in
                let comparison: ComparisonResult
                switch key {
                case "Date": comparison = NSNumber(value: lhs.createdAtMilliseconds).compare(NSNumber(value: rhs.createdAtMilliseconds))
                case "State": comparison = lhs.state.compare(rhs.state)
                case "Mode": comparison = (lhs.mode ?? "").compare(rhs.mode ?? "")
                case "Markets": comparison = (lhs.markets?.joined(separator: "/") ?? "").compare(rhs.markets?.joined(separator: "/") ?? "")
                case "Costs": comparison = (lhs.costAssumptionLabel ?? "").compare(rhs.costAssumptionLabel ?? "")
                case "F08": comparison = (lhs.f08Status ?? "").compare(rhs.f08Status ?? "")
                case "Pinned": comparison = NSNumber(value: historyPinnedIds.contains(lhs.id)).compare(NSNumber(value: historyPinnedIds.contains(rhs.id)))
                default: comparison = lhs.id.compare(rhs.id)
                }
                if comparison == .orderedSame { return lhs.id < rhs.id }
                return descriptor.ascending ? comparison == .orderedAscending : comparison == .orderedDescending
            }
            historyCount.stringValue = "표시 \(filteredRuns.count) / \(historyTotal) · 표시된 행 정렬 · KST"
            restoringHistorySelection = true; tableView.reloadData(); restoringHistorySelection = false
            restoreHistorySelection(); updateHistorySelection()
            return
        }
        tableView.reloadData()
        if tableView == comparisonsTable { selectComparisonRow() }
    }
}
