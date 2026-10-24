import AppKit

private struct DecisionAnalysisDrilldownRow {
    let title: String
    let count: String
    let unit: String
    let help: String
    let filter: DecisionAnalysisFilter?
}

private enum DecisionAnalysisSavedExplanationPresentation {
    static func lines(for explanation: AutomationJSONValue, masked: Bool) -> [String] {
        guard !masked else {
            return ["개인정보 숨김 · 저장된 판단 근거를 표시하지 않습니다."]
        }
        guard case .object(let fields) = explanation else {
            return ["저장된 판단 근거 형식을 해석할 수 없습니다."]
        }

        let conditionTrace = fields["condition_trace"]
        var lines = RecordedDecisionPresentation.lines(for: .object(fields), masked: false)
        lines.removeAll { line in
            line.hasPrefix("전략 행동:")
                || line.hasPrefix("제약 적용 후 행동:")
                || line.hasPrefix("실행 상태:")
                || line.hasPrefix("조건 기록 상태:")
                || line.hasPrefix("정책 리비전 참조:")
                || line.hasPrefix("저장 조건 평가:")
                || line.hasPrefix("조건 비교 기록:")
                || line.hasPrefix("조건 ")
                || line.hasPrefix("  실제 비교 입력:")
                || line.hasPrefix("연결된 주문:")
                || line.hasPrefix("주문 ·")
                || line.hasPrefix("주문 원본:")
        }
        let actionLines = [
            "전략 행동 · \(actionLabel(code(fields["strategy_action"])))",
            "제약 후 행동 · \(actionLabel(code(fields["constrained_action"])))",
            "실행 상태 · \(executionLabel(code(fields["execution_state"])))"
        ]
        let decisionTimeIndex = lines.firstIndex(where: { $0.hasPrefix("결정 시각:") }) ?? -1
        lines.insert(contentsOf: actionLines, at: min(lines.count, decisionTimeIndex + 1))
        let ruleIndex = lines.firstIndex(where: { $0.hasPrefix("일치 규칙 ID:") })
            ?? (lines.firstIndex(where: { $0.hasPrefix("결정 시각:") }) ?? -1) + actionLines.count + 1
        lines.insert("조건 평가 기록 · \(captureLabel(code(fields["condition_trace_capture"])))", at: min(lines.count, ruleIndex + 1))
        let conditionAndOrderLines = conditionLines(conditionTrace) + orderLines(fields["orders"])
        let fillsIndex = lines.firstIndex(where: { $0.hasPrefix("연결된 체결:") }) ?? lines.endIndex
        lines.insert(contentsOf: conditionAndOrderLines, at: fillsIndex)
        return lines
    }

    private static func orderLines(_ value: AutomationJSONValue?) -> [String] {
        guard let value else { return ["연결 주문 · 미기록"] }
        guard case .array(let orders) = value else {
            if isNull(value) { return ["연결 주문 · 미기록"] }
            return ["연결 주문 형식 · 확인 필요"]
        }
        guard !orders.isEmpty else { return ["연결 주문 · 0건"] }
        var lines = ["연결 주문 · \(orders.count)건"]
        for order in orders.prefix(32) {
            guard case .object(let fields) = order else {
                lines.append("주문 원본 · \(valueText(order))")
                continue
            }
            let identifier = valueText(fields["order_id"])
            let state = executionLabel(code(fields["state"]))
            let reason = valueText(fields["reason"])
            lines.append("주문 · \(identifier) · \(state) · 사유 \(reason)")
        }
        if orders.count > 32 { lines.append("나머지 \(orders.count - 32)개 주문은 원본 JSON에서 확인하세요.") }
        return lines
    }

    private static func conditionLines(_ value: AutomationJSONValue?) -> [String] {
        guard let value else { return ["저장 조건 비교 · 미기록"] }
        guard case .array(let conditions) = value else {
            if case .null = value { return ["저장 조건 비교 · 미기록"] }
            return ["저장 조건 비교 형식 · 확인 필요"]
        }
        guard !conditions.isEmpty else { return ["저장 조건 평가 · 0개"] }

        var lines = ["저장 조건 평가 · \(conditions.count)개"]
        for (index, condition) in conditions.prefix(32).enumerated() {
            guard case .object(let fields) = condition else {
                lines.append("조건 \(index + 1) · 원본 형식 확인 필요")
                lines.append("  원본 · \(valueText(condition))")
                continue
            }
            let outcome = savedOutcome(fields["outcome"])
            let role = roleLabel(code(fields["role"]))
            lines.append("조건 \(index + 1) · \(role) · \(outcomeLabel(outcome.state))")
            if let reason = outcome.reason {
                lines.append("  판단 사유 · \(reasonLabel(reason)) [\(reason)]")
            }
            if let path = string(fields["ast_path"]) {
                lines.append("  근거 위치 · \(path)")
            }
            if let result = fields["engine_result"], !isNull(result) {
                lines.append("  저장 평가값 · \(engineResultLabel(result))")
            } else if let legacyResult = fields["result"], !isNull(legacyResult) {
                lines.append("  추가 저장 결과 · \(engineResultLabel(legacyResult))")
            }
            if let evidence = fields["evidence"], !isNull(evidence) {
                lines.append("  저장 비교 입력 · \(evidenceLabel(evidence))")
            } else {
                lines.append(outcome.state == "NOT_EVALUATED"
                    ? "  저장 비교 입력 · 미평가"
                    : "  저장 비교 입력 · 미기록")
            }
        }
        if conditions.count > 32 {
            lines.append("나머지 \(conditions.count - 32)개 조건은 원본 JSON에서 확인하세요.")
        }
        return lines
    }

    private static func savedOutcome(_ value: AutomationJSONValue?) -> (state: String, reason: String?) {
        guard let value else { return ("NOT_CAPTURED", nil) }
        if case .string(let state) = value { return (state, nil) }
        if case .boolean(let result) = value { return (result ? "TRUE" : "FALSE", nil) }
        guard case .object(let fields) = value else { return ("NOT_CAPTURED", nil) }

        if let state = string(fields["status"]) ?? string(fields["kind"]) {
            let reason = fields["reason"].map(code).flatMap { $0.isEmpty ? nil : $0 }
            return (state, reason)
        }
        if fields.count == 1, let (state, associated) = fields.first {
            if isNull(associated) { return (state, nil) }
            if case .object(let nested) = associated {
                return (state, nested["reason"].map(code) ?? code(associated))
            }
            return (state, code(associated))
        }
        if let state = string(fields["value"]) { return (state, fields["reason"].map(code)) }
        return ("NOT_CAPTURED", nil)
    }

    private static func outcomeLabel(_ state: String) -> String {
        switch state {
        case "TRUE": "충족"
        case "FALSE": "불충족"
        case "UNKNOWN": "확인 불가"
        case "NOT_EVALUATED": "미평가"
        case "NOT_CAPTURED": "미기록"
        default: "결과 확인 필요 · \(state)"
        }
    }

    private static func engineResultLabel(_ value: AutomationJSONValue) -> String {
        switch value {
        case .boolean(let result): return result ? "저장 true" : "저장 false"
        case .string(let result): return result
        default: return "저장 \(valueText(value))"
        }
    }

    static func comparisonFacts(for detail: DecisionAnalysisDecisionDetail, masked: Bool) -> [String] {
        guard !masked else { return ["행동·비중·저장 사유 · 개인정보 숨김"] }
        guard case .object(let fields) = detail.explanation else {
            return ["행동·비중·저장 사유 · 설명 형식 확인 필요"]
        }
        let action = "\(actionLabel(detail.row.strategy_action)) → \(actionLabel(detail.row.constrained_action)) → \(executionLabel(detail.row.execution_state))"
        let weights = "실제 \(valueText(fields["actual_weight_at_decision"])) · 목표 \(valueText(fields["raw_target_weight"])) · 제약 후 \(valueText(fields["constrained_target_weight"]))"
        let reasons = detail.row.reasons.prefix(3).map { reason in
            let label = reason.label_ko.isEmpty ? "저장 이유" : reason.label_ko
            return "\(stageLabel(reason.stage)) · \(label)"
        }
        let reasonSummary: String
        if reasons.isEmpty {
            reasonSummary = "없음"
        } else {
            let more = detail.row.reasons.count > reasons.count ? " · 외 \(detail.row.reasons.count - reasons.count)건" : ""
            reasonSummary = reasons.joined(separator: " / ") + more
        }
        return ["행동 · \(action)", "비중 · \(weights)", "저장 사유 · \(reasonSummary)"]
    }

    private static func evidenceLabel(_ value: AutomationJSONValue) -> String {
        guard case .object(let fields) = value else { return valueText(value) }
        let kind = code(fields["kind"])
        switch kind {
        case "COMPARISON", "COUNT_COMPARISON", "WEIGHT_COMPARISON":
            return "\(valueText(fields["left"])) \(operatorLabel(code(fields["comparison"]))) \(valueText(fields["right"]))"
        case "WEIGHT_BAND":
            return "목표 \(valueText(fields["target"])) · 실제 \(valueText(fields["actual"])) · 차이 \(valueText(fields["difference"])) · 허용 범위 \(valueText(fields["band"]))"
        case "POSITION":
            return "기대 상태 \(positionLabel(code(fields["expected"]))) · 실제 상태 \(positionLabel(code(fields["actual"])))"
        case "CROSSING":
            let previous = fields["previous"]
            let previousLabel = previous.map(isNull) == true
                ? "이전 입력 미기록"
                : "이전 입력 \(operandPairLabel(previous))"
            return "\(previousLabel) · 현재 입력 \(operandPairLabel(fields["current"]))"
        default:
            return valueText(value)
        }
    }

    private static func operandPairLabel(_ value: AutomationJSONValue?) -> String {
        guard case .object(let fields) = value,
              let left = fields["left"], let right = fields["right"] else {
            return valueText(value)
        }
        return "왼쪽 \(valueText(left)) · 오른쪽 \(valueText(right))"
    }

    private static func operatorLabel(_ value: String) -> String {
        switch value {
        case "GT", "GREATER_THAN": ">"
        case "GTE", "GREATER_THAN_OR_EQUAL": "≥"
        case "LT", "LESS_THAN": "<"
        case "LTE", "LESS_THAN_OR_EQUAL": "≤"
        case "EQ", "EQUAL": "="
        case "NE", "NOT_EQUAL": "≠"
        default: value
        }
    }

    private static func positionLabel(_ value: String) -> String {
        switch value {
        case "CASH": "현금"
        case "LONG": "보유"
        default: value.isEmpty ? "미기록" : value
        }
    }

    static func roleLabel(_ value: String) -> String {
        switch value {
        case "ENTRY": "진입"
        case "FILTER": "필터"
        case "EXIT": "청산"
        case "CLASSIFICATION": "분류"
        case "UNSPECIFIED", "": "구분 미기록"
        default: "조건 역할 확인"
        }
    }

    private static func reasonLabel(_ value: String) -> String {
        switch value {
        case "CROSS_HISTORY_UNAVAILABLE": "과거 입력 확인 불가"
        case "AND_SHORT_CIRCUIT": "AND 평가 생략"
        case "OR_SHORT_CIRCUIT": "OR 평가 생략"
        case "IF_BRANCH_NOT_SELECTED": "선택되지 않은 분기"
        case "PRIOR_RULE_MATCHED": "앞선 규칙 선택"
        case "RULE_NOT_MATCHED": "규칙 불일치"
        default: "저장된 사유"
        }
    }

    private static func captureLabel(_ value: String) -> String {
        switch value {
        case "CAPTURED": "완전 기록"
        case "PARTIAL_CAPTURE": "일부 기록"
        case "NOT_CAPTURED": "미기록"
        case "NOT_DECISION_EVENT": "판단 외 이벤트"
        default: "확인 필요"
        }
    }

    static func actionLabel(_ value: String) -> String {
        switch value {
        case "BUY": "매수"
        case "SELL": "매도"
        case "HOLD": "유지"
        case "NOT_EVALUATED": "미평가"
        case "NOT_CAPTURED": "미기록"
        default: "확인 필요"
        }
    }

    static func executionLabel(_ value: String) -> String {
        switch value {
        case "NOT_EVALUATED": "미평가"
        case "NO_ORDER": "주문 없음"
        case "REJECTED": "거부"
        case "PENDING": "대기"
        case "FILLED": "체결"
        case "PARTIALLY_FILLED": "부분 체결"
        case "CANCELLED": "취소"
        case "EXPIRED": "만료"
        case "NOT_CAPTURED": "미기록"
        default: "확인 필요"
        }
    }

    static func stageLabel(_ value: String) -> String {
        switch value {
        case "DATA_READINESS": "데이터 준비"
        case "STRATEGY": "전략"
        case "CONSTRAINTS": "제약"
        case "ADMISSION": "주문 허용"
        case "EXECUTION": "실행"
        case "LIFECYCLE": "상태 변화"
        default: "기타"
        }
    }

    static func outcomeLabelForSummary(_ state: String) -> String {
        switch state {
        case "TRUE": return "충족"
        case "FALSE": return "불충족"
        case "UNKNOWN": return "확인 불가"
        case "NOT_EVALUATED": return "미평가"
        case "NOT_CAPTURED": return "미기록"
        default: return "확인 필요"
        }
    }

    static func observationKindLabel(_ value: String) -> String {
        switch value {
        case "STARTED": "시작"
        case "PAUSED": "일시정지"
        case "RESUMED": "재개"
        case "STOPPED": "종료"
        case "RECOVERY_REQUIRED": "복구 필요"
        case "RECOVERY_BLOCKED": "복구 차단"
        case "CANDLE_OBSERVED": "봉 관측"
        case "DECISION_PREPARATION": "판단 준비"
        default: "판단 외 기록"
        }
    }

    static func clockBasisLabel(_ value: String) -> String {
        switch value {
        case "preparation_decision_time": "준비 판단 시각"
        case "paper_source_or_finalized_time": "PAPER 원천/확정 시각"
        case "automation_event_time": "자동화 이벤트 시각"
        default: "시각 기준 확인 필요"
        }
    }

    static func captureLabelForDisplay(_ value: String) -> String {
        captureLabel(value)
    }

    private static func code(_ value: AutomationJSONValue?) -> String {
        guard let value else { return "" }
        if case .string(let value) = value { return value }
        if case .object(let fields) = value {
            if let kind = string(fields["kind"]) { return kind }
            if fields.count == 1, let key = fields.keys.first { return key }
        }
        return ""
    }

    private static func string(_ value: AutomationJSONValue?) -> String? {
        guard let value, case .string(let string) = value else { return nil }
        return string
    }

    private static func isNull(_ value: AutomationJSONValue) -> Bool {
        if case .null = value { return true }
        return false
    }

    private static func valueText(_ value: AutomationJSONValue?) -> String {
        guard let value else { return "미기록" }
        switch value {
        case .null: return "미기록"
        case .string(let text): return text.isEmpty ? "미기록" : text
        case .number(let number): return NSDecimalNumber(decimal: number).stringValue
        case .boolean(let result): return result ? "true" : "false"
        case .object(let fields):
            if fields.count == 1, let wrapped = fields["value"] { return valueText(wrapped) }
            return compact(value)
        case .array:
            return compact(value)
        }
    }

    private static func compact(_ value: AutomationJSONValue) -> String {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        guard let data = try? encoder.encode(value), let result = String(data: data, encoding: .utf8) else {
            return "원본 입력 확인 필요"
        }
        return result.replacingOccurrences(of: "\n", with: " ")
    }
}

final class DecisionAnalysisWindowController: NSWindowController, NSWindowDelegate, NSTableViewDataSource, NSTableViewDelegate, NSTextFieldDelegate {
    private static let pageLimit: UInt32 = 100
    private let service: DecisionAnalysisMCPService
    private let privacyIsMasked: () -> Bool
    private let startField = NSTextField(string: "")
    private let endField = NSTextField(string: "")
    private let marketPopup = NSPopUpButton(frame: .zero, pullsDown: false)
    private let refreshButton = NSButton(title: "조회 / 새로고침", target: nil, action: nil)
    private let pinButton = NSButton(title: "선택 판단 고정", target: nil, action: nil)
    private let compareButton = NSButton(title: "고정 판단과 비교", target: nil, action: nil)
    private let previousPageButton = NSButton(title: "이전 페이지", target: nil, action: nil)
    private let nextPageButton = NSButton(title: "다음 페이지", target: nil, action: nil)
    private let identityLabel = NSTextField(wrappingLabelWithString: "저장된 모의 또는 자동화 세션을 선택하세요.")
    private let statusLabel = NSTextField(wrappingLabelWithString: "판단 분석은 저장된 사실만 조회합니다.")
    private let overviewLabel = NSTextField(wrappingLabelWithString: "기간 총계: 조회 전")
    private let overviewDetailLabel = NSTextField(wrappingLabelWithString: "전략 행동, 제약 후 행동, 실행 상태와 조건 추적 캡처를 별도 단위로 표시합니다.")
    private let drilldownLabel = NSTextField(wrappingLabelWithString: "분류·이유 집계는 기간 전체 기준입니다. 이유는 겹칠 수 있고 조건은 발생 수만 표시합니다.")
    private let listLabel = NSTextField(wrappingLabelWithString: "선택한 저장 판단")
    private let drilldownTable = NSTableView()
    private let decisionTable = NSTableView()
    private let explanationText = NSTextView()
    private let comparisonText = NSTextView()
    private let explanationScroll = TerminalWrappingTextScrollView()
    private let comparisonScroll = TerminalWrappingTextScrollView()
    private let rawExplanationButton = NSButton(title: "원본 JSON 보기", target: nil, action: nil)
    private let copyIdentityButton = NSButton(title: "저장 ID 복사", target: nil, action: nil)
    private var analysisSplitView: NSSplitView?

    private var sessionContext: DecisionAnalysisSessionContext?
    private var analysisTransportContext: NativeMCPTransportContext?
    private var period: DecisionAnalysisPeriod?
    private var population: DecisionAnalysisPopulation?
    private var selection: DecisionAnalysisSelection?
    private var overview: DecisionAnalysisTotals?
    private var populationIsComplete = false
    private var summaryRows: [DecisionAnalysisDrilldownRow] = []
    private var decisions: [DecisionAnalysisDecisionRow] = []
    private var page: DecisionAnalysisPage?
    private var pageFilter: DecisionAnalysisFilter?
    private var selectedDecisionID: DecisionAnalysisDecisionID?
    private var selectedDetail: DecisionAnalysisDecisionDetail?
    private var comparison: DecisionAnalysisComparison?
    private var comparedSourceSnapshot: DecisionAnalysisSourceSnapshot?
    private var pinnedSelection: DecisionAnalysisSelection?
    private var pinnedDecisionID: DecisionAnalysisDecisionID?
    private var receivedAt: Date?
    private var lastMeasurementText: String?
    private var activeReadRequest: NativeMCPRequest?
    private var requestGeneration: UInt64 = 0
    private var isClosed = false
    private var isStale = false
    private var isUpdatingTableSelection = false
    private var isShowingRawExplanation = false

    var onClose: (() -> Void)?

    init(
        service: DecisionAnalysisMCPService = DecisionAnalysisMCPService(),
        privacyIsMasked: @escaping () -> Bool = { false }
    ) {
        self.service = service
        self.privacyIsMasked = privacyIsMasked
        super.init(window: nil)
        buildWindow()
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) { nil }

    deinit {
        activeReadRequest?.cancel()
    }

    func showAnalysis(for context: DecisionAnalysisSessionContext) {
        guard !isClosed else { return }
        let contextChanged = sessionContext != context
        if contextChanged {
            cancelActiveRead()
            sessionContext = context
            analysisTransportContext = nil
            period = Self.defaultPeriod(endingAt: Date())
            startField.stringValue = period?.start ?? ""
            endField.stringValue = period?.end ?? ""
            population = nil
            selection = nil
            overview = nil
            populationIsComplete = false
            summaryRows = []
            decisions = []
            page = nil
            pageFilter = nil
            selectedDecisionID = nil
            selectedDetail = nil
            comparison = nil
            comparedSourceSnapshot = nil
            pinnedSelection = nil
            pinnedDecisionID = nil
            isStale = false
            isShowingRawExplanation = false
            receivedAt = nil
            lastMeasurementText = nil
            populateMarketChoices(for: context)
            renderAll()
        }
        showWindow(nil)
        window?.makeKeyAndOrderFront(nil)
        window?.contentView?.layoutSubtreeIfNeeded()
        applyBalancedSplitPosition()
        if contextChanged { refreshAnalysis() }
    }

    func refreshPrivacyPresentation() {
        renderIdentity()
        renderOverview()
        renderSelectionDetails()
        drilldownTable.reloadData()
        decisionTable.reloadData()
    }

    func numberOfRows(in tableView: NSTableView) -> Int {
        if tableView === drilldownTable { return summaryRows.count }
        if tableView === decisionTable { return decisions.count }
        return 0
    }

    func tableView(_ tableView: NSTableView, viewFor tableColumn: NSTableColumn?, row: Int) -> NSView? {
        guard let key = tableColumn?.identifier.rawValue else { return nil }
        let value: String
        if tableView === drilldownTable, summaryRows.indices.contains(row) {
            let item = summaryRows[row]
            switch key {
            case "group": value = item.title
            case "count": value = item.count
            case "unit": value = item.unit
            default: value = ""
            }
        } else if tableView === decisionTable, decisions.indices.contains(row) {
            let item = decisions[row]
            let masked = shouldMaskIdentity
            switch key {
            case "time":
                value = item.decision_time ?? "시각 미기록 · 기간 포함 확인 불가"
            case "market":
                value = AutomationReviewPrivacyPresentation.identity(item.market, masked: masked)
            case "action":
                value = "\(DecisionAnalysisSavedExplanationPresentation.actionLabel(item.strategy_action)) → \(DecisionAnalysisSavedExplanationPresentation.actionLabel(item.constrained_action))"
            case "execution":
                value = DecisionAnalysisSavedExplanationPresentation.executionLabel(item.execution_state)
            case "placement":
                value = item.period_placement == .inPeriod
                    ? "선택 기간 내"
                    : "판단 시각 미기록 · 기간 포함 확인 불가"
            default:
                value = ""
            }
        } else {
            return nil
        }

        let cell = NSTextField(labelWithString: value)
        cell.font = TerminalTheme.table
        cell.lineBreakMode = .byTruncatingTail
        cell.toolTip = value
        cell.setAccessibilityLabel(value)
        return cell
    }

    func tableViewSelectionDidChange(_ notification: Notification) {
        guard !isUpdatingTableSelection, !isStale,
              let table = notification.object as? NSTableView else { return }
        if table === drilldownTable {
            guard summaryRows.indices.contains(table.selectedRow) else { return }
            selectDrilldown(summaryRows[table.selectedRow])
        } else if table === decisionTable,
                  decisions.indices.contains(table.selectedRow) {
            loadDecision(decisions[table.selectedRow])
        }
    }

    func controlTextDidChange(_ notification: Notification) {
        guard let field = notification.object as? NSTextField,
              field === startField || field === endField else { return }
        markStale("기간 조건이 바뀌었습니다. 마지막 확인 결과는 유지됩니다. 새로고침을 선택하세요.")
    }

    func windowWillClose(_ notification: Notification) {
        _ = notification
        guard !isClosed else { return }
        isClosed = true
        cancelActiveRead()
        onClose?()
        onClose = nil
    }

    func windowDidMiniaturize(_ notification: Notification) {
        _ = notification
        markStale("최소화되어 조회를 취소했습니다. 마지막 확인 결과는 유지됩니다. 창을 복원한 뒤 새로고침하세요.")
    }

    func windowDidResize(_ notification: Notification) {
        _ = notification
        applyBalancedSplitPosition()
    }

    @objc private func refreshAnalysis() {
        guard let context = sessionContext else { return }
        guard let period = validatedPeriod() else { return }
        let market = selectedMarket
        let population = context.population(period: period, market: market)
        guard let transportContext = service.currentTransportContext() else {
            markStale("Backend 연결을 확인할 수 없습니다. 마지막으로 받은 분석 값은 그대로 표시합니다.")
            return
        }

        self.period = period
        self.population = population
        isStale = overview != nil || page != nil || selectedDetail != nil
        renderAll()
        statusLabel.stringValue = isStale
            ? "새 저장 자료 조회 중 · 이전 확인 결과는 유지됩니다."
            : "저장된 판단 기간 전체를 조회 중입니다."
        statusLabel.textColor = TerminalTheme.secondaryText

        let ticket = beginRead()
        let request = service.overview(
            population: population,
            expectedMode: context.mode,
            expectedTransportContext: transportContext
        ) { [weak self] result in
            DispatchQueue.main.async {
                guard let self, self.accepts(ticket) else { return }
                self.activeReadRequest = nil
                switch result {
                case .failure(let error):
                    self.markStale(self.failureMessage(error))
                case .success(let value):
                    self.analysisTransportContext = transportContext
                    self.selection = value.0.selection
                    self.overview = value.2
                    self.populationIsComplete = value.1
                    self.receivedAt = Date()
                    self.lastMeasurementText = Self.measurementLabel("기간 전체 조회", value.3)
                    self.summaryRows = value.1
                        ? Self.makeDrilldownRows(from: value.2, selectedMarket: population.market)
                        : []
                    self.page = nil
                    self.decisions = []
                    self.selectedDecisionID = nil
                    self.selectedDetail = nil
                    self.comparison = nil
                    self.comparedSourceSnapshot = nil
                    self.renderAll()
                    if !value.1 {
                        self.isStale = false
                        self.statusLabel.stringValue = "선택한 기간의 저장 사실을 완전하게 집계하지 못했습니다. 기간을 좁혀 다시 조회하세요."
                        self.statusLabel.textColor = TerminalTheme.negative
                        self.updateControls()
                    } else {
                        self.statusLabel.stringValue = "저장 기준 확인 완료 · 목록은 기간 내 판단과 시각 미기록 판단을 나눠 표시합니다."
                        self.statusLabel.textColor = TerminalTheme.secondaryText
                        self.loadDecisions(offset: 0, filter: self.pageFilter, context: transportContext)
                    }
                }
            }
        }
        retain(request, for: ticket)
    }

    @objc private func marketSelectionChanged() {
        guard marketPopup.selectedItem != nil else { return }
        markStale("시장 조건이 바뀌었습니다. 마지막 확인 결과는 유지됩니다. 새로고침을 선택하세요.")
    }

    @objc private func selectPreviousPage() {
        guard let page, page.offset > 0, !isStale,
              let context = currentAnalysisTransportContext() else { return }
        let prior = page.offset > Self.pageLimit ? page.offset - Self.pageLimit : 0
        loadDecisions(offset: prior, filter: pageFilter, context: context)
    }

    @objc private func selectNextPage() {
        guard let page, let next = page.next_offset, !isStale,
              let context = currentAnalysisTransportContext() else { return }
        loadDecisions(offset: next, filter: pageFilter, context: context)
    }

    @objc private func pinSelectedDecision() {
        guard !isStale, let selection, let selectedDecisionID,
              selectedDetail?.row.decision_id == selectedDecisionID else { return }
        pinnedSelection = selection
        pinnedDecisionID = selectedDecisionID
        pinButton.title = "선택 판단 고정됨"
        statusLabel.stringValue = "비교 기준을 현재 저장 판단의 세션·정책·기간·기록 ID에 고정했습니다."
        statusLabel.textColor = TerminalTheme.secondaryText
        updateControls()
    }

    @objc private func compareSelectedDecision() {
        guard !isStale, let context = currentAnalysisTransportContext() else { return }
        loadComparison(expectedTransportContext: context)
    }

    @objc private func toggleRawExplanation() {
        guard selectedDetail != nil, !shouldMaskIdentity else { return }
        isShowingRawExplanation.toggle()
        renderSelectionDetails()
    }

    @objc private func copySelectedIdentity() {
        guard let sessionContext, !shouldMaskIdentity else { return }
        var lines = [
            "family=\(sessionContext.source.family)",
            "session_id=\(sessionContext.source.sessionID)",
            "mode=\(sessionContext.mode)",
            "policy_id=\(sessionContext.policyReference.policy_id)",
            "revision_id=\(sessionContext.policyReference.revision_id)",
            "definition_digest=\(sessionContext.policyReference.definition_digest)"
        ]
        if let population {
            lines.append("period.start=\(population.period.start)")
            lines.append("period.end=\(population.period.end)")
            lines.append("market=\(population.market ?? "ALL")")
        }
        if let fence = selection?.fence {
            switch fence {
            case .paper(let sequence): lines.append("saved_sequence=\(sequence)")
            case .automation(let digest): lines.append("snapshot_identity=\(digest)")
            }
        }
        if let selectedDecisionID,
           let encoded = try? JSONEncoder().encode(selectedDecisionID),
           let identity = String(data: encoded, encoding: .utf8) {
            lines.append("selected_decision_id=\(identity)")
        }
        if let digest = selectedDetail?.row.saved_fact_digest {
            lines.append("saved_fact_digest=\(digest)")
        }
        NSPasteboard.general.clearContents()
        _ = NSPasteboard.general.setString(lines.joined(separator: "\n"), forType: .string)
        statusLabel.stringValue = "저장 ID를 클립보드에 복사했습니다."
        statusLabel.textColor = TerminalTheme.secondaryText
    }

    private var selectedMarket: String? {
        marketPopup.selectedItem?.representedObject as? String
    }

    private var shouldMaskIdentity: Bool {
        guard let sessionContext, case .automation = sessionContext.source else { return false }
        return privacyIsMasked()
    }

    private func buildWindow() {
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 1200, height: 800),
            styleMask: [.titled, .closable, .miniaturizable, .resizable],
            backing: .buffered,
            defer: true
        )
        window.title = "판단 분석 · 읽기 전용"
        window.isReleasedWhenClosed = false
        window.minSize = NSSize(width: 980, height: 700)
        window.delegate = self
        super.window = window

        let content = TerminalSurfaceView()
        let stack = NSStackView()
        stack.orientation = .vertical
        stack.alignment = .leading
        stack.distribution = .fill
        stack.spacing = 8
        stack.translatesAutoresizingMaskIntoConstraints = false
        content.addSubview(stack)

        let title = NSTextField(labelWithString: "판단 분석")
        title.font = TerminalTheme.screenTitle
        title.textColor = .labelColor
        title.setAccessibilityLabel("저장된 판단 분석")

        identityLabel.font = TerminalTheme.monospacedFont(ofSize: 11)
        identityLabel.textColor = TerminalTheme.secondaryText
        identityLabel.maximumNumberOfLines = 0
        identityLabel.lineBreakMode = .byWordWrapping
        identityLabel.setAccessibilityLabel("선택한 저장 세션, 모드, 동결 정책과 분석 기간")

        statusLabel.font = TerminalTheme.body
        statusLabel.textColor = TerminalTheme.secondaryText
        statusLabel.maximumNumberOfLines = 0
        statusLabel.lineBreakMode = .byWordWrapping
        statusLabel.setAccessibilityLabel("분석 조회 상태와 마지막 확인 결과 안내")

        configureFilters()
        configureTables()
        configureTextViews()

        overviewLabel.font = TerminalTheme.monospacedDigitFont(ofSize: 12, weight: .semibold)
        overviewLabel.textColor = .labelColor
        overviewLabel.maximumNumberOfLines = 0
        overviewLabel.lineBreakMode = .byWordWrapping
        overviewLabel.setAccessibilityLabel("선택 기간 내 판단 총계와 미배치 판단 수")
        overviewDetailLabel.font = TerminalTheme.monospacedFont(ofSize: 11)
        overviewDetailLabel.textColor = TerminalTheme.secondaryText
        overviewDetailLabel.maximumNumberOfLines = 0
        overviewDetailLabel.lineBreakMode = .byWordWrapping
        overviewDetailLabel.setAccessibilityLabel("전략, 제약, 실행, 조건 캡처와 연결된 주문·체결 수")

        drilldownLabel.font = TerminalTheme.caption
        drilldownLabel.textColor = TerminalTheme.secondaryText
        drilldownLabel.maximumNumberOfLines = 0
        drilldownLabel.lineBreakMode = .byWordWrapping
        drilldownLabel.setAccessibilityLabel("판단 분류, 겹치는 이유, 발생 횟수만 있는 조건의 의미")
        listLabel.font = TerminalTheme.sectionHeading
        listLabel.textColor = .labelColor
        listLabel.setAccessibilityLabel("선택한 저장 기준의 판단 목록과 필터 결과")

        let overviewPanel = panelStack([overviewLabel, overviewDetailLabel])
        let split = makeAnalysisSplitView()
        analysisSplitView = split
        let pager = NSStackView(views: [previousPageButton, nextPageButton, NSView()])
        pager.orientation = .horizontal
        pager.alignment = .centerY
        pager.spacing = 8
        previousPageButton.target = self
        previousPageButton.action = #selector(selectPreviousPage)
        nextPageButton.target = self
        nextPageButton.action = #selector(selectNextPage)
        previousPageButton.setAccessibilityLabel("저장 판단 이전 페이지 조회")
        nextPageButton.setAccessibilityLabel("저장 판단 다음 페이지 조회")

        for view in [title, identityLabel, statusLabel, filtersRow(), overviewPanel, drilldownLabel, split, pager] {
            stack.addArrangedSubview(view)
            view.widthAnchor.constraint(equalTo: stack.widthAnchor).isActive = true
        }
        split.heightAnchor.constraint(greaterThanOrEqualToConstant: 360).isActive = true
        NSLayoutConstraint.activate([
            stack.leadingAnchor.constraint(equalTo: content.leadingAnchor, constant: 16),
            stack.trailingAnchor.constraint(equalTo: content.trailingAnchor, constant: -16),
            stack.topAnchor.constraint(equalTo: content.topAnchor, constant: 12),
            stack.bottomAnchor.constraint(equalTo: content.bottomAnchor, constant: -12)
        ])
        window.contentView = content
    }

    private func configureFilters() {
        startField.placeholderString = "2026-10-18T00:00:00Z"
        endField.placeholderString = "2026-10-25T00:00:00Z"
        startField.font = TerminalTheme.monospacedDigitFont(ofSize: 11)
        endField.font = TerminalTheme.monospacedDigitFont(ofSize: 11)
        startField.setAccessibilityLabel("UTC 분석 시작 시각 포함")
        endField.setAccessibilityLabel("UTC 분석 종료 시각 미포함")
        startField.delegate = self
        endField.delegate = self
        startField.widthAnchor.constraint(greaterThanOrEqualToConstant: 208).isActive = true
        endField.widthAnchor.constraint(greaterThanOrEqualToConstant: 208).isActive = true
        marketPopup.setAccessibilityLabel("판단 분석 시장 필터")
        configureButton(refreshButton, action: #selector(refreshAnalysis), help: "선택 기간 [시작, 종료) UTC의 저장 사실을 다시 조회합니다.")
        configureButton(pinButton, action: #selector(pinSelectedDecision), help: "현재 저장 판단의 세션, 정책, 기간과 기록 ID를 비교 기준으로 고정합니다.")
        configureButton(compareButton, action: #selector(compareSelectedDecision), help: "고정 기준이 있으면 그 판단과, 없으면 같은 세션·시장 이전 판단과 비교합니다.")
        refreshButton.setAccessibilityLabel("선택 기간 저장 판단을 새로 조회")
        pinButton.setAccessibilityLabel("선택한 저장 판단을 비교 기준으로 고정")
        compareButton.setAccessibilityLabel("선택 판단과 이전 또는 고정 판단 비교")
        configureButton(copyIdentityButton, action: #selector(copySelectedIdentity), help: "세션, 정책 리비전, 기간과 저장 기준의 전체 ID를 복사합니다. 개인정보 숨김이 켜져 있으면 복사하지 않습니다.")
        copyIdentityButton.setAccessibilityLabel("선택한 저장 자료와 정책 ID 복사")
        pinButton.isEnabled = false
        compareButton.isEnabled = false
    }

    private func configureTables() {
        drilldownTable.dataSource = self
        drilldownTable.delegate = self
        drilldownTable.setAccessibilityLabel("기간 전체 판단 분류, 이유와 조건 발생 집계")
        addColumns(to: drilldownTable, columns: [
            ("group", "분류 · 이유 · 조건", 290),
            ("count", "수", 112),
            ("unit", "단위", 150)
        ])

        decisionTable.dataSource = self
        decisionTable.delegate = self
        decisionTable.setAccessibilityLabel("저장 기준 고정 판단 목록")
        addColumns(to: decisionTable, columns: [
            ("time", "저장 판단 시각", 188),
            ("market", "시장", 96),
            ("action", "전략 → 제약", 142),
            ("execution", "실행 상태", 124),
            ("placement", "기간 배치", 180)
        ])
        TerminalTheme.prepareTable(drilldownTable)
        TerminalTheme.prepareTable(decisionTable)
    }

    private func configureTextViews() {
        explanationText.isEditable = false
        explanationText.isSelectable = true
        explanationText.font = TerminalTheme.monospacedFont(ofSize: 11)
        explanationText.textColor = .labelColor
        explanationText.backgroundColor = TerminalTheme.chart
        explanationText.setAccessibilityLabel("저장된 판단 행동, 조건 평가, 이유, 주문과 체결")
        explanationScroll.configureWrapping(textView: explanationText)
        explanationScroll.drawsBackground = true
        explanationScroll.backgroundColor = TerminalTheme.chart
        explanationScroll.setAccessibilityLabel("원래 저장된 판단 설명의 스크롤 영역")

        comparisonText.isEditable = false
        comparisonText.isSelectable = true
        comparisonText.font = TerminalTheme.monospacedFont(ofSize: 11)
        comparisonText.textColor = .labelColor
        comparisonText.backgroundColor = TerminalTheme.chart
        comparisonText.setAccessibilityLabel("이전 또는 고정 판단과의 저장 사실 비교")
        comparisonScroll.configureWrapping(textView: comparisonText)
        comparisonScroll.drawsBackground = true
        comparisonScroll.backgroundColor = TerminalTheme.chart
        comparisonScroll.setAccessibilityLabel("판단 비교 결과의 스크롤 영역")
        configureButton(rawExplanationButton, action: #selector(toggleRawExplanation), help: "요약 설명과 원본 저장 JSON 표시를 전환합니다.")
        rawExplanationButton.setAccessibilityLabel("저장 판단 설명의 원본 JSON 표시 전환")
    }

    private func makeAnalysisSplitView() -> NSSplitView {
        let split = NSSplitView()
        split.isVertical = true
        split.dividerStyle = .thin
        split.translatesAutoresizingMaskIntoConstraints = false

        let left = NSView()
        let leftStack = NSStackView()
        leftStack.orientation = .vertical
        leftStack.alignment = .leading
        leftStack.spacing = 6
        leftStack.translatesAutoresizingMaskIntoConstraints = false
        left.addSubview(leftStack)
        NSLayoutConstraint.activate([
            leftStack.leadingAnchor.constraint(equalTo: left.leadingAnchor),
            leftStack.trailingAnchor.constraint(equalTo: left.trailingAnchor),
            leftStack.topAnchor.constraint(equalTo: left.topAnchor),
            leftStack.bottomAnchor.constraint(equalTo: left.bottomAnchor)
        ])

        let drilldownScroll = tableScroll(drilldownTable, minimumHeight: 170)
        let decisionScroll = tableScroll(decisionTable, minimumHeight: 190)
        leftStack.addArrangedSubview(drilldownScroll)
        leftStack.addArrangedSubview(listLabel)
        leftStack.addArrangedSubview(decisionScroll)
        for view in [drilldownScroll, listLabel, decisionScroll] {
            view.widthAnchor.constraint(equalTo: leftStack.widthAnchor).isActive = true
        }

        let right = NSView()
        let rightStack = NSStackView()
        rightStack.orientation = .vertical
        rightStack.alignment = .leading
        rightStack.spacing = 6
        rightStack.translatesAutoresizingMaskIntoConstraints = false
        right.addSubview(rightStack)
        NSLayoutConstraint.activate([
            rightStack.leadingAnchor.constraint(equalTo: right.leadingAnchor),
            rightStack.trailingAnchor.constraint(equalTo: right.trailingAnchor),
            rightStack.topAnchor.constraint(equalTo: right.topAnchor),
            rightStack.bottomAnchor.constraint(equalTo: right.bottomAnchor)
        ])

        let detailTitle = NSTextField(labelWithString: "저장 판단 설명")
        detailTitle.font = TerminalTheme.sectionHeading
        detailTitle.setAccessibilityLabel("저장 판단 설명")
        let compareTitle = NSTextField(labelWithString: "이전 또는 고정 판단 비교")
        compareTitle.font = TerminalTheme.sectionHeading
        compareTitle.setAccessibilityLabel("비교 기준과 달라진 저장 필드")
        for (view, minimumHeight) in [(explanationScroll as NSView, CGFloat(210)), (comparisonScroll as NSView, CGFloat(160))] {
            view.heightAnchor.constraint(greaterThanOrEqualToConstant: minimumHeight).isActive = true
            view.setContentHuggingPriority(.defaultLow, for: .vertical)
            view.setContentCompressionResistancePriority(.defaultLow, for: .vertical)
        }
        rightStack.addArrangedSubview(detailTitle)
        rightStack.addArrangedSubview(rawExplanationButton)
        rightStack.addArrangedSubview(explanationScroll)
        rightStack.addArrangedSubview(compareTitle)
        rightStack.addArrangedSubview(comparisonScroll)
        detailTitle.widthAnchor.constraint(equalTo: rightStack.widthAnchor).isActive = true
        compareTitle.widthAnchor.constraint(equalTo: rightStack.widthAnchor).isActive = true
        explanationScroll.widthAnchor.constraint(equalTo: rightStack.widthAnchor).isActive = true
        comparisonScroll.widthAnchor.constraint(equalTo: rightStack.widthAnchor).isActive = true

        split.addArrangedSubview(left)
        split.addArrangedSubview(right)
        left.widthAnchor.constraint(lessThanOrEqualTo: split.widthAnchor, multiplier: 0.58).isActive = true
        right.widthAnchor.constraint(greaterThanOrEqualToConstant: 360).isActive = true
        split.setHoldingPriority(.defaultLow, forSubviewAt: 0)
        split.setHoldingPriority(.defaultLow, forSubviewAt: 1)
        return split
    }

    private func applyBalancedSplitPosition() {
        guard let split = analysisSplitView, split.bounds.width > 0 else { return }
        let desiredLeftWidth = min(620, max(560, split.bounds.width - 380))
        split.setPosition(desiredLeftWidth, ofDividerAt: 0)
        split.layoutSubtreeIfNeeded()
    }

    private func filtersRow() -> NSView {
        let periodRow = NSStackView(views: [
            NSTextField(labelWithString: "시작 포함 · UTC"), startField,
            NSTextField(labelWithString: "종료 미포함 · UTC"), endField
        ])
        periodRow.orientation = TerminalTheme.textScale >= 1.5 ? .vertical : .horizontal
        periodRow.alignment = TerminalTheme.textScale >= 1.5 ? .leading : .centerY
        periodRow.spacing = 7

        let actionRow = NSStackView(views: [
            NSTextField(labelWithString: "시장"), marketPopup,
            refreshButton, pinButton, compareButton, copyIdentityButton
        ])
        actionRow.orientation = TerminalTheme.textScale >= 1.5 ? .vertical : .horizontal
        actionRow.alignment = TerminalTheme.textScale >= 1.5 ? .leading : .centerY
        actionRow.spacing = 7

        let rows = NSStackView(views: [periodRow, actionRow])
        rows.orientation = .vertical
        rows.alignment = .leading
        rows.spacing = 6
        rows.setAccessibilityLabel("분석 기간은 UTC 반개구간이며 기간과 시장 변경은 명시적으로 다시 조회합니다.")
        return rows
    }

    private func panelStack(_ views: [NSView]) -> NSView {
        let panel = NSBox()
        panel.boxType = .custom
        panel.cornerRadius = 8
        panel.borderWidth = 1
        panel.borderColor = TerminalTheme.border
        panel.fillColor = TerminalTheme.panel
        let stack = NSStackView(views: views)
        stack.orientation = .vertical
        stack.alignment = .leading
        stack.spacing = 5
        stack.translatesAutoresizingMaskIntoConstraints = false
        panel.addSubview(stack)
        NSLayoutConstraint.activate([
            stack.leadingAnchor.constraint(equalTo: panel.leadingAnchor, constant: 10),
            stack.trailingAnchor.constraint(equalTo: panel.trailingAnchor, constant: -10),
            stack.topAnchor.constraint(equalTo: panel.topAnchor, constant: 8),
            stack.bottomAnchor.constraint(equalTo: panel.bottomAnchor, constant: -8)
        ])
        for view in views { view.widthAnchor.constraint(equalTo: stack.widthAnchor).isActive = true }
        return panel
    }

    private func tableScroll(_ table: NSTableView, minimumHeight: CGFloat) -> NSScrollView {
        let scroll = NSScrollView()
        scroll.documentView = table
        scroll.hasVerticalScroller = true
        scroll.hasHorizontalScroller = true
        scroll.autohidesScrollers = true
        scroll.drawsBackground = false
        scroll.heightAnchor.constraint(greaterThanOrEqualToConstant: minimumHeight).isActive = true
        scroll.setContentHuggingPriority(.defaultLow, for: .vertical)
        scroll.setContentCompressionResistancePriority(.defaultLow, for: .vertical)
        return scroll
    }

    private func addColumns(to table: NSTableView, columns: [(String, String, CGFloat)]) {
        for (identifier, title, width) in columns {
            let column = NSTableColumn(identifier: NSUserInterfaceItemIdentifier(identifier))
            column.title = title
            column.width = width
            column.minWidth = width
            table.addTableColumn(column)
        }
    }

    private func configureButton(_ button: NSButton, action: Selector, help: String) {
        button.bezelStyle = .rounded
        button.target = self
        button.action = action
        button.toolTip = help
        button.setAccessibilityHelp(help)
    }

    private func populateMarketChoices(for context: DecisionAnalysisSessionContext) {
        marketPopup.removeAllItems()
        marketPopup.addItem(withTitle: "전체 시장")
        marketPopup.lastItem?.representedObject = nil
        for market in context.markets where !market.isEmpty {
            marketPopup.addItem(withTitle: market)
            marketPopup.lastItem?.representedObject = market
        }
        marketPopup.selectItem(at: 0)
        marketPopup.target = self
        marketPopup.action = #selector(marketSelectionChanged)
    }

    private func validatedPeriod() -> DecisionAnalysisPeriod? {
        let start = startField.stringValue.trimmingCharacters(in: .whitespacesAndNewlines)
        let end = endField.stringValue.trimmingCharacters(in: .whitespacesAndNewlines)
        guard start.hasSuffix("Z"), end.hasSuffix("Z"),
              let startInstant = DecisionAnalysisInstant.utc(start),
              let endInstant = DecisionAnalysisInstant.utc(end), startInstant < endInstant else {
            markStale("UTC 시작·종료 시각을 확인하세요. [시작, 종료) 형식이며 시작은 종료보다 앞서야 합니다.")
            statusLabel.textColor = TerminalTheme.negative
            return nil
        }
        return DecisionAnalysisPeriod(start: start, end: end)
    }

    private func loadDecisions(
        offset: UInt32,
        filter: DecisionAnalysisFilter?,
        context transportContext: NativeMCPTransportContext
    ) {
        guard let selection, let sessionContext, !isClosed else { return }
        if offset == 0, filter != pageFilter {
            isStale = !decisions.isEmpty
            statusLabel.stringValue = "선택한 요약의 판단 목록을 다시 읽습니다. 이전 목록은 잠시 유지하고 선택을 막습니다."
            statusLabel.textColor = TerminalTheme.secondaryText
            renderAll()
        }
        let ticket = beginRead()
        let request = service.decisions(
            selection: selection,
            page: (offset: offset, limit: Self.pageLimit),
            filter: filter,
            expectedMode: sessionContext.mode,
            expectedTransportContext: transportContext
        ) { [weak self] result in
            DispatchQueue.main.async {
                guard let self, self.accepts(ticket) else { return }
                self.activeReadRequest = nil
                switch result {
                case .failure(let error):
                    self.markStale(self.failureMessage(error))
                case .success(let value):
                    self.page = value.0
                    self.decisions = value.0.items
                    self.pageFilter = filter
                    self.population = selection.population
                    self.receivedAt = Date()
                    self.lastMeasurementText = Self.measurementLabel("판단 목록 조회", value.1)
                    self.selectedDecisionID = nil
                    self.selectedDetail = nil
                    self.comparison = nil
                    self.comparedSourceSnapshot = nil
                    self.isStale = false
                    self.statusLabel.stringValue = self.pageStatus(value.0, filter: filter)
                    self.statusLabel.textColor = TerminalTheme.secondaryText
                    self.renderAll()
                }
            }
        }
        retain(request, for: ticket)
    }

    private func selectDrilldown(_ row: DecisionAnalysisDrilldownRow) {
        guard !isStale, let context = currentAnalysisTransportContext() else { return }
        guard let filter = row.filter else {
            drilldownLabel.stringValue = row.help
            drilldownLabel.setAccessibilityValue(row.help)
            return
        }
        drilldownLabel.stringValue = row.help
        drilldownLabel.setAccessibilityValue(row.help)
        loadDecisions(offset: 0, filter: filter, context: context)
    }

    private func loadDecision(_ row: DecisionAnalysisDecisionRow) {
        guard !isStale, let selection, let sessionContext,
              let transportContext = currentAnalysisTransportContext() else { return }
        selectedDecisionID = row.decision_id
        statusLabel.stringValue = "선택한 저장 판단을 조회 중입니다. 기존 상세와 비교는 마지막 확인 결과로 유지합니다."
        statusLabel.textColor = TerminalTheme.secondaryText
        renderSelectionDetails()
        let ticket = beginRead()
        let request = service.decision(
            selection: selection,
            decisionID: row.decision_id,
            expectedMode: sessionContext.mode,
            expectedTransportContext: transportContext
        ) { [weak self] result in
            DispatchQueue.main.async {
                guard let self, self.accepts(ticket), self.selectedDecisionID == row.decision_id else { return }
                self.activeReadRequest = nil
                switch result {
                case .failure(let error):
                    self.markStale(self.failureMessage(error))
                case .success(let value):
                    self.selectedDetail = value.0
                    self.isShowingRawExplanation = false
                    self.receivedAt = Date()
                    self.lastMeasurementText = Self.measurementLabel("원본 판단 상세", value.1)
                    self.statusLabel.stringValue = "원래 저장 설명 확인 완료 · 비교 기준을 조회 중입니다."
                    self.statusLabel.textColor = TerminalTheme.secondaryText
                    self.renderSelectionDetails()
                    self.loadComparison(expectedTransportContext: transportContext)
                }
            }
        }
        retain(request, for: ticket)
    }

    private func loadComparison(expectedTransportContext: NativeMCPTransportContext) {
        guard !isStale, let selection, let sessionContext,
              let decisionID = selectedDecisionID else { return }
        let target: DecisionAnalysisComparisonTarget
        if let pinnedSelection, let pinnedDecisionID {
            target = .selected(selection: pinnedSelection, decisionID: pinnedDecisionID)
        } else {
            target = .previous
        }
        let ticket = beginRead()
        let request = service.compare(
            selection: selection,
            decisionID: decisionID,
            against: target,
            expectedMode: sessionContext.mode,
            expectedTransportContext: expectedTransportContext
        ) { [weak self] result in
            DispatchQueue.main.async {
                guard let self, self.accepts(ticket), self.selectedDecisionID == decisionID else { return }
                self.activeReadRequest = nil
                switch result {
                case .failure(let error):
                    self.markStale(self.failureMessage(error))
                case .success(let value):
                    self.comparison = value.0
                    self.comparedSourceSnapshot = value.1
                    self.receivedAt = Date()
                    self.lastMeasurementText = Self.measurementLabel("판단 비교", value.2)
                    self.statusLabel.stringValue = self.targetLabel(target)
                    self.statusLabel.textColor = TerminalTheme.secondaryText
                    self.renderSelectionDetails()
                }
            }
        }
        retain(request, for: ticket)
    }

    private func targetLabel(_ target: DecisionAnalysisComparisonTarget) -> String {
        let received = receivedAt.map { Self.compactUTCLabel($0) } ?? "미확인"
        let measurement = lastMeasurementText.map { " · \($0)" } ?? ""
        switch target {
        case .previous:
            let outsidePeriod: Bool
            if let selection, let comparedSourceSnapshot {
                outsidePeriod = !comparedSourceSnapshot.selection.population.period.matches(selection.population.period)
            } else {
                outsidePeriod = false
            }
            let periodNote = outsidePeriod ? "비교 기준은 선택 기간 이전 기록이며 기간 통계에 포함되지 않습니다. " : ""
            return "\(periodNote)같은 세션·시장의 직전 저장 판단과 비교했습니다. 목록의 이전 행은 사용하지 않습니다. · 수신 \(received)\(measurement)"
        case .selected:
            return "고정 판단의 세션·모드·정책·저장 기준을 함께 비교했습니다. · 수신 \(received)\(measurement)"
        }
    }

    private func beginRead() -> UInt64 {
        requestGeneration &+= 1
        activeReadRequest?.cancel()
        activeReadRequest = nil
        updateControls()
        return requestGeneration
    }

    private func accepts(_ ticket: UInt64) -> Bool {
        !isClosed && requestGeneration == ticket && window?.isVisible == true && window?.isMiniaturized == false
    }

    private func retain(_ request: NativeMCPRequest, for ticket: UInt64) {
        if accepts(ticket), request.isPending { activeReadRequest = request }
    }

    private func currentAnalysisTransportContext() -> NativeMCPTransportContext? {
        guard let analysisTransportContext else {
            markStale("저장 기준을 조회한 연결을 확인할 수 없습니다. 새로고침하세요.")
            return nil
        }
        guard service.isCurrentTransportContext(analysisTransportContext) else {
            markStale("Backend 연결이 바뀌었습니다. 마지막 확인 결과는 유지됩니다. 새로고침을 선택하세요.")
            return nil
        }
        return analysisTransportContext
    }

    private func cancelActiveRead() {
        requestGeneration &+= 1
        activeReadRequest?.cancel()
        activeReadRequest = nil
        updateControls()
    }

    private func markStale(_ message: String) {
        cancelActiveRead()
        isStale = overview != nil || page != nil || selectedDetail != nil
        statusLabel.stringValue = message
        statusLabel.textColor = TerminalTheme.negative
        renderAll()
    }

    private func failureMessage(_ error: Error) -> String {
        "조회가 완료되지 않았습니다 · \(error.localizedDescription) · 마지막 확인 결과는 유지됩니다. 기간 제한 오류이면 시작·종료 범위를 좁혀 다시 조회하세요."
    }

    private func pageStatus(_ page: DecisionAnalysisPage, filter: DecisionAnalysisFilter?) -> String {
        let filterText = filter.map(Self.filterLabel) ?? "전체 판단"
        let currentPage = page.returned_count == 0
            ? "현재 페이지 0건"
            : "현재 목록 \(UInt64(page.offset) + 1)부터 \(UInt64(page.offset) + UInt64(page.returned_count))까지 · \(page.returned_count)건"
        let received = receivedAt.map { Self.compactUTCLabel($0) } ?? "미확인"
        let measurement = lastMeasurementText.map { " · \($0)" } ?? ""
        return "\(filterText) · 목록 전체 \(page.total_count)건 (선택 기간 내 \(page.in_period_count) · 시각 미기록 \(page.unplaced_count)) · \(currentPage) · 수신 \(received)\(measurement)"
    }

    private func renderAll() {
        renderIdentity()
        renderOverview()
        renderSelectionDetails()
        drilldownTable.reloadData()
        decisionTable.reloadData()
        updateControls()
    }

    private func renderIdentity() {
        guard let context = sessionContext else {
            identityLabel.stringValue = "저장된 모의 또는 자동화 세션을 선택하세요."
            return
        }
        let masked = shouldMaskIdentity
        let sourceLabel: String
        switch context.source {
        case .paper(let sessionID):
            sourceLabel = "지속 모의 · 세션 \(Self.compactIdentity(sessionID, masked: masked))"
        case .automation(let sessionID):
            sourceLabel = "\(Self.modeLabel(context.mode)) · 세션 \(Self.compactIdentity(sessionID, masked: masked))"
        }
        let policy = selection?.population.policy_ref ?? context.policyReference
        let policyLabel = "정책 \(Self.compactIdentity(policy.policy_id, masked: masked)) · 리비전 \(Self.compactIdentity(policy.revision_id, masked: masked))"
        let populationLabel: String
        if let population {
            populationLabel = "시장 \(population.market ?? "전체 시장") · 기간 [\(population.period.start), \(population.period.end)) UTC"
        } else if let period {
            populationLabel = "시장 전체 · 기간 [\(period.start), \(period.end)) UTC"
        } else {
            populationLabel = "분석 조건 미확인"
        }
        let fenceStatus = selection == nil ? "저장 기준 조회 전" : "저장 기준 확인"
        let receivedLabel = receivedAt.map { "수신 \(Self.compactUTCLabel($0))" } ?? "수신 전"
        identityLabel.stringValue = "\(sourceLabel) · \(policyLabel)\n\(populationLabel)\n\(fenceStatus) · \(receivedLabel)"
        identityLabel.toolTip = masked ? AutomationReviewPrivacyPresentation.hiddenText : fullIdentityDetails(context: context, policy: policy)
        identityLabel.setAccessibilityValue(identityLabel.stringValue)
        identityLabel.setAccessibilityHelp("선택 기간은 UTC [시작, 종료)입니다. 전체 세션·정책·저장 기준 ID는 ‘저장 ID 복사’로 가져올 수 있습니다.")
    }

    private func fullIdentityDetails(context: DecisionAnalysisSessionContext, policy: DecisionAnalysisPolicyReference) -> String {
        var values = [
            "family=\(context.source.family)",
            "session_id=\(context.source.sessionID)",
            "mode=\(context.mode)",
            "policy_id=\(policy.policy_id)",
            "revision_id=\(policy.revision_id)",
            "definition_digest=\(policy.definition_digest)"
        ]
        if let population {
            values.append("market=\(population.market ?? "ALL")")
            values.append("period.start=\(population.period.start)")
            values.append("period.end=\(population.period.end)")
        }
        if let selection {
            values.append("snapshot_fence=\(Self.fenceIdentity(selection.fence))")
        }
        if let selectedDecisionID {
            values.append("decision_id=\(selectedDecisionID.displayIdentity)")
        }
        if let selectedDetail {
            values.append("saved_fact_digest=\(selectedDetail.row.saved_fact_digest)")
        }
        return values.joined(separator: "\n")
    }

    private func renderOverview() {
        guard let overview else {
            overviewLabel.stringValue = "기간 총계: 조회 전"
            overviewDetailLabel.stringValue = "정책을 재평가하지 않으며 저장된 판단·관측·연결 lifecycle 사실만 조회합니다."
            return
        }
        guard populationIsComplete else {
            overviewLabel.stringValue = "기간 전체 집계 불완전 · 총계 표시를 보류했습니다. 기간을 좁혀 다시 조회하세요."
            overviewDetailLabel.stringValue = "집계가 불완전하여 기간 총계와 분류를 표시하지 않습니다. 기간을 좁혀 다시 조회하세요."
            return
        }

        overviewLabel.stringValue = "기간 내 판단 \(overview.decisions)건 · 저장 판단 시각 확인 \(overview.decisions_with_time)건 · 시각 미기록/기간 밖 배치 불가 \(overview.unplaced_decisions)건"
        let strategy = Self.actionSummary("전략", overview.strategy)
        let constrained = Self.actionSummary("제약 후", overview.constrained)
        let execution = [
            "체결 \(overview.execution.filled)",
            "부분 체결 \(overview.execution.partially_filled)",
            "대기 \(overview.execution.pending)",
            "주문 없음 \(overview.execution.no_order)",
            "거부 \(overview.execution.rejected)",
            "미평가 \(overview.execution.not_evaluated)",
            "미기록 \(overview.execution.not_captured)"
        ].joined(separator: " · ")
        let capture = overview.condition_trace_capture
        let linked = "저장 판단에 연결된 주문 \(overview.orders)건 (판단 \(overview.decisions_with_orders)) · 체결 \(overview.fills)건 (판단 \(overview.decisions_with_fills))"
        let conditionCapture = "조건 기록 · 기록 \(capture.captured) · 일부 기록 \(capture.partial_capture) · 미기록 \(capture.not_captured) · 판단 외 \(capture.not_decision_event)"
        let observationText: String
        let visibleObservations = Self.observations(overview.non_decision_observations, selectedMarket: population?.market)
        if visibleObservations.isEmpty {
            observationText = "판단 외 기록 · 이 기간에 저장된 분류 관측 없음"
        } else {
            observationText = "판단 외 기록 · " + visibleObservations.map {
                "\(Self.observationScopeLabel($0.scope)) · \(DecisionAnalysisSavedExplanationPresentation.observationKindLabel($0.kind)) \($0.observations)건 · 시각 미기록 \($0.unplaced) · \(DecisionAnalysisSavedExplanationPresentation.captureLabelForDisplay($0.condition_trace_capture)) · \(DecisionAnalysisSavedExplanationPresentation.clockBasisLabel($0.basis_time))"
            }.joined(separator: " / ")
        }
        overviewDetailLabel.stringValue = [
            strategy,
            constrained,
            "실행 · \(execution)",
            linked,
            conditionCapture,
            observationText,
            "저장 시각은 판단 시각과 기록 종류별 원래 시각 기준을 사용합니다."
        ].joined(separator: "\n")
        overviewDetailLabel.setAccessibilityValue(overviewDetailLabel.stringValue)
    }

    private func renderSelectionDetails() {
        let masked = shouldMaskIdentity
        if masked { isShowingRawExplanation = false }
        rawExplanationButton.isHidden = selectedDetail == nil || masked
        if let detail = selectedDetail {
            let row = detail.row
            let title: String
            switch row.decision_id {
            case .paper(let sequence, let signalID):
                title = "저장 판단 · 모의 세션 · 이벤트 #\(sequence) · 신호 \(Self.compactIdentity(signalID, masked: masked))"
            case .automation(let intentID):
                title = "저장 판단 · 자동화 · intent \(Self.compactIdentity(intentID, masked: masked))"
            }
            let time = row.decision_time ?? "미기록 · 기간 포함 시각 알 수 없음"
            var headerLines: [String] = []
            if let selectedDecisionID, selectedDecisionID != row.decision_id {
                headerLines.append("선택한 판단 \(decisionIdentityLabel(selectedDecisionID, masked: masked))을 읽는 중 · 아래는 마지막으로 받은 판단 \(decisionIdentityLabel(row.decision_id, masked: masked))입니다.")
            }
            headerLines.append(contentsOf: [
                title,
                "시장 \(AutomationReviewPrivacyPresentation.identity(row.market, masked: masked)) · 저장 시각 \(time) · \(Self.placementLabel(row.period_placement))",
                "전략 \(DecisionAnalysisSavedExplanationPresentation.actionLabel(row.strategy_action)) → 제약 후 \(DecisionAnalysisSavedExplanationPresentation.actionLabel(row.constrained_action)) → 실행 \(DecisionAnalysisSavedExplanationPresentation.executionLabel(row.execution_state))",
                "저장 사실 ID · \(Self.compactIdentity(row.saved_fact_digest, masked: masked))"
            ])
            let header = headerLines.joined(separator: "\n")
            let explanation: String
            if isShowingRawExplanation && !masked {
                explanation = detail.explanation.redactedForDisplay(hidingMonetaryValues: false).formattedJSON
            } else {
                explanation = DecisionAnalysisSavedExplanationPresentation.lines(
                    for: detail.explanation,
                    masked: masked
                ).joined(separator: "\n")
            }
            explanationText.string = header + "\n\n저장된 설명 · 현재 정책/데이터로 재평가하지 않음\n" + explanation
            rawExplanationButton.title = isShowingRawExplanation ? "설명 요약 보기" : "원본 JSON 보기"
            rawExplanationButton.toolTip = isShowingRawExplanation
                ? "기록 필드를 읽기 쉬운 한국어 설명으로 전환합니다."
                : "저장된 설명 객체를 JSON으로 표시합니다. 개인정보 숨김이 켜져 있으면 표시하지 않습니다."
        } else if let selectedDecisionID {
            explanationText.string = "\(decisionIdentityLabel(selectedDecisionID, masked: masked)) · 원래 저장 설명을 읽는 중입니다."
        } else {
            explanationText.string = "목록에서 한 판단을 선택하면 저장된 행동, 평가 결과, 비교 입력, 이유와 연결 주문·체결을 읽기 쉬운 설명으로 표시합니다."
        }
        explanationText.setAccessibilityValue(explanationText.string)

        if let comparison {
            comparisonText.string = comparisonLines(comparison, masked: masked).joined(separator: "\n")
        } else if selectedDecisionID != nil {
            if let pinnedDecisionID {
                comparisonText.string = "고정 비교 기준 · \(decisionIdentityLabel(pinnedDecisionID, masked: masked))\n두 판단의 세션, 모드, 정책, 기간과 저장 기준을 각각 표시합니다."
            } else {
                comparisonText.string = "같은 세션·시장 안에서 목록 필터와 무관한 직전 저장 판단을 조회합니다."
            }
        } else {
            comparisonText.string = "선택 판단의 비교를 표시합니다. 기간 밖 또는 시각 미기록 판단을 임의로 이전 기록으로 지정하지 않습니다."
        }
        comparisonText.setAccessibilityValue(comparisonText.string)
        updateControls()
    }

    private func comparisonLines(_ comparison: DecisionAnalysisComparison, masked: Bool) -> [String] {
        let selected = comparison.selected
        var lines = ["비교 기준 · \(comparison.kind == "previous" ? "같은 세션·시장의 직전 판단" : "고정한 판단")"]
        if let compared = comparison.compared {
            if comparison.kind == "previous",
               let selection,
               let comparedSourceSnapshot,
               !comparedSourceSnapshot.selection.population.period.matches(selection.population.period) {
                lines.append("비교 기준은 선택 기간 이전 기록이며 기간 통계에 포함되지 않습니다.")
            }
            lines.append("달라진 저장 사실 · \(comparison.changed_fields.isEmpty ? "없음" : comparison.changed_fields.map(Self.changedFieldLabel).joined(separator: ", "))")
            lines.append(contentsOf: comparisonSideLines(
                "선택한 판단",
                detail: selected,
                source: selection?.population.source,
                mode: sessionContext?.mode,
                policy: selection?.population.policy_ref ?? sessionContext?.policyReference,
                population: selection?.population,
                masked: masked
            ))
            lines.append(contentsOf: comparisonSideLines(
                "비교한 판단",
                detail: compared,
                source: comparedSourceSnapshot?.selection.population.source,
                mode: comparedSourceSnapshot?.mode,
                policy: comparedSourceSnapshot?.policy_ref,
                population: comparedSourceSnapshot?.selection.population,
                masked: masked
            ))
        } else {
            let reason = comparison.unavailable_reason.map(Self.unavailableReasonLabel) ?? "비교 근거를 확인할 수 없음"
            lines.append("비교 불가 · \(reason)")
            lines.append("달라진 저장 사실 · 비교 대상 없음")
            lines.append(contentsOf: comparisonSideLines(
                "선택한 판단",
                detail: selected,
                source: selection?.population.source,
                mode: sessionContext?.mode,
                policy: selection?.population.policy_ref ?? sessionContext?.policyReference,
                population: selection?.population,
                masked: masked
            ))
        }
        return lines
    }

    private func comparisonSideLines(
        _ title: String,
        detail: DecisionAnalysisDecisionDetail,
        source: DecisionAnalysisSource?,
        mode: String?,
        policy: DecisionAnalysisPolicyReference?,
        population: DecisionAnalysisPopulation?,
        masked: Bool
    ) -> [String] {
        var lines = ["\(title) · \(decisionIdentityLabel(detail.row.decision_id, masked: masked)) · \(detail.row.decision_time ?? "시각 미기록")"]
        if let source, let mode, let policy, let population {
            lines.append(sourceContextLabel(
                source: source,
                mode: mode,
                policy: policy,
                population: population,
                market: detail.row.market,
                masked: masked
            ))
        }
        lines.append(contentsOf: DecisionAnalysisSavedExplanationPresentation.comparisonFacts(
            for: detail,
            masked: masked
        ).map { "  \($0)" })
        return lines
    }

    private func sourceContextLabel(
        source: DecisionAnalysisSource,
        mode: String,
        policy: DecisionAnalysisPolicyReference,
        population: DecisionAnalysisPopulation,
        market: String,
        masked: Bool
    ) -> String {
        "\(Self.modeLabel(mode)) · 세션 \(Self.compactIdentity(source.sessionID, masked: masked)) · 시장 \(market) · 정책 \(Self.compactIdentity(policy.policy_id, masked: masked))/\(Self.compactIdentity(policy.revision_id, masked: masked)) · [\(population.period.start), \(population.period.end)) UTC"
    }

    private func updateControls() {
        let isPending = activeReadRequest?.isPending == true
        let hasSelection = selection != nil
        refreshButton.isEnabled = sessionContext != nil && !isPending
        marketPopup.isEnabled = sessionContext != nil
        startField.isEnabled = sessionContext != nil
        endField.isEnabled = sessionContext != nil
        drilldownTable.isEnabled = populationIsComplete && !isStale && !isPending
        decisionTable.isEnabled = hasSelection && !isStale && !isPending
        previousPageButton.isEnabled = !isStale && !isPending && (page?.offset ?? 0) > 0
        nextPageButton.isEnabled = !isStale && !isPending && page?.next_offset != nil
        let detailMatchesSelection = selectedDetail?.row.decision_id == selectedDecisionID
        pinButton.isEnabled = !isStale && !isPending && selectedDecisionID != nil && detailMatchesSelection
        compareButton.isEnabled = !isStale && !isPending && selectedDecisionID != nil && detailMatchesSelection
        rawExplanationButton.isEnabled = !isStale && !isPending && selectedDetail != nil && !shouldMaskIdentity
        copyIdentityButton.isEnabled = sessionContext != nil && !shouldMaskIdentity
        pinButton.title = pinnedDecisionID == nil ? "선택 판단 고정" : "선택 판단 고정됨"
        compareButton.title = pinnedDecisionID == nil ? "이전 판단과 비교" : "고정 판단과 비교"
        compareButton.setAccessibilityLabel(compareButton.title)
    }

    private static func defaultPeriod(endingAt end: Date) -> DecisionAnalysisPeriod {
        let start = end.addingTimeInterval(-7 * 24 * 60 * 60)
        return DecisionAnalysisPeriod(start: utcLabel(start), end: utcLabel(end))
    }

    private static func utcLabel(_ date: Date) -> String {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        return formatter.string(from: date)
    }

    private static func compactUTCLabel(_ date: Date) -> String {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime]
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        return formatter.string(from: date)
    }

    private static func compactIdentity(_ value: String, masked: Bool) -> String {
        guard !masked else { return AutomationReviewPrivacyPresentation.hiddenText }
        guard value.count > 16 else { return value }
        return String(value.prefix(12)) + "…"
    }

    private static func modeLabel(_ value: String) -> String {
        switch value {
        case "SIMULATED": "지속 모의"
        case "MOCK": "자동화 모의"
        case "LIVE": "자동화 실계좌"
        default: "모드 확인 필요"
        }
    }

    private static func fenceIdentity(_ fence: DecisionAnalysisFence) -> String {
        switch fence {
        case .paper(let sequence): "PAPER sequence \(sequence)"
        case .automation(let digest): "automation digest \(digest)"
        }
    }

    private static func placementLabel(_ value: DecisionAnalysisPeriodPlacement) -> String {
        switch value {
        case .inPeriod: "선택 기간 내"
        case .unplaced: "판단 시각 미기록 · 기간 포함 확인 불가"
        }
    }

    private static func unavailableReasonLabel(_ value: DecisionAnalysisUnavailableReason) -> String {
        switch value {
        case .noPreviousDecision: "같은 세션·시장·저장 기준 안에 확인 가능한 앞선 판단이 없습니다."
        case .missingDecisionTime: "선택 판단의 저장 시간이 없어 이전 판단을 시간 순서로 정할 수 없습니다."
        }
    }

    private static func measurementLabel(_ action: String, _ measurement: DecisionAnalysisMeasurement) -> String {
        "\(action) · 처리 \(measurement.elapsed_millis) ms · \(measurement.response_bytes) B"
    }

    private static func actionSummary(_ label: String, _ counts: DecisionAnalysisActionCounts) -> String {
        "\(label) · 매수 \(counts.buy) · 매도 \(counts.sell) · 유지 \(counts.hold) · 미평가 \(counts.not_evaluated) · 미기록 \(counts.not_captured)"
    }

    private static func filterLabel(_ filter: DecisionAnalysisFilter) -> String {
        var fields: [String] = []
        if let value = filter.strategy_action {
            fields.append("전략 \(DecisionAnalysisSavedExplanationPresentation.actionLabel(value))")
        }
        if let value = filter.constrained_action {
            fields.append("제약 후 \(DecisionAnalysisSavedExplanationPresentation.actionLabel(value))")
        }
        if let value = filter.execution_state {
            fields.append("실행 \(DecisionAnalysisSavedExplanationPresentation.executionLabel(value))")
        }
        if let stage = filter.stage, filter.reason_code != nil {
            fields.append("저장 이유 · \(DecisionAnalysisSavedExplanationPresentation.stageLabel(stage))")
        }
        return fields.joined(separator: " + ")
    }

    private func decisionIdentityLabel(_ identity: DecisionAnalysisDecisionID, masked: Bool) -> String {
        switch identity {
        case .paper(let sequence, let signalID):
            "모의 판단 #\(sequence) · 신호 \(Self.compactIdentity(signalID, masked: masked))"
        case .automation(let intentID):
            "자동화 판단 · \(Self.compactIdentity(intentID, masked: masked))"
        }
    }

    private static func changedFieldLabel(_ field: String) -> String {
        switch field {
        case "source_family": "저장 자료 종류"
        case "session_id": "세션"
        case "mode": "세션 모드"
        case "policy_ref": "동결 정책 리비전"
        case "market": "시장"
        case "decision_time": "저장 판단 시각"
        case "strategy_action": "전략 행동"
        case "constrained_action": "제약 후 행동"
        case "execution_state": "실행 상태"
        case "actual_weight_at_decision": "판단 시 실제 비중"
        case "raw_target_weight": "원본 목표 비중"
        case "constrained_target_weight": "제약 후 목표 비중"
        case "matched_rule_id": "일치 규칙"
        case "condition_trace_capture": "조건 기록 상태"
        case "condition_trace": "저장 조건 내용"
        case "reasons": "저장 이유"
        case "orders": "연결 주문"
        case "fills": "연결 체결"
        default: field
        }
    }

    private static func makeDrilldownRows(
        from totals: DecisionAnalysisTotals,
        selectedMarket: String?
    ) -> [DecisionAnalysisDrilldownRow] {
        var rows: [DecisionAnalysisDrilldownRow] = []

        func appendActionRows(_ label: String, _ counts: DecisionAnalysisActionCounts, strategy: Bool) {
            for (action, count) in [
                ("BUY", counts.buy),
                ("SELL", counts.sell),
                ("HOLD", counts.hold),
                ("NOT_EVALUATED", counts.not_evaluated),
                ("NOT_CAPTURED", counts.not_captured)
            ] {
                let filter = strategy
                    ? DecisionAnalysisFilter(strategyAction: action)
                    : DecisionAnalysisFilter(constrainedAction: action)
                rows.append(DecisionAnalysisDrilldownRow(
                    title: "\(label) 행동 · \(DecisionAnalysisSavedExplanationPresentation.actionLabel(action))",
                    count: String(count),
                    unit: "기간 내 판단",
                    help: "기간 전체에서 \(label) 행동이 \(DecisionAnalysisSavedExplanationPresentation.actionLabel(action))으로 저장된 판단을 찾습니다. 원본 코드 \(action) · 시각 미기록 행은 별도 표시됩니다.",
                    filter: filter
                ))
            }
        }

        appendActionRows("전략", totals.strategy, strategy: true)
        appendActionRows("제약 후", totals.constrained, strategy: false)
        for (state, count) in [
            ("FILLED", totals.execution.filled),
            ("PARTIALLY_FILLED", totals.execution.partially_filled),
            ("PENDING", totals.execution.pending),
            ("REJECTED", totals.execution.rejected),
            ("NO_ORDER", totals.execution.no_order),
            ("CANCELLED", totals.execution.cancelled),
            ("EXPIRED", totals.execution.expired),
            ("NOT_EVALUATED", totals.execution.not_evaluated),
            ("NOT_CAPTURED", totals.execution.not_captured)
        ] {
            rows.append(DecisionAnalysisDrilldownRow(
                title: "실행 · \(DecisionAnalysisSavedExplanationPresentation.executionLabel(state))",
                count: String(count),
                unit: "판단",
                help: "\(DecisionAnalysisSavedExplanationPresentation.executionLabel(state))로 저장된 실행 상태입니다. 원본 코드 \(state) · 이 수는 주문·체결 건수와 다른 판단 수입니다.",
                filter: DecisionAnalysisFilter(executionState: state)
            ))
        }
        rows.append(contentsOf: totals.reasons.map { reason in
            DecisionAnalysisDrilldownRow(
                title: "저장 이유 · \(DecisionAnalysisSavedExplanationPresentation.stageLabel(reason.stage))",
                count: "\(reason.distinct_decisions) / \(reason.occurrences)",
                unit: "겹칠 수 있는 판단 / 발생",
                help: "원본 reason code \(reason.code) · 이유 그룹은 겹칠 수 있으므로 판단 총계와 합산하지 않습니다.",
                filter: DecisionAnalysisFilter(stage: reason.stage, reasonCode: reason.code)
            )
        })
        rows.append(contentsOf: totals.conditions.map { condition in
            DecisionAnalysisDrilldownRow(
                title: "조건 · \(DecisionAnalysisSavedExplanationPresentation.roleLabel(condition.role)) · \(DecisionAnalysisSavedExplanationPresentation.outcomeLabelForSummary(condition.outcome.state))",
                count: String(condition.occurrences),
                unit: "발생 · 판단 수 아님",
                help: "정책 리비전 \(condition.policy_ref.policy_id)/\(condition.policy_ref.revision_id) · 원본 위치 \(condition.ast_path) · scope \(condition.scope.formattedJSON) · 저장 결과 \(condition.outcome.state) · 사유 \(condition.outcome.reason ?? "미기록"). 조건 발생 수는 판단 선택기가 아닙니다.",
                filter: nil
            )
        })
        rows.append(contentsOf: observations(totals.non_decision_observations, selectedMarket: selectedMarket).map { observation in
            DecisionAnalysisDrilldownRow(
                title: "\(observationScopeLabel(observation.scope)) · \(DecisionAnalysisSavedExplanationPresentation.observationKindLabel(observation.kind)) · \(DecisionAnalysisSavedExplanationPresentation.captureLabelForDisplay(observation.condition_trace_capture))",
                count: "\(observation.observations) / \(observation.unplaced)",
                unit: "관측 / 시각 미기록",
                help: "원본 관측 종류 \(observation.kind) · 범위 \(Self.observationScopeCode(observation.scope)) · 캡처 \(observation.condition_trace_capture) · \(DecisionAnalysisSavedExplanationPresentation.clockBasisLabel(observation.basis_time)) · 시각 미기록 \(observation.unplaced)건은 기간에 배정하지 않았습니다.",
                filter: nil
            )
        })
        return rows
    }

    private static func observations(
        _ values: [DecisionAnalysisObservationCount],
        selectedMarket: String?
    ) -> [DecisionAnalysisObservationCount] {
        guard let selectedMarket else { return values }
        return values.filter { observation in
            switch observation.scope {
            case .market(let market): market == selectedMarket
            case .sessionWide, .unknown: true
            }
        }
    }

    private static func observationScopeLabel(_ scope: DecisionAnalysisObservationScope) -> String {
        switch scope {
        case .market(let market): "\(marketCaption(market)) 기록"
        case .sessionWide: "세션 전체 기록"
        case .unknown: "종목 범위 확인 불가"
        }
    }

    private static func observationScopeCode(_ scope: DecisionAnalysisObservationScope) -> String {
        switch scope {
        case .market(let market): "market=\(market)"
        case .sessionWide: "session_wide"
        case .unknown: "unknown"
        }
    }

    private static func marketCaption(_ market: String) -> String {
        switch market {
        case "KRW-BTC": "BTC"
        case "KRW-ETH": "ETH"
        case "KRW-SOL": "SOL"
        default: market
        }
    }
}
