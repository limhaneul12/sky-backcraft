import AppKit
import Foundation

enum DecisionAnalysisGUIFixture {
    static func runLayoutRegressions() throws {
        let application = NSApplication.shared
        application.setActivationPolicy(.accessory)
        try verifySourceWorkspaceEntryPoints()
        try verifyAnalysisWindow(
            family: "paper",
            mode: "SIMULATED",
            privacyIsMasked: false,
            staleDecisionList: false,
            previousBeforePeriod: true,
            textScale: 1.0,
            selectDecision: true
        )
        try verifyAnalysisWindow(
            family: "automation",
            mode: "LIVE",
            privacyIsMasked: true,
            staleDecisionList: false,
            previousBeforePeriod: false,
            textScale: 2.0,
            selectDecision: true
        )
        try verifyAnalysisWindow(
            family: "automation",
            mode: "MOCK",
            privacyIsMasked: false,
            staleDecisionList: true,
            previousBeforePeriod: false,
            textScale: 1.0,
            selectDecision: false
        )
        try verifyNoPreviousComparisonSummaryAppearsFirst()
        let capturedTrace = try capturedRustConditionTrace()
        try verifyAnalysisWindow(
            family: "paper",
            mode: "SIMULATED",
            privacyIsMasked: false,
            staleDecisionList: false,
            previousBeforePeriod: false,
            textScale: 1.0,
            selectDecision: true,
            conditionTraceOverride: capturedTrace
        )
        try verifyIncompleteOverviewDoesNotRenderGroupRows()
        try verifyNewOverviewClearsPreviousPageRows()
        try verifyObservationScopesForSelectedMarket()
        print("PASS paper and automation workspaces expose the native 판단 분석 entry")
        print("PASS 1200x800 and resized analysis windows keep period, summary, list and explanation controls reachable")
        print("PASS original explanation scroll document has nonzero bounds and laid-out glyphs intersecting its clip")
        print("PASS LIVE privacy presentation masks source identity and saved explanation details")
        print("PASS mismatched follow-up snapshot retains overview values as stale")
        print("PASS incomplete overview hides grouped totals and new overview clears prior list rows")
        print("PASS market-scoped observations exclude other markets and preserve session-wide/unknown scopes")
        print("PASS no-previous comparison summary appears before per-side facts without duplicating the long trace")
        print("PASS captured Rust condition outcomes and evidence render without invented missing-result warnings")
    }

    private static func capturedRustConditionTrace() throws -> [[String: Any]] {
        guard let directory = ProcessInfo.processInfo.environment["SKY_BACKCRAFT_DECISION_ANALYSIS_WIRE_DIRECTORY"] else {
            throw DecisionAnalysisFixtureFailure.assertion("native gate did not supply the required Rust wire directory")
        }
        let url = URL(fileURLWithPath: directory, isDirectory: true)
            .appendingPathComponent("paper-captured-decision.json")
        let data = try Data(contentsOf: url)
        guard let response = try JSONSerialization.jsonObject(with: data) as? [String: Any],
              let decision = response["decision"] as? [String: Any],
              let explanation = decision["explanation"] as? [String: Any],
              let trace = explanation["condition_trace"] as? [[String: Any]],
              !trace.isEmpty else {
            throw DecisionAnalysisFixtureFailure.assertion("captured Rust detail has no condition trace")
        }
        return trace
    }

    private static func verifyNoPreviousComparisonSummaryAppearsFirst() throws {
        TerminalTheme.setTextScale(1.0)
        let environment = try DecisionAnalysisFixtureWire.fixtureEnvironment(
            mode: "SIMULATED",
            handler: DecisionAnalysisFixtureWire.handler(mode: "SIMULATED")
        )
        let controller = DecisionAnalysisWindowController(service: environment.1)
        controller.showAnalysis(for: DecisionAnalysisFixtureWire.context(
            family: "paper",
            sessionID: "gui-no-previous-summary-paper",
            mode: "SIMULATED"
        ))
        guard let window = controller.window, let root = window.contentView,
              let status = findLabel("분석 조회 상태와 마지막 확인 결과 안내", in: root),
              let decisionsTable = findTable("저장 기준 고정 판단 목록", in: root),
              let explanation = findTextView("저장된 판단 행동, 조건 평가, 이유, 주문과 체결", in: root),
              let comparison = findTextView("이전 또는 고정 판단과의 저장 사실 비교", in: root) else {
            throw DecisionAnalysisFixtureFailure.assertion("no-previous comparison fixture controls did not construct")
        }
        try DecisionAnalysisFixtureAssertions.waitUntil("long no-previous decision page") {
            status.stringValue.contains("목록 전체 2건") && decisionsTable.numberOfRows == 2
        }
        decisionsTable.selectRowIndexes(IndexSet(integer: 0), byExtendingSelection: false)
        controller.tableViewSelectionDidChange(Notification(name: NSTableView.selectionDidChangeNotification, object: decisionsTable))
        try DecisionAnalysisFixtureAssertions.waitUntil("no-previous comparison summary") {
            comparison.string.contains("비교 불가")
        }

        let lines = comparison.string.components(separatedBy: "\n")
        let unavailableIndex = lines.firstIndex(where: { $0.hasPrefix("비교 불가 · 같은 세션·시장·저장 기준") })
        let changedIndex = lines.firstIndex(of: "달라진 저장 사실 · 비교 대상 없음")
        let actionIndex = lines.firstIndex(where: { $0.hasPrefix("  행동 ·") })
        let weightIndex = lines.firstIndex(where: { $0.hasPrefix("  비중 ·") })
        let reasonIndex = lines.firstIndex(where: { $0.hasPrefix("  저장 사유 ·") })
        try DecisionAnalysisFixtureAssertions.expect(
            unavailableIndex != nil && changedIndex != nil && actionIndex != nil && weightIndex != nil && reasonIndex != nil
                && unavailableIndex! < changedIndex! && changedIndex! < actionIndex!
                && actionIndex! < weightIndex! && weightIndex! < reasonIndex!,
            "no-previous reason, unavailable delta and compact selected facts must appear in order at the top"
        )
        try DecisionAnalysisFixtureAssertions.expect(explanation.string.contains("조건 32 ·")
                                                       && !comparison.string.contains("조건 32 ·")
                                                       && !comparison.string.contains("기간·시장 범위 안에"),
                                                     "comparison must not bury its conclusion under the long trace or imply period-only predecessor bounds")
        window.close()
        environment.0.disconnect()
    }

    private static func verifyIncompleteOverviewDoesNotRenderGroupRows() throws {
        TerminalTheme.setTextScale(1.0)
        let environment = try DecisionAnalysisFixtureWire.fixtureEnvironment(
            mode: "SIMULATED",
            handler: DecisionAnalysisFixtureWire.handler(mode: "SIMULATED", incompleteOverview: true)
        )
        let controller = DecisionAnalysisWindowController(service: environment.1)
        controller.showAnalysis(for: DecisionAnalysisFixtureWire.context(
            family: "paper",
            sessionID: "gui-incomplete-paper",
            mode: "SIMULATED"
        ))
        guard let window = controller.window, let root = window.contentView,
              let overview = findLabel("선택 기간 내 판단 총계와 미배치 판단 수", in: root),
              let summaryTable = findTable("기간 전체 판단 분류, 이유와 조건 발생 집계", in: root),
              let decisionsTable = findTable("저장 기준 고정 판단 목록", in: root),
              let status = findLabel("분석 조회 상태와 마지막 확인 결과 안내", in: root) else {
            throw DecisionAnalysisFixtureFailure.assertion("incomplete overview controls did not construct")
        }
        try DecisionAnalysisFixtureAssertions.waitUntil("incomplete period response") {
            status.stringValue.contains("완전하게 집계하지 못했습니다")
        }
        try DecisionAnalysisFixtureAssertions.expect(overview.stringValue.contains("집계 불완전")
                                                       && summaryTable.numberOfRows == 0
                                                       && decisionsTable.numberOfRows == 0,
                                                     "incomplete population must not expose any grouped totals or decision rows")
        try DecisionAnalysisFixtureAssertions.expect(DecisionAnalysisFixtureURLProtocol.capturedToolCalls().map(\.action) == ["overview"],
                                                     "incomplete overview must not start a paged decision request")
        window.close()
        environment.0.disconnect()
    }

    private static func verifyNewOverviewClearsPreviousPageRows() throws {
        TerminalTheme.setTextScale(1.0)
        let environment = try DecisionAnalysisFixtureWire.fixtureEnvironment(
            mode: "SIMULATED",
            handler: DecisionAnalysisFixtureWire.handler(mode: "SIMULATED")
        )
        let controller = DecisionAnalysisWindowController(service: environment.1)
        controller.showAnalysis(for: DecisionAnalysisFixtureWire.context(
            family: "paper",
            sessionID: "gui-replaced-page-paper",
            mode: "SIMULATED"
        ))
        guard let window = controller.window, let root = window.contentView,
              let status = findLabel("분석 조회 상태와 마지막 확인 결과 안내", in: root),
              let decisionsTable = findTable("저장 기준 고정 판단 목록", in: root),
              let summaryTable = findTable("기간 전체 판단 분류, 이유와 조건 발생 집계", in: root),
              let refresh = descendants(of: root).compactMap({ $0 as? NSButton }).first(where: { $0.title == "조회 / 새로고침" }) else {
            throw DecisionAnalysisFixtureFailure.assertion("page replacement controls did not construct")
        }
        try DecisionAnalysisFixtureAssertions.waitUntil("initial decision page") {
            status.stringValue.contains("목록 전체 2건") && decisionsTable.numberOfRows == 2
        }

        DecisionAnalysisFixtureURLProtocol.install(handler: DecisionAnalysisFixtureWire.handler(
            mode: "SIMULATED",
            heldAction: "decisions"
        ))
        refresh.performClick(nil)
        try DecisionAnalysisFixtureAssertions.waitUntil("replacement overview and pending page") {
            let calls = DecisionAnalysisFixtureURLProtocol.capturedToolCalls()
            return calls.map(\.action) == ["overview", "decisions"]
        }
        try DecisionAnalysisFixtureAssertions.expect(decisionsTable.numberOfRows == 0
                                                       && !decisionsTable.isEnabled
                                                       && summaryTable.numberOfRows >= 20,
                                                     "a fresh overview must clear old decision rows while its replacement page is pending")
        window.close()
        environment.0.disconnect()
    }

    private static func verifyObservationScopesForSelectedMarket() throws {
        TerminalTheme.setTextScale(1.0)
        let environment = try DecisionAnalysisFixtureWire.fixtureEnvironment(
            mode: "SIMULATED",
            handler: DecisionAnalysisFixtureWire.handler(mode: "SIMULATED")
        )
        let controller = DecisionAnalysisWindowController(service: environment.1)
        controller.showAnalysis(for: DecisionAnalysisFixtureWire.context(
            family: "paper",
            sessionID: "gui-observation-scope-paper",
            mode: "SIMULATED"
        ))
        guard let window = controller.window, let root = window.contentView,
              let status = findLabel("분석 조회 상태와 마지막 확인 결과 안내", in: root),
              let identity = findLabel("선택한 저장 세션, 모드, 동결 정책과 분석 기간", in: root),
              let summaryTable = findTable("기간 전체 판단 분류, 이유와 조건 발생 집계", in: root),
              let marketPopup = descendants(of: root).compactMap({ $0 as? NSPopUpButton }).first(where: {
                  $0.accessibilityLabel() == "판단 분석 시장 필터"
              }),
              let refresh = descendants(of: root).compactMap({ $0 as? NSButton }).first(where: {
                  $0.title == "조회 / 새로고침"
              }) else {
            throw DecisionAnalysisFixtureFailure.assertion("observation-scope filter controls did not construct")
        }
        try DecisionAnalysisFixtureAssertions.waitUntil("initial observation scope overview") {
            status.stringValue.contains("목록 전체 2건")
        }
        marketPopup.selectItem(withTitle: "KRW-BTC")
        guard let marketAction = marketPopup.action else {
            throw DecisionAnalysisFixtureFailure.assertion("market filter action is missing")
        }
        _ = NSApp.sendAction(marketAction, to: marketPopup.target, from: marketPopup)
        refresh.performClick(nil)
        try DecisionAnalysisFixtureAssertions.waitUntil("BTC scoped observation overview") {
            identity.stringValue.contains("시장 KRW-BTC") && status.stringValue.contains("목록 전체 2건")
        }

        let groupColumn = summaryTable.tableColumns.first { $0.identifier.rawValue == "group" }
        guard let groupColumn else { throw DecisionAnalysisFixtureFailure.assertion("summary group column is missing") }
        let titles = (0..<summaryTable.numberOfRows).compactMap {
            (controller.tableView(summaryTable, viewFor: groupColumn, row: $0) as? NSTextField)?.stringValue
        }
        try DecisionAnalysisFixtureAssertions.expect(titles.contains(where: { $0.contains("BTC 기록") })
                                                       && titles.contains(where: { $0.contains("세션 전체 기록") })
                                                       && titles.contains(where: { $0.contains("종목 범위 확인 불가") })
                                                       && !titles.contains(where: { $0.contains("ETH 기록") }),
                                                     "market filter must exclude ETH observations but retain and label session-wide/unknown scope")
        window.close()
        environment.0.disconnect()
    }

    static func runAppearanceDemo() throws {
        let application = NSApplication.shared
        application.setActivationPolicy(.regular)
        let mode = CommandLine.arguments.contains("--automation") ? "LIVE" : "SIMULATED"
        let family = mode == "SIMULATED" ? "paper" : "automation"
        let environment = try DecisionAnalysisFixtureWire.fixtureEnvironment(
            mode: mode,
            handler: DecisionAnalysisFixtureWire.handler(mode: mode)
        )
        let controller = DecisionAnalysisWindowController(
            service: environment.1,
            privacyIsMasked: { CommandLine.arguments.contains("--masked") }
        )
        let context = DecisionAnalysisFixtureWire.context(
            family: family,
            sessionID: family == "paper" ? "gui-paper-fixture" : "gui-live-fixture",
            mode: mode
        )
        let delegate = AppearanceFixtureDelegate()
        application.delegate = delegate
        controller.showAnalysis(for: context)
        withExtendedLifetime((controller, delegate, environment.0)) {
            application.run()
        }
    }

    private static func verifySourceWorkspaceEntryPoints() throws {
        TerminalTheme.setTextScale(1.0)
        let environment = try DecisionAnalysisFixtureWire.fixtureEnvironment(
            mode: "SIMULATED",
            handler: DecisionAnalysisFixtureWire.handler(mode: "SIMULATED")
        )
        let paper = PaperTradingWorkspaceView(
            service: PaperSessionMCPService(client: environment.0),
            jobsQueueView: NSView(),
            onSummaryChanged: { _ in }
        )
        let automation = AutomationWorkspaceView(
            policies: [],
            service: AutomationSessionMCPService(client: environment.0),
            realtimeFeedService: RealtimeFeedMCPService.shared,
            policyReader: PolicyDetailMCPReader(client: environment.0),
            privacyIsMasked: { false },
            onTogglePrivacyMask: {},
            onSummaryChanged: { _ in }
        )
        let paperButton = descendants(of: paper).compactMap { $0 as? NSButton }.first {
            $0.title == "판단 분석"
        }
        let automationButton = descendants(of: automation).compactMap { $0 as? NSButton }.first {
            $0.title == "판단 분석"
        }
        try DecisionAnalysisFixtureAssertions.expect(
            paperButton?.accessibilityLabel() == "선택한 지속 PAPER 세션의 저장 판단 분석 열기",
            "PAPER workspace decision-analysis entry and accessibility label must exist"
        )
        try DecisionAnalysisFixtureAssertions.expect(
            automationButton?.accessibilityLabel() == "선택한 전략 세션의 저장 판단 분석 열기",
            "automation workspace decision-analysis entry and accessibility label must exist"
        )
        try DecisionAnalysisFixtureAssertions.expect(DecisionAnalysisFixtureURLProtocol.capturedToolCalls().isEmpty,
                                             "constructing workspace entry points must not query sessions or runtime state")
        environment.0.disconnect()
    }

    private static func verifyAnalysisWindow(
        family: String,
        mode: String,
        privacyIsMasked: Bool,
        staleDecisionList: Bool,
        previousBeforePeriod: Bool,
        textScale: CGFloat,
        selectDecision: Bool,
        conditionTraceOverride: [[String: Any]]? = nil
    ) throws {
        TerminalTheme.setTextScale(textScale)
        let environment = try DecisionAnalysisFixtureWire.fixtureEnvironment(
            mode: mode,
            handler: DecisionAnalysisFixtureWire.handler(
                mode: mode,
                staleAction: staleDecisionList ? "decisions" : nil,
                previousBeforePeriod: previousBeforePeriod,
                conditionTraceOverride: conditionTraceOverride
            )
        )
        let controller = DecisionAnalysisWindowController(
            service: environment.1,
            privacyIsMasked: { privacyIsMasked }
        )
        controller.showAnalysis(for: DecisionAnalysisFixtureWire.context(
            family: family,
            sessionID: family == "paper" ? "gui-paper-session" : "gui-automation-session",
            mode: mode
        ))
        guard let window = controller.window, let root = window.contentView else {
            throw DecisionAnalysisFixtureFailure.assertion("decision-analysis window did not construct")
        }
        try DecisionAnalysisFixtureAssertions.expect(window.isVisible, "decision-analysis window must be visible after its explicit open action")
        guard let status = findLabel("분석 조회 상태와 마지막 확인 결과 안내", in: root),
              let overview = findLabel("선택 기간 내 판단 총계와 미배치 판단 수", in: root) else {
            throw DecisionAnalysisFixtureFailure.assertion("analysis status or overview label did not construct")
        }
        do {
            try DecisionAnalysisFixtureAssertions.waitUntil("decision-analysis overview or query error") {
                overview.stringValue.contains("기간 내 판단 1건") || status.stringValue.contains("조회가 완료되지 않았습니다")
            }
        } catch {
            let actions = DecisionAnalysisFixtureURLProtocol.capturedToolCalls().map { $0.action ?? "?" }
            throw DecisionAnalysisFixtureFailure.assertion("\(error) · status=\(status.stringValue) · calls=\(actions)")
        }
        guard overview.stringValue.contains("기간 내 판단 1건") else {
            throw DecisionAnalysisFixtureFailure.assertion(
                "analysis overview failed: \(status.stringValue) · fixture MCP calls \(DecisionAnalysisFixtureURLProtocol.capturedToolCalls().map(\.action))"
            )
        }
        root.layoutSubtreeIfNeeded()
        window.displayIfNeeded()

        guard let identity = findLabel("선택한 저장 세션, 모드, 동결 정책과 분석 기간", in: root),
              let summaryTable = findTable("기간 전체 판단 분류, 이유와 조건 발생 집계", in: root),
              let decisionsTable = findTable("저장 기준 고정 판단 목록", in: root),
              let explanation = findTextView("저장된 판단 행동, 조건 평가, 이유, 주문과 체결", in: root),
              let comparisonView = findTextView("이전 또는 고정 판단과의 저장 사실 비교", in: root),
              let copyIdentityButton = descendants(of: root).compactMap({ $0 as? NSButton }).first(where: {
                  $0.title == "저장 ID 복사"
              }) else {
            throw DecisionAnalysisFixtureFailure.assertion("analysis labels, summary table, decision table or explanation view did not construct")
        }
        let primaryButtons = ["조회 / 새로고침", "저장 ID 복사", "이전 페이지", "다음 페이지"].compactMap { title in
            descendants(of: root).compactMap { $0 as? NSButton }.first(where: { $0.title == title })
        }
        try DecisionAnalysisFixtureAssertions.expect(overview.stringValue.contains("시각 미기록/기간 밖 배치 불가 1건"),
                                             "period overview must display unplaced count outside its period total")
        try DecisionAnalysisFixtureAssertions.expect(identity.stringValue.contains("수신 ")
                                                && identity.stringValue.contains("저장 기준 확인"),
                                             "saved source and app receipt clock must be visible")
        try DecisionAnalysisFixtureAssertions.expect(summaryTable.numberOfRows >= 20,
                                             "overview must present action, execution, reason and condition groups")
        try DecisionAnalysisFixtureAssertions.expect(status.frame.width > 0 && overview.frame.width > 0,
                                             "status and period total labels must have visible bounds")
        let initialDetailWidth = explanation.enclosingScrollView?.frame.width ?? 0
        let initialComparisonWidth = comparisonView.enclosingScrollView?.frame.width ?? 0
        try DecisionAnalysisFixtureAssertions.expect(initialDetailWidth >= 360 && initialComparisonWidth >= 360,
                                                     "detail and comparison inspector widths are \(initialDetailWidth)/\(initialComparisonWidth), expected at least 360 logical points")
        try DecisionAnalysisFixtureAssertions.expect(copyIdentityButton.isEnabled != privacyIsMasked,
                                                     "full saved identity copy must follow the automation privacy mask")

        let callsBeforeConditionSelection = DecisionAnalysisFixtureURLProtocol.capturedToolCalls().count
        summaryTable.selectRowIndexes(IndexSet(integer: 20), byExtendingSelection: false)
        controller.tableViewSelectionDidChange(Notification(name: NSTableView.selectionDidChangeNotification, object: summaryTable))
        try DecisionAnalysisFixtureAssertions.expect(DecisionAnalysisFixtureURLProtocol.capturedToolCalls().count == callsBeforeConditionSelection,
                                             "occurrence-only condition groups must not act as decision selectors")

        if staleDecisionList {
            try DecisionAnalysisFixtureAssertions.waitUntil("stale follow-up status") {
                status.stringValue.contains("마지막 확인 결과")
            }
            try DecisionAnalysisFixtureAssertions.expect(overview.stringValue.contains("기간 내 판단 1건"),
                                                 "failed follow-up must retain the last overview totals")
        } else {
            summaryTable.selectRowIndexes(IndexSet(integer: 0), byExtendingSelection: false)
            controller.tableViewSelectionDidChange(Notification(name: NSTableView.selectionDidChangeNotification, object: summaryTable))
            try DecisionAnalysisFixtureAssertions.waitUntil("filtered decision list") {
                status.stringValue.contains("전략 매수")
            }
            let selected = decisionsTable.tableColumns.firstIndex {
                $0.identifier.rawValue == "placement"
            }!
            let unplacedCell = controller.tableView(decisionsTable, viewFor: decisionsTable.tableColumns[selected], row: 1) as? NSTextField
            try DecisionAnalysisFixtureAssertions.expect(unplacedCell?.stringValue == "판단 시각 미기록 · 기간 포함 확인 불가",
                                                 "unplaced rows must not be labeled as in-period")

            if selectDecision {
                decisionsTable.selectRowIndexes(IndexSet(integer: 0), byExtendingSelection: false)
                controller.tableViewSelectionDidChange(Notification(name: NSTableView.selectionDidChangeNotification, object: decisionsTable))
                do {
                    try DecisionAnalysisFixtureAssertions.waitUntil("original saved explanation") {
                        explanation.string.contains("저장된 설명")
                    }
                } catch {
                    let actions = DecisionAnalysisFixtureURLProtocol.capturedToolCalls().map { $0.action ?? "?" }
                    throw DecisionAnalysisFixtureFailure.assertion("\(error) · status=\(status.stringValue) · detail=\(explanation.string.prefix(240)) · calls=\(actions)")
                }
                if privacyIsMasked {
                    try DecisionAnalysisFixtureAssertions.expect(explanation.string.contains("개인정보 숨김"),
                                                         "LIVE privacy mask must hide original condition operands")
                    try DecisionAnalysisFixtureAssertions.expect(identity.stringValue.contains("개인정보 숨김"),
                                                         "LIVE privacy mask must hide session and frozen policy identity")
                    try DecisionAnalysisFixtureAssertions.expect(!explanation.string.contains("101.25"),
                                                         "LIVE privacy mask must not reveal saved numeric operands")
                } else if conditionTraceOverride != nil {
                    try DecisionAnalysisFixtureAssertions.expect(
                        explanation.string.contains("조건 1 · 진입 · 충족")
                            && explanation.string.contains("0.25 > 0.2")
                            && explanation.string.contains("조건 2 · 진입 · 불충족")
                            && explanation.string.contains("0.1 > 0.3")
                            && explanation.string.contains("조건 3 · 진입 · 확인 불가")
                            && explanation.string.contains("이전 입력 미기록")
                            && explanation.string.contains("조건 4 · 진입 · 미평가")
                            && !explanation.string.contains("엔진 반환 · 미기록"),
                        "actual Rust trace must preserve typed result/evidence and omit an invented missing-result warning"
                    )
                } else {
                    try DecisionAnalysisFixtureAssertions.expect(explanation.string.contains("조건 1 · 진입 · 불충족")
                                                            && explanation.string.contains("101.2500000000000000000000001 > 99.8750000000000000000000001")
                                                            && explanation.string.contains("확인 불가")
                                                            && explanation.string.contains("미평가")
                                                            && explanation.string.contains("저장 false")
                                                            && explanation.string.contains("이전 입력 미기록 · 현재 입력 왼쪽 0.75 · 오른쪽 0.5")
                                                            && !explanation.string.contains("\"condition_trace\""),
                                                         "readable explanation must preserve exact decimal strings, crossing operands, tagged states and stored engine_result without recalculation · \(explanation.string.suffix(720))")
                }

                try DecisionAnalysisFixtureAssertions.waitUntil("previous decision comparison") {
                    descendants(of: root).compactMap { $0 as? NSTextView }
                        .first(where: { $0.accessibilityLabel() == "이전 또는 고정 판단과의 저장 사실 비교" })?
                        .string.contains("비교 불가") == true
                        || (previousBeforePeriod && descendants(of: root).compactMap { $0 as? NSTextView }
                            .first(where: { $0.accessibilityLabel() == "이전 또는 고정 판단과의 저장 사실 비교" })?
                            .string.contains("비교 기준은 선택 기간 이전 기록이며 기간 통계에 포함되지 않습니다.") == true)
                }
                if previousBeforePeriod {
                    let compareLines = comparisonView.string.components(separatedBy: "\n")
                    let contextNote = compareLines.firstIndex(of: "비교 기준은 선택 기간 이전 기록이며 기간 통계에 포함되지 않습니다.")
                    let changedFacts = compareLines.firstIndex(where: { $0.hasPrefix("달라진 저장 사실 ·") })
                    let selectedActions = compareLines.firstIndex(where: { $0.hasPrefix("  행동 ·") })
                    let selectedWeights = compareLines.firstIndex(where: { $0.hasPrefix("  비중 ·") })
                    try DecisionAnalysisFixtureAssertions.expect(
                        contextNote != nil && changedFacts != nil && selectedActions != nil && selectedWeights != nil
                            && contextNote! < changedFacts! && changedFacts! < selectedActions!
                            && selectedActions! < selectedWeights! && !comparisonView.string.contains("조건 1 ·"),
                        "pre-period note and changed-field conclusion must precede compact facts without duplicating saved traces"
                    )
                }
                try verifyExplanationScroll(explanation, window: window, root: root)

                if !privacyIsMasked {
                    guard let rawButton = descendants(of: root).compactMap({ $0 as? NSButton }).first(where: {
                        $0.title == "원본 JSON 보기"
                    }) else {
                        throw DecisionAnalysisFixtureFailure.assertion("secondary original-JSON disclosure button is missing")
                    }
                    rawButton.performClick(nil)
                    try DecisionAnalysisFixtureAssertions.expect(explanation.string.contains("\"condition_trace\""),
                                                                 "original saved JSON must remain available as a secondary disclosure")
                    guard let summaryButton = descendants(of: root).compactMap({ $0 as? NSButton }).first(where: {
                        $0.title == "설명 요약 보기"
                    }) else {
                        throw DecisionAnalysisFixtureFailure.assertion("readable explanation return action is missing")
                    }
                    summaryButton.performClick(nil)

                    if family == "paper" {
                        guard let pin = descendants(of: root).compactMap({ $0 as? NSButton }).first(where: {
                            $0.title == "선택 판단 고정"
                        }) else {
                            throw DecisionAnalysisFixtureFailure.assertion("explicit comparison pin button is missing")
                        }
                        pin.performClick(nil)
                        decisionsTable.selectRowIndexes(IndexSet(integer: 1), byExtendingSelection: false)
                        controller.tableViewSelectionDidChange(Notification(name: NSTableView.selectionDidChangeNotification, object: decisionsTable))
                        guard let comparisonView = findTextView("이전 또는 고정 판단과의 저장 사실 비교", in: root) else {
                            throw DecisionAnalysisFixtureFailure.assertion("comparison details view is missing")
                        }
                        try DecisionAnalysisFixtureAssertions.waitUntil("pinned selected-record comparison") {
                            comparisonView.string.contains("비교 기준 · 고정한 판단")
                                && comparisonView.string.contains("달라진 저장 사실")
                        }
                        try DecisionAnalysisFixtureAssertions.expect(explanation.string.contains("판단 시각 미기록 · 기간 포함 확인 불가"),
                                                                     "pinned unplaced row must remain visibly outside period placement")
                    }
                }
            }
        }

        window.setContentSize(NSSize(width: 980, height: 700))
        root.layoutSubtreeIfNeeded()
        window.displayIfNeeded()
        try DecisionAnalysisFixtureAssertions.expect(root.bounds.width > 0 && root.bounds.height > 0,
                                             "analysis root must retain nonzero bounds after resize")
        try DecisionAnalysisFixtureAssertions.expect(identity.frame.width > 0 && overview.frame.width > 0
                                                && (summaryTable.enclosingScrollView?.contentView.bounds.width ?? 0) > 0,
                                             "identity, overview and summary list remain laid out after resize")
        let resizedDetailWidth = explanation.enclosingScrollView?.frame.width ?? 0
        let resizedComparisonWidth = comparisonView.enclosingScrollView?.frame.width ?? 0
        try DecisionAnalysisFixtureAssertions.expect(resizedDetailWidth >= 360 && resizedComparisonWidth >= 360,
                                                     "resized inspector widths are \(resizedDetailWidth)/\(resizedComparisonWidth), expected at least 360 logical points")
        try DecisionAnalysisFixtureAssertions.expect(primaryButtons.count == 4
                                                       && primaryButtons.allSatisfy({ root.visibleRect.contains(root.convert($0.bounds, from: $0)) }),
                                                     "primary query, identity, and pagination controls must remain reachable after resize")
        if selectDecision { try verifyExplanationScroll(explanation, window: window, root: root) }
        window.close()
        environment.0.disconnect()
    }

    private static func verifyExplanationScroll(_ textView: NSTextView, window: NSWindow, root: NSView) throws {
        guard let scroll = textView.enclosingScrollView,
              let container = textView.textContainer,
              let manager = textView.layoutManager else {
            throw DecisionAnalysisFixtureFailure.assertion("original explanation is not in a native wrapping scroll view")
        }
        root.layoutSubtreeIfNeeded()
        window.displayIfNeeded()
        textView.layoutSubtreeIfNeeded()
        manager.ensureLayout(for: container)
        let clip = textView.convert(scroll.contentView.bounds, from: scroll.contentView)
        let glyphs = manager.usedRect(for: container)
        try DecisionAnalysisFixtureAssertions.expect(textView.frame.width > 0 && textView.frame.height > 0,
                                             "explanation document must have nonzero bounds")
        try DecisionAnalysisFixtureAssertions.expect(clip.width > 0 && clip.height > 0,
                                             "explanation scroll clip must have nonzero bounds")
        try DecisionAnalysisFixtureAssertions.expect(glyphs.width > 0 && glyphs.height > 0 && glyphs.intersects(clip),
                                             "laid-out explanation glyphs must intersect the visible clip")
    }

    private static func findLabel(_ accessibilityLabel: String, in root: NSView) -> NSTextField? {
        descendants(of: root).compactMap { $0 as? NSTextField }.first {
            $0.accessibilityLabel() == accessibilityLabel
        }
    }

    private static func findTable(_ accessibilityLabel: String, in root: NSView) -> NSTableView? {
        descendants(of: root).compactMap { $0 as? NSTableView }.first {
            $0.accessibilityLabel() == accessibilityLabel
        }
    }

    private static func findTextView(_ accessibilityLabel: String, in root: NSView) -> NSTextView? {
        descendants(of: root).compactMap { $0 as? NSTextView }.first {
            $0.accessibilityLabel() == accessibilityLabel
        }
    }

    private static func descendants(of root: NSView) -> [NSView] {
        [root] + root.subviews.flatMap(descendants)
    }

    private static func rect(of view: NSView, in ancestor: NSView) -> NSRect {
        ancestor.convert(view.bounds, from: view)
    }

    private final class AppearanceFixtureDelegate: NSObject, NSApplicationDelegate {
        func applicationShouldTerminateAfterLastWindowClosed(_ sender: NSApplication) -> Bool { true }
    }
}
