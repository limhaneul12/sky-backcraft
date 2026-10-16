import AppKit

/// Strategy-first performance browser. Policy identity is exact; runs and models are never merged.
final class StrategyPerformanceView: NSView {
    private struct CardRecord {
        let run: RealRun
        let model: RealModelComparison
    }

    private let reader: DatabaseReader
    private let policies: [RealPolicy]
    private let runScopeIds: Set<String>?
    private let policyRevisionScopeId: String?
    private let ledgerReader = LedgerMCPReader()
    private let onOpenRun: (RealRun, String) -> Void
    private let onPolicyChange: (String?) -> Void
    private let workQueue = DispatchQueue(label: "dev.skybackcraft.strategy-performance-reader")

    private let policyPopup = NSPopUpButton()
    private let marketPopup = NSPopUpButton()
    private let modeControl = NSSegmentedControl(
        labels: ["자산 곡선", "낙폭"],
        trackingMode: .selectOne,
        target: nil,
        action: nil
    )
    private let refreshButton = NSButton(title: "새로고침", target: nil, action: nil)
    private let exportButton = NSButton(title: "CSV", target: nil, action: nil)
    private let statusLabel = NSTextField(wrappingLabelWithString: "전략을 선택하면 관련 실행을 그래프로 비교할 수 있습니다.")
    private let unavailableControls = NSStackView()
    private let unavailablePopup = NSPopUpButton()
    private let unavailableButton = NSButton(title: "실행 근거", target: nil, action: nil)
    private var unavailableHeight: NSLayoutConstraint?
    private let pageLabel = NSTextField(labelWithString: "")
    private let previousButton = NSButton(title: "이전", target: nil, action: nil)
    private let nextButton = NSButton(title: "다음", target: nil, action: nil)
    private let scrollView = NSScrollView()
    private let gridView = StrategyCardGridView()

    private var selectedPolicyId: String?
    private var strategyRuns: [RealRun] = []
    private var unavailableRuns: [RealRun] = []
    private var allRecords: [CardRecord] = []
    private var pageIndex = 0
    private let pageSize = 12
    private var dataGeneration: UInt64 = 0
    private var generation: UInt64 = 0
    private var chartQueue: [(StrategyPerformanceCard, CardRecord)] = []
    private var chartRequestActive = false

    init(
        reader: DatabaseReader,
        policies: [RealPolicy],
        initialPolicyId: String?,
        runScopeIds: Set<String>? = nil,
        policyRevisionScopeId: String? = nil,
        onOpenRun: @escaping (RealRun, String) -> Void,
        onPolicyChange: @escaping (String?) -> Void
    ) {
        self.reader = reader
        self.policies = policies.sorted {
            let comparison = $0.name.localizedStandardCompare($1.name)
            return comparison == .orderedSame ? $0.policyId < $1.policyId : comparison == .orderedAscending
        }
        self.runScopeIds = runScopeIds
        self.policyRevisionScopeId = policyRevisionScopeId
        self.onOpenRun = onOpenRun
        self.onPolicyChange = onPolicyChange
        super.init(frame: .zero)
        buildView()
        populatePolicies()

        if let initialPolicyId,
           policies.contains(where: { $0.policyId == initialPolicyId }) {
            selectPolicy(id: initialPolicyId)
            reloadStrategy()
        } else {
            policyPopup.selectItem(at: 0)
            showEmptySelection()
        }
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) { fatalError("init(coder:) is unavailable") }

    override func layout() {
        super.layout()
        gridView.reflow(for: scrollView.contentView.bounds.width)
    }

    override var wantsDefaultClipping: Bool { true }

    override func draw(_ dirtyRect: NSRect) {
        TerminalTheme.surface.setFill()
        bounds.fill()
    }

    override func viewDidChangeEffectiveAppearance() {
        super.viewDidChangeEffectiveAppearance()
        needsDisplay = true
    }

    private func buildView() {
        let baseTitle = runScopeIds == nil ? "전략별 성과" : "선택 실행 비교"
        let titleText = policyRevisionScopeId.map { "\(baseTitle) · 리비전 \(Self.shortRevision($0))" } ?? baseTitle
        let title = NSTextField(labelWithString: titleText)
        title.font = .systemFont(ofSize: 20, weight: .semibold)
        title.toolTip = policyRevisionScopeId.map { "고정 정책 리비전 ID \($0)" }
        let subtitleText: String
        if let runScopeIds {
            let revisionNotice = policyRevisionScopeId.map { " 정책 리비전 \(Self.shortRevision($0))으로 고정합니다." } ?? ""
            subtitleText = "선택한 \(runScopeIds.count)개 실행 안에서 전략 성과를 비교합니다.\(revisionNotice) 기간·비용·자본과 원장 경계는 서로 독립이며 합산하거나 이어 붙이지 않습니다."
        } else {
            subtitleText = "정책을 기준으로 찾고, 각 실행·시장 원장은 독립 그래프로 확인합니다. 서로 다른 기간과 비용 조건을 평균내거나 이어 붙이지 않습니다."
        }
        let subtitle = NSTextField(wrappingLabelWithString: subtitleText)
        subtitle.font = TerminalTheme.body
        subtitle.textColor = TerminalTheme.secondaryText
        subtitle.toolTip = policyRevisionScopeId.map { "고정 정책 리비전 ID \($0)" }

        policyPopup.target = self
        policyPopup.action = #selector(policyChanged)
        policyPopup.setAccessibilityLabel("전략 선택")
        policyPopup.toolTip = "등록된 정책 ID를 정확히 기준으로 조회합니다."

        marketPopup.addItems(withTitles: ["전체 시장", "BTC", "ETH", "XRP"])
        marketPopup.target = self
        marketPopup.action = #selector(marketChanged)
        marketPopup.setAccessibilityLabel("시장 필터")

        modeControl.selectedSegment = 0
        modeControl.target = self
        modeControl.action = #selector(modeChanged)
        modeControl.setAccessibilityLabel("그래프 종류")

        refreshButton.bezelStyle = .rounded
        refreshButton.target = self
        refreshButton.action = #selector(refreshRequested)

        exportButton.target = self
        exportButton.action = #selector(exportSelectedStrategy)
        exportButton.toolTip = "현재 전략과 시장 조건의 전체 성과를 내보냅니다. 화면 페이지에 한정하지 않으며 실패 실행은 미확인 값으로 구분합니다."
        let controls = NSStackView(views: [policyPopup, marketPopup, modeControl, refreshButton, exportButton])
        controls.orientation = .horizontal
        controls.alignment = .centerY
        controls.spacing = 10
        policyPopup.widthAnchor.constraint(greaterThanOrEqualToConstant: 260).isActive = true
        marketPopup.widthAnchor.constraint(equalToConstant: 112).isActive = true

        statusLabel.font = .systemFont(ofSize: 13)
        statusLabel.textColor = TerminalTheme.secondaryText
        statusLabel.maximumNumberOfLines = 2
        statusLabel.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)

        let unavailableLabel = NSTextField(labelWithString: "성과 없는 실행")
        unavailableLabel.font = .systemFont(ofSize: 12, weight: .medium)
        unavailableLabel.textColor = TerminalTheme.secondaryText
        unavailablePopup.setAccessibilityLabel("실패 또는 성과 요약이 없는 실행")
        unavailablePopup.widthAnchor.constraint(greaterThanOrEqualToConstant: 360).isActive = true
        unavailableButton.bezelStyle = .rounded
        unavailableButton.target = self
        unavailableButton.action = #selector(openUnavailableRun)
        unavailableControls.orientation = .horizontal
        unavailableControls.alignment = .centerY
        unavailableControls.spacing = 8
        unavailableControls.addArrangedSubview(unavailableLabel)
        unavailableControls.addArrangedSubview(unavailablePopup)
        unavailableControls.addArrangedSubview(unavailableButton)
        unavailableControls.isHidden = true
        unavailableHeight = unavailableControls.heightAnchor.constraint(equalToConstant: 0)
        unavailableHeight?.isActive = true

        scrollView.hasVerticalScroller = true
        scrollView.hasHorizontalScroller = false
        scrollView.autohidesScrollers = true
        scrollView.drawsBackground = false
        scrollView.documentView = gridView
        gridView.translatesAutoresizingMaskIntoConstraints = false
        NSLayoutConstraint.activate([
            gridView.leadingAnchor.constraint(equalTo: scrollView.contentView.leadingAnchor),
            gridView.trailingAnchor.constraint(equalTo: scrollView.contentView.trailingAnchor),
            gridView.topAnchor.constraint(equalTo: scrollView.contentView.topAnchor)
        ])

        previousButton.target = self
        previousButton.action = #selector(previousPage)
        nextButton.target = self
        nextButton.action = #selector(nextPage)
        let pager = NSStackView(views: [previousButton, pageLabel, nextButton])
        pager.orientation = .horizontal
        pager.alignment = .centerY
        pager.spacing = 10
        pageLabel.alignment = .center
        pageLabel.font = .monospacedDigitSystemFont(ofSize: 12, weight: .medium)

        for view in [title, subtitle, controls, statusLabel, unavailableControls, scrollView, pager] {
            view.translatesAutoresizingMaskIntoConstraints = false
            addSubview(view)
        }
        NSLayoutConstraint.activate([
            title.topAnchor.constraint(equalTo: topAnchor, constant: 18),
            title.leadingAnchor.constraint(equalTo: leadingAnchor, constant: 20),
            title.trailingAnchor.constraint(lessThanOrEqualTo: trailingAnchor, constant: -20),

            subtitle.topAnchor.constraint(equalTo: title.bottomAnchor, constant: 5),
            subtitle.leadingAnchor.constraint(equalTo: title.leadingAnchor),
            subtitle.trailingAnchor.constraint(equalTo: trailingAnchor, constant: -20),

            controls.topAnchor.constraint(equalTo: subtitle.bottomAnchor, constant: 14),
            controls.leadingAnchor.constraint(equalTo: title.leadingAnchor),
            controls.trailingAnchor.constraint(lessThanOrEqualTo: trailingAnchor, constant: -20),

            statusLabel.topAnchor.constraint(equalTo: controls.bottomAnchor, constant: 10),
            statusLabel.leadingAnchor.constraint(equalTo: title.leadingAnchor),
            statusLabel.trailingAnchor.constraint(equalTo: trailingAnchor, constant: -20),

            unavailableControls.topAnchor.constraint(equalTo: statusLabel.bottomAnchor, constant: 8),
            unavailableControls.leadingAnchor.constraint(equalTo: title.leadingAnchor),
            unavailableControls.trailingAnchor.constraint(lessThanOrEqualTo: trailingAnchor, constant: -20),

            scrollView.topAnchor.constraint(equalTo: unavailableControls.bottomAnchor, constant: 10),
            scrollView.leadingAnchor.constraint(equalTo: title.leadingAnchor),
            scrollView.trailingAnchor.constraint(equalTo: trailingAnchor, constant: -20),
            scrollView.bottomAnchor.constraint(equalTo: pager.topAnchor, constant: -10),

            pager.centerXAnchor.constraint(equalTo: scrollView.centerXAnchor),
            pager.bottomAnchor.constraint(equalTo: bottomAnchor, constant: -12),
            pager.heightAnchor.constraint(equalToConstant: 26)
        ])
    }

    private func populatePolicies() {
        policyPopup.removeAllItems()
        policyPopup.addItem(withTitle: "전략을 선택하세요")
        policyPopup.lastItem?.representedObject = nil
        for policy in policies {
            let title = policyDisplayName(policy)
            policyPopup.addItem(withTitle: title)
            policyPopup.lastItem?.representedObject = policy.policyId
            policyPopup.lastItem?.toolTip = "\(title) · \(policy.family) · 정책 ID \(policy.policyId)"
        }
    }

    private func selectPolicy(id: String) {
        guard let index = policies.firstIndex(where: { $0.policyId == id }) else { return }
        policyPopup.selectItem(at: index + 1)
        selectedPolicyId = id
    }

    @objc private func policyChanged() {
        dataGeneration &+= 1
        generation &+= 1
        selectedPolicyId = policyPopup.selectedItem?.representedObject as? String
        onPolicyChange(selectedPolicyId)
        pageIndex = 0
        reloadStrategy()
    }

    @objc private func marketChanged() {
        generation &+= 1
        pageIndex = 0
        renderCurrentPage()
    }

    @objc private func modeChanged() {
        let mode = ChartMode(rawValue: modeControl.selectedSegment) ?? .equity
        gridView.cards.forEach { $0.setChartMode(mode) }
    }

    @objc private func refreshRequested() {
        dataGeneration &+= 1
        generation &+= 1
        pageIndex = 0
        reloadStrategy()
    }

    @objc private func previousPage() {
        guard pageIndex > 0 else { return }
        generation &+= 1
        pageIndex -= 1
        renderCurrentPage()
    }

    @objc private func nextPage() {
        guard pageIndex + 1 < pageCount else { return }
        generation &+= 1
        pageIndex += 1
        renderCurrentPage()
    }

    @objc private func openUnavailableRun() {
        let index = unavailablePopup.indexOfSelectedItem
        guard unavailableRuns.indices.contains(index) else { return }
        onOpenRun(unavailableRuns[index], "")
    }

    private func reloadStrategy() {
        guard let policyId = selectedPolicyId else {
            showEmptySelection()
            return
        }
        cancelChartQueue()
        let requestDataGeneration = dataGeneration
        setLoading(true)
        strategyRuns = []
        unavailableRuns = []
        allRecords = []
        updateUnavailableControls()
        gridView.replaceCards([])
        statusLabel.stringValue = "전략의 실행과 모델 원장을 조회하고 있습니다…"

        workQueue.async { [weak self] in
            guard let self else { return }
            do {
                var runs: [RealRun] = []
                var cursor: RunHistoryCursor?
                repeat {
                    let page = try self.reader.fetchRunHistoryPage(
                        filter: RunHistoryFilter(
                            policyId: policyId,
                            policyRevisionId: self.policyRevisionScopeId,
                            pinnedRunIds: self.runScopeIds?.sorted()
                        ),
                        cursor: cursor,
                        limit: 100
                    )
                    runs.append(contentsOf: page.items)
                    cursor = page.nextCursor
                } while cursor != nil
                if let runScopeIds = self.runScopeIds {
                    runs = runs.filter { runScopeIds.contains($0.id) }
                }
                if let policyRevisionScopeId = self.policyRevisionScopeId {
                    runs = runs.filter { $0.policyRevisionIds?.contains(policyRevisionScopeId) == true }
                }

                var records: [CardRecord] = []
                for run in runs {
                    let comparisons = try self.reader.fetchModelComparisons(for: run.id)
                    records.append(contentsOf: comparisons
                        .filter {
                            $0.policyId == policyId &&
                            (self.policyRevisionScopeId == nil || $0.policyRevisionId == self.policyRevisionScopeId)
                        }
                        .map { CardRecord(run: run, model: $0) })
                }
                DispatchQueue.main.async { [weak self] in
                    guard let self, self.dataGeneration == requestDataGeneration,
                          self.selectedPolicyId == policyId else { return }
                    let runsWithResults = Set(records.map { $0.run.id })
                    self.strategyRuns = runs
                    self.unavailableRuns = runs.filter { !runsWithResults.contains($0.id) }
                    self.allRecords = records
                    self.updateUnavailableControls()
                    self.setLoading(false)
                    self.renderCurrentPage()
                }
            } catch {
                DispatchQueue.main.async { [weak self] in
                    guard let self, self.dataGeneration == requestDataGeneration else { return }
                    self.strategyRuns = []
                    self.unavailableRuns = []
                    self.allRecords = []
                    self.updateUnavailableControls()
                    self.setLoading(false)
                    self.gridView.replaceCards([])
                    self.statusLabel.stringValue = error.localizedDescription
                    self.updatePager()
                }
            }
        }
    }

    private var filteredRecords: [CardRecord] {
        let market = selectedMarket
        guard let market else { return allRecords }
        return allRecords.filter {
            $0.model.market == market || $0.model.market.hasSuffix("-\(market)")
        }
    }

    private var selectedMarket: String? {
        switch marketPopup.indexOfSelectedItem {
        case 1: return "BTC"
        case 2: return "ETH"
        case 3: return "XRP"
        default: return nil
        }
    }

    private var pageCount: Int {
        let count = filteredRecords.count
        return count == 0 ? 0 : Int(ceil(Double(count) / Double(pageSize)))
    }

    private func renderCurrentPage() {
        cancelChartQueue()
        guard let policyId = selectedPolicyId else {
            showEmptySelection()
            return
        }
        let records = filteredRecords
        exportButton.isEnabled = !records.isEmpty || !unavailableRuns.isEmpty
        let policy = policies.first(where: { $0.policyId == policyId })
        let policyName = policy.map { policyDisplayName($0) } ?? "선택한 전략"
        if records.isEmpty {
            gridView.replaceCards([])
            statusLabel.stringValue = summaryText(policyName: policyName, visibleResultCount: 0)
            updatePager()
            return
        }

        pageIndex = min(pageIndex, max(pageCount - 1, 0))
        let lower = pageIndex * pageSize
        let upper = min(lower + pageSize, records.count)
        let visible = Array(records[lower..<upper])
        let cards = visible.map { record in
            StrategyPerformanceCard(
                policyName: policyName,
                run: record.run,
                model: record.model,
                chartMode: ChartMode(rawValue: modeControl.selectedSegment) ?? .equity,
                onOpen: { [weak self] in self?.onOpenRun(record.run, record.model.modelId) }
            )
        }
        gridView.replaceCards(cards)
        statusLabel.stringValue = summaryText(policyName: policyName, visibleResultCount: records.count)
        updatePager()
        chartQueue = Array(zip(cards, visible))
        loadNextChart(generation: generation)
    }

    private func policyDisplayName(_ policy: RealPolicy) -> String {
        let name = policy.name.isEmpty ? "이름 없는 전략" : policy.name
        let duplicateCount = policies.lazy.filter { $0.name == policy.name }.prefix(2).count
        guard duplicateCount > 1 else { return name }
        let identity = policy.policyId.hasPrefix("policy-") ? policy.policyId.dropFirst(7) : policy.policyId[...]
        return "\(name) · \(identity.prefix(8))"
    }

    private func summaryText(policyName: String, visibleResultCount: Int) -> String {
        let resultRunCount = Set(allRecords.map { $0.run.id }).count
        let valuePrefix: String
        if let runScopeIds {
            let scopedPolicyRunIds = Set(strategyRuns.map(\.id))
            let missingCount = runScopeIds.subtracting(scopedPolicyRunIds).count
            valuePrefix = "\(policyName) · 선택 \(runScopeIds.count)실행 · 전략 해당 \(strategyRuns.count)실행 · 해당 전략 없음/미제공 \(missingCount)실행"
        } else {
            valuePrefix = "\(policyName) · 총 \(strategyRuns.count)실행"
        }
        let revisionText = policyRevisionScopeId.map { " · 리비전 \(Self.shortRevision($0)) 고정" } ?? ""
        var value = "\(valuePrefix)\(revisionText) · 성과 \(resultRunCount)실행/\(allRecords.count)결과 · 실패/성과 없음 \(unavailableRuns.count)실행"
        if let market = selectedMarket {
            value += " · \(market) 표시 \(visibleResultCount)결과"
        }
        return value
    }

    private func updateUnavailableControls() {
        unavailablePopup.removeAllItems()
        for run in unavailableRuns {
            let state = run.state == "FAILED" ? "실패" : "\(TerminalPresentation.state(run.state)) · 성과 요약 없음"
            let markets = run.markets?.map(Self.shortMarket).joined(separator: "/") ?? "시장 미확인"
            let title = "\(state) · \(run.createdDateStr) KST · \(markets) · \(run.costAssumptionLabel ?? "비용 미확인")"
            unavailablePopup.addItem(withTitle: title)
            unavailablePopup.lastItem?.toolTip = "실행 ID \(run.id)\n\(run.windowStartUTC ?? "시작 미확인") → \(run.windowEndUTC ?? "종료 미확인") UTC"
        }
        unavailableControls.isHidden = unavailableRuns.isEmpty
        unavailableHeight?.constant = unavailableRuns.isEmpty ? 0 : 26
        unavailableButton.isEnabled = !unavailableRuns.isEmpty
    }

    private static func shortMarket(_ market: String) -> String {
        market.hasPrefix("KRW-") ? String(market.dropFirst(4)) : market
    }

    @objc private func exportSelectedStrategy() {
        let name = policies.first(where: { $0.policyId == selectedPolicyId })?.name
        let scopeMarket = selectedMarket.map { $0.hasPrefix("KRW-") ? $0 : "KRW-" + $0 }
        let valid = filteredRecords.map { LedgerCSVRecord(run: $0.run, model: $0.model, policyName: name,
                                                         scopePolicyId: selectedPolicyId, scopeMarket: scopeMarket,
                                                         scopeRunIds: runScopeIds?.sorted(),
                                                         scopePolicyRevisionId: policyRevisionScopeId) }
        let missing = unavailableRuns.filter { run in
            guard let market = selectedMarket else { return true }
            return run.markets?.contains(where: { $0 == market || $0.hasSuffix("-" + market) }) == true
        }.map { LedgerCSVRecord(run: $0, model: nil, policyName: name,
                               scopePolicyId: selectedPolicyId, scopeMarket: scopeMarket,
                               scopeRunIds: runScopeIds?.sorted(),
                               scopePolicyRevisionId: policyRevisionScopeId) }
        do {
            let data = try LedgerCSVExport.models(valid + missing)
            LedgerCSVExport.save(data, suggestedName: "backcraft-strategy-results.csv", window: window)
        } catch { LedgerCSVExport.show(error, window: window) }
    }

    private func updatePager() {
        let pages = pageCount
        pageLabel.stringValue = pages == 0 ? "표시할 결과 없음" : "\(pageIndex + 1) / \(pages) · 총 \(filteredRecords.count)개"
        previousButton.isEnabled = pageIndex > 0
        nextButton.isEnabled = pageIndex + 1 < pages
    }

    private func showEmptySelection() {
        cancelChartQueue()
        selectedPolicyId = nil
        strategyRuns = []
        unavailableRuns = []
        allRecords = []
        updateUnavailableControls()
        gridView.replaceCards([])
        if let runScopeIds {
            let revision = policyRevisionScopeId.map { " 정책 리비전 \(Self.shortRevision($0)) 범위만 사용합니다." } ?? ""
            statusLabel.stringValue = "선택한 \(runScopeIds.count)개 실행에서 비교할 전략을 선택하세요.\(revision) 선택 범위 밖 실행은 조회하지 않습니다."
        } else {
            statusLabel.stringValue = "전략을 선택하면 관련 실행을 그래프로 비교할 수 있습니다."
        }
        updatePager()
        setLoading(false)
    }

    private func setLoading(_ loading: Bool) {
        policyPopup.isEnabled = true
        refreshButton.isEnabled = !loading && selectedPolicyId != nil
        exportButton.isEnabled = !loading && selectedPolicyId != nil && (!allRecords.isEmpty || !unavailableRuns.isEmpty)
        marketPopup.isEnabled = true
    }

    private static func shortRevision(_ raw: String) -> String {
        let prefix = "policy-revision-"
        let identity = raw.hasPrefix(prefix) ? String(raw.dropFirst(prefix.count)) : raw
        return String(identity.prefix(8))
    }

    private func cancelChartQueue() {
        chartQueue.removeAll()
    }

    private func loadNextChart(generation requestGeneration: UInt64) {
        guard !chartRequestActive, generation == requestGeneration,
              !chartQueue.isEmpty else { return }
        chartRequestActive = true
        let (card, record) = chartQueue.removeFirst()
        card.showChartLoading()

        if record.run.ledgerState == "COMPACTED" {
            ledgerReader.marks(
                runId: record.run.id,
                modelId: record.model.modelId,
                expectedCount: record.model.accountMarkCount
            ) { [weak self, weak card] result in
                DispatchQueue.main.async {
                    guard let self else { return }
                    if self.generation == requestGeneration, let card {
                        card.applyChartResult(result, totalCount: record.model.accountMarkCount)
                    }
                    self.chartRequestActive = false
                    self.loadNextChart(generation: self.generation)
                }
            }
        } else {
            workQueue.async { [weak self, weak card] in
                guard let self else { return }
                let result: Result<[RealAccountMark], Error>
                do {
                    result = .success(try self.reader.fetchAccountMarks(
                        for: record.run.id,
                        modelId: record.model.modelId,
                        maxPoints: 1_000
                    ))
                } catch {
                    result = .failure(error)
                }
                DispatchQueue.main.async {
                    if self.generation == requestGeneration, let card {
                        card.applyChartResult(result, totalCount: record.model.accountMarkCount)
                    }
                    self.chartRequestActive = false
                    self.loadNextChart(generation: self.generation)
                }
            }
        }
    }
}

private final class StrategyCardGridView: NSView {
    private(set) var cards: [StrategyPerformanceCard] = []
    private var heightConstraint: NSLayoutConstraint?
    override var isFlipped: Bool { true }

    func replaceCards(_ newCards: [StrategyPerformanceCard]) {
        cards.forEach { $0.removeFromSuperview() }
        cards = newCards
        cards.forEach { addSubview($0) }
        reflow(for: bounds.width)
    }

    func reflow(for availableWidth: CGFloat) {
        guard availableWidth > 0 else { return }
        let gap: CGFloat = 14
        let columns = availableWidth >= 760 ? 2 : 1
        let cardWidth = floor((availableWidth - CGFloat(columns - 1) * gap) / CGFloat(columns))
        let rows = cards.isEmpty ? 0 : Int(ceil(Double(cards.count) / Double(columns)))
        var yOffset: CGFloat = 0
        for row in 0..<rows {
            let startIndex = row * columns
            let endIndex = min(startIndex + columns, cards.count)
            let rowCards = cards[startIndex..<endIndex]
            let rowHeight = rowCards.map { $0.requiredHeight(forWidth: cardWidth) }.max()
                ?? max(338, ceil(338 * TerminalTheme.textScale))
            for (column, card) in rowCards.enumerated() {
                card.frame = NSRect(
                    x: CGFloat(column) * (cardWidth + gap),
                    y: yOffset,
                    width: cardWidth,
                    height: rowHeight
                )
            }
            yOffset += rowHeight
            if row < rows - 1 {
                yOffset += gap
            }
        }
        let requiredHeight = max(1, yOffset)
        if heightConstraint == nil {
            heightConstraint = heightAnchor.constraint(equalToConstant: requiredHeight)
            heightConstraint?.isActive = true
        } else {
            heightConstraint?.constant = requiredHeight
        }
    }
}

private final class StrategyPerformanceCard: NSView {
    private let titleLabel: NSTextField
    private let periodLabel: NSTextField
    private let metricsLabel: NSTextField
    private let chartView = EquityChartView()
    private let chartStatus = NSTextField(labelWithString: "차트 대기")
    private let initialEquity: Double

    init(
        policyName: String,
        run: RealRun,
        model: RealModelComparison,
        chartMode: ChartMode,
        onOpen: @escaping () -> Void
    ) {
        initialEquity = model.initialEquity
        let title = NSTextField(labelWithString: "\(policyName) · \(Self.shortMarket(model.market))")
        let period = NSTextField(wrappingLabelWithString: Self.periodText(run, model: model))
        let returnText = model.netReturnDecimal.map { Self.percent($0, signed: true) }
            ?? (model.netReturnNullReason.map { "산출 불가 · \($0)" } ?? "산출 불가")
        let mddText = model.maxDrawdownDecimal.map { Self.percent($0) }
            ?? (model.maxDrawdownNullReason.map { "산출 불가 · \($0)" } ?? "산출 불가")
        let pnlText = Formatters.money(model.netPnlDecimal, signed: true)
        let feesText = Formatters.money(model.cumulativeFeesDecimal)
        let metrics = NSTextField(wrappingLabelWithString:
            "수익률 \(returnText)   MDD \(mddText)   손익 \(pnlText)\n수수료 \(feesText)   체결 \(model.fillCount)건   청산 \(model.closedEpisodes)건"
        )
        titleLabel = title
        periodLabel = period
        metricsLabel = metrics
        super.init(frame: .zero)
        wantsLayer = true
        layer?.cornerRadius = 8
        layer?.masksToBounds = true
        layer?.borderWidth = 1
        layer?.borderColor = TerminalTheme.border.cgColor

        title.font = TerminalTheme.systemFont(ofSize: 15, weight: .semibold)
        title.lineBreakMode = .byTruncatingTail
        title.setContentCompressionResistancePriority(.required, for: .vertical)
        title.toolTip = "정책 ID \(model.policyId ?? "미확인") · 실행 ID \(run.id) · 모델 ID \(model.modelId)"

        let detailButton = ClosureButton(title: "실행 상세", action: onOpen)
        detailButton.bezelStyle = .rounded
        detailButton.toolTip = "실행 \(run.id) · 모델 \(model.modelId)"
        detailButton.setAccessibilityLabel("실행 상세 · \(Self.shortMarket(model.market)) · \(run.id)")

        period.font = TerminalTheme.systemFont(ofSize: 11)
        period.textColor = TerminalTheme.secondaryText
        period.maximumNumberOfLines = 0
        period.lineBreakMode = .byWordWrapping
        period.setContentCompressionResistancePriority(.required, for: .vertical)
        period.setContentHuggingPriority(.required, for: .vertical)
        period.toolTip = "실행 ID \(run.id)\n모델 ID \(model.modelId)\n정책 revision \(model.policyRevisionId ?? "미확인")"

        metrics.font = TerminalTheme.monospacedDigitFont(ofSize: 12, weight: .medium)
        metrics.maximumNumberOfLines = 0
        metrics.lineBreakMode = .byWordWrapping
        metrics.setContentCompressionResistancePriority(.required, for: .vertical)
        metrics.setContentHuggingPriority(.required, for: .vertical)
        let returnRaw = model.netReturnDecimal?.description ?? (model.netReturnNullReason.map { "null(\($0))" } ?? "null")
        let mddRaw = model.maxDrawdownDecimal?.description ?? (model.maxDrawdownNullReason.map { "null(\($0))" } ?? "null")
        metrics.toolTip = "net_return=\(returnRaw)\nmax_drawdown=\(mddRaw)\nnet_pnl=\(model.netPnlDecimal)\nfees=\(model.cumulativeFeesDecimal)"
        setAccessibilityElement(true)
        setAccessibilityRole(.group)
        setAccessibilityLabel("\(policyName) · \(Self.shortMarket(model.market)) · \(run.id)")
        setAccessibilityValue(
            "수익률 \(returnText), 최대 낙폭 \(mddText), 순손익 \(pnlText), 수수료 \(feesText), 체결 \(model.fillCount)건, 청산 \(model.closedEpisodes)건"
        )

        chartView.title = "\(Self.shortMarket(model.market)) · 계좌 평가액 KRW"
        chartView.emptyMessage = "검증된 계좌 마크를 불러오는 중입니다."
        chartView.setMode(chartMode)
        chartView.setPoints([], initialEquity: model.initialEquity)

        chartStatus.font = TerminalTheme.systemFont(ofSize: 11)
        chartStatus.textColor = TerminalTheme.secondaryText
        chartStatus.maximumNumberOfLines = 0
        chartStatus.lineBreakMode = .byWordWrapping
        chartStatus.setContentCompressionResistancePriority(.required, for: .vertical)

        for view in [title, detailButton, period, metrics, chartView, chartStatus] {
            view.translatesAutoresizingMaskIntoConstraints = false
            addSubview(view)
        }
        NSLayoutConstraint.activate([
            title.topAnchor.constraint(equalTo: topAnchor, constant: 14),
            title.leadingAnchor.constraint(equalTo: leadingAnchor, constant: 14),
            title.trailingAnchor.constraint(lessThanOrEqualTo: detailButton.leadingAnchor, constant: -8),

            detailButton.centerYAnchor.constraint(equalTo: title.centerYAnchor),
            detailButton.trailingAnchor.constraint(equalTo: trailingAnchor, constant: -12),

            period.topAnchor.constraint(equalTo: title.bottomAnchor, constant: 6),
            period.leadingAnchor.constraint(equalTo: title.leadingAnchor),
            period.trailingAnchor.constraint(equalTo: trailingAnchor, constant: -14),

            metrics.topAnchor.constraint(equalTo: period.bottomAnchor, constant: 8),
            metrics.leadingAnchor.constraint(equalTo: title.leadingAnchor),
            metrics.trailingAnchor.constraint(equalTo: trailingAnchor, constant: -14),

            chartView.topAnchor.constraint(equalTo: metrics.bottomAnchor, constant: 10),
            chartView.leadingAnchor.constraint(equalTo: leadingAnchor, constant: 10),
            chartView.trailingAnchor.constraint(equalTo: trailingAnchor, constant: -10),
            chartView.bottomAnchor.constraint(equalTo: chartStatus.topAnchor, constant: -5),

            chartStatus.leadingAnchor.constraint(equalTo: title.leadingAnchor),
            chartStatus.trailingAnchor.constraint(equalTo: trailingAnchor, constant: -14),
            chartStatus.bottomAnchor.constraint(equalTo: bottomAnchor, constant: -10)
        ])
    }

    override func setFrameSize(_ newSize: NSSize) {
        updatePreferredTextWidths(for: newSize.width)
        super.setFrameSize(newSize)
    }

    override func layout() {
        updatePreferredTextWidths(for: bounds.width)
        super.layout()
    }

    private func updatePreferredTextWidths(for cardWidth: CGFloat) {
        guard cardWidth > 28 else { return }
        let textWidth = cardWidth - 28
        for label in [periodLabel, metricsLabel, chartStatus] {
            if abs(label.preferredMaxLayoutWidth - textWidth) > 0.5 {
                label.preferredMaxLayoutWidth = textWidth
                label.invalidateIntrinsicContentSize()
            }
        }
    }

    func requiredHeight(forWidth width: CGFloat) -> CGFloat {
        let baseHeight = max(338, ceil(338 * TerminalTheme.textScale))
        guard width > 28 else { return baseHeight }
        updatePreferredTextWidths(for: width)
        let textWidth = width - 28
        let titleH = Self.measuredTextHeight(titleLabel.stringValue, font: titleLabel.font, width: max(80, textWidth - 96))
        let periodH = Self.measuredTextHeight(periodLabel.stringValue, font: periodLabel.font, width: textWidth)
        let metricsH = Self.measuredTextHeight(metricsLabel.stringValue, font: metricsLabel.font, width: textWidth)
        let statusH = Self.measuredTextHeight(chartStatus.stringValue, font: chartStatus.font, width: textWidth)
        let minChartH = max(168, ceil(168 * TerminalTheme.textScale))
        let needed = ceil(14 + titleH + 6 + periodH + 8 + metricsH + 10 + minChartH + 5 + statusH + 10)
        return max(baseHeight, needed)
    }

    private static func measuredTextHeight(_ text: String, font: NSFont?, width: CGFloat) -> CGFloat {
        guard !text.isEmpty, width > 0 else { return 16 }
        let resolvedFont = font ?? NSFont.systemFont(ofSize: 12)
        let storage = NSTextStorage(attributedString: NSAttributedString(string: text, attributes: [.font: resolvedFont]))
        let manager = NSLayoutManager()
        let container = NSTextContainer(containerSize: NSSize(width: width, height: 10_000))
        container.lineFragmentPadding = 0
        container.lineBreakMode = .byWordWrapping
        manager.addTextContainer(container)
        storage.addLayoutManager(manager)
        manager.ensureLayout(for: container)
        return ceil(manager.usedRect(for: container).height)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) { fatalError("init(coder:) is unavailable") }

    override var wantsDefaultClipping: Bool { true }

    override func draw(_ dirtyRect: NSRect) {
        TerminalTheme.panel.setFill()
        bounds.fill()
    }

    override func viewDidChangeEffectiveAppearance() {
        super.viewDidChangeEffectiveAppearance()
        layer?.borderColor = TerminalTheme.border.cgColor
    }

    func setChartMode(_ mode: ChartMode) { chartView.setMode(mode) }

    func showChartLoading() {
        chartStatus.stringValue = "검증된 원장 차트 조회 중…"
        chartView.emptyMessage = "검증된 원장 차트를 불러오는 중입니다."
        chartView.needsDisplay = true
    }

    func applyChartResult(_ result: Result<[RealAccountMark], Error>, totalCount: Int) {
        switch result {
        case .success(let marks):
            if marks.isEmpty {
                chartView.emptyMessage = "차트를 그릴 원장 마크가 없습니다."
            }
            chartView.setPoints(marks, initialEquity: initialEquity)
            chartStatus.stringValue = "화면용 \(marks.count) / 원장 \(totalCount) 마크"
        case .failure(let error):
            chartView.setPoints([], initialEquity: initialEquity)
            chartView.emptyMessage = "차트 원장을 확인할 수 없습니다. 요약 지표는 보존된 실행 값입니다."
            chartStatus.stringValue = error.localizedDescription
            chartStatus.toolTip = error.localizedDescription
            chartView.needsDisplay = true
        }
        NSAccessibility.post(element: self, notification: .valueChanged)
    }

    private static func shortMarket(_ market: String) -> String {
        market.hasPrefix("KRW-") ? String(market.dropFirst(4)) : market
    }

    private static func periodText(_ run: RealRun, model: RealModelComparison) -> String {
        let created = displayKST(run.createdAtUTC) ?? run.createdDateStr + " KST"
        let start = displayKST(run.windowStartUTC) ?? "시작 미확인"
        let end = displayKST(run.windowEndUTC) ?? "종료 미확인"
        let revision = model.policyRevisionId.map { "리비전 \(Self.shortRevision($0))" } ?? "리비전 미확인"
        let capital = TerminalPresentation.state(run.capitalMode ?? "")
        return "\(revision) · 실행 \(created)\n\(start) → \(end)\n\(run.costAssumptionLabel ?? "비용 조건 미확인") · \(capital)"
    }

    private static func displayKST(_ raw: String?) -> String? {
        guard let raw else { return nil }
        let precise = ISO8601DateFormatter()
        precise.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        let ordinary = ISO8601DateFormatter()
        ordinary.formatOptions = [.withInternetDateTime]
        guard let date = precise.date(from: raw) ?? ordinary.date(from: raw) else { return nil }
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "ko_KR")
        formatter.timeZone = TimeZone(identifier: "Asia/Seoul")
        formatter.dateFormat = "yyyy-MM-dd HH:mm 'KST'"
        return formatter.string(from: date)
    }

    private static func shortRevision(_ raw: String) -> String {
        let prefix = "policy-revision-"
        let identity = raw.hasPrefix(prefix) ? String(raw.dropFirst(prefix.count)) : raw
        return String(identity.prefix(8))
    }

    private static func percent(_ value: Decimal, signed: Bool = false) -> String {
        let formatter = NumberFormatter()
        formatter.locale = Locale(identifier: "ko_KR")
        formatter.numberStyle = .decimal
        formatter.minimumFractionDigits = 2
        formatter.maximumFractionDigits = 4
        if signed { formatter.positivePrefix = "+" }
        let scaled = NSDecimalNumber(decimal: value).multiplying(by: 100)
        return (formatter.string(from: scaled) ?? scaled.stringValue) + "%"
    }
}

private final class ClosureButton: NSButton {
    private let closure: () -> Void

    init(title: String, action: @escaping () -> Void) {
        closure = action
        super.init(frame: .zero)
        self.title = title
        target = self
        self.action = #selector(invoke)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) { fatalError("init(coder:) is unavailable") }

    @objc private func invoke() { closure() }
}
