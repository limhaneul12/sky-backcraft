import AppKit

struct StrategyChartReadPacing {
    // NativeMCPClient.call posts initialize and tools/call for each chart read.
    static let httpPostsPerChartRead = 2
    static let minimumInterval: TimeInterval = 0.2

    static func delay(lastStartedAt: TimeInterval?, now: TimeInterval) -> TimeInterval {
        guard let lastStartedAt else { return 0 }
        return max(0, minimumInterval - (now - lastStartedAt))
    }
}

/// Strategy-first performance browser. Policy identity is exact; runs and models are never merged.
final class StrategyPerformanceView: NSView {
    private struct CardRecord {
        let run: RealRun
        let model: RealModelComparison
    }

    private let reader: DatabaseReader
    private var policies: [RealPolicy]
    private let runScopeIds: Set<String>?
    private let policyRevisionScopeId: String?
    private let ledgerReader = LedgerMCPReader()
    private let onOpenRun: (RealRun, String) -> Void
    private let onPolicyChange: (String?) -> Void
    private let onRefreshCatalog: (() -> Void)?
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
    private let unavailableDisclosure = NSButton(title: "성과 없는 실행", target: nil, action: nil)
    private let unavailablePopup = NSPopUpButton()
    private let unavailableButton = NSButton(title: "실행 근거", target: nil, action: nil)
    private var unavailableExpanded = false
    private let pageLabel = NSTextField(labelWithString: "")
    private let previousButton = NSButton(title: "이전", target: nil, action: nil)
    private let nextButton = NSButton(title: "다음", target: nil, action: nil)
    private let scrollView = NSScrollView()
    private let gridView = StrategyCardGridView()
    private let pagerRow = NSStackView()
    private var pagerHeight: NSLayoutConstraint?
    private let emptyState = TerminalEmptyStateView(
        symbolName: "chart.xyaxis.line",
        title: "전략을 선택하세요",
        message: "정책을 고르면 실행·시장별 자산 곡선과 핵심 지표가 카드로 표시됩니다."
    )

    private var selectedPolicyId: String?
    private var loadedPolicyId: String?
    private var strategyRuns: [RealRun] = []
    private var unavailableRuns: [RealRun] = []
    private var allRecords: [CardRecord] = []
    private var pageIndex = 0
    private let pageSize = 12
    private var dataGeneration: UInt64 = 0
    private var generation: UInt64 = 0
    private var workspaceVisible: Bool
    private var needsDataReloadWhenVisible = false
    private var needsRenderWhenVisible = false
    private var dataLoadInFlight = false
    private var dataReadRequest: NativeMCPRequest?
    private var chartQueue: [(StrategyPerformanceCard, CardRecord)] = []
    private var chartRequestActive = false
    private var activeChartRequest: NativeMCPRequest?
    private var activeChartItem: (StrategyPerformanceCard, CardRecord)?
    private var chartStartWorkItem: DispatchWorkItem?
    private var lastChartRequestStartedAt: TimeInterval?

    init(
        reader: DatabaseReader,
        policies: [RealPolicy],
        initialPolicyId: String?,
        runScopeIds: Set<String>? = nil,
        policyRevisionScopeId: String? = nil,
        onOpenRun: @escaping (RealRun, String) -> Void,
        onPolicyChange: @escaping (String?) -> Void,
        onRefreshCatalog: (() -> Void)? = nil,
        initiallyVisible: Bool = true
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
        self.onRefreshCatalog = onRefreshCatalog
        self.workspaceVisible = initiallyVisible
        super.init(frame: .zero)
        buildView()
        populatePolicies()

        if let initialPolicyId,
           policies.contains(where: { $0.policyId == initialPolicyId }) {
            selectPolicy(id: initialPolicyId)
            if workspaceVisible {
                reloadStrategy()
            } else {
                needsDataReloadWhenVisible = true
            }
        } else {
            policyPopup.selectItem(at: 0)
            showEmptySelection()
        }
    }

    deinit {
        chartStartWorkItem?.cancel()
        dataReadRequest?.cancel()
        activeChartRequest?.cancel()
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) { fatalError("init(coder:) is unavailable") }

    func setWorkspaceVisible(_ visible: Bool) {
        guard workspaceVisible != visible else { return }
        workspaceVisible = visible
        if !visible {
            generation &+= 1
            pauseChartQueue()
            if let request = dataReadRequest, request.isPending {
                dataGeneration &+= 1
                dataReadRequest = nil
                dataLoadInFlight = false
                needsDataReloadWhenVisible = selectedPolicyId != nil
                request.cancel()
                setLoading(false)
            }
            return
        }

        if needsDataReloadWhenVisible, selectedPolicyId != nil {
            needsDataReloadWhenVisible = false
            reloadStrategy()
        } else if needsRenderWhenVisible {
            needsRenderWhenVisible = false
            renderCurrentPage()
        } else {
            loadNextChart(generation: generation)
        }
    }

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
        title.font = TerminalTheme.screenTitle
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
        exportButton.bezelStyle = .rounded
        exportButton.toolTip = "현재 전략과 시장 조건의 전체 성과를 내보냅니다. 화면 페이지에 한정하지 않으며 실패 실행은 미확인 값으로 구분합니다."
        let combinedToolbar = TerminalTheme.textScale < 1.35
        let policyMinWidth = policyPopup.widthAnchor.constraint(greaterThanOrEqualToConstant: 260 * TerminalTheme.textScale)
        policyMinWidth.priority = combinedToolbar ? .defaultHigh : .required
        policyMinWidth.isActive = true
        marketPopup.widthAnchor.constraint(equalToConstant: 112 * TerminalTheme.textScale).isActive = true
        let identityControls = NSStackView(views: [policyPopup, marketPopup])
        identityControls.orientation = TerminalTheme.textScale >= 1.5 ? .vertical : .horizontal
        identityControls.alignment = TerminalTheme.textScale >= 1.5 ? .leading : .centerY
        identityControls.spacing = 8

        let displayControls = NSStackView(views: [modeControl, refreshButton, exportButton])
        displayControls.orientation = TerminalTheme.textScale >= 1.5 ? .vertical : .horizontal
        displayControls.alignment = TerminalTheme.textScale >= 1.5 ? .leading : .centerY
        displayControls.spacing = 8

        let controls = NSStackView(views: [identityControls, displayControls])
        controls.orientation = combinedToolbar ? .horizontal : .vertical
        controls.alignment = combinedToolbar ? .centerY : .leading
        controls.spacing = combinedToolbar ? 14 : 6

        statusLabel.font = TerminalTheme.monospacedDigitFont(ofSize: 12)
        statusLabel.textColor = .secondaryLabelColor
        statusLabel.maximumNumberOfLines = 0
        statusLabel.lineBreakMode = .byWordWrapping
        statusLabel.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)

        unavailableDisclosure.setButtonType(.pushOnPushOff)
        unavailableDisclosure.bezelStyle = .rounded
        unavailableDisclosure.font = TerminalTheme.systemFont(ofSize: 12, weight: .medium)
        unavailableDisclosure.target = self
        unavailableDisclosure.action = #selector(toggleUnavailableRuns(_:))
        unavailableDisclosure.setAccessibilityLabel("성과 요약이 없는 실행 목록")
        unavailableDisclosure.isHidden = true
        unavailablePopup.setAccessibilityLabel("실패 또는 성과 요약이 없는 실행")
        unavailablePopup.widthAnchor.constraint(greaterThanOrEqualToConstant: 360 * TerminalTheme.textScale).isActive = true
        unavailableButton.bezelStyle = .rounded
        unavailableButton.target = self
        unavailableButton.action = #selector(openUnavailableRun)
        unavailableControls.orientation = TerminalTheme.textScale >= 1.5 ? .vertical : .horizontal
        unavailableControls.alignment = TerminalTheme.textScale >= 1.5 ? .leading : .centerY
        unavailableControls.spacing = 8
        unavailableControls.addArrangedSubview(unavailablePopup)
        unavailableControls.addArrangedSubview(unavailableButton)
        unavailableControls.isHidden = true

        let unavailableSection = NSStackView(views: [unavailableDisclosure, unavailableControls])
        unavailableSection.orientation = .vertical
        unavailableSection.alignment = .leading
        unavailableSection.spacing = 6
        unavailableSection.detachesHiddenViews = true

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
        let pager = pagerRow
        for button in [previousButton, nextButton] {
            button.bezelStyle = .rounded
            button.controlSize = .small
        }
        pager.setViews([previousButton, pageLabel, nextButton], in: .center)
        pager.orientation = .horizontal
        pager.alignment = .centerY
        pager.spacing = 10
        pageLabel.alignment = .center
        pageLabel.font = TerminalTheme.monospacedDigitFont(ofSize: 12, weight: .medium)
        pageLabel.textColor = .secondaryLabelColor
        emptyState.isHidden = true

        for view in [title, subtitle, controls, statusLabel, unavailableSection, scrollView, emptyState, pager] {
            view.translatesAutoresizingMaskIntoConstraints = false
            addSubview(view)
        }
        NSLayoutConstraint.activate([
            title.topAnchor.constraint(equalTo: topAnchor, constant: 12),
            title.leadingAnchor.constraint(equalTo: leadingAnchor, constant: 20),
            title.trailingAnchor.constraint(lessThanOrEqualTo: trailingAnchor, constant: -20),

            subtitle.topAnchor.constraint(equalTo: title.bottomAnchor, constant: 4),
            subtitle.leadingAnchor.constraint(equalTo: title.leadingAnchor),
            subtitle.trailingAnchor.constraint(equalTo: trailingAnchor, constant: -20),

            controls.topAnchor.constraint(equalTo: subtitle.bottomAnchor, constant: 14),
            controls.leadingAnchor.constraint(equalTo: title.leadingAnchor),
            controls.trailingAnchor.constraint(lessThanOrEqualTo: trailingAnchor, constant: -20),

            statusLabel.topAnchor.constraint(equalTo: controls.bottomAnchor, constant: 10),
            statusLabel.leadingAnchor.constraint(equalTo: title.leadingAnchor),
            statusLabel.trailingAnchor.constraint(equalTo: trailingAnchor, constant: -20),

            unavailableSection.topAnchor.constraint(equalTo: statusLabel.bottomAnchor, constant: 8),
            unavailableSection.leadingAnchor.constraint(equalTo: title.leadingAnchor),
            unavailableSection.trailingAnchor.constraint(lessThanOrEqualTo: trailingAnchor, constant: -20),

            scrollView.topAnchor.constraint(equalTo: unavailableSection.bottomAnchor, constant: 10),
            scrollView.leadingAnchor.constraint(equalTo: title.leadingAnchor),
            scrollView.trailingAnchor.constraint(equalTo: trailingAnchor, constant: -20),
            scrollView.bottomAnchor.constraint(equalTo: pager.topAnchor, constant: -10),

            emptyState.leadingAnchor.constraint(equalTo: scrollView.leadingAnchor),
            emptyState.trailingAnchor.constraint(equalTo: scrollView.trailingAnchor),
            emptyState.topAnchor.constraint(equalTo: scrollView.topAnchor),
            emptyState.bottomAnchor.constraint(equalTo: scrollView.bottomAnchor),

            pager.centerXAnchor.constraint(equalTo: scrollView.centerXAnchor),
            pager.bottomAnchor.constraint(equalTo: bottomAnchor, constant: -12)
        ])
        let height = pager.heightAnchor.constraint(equalToConstant: 0)
        height.isActive = true
        pagerHeight = height
        pager.isHidden = true
    }

    /// Shows a centered explanation in place of the card grid; `nil` restores the grid.
    private func setEmptyState(_ title: String?, message: String = "") {
        emptyState.isHidden = title == nil
        guard let title else { return }
        emptyState.titleLabel.stringValue = title
        emptyState.messageLabel.stringValue = message
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
        if !workspaceVisible, window?.isVisible == true, window?.isMiniaturized == false {
            workspaceVisible = true
        }
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

    func updatePolicies(_ newPolicies: [RealPolicy]) {
        let sorted = newPolicies.sorted {
            let comparison = $0.name.localizedStandardCompare($1.name)
            return comparison == .orderedSame ? $0.policyId < $1.policyId : comparison == .orderedAscending
        }
        let previousSelected = selectedPolicyId
        policies = sorted
        populatePolicies()
        if let previousSelected,
           policies.contains(where: { $0.policyId == previousSelected }) {
            selectPolicy(id: previousSelected)
        } else {
            selectedPolicyId = nil
            policyPopup.selectItem(at: 0)
            if loadedPolicyId != nil {
                showEmptySelection()
            }
        }
    }

    @objc private func refreshRequested() {
        if !workspaceVisible, window?.isVisible == true, window?.isMiniaturized == false {
            workspaceVisible = true
        }
        onRefreshCatalog?()
        dataGeneration &+= 1
        generation &+= 1
        pageIndex = Self.pagerPresentation(
            currentPageIndex: pageIndex,
            pageCount: pageCount,
            resultCount: filteredRecords.count
        ).pageIndex
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
        guard workspaceVisible else {
            needsDataReloadWhenVisible = true
            return
        }

        let preservesReadyData = loadedPolicyId == policyId
        dataReadRequest?.cancel()
        dataReadRequest = nil
        if preservesReadyData {
            pauseChartQueue()
        } else {
            cancelChartQueue()
            loadedPolicyId = nil
            strategyRuns = []
            unavailableRuns = []
            allRecords = []
            updateUnavailableControls()
            gridView.replaceCards([])
            setEmptyState(nil)
        }

        let requestDataGeneration = dataGeneration
        let request = NativeMCPRequest()
        dataReadRequest = request
        dataLoadInFlight = true
        setLoading(true)
        let policyName = policies.first(where: { $0.policyId == policyId }).map(policyDisplayName) ?? "선택한 전략"
        statusLabel.stringValue = preservesReadyData
            ? "\(policyName) · 새로고침 중 · 이전에 확인한 결과를 유지합니다."
            : "전략의 실행과 모델 원장을 조회하고 있습니다…"
        let runScopeIds = self.runScopeIds
        let pinnedRunIds = runScopeIds?.sorted()
        let policyRevisionScopeId = self.policyRevisionScopeId
        let reader = self.reader

        workQueue.async { [weak self, request] in
            guard let self else { return }
            do {
                var runs: [RealRun] = []
                var cursor: RunHistoryCursor?
                repeat {
                    guard request.isPending else { return }
                    let page = try reader.fetchRunHistoryPage(
                        filter: RunHistoryFilter(
                            policyId: policyId,
                            policyRevisionId: policyRevisionScopeId,
                            pinnedRunIds: pinnedRunIds
                        ),
                        cursor: cursor,
                        limit: 100
                    )
                    guard request.isPending else { return }
                    runs.append(contentsOf: page.items)
                    cursor = page.nextCursor
                } while cursor != nil
                if let runScopeIds {
                    runs = runs.filter { runScopeIds.contains($0.id) }
                }
                if let policyRevisionScopeId {
                    runs = runs.filter { $0.policyRevisionIds?.contains(policyRevisionScopeId) == true }
                }

                var records: [CardRecord] = []
                var unavailableRuns: [RealRun] = []
                var batchStart = 0
                while batchStart < runs.count {
                    guard request.isPending else { return }
                    let batchEnd = min(batchStart + 100, runs.count)
                    let batchRuns = Array(runs[batchStart..<batchEnd])
                    let runIds = batchRuns.map(\.id)
                    let batch = try autoreleasepool {
                        let comparisons = try reader.fetchModelComparisons(for: runIds)
                        return Self.assembleCardRecords(
                            runs: batchRuns,
                            comparisons: comparisons,
                            policyId: policyId,
                            policyRevisionScopeId: policyRevisionScopeId
                        )
                    }
                    guard request.isPending else { return }
                    records.append(contentsOf: batch.records.map { CardRecord(run: $0.run, model: $0.model) })
                    unavailableRuns.append(contentsOf: batch.unavailableRuns)
                    batchStart = batchEnd
                }

                guard request.complete() else { return }
                DispatchQueue.main.async { [weak self] in
                    guard let self,
                          self.dataReadRequest === request,
                          self.dataGeneration == requestDataGeneration,
                          self.selectedPolicyId == policyId else { return }
                    self.dataReadRequest = nil
                    self.dataLoadInFlight = false
                    self.generation &+= 1
                    self.cancelChartQueue()
                    self.loadedPolicyId = policyId
                    self.strategyRuns = runs
                    self.unavailableRuns = unavailableRuns
                    self.allRecords = records
                    self.updateUnavailableControls()
                    self.setLoading(false)
                    if self.workspaceVisible {
                        self.needsRenderWhenVisible = false
                        self.renderCurrentPage()
                    } else {
                        self.needsRenderWhenVisible = true
                    }
                }
            } catch {
                DispatchQueue.main.async { [weak self] in
                    guard request.complete(), let self,
                          self.dataReadRequest === request,
                          self.dataGeneration == requestDataGeneration,
                          self.selectedPolicyId == policyId else { return }
                    self.dataReadRequest = nil
                    self.dataLoadInFlight = false
                    self.setLoading(false)
                    if self.loadedPolicyId == policyId {
                        self.statusLabel.stringValue = "\(self.summaryText(policyName: policyName, visibleResultCount: self.filteredRecords.count)) · 새로고침 실패 · 이전 결과 유지: \(error.localizedDescription)"
                    } else {
                        self.strategyRuns = []
                        self.unavailableRuns = []
                        self.allRecords = []
                        self.loadedPolicyId = nil
                        self.updateUnavailableControls()
                        self.gridView.replaceCards([])
                        self.setEmptyState("성과를 불러오지 못했습니다", message: error.localizedDescription)
                        self.statusLabel.stringValue = error.localizedDescription
                    }
                    self.updatePager()
                    if self.workspaceVisible {
                        self.loadNextChart(generation: self.generation)
                    } else {
                        self.needsRenderWhenVisible = true
                    }
                }
            }
        }
    }

    static func assembleCardRecords(
        runs: [RealRun],
        comparisons: [RealModelComparison],
        policyId: String,
        policyRevisionScopeId: String?
    ) -> (records: [(run: RealRun, model: RealModelComparison)], unavailableRuns: [RealRun]) {
        let comparisonsByRun = Dictionary(grouping: comparisons, by: \.runId)
        var records: [(run: RealRun, model: RealModelComparison)] = []
        var runIdsWithResults = Set<String>()
        for run in runs {
            for model in comparisonsByRun[run.id] ?? [] where
                model.policyId == policyId &&
                (policyRevisionScopeId == nil || model.policyRevisionId == policyRevisionScopeId) {
                records.append((run: run, model: model))
                runIdsWithResults.insert(run.id)
            }
        }
        return (records, runs.filter { !runIdsWithResults.contains($0.id) })
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
        pageIndex = Self.pagerPresentation(
            currentPageIndex: pageIndex,
            pageCount: pageCount,
            resultCount: records.count
        ).pageIndex
        if records.isEmpty {
            gridView.replaceCards([])
            statusLabel.stringValue = summaryText(policyName: policyName, visibleResultCount: 0)
            setEmptyState(
                "표시할 성과가 없습니다",
                message: unavailableRuns.isEmpty
                    ? "이 조건에 해당하는 실행 결과가 아직 없습니다."
                    : "성과 요약이 없는 실행만 있습니다. 위의 ‘성과 없는 실행’에서 근거를 확인하세요."
            )
            updatePager()
            return
        }
        setEmptyState(nil)

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
        if dataLoadInFlight {
            value += loadedPolicyId == selectedPolicyId ? " · 저장된 결과 표시 · 새로고침 중" : " · 선택 정책 조회 중"
        }
        return value
    }

    private func updateUnavailableControls() {
        unavailablePopup.removeAllItems()
        if unavailableRuns.isEmpty { unavailableExpanded = false }
        for run in unavailableRuns {
            let state = run.state == "FAILED" ? "실패" : "\(TerminalPresentation.state(run.state)) · 성과 요약 없음"
            let markets = run.markets?.map(Self.shortMarket).joined(separator: "/") ?? "시장 미확인"
            let title = "\(state) · \(run.createdDateStr) KST · \(markets) · \(run.costAssumptionLabel ?? "비용 미확인")"
            unavailablePopup.addItem(withTitle: title)
            unavailablePopup.lastItem?.toolTip = "실행 ID \(run.id)\n\(run.windowStartUTC ?? "시작 미확인") → \(run.windowEndUTC ?? "종료 미확인") UTC"
        }
        unavailableDisclosure.isHidden = unavailableRuns.isEmpty
        unavailableDisclosure.title = "성과 없는 실행 · \(unavailableRuns.count)개"
        unavailableDisclosure.state = unavailableExpanded ? .on : .off
        unavailableDisclosure.setAccessibilityValue("\(unavailableRuns.count)개 · \(unavailableExpanded ? "펼쳐짐" : "접힘")")
        unavailableControls.isHidden = unavailableRuns.isEmpty || !unavailableExpanded
        unavailableButton.isEnabled = !unavailableRuns.isEmpty
    }

    @objc private func toggleUnavailableRuns(_ sender: NSButton) {
        unavailableExpanded = sender.state == .on
        updateUnavailableControls()
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
        let presentation = Self.pagerPresentation(
            currentPageIndex: pageIndex,
            pageCount: pages,
            resultCount: filteredRecords.count
        )
        pageIndex = presentation.pageIndex
        pageLabel.stringValue = presentation.label
        previousButton.isEnabled = presentation.previousEnabled
        nextButton.isEnabled = presentation.nextEnabled
        let showsPager = pages > 1
        pagerRow.isHidden = !showsPager
        pagerHeight?.constant = showsPager ? 26 : 0
    }

    static func pagerPresentation(
        currentPageIndex: Int,
        pageCount: Int,
        resultCount: Int
    ) -> (pageIndex: Int, label: String, previousEnabled: Bool, nextEnabled: Bool) {
        let pages = max(pageCount, 0)
        let pageIndex = min(max(currentPageIndex, 0), max(pages - 1, 0))
        return (
            pageIndex: pageIndex,
            label: pages == 0 ? "표시할 결과 없음" : "\(pageIndex + 1) / \(pages) · 총 \(resultCount)개",
            previousEnabled: pageIndex > 0,
            nextEnabled: pageIndex + 1 < pages
        )
    }

    private func showEmptySelection() {
        if let request = dataReadRequest {
            dataGeneration &+= 1
            request.cancel()
        }
        dataReadRequest = nil
        dataLoadInFlight = false
        loadedPolicyId = nil
        needsDataReloadWhenVisible = false
        needsRenderWhenVisible = false
        cancelChartQueue()
        selectedPolicyId = nil
        strategyRuns = []
        unavailableRuns = []
        allRecords = []
        updateUnavailableControls()
        gridView.replaceCards([])
        setEmptyState(
            "전략을 선택하세요",
            message: "정책을 고르면 실행·시장별 자산 곡선과 핵심 지표가 카드로 표시됩니다."
        )
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
        refreshButton.isEnabled = !loading
        exportButton.isEnabled = !loading && selectedPolicyId != nil && (!allRecords.isEmpty || !unavailableRuns.isEmpty)
        marketPopup.isEnabled = true
    }

    private static func shortRevision(_ raw: String) -> String {
        let prefix = "policy-revision-"
        let identity = raw.hasPrefix(prefix) ? String(raw.dropFirst(prefix.count)) : raw
        return String(identity.prefix(8))
    }

    private func cancelChartQueue() {
        chartStartWorkItem?.cancel()
        chartStartWorkItem = nil
        chartQueue.removeAll()
        activeChartItem = nil
        activeChartRequest?.cancel()
    }

    private func pauseChartQueue() {
        chartStartWorkItem?.cancel()
        chartStartWorkItem = nil
        if let activeChartItem {
            chartQueue.insert(activeChartItem, at: 0)
            self.activeChartItem = nil
        }
        activeChartRequest?.cancel()
    }

    private func loadNextChart(generation requestGeneration: UInt64) {
        guard workspaceVisible, !chartRequestActive, generation == requestGeneration,
              !chartQueue.isEmpty else { return }
        if chartQueue[0].1.run.ledgerState == "COMPACTED" {
            let now = ProcessInfo.processInfo.systemUptime
            let delay = StrategyChartReadPacing.delay(lastStartedAt: lastChartRequestStartedAt, now: now)
            if delay > 0 {
                guard chartStartWorkItem == nil else { return }
                let workItem = DispatchWorkItem { [weak self] in
                    guard let self else { return }
                    self.chartStartWorkItem = nil
                    self.loadNextChart(generation: requestGeneration)
                }
                chartStartWorkItem = workItem
                DispatchQueue.main.asyncAfter(deadline: .now() + delay, execute: workItem)
                return
            }
            lastChartRequestStartedAt = now
        } else {
            chartStartWorkItem?.cancel()
            chartStartWorkItem = nil
        }
        chartRequestActive = true
        let (card, record) = chartQueue.removeFirst()
        activeChartItem = (card, record)
        card.showChartLoading()
        let requestDataGeneration = dataGeneration

        if record.run.ledgerState == "COMPACTED" {
            activeChartRequest = ledgerReader.marks(
                runId: record.run.id,
                modelId: record.model.modelId,
                expectedCount: record.model.accountMarkCount
            ) { [weak self, weak card] result in
                DispatchQueue.main.async {
                    guard let self else { return }
                    self.finishChartRead(
                        generation: requestGeneration,
                        dataGeneration: requestDataGeneration,
                        card: card,
                        record: record,
                        result: result
                    )
                }
            }
        } else {
            let request = NativeMCPRequest()
            activeChartRequest = request
            request.setCancellationHandler { [weak self, weak card] in
                DispatchQueue.main.async {
                    guard let self else { return }
                    self.finishChartRead(
                        generation: requestGeneration,
                        dataGeneration: requestDataGeneration,
                        card: card,
                        record: record,
                        result: .failure(NativeMCPError.cancelled)
                    )
                }
            }
            workQueue.async { [weak self, weak card] in
                guard let self else { return }
                guard request.isPending else { return }
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
                guard request.complete() else { return }
                DispatchQueue.main.async {
                    self.finishChartRead(
                        generation: requestGeneration,
                        dataGeneration: requestDataGeneration,
                        card: card,
                        record: record,
                        result: result
                    )
                }
            }
        }
    }

    private func finishChartRead(
        generation requestGeneration: UInt64,
        dataGeneration requestDataGeneration: UInt64,
        card: StrategyPerformanceCard?,
        record: CardRecord,
        result: Result<[RealAccountMark], Error>
    ) {
        activeChartRequest = nil
        activeChartItem = nil
        chartRequestActive = false
        if workspaceVisible,
           generation == requestGeneration,
           dataGeneration == requestDataGeneration,
           let card {
            card.applyChartResult(result, totalCount: record.model.accountMarkCount)
        }
        if workspaceVisible {
            loadNextChart(generation: generation)
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

/// One KPI: small secondary label over a tabular-digit value. Unknown values stay textual, never zero.
private final class StrategyMetricTile: NSView {
    static var height: CGFloat { ceil(50 * TerminalTheme.textScale) }

    init(title: String, value: String, tone: NSColor, toolTip: String?) {
        super.init(frame: .zero)
        let titleLabel = NSTextField(labelWithString: title)
        titleLabel.font = TerminalTheme.systemFont(ofSize: 11, weight: .medium)
        titleLabel.textColor = .secondaryLabelColor
        let valueLabel = NSTextField(labelWithString: value)
        valueLabel.font = TerminalTheme.monospacedDigitFont(ofSize: 15, weight: .semibold)
        valueLabel.textColor = tone
        valueLabel.lineBreakMode = .byTruncatingTail
        valueLabel.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        let stack = NSStackView(views: [titleLabel, valueLabel])
        stack.orientation = .vertical
        stack.alignment = .leading
        stack.spacing = 3
        stack.translatesAutoresizingMaskIntoConstraints = false
        addSubview(stack)
        NSLayoutConstraint.activate([
            heightAnchor.constraint(equalToConstant: Self.height),
            stack.leadingAnchor.constraint(equalTo: leadingAnchor, constant: 10),
            stack.trailingAnchor.constraint(lessThanOrEqualTo: trailingAnchor, constant: -8),
            stack.centerYAnchor.constraint(equalTo: centerYAnchor)
        ])
        self.toolTip = toolTip
        setAccessibilityElement(false)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) { nil }

    override var wantsDefaultClipping: Bool { true }

    override func draw(_ dirtyRect: NSRect) {
        TerminalTheme.surface.setFill()
        NSBezierPath(roundedRect: bounds, xRadius: 6, yRadius: 6).fill()
    }

    override func viewDidChangeEffectiveAppearance() {
        super.viewDidChangeEffectiveAppearance()
        needsDisplay = true
    }
}

private final class StrategyPerformanceCard: NSView {
    private let titleLabel: NSTextField
    private let periodLabel: NSTextField
    private let metricsGrid: NSStackView
    private let detailButton: ClosureButton
    private let chartView = EquityChartView()
    private let chartStatus = NSTextField(labelWithString: "차트 대기")
    private let initialEquity: Double
    private let onOpen: () -> Void

    private static var metricRows: Int { TerminalTheme.textScale >= 1.5 ? 2 : 1 }
    private static var metricsHeight: CGFloat {
        CGFloat(metricRows) * StrategyMetricTile.height + CGFloat(metricRows - 1) * 8
    }

    init(
        policyName: String,
        run: RealRun,
        model: RealModelComparison,
        chartMode: ChartMode,
        onOpen: @escaping () -> Void
    ) {
        self.onOpen = onOpen
        initialEquity = model.initialEquity
        let title = NSTextField(labelWithString: "\(policyName) · \(Self.shortMarket(model.market))")
        let period = NSTextField(wrappingLabelWithString:
            "\(Self.periodText(run, model: model)) · 체결 \(model.fillCount)건 · 청산 \(model.closedEpisodes)건"
        )
        let returnText = model.netReturnDecimal.map { Self.percent($0, signed: true) }
            ?? (model.netReturnNullReason.map { "산출 불가 · \($0)" } ?? "산출 불가")
        let mddText = model.maxDrawdownDecimal.map { Self.percent($0) }
            ?? (model.maxDrawdownNullReason.map { "산출 불가 · \($0)" } ?? "산출 불가")
        let pnlText = Formatters.money(model.netPnlDecimal, signed: true)
        let feesText = Formatters.money(model.cumulativeFeesDecimal)
        let returnRaw = model.netReturnDecimal?.description ?? (model.netReturnNullReason.map { "null(\($0))" } ?? "null")
        let mddRaw = model.maxDrawdownDecimal?.description ?? (model.maxDrawdownNullReason.map { "null(\($0))" } ?? "null")

        func signTone(_ value: Decimal?) -> NSColor {
            guard let value else { return .tertiaryLabelColor }
            return value > 0 ? TerminalTheme.positive : (value < 0 ? TerminalTheme.negative : .labelColor)
        }
        let tiles = [
            StrategyMetricTile(title: "수익률", value: model.netReturnDecimal == nil ? "산출 불가" : returnText,
                               tone: signTone(model.netReturnDecimal), toolTip: "net_return=\(returnRaw)"),
            StrategyMetricTile(title: "최대 낙폭", value: model.maxDrawdownDecimal == nil ? "산출 불가" : mddText,
                               tone: model.maxDrawdownDecimal.map { $0 > 0 ? TerminalTheme.negative : .labelColor } ?? .tertiaryLabelColor,
                               toolTip: "max_drawdown=\(mddRaw)"),
            StrategyMetricTile(title: "순손익", value: pnlText, tone: signTone(model.netPnlDecimal),
                               toolTip: "net_pnl=\(model.netPnlDecimal)"),
            StrategyMetricTile(title: "수수료", value: feesText, tone: .labelColor,
                               toolTip: "fees=\(model.cumulativeFeesDecimal)")
        ]
        func metricRow(_ items: [NSView]) -> NSStackView {
            let row = NSStackView(views: items)
            row.orientation = .horizontal
            row.distribution = .fillEqually
            row.spacing = 8
            return row
        }
        let grid = Self.metricRows == 1
            ? NSStackView(views: [metricRow(tiles)])
            : NSStackView(views: [metricRow(Array(tiles.prefix(2))), metricRow(Array(tiles.suffix(2)))])
        grid.orientation = .vertical
        grid.alignment = .leading
        grid.spacing = 8
        for row in grid.arrangedSubviews {
            row.widthAnchor.constraint(equalTo: grid.widthAnchor).isActive = true
        }

        let button = ClosureButton(title: "실행 상세", action: onOpen)
        titleLabel = title
        periodLabel = period
        metricsGrid = grid
        detailButton = button
        super.init(frame: .zero)
        wantsLayer = true
        layer?.cornerRadius = 8
        layer?.masksToBounds = true
        layer?.borderWidth = 1
        layer?.borderColor = TerminalTheme.border.cgColor

        title.font = TerminalTheme.systemFont(ofSize: 14, weight: .semibold)
        title.lineBreakMode = .byTruncatingTail
        title.setContentCompressionResistancePriority(.required, for: .vertical)
        title.toolTip = "정책 ID \(model.policyId ?? "미확인") · 실행 ID \(run.id) · 모델 ID \(model.modelId)"

        detailButton.bezelStyle = .rounded
        detailButton.controlSize = .small
        detailButton.image = NSImage(systemSymbolName: "chevron.right", accessibilityDescription: nil)
        detailButton.imagePosition = .imageTrailing
        detailButton.toolTip = "실행 \(run.id) · 모델 \(model.modelId)"
        detailButton.setAccessibilityLabel("실행 상세 · \(Self.shortMarket(model.market)) · \(run.id)")

        let clickRecognizer = NSClickGestureRecognizer(target: self, action: #selector(handleCardClick(_:)))
        clickRecognizer.delaysPrimaryMouseButtonEvents = false
        addGestureRecognizer(clickRecognizer)

        period.font = TerminalTheme.monospacedDigitFont(ofSize: 11)
        period.textColor = .secondaryLabelColor
        period.maximumNumberOfLines = 0
        period.lineBreakMode = .byWordWrapping
        period.setContentCompressionResistancePriority(.required, for: .vertical)
        period.setContentHuggingPriority(.required, for: .vertical)
        period.toolTip = "실행 ID \(run.id)\n모델 ID \(model.modelId)\n정책 revision \(model.policyRevisionId ?? "미확인")"

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
        chartStatus.textColor = .tertiaryLabelColor
        chartStatus.maximumNumberOfLines = 0
        chartStatus.lineBreakMode = .byWordWrapping
        chartStatus.setContentCompressionResistancePriority(.required, for: .vertical)

        for view in [title, detailButton, period, grid, chartView, chartStatus] {
            view.translatesAutoresizingMaskIntoConstraints = false
            addSubview(view)
        }
        NSLayoutConstraint.activate([
            title.topAnchor.constraint(equalTo: topAnchor, constant: 14),
            title.leadingAnchor.constraint(equalTo: leadingAnchor, constant: 14),
            title.trailingAnchor.constraint(lessThanOrEqualTo: detailButton.leadingAnchor, constant: -8),

            detailButton.centerYAnchor.constraint(equalTo: title.centerYAnchor),
            detailButton.trailingAnchor.constraint(equalTo: trailingAnchor, constant: -12),

            period.topAnchor.constraint(equalTo: title.bottomAnchor, constant: 4),
            period.leadingAnchor.constraint(equalTo: title.leadingAnchor),
            period.trailingAnchor.constraint(equalTo: trailingAnchor, constant: -14),

            grid.topAnchor.constraint(equalTo: period.bottomAnchor, constant: 10),
            grid.leadingAnchor.constraint(equalTo: title.leadingAnchor),
            grid.trailingAnchor.constraint(equalTo: trailingAnchor, constant: -14),
            grid.heightAnchor.constraint(equalToConstant: Self.metricsHeight),

            chartView.topAnchor.constraint(equalTo: grid.bottomAnchor, constant: 8),
            chartView.leadingAnchor.constraint(equalTo: leadingAnchor, constant: 6),
            chartView.trailingAnchor.constraint(equalTo: trailingAnchor, constant: -10),
            chartView.bottomAnchor.constraint(equalTo: chartStatus.topAnchor, constant: -4),

            chartStatus.leadingAnchor.constraint(equalTo: title.leadingAnchor),
            chartStatus.trailingAnchor.constraint(equalTo: trailingAnchor, constant: -14),
            chartStatus.bottomAnchor.constraint(equalTo: bottomAnchor, constant: -10)
        ])
    }

    override func setFrameSize(_ newSize: NSSize) {
        updatePreferredTextWidths(for: newSize.width)
        super.setFrameSize(newSize)
    }

    @objc private func handleCardClick(_ recognizer: NSClickGestureRecognizer) {
        let location = recognizer.location(in: self)
        if detailButton.frame.contains(location) { return }
        if chartView.frame.contains(location) && (NSApp.currentEvent?.clickCount ?? 1) < 2 { return }
        onOpen()
    }

    override func layout() {
        updatePreferredTextWidths(for: bounds.width)
        super.layout()
    }

    private func updatePreferredTextWidths(for cardWidth: CGFloat) {
        guard cardWidth > 28 else { return }
        let textWidth = cardWidth - 28
        for label in [periodLabel, chartStatus] {
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
        let statusH = Self.measuredTextHeight(chartStatus.stringValue, font: chartStatus.font, width: textWidth)
        let minChartH = max(168, ceil(168 * TerminalTheme.textScale))
        let needed = ceil(14 + titleH + 4 + periodH + 10 + Self.metricsHeight + 8 + minChartH + 4 + statusH + 10)
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
