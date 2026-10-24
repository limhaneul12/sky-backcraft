import AppKit

/// Native read-only policy inventory. Registration and historical use never imply an active session.
final class PolicyInventoryView: NSView, NSTableViewDataSource, NSTableViewDelegate, NSSearchFieldDelegate {
    private let reader: DatabaseReader
    private let policies: [RealPolicy]
    private let duplicatePolicyNames: Set<String>
    private let detailReader: PolicyDetailMCPReader
    private let onOpenRun: (RealRun, String, String) -> Void
    private let onShowStrategy: (String) -> Void
    private let workQueue = DispatchQueue(label: "dev.skybackcraft.policy-inventory-reader")

    private let searchField = NSSearchField()
    private let originPopup = NSPopUpButton()
    private let usagePopup = NSPopUpButton()
    private let marketPopup = NSPopUpButton()
    private let countLabel = NSTextField(labelWithString: "")
    private let marketStatusLabel = NSTextField(wrappingLabelWithString: "")
    private let policyTable = NSTableView()
    private let detailScroll = NSScrollView()
    private let detailTitle = NSTextField(labelWithString: "정책을 선택하세요")
    private let detailIdentity = NSTextField(wrappingLabelWithString: "정책을 선택하면 정책 ID와 리비전 ID가 표시됩니다.")
    private let detailMeta = NSTextField(wrappingLabelWithString: "등록된 정책은 실행 중인 세션을 뜻하지 않습니다.")
    private let definitionText = NSTextView()
    private let revisionTable = NSTableView()
    private let runTable = NSTableView()
    private let relatedRunStatusLabel = NSTextField(wrappingLabelWithString: "")
    private let openRunButton = NSButton(title: "선택 실행 열기", target: nil, action: nil)
    private let showStrategyButton = NSButton(title: "성과에서 보기", target: nil, action: nil)
    private var filteredPolicies: [RealPolicy] = []
    private var selectedPolicyIds: Set<String> = []
    private var focusedPolicyId: String?
    private var marketsByPolicy: [String: Set<String>] = [:]
    private var currentDetail: PolicyDetailSnapshot?
    private var allPolicyRuns: [RealRun] = []
    private var relatedRuns: [RealRun] = []
    private var relatedRunsLoaded = false
    private var relatedRunsError: String?
    private var generation: UInt64 = 0
    private var revisionGeneration: UInt64 = 0
    private var restoringSelection = false
    private var restoringRevisionSelection = false
    private var lastInspectorViewportSize: NSSize?

    init(reader: DatabaseReader, policies: [RealPolicy],
         onOpenRun: @escaping (RealRun, String, String) -> Void,
         onShowStrategy: @escaping (String) -> Void) {
        self.reader = reader
        self.policies = policies.sorted { lhs, rhs in
            let compared = lhs.name.localizedStandardCompare(rhs.name)
            return compared == .orderedSame ? lhs.policyId < rhs.policyId : compared == .orderedAscending
        }
        duplicatePolicyNames = Set(Dictionary(grouping: policies, by: \.name).compactMap { name, matches in
            matches.count > 1 ? name : nil
        })
        detailReader = PolicyDetailMCPReader()
        self.onOpenRun = onOpenRun
        self.onShowStrategy = onShowStrategy
        super.init(frame: .zero)
        buildView()
        filteredPolicies = self.policies
        policyTable.reloadData()
        updateCount()
        if self.policies.isEmpty { showEmptyDetail() }
        loadMarketIndex()
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) { fatalError("init(coder:) is unavailable") }

    override var wantsDefaultClipping: Bool { true }
    override func draw(_ dirtyRect: NSRect) { TerminalTheme.surface.setFill(); bounds.fill() }
    override func viewDidChangeEffectiveAppearance() { super.viewDidChangeEffectiveAppearance(); needsDisplay = true }

    override func layout() {
        super.layout()
        let viewportSize = detailScroll.contentView.bounds.size
        guard viewportSize.width > 0, viewportSize.height > 0,
              lastInspectorViewportSize != viewportSize else { return }
        lastInspectorViewportSize = viewportSize
        scrollInspectorToTop()
    }

    override func viewDidMoveToWindow() {
        super.viewDidMoveToWindow()
        guard window != nil else { return }
        DispatchQueue.main.async { [weak self] in
            guard let self, self.window != nil else { return }
            self.scrollInspectorToTop()
        }
    }

    private func scrollInspectorToTop() {
        guard detailScroll.documentView != nil else { return }
        detailScroll.layoutSubtreeIfNeeded()
        detailScroll.contentView.scroll(to: .zero)
        detailScroll.reflectScrolledClipView(detailScroll.contentView)
    }

    private func buildView() {
        let title = NSTextField(labelWithString: "정책 카탈로그")
        title.font = TerminalTheme.screenTitle
        title.setAccessibilityLabel("정책 카탈로그")
        title.setAccessibilityValue(title.stringValue)
        let subtitle = NSTextField(wrappingLabelWithString: "불변 정책과 리비전, 과거 원장 참조를 확인합니다. 과거 실행·일시중지 수집은 지속모의 또는 실거래 활성 상태가 아닙니다.")
        subtitle.font = TerminalTheme.body
        subtitle.textColor = TerminalTheme.secondaryText

        searchField.placeholderString = "이름 · 정책 ID · 분류 검색"
        searchField.delegate = self
        searchField.setAccessibilityLabel("정책 검색")
        originPopup.addItems(withTitles: ["전체 출처", "내장", "사용자"])
        originPopup.setAccessibilityLabel("정책 출처 필터")
        usagePopup.addItems(withTitles: ["전체 상태", "과거 원장 참조", "미사용"])
        usagePopup.setAccessibilityLabel("정책 상태 필터")
        marketPopup.addItems(withTitles: ["전체 시장", "KRW-BTC", "KRW-ETH", "KRW-XRP"])
        marketPopup.setAccessibilityLabel("정책 시장 필터")
        marketPopup.isEnabled = false
        marketPopup.toolTip = "정책별 시장 참조를 계산하는 중입니다."
        for popup in [originPopup, usagePopup, marketPopup] {
            popup.target = self; popup.action = #selector(filtersChanged)
        }
        countLabel.textColor = TerminalTheme.secondaryText
        countLabel.alignment = .right
        countLabel.setAccessibilityLabel("정책 카탈로그 결과와 선택 수")
        searchField.setContentHuggingPriority(.defaultLow, for: .horizontal)
        let searchRow = NSStackView(views: [searchField, countLabel])
        searchRow.orientation = .horizontal; searchRow.alignment = .centerY; searchRow.spacing = 8
        let filters = NSStackView(views: [originPopup, usagePopup, marketPopup])
        let filtersStackVertically = TerminalTheme.textScale >= 1.5
        filters.orientation = filtersStackVertically ? .vertical : .horizontal
        filters.alignment = filtersStackVertically ? .leading : .centerY
        filters.spacing = 8
        if filtersStackVertically {
            for popup in [originPopup, usagePopup, marketPopup] {
                popup.widthAnchor.constraint(equalTo: filters.widthAnchor).isActive = true
            }
        }
        marketStatusLabel.stringValue = "정책별 관련 원장을 확인하는 중입니다."
        marketStatusLabel.font = TerminalTheme.systemFont(ofSize: 12)
        marketStatusLabel.textColor = TerminalTheme.secondaryText
        marketStatusLabel.setAccessibilityLabel("정책 시장 필터 상태")
        marketStatusLabel.setAccessibilityValue(marketStatusLabel.stringValue)
        let controls = NSStackView(views: [searchRow, filters, marketStatusLabel])
        controls.orientation = .vertical; controls.alignment = .leading; controls.spacing = 6
        searchField.widthAnchor.constraint(greaterThanOrEqualToConstant: max(230, 230 * TerminalTheme.textScale)).isActive = true
        countLabel.setContentHuggingPriority(.required, for: .horizontal)

        configurePolicyTable()
        policyTable.setAccessibilityLabel("정책 카탈로그 테이블")
        let policyScroll = scroll(for: policyTable)
        policyScroll.setAccessibilityLabel("정책 목록")

        detailTitle.font = TerminalTheme.systemFont(ofSize: 17, weight: .semibold)
        detailTitle.setAccessibilityLabel("선택된 정책 이름")
        detailTitle.setAccessibilityValue(detailTitle.stringValue)
        detailIdentity.font = TerminalTheme.monospacedFont(ofSize: 10, weight: .regular)
        detailIdentity.textColor = TerminalTheme.secondaryText
        detailIdentity.setAccessibilityLabel("선택된 정책과 리비전의 불변 ID")
        detailIdentity.setAccessibilityValue(detailIdentity.stringValue)
        detailMeta.font = TerminalTheme.systemFont(ofSize: 12)
        detailMeta.textColor = TerminalTheme.secondaryText
        detailMeta.setAccessibilityLabel("선택 정책 분류와 상세 조회 상태")
        detailMeta.setAccessibilityValue(detailMeta.stringValue)
        definitionText.isEditable = false
        definitionText.isSelectable = true
        definitionText.font = TerminalTheme.monospacedFont(ofSize: 11, weight: .regular)
        definitionText.textColor = .labelColor
        definitionText.backgroundColor = TerminalTheme.chart
        definitionText.setAccessibilityLabel("선택 정책 리비전 정의 JSON")
        let definitionScroll = TerminalWrappingTextScrollView()
        definitionScroll.configureWrapping(textView: definitionText, inset: NSSize(width: 10, height: 10))
        definitionScroll.borderType = .bezelBorder
        definitionScroll.heightAnchor.constraint(greaterThanOrEqualToConstant: max(135, 135 * TerminalTheme.textScale)).isActive = true

        configureRevisionTable()
        configureRunTable()
        revisionTable.setAccessibilityLabel("정책 리비전 기록 테이블")
        runTable.setAccessibilityLabel("정책 관련 원장 실행 테이블")
        let revisionScroll = scroll(for: revisionTable)
        revisionScroll.heightAnchor.constraint(greaterThanOrEqualToConstant: max(68, TerminalTheme.tableRowHeight * 2)).isActive = true
        let revisions = labeledSection("리비전 기록", content: revisionScroll, minimumHeight: 96)
        relatedRunStatusLabel.font = TerminalTheme.systemFont(ofSize: 11)
        relatedRunStatusLabel.textColor = TerminalTheme.secondaryText
        relatedRunStatusLabel.setAccessibilityLabel("선택 리비전의 관련 실행 조회 상태")
        relatedRunStatusLabel.stringValue = "정책을 선택하면 관련 실행을 확인할 수 있습니다."
        relatedRunStatusLabel.setAccessibilityValue(relatedRunStatusLabel.stringValue)
        let runScroll = scroll(for: runTable)
        runScroll.heightAnchor.constraint(greaterThanOrEqualToConstant: max(90, TerminalTheme.tableRowHeight * 2)).isActive = true
        let runContent = NSStackView(views: [runScroll, relatedRunStatusLabel])
        runContent.orientation = .vertical; runContent.alignment = .leading; runContent.spacing = 4
        let runs = labeledSection("관련 원장 실행", content: runContent, minimumHeight: 145)
        openRunButton.target = self; openRunButton.action = #selector(openSelectedRun)
        openRunButton.setAccessibilityLabel("선택된 관련 원장 실행 열기")
        showStrategyButton.target = self; showStrategyButton.action = #selector(showSelectedStrategy)
        showStrategyButton.setAccessibilityLabel("선택된 정책을 전략별 성과에서 보기")
        openRunButton.isEnabled = false; showStrategyButton.isEnabled = false
        let actions = NSStackView(views: [showStrategyButton, openRunButton])
        actions.orientation = TerminalTheme.textScale >= 1.5 ? .vertical : .horizontal
        actions.alignment = TerminalTheme.textScale >= 1.5 ? .leading : .centerY
        actions.spacing = 8
        if TerminalTheme.textScale >= 1.5 {
            for button in [showStrategyButton, openRunButton] {
                button.widthAnchor.constraint(equalTo: actions.widthAnchor).isActive = true
            }
        }

        let detailStack = NSStackView(views: [detailTitle, detailIdentity, detailMeta, definitionScroll, revisions, runs, actions])
        detailStack.orientation = .vertical; detailStack.alignment = .leading; detailStack.spacing = 10
        for view in [detailTitle, detailIdentity, detailMeta, definitionScroll, revisions, runs, actions] {
            view.widthAnchor.constraint(equalTo: detailStack.widthAnchor).isActive = true
        }
        let detailContainer = TerminalFlippedDocumentView()
        detailContainer.usesPanel = true
        detailContainer.translatesAutoresizingMaskIntoConstraints = false
        detailStack.translatesAutoresizingMaskIntoConstraints = false; detailContainer.addSubview(detailStack)
        NSLayoutConstraint.activate([
            detailStack.leadingAnchor.constraint(equalTo: detailContainer.leadingAnchor, constant: 14),
            detailStack.trailingAnchor.constraint(equalTo: detailContainer.trailingAnchor, constant: -14),
            detailStack.topAnchor.constraint(equalTo: detailContainer.topAnchor, constant: 14),
            detailStack.bottomAnchor.constraint(equalTo: detailContainer.bottomAnchor, constant: -14)
        ])

        detailScroll.documentView = detailContainer
        detailScroll.hasVerticalScroller = true
        detailScroll.hasHorizontalScroller = false
        detailScroll.autohidesScrollers = true
        detailScroll.drawsBackground = false
        detailScroll.setAccessibilityLabel("선택 정책 상세 및 리비전 검사기")
        detailContainer.widthAnchor.constraint(equalTo: detailScroll.contentView.widthAnchor).isActive = true

        let split = NSSplitView()
        split.isVertical = true; split.dividerStyle = .thin
        split.setAccessibilityLabel("정책 카탈로그 목록과 선택 상세 분할 보기")
        split.addArrangedSubview(policyScroll); split.addArrangedSubview(detailScroll)
        policyScroll.widthAnchor.constraint(greaterThanOrEqualToConstant: 390).isActive = true
        detailScroll.widthAnchor.constraint(greaterThanOrEqualToConstant: 430).isActive = true

        let root = NSStackView(views: [title, subtitle, controls, split])
        root.orientation = .vertical; root.alignment = .leading; root.spacing = 10
        root.translatesAutoresizingMaskIntoConstraints = false; addSubview(root)
        for view in [title, subtitle, controls, split] { view.widthAnchor.constraint(equalTo: root.widthAnchor).isActive = true }
        for view in [searchRow, filters, marketStatusLabel] {
            view.widthAnchor.constraint(equalTo: controls.widthAnchor).isActive = true
        }
        searchField.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        NSLayoutConstraint.activate([
            root.leadingAnchor.constraint(equalTo: leadingAnchor, constant: 20),
            root.trailingAnchor.constraint(equalTo: trailingAnchor, constant: -20),
            root.topAnchor.constraint(equalTo: topAnchor, constant: 18),
            root.bottomAnchor.constraint(equalTo: bottomAnchor, constant: -18)
        ])
    }

    private func configurePolicyTable() {
        let columns: [(String, String, CGFloat)] = [
            ("Name", "정책", 210), ("PolicyID", "정책 ID", 220), ("Origin", "출처", 72), ("Usage", "상태", 118)
        ]
        for (id, title, width) in columns {
            let column = NSTableColumn(identifier: NSUserInterfaceItemIdentifier(id))
            column.title = title
            column.width = width
            if id == "Name" {
                column.minWidth = 175
                column.resizingMask = .autoresizingMask
            } else {
                column.minWidth = width
                column.maxWidth = width
                column.resizingMask = []
            }
            policyTable.addTableColumn(column)
        }
        policyTable.dataSource = self; policyTable.delegate = self
        policyTable.target = self
        policyTable.doubleAction = #selector(showSelectedStrategy)
        policyTable.allowsMultipleSelection = true
        TerminalTheme.prepareTable(policyTable)
        policyTable.allowsMultipleSelection = true
        policyTable.columnAutoresizingStyle = .firstColumnOnlyAutoresizingStyle
    }

    private func configureRevisionTable() {
        for (id, title, width) in [("Revision", "리비전", 64), ("Created", "생성 UTC", 170), ("Digest", "정의 digest", 250)] {
            let column = NSTableColumn(identifier: NSUserInterfaceItemIdentifier(id))
            column.title = title; column.width = CGFloat(width); revisionTable.addTableColumn(column)
        }
        revisionTable.dataSource = self; revisionTable.delegate = self
        TerminalTheme.prepareTable(revisionTable)
        revisionTable.columnAutoresizingStyle = .lastColumnOnlyAutoresizingStyle
    }

    private func configureRunTable() {
        for (id, title, width) in [("Run", "실행 ID", 230), ("State", "상태", 80), ("Market", "시장", 110), ("Date", "실행일", 145)] {
            let column = NSTableColumn(identifier: NSUserInterfaceItemIdentifier(id))
            column.title = title; column.width = CGFloat(width); runTable.addTableColumn(column)
        }
        runTable.dataSource = self; runTable.delegate = self
        runTable.target = self
        runTable.doubleAction = #selector(openSelectedRun)
        TerminalTheme.prepareTable(runTable)
        runTable.columnAutoresizingStyle = .lastColumnOnlyAutoresizingStyle
    }

    private func scroll(for table: NSTableView) -> NSScrollView {
        let scroll = NSScrollView(); scroll.hasVerticalScroller = true; scroll.hasHorizontalScroller = true
        scroll.autohidesScrollers = true
        scroll.documentView = table; return scroll
    }

    private func labeledSection(_ title: String, content: NSView, minimumHeight: CGFloat) -> NSView {
        let label = NSTextField(labelWithString: title); label.font = TerminalTheme.sectionHeading
        let stack = NSStackView(views: [label, content]); stack.orientation = .vertical; stack.alignment = .leading; stack.spacing = 5
        content.widthAnchor.constraint(equalTo: stack.widthAnchor).isActive = true
        stack.heightAnchor.constraint(greaterThanOrEqualToConstant: max(minimumHeight, TerminalTheme.tableRowHeight * 2 + 30)).isActive = true
        return stack
    }

    func controlTextDidChange(_ notification: Notification) { applyFilters() }
    @objc private func filtersChanged() { applyFilters() }

    private func applyFilters() {
        let preservedSelection = selectedPolicyIds
        let query = searchField.stringValue.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        let origin = originPopup.indexOfSelectedItem == 1 ? "BUILTIN" : originPopup.indexOfSelectedItem == 2 ? "USER" : nil
        let usage: PolicyUsageState? = usagePopup.indexOfSelectedItem == 1 ? .historicalOnly : usagePopup.indexOfSelectedItem == 2 ? .unused : nil
        let market = marketPopup.indexOfSelectedItem == 0 ? nil : marketPopup.titleOfSelectedItem
        filteredPolicies = policies.filter { policy in
            let textMatch = query.isEmpty || [policy.name, policy.policyId, policy.family].contains { $0.lowercased().contains(query) }
            let marketMatch = market.map { marketsByPolicy[policy.policyId]?.contains($0) == true } ?? true
            return textMatch && (origin == nil || policy.origin == origin) && (usage == nil || policy.usageState == usage) && marketMatch
        }
        selectedPolicyIds = preservedSelection
        policyTable.reloadData()
        restoreSelection()
        let focusedIsVisible = focusedPolicyId.map { id in
            filteredPolicies.contains(where: { $0.policyId == id })
        } ?? false
        if !focusedIsVisible {
            if let visibleSelection = filteredPolicies.first(where: { selectedPolicyIds.contains($0.policyId) }) {
                focusedPolicyId = visibleSelection.policyId
                loadFocusedPolicy(visibleSelection)
            } else {
                focusedPolicyId = nil
                generation &+= 1
                revisionGeneration &+= 1
                let message = selectedPolicyIds.isEmpty
                    ? (filteredPolicies.isEmpty
                        ? (policies.isEmpty ? "등록된 정책이 없습니다." : "현재 검색·필터와 일치하는 정책이 없습니다.")
                        : "정책을 선택하면 상세 정보가 표시됩니다.")
                    : "선택한 정책이 현재 검색·필터에서 숨겨졌습니다. 선택은 유지되며 필터를 해제하면 다시 표시됩니다."
                showEmptyDetail(message: message)
            }
        }
        updateCount()
    }

    private func restoreSelection() {
        let rows = IndexSet(filteredPolicies.indices.filter { selectedPolicyIds.contains(filteredPolicies[$0].policyId) })
        restoringSelection = true
        policyTable.selectRowIndexes(rows, byExtendingSelection: false)
        restoringSelection = false
    }

    private func updateCount() {
        let suffix = selectedPolicyIds.isEmpty ? "" : " · \(selectedPolicyIds.count)개 선택"
        countLabel.stringValue = "\(filteredPolicies.count) / \(policies.count)개\(suffix)"
        countLabel.setAccessibilityValue(countLabel.stringValue)
    }

    private func loadMarketIndex() {
        workQueue.async { [weak self] in
            guard let self else { return }
            var map: [String: Set<String>] = [:]
            do {
                for policy in self.policies {
                    var cursor: RunHistoryCursor?
                    var pages = 0
                    repeat {
                        let page = try self.reader.fetchRunHistoryPage(
                            filter: RunHistoryFilter(policyId: policy.policyId), cursor: cursor, limit: 100
                        )
                        for run in page.items { map[policy.policyId, default: []].formUnion(run.markets ?? []) }
                        cursor = page.nextCursor; pages += 1
                    } while cursor != nil && pages < 10
                    guard cursor == nil else { throw PolicyDetailReadError.resourceLimit("시장 필터 원장 페이지") }
                }
                DispatchQueue.main.async { [weak self] in
                    self?.marketsByPolicy = map
                    self?.marketPopup.isEnabled = true
                    self?.marketPopup.toolTip = "관련 원장의 시장을 기준으로 필터링합니다."
                    self?.marketStatusLabel.stringValue = "정책별 관련 원장을 확인했습니다."
                    self?.marketStatusLabel.textColor = TerminalTheme.secondaryText
                    self?.marketStatusLabel.setAccessibilityValue(self?.marketStatusLabel.stringValue ?? "")
                    self?.applyFilters()
                }
            } catch {
                DispatchQueue.main.async { [weak self] in
                    self?.marketPopup.isEnabled = false
                    self?.marketPopup.toolTip = "시장 필터를 계산할 수 없습니다: \(error.localizedDescription)"
                    let message = "시장 필터를 사용할 수 없습니다: \(error.localizedDescription)"
                    self?.marketStatusLabel.stringValue = message
                    self?.marketStatusLabel.toolTip = message
                    self?.marketStatusLabel.textColor = .systemOrange
                    self?.marketStatusLabel.setAccessibilityValue(message)
                }
            }
        }
    }

    private func loadFocusedPolicy(_ policy: RealPolicy) {
        generation &+= 1; let requestGeneration = generation
        revisionGeneration &+= 1
        currentDetail = nil; allPolicyRuns = []; relatedRuns = []
        relatedRunsLoaded = false
        relatedRunsError = nil
        detailTitle.stringValue = policy.name
        detailTitle.setAccessibilityValue(detailTitle.stringValue)
        detailIdentity.stringValue = "Policy ID: \(policy.policyId)\nHead revision ID: \(policy.headRevisionId)"
        detailIdentity.toolTip = detailIdentity.stringValue
        detailIdentity.setAccessibilityValue(detailIdentity.stringValue)
        detailMeta.stringValue = "정책 정의와 원장 참조를 읽는 중…"
        detailMeta.textColor = TerminalTheme.secondaryText
        detailMeta.toolTip = nil
        detailMeta.setAccessibilityValue(detailMeta.stringValue)
        definitionText.string = "조회 중…"
        relatedRunStatusLabel.stringValue = "관련 원장 실행을 확인하는 중입니다."
        relatedRunStatusLabel.textColor = TerminalTheme.secondaryText
        relatedRunStatusLabel.toolTip = nil
        relatedRunStatusLabel.setAccessibilityValue(relatedRunStatusLabel.stringValue)
        revisionTable.reloadData(); runTable.reloadData()
        openRunButton.isEnabled = false; showStrategyButton.isEnabled = true
        scrollInspectorToTop()

        detailReader.fetchDetail(for: policy) { [weak self] result in
            guard let self, requestGeneration == self.generation else { return }
            switch result {
            case .success(let detail):
                self.currentDetail = detail
                self.renderDetail(policy: policy, detail: detail)
                self.revisionTable.reloadData()
                self.restoringRevisionSelection = true
                if let headRow = detail.history.firstIndex(where: { $0.reference == detail.reference }) {
                    self.revisionTable.selectRowIndexes(IndexSet(integer: headRow), byExtendingSelection: false)
                }
                self.restoringRevisionSelection = false
                self.updateRelatedRuns()
            case .failure(let error):
                self.detailMeta.stringValue = "정책 Backend 상세를 확인할 수 없습니다. 로컬 원장 정보는 유지됩니다."
                self.detailMeta.textColor = .systemOrange
                self.detailMeta.setAccessibilityValue(self.detailMeta.stringValue)
                self.detailMeta.toolTip = error.localizedDescription
                self.definitionText.string = error.localizedDescription
                self.relatedRuns = []
                self.runTable.reloadData()
                self.openRunButton.isEnabled = false
            }
        }
        workQueue.async { [weak self] in
            guard let self else { return }
            do {
                var runs: [RealRun] = []; var cursor: RunHistoryCursor?; var pages = 0
                repeat {
                    let page = try self.reader.fetchRunHistoryPage(filter: RunHistoryFilter(policyId: policy.policyId), cursor: cursor, limit: 100)
                    runs.append(contentsOf: page.items); cursor = page.nextCursor; pages += 1
                } while cursor != nil && pages < 10
                guard cursor == nil, runs.count == policy.historicalRunCount else {
                    throw PolicyDetailReadError.identityMismatch("\(policy.policyId)의 원장 run count")
                }
                DispatchQueue.main.async { [weak self] in
                    guard let self, requestGeneration == self.generation else { return }
                    self.allPolicyRuns = runs
                    self.relatedRunsLoaded = true
                    self.relatedRunsError = nil
                    self.updateRelatedRuns()
                }
            } catch {
                DispatchQueue.main.async { [weak self] in
                    guard let self, requestGeneration == self.generation else { return }
                    self.allPolicyRuns = []
                    self.relatedRunsLoaded = true
                    self.relatedRunsError = error.localizedDescription
                    self.relatedRuns = []; self.runTable.reloadData()
                    self.updateRelatedRunStatus()
                    self.openRunButton.isEnabled = false
                }
            }
        }
    }

    private func updateRelatedRuns() {
        guard let revisionId = currentDetail?.reference.revisionId else {
            relatedRuns = []
            runTable.reloadData()
            updateRelatedRunStatus()
            openRunButton.isEnabled = false
            return
        }
        relatedRuns = allPolicyRuns.filter { $0.policyRevisionIds?.contains(revisionId) == true }
        runTable.reloadData()
        if !relatedRuns.isEmpty && !relatedRuns.indices.contains(runTable.selectedRow) {
            runTable.selectRowIndexes(IndexSet(integer: 0), byExtendingSelection: false)
        }
        updateRelatedRunStatus()
        openRunButton.isEnabled = relatedRuns.indices.contains(runTable.selectedRow)
    }

    private func updateRelatedRunStatus() {
        let message: String
        if let relatedRunsError {
            message = "관련 실행을 불러오지 못했습니다: \(relatedRunsError)"
        } else if focusedPolicyId == nil {
            message = "정책을 선택하면 관련 실행을 확인할 수 있습니다."
        } else if currentDetail == nil {
            message = "정책 리비전 identity 확인 전에는 관련 실행을 표시하지 않습니다."
        } else if !relatedRunsLoaded {
            message = "관련 원장 실행을 확인하는 중입니다."
        } else if relatedRuns.isEmpty {
            message = "선택한 리비전을 참조하는 과거 실행이 없습니다."
        } else {
            message = "선택한 리비전을 참조하는 과거 실행 \(relatedRuns.count)건입니다."
        }
        relatedRunStatusLabel.stringValue = message
        relatedRunStatusLabel.textColor = relatedRunsError == nil ? TerminalTheme.secondaryText : .systemOrange
        relatedRunStatusLabel.toolTip = message
        relatedRunStatusLabel.setAccessibilityValue(message)
    }

    private func renderDetail(policy: RealPolicy, detail: PolicyDetailSnapshot) {
        let usage = policy.usageState == .historicalOnly ? "HISTORICAL_ONLY · 과거 \(policy.historicalRunCount)회 참조" : "UNUSED · 원장 참조 없음"
        let testHint = Self.testNameHint(policy) ? " · 이름 기반 TEST_ONLY 후보(명시 category 없음)" : ""
        detailMeta.stringValue = "\(TerminalPresentation.state(detail.origin)) · 분류 \(Self.familyTitle(detail.family)) · \(usage)\(testHint)\n등록 \(detail.header.createdAt) · 선택 리비전 \(detail.revisionNumber)/\(detail.header.revisionCount)"
        detailMeta.textColor = TerminalTheme.secondaryText
        detailMeta.toolTip = nil
        detailMeta.setAccessibilityValue(detailMeta.stringValue)
        detailIdentity.stringValue = "Policy ID: \(policy.policyId)\nRevision ID: \(detail.reference.revisionId)\nDefinition digest: \(detail.reference.definitionDigest)"
        detailIdentity.toolTip = detailIdentity.stringValue
        detailIdentity.setAccessibilityValue(detailIdentity.stringValue)
        let d = detail.definition
        let structure: String
        if d.programKind == "RULES" {
            structure = "규칙형 · indicators \(d.indicatorCount.map(String.init) ?? "미확인") · states \(d.stateCount.map(String.init) ?? "미확인") · rules \(d.ruleCount.map(String.init) ?? "미확인") · expiry \(d.signalExpiry ?? "미확인")"
        } else { structure = "내장형 · strategy \(d.strategyKind ?? "UNKNOWN")" }
        definitionText.string = """
        \(d.name)
        \(d.description.isEmpty ? "설명 없음" : d.description)

        정책 ID: \(policy.policyId)
        Revision ID: \(detail.reference.revisionId)
        Definition digest: \(detail.reference.definitionDigest)
        Parent revision: \(detail.parentRevisionId ?? "없음")
        Request ID: \(detail.requestId)
        Schema: \(d.schemaVersion)
        Program: \(structure)
        Source intervals: \(d.sourceIntervals.isEmpty ? "정책 정의에 별도 interval 없음" : d.sourceIntervals.joined(separator: ", "))
        Decision / execution interval: 정책 정의에 없음 · 관련 실행에서 확인
        Target / risk: 아래 불변 정의 JSON에 저장된 필드만 표시
        Evidence connection: \(d.evidenceReferenced ? "정의에서 evidence 입력 참조 확인" : "정책 정의에 없음")
        Cost: 정책 정의에 없음 · 관련 실행 가정에서 확인
        Ownership: \(TerminalPresentation.state(detail.origin))
        실거래 / 지속모의 연결: 이 읽기 계약으로 확인 불가

        불변 정의 JSON
        \(d.exactJSON)
        """
    }

    private static func testNameHint(_ policy: RealPolicy) -> Bool {
        let text = "\(policy.name) \(policy.policyId)".lowercased()
        return ["test", "e2e", "visibility", "fixture"].contains { text.contains($0) }
    }

    private static func familyTitle(_ raw: String) -> String {
        switch raw {
        case "OTHER": return "기타"
        case "BUY_AND_HOLD", "BH": return "매수 후 보유 (BH)"
        default: return raw
        }
    }

    func numberOfRows(in tableView: NSTableView) -> Int {
        if tableView == policyTable { return filteredPolicies.count }
        if tableView == revisionTable { return currentDetail?.history.count ?? 0 }
        if tableView == runTable { return relatedRuns.count }
        return 0
    }

    func tableView(_ tableView: NSTableView, viewFor tableColumn: NSTableColumn?, row: Int) -> NSView? {
        let id = tableColumn?.identifier.rawValue ?? ""
        let value: String
        if tableView == policyTable {
            guard filteredPolicies.indices.contains(row) else { return nil }
            let policy = filteredPolicies[row]
            switch id {
            case "Name":
                value = duplicatePolicyNames.contains(policy.name)
                    ? "\(policy.name) · \(policy.policyId.suffix(8))"
                    : policy.name
            case "PolicyID": value = policy.policyId
            case "Origin": value = TerminalPresentation.state(policy.origin)
            case "Usage": value = policy.usageState == .historicalOnly ? "과거 \(policy.historicalRunCount)회 참조" : "미사용"
            default: value = ""
            }
            let testHint = Self.testNameHint(policy) ? "\n이름 기반 TEST_ONLY 후보 · 명시 category 없음" : ""
            return cell(value, tooltip: "\(policy.policyId)\n분류 \(Self.familyTitle(policy.family))\nhead \(policy.headRevisionId)\n\(policy.historicalModelCount) models\(testHint)")
        }
        if tableView == revisionTable {
            guard let item = currentDetail?.history[safe: row] else { return nil }
            switch id {
            case "Revision": value = "r\(item.revisionNumber)"
            case "Created": value = item.createdAt
            case "Digest": value = item.reference.definitionDigest
            default: value = ""
            }
            return cell(value, tooltip: "\(item.reference.revisionId)\nparent \(item.parentRevisionId ?? "없음")\nrequest \(item.requestId)")
        }
        guard let run = relatedRuns[safe: row] else { return nil }
        switch id {
        case "Run": value = run.id
        case "State": value = TerminalPresentation.state(run.state)
        case "Market": value = run.markets?.joined(separator: ", ") ?? "미확인"
        case "Date": value = run.createdDateStr
        default: value = ""
        }
        return cell(value, tooltip: "\(run.id)\n\(run.windowStartUTC ?? "시작 미확인") → \(run.windowEndUTC ?? "종료 미확인")\n비용 \(run.costAssumptionLabel ?? "미확인")")
    }

    private func cell(_ value: String, tooltip: String) -> NSTextField {
        let cell = NSTextField(labelWithString: value)
        cell.font = TerminalTheme.table
        cell.textColor = .labelColor
        cell.lineBreakMode = .byTruncatingMiddle
        cell.toolTip = tooltip
        cell.setAccessibilityLabel(value)
        cell.setAccessibilityValue(value)
        return cell
    }

    func tableViewSelectionDidChange(_ notification: Notification) {
        guard let table = notification.object as? NSTableView else { return }
        if table == policyTable {
            guard !restoringSelection else { return }
            let visiblePolicyIds = Set(filteredPolicies.map(\.policyId))
            let hiddenSelection = selectedPolicyIds.subtracting(visiblePolicyIds)
            let visibleSelection = Set(table.selectedRowIndexes.compactMap { filteredPolicies[safe: $0]?.policyId })
            selectedPolicyIds = hiddenSelection.union(visibleSelection)
            let focusRow = table.selectedRowIndexes.last ?? -1
            if let policy = filteredPolicies[safe: focusRow] {
                focusedPolicyId = policy.policyId; loadFocusedPolicy(policy)
            } else {
                focusedPolicyId = nil
                generation &+= 1
                revisionGeneration &+= 1
                let message = selectedPolicyIds.isEmpty
                    ? nil
                    : "선택한 정책이 현재 검색·필터에서 숨겨졌습니다. 선택은 유지되며 필터를 해제하면 다시 표시됩니다."
                showEmptyDetail(message: message)
            }
            updateCount()
        } else if table == runTable {
            openRunButton.isEnabled = relatedRuns.indices.contains(table.selectedRow)
        } else if table == revisionTable {
            guard !restoringRevisionSelection,
                  let detail = currentDetail,
                  let item = detail.history[safe: table.selectedRow],
                  let policyId = focusedPolicyId,
                  let policy = policies.first(where: { $0.policyId == policyId }) else { return }
            if item.reference == detail.reference {
                updateRelatedRuns()
                return
            }
            revisionGeneration &+= 1
            let requestGeneration = revisionGeneration
            detailMeta.stringValue = "리비전 r\(item.revisionNumber)의 불변 정의를 읽는 중…"
            detailMeta.textColor = TerminalTheme.secondaryText
            detailMeta.toolTip = nil
            detailMeta.setAccessibilityValue(detailMeta.stringValue)
            detailIdentity.stringValue = "Policy ID: \(policy.policyId)\nRevision ID: \(item.reference.revisionId)\nDefinition digest: \(item.reference.definitionDigest)"
            detailIdentity.toolTip = detailIdentity.stringValue
            detailIdentity.setAccessibilityValue(detailIdentity.stringValue)
            definitionText.string = "조회 중…"
            relatedRuns = []
            runTable.reloadData()
            relatedRunStatusLabel.stringValue = "선택 리비전의 identity를 확인하는 중입니다."
            relatedRunStatusLabel.setAccessibilityValue(relatedRunStatusLabel.stringValue)
            openRunButton.isEnabled = false
            scrollInspectorToTop()
            detailReader.fetchRevision(item, within: detail) { [weak self] result in
                guard let self, requestGeneration == self.revisionGeneration,
                      policy.policyId == self.focusedPolicyId else { return }
                switch result {
                case .success(let selectedRevision):
                    self.currentDetail = selectedRevision
                    self.renderDetail(policy: policy, detail: selectedRevision)
                    self.updateRelatedRuns()
                case .failure(let error):
                    self.detailMeta.stringValue = "선택한 리비전의 identity를 검증할 수 없습니다."
                    self.detailMeta.textColor = .systemOrange
                    self.detailMeta.setAccessibilityValue(self.detailMeta.stringValue)
                    self.detailMeta.toolTip = error.localizedDescription
                    self.definitionText.string = error.localizedDescription
                    self.relatedRuns = []
                    self.runTable.reloadData()
                    let message = "선택한 리비전 identity를 확인하지 못해 관련 실행을 표시하지 않습니다."
                    self.relatedRunStatusLabel.stringValue = message
                    self.relatedRunStatusLabel.toolTip = error.localizedDescription
                    self.relatedRunStatusLabel.setAccessibilityValue(message)
                    self.openRunButton.isEnabled = false
                }
            }
        }
    }

    private func showEmptyDetail(message: String? = nil) {
        currentDetail = nil; allPolicyRuns = []; relatedRuns = []
        relatedRunsLoaded = false
        relatedRunsError = nil
        detailTitle.stringValue = "정책을 선택하세요"
        detailTitle.setAccessibilityValue(detailTitle.stringValue)
        detailIdentity.stringValue = selectedPolicyIds.isEmpty
            ? (policies.isEmpty ? "등록된 정책이 없어 identity가 없습니다." : "정책을 선택하면 정책 ID와 리비전 ID가 표시됩니다.")
            : "선택한 정책 ID는 현재 검색·필터에서 숨겨져 있습니다."
        detailIdentity.setAccessibilityValue(detailIdentity.stringValue)
        detailMeta.stringValue = message ?? (policies.isEmpty
            ? "등록된 정책이 없습니다."
            : (filteredPolicies.isEmpty ? "현재 검색·필터와 일치하는 정책이 없습니다." : "등록된 정책은 실행 중인 세션을 뜻하지 않습니다."))
        detailMeta.textColor = TerminalTheme.secondaryText
        detailMeta.toolTip = nil
        detailMeta.setAccessibilityValue(detailMeta.stringValue)
        definitionText.string = ""
        revisionTable.reloadData(); runTable.reloadData()
        relatedRunStatusLabel.stringValue = filteredPolicies.isEmpty ? "관련 실행이 없습니다." : "정책을 선택하면 관련 실행을 확인할 수 있습니다."
        relatedRunStatusLabel.textColor = TerminalTheme.secondaryText
        relatedRunStatusLabel.toolTip = nil
        relatedRunStatusLabel.setAccessibilityValue(relatedRunStatusLabel.stringValue)
        openRunButton.isEnabled = false; showStrategyButton.isEnabled = false
        scrollInspectorToTop()
    }

    @objc private func openSelectedRun() {
        guard let run = relatedRuns[safe: runTable.selectedRow],
              let policyId = focusedPolicyId,
              let revisionId = currentDetail?.reference.revisionId,
              run.policyRevisionIds?.contains(revisionId) == true else { return }
        onOpenRun(run, policyId, revisionId)
    }

    @objc private func showSelectedStrategy() {
        guard let policyId = focusedPolicyId else { return }
        onShowStrategy(policyId)
    }
}

private extension Collection where Index == Int {
    subscript(safe index: Int) -> Element? { indices.contains(index) ? self[index] : nil }
}
