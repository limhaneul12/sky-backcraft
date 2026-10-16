import AppKit

private struct LedgerColumn {
    let id: String
    let title: String
    let width: CGFloat
}

/// Read-only cost and immutable ledger drill-down for one selected run/model.
final class ModelLedgerDetailWindowController: NSWindowController, NSWindowDelegate,
    NSTableViewDataSource, NSTableViewDelegate {
    private let run: RealRun
    private let model: RealModelComparison
    private let policyName: String
    private let reader: LedgerDetailMCPReader

    private let contextLabel = NSTextField(wrappingLabelWithString: "")
    private let costLabel = NSTextField(wrappingLabelWithString: "비용 요약을 불러오는 중…")
    private let costProgress = NSProgressIndicator()
    private let sectionControl = NSSegmentedControl(labels: LedgerDetailSection.allCases.map(\.title),
                                                     trackingMode: .selectOne, target: nil, action: nil)
    private let episodeSummaryLabel = NSTextField(wrappingLabelWithString: "")
    private let table = NSTableView()
    private let statusLabel = NSTextField(labelWithString: "원장을 불러오는 중…")
    private let pageProgress = NSProgressIndicator()
    private let moreButton = NSButton(title: "다음 100건", target: nil, action: nil)
    private let detailText = NSTextView()
    private let copyDetailButton = NSButton(title: "선택 상세 복사", target: nil, action: nil)

    private var section: LedgerDetailSection = .fills
    private var records: [LedgerDetailRecord] = []
    private var totalCount = 0
    private var nextCursor: LedgerDetailCursor?
    private var sectionGeneration: UInt64 = 0
    private var costGeneration: UInt64 = 0
    private var sectionLoading = false
    private var currentTruncatedReason: String?
    private var columns: [LedgerColumn] = []
    private(set) var typedFills: [LedgerFillDetail] = []
    var onFillsLoaded: (([LedgerFillDetail], Int) -> Void)?

    init(run: RealRun, model: RealModelComparison, policyName: String,
         client: NativeMCPClient = .shared) {
        self.run = run
        self.model = model
        self.policyName = policyName
        reader = LedgerDetailMCPReader(client: client)
        super.init(window: nil)
        buildWindow()
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) { fatalError("init(coder:) is not supported") }

    func showLedgerDetails() {
        showWindow(nil)
        window?.makeKeyAndOrderFront(nil)
        loadCosts()
        loadSection(reset: true)
    }

    private func buildWindow() {
        let window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 1120, height: 760),
                              styleMask: [.titled, .closable, .resizable, .miniaturizable],
                              backing: .buffered, defer: false)
        window.title = "과거 모델 비용 · 원장 상세"
        window.minSize = NSSize(width: 920, height: 640)
        window.delegate = self
        window.center()
        self.window = window

        let root = TerminalSurfaceView()
        root.translatesAutoresizingMaskIntoConstraints = false
        window.contentView = root

        let title = NSTextField(labelWithString: "\(model.market) · \(policyName)")
        title.font = .systemFont(ofSize: 22, weight: .semibold)
        title.setAccessibilityLabel("모델 비용 및 원장 상세")
        contextLabel.font = .systemFont(ofSize: 12)
        contextLabel.textColor = TerminalTheme.secondaryText
        contextLabel.maximumNumberOfLines = 4
        contextLabel.stringValue = "과거 모델링 체결 원장 · 실제 주문 아님\n선택 실행 · \(Self.kst(run.windowStartUTC)) → \(Self.kst(run.windowEndUTC))\n비용 가정 · \(run.costAssumptionLabel ?? "미확인")   원장 · \(run.ledgerState)   F08 · \(model.f08Status ?? run.f08Status ?? "미검증")\n모델 · \(Self.short(model.modelId))   실행 · \(Self.short(run.id))"
        contextLabel.toolTip = "모델 ID: \(model.modelId)\n실행 ID: \(run.id)\n정책 리비전: \(model.policyRevisionId ?? "미확인")\n정책 정의 digest: \(model.policyDefinitionDigest ?? "미확인")"

        let copyIds = NSButton(title: "전체 ID 복사", target: self, action: #selector(copyIdentifiers))
        copyIds.bezelStyle = .rounded
        let headerRow = NSStackView(views: [title, NSView(), copyIds])
        headerRow.orientation = .horizontal
        headerRow.alignment = .centerY
        headerRow.spacing = 12

        let costPanel = TerminalSurfaceView()
        costPanel.usesPanel = true
        costPanel.translatesAutoresizingMaskIntoConstraints = false
        let costTitle = NSTextField(labelWithString: "비용과 종결 상태")
        costTitle.font = .systemFont(ofSize: 14, weight: .semibold)
        costLabel.font = TerminalTheme.table
        costLabel.maximumNumberOfLines = 3
        costLabel.textColor = .labelColor
        let costNote = NSTextField(wrappingLabelWithString: "수수료는 명시적 현금 비용입니다. 체결가격 내재 비용은 부호 있는 KRW 진단값이며 이미 가격에 반영되어 있으므로 다시 차감하지 않습니다. 모델 전체 보유·노출 집계는 비용 요약에 없어 산출하지 않으며, 과거 에피소드에 기록된 값만 표시합니다.")
        costNote.font = .systemFont(ofSize: 12)
        costNote.textColor = TerminalTheme.secondaryText
        costProgress.style = .spinning
        costProgress.controlSize = .small
        costProgress.startAnimation(nil)
        let costHeading = NSStackView(views: [costTitle, costProgress, NSView()])
        costHeading.orientation = .horizontal
        costHeading.spacing = 8
        let costStack = NSStackView(views: [costHeading, costLabel, costNote])
        costStack.orientation = .vertical
        costStack.alignment = .leading
        costStack.spacing = 7
        costStack.translatesAutoresizingMaskIntoConstraints = false
        costPanel.addSubview(costStack)

        sectionControl.selectedSegment = 0
        sectionControl.target = self
        sectionControl.action = #selector(sectionChanged)
        sectionControl.segmentStyle = .rounded
        sectionControl.setAccessibilityLabel("원장 섹션")
        episodeSummaryLabel.font = .systemFont(ofSize: 12, weight: .medium)
        episodeSummaryLabel.textColor = TerminalTheme.secondaryText
        episodeSummaryLabel.maximumNumberOfLines = 4
        episodeSummaryLabel.isHidden = true

        configureTable(for: section)
        table.dataSource = self
        table.delegate = self
        table.target = self
        table.doubleAction = #selector(copySelectedDetail)
        TerminalTheme.prepareTable(table)
        let tableScroll = NSScrollView()
        tableScroll.documentView = table
        tableScroll.hasVerticalScroller = true
        tableScroll.hasHorizontalScroller = true
        tableScroll.autohidesScrollers = true

        detailText.isEditable = false
        detailText.isSelectable = true
        detailText.drawsBackground = true
        detailText.backgroundColor = TerminalTheme.panel
        detailText.textColor = .labelColor
        detailText.font = .monospacedSystemFont(ofSize: 12, weight: .regular)
        detailText.textContainerInset = NSSize(width: 12, height: 12)
        detailText.string = "표에서 행을 선택하면 읽기 쉬운 요약과 정확한 원본 필드를 확인할 수 있습니다."
        let detailScroll = NSScrollView()
        detailScroll.documentView = detailText
        detailScroll.hasVerticalScroller = true

        let split = NSSplitView()
        split.isVertical = true
        split.dividerStyle = .thin
        split.addArrangedSubview(tableScroll)
        split.addArrangedSubview(detailScroll)
        tableScroll.widthAnchor.constraint(greaterThanOrEqualToConstant: 590).isActive = true
        detailScroll.widthAnchor.constraint(greaterThanOrEqualToConstant: 270).isActive = true

        statusLabel.font = .systemFont(ofSize: 12)
        statusLabel.textColor = TerminalTheme.secondaryText
        pageProgress.style = .spinning
        pageProgress.controlSize = .small
        pageProgress.startAnimation(nil)
        moreButton.target = self
        moreButton.action = #selector(loadMore)
        moreButton.isEnabled = false
        copyDetailButton.target = self
        copyDetailButton.action = #selector(copySelectedDetail)
        copyDetailButton.isEnabled = false
        let footer = NSStackView(views: [pageProgress, statusLabel, NSView(), copyDetailButton, moreButton])
        footer.orientation = .horizontal
        footer.alignment = .centerY
        footer.spacing = 10

        let stack = NSStackView(views: [headerRow, contextLabel, costPanel, sectionControl,
                                        episodeSummaryLabel, split, footer])
        stack.orientation = .vertical
        stack.alignment = .leading
        stack.spacing = 12
        stack.translatesAutoresizingMaskIntoConstraints = false
        root.addSubview(stack)
        headerRow.widthAnchor.constraint(equalTo: stack.widthAnchor).isActive = true
        contextLabel.widthAnchor.constraint(equalTo: stack.widthAnchor).isActive = true
        costPanel.widthAnchor.constraint(equalTo: stack.widthAnchor).isActive = true
        sectionControl.widthAnchor.constraint(lessThanOrEqualTo: stack.widthAnchor).isActive = true
        episodeSummaryLabel.widthAnchor.constraint(equalTo: stack.widthAnchor).isActive = true
        split.widthAnchor.constraint(equalTo: stack.widthAnchor).isActive = true
        footer.widthAnchor.constraint(equalTo: stack.widthAnchor).isActive = true
        split.setContentHuggingPriority(.defaultLow, for: .vertical)

        NSLayoutConstraint.activate([
            stack.leadingAnchor.constraint(equalTo: root.leadingAnchor, constant: 20),
            stack.trailingAnchor.constraint(equalTo: root.trailingAnchor, constant: -20),
            stack.topAnchor.constraint(equalTo: root.topAnchor, constant: 18),
            stack.bottomAnchor.constraint(equalTo: root.bottomAnchor, constant: -16),
            costStack.leadingAnchor.constraint(equalTo: costPanel.leadingAnchor, constant: 14),
            costStack.trailingAnchor.constraint(equalTo: costPanel.trailingAnchor, constant: -14),
            costStack.topAnchor.constraint(equalTo: costPanel.topAnchor, constant: 11),
            costStack.bottomAnchor.constraint(equalTo: costPanel.bottomAnchor, constant: -11),
            split.heightAnchor.constraint(greaterThanOrEqualToConstant: 330)
        ])
    }

    private func loadCosts() {
        costGeneration &+= 1
        let generation = costGeneration
        costProgress.isHidden = false
        reader.costs(runId: run.id, modelId: model.modelId, expectedFillCount: model.fillCount,
                     expectedTerminalEquity: model.finalEquityDecimal,
                     expectedPolicyId: model.policyId,
                     expectedPolicyRevisionId: model.policyRevisionId,
                     expectedPolicyDefinitionDigest: model.policyDefinitionDigest) { [weak self] result in
            guard let self, generation == self.costGeneration, self.window != nil else { return }
            self.costProgress.stopAnimation(nil); self.costProgress.isHidden = true
            switch result {
            case .success(let cost):
                let terminal = cost.terminalEquity.map { Self.money2($0) } ?? "산출 불가"
                let sequence = cost.terminalMarkSequence.map(String.init) ?? "산출 불가"
                self.costLabel.stringValue = "매수 체결대금 \(Self.money2(cost.buyNotional))   매도 체결대금 \(Self.money2(cost.sellNotional))   명시적 수수료 \(Self.money2(cost.explicitFees))\n체결가격 내재 비용(진단) \(Self.money2(cost.embeddedPriceCost, signed: true))   종결 평가액 \(terminal)   종결 mark seq \(sequence)"
                self.costLabel.toolTip = "정확한 과거 원장 값\n매수 체결대금=\(Self.exact(cost.buyNotional)) KRW\n매도 체결대금=\(Self.exact(cost.sellNotional)) KRW\n명시적 수수료=\(Self.exact(cost.explicitFees)) KRW\n체결가격 내재 비용=\(Self.exact(cost.embeddedPriceCost)) KRW\n종결 평가액=\(cost.terminalEquity.map(Self.exact) ?? "미제공") KRW\n원본 상태=\(cost.status ?? "미제공")\n정책 리비전=\(cost.policyRevisionId ?? "미제공")\n정책 정의 digest=\(cost.policyDefinitionDigest ?? "미제공")"
            case .failure(let error):
                self.costLabel.stringValue = "비용 요약을 확인할 수 없습니다. \(error.localizedDescription)"
                self.costLabel.textColor = TerminalTheme.negative
            }
        }
    }

    private func loadSection(reset: Bool) {
        guard !sectionLoading else { return }
        sectionLoading = true
        sectionGeneration &+= 1
        let generation = sectionGeneration
        let requestedCursor = reset ? nil : nextCursor
        if reset {
            records = []; typedFills = []; totalCount = 0; nextCursor = nil; currentTruncatedReason = nil
            table.reloadData(); detailText.string = "불러오는 중…"; copyDetailButton.isEnabled = false
            if section == .episodes {
                episodeSummaryLabel.isHidden = false
                episodeSummaryLabel.stringValue = "과거 원장 집계를 불러오는 중…"
            } else {
                episodeSummaryLabel.isHidden = true
            }
        }
        pageProgress.isHidden = false
        pageProgress.startAnimation(nil)
        moreButton.isEnabled = false
        statusLabel.stringValue = "\(section.title) 원장을 읽는 중…"
        let expected: Int? = section == .fills ? model.fillCount : (section == .episodes ? model.episodeCount : nil)
        reader.page(runId: run.id, modelId: model.modelId, section: section,
                    cursor: requestedCursor, expectedTotalCount: expected,
                    expectedMarket: model.market) { [weak self] result in
            guard let self, generation == self.sectionGeneration, self.window != nil else { return }
            self.sectionLoading = false
            self.pageProgress.stopAnimation(nil); self.pageProgress.isHidden = true
            switch result {
            case .success(let page):
                let selectedIdentity = self.selectedRecordIdentity()
                self.detailText.textColor = .labelColor
                if reset { self.records = page.records } else { self.records.append(contentsOf: page.records) }
                self.totalCount = page.totalCount
                self.nextCursor = page.nextCursor
                self.currentTruncatedReason = page.truncatedReason
                self.applyLoadedSort()
                if self.section == .fills {
                    self.typedFills = self.records.compactMap { if case .fill(let f) = $0 { return f }; return nil }
                        .sorted { $0.identity.sequence < $1.identity.sequence }
                    self.onFillsLoaded?(self.typedFills, page.totalCount)
                }
                self.table.reloadData()
                self.restoreSelection(identity: selectedIdentity)
                self.moreButton.isEnabled = page.nextCursor != nil
                let suffix = page.truncatedReason.map { " · 제한 사유: \($0)" } ?? ""
                self.statusLabel.stringValue = page.totalCount == 0
                    ? "\(self.section.title) 기록이 없습니다. 0건은 확인된 빈 결과입니다."
                    : "\(self.records.count)/\(page.totalCount)건 표시\(suffix)"
                self.detailText.string = page.totalCount == 0
                    ? "선택한 실행·모델에는 \(self.section.title) 기록이 없습니다."
                    : "행을 선택하면 상세 필드를 확인할 수 있습니다."
                if self.section == .episodes { self.updateEpisodeSummary() }
            case .failure(let error):
                self.statusLabel.stringValue = "\(self.section.title) 원장을 읽지 못했습니다."
                self.detailText.string = error.localizedDescription
                self.detailText.textColor = TerminalTheme.negative
                if self.section == .episodes {
                    self.episodeSummaryLabel.isHidden = false
                    self.episodeSummaryLabel.stringValue = "과거 원장 집계 · 확인 불가 — \(error.localizedDescription)"
                }
            }
        }
    }

    @objc private func sectionChanged() {
        sectionGeneration &+= 1
        sectionLoading = false
        section = LedgerDetailSection.allCases[sectionControl.selectedSegment]
        configureTable(for: section)
        loadSection(reset: true)
    }

    @objc private func loadMore() { loadSection(reset: false) }

    private func updateEpisodeSummary() {
        episodeSummaryLabel.isHidden = false
        guard nextCursor == nil, records.count == totalCount, currentTruncatedReason == nil else {
            episodeSummaryLabel.stringValue = "과거 원장 집계 · 부분 조회 \(records.count)/\(totalCount)건 — 페이지가 모두 로드되기 전에는 손익·보유시간 합계를 확정하지 않습니다."
            return
        }
        let episodes = records.compactMap { record -> LedgerEpisodeDetail? in
            if case .episode(let episode) = record { return episode }
            return nil
        }
        guard episodes.count == totalCount else {
            episodeSummaryLabel.stringValue = "과거 원장 집계 · 레코드 유형 불일치 — 합계를 산출하지 않습니다."
            return
        }
        if episodes.isEmpty {
            if model.status == "COMPLETED", model.fillCount == 0, model.episodeCount == 0,
               model.netPnlDecimal == .zero {
                episodeSummaryLabel.stringValue = "과거 원장 집계 · MCP에서 에피소드 0건 확인 · 체결 0건 · 실현 ₩0 · 마크 미실현 ₩0 · 미종결 0건. 확인된 무거래 결과입니다."
            } else {
                episodeSummaryLabel.stringValue = "과거 원장 집계 · MCP에서 에피소드 0건 확인. 모델 상태·체결·손익과 무거래 조건이 일치하지 않아 손익 합계는 확인 불가입니다."
            }
            return
        }
        guard let realized = Self.exactSum(episodes.map(\.netRealized)),
              let unrealized = Self.exactSum(episodes.map(\.markedUnrealized)),
              let combined = Self.exactSum([realized, unrealized]),
              let residualQuantity = Self.exactSum(episodes.map(\.residualQuantity)),
              let holding = Self.holdingFacts(episodes) else {
            episodeSummaryLabel.stringValue = "과거 원장 집계 · 숫자 합산 범위를 벗어나 손익·보유시간 합계를 확인할 수 없습니다."
            return
        }
        let openCount = episodes.filter { $0.status == "OPEN" }.count
        let residualCount = episodes.filter { $0.residualQuantity != .zero }.count
        let lifecycle = "과거 미종결(OPEN) \(openCount)건 — 현재 실거래 활성 상태를 뜻하지 않음 · 잔여수량 기록 \(residualCount)건 / 합계 \(Self.number(residualQuantity)) · 보유시간 합계 \(Self.duration(holding.total)) / 최장 \(Self.duration(holding.longest))"
        if combined == model.netPnlDecimal {
            episodeSummaryLabel.stringValue = "과거 원장 집계 · 전체 \(episodes.count)건 · 순실현 \(Self.money(realized, signed: true)) · 마크 미실현 \(Self.money(unrealized, signed: true)) · 모델 순손익과 정확히 일치\n\(lifecycle)"
        } else {
            episodeSummaryLabel.stringValue = "과거 원장 집계 · 전체 \(episodes.count)건 · 순실현 \(Self.money(realized, signed: true)) · 마크 미실현 \(Self.money(unrealized, signed: true)) · 모델 순손익과 정의 불일치(모델 \(Self.money(model.netPnlDecimal, signed: true))) — 에피소드 합을 총손익으로 사용하지 않음\n\(lifecycle)"
        }
    }

    private static func exactSum(_ values: [Decimal]) -> Decimal? {
        var total = Decimal.zero
        for value in values {
            var left = total, right = value, result = Decimal.zero
            guard NSDecimalAdd(&result, &left, &right, .plain) == .noError, !result.isNaN else { return nil }
            total = result
        }
        return total
    }

    private static func holdingFacts(_ episodes: [LedgerEpisodeDetail]) -> (total: UInt64, longest: UInt64)? {
        var total: UInt64 = 0, longest: UInt64 = 0
        for episode in episodes {
            let addition = total.addingReportingOverflow(episode.holdingSeconds)
            guard !addition.overflow else { return nil }
            total = addition.partialValue
            longest = max(longest, episode.holdingSeconds)
        }
        return (total, longest)
    }

    @objc private func copyIdentifiers() {
        NSPasteboard.general.clearContents()
        NSPasteboard.general.setString("run_id=\(run.id)\nmodel_id=\(model.modelId)\npolicy_revision_id=\(model.policyRevisionId ?? "null")\npolicy_definition_digest=\(model.policyDefinitionDigest ?? "null")", forType: .string)
    }

    @objc private func copySelectedDetail() {
        guard table.selectedRow >= 0, table.selectedRow < records.count else { return }
        NSPasteboard.general.clearContents()
        NSPasteboard.general.setString(detailText.string, forType: .string)
    }

    func windowWillClose(_ notification: Notification) {
        costGeneration &+= 1
        sectionGeneration &+= 1
        sectionLoading = false
    }

    func numberOfRows(in tableView: NSTableView) -> Int { records.count }

    func tableView(_ tableView: NSTableView, viewFor tableColumn: NSTableColumn?, row: Int) -> NSView? {
        guard let tableColumn, row >= 0, row < records.count else { return nil }
        let identifier = NSUserInterfaceItemIdentifier("LedgerValue")
        let cell: NSTableCellView
        if let reused = tableView.makeView(withIdentifier: identifier, owner: self) as? NSTableCellView {
            cell = reused
        } else {
            cell = NSTableCellView(); cell.identifier = identifier
            let label = NSTextField(labelWithString: "")
            label.font = TerminalTheme.table
            label.lineBreakMode = .byTruncatingTail
            label.translatesAutoresizingMaskIntoConstraints = false
            cell.textField = label; cell.addSubview(label)
            NSLayoutConstraint.activate([
                label.leadingAnchor.constraint(equalTo: cell.leadingAnchor, constant: 6),
                label.trailingAnchor.constraint(equalTo: cell.trailingAnchor, constant: -6),
                label.centerYAnchor.constraint(equalTo: cell.centerYAnchor)
            ])
        }
        let raw = value(for: records[row], column: tableColumn.identifier.rawValue)
        cell.textField?.stringValue = raw.display
        cell.textField?.toolTip = raw.tooltip
        cell.textField?.alignment = raw.numeric ? .right : .left
        cell.textField?.lineBreakMode = .byTruncatingTail
        return cell
    }

    func tableViewSelectionDidChange(_ notification: Notification) {
        guard table.selectedRow >= 0, table.selectedRow < records.count else {
            copyDetailButton.isEnabled = false; return
        }
        updateDetail(for: table.selectedRow)
    }

    func tableView(_ tableView: NSTableView, sortDescriptorsDidChange oldDescriptors: [NSSortDescriptor]) {
        let selectedIdentity = selectedRecordIdentity()
        applyLoadedSort()
        tableView.reloadData()
        restoreSelection(identity: selectedIdentity)
    }

    private func selectedRecordIdentity() -> String? {
        guard table.selectedRow >= 0, table.selectedRow < records.count else { return nil }
        return recordIdentity(records[table.selectedRow])
    }

    private func restoreSelection(identity: String?) {
        guard let identity, let row = records.firstIndex(where: { recordIdentity($0) == identity }) else {
            table.deselectAll(nil)
            copyDetailButton.isEnabled = false
            detailText.string = records.isEmpty ? "표시할 원장 기록이 없습니다." : "행을 선택하면 상세 필드를 확인할 수 있습니다."
            return
        }
        table.selectRowIndexes(IndexSet(integer: row), byExtendingSelection: false)
        table.scrollRowToVisible(row)
        updateDetail(for: row)
    }

    private func updateDetail(for row: Int) {
        guard row >= 0, row < records.count else { return }
        copyDetailButton.isEnabled = true
        detailText.textColor = .labelColor
        detailText.string = readableDetail(records[row]) + "\n\n정확한 원본 JSON\n" + records[row].rawJSON
    }

    private func recordIdentity(_ record: LedgerDetailRecord) -> String {
        switch record {
        case .fill(let value): return "fill:\(value.fillId)"
        case .order(let value): return "order:\(value.orderId):\(value.identity.sequence)"
        case .episode(let value): return "episode:\(value.episodeId)"
        case .signal(let value): return "signal:\(value.signalId)"
        }
    }

    private func applyLoadedSort() {
        guard section == .fills, let descriptor = table.sortDescriptors.first,
              let key = descriptor.key,
              ["Time", "Side", "Price", "Qty", "Fee", "Embedded"].contains(key) else { return }
        records.sort { left, right in
            guard case .fill(let lhs) = left, case .fill(let rhs) = right else { return false }
            let comparison: ComparisonResult
            switch key {
            case "Time": comparison = lhs.identity.accountingTime.compare(rhs.identity.accountingTime)
            case "Side": comparison = lhs.side.compare(rhs.side)
            case "Price": comparison = NSDecimalNumber(decimal: lhs.price).compare(NSDecimalNumber(decimal: rhs.price))
            case "Qty": comparison = NSDecimalNumber(decimal: lhs.quantity).compare(NSDecimalNumber(decimal: rhs.quantity))
            case "Fee": comparison = NSDecimalNumber(decimal: lhs.fee).compare(NSDecimalNumber(decimal: rhs.fee))
            case "Embedded": comparison = NSDecimalNumber(decimal: lhs.embeddedPriceCost).compare(NSDecimalNumber(decimal: rhs.embeddedPriceCost))
            default: comparison = .orderedSame
            }
            if comparison == .orderedSame { return lhs.identity.sequence < rhs.identity.sequence }
            return descriptor.ascending ? comparison == .orderedAscending : comparison == .orderedDescending
        }
    }

    private func configureTable(for section: LedgerDetailSection) {
        table.removeAllColumns()
        table.sortDescriptors = []
        switch section {
        case .fills:
            columns = [.init(id: "Time", title: "일시 KST", width: 118), .init(id: "Side", title: "방향", width: 52),
                       .init(id: "Price", title: "가격 (KRW)", width: 108), .init(id: "Qty", title: "수량", width: 116),
                       .init(id: "Fee", title: "수수료 (KRW)", width: 112), .init(id: "Embedded", title: "내재 비용 (KRW)", width: 126)]
        case .orders, .orderEvents:
            columns = [.init(id: "Time", title: "일시 KST", width: 118), .init(id: "Side", title: "방향", width: 52),
                       .init(id: "Type", title: "유형", width: 100), .init(id: "Qty", title: "요청 수량", width: 112),
                       .init(id: "Status", title: "상태", width: 94), .init(id: "Reason", title: "사유", width: 150)]
        case .episodes:
            columns = [.init(id: "Time", title: "시작 KST", width: 118), .init(id: "Status", title: "상태", width: 86),
                       .init(id: "Realized", title: "순실현 (KRW)", width: 122), .init(id: "Unrealized", title: "미실현 (KRW)", width: 112),
                       .init(id: "Holding", title: "보유 시간", width: 92), .init(id: "Fills", title: "체결", width: 52)]
        case .signals:
            columns = [.init(id: "Time", title: "일시 KST", width: 118), .init(id: "Outcome", title: "결과", width: 115),
                       .init(id: "Side", title: "방향", width: 74), .init(id: "Target", title: "목표 비중", width: 88),
                       .init(id: "Reason", title: "판단 사유", width: 205)]
        }
        for spec in columns {
            let column = NSTableColumn(identifier: NSUserInterfaceItemIdentifier(spec.id))
            column.title = spec.title; column.width = spec.width; column.minWidth = spec.width
            column.resizingMask = [.userResizingMask]
            if section == .fills {
                column.sortDescriptorPrototype = NSSortDescriptor(key: spec.id, ascending: true)
            }
            table.addTableColumn(column)
        }
        TerminalTheme.prepareTable(table)
        table.reloadData()
    }

    private func value(for record: LedgerDetailRecord, column: String) -> (display: String, tooltip: String?, numeric: Bool) {
        switch record {
        case .fill(let fill):
            switch column {
            case "Time": return (Self.shortKST(fill.identity.accountingTime), Self.kst(fill.identity.accountingTime), false)
            case "Side": return (Self.side(fill.side), fill.side, false)
            case "Price": return (Self.numberLimited(fill.price, decimals: 8), Self.exact(fill.price), true)
            case "Qty": return (Self.numberLimited(fill.quantity, decimals: 8), Self.exact(fill.quantity), true)
            case "Fee": return (Self.money2(fill.fee), Self.exact(fill.fee), true)
            case "Embedded": return (Self.money2(fill.embeddedPriceCost, signed: true), "정확한 값: \(Self.exact(fill.embeddedPriceCost)) KRW\n이미 체결가격에 반영된 진단값", true)
            default: return ("—", nil, false)
            }
        case .order(let order):
            switch column {
            case "Time": return (Self.shortKST(order.createdAt), Self.kst(order.createdAt), false)
            case "Side": return (Self.side(order.side), order.side, false)
            case "Type": return (Self.presentation(order.orderType), order.orderType, false)
            case "Qty": return (Self.numberLimited(order.requestedQuantity, decimals: 8), Self.exact(order.requestedQuantity), true)
            case "Status": return (Self.presentation(order.status), order.status, false)
            case "Reason": return (Self.presentation(order.reason), order.reason, false)
            default: return ("—", nil, false)
            }
        case .episode(let episode):
            switch column {
            case "Time": return (Self.shortKST(episode.openedAt), Self.kst(episode.openedAt), false)
            case "Status": return (Self.episodeStatus(episode.status), episode.status, false)
            case "Realized": return (Self.money2(episode.netRealized, signed: true), Self.exact(episode.netRealized), true)
            case "Unrealized": return (Self.money2(episode.markedUnrealized, signed: true), Self.exact(episode.markedUnrealized), true)
            case "Holding": return (Self.duration(episode.holdingSeconds), nil, true)
            case "Fills": return (String(episode.fillCount), nil, true)
            default: return ("—", nil, false)
            }
        case .signal(let signal):
            switch column {
            case "Time": return (Self.shortKST(signal.signalTime), Self.kst(signal.signalTime), false)
            case "Outcome": return (Self.presentation(signal.outcome), signal.outcome, false)
            case "Side": return (signal.intendedSide.map(Self.side) ?? "없음", signal.intendedSide, false)
            case "Target": return (Self.percent2(signal.constrainedTargetWeight), Self.exact(signal.constrainedTargetWeight), true)
            case "Reason": return (signal.reasons.map(Self.presentation).joined(separator: ", "), signal.reasons.joined(separator: ", "), false)
            default: return ("—", nil, false)
            }
        }
    }

    private func readableDetail(_ record: LedgerDetailRecord) -> String {
        let ids = "실행 ID\n\(run.id)\n\n모델 ID\n\(model.modelId)"
        switch record {
        case .fill(let fill):
            return "체결 상세\n\n회계 시각 · \(Self.kst(fill.identity.accountingTime))\n체결 시각 계약 · \(Self.timing(fill.timing))\n시장 · \(fill.identity.market)\n방향 · \(Self.side(fill.side))\n가격 · \(Self.money(fill.price))\n수량 · \(Self.number(fill.quantity))\n체결대금 · \(Self.money(fill.notional))\n명시적 수수료 · \(Self.money(fill.fee)) (\(Self.number(fill.feeBps)) bp)\n판단 기준가 · \(Self.money(fill.decisionReference))\n봉 시가 proxy · \(Self.money(fill.barOpenProxy))\narrival mid · \(fill.arrivalMid.map { Self.money($0) } ?? "미제공 (\(fill.arrivalMidNullReason))")\n불리한 슬리피지 · \(Self.number(fill.adverseSlippageBps)) bp\n단위당 가격차 · \(Self.money(fill.priceDifferencePerUnit, signed: true)) / base unit\n체결가격 내재 비용 · \(Self.money(fill.embeddedPriceCost, signed: true)) (재차감 금지)\n스프레드 개별 분해 · 미제공\n시장충격 개별 분해 · 미제공\n산출 원천 · \(Self.presentation(fill.executionOrigin)) (\(fill.executionOrigin))\n실제 체결 관측 · \(fill.fillObserved ? "예" : "아니요")\n유동성 가정 · \(fill.liquidityRoleAssumption)\n유동성 기준시각 · \(Self.kst(fill.liquiditySourceCloseTime))\n엔진 모델 · \(fill.modelVersion)\n회계 mark seq · \(fill.accountingMarkSequence)\n종결용 강제 체결 · \(fill.artificialTerminalExit ? "예" : "아니요")\n\n체결 ID\n\(fill.fillId)\n\n주문 ID\n\(fill.orderId)\n\n에피소드 ID\n\(fill.episodeId)\n\n\(ids)"
        case .order(let order):
            return "주문 상세\n\n시각 · \(Self.kst(order.createdAt))\n시장 · \(order.identity.market)\n방향 · \(Self.side(order.side))\n유형 · \(Self.presentation(order.orderType))\n요청 가격 · \(Self.requestedPrice(order))\n요청 수량 · \(Self.number(order.requestedQuantity))\n누적 체결 수량 · \(Self.number(order.cumulativeFilledQuantity))\n예약 현금 · \(Self.money(order.reservedCash))\n상태 · \(Self.presentation(order.status))\n사유 · \(Self.presentation(order.reason))\n\n주문 ID\n\(order.orderId)\n\n시그널 ID\n\(order.parentSignalId)\n\n\(ids)"
        case .episode(let episode):
            return "거래 에피소드 상세\n\n시장 · \(episode.market)\n상태 · \(Self.episodeStatus(episode.status))\n시작 · \(Self.kst(episode.openedAt))\n종료 · \(Self.episodeClosedAt(episode))\n보유 시간 · \(Self.duration(episode.holdingSeconds))\n실현 가격 손익 · \(Self.money(episode.realizedPricePnL, signed: true))\n수수료 · \(Self.money(episode.fees))\n순실현 손익 · \(Self.money(episode.netRealized, signed: true))\n잔여 수량 · \(Self.number(episode.residualQuantity))\n잔여 원가 · \(Self.money(episode.residualBasis))\n마크 미실현 손익 · \(Self.money(episode.markedUnrealized, signed: true))\n체결/주문 · \(episode.fillCount)/\(episode.orderCount)건\n종료 사유 · \(episode.exitReason.map(Self.presentation) ?? "산출 불가")\n\n에피소드 ID\n\(episode.episodeId)\n\n\(ids)"
        case .signal(let signal):
            return "시그널 상세\n\n시각 · \(Self.kst(signal.signalTime))\n시장 · \(signal.identity.market)\n전략 유형 · \(Self.presentation(signal.strategy))\n결과 · \(Self.presentation(signal.outcome))\n의도 방향 · \(signal.intendedSide.map(Self.side) ?? "없음")\n원시 목표 비중 · \(Self.percent(signal.rawTargetWeight))\n제약 후 목표 비중 · \(Self.percent(signal.constrainedTargetWeight))\n판단 사유 · \(signal.reasons.map(Self.presentation).joined(separator: ", "))\n\n시그널 ID\n\(signal.signalId)\n\n\(ids)"
        }
    }

    private static func short(_ value: String) -> String {
        value.count <= 18 ? value : "\(value.prefix(10))…\(value.suffix(6))"
    }
    private static func exact(_ value: Decimal) -> String {
        NSDecimalNumber(decimal: value).stringValue
    }
    private static func number(_ value: Decimal) -> String {
        let formatter = NumberFormatter(); formatter.numberStyle = .decimal; formatter.locale = Locale(identifier: "ko_KR")
        formatter.minimumFractionDigits = 0; formatter.maximumFractionDigits = 12
        return formatter.string(from: NSDecimalNumber(decimal: value)) ?? value.description
    }
    private static func numberLimited(_ value: Decimal, decimals: Int) -> String {
        let formatter = NumberFormatter(); formatter.numberStyle = .decimal; formatter.locale = Locale(identifier: "ko_KR")
        formatter.minimumFractionDigits = 0; formatter.maximumFractionDigits = decimals
        return formatter.string(from: NSDecimalNumber(decimal: value)) ?? value.description
    }
    private static func money(_ value: Decimal, signed: Bool = false) -> String {
        let magnitude = value < 0 ? -value : value
        let prefix = value < 0 ? "−" : (signed ? "+" : "")
        return prefix + "₩" + number(magnitude)
    }
    private static func money2(_ value: Decimal, signed: Bool = false) -> String {
        let magnitude = value < 0 ? -value : value
        let prefix = value < 0 ? "−" : (signed ? "+" : "")
        let formatter = NumberFormatter(); formatter.numberStyle = .decimal; formatter.locale = Locale(identifier: "ko_KR")
        formatter.minimumFractionDigits = 2; formatter.maximumFractionDigits = 2
        let digits = formatter.string(from: NSDecimalNumber(decimal: magnitude)) ?? magnitude.description
        return prefix + "₩" + digits
    }
    private static func percent(_ value: Decimal) -> String {
        let scaled = NSDecimalNumber(decimal: value).multiplying(by: 100).decimalValue
        return number(scaled) + "%"
    }
    private static func percent2(_ value: Decimal) -> String {
        let scaled = NSDecimalNumber(decimal: value).multiplying(by: 100).decimalValue
        return numberLimited(scaled, decimals: 2) + "%"
    }
    private static func duration(_ seconds: UInt64) -> String {
        let hours = seconds / 3600, minutes = (seconds % 3600) / 60
        return hours > 0 ? "\(hours)시간 \(minutes)분" : "\(minutes)분"
    }
    private static func timing(_ value: LedgerFillTimingDetail) -> String {
        switch value {
        case .exact(let at, _): return "정확 시각 · \(kst(at))"
        case .interval(let start, _, let end, _, let accountingAt, _):
            return "구간 \(kst(start)) → \(kst(end)); 회계 \(kst(accountingAt))"
        }
    }
    private static func side(_ raw: String) -> String {
        raw == "BUY" ? "매수" : (raw == "SELL" ? "매도" : presentation(raw))
    }
    private static func episodeStatus(_ raw: String) -> String {
        raw == "OPEN" ? "과거 미종결" : presentation(raw)
    }
    private static func requestedPrice(_ order: LedgerOrderDetail) -> String {
        if let price = order.requestedPrice { return money(price) }
        if order.orderType == "MARKET" { return "시장가 주문 · 지정가격 없음" }
        return "미확인 · 원본 주문유형 \(order.orderType)"
    }
    private static func episodeClosedAt(_ episode: LedgerEpisodeDetail) -> String {
        if let closedAt = episode.closedAt { return kst(closedAt) }
        if episode.status == "OPEN" { return "과거 원장 미종결" }
        return "미확인 · 원본 상태 \(episode.status)"
    }
    private static func presentation(_ raw: String) -> String {
        let words = raw.lowercased().split(separator: "_").map(String.init)
        return words.map { word in
            ["completed": "완료", "open": "진행 중", "closed": "종료", "filled": "체결 완료",
             "partially": "부분", "cancelled": "취소", "rejected": "거절", "expired": "만료",
             "buy": "매수", "sell": "매도", "executed": "실행", "no": "없음", "action": "행동",
             "market": "시장가", "passive": "지정가", "created": "생성"][word] ?? word
        }.joined(separator: " ")
    }
    private static func kst(_ value: String?) -> String {
        guard let value else { return "산출 불가" }
        let precise = ISO8601DateFormatter(); precise.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        guard let date = precise.date(from: value) ?? ISO8601DateFormatter().date(from: value) else { return value }
        return kst(date)
    }
    private static func kst(_ date: Date) -> String {
        let formatter = DateFormatter(); formatter.locale = Locale(identifier: "ko_KR")
        formatter.timeZone = TimeZone(identifier: "Asia/Seoul")
        formatter.dateFormat = "yyyy-MM-dd HH:mm:ss"
        return formatter.string(from: date) + " KST"
    }
    private static func shortKST(_ date: Date) -> String {
        let formatter = DateFormatter(); formatter.locale = Locale(identifier: "ko_KR")
        formatter.timeZone = TimeZone(identifier: "Asia/Seoul")
        formatter.dateFormat = "MM-dd HH:mm:ss"
        return formatter.string(from: date)
    }
}
