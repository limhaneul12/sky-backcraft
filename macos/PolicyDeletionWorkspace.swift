import AppKit

struct PolicyDeletionSelectionState: Equatable {
    private(set) var selectedPolicyIDs: Set<String> = []

    var orderedPolicyIDs: [String] { selectedPolicyIDs.sorted() }

    mutating func setSelected(_ policyID: String, selected: Bool) {
        guard !policyID.isEmpty else { return }
        if selected { selectedPolicyIDs.insert(policyID) }
        else { selectedPolicyIDs.remove(policyID) }
    }

    mutating func selectAllFiltered(_ policyIDs: [String]) {
        selectedPolicyIDs.formUnion(policyIDs.filter { !$0.isEmpty })
    }

    mutating func clear() {
        selectedPolicyIDs.removeAll()
    }

    mutating func removeConfirmedDeleted(_ policyIDs: Set<String>) {
        selectedPolicyIDs.subtract(policyIDs)
    }

    func hiddenSelectionCount(visiblePolicyIDs: Set<String>) -> Int {
        selectedPolicyIDs.subtracting(visiblePolicyIDs).count
    }
}

struct PolicyDeletionBatchPlan: Equatable {
    static let maximumBatchSize = 100

    let policyIDs: [String]
    private(set) var nextBatchStart = 0

    init(policyIDs: [String]) {
        self.policyIDs = policyIDs.sorted()
    }

    var totalCount: Int { policyIDs.count }
    var currentBatchIndex: Int { nextBatchStart / Self.maximumBatchSize }
    var currentBatchIDs: [String] {
        Array(policyIDs.dropFirst(nextBatchStart).prefix(Self.maximumBatchSize))
    }
    var remainingCount: Int { max(0, totalCount - nextBatchStart - currentBatchIDs.count) }
    var hasNextBatch: Bool { remainingCount > 0 }

    mutating func advanceAfterExplicitUserAction() -> Bool {
        guard hasNextBatch else { return false }
        nextBatchStart += currentBatchIDs.count
        return true
    }
}

enum PolicyDeletionConfirmation {
    static func allowedSingleValues(policyID: String, policyName: String?, currentPolicyNames: [String]) -> Set<String> {
        guard let policyName, !policyName.isEmpty,
              currentPolicyNames.filter({ $0 == policyName }).count == 1 else {
            return [policyID]
        }
        return [policyID, policyName]
    }
}

struct PolicyDeletionUnknownOutcomeFence: Equatable {
    let transportContext: NativeMCPTransportContext?
    private(set) var hasVerifiedNotFound = false
    private(set) var hasMatchingHelperExit = false

    mutating func recordNotFound(for context: NativeMCPTransportContext?) {
        guard context == transportContext else { return }
        hasVerifiedNotFound = true
    }

    mutating func recordHelperExit(_ context: NativeMCPTransportContext) {
        guard transportContext == context, context.owned_helper != nil else { return }
        hasMatchingHelperExit = true
    }

    func mayRelease(processIsAbsent: Bool) -> Bool {
        guard hasVerifiedNotFound, transportContext?.owned_helper != nil else { return false }
        return hasMatchingHelperExit || processIsAbsent
    }
}

final class PolicyDeletionWorkspace: NSView, NSTableViewDataSource, NSTableViewDelegate, NSSearchFieldDelegate {
    private enum Phase {
        case idle
        case checkingCapability
        case previewing
        case reviewing
        case submitting
        case querying
        case pending
        case unknown
        case terminal
        case intentError
    }

    private let service: PolicyDeletionService
    private let intentStore: PolicyDeletionIntentStore
    private let onPoliciesReloaded: () -> Void
    private let outerScroll = NSScrollView()
    private let table = NSTableView()
    private let searchField = NSSearchField()
    private let familyPopup = NSPopUpButton()
    private let usagePopup = NSPopUpButton()
    private let selectionLabel = NSTextField(wrappingLabelWithString: "")
    private let listEmptyLabel = NSTextField(wrappingLabelWithString: "")
    private let pageLabel = NSTextField(labelWithString: "")
    private let capabilityLabel = NSTextField(wrappingLabelWithString: "삭제 가능 여부를 서버에서 확인합니다.")
    private let statusLabel = NSTextField(wrappingLabelWithString: "")
    private let detailsText = NSTextView()
    private let confirmationField = NSTextField(string: "")
    private let selectFilteredButton = NSButton(title: "필터 전체 선택", target: nil, action: nil)
    private let clearSelectionButton = NSButton(title: "선택 해제", target: nil, action: nil)
    private let previewButton = NSButton(title: "삭제 영향 확인", target: nil, action: nil)
    private let previousPageButton = NSButton(title: "이전 페이지", target: nil, action: nil)
    private let nextPageButton = NSButton(title: "다음 페이지", target: nil, action: nil)
    private let queryPendingButton = NSButton(title: "결과 조회", target: nil, action: nil)
    private let continueBatchButton = NSButton(title: "다음 묶음", target: nil, action: nil)
    private let confirmBatchButton = NSButton(title: "삭제 요청", target: nil, action: nil)
    private let cancelReviewButton = NSButton(title: "닫기", target: nil, action: nil)

    private var allPolicies: [RealPolicy] = []
    private var userPolicies: [RealPolicy] = []
    private var filteredPolicies: [RealPolicy] = []
    private var pagePolicies: [RealPolicy] = []
    private var selection = PolicyDeletionSelectionState()
    private var currentPage = 0
    private let pageSize = 50
    private var phase: Phase = .idle
    private var isVisibleWorkspace = false
    private var capability: PolicyDeletionCapability?
    private var capabilityRequest: NativeMCPRequest?
    private var previewRequest: NativeMCPRequest?
    private var writeRequest: NativeMCPRequest?
    private var queryRequest: NativeMCPRequest?
    private var pendingIntent: PolicyDeletionPendingIntent?
    private var intentLoadError: String?
    private var pendingReadback: PolicyDeletionReadback?
    private var recoveryFence: PolicyDeletionUnknownOutcomeFence?
    private var helperExitObserver: NSObjectProtocol?
    private var batchPlan: PolicyDeletionBatchPlan?
    private var activeBatchIDs: [String] = []
    private var activePreviews: [PolicyDeletePreview] = []
    private var selectionGeneration: UInt64 = 0
    private var readGeneration: UInt64 = 0
    private var expiryTimer: Timer?
    private var statusTextValue = ""

    init(
        service: PolicyDeletionService,
        intentStore: PolicyDeletionIntentStore,
        userPolicies: [RealPolicy],
        allPolicies: [RealPolicy],
        onPoliciesReloaded: @escaping () -> Void
    ) {
        self.service = service
        self.intentStore = intentStore
        self.onPoliciesReloaded = onPoliciesReloaded
        super.init(frame: .zero)
        helperExitObserver = NotificationCenter.default.addObserver(
            forName: .nativeMCPOwnedHelperExited,
            object: NativeMCPClient.shared,
            queue: .main
        ) { [weak self] notification in
            self?.handleOwnedHelperExit(notification)
        }
        do {
            if let pending = try intentStore.load() {
                pendingIntent = pending
                recoveryFence = PolicyDeletionUnknownOutcomeFence(transportContext: pending.transport_context)
                selection.selectAllFiltered(pending.expected_items.map(\.policy_id))
                phase = .unknown
                statusTextValue = "이전 요청의 결과를 확인해야 합니다. 확인을 마칠 때까지 새 요청은 멈춥니다."
            }
        } catch {
            intentLoadError = error.localizedDescription
            phase = .intentError
            statusTextValue = "이전 요청 상태를 확인할 수 없습니다. 새 요청은 멈춰 있습니다. \(error.localizedDescription)"
        }
        buildView()
        updatePolicies(userPolicies: userPolicies, allPolicies: allPolicies)
        updateControlEnablement()
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) { nil }

    deinit {
        expiryTimer?.invalidate()
        capabilityRequest?.cancel()
        queryRequest?.cancel()
        previewRequest?.cancel()
        if let helperExitObserver {
            NotificationCenter.default.removeObserver(helperExitObserver)
        }
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

    func updatePolicies(userPolicies: [RealPolicy], allPolicies: [RealPolicy]) {
        self.userPolicies = userPolicies.filter { $0.origin == "USER" }
        self.allPolicies = allPolicies
        populateFamilyFilter()
        applyFilters(resetPage: false)
    }

    func showPolicyRefreshError(_ error: Error) {
        statusTextValue = "정책 목록을 새로 고치지 못했습니다. 이전 목록을 표시합니다. \(error.localizedDescription)"
        renderStatus()
    }

    func setWorkspaceVisible(_ visible: Bool) {
        guard visible != isVisibleWorkspace else {
            if visible { refreshForCurrentContext() }
            return
        }
        isVisibleWorkspace = visible
        if visible {
            refreshForCurrentContext()
        } else {
            readGeneration &+= 1
            capabilityRequest?.cancel()
            capabilityRequest = nil
            previewRequest?.cancel()
            previewRequest = nil
            expiryTimer?.invalidate()
            expiryTimer = nil
            if phase == .previewing {
                phase = .idle
                activeBatchIDs = []
                activePreviews = []
            }
        }
        updateControlEnablement()
    }

    func refreshForCurrentContext() {
        guard isVisibleWorkspace, capabilityRequest == nil else { return }
        readGeneration &+= 1
        let generation = readGeneration
        if pendingIntent == nil, intentLoadError == nil {
            phase = .checkingCapability
            statusTextValue = "삭제 기능을 확인하고 있습니다."
        }
        renderStatus()
        let request = service.loadCapability { [weak self] result in
            guard let self, self.isVisibleWorkspace, self.readGeneration == generation else { return }
            self.capabilityRequest = nil
            switch result {
            case .failure(let error):
                self.capability = nil
                self.statusTextValue = "삭제 기능을 확인할 수 없습니다. 영향 확인만 가능합니다. \(error.localizedDescription)"
            case .success(let capability):
                self.capability = capability
                if let pending = self.pendingIntent {
                    if capability?.request_readback == true {
                        self.queryPendingIntent(pending)
                    } else {
                        self.phase = .unknown
                        self.statusTextValue = "요청 결과를 확인할 수 없습니다. 새 요청은 멈춰 있습니다."
                    }
                } else if capability?.permitsPolicyOnlyDelete == true {
                    self.phase = .idle
                    self.statusTextValue = "삭제 요청을 사용할 수 있습니다. 참조 중인 정책은 유지됩니다."
                } else {
                    self.phase = .idle
                    self.statusTextValue = "삭제 요청을 사용할 수 없습니다. 영향 확인만 가능합니다."
                }
            }
            self.renderStatus()
            self.updateControlEnablement()
            self.schedulePreviewExpiryCheck()
        }
        capabilityRequest = request.isPending ? request : nil
    }

    private func buildView() {
        outerScroll.hasVerticalScroller = true
        outerScroll.hasHorizontalScroller = false
        outerScroll.drawsBackground = false
        outerScroll.translatesAutoresizingMaskIntoConstraints = false
        outerScroll.identifier = NSUserInterfaceItemIdentifier("PolicyDeletionWorkspaceScroll")
        outerScroll.setAccessibilityLabel("정책 삭제 내용, 세로로 스크롤 가능")
        addSubview(outerScroll)

        let documentView = TerminalFlippedDocumentView()
        documentView.translatesAutoresizingMaskIntoConstraints = false
        outerScroll.documentView = documentView

        let title = NSTextField(labelWithString: "정책 삭제")
        title.font = TerminalTheme.screenTitle
        title.setAccessibilityLabel("사용자 정책 삭제 작업공간")

        let hint = NSTextField(wrappingLabelWithString: "사용자 정책의 삭제 가능 여부를 서버 미리보기로 확인합니다. 참조 중인 정책은 유지됩니다.")
        hint.font = TerminalTheme.body
        hint.textColor = TerminalTheme.secondaryText

        searchField.placeholderString = "이름 · 정책 ID · 패밀리 검색"
        searchField.delegate = self
        searchField.setAccessibilityLabel("사용자 정책 검색")

        familyPopup.addItem(withTitle: "모든 패밀리")
        familyPopup.setAccessibilityLabel("삭제 목록 패밀리 필터")
        familyPopup.target = self
        familyPopup.action = #selector(handleFilterChanged)

        usagePopup.addItems(withTitles: ["모든 사용 상태", "미참조", "과거 원장 참조"])
        usagePopup.setAccessibilityLabel("삭제 목록 사용 상태 필터")
        usagePopup.target = self
        usagePopup.action = #selector(handleFilterChanged)

        let filterRow = NSStackView(views: [searchField, familyPopup, usagePopup])
        let filterStacksVertically = TerminalTheme.textScale >= 1.5
        filterRow.orientation = filterStacksVertically ? .vertical : .horizontal
        filterRow.alignment = filterStacksVertically ? .leading : .centerY
        filterRow.spacing = filterStacksVertically ? 6 : 8
        searchField.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        if filterStacksVertically {
            for control in [searchField, familyPopup, usagePopup] {
                control.widthAnchor.constraint(equalTo: filterRow.widthAnchor).isActive = true
            }
        } else {
            searchField.widthAnchor.constraint(greaterThanOrEqualToConstant: 240).isActive = true
        }

        for (button, action, label) in [
            (selectFilteredButton, #selector(handleSelectFiltered), "현재 필터의 정책 모두 선택"),
            (clearSelectionButton, #selector(handleClearSelection), "모든 정책 선택 해제"),
            (previewButton, #selector(handlePreviewSelection), "선택한 정책의 삭제 영향 확인"),
            (queryPendingButton, #selector(handleQueryPending), "이전 삭제 요청 결과 조회"),
            (continueBatchButton, #selector(handleContinueBatch), "다음 정책 묶음 확인"),
            (confirmBatchButton, #selector(handleConfirmBatch), "이번 정책 묶음 삭제 요청"),
            (cancelReviewButton, #selector(handleCancelReview), "삭제 영향 확인 닫기")
        ] {
            button.bezelStyle = .rounded
            button.target = self
            button.action = action
            button.setAccessibilityLabel(label)
        }
        confirmBatchButton.contentTintColor = .systemRed
        confirmBatchButton.keyEquivalent = ""
        confirmBatchButton.setAccessibilityLabel("유효한 미리보기와 확인 문구로 정책 삭제 요청")
        cancelReviewButton.keyEquivalent = "\u{1b}"
        cancelReviewButton.setAccessibilityLabel("삭제 영향 확인 취소. 삭제 요청을 보내지 않습니다.")
        previewButton.setAccessibilityLabel("선택한 사용자 정책의 서버 삭제 영향 확인")
        queryPendingButton.setAccessibilityLabel("저장된 삭제 요청 결과 조회")
        continueBatchButton.setAccessibilityLabel("다음 사용자 정책 묶음의 삭제 영향 확인")

        let selectionActions = NSStackView(views: [selectFilteredButton, clearSelectionButton])
        let selectionActionsStackVertically = TerminalTheme.textScale >= 1.5
        selectionActions.orientation = selectionActionsStackVertically ? .vertical : .horizontal
        selectionActions.alignment = selectionActionsStackVertically ? .leading : .centerY
        selectionActions.spacing = 8
        if selectionActionsStackVertically {
            for button in [selectFilteredButton, clearSelectionButton] {
                button.widthAnchor.constraint(equalTo: selectionActions.widthAnchor).isActive = true
            }
        }

        let workflowActions = NSStackView(views: [previewButton, queryPendingButton, continueBatchButton])
        workflowActions.orientation = TerminalTheme.textScale >= 1.5 ? .vertical : .horizontal
        workflowActions.alignment = TerminalTheme.textScale >= 1.5 ? .leading : .centerY
        workflowActions.spacing = 8
        if TerminalTheme.textScale >= 1.5 {
            for button in [previewButton, queryPendingButton, continueBatchButton] {
                button.widthAnchor.constraint(equalTo: workflowActions.widthAnchor).isActive = true
            }
        }

        selectionLabel.font = TerminalTheme.monospacedDigitFont(ofSize: 12, weight: .regular)
        selectionLabel.textColor = TerminalTheme.secondaryText
        selectionLabel.setAccessibilityLabel("정책 선택 범위와 페이지 상태")
        listEmptyLabel.font = TerminalTheme.body
        listEmptyLabel.textColor = TerminalTheme.secondaryText
        listEmptyLabel.setAccessibilityLabel("사용자 정책 목록 상태")
        listEmptyLabel.isHidden = true
        pageLabel.font = TerminalTheme.monospacedDigitFont(ofSize: 12)
        pageLabel.textColor = TerminalTheme.secondaryText
        pageLabel.setAccessibilityLabel("사용자 정책 목록 페이지")

        previousPageButton.target = self
        previousPageButton.action = #selector(handlePreviousPage)
        nextPageButton.target = self
        nextPageButton.action = #selector(handleNextPage)
        previousPageButton.setAccessibilityLabel("이전 사용자 정책 페이지")
        nextPageButton.setAccessibilityLabel("다음 사용자 정책 페이지")
        let pageRow = NSStackView(views: [previousPageButton, pageLabel, nextPageButton])
        let pageRowStackVertically = TerminalTheme.textScale >= 1.5
        pageRow.orientation = pageRowStackVertically ? .vertical : .horizontal
        pageRow.alignment = pageRowStackVertically ? .leading : .centerY
        pageRow.spacing = 8
        if pageRowStackVertically {
            for control in [previousPageButton, pageLabel, nextPageButton] {
                control.widthAnchor.constraint(equalTo: pageRow.widthAnchor).isActive = true
            }
        }

        setupPolicyTable()
        let tableScroll = NSScrollView()
        tableScroll.documentView = table
        tableScroll.hasVerticalScroller = true
        tableScroll.hasHorizontalScroller = true
        tableScroll.autohidesScrollers = true
        tableScroll.setAccessibilityLabel("사용자 정책 테이블, 가로 및 세로 스크롤 가능")
        tableScroll.heightAnchor.constraint(greaterThanOrEqualToConstant: max(330, TerminalTheme.tableRowHeight * 7)).isActive = true

        capabilityLabel.font = TerminalTheme.body
        capabilityLabel.textColor = TerminalTheme.secondaryText
        capabilityLabel.setAccessibilityLabel("서버 정책 삭제 capability 상태")
        capabilityLabel.setAccessibilityValue(capabilityLabel.stringValue)
        statusLabel.font = TerminalTheme.body
        statusLabel.textColor = TerminalTheme.secondaryText
        statusLabel.setAccessibilityLabel("정책 삭제 요청 상태")
        statusLabel.setAccessibilityValue(statusLabel.stringValue)

        let inspectorTitle = NSTextField(labelWithString: "선택 정책과 삭제 영향")
        inspectorTitle.font = TerminalTheme.sectionHeading
        inspectorTitle.setAccessibilityLabel("정책 선택 및 삭제 영향 검사기")
        let reviewTitle = NSTextField(labelWithString: "서버 삭제 영향 및 요청 결과")
        reviewTitle.font = TerminalTheme.sectionHeading
        detailsText.isEditable = false
        detailsText.isSelectable = true
        detailsText.font = TerminalTheme.table
        detailsText.setAccessibilityLabel("선택한 정책의 삭제 영향과 요청 결과")
        let detailsScroll = TerminalWrappingTextScrollView()
        detailsScroll.configureWrapping(textView: detailsText, inset: NSSize(width: 10, height: 10))
        detailsScroll.borderType = .lineBorder
        detailsScroll.setAccessibilityLabel("정책 선택 범위, 서버 삭제 영향, 요청 결과")
        detailsScroll.heightAnchor.constraint(equalToConstant: max(220, 220 * TerminalTheme.textScale)).isActive = true

        confirmationField.placeholderString = "표시된 확인 문구를 입력하세요"
        confirmationField.font = TerminalTheme.table
        confirmationField.setAccessibilityLabel("이번 정책 삭제 배치 확인 문구")
        confirmationField.target = self
        confirmationField.action = #selector(handleConfirmationChanged)

        let confirmationRow = NSStackView(views: [confirmationField, cancelReviewButton, confirmBatchButton])
        confirmationRow.orientation = .vertical
        confirmationRow.alignment = .leading
        confirmationRow.spacing = 8
        for control in [confirmationField, cancelReviewButton, confirmBatchButton] {
            control.widthAnchor.constraint(equalTo: confirmationRow.widthAnchor).isActive = true
        }

        let listStack = NSStackView(views: [filterRow, selectionActions, selectionLabel, listEmptyLabel, tableScroll, pageRow])
        listStack.orientation = .vertical
        listStack.alignment = .leading
        listStack.spacing = 10
        let listPanel = TerminalSurfaceView()
        listPanel.usesPanel = true
        listPanel.drawsBorder = true
        listPanel.cornerRadius = 8
        listPanel.setAccessibilityLabel("사용자 정책 검색, 선택, 페이지 목록")
        listStack.translatesAutoresizingMaskIntoConstraints = false
        listPanel.addSubview(listStack)
        NSLayoutConstraint.activate([
            listStack.leadingAnchor.constraint(equalTo: listPanel.leadingAnchor, constant: 12),
            listStack.trailingAnchor.constraint(equalTo: listPanel.trailingAnchor, constant: -12),
            listStack.topAnchor.constraint(equalTo: listPanel.topAnchor, constant: 12),
            listStack.bottomAnchor.constraint(equalTo: listPanel.bottomAnchor, constant: -12),
            filterRow.widthAnchor.constraint(equalTo: listStack.widthAnchor),
            selectionActions.widthAnchor.constraint(equalTo: listStack.widthAnchor),
            selectionLabel.widthAnchor.constraint(equalTo: listStack.widthAnchor),
            listEmptyLabel.widthAnchor.constraint(equalTo: listStack.widthAnchor),
            tableScroll.widthAnchor.constraint(equalTo: listStack.widthAnchor),
            pageRow.widthAnchor.constraint(equalTo: listStack.widthAnchor)
        ])

        let inspectorStack = NSStackView(views: [
            inspectorTitle, capabilityLabel, statusLabel, workflowActions,
            reviewTitle, detailsScroll, confirmationRow
        ])
        inspectorStack.orientation = .vertical
        inspectorStack.alignment = .leading
        inspectorStack.spacing = 10
        inspectorStack.translatesAutoresizingMaskIntoConstraints = false
        let inspectorPanel = TerminalSurfaceView()
        inspectorPanel.usesPanel = true
        inspectorPanel.drawsBorder = true
        inspectorPanel.cornerRadius = 8
        inspectorPanel.setAccessibilityLabel("선택 정책 삭제 영향과 요청 상태 검사기")
        inspectorStack.translatesAutoresizingMaskIntoConstraints = false
        inspectorPanel.addSubview(inspectorStack)
        NSLayoutConstraint.activate([
            inspectorStack.leadingAnchor.constraint(equalTo: inspectorPanel.leadingAnchor, constant: 12),
            inspectorStack.trailingAnchor.constraint(equalTo: inspectorPanel.trailingAnchor, constant: -12),
            inspectorStack.topAnchor.constraint(equalTo: inspectorPanel.topAnchor, constant: 12),
            inspectorStack.bottomAnchor.constraint(equalTo: inspectorPanel.bottomAnchor, constant: -12),
            inspectorTitle.widthAnchor.constraint(equalTo: inspectorStack.widthAnchor),
            capabilityLabel.widthAnchor.constraint(equalTo: inspectorStack.widthAnchor),
            statusLabel.widthAnchor.constraint(equalTo: inspectorStack.widthAnchor),
            workflowActions.widthAnchor.constraint(equalTo: inspectorStack.widthAnchor),
            reviewTitle.widthAnchor.constraint(equalTo: inspectorStack.widthAnchor),
            detailsScroll.widthAnchor.constraint(equalTo: inspectorStack.widthAnchor),
            confirmationRow.widthAnchor.constraint(equalTo: inspectorStack.widthAnchor)
        ])

        let split = NSSplitView()
        split.isVertical = true
        split.dividerStyle = .thin
        split.setAccessibilityLabel("사용자 정책 목록과 삭제 영향 검사기")
        split.addArrangedSubview(listPanel)
        split.addArrangedSubview(inspectorPanel)
        listPanel.widthAnchor.constraint(greaterThanOrEqualToConstant: 380).isActive = true
        inspectorPanel.widthAnchor.constraint(greaterThanOrEqualToConstant: 420).isActive = true
        split.heightAnchor.constraint(greaterThanOrEqualToConstant: max(500, TerminalTheme.tableRowHeight * 10)).isActive = true

        let mainStack = NSStackView(views: [title, hint, split])
        mainStack.orientation = .vertical
        mainStack.alignment = .leading
        mainStack.spacing = 12
        mainStack.translatesAutoresizingMaskIntoConstraints = false
        documentView.addSubview(mainStack)
        NSLayoutConstraint.activate([
            outerScroll.leadingAnchor.constraint(equalTo: leadingAnchor),
            outerScroll.trailingAnchor.constraint(equalTo: trailingAnchor),
            outerScroll.topAnchor.constraint(equalTo: topAnchor),
            outerScroll.bottomAnchor.constraint(equalTo: bottomAnchor),
            documentView.widthAnchor.constraint(equalTo: outerScroll.contentView.widthAnchor),
            mainStack.leadingAnchor.constraint(equalTo: documentView.leadingAnchor, constant: 20),
            mainStack.trailingAnchor.constraint(equalTo: documentView.trailingAnchor, constant: -20),
            mainStack.topAnchor.constraint(equalTo: documentView.topAnchor, constant: 16),
            mainStack.bottomAnchor.constraint(equalTo: documentView.bottomAnchor, constant: -16),
            title.widthAnchor.constraint(equalTo: mainStack.widthAnchor),
            hint.widthAnchor.constraint(equalTo: mainStack.widthAnchor),
            split.widthAnchor.constraint(equalTo: mainStack.widthAnchor)
        ])
        setReviewVisible(false)
    }

    private func setupPolicyTable() {
        for (id, title, width) in [
            ("PolicyDeleteSelect", "선택", 46),
            ("PolicyDeleteName", "정책명", 190),
            ("PolicyDeleteID", "사용자 정책 ID", 190),
            ("PolicyDeleteFamily", "패밀리", 90),
            ("PolicyDeleteRevisions", "리비전 수", 80),
            ("PolicyDeleteUsage", "과거 원장 참조", 120)
        ] {
            let column = NSTableColumn(identifier: NSUserInterfaceItemIdentifier(id))
            column.title = title
            column.width = CGFloat(width)
            table.addTableColumn(column)
        }
        table.dataSource = self
        table.delegate = self
        table.target = self
        table.action = #selector(handlePolicyTableClick)
        table.setAccessibilityLabel("사용자 정책 단일·필터·선택 삭제 목록")
        TerminalTheme.prepareTable(table)
        table.columnAutoresizingStyle = .noColumnAutoresizing
    }

    private func populateFamilyFilter() {
        let current = familyPopup.titleOfSelectedItem
        let families = Array(Set(userPolicies.map(\.family))).sorted()
        familyPopup.removeAllItems()
        familyPopup.addItem(withTitle: "모든 패밀리")
        familyPopup.addItems(withTitles: families)
        if let current, familyPopup.itemTitles.contains(current) { familyPopup.selectItem(withTitle: current) }
        else { familyPopup.selectItem(at: 0) }
    }

    private func applyFilters(resetPage: Bool) {
        let query = searchField.stringValue.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        let family = familyPopup.indexOfSelectedItem > 0 ? familyPopup.titleOfSelectedItem : nil
        let usageIndex = usagePopup.indexOfSelectedItem
        filteredPolicies = userPolicies.filter { policy in
            let textMatch = query.isEmpty || [policy.policyId, policy.name, policy.family]
                .contains { $0.lowercased().contains(query) }
            let familyMatch = family.map { policy.family == $0 } ?? true
            let usageMatch = usageIndex == 1 ? policy.usageState == .unused
                : (usageIndex == 2 ? policy.usageState == .historicalOnly : true)
            return textMatch && familyMatch && usageMatch
        }
        if resetPage { currentPage = 0 }
        let pageCount = max(1, (filteredPolicies.count + pageSize - 1) / pageSize)
        currentPage = min(max(currentPage, 0), pageCount - 1)
        let start = currentPage * pageSize
        pagePolicies = Array(filteredPolicies.dropFirst(start).prefix(pageSize))
        table.reloadData()
        updateSummary()
        updateControlEnablement()
    }

    private func updateSummary() {
        let visibleIDs = Set(pagePolicies.map(\.policyId))
        let hidden = selection.hiddenSelectionCount(visiblePolicyIDs: visibleIDs)
        let unavailable = missingSelectedIDs.count
        selectionLabel.stringValue = "필터 \(filteredPolicies.count) · 사용자 정책 \(userPolicies.count) · 선택 \(selection.selectedPolicyIDs.count) · 현재 페이지 \(selection.selectedPolicyIDs.intersection(visibleIDs).count) · 숨김 \(hidden) · 확인 필요 \(unavailable)"
        selectionLabel.setAccessibilityValue(selectionLabel.stringValue)
        let emptyMessage = userPolicies.isEmpty
            ? "사용자 정책이 없습니다."
            : (filteredPolicies.isEmpty ? "현재 검색·필터와 일치하는 정책이 없습니다." : "")
        listEmptyLabel.stringValue = emptyMessage
        listEmptyLabel.setAccessibilityValue(emptyMessage)
        listEmptyLabel.isHidden = emptyMessage.isEmpty
        let pageCount = max(1, (filteredPolicies.count + pageSize - 1) / pageSize)
        pageLabel.stringValue = "페이지 \(currentPage + 1) / \(pageCount) · 한 번에 \(pageSize)개"
        pageLabel.setAccessibilityValue(pageLabel.stringValue)
        pageLabel.toolTip = "현재 필터에 일치하는 모든 정책 중 표시 구간입니다. 선택은 policy_id로 유지됩니다."
        if pendingIntent == nil, pendingReadback == nil, activePreviews.isEmpty {
            renderSelectionScope()
        }
    }

    private func renderSelectionScope() {
        let selectedIDs = selection.orderedPolicyIDs
        let batchIDs = batchPlan?.currentBatchIDs ?? Array(selectedIDs.prefix(PolicyDeletionBatchPlan.maximumBatchSize))
        let entries = batchIDs.map { policyID -> String in
            guard let policy = userPolicies.first(where: { $0.policyId == policyID }) else {
                return "policy_id: \(policyID) · 현재 정책 projection에 없음, 목록 새로고침 필요"
            }
            return "policy_id: \(policy.policyId)\nname: \(policy.name)\nhead_revision_id: \(policy.headRevisionId)\nrevision_count: \(policy.revisionCount) · historical_run_count: \(policy.historicalRunCount) · usage: \(policy.usageState.rawValue)"
        }
        let continuation = selectedIDs.count > batchIDs.count
            ? "\n\n다음 묶음에서 확인할 정책: \(selectedIDs.count - batchIDs.count)건"
            : ""
        detailsText.string = selectedIDs.isEmpty
            ? """
              사용자 정책을 선택하면 exact policy_id와 리비전 identity가 여기에 표시됩니다.

              이 목록은 카탈로그 projection입니다. dependents, blockers와 삭제 범위는 ‘삭제 영향 확인’을 눌러 서버 미리보기를 읽은 뒤 표시됩니다.
              """
            : """
              카탈로그 선택 범위 · 전체 선택 \(selectedIDs.count)건 · 현재 묶음 \(batchIDs.count)건
              이 내용은 카탈로그 projection이며 서버 삭제 scope가 아닙니다. dependents, blockers와 삭제 범위는 서버 미리보기 결과에서 확인하세요.

              \(entries.joined(separator: "\n\n"))\(continuation)
              """
        detailsText.toolTip = detailsText.string
        detailsText.setAccessibilityValue(detailsText.string)
    }

    func numberOfRows(in tableView: NSTableView) -> Int {
        tableView === table ? pagePolicies.count : 0
    }

    func tableView(_ tableView: NSTableView, viewFor tableColumn: NSTableColumn?, row: Int) -> NSView? {
        guard tableView === table,
              pagePolicies.indices.contains(row),
              let identifier = tableColumn?.identifier.rawValue else { return nil }
        let policy = pagePolicies[row]
        if identifier == "PolicyDeleteSelect" {
            let checkbox = NSButton(checkboxWithTitle: "", target: self, action: #selector(handlePolicyCheckbox(_:)))
            checkbox.tag = row
            checkbox.state = selection.selectedPolicyIDs.contains(policy.policyId) ? .on : .off
            checkbox.setAccessibilityLabel("\(policy.name) (\(policy.policyId)) 선택")
            checkbox.setAccessibilityValue(selection.selectedPolicyIDs.contains(policy.policyId) ? "선택됨" : "선택 안 됨")
            return checkbox
        }

        let value: String
        switch identifier {
        case "PolicyDeleteID": value = policy.policyId
        case "PolicyDeleteName": value = policy.name
        case "PolicyDeleteFamily": value = policy.family
        case "PolicyDeleteRevisions": value = String(policy.revisionCount)
        case "PolicyDeleteUsage": value = policy.usageState.rawValue
        default: return nil
        }
        let cell = NSTextField(labelWithString: value)
        cell.font = TerminalTheme.table
        cell.lineBreakMode = .byTruncatingMiddle
        cell.toolTip = value
        cell.setAccessibilityLabel("\(tableColumn?.title ?? identifier): \(value)")
        return cell
    }

    func controlTextDidChange(_ notification: Notification) {
        applyFilters(resetPage: true)
    }

    @objc private func handleFilterChanged() {
        applyFilters(resetPage: true)
    }

    @objc private func handlePolicyCheckbox(_ sender: NSButton) {
        guard pagePolicies.indices.contains(sender.tag) else { return }
        selection.setSelected(pagePolicies[sender.tag].policyId, selected: sender.state == .on)
        selectionGeneration &+= 1
        invalidatePreviewAfterSelectionChange()
        table.reloadData()
        updateSummary()
        updateControlEnablement()
    }

    @objc private func handlePolicyTableClick() {
        let row = table.clickedRow
        let column = table.clickedColumn
        guard column > 0, pagePolicies.indices.contains(row) else { return }
        let policyId = pagePolicies[row].policyId
        let shouldSelect = !selection.selectedPolicyIDs.contains(policyId)
        selection.setSelected(policyId, selected: shouldSelect)
        selectionGeneration &+= 1
        invalidatePreviewAfterSelectionChange()
        table.reloadData()
        table.selectRowIndexes(IndexSet(integer: row), byExtendingSelection: false)
        updateSummary()
        updateControlEnablement()
    }

    @objc private func handleSelectFiltered() {
        selection.selectAllFiltered(filteredPolicies.map(\.policyId))
        selectionGeneration &+= 1
        invalidatePreviewAfterSelectionChange()
        table.reloadData()
        updateSummary()
        updateControlEnablement()
    }

    @objc private func handleClearSelection() {
        selection.clear()
        selectionGeneration &+= 1
        invalidatePreviewAfterSelectionChange()
        batchPlan = nil
        table.reloadData()
        updateSummary()
        updateControlEnablement()
    }

    @objc private func handlePreviousPage() {
        guard currentPage > 0 else { return }
        currentPage -= 1
        applyFilters(resetPage: false)
    }

    @objc private func handleNextPage() {
        let pageCount = max(1, (filteredPolicies.count + pageSize - 1) / pageSize)
        guard currentPage + 1 < pageCount else { return }
        currentPage += 1
        applyFilters(resetPage: false)
    }

    @objc private func handlePreviewSelection() {
        guard canStartPreview else { return }
        let selectedIDs = selection.orderedPolicyIDs
        let availableIDs = Set(userPolicies.map(\.policyId))
        let missingIDs = selectedIDs.filter { !availableIDs.contains($0) }
        guard missingIDs.isEmpty else {
            statusTextValue = "정책 목록을 새로 고친 뒤 다시 확인하세요. \(missingIDs.joined(separator: ", "))"
            renderStatus()
            return
        }
        batchPlan = PolicyDeletionBatchPlan(policyIDs: selectedIDs)
        beginPreviewForCurrentBatch()
    }

    private func beginPreviewForCurrentBatch() {
        guard pendingIntent == nil, let batchPlan else { return }
        let ids = batchPlan.currentBatchIDs
        guard (1...100).contains(ids.count) else {
            statusTextValue = "선택한 정책 묶음이 올바르지 않습니다. 다시 선택하세요."
            phase = .idle
            renderStatus()
            return
        }
        activeBatchIDs = ids
        activePreviews = []
        pendingReadback = nil
        phase = .previewing
        statusTextValue = "선택 \(batchPlan.totalCount)건 · 이번 묶음 \(ids.count)건의 삭제 영향을 확인하고 있습니다."
        setReviewVisible(false)
        renderStatus()
        updateControlEnablement()

        let generation = selectionGeneration
        let request = service.readPreviews(policyIDs: ids) { [weak self] result in
            guard let self,
                  self.isVisibleWorkspace,
                  self.selectionGeneration == generation,
                  self.phase == .previewing,
                  self.activeBatchIDs == ids else { return }
            self.previewRequest = nil
            switch result {
            case .failure(let error):
                self.activePreviews = []
                self.phase = .idle
                self.statusTextValue = "삭제 영향 확인에 실패했습니다. 요청은 보내지 않았습니다. \(error.localizedDescription)"
                self.setReviewVisible(false)
            case .success(let previews):
                guard previews.map(\.resource.policy_id) == ids else {
                    self.activePreviews = []
                    self.phase = .idle
                    self.statusTextValue = "정책 목록이 달라 요청을 막았습니다. 다시 확인하세요."
                    self.setReviewVisible(false)
                    self.updateControlEnablement()
                    return
                }
                self.activePreviews = previews
                self.phase = .reviewing
                self.confirmationField.stringValue = ""
                self.renderReviewDetails()
                self.setReviewVisible(true)
                self.window?.makeFirstResponder(self.cancelReviewButton)
                if self.locallyEligiblePreviewCount == 0 {
                    self.statusTextValue = "이 묶음에서 삭제할 수 있는 정책이 없습니다. 참조 중인 정책은 유지됩니다."
                } else if self.locallyEligiblePreviewCount < ids.count {
                    self.statusTextValue = "참조 중인 정책은 유지됩니다. 항목별 결과를 확인하세요."
                } else {
                    self.statusTextValue = "참조 중인 정책은 유지됩니다. 확인 후 삭제를 요청하세요."
                }
            }
            self.renderStatus()
            self.updateControlEnablement()
            self.schedulePreviewExpiryCheck()
        }
        previewRequest = request.isPending ? request : nil
    }

    @objc private func handleConfirmationChanged() {
        updateControlEnablement()
    }

    @objc private func handleCancelReview() {
        guard pendingIntent == nil else { return }
        previewRequest?.cancel()
        previewRequest = nil
        expiryTimer?.invalidate()
        expiryTimer = nil
        phase = .idle
        batchPlan = nil
        activeBatchIDs = []
        activePreviews = []
        confirmationField.stringValue = ""
        statusTextValue = "삭제 영향 확인을 닫았습니다. 정책은 변경되지 않았습니다."
        setReviewVisible(false)
        renderStatus()
        updateControlEnablement()
    }

    @objc private func handleConfirmBatch() {
        guard phase == .reviewing,
              pendingIntent == nil,
              capability?.permitsPolicyOnlyDelete == true,
              allActivePreviewsStructurallyValid,
              locallyEligiblePreviewCount > 0,
              batchPlan != nil else { return }
        let allowedConfirmations = activeBatchIDs.count == 1
            ? allowedSingleConfirmations(for: activeBatchIDs[0])
            : []
        let requestID = UUID().uuidString.lowercased()
        let request: PolicyDeletionWriteRequest
        do {
            request = try service.makeWriteRequest(
                requestID: requestID,
                expectedPolicyIDs: activeBatchIDs,
                previews: activePreviews,
                confirmationText: confirmationField.stringValue,
                allowedSingleConfirmations: allowedConfirmations
            )
        } catch {
            statusTextValue = "확인 문구가 맞지 않거나 확인 내용이 만료되었습니다. 다시 확인하세요. \(error.localizedDescription)"
            phase = .idle
            self.batchPlan = nil
            activePreviews = []
            expiryTimer?.invalidate()
            expiryTimer = nil
            setReviewVisible(false)
            renderStatus()
            updateControlEnablement()
            return
        }

        let expectedItems = request.items.compactMap { item -> PolicyDeletionExpectedItem? in
            guard let scopeDigest = item.preview?.scope_digest else { return nil }
            return PolicyDeletionExpectedItem(policy_id: item.policy_id, scope_digest: scopeDigest)
        }
        guard expectedItems.count == request.items.count else {
            statusTextValue = "요청을 만들 수 없습니다. 삭제 영향을 다시 확인하세요."
            phase = .idle
            self.batchPlan = nil
            activePreviews = []
            setReviewVisible(false)
            renderStatus()
            updateControlEnablement()
            return
        }
        guard let transportContext = service.currentTransportContext() else {
            statusTextValue = "연결이 바뀌었거나 끊겼습니다. 삭제 영향을 다시 확인하세요."
            updateControlEnablement()
            renderStatus()
            return
        }
        if let helper = transportContext.owned_helper,
           helper.data_root_identity != intentStore.dataRootIdentity {
            statusTextValue = "데이터 원본이 달라 요청하지 않았습니다."
            updateControlEnablement()
            renderStatus()
            return
        }
        let intent = PolicyDeletionPendingIntent(
            request_id: requestID,
            data_root_identity: intentStore.dataRootIdentity,
            expected_items: expectedItems,
            transport_context: transportContext
        )
        do {
            try intentStore.save(intent)
        } catch {
            statusTextValue = "요청을 안전하게 저장하지 못해 보내지 않았습니다. \(error.localizedDescription)"
            phase = .intentError
            intentLoadError = error.localizedDescription
            renderStatus()
            updateControlEnablement()
            return
        }

        pendingIntent = intent
        pendingReadback = nil
        recoveryFence = PolicyDeletionUnknownOutcomeFence(transportContext: transportContext)
        phase = .submitting
        confirmationField.stringValue = ""
        expiryTimer?.invalidate()
        expiryTimer = nil
        statusTextValue = "삭제 요청을 보냈습니다. 결과를 확인하고 있습니다."
        renderStatus()
        renderReviewDetails()
        updateControlEnablement()

        writeRequest = service.submit(
            request,
            expectedItems: expectedItems,
            allowedSingleConfirmations: allowedConfirmations,
            expectedTransportContext: transportContext
        ) { [weak self] result in
            guard let self, self.pendingIntent?.request_id == requestID else { return }
            self.writeRequest = nil
            switch result {
            case .failure(let error):
                self.phase = .unknown
                self.statusTextValue = "요청 결과를 확인할 수 없습니다. 새 요청은 멈춰 있습니다. \(error.localizedDescription)"
                self.renderReviewDetails()
                self.renderStatus()
                self.updateControlEnablement()
            case .success(let readback):
                self.acceptReadback(readback, intent: intent)
            }
        }
    }

    @objc private func handleQueryPending() {
        guard let intent = pendingIntent else { return }
        queryPendingIntent(intent)
    }

    private func queryPendingIntent(_ intent: PolicyDeletionPendingIntent) {
        guard pendingIntent?.request_id == intent.request_id,
              intent.data_root_identity == intentStore.dataRootIdentity,
              capability?.request_readback == true,
              queryRequest == nil else { return }
        phase = .querying
        statusTextValue = "이전 요청 결과를 확인하고 있습니다."
        renderStatus()
        updateControlEnablement()
        let request = service.query(requestID: intent.request_id, expectedItems: intent.expected_items) { [weak self] result in
            guard let self, self.pendingIntent?.request_id == intent.request_id else { return }
            self.queryRequest = nil
            switch result {
            case .failure(let error):
                self.phase = .unknown
                self.statusTextValue = "요청 결과를 확인하지 못했습니다. 새 요청은 멈춰 있습니다. \(error.localizedDescription)"
                self.renderStatus()
                self.updateControlEnablement()
            case .success(let readback):
                self.acceptReadback(readback, intent: intent)
            }
        }
        queryRequest = request.isPending ? request : nil
    }

    private func acceptReadback(_ readback: PolicyDeletionReadback, intent: PolicyDeletionPendingIntent) {
        guard pendingIntent?.request_id == intent.request_id else { return }
        pendingReadback = readback
        guard readback.found else {
            phase = .unknown
            statusTextValue = "요청 결과가 아직 보이지 않습니다. 새 요청은 멈춰 있습니다."
            recoveryFence?.recordNotFound(for: intent.transport_context)
            renderReviewDetails()
            renderStatus()
            updateControlEnablement()
            releaseNotFoundFenceAfterConfirmedHelperExit(intent)
            return
        }
        guard let state = readback.state else {
            phase = .unknown
            statusTextValue = "요청이 끝나지 않았습니다. 새 요청은 멈춰 있습니다."
            renderStatus()
            updateControlEnablement()
            return
        }
        renderReviewDetails()
        if state == .pending {
            phase = .pending
            statusTextValue = "요청 처리 중입니다. 새 요청은 멈춰 있습니다."
            renderStatus()
            updateControlEnablement()
            return
        }

        do {
            try intentStore.clear(requestID: intent.request_id)
        } catch {
            phase = .unknown
            statusTextValue = "요청 결과를 확인했지만 다음 요청을 준비하지 못했습니다. 새 요청은 멈춰 있습니다. \(error.localizedDescription)"
            renderStatus()
            updateControlEnablement()
            return
        }
        pendingIntent = nil
        intentLoadError = nil
        recoveryFence = nil
        let deletedIDs = Set((readback.items ?? []).filter { $0.status == .deleted }.map(\.policy_id))
        selection.removeConfirmedDeleted(deletedIDs)
        phase = .terminal
        statusTextValue = "요청이 끝났습니다. 남은 정책은 새로 확인해야 합니다."
        onPoliciesReloaded()
        renderReviewDetails()
        renderStatus()
        updateSummary()
        updateControlEnablement()
    }

    private func handleOwnedHelperExit(_ notification: Notification) {
        guard let context = notification.userInfo?["context"] as? NativeMCPTransportContext,
              let intent = pendingIntent,
              intent.transport_context == context else { return }
        recoveryFence?.recordHelperExit(context)
        releaseNotFoundFenceAfterConfirmedHelperExit(intent)
    }

    private func releaseNotFoundFenceAfterConfirmedHelperExit(_ intent: PolicyDeletionPendingIntent) {
        let processIsAbsent = service.ownedHelperIsAbsent(
            in: intent.transport_context,
            expectedDataRootIdentity: intent.data_root_identity
        )
        guard pendingIntent?.request_id == intent.request_id,
              pendingReadback?.found == false,
              recoveryFence?.mayRelease(processIsAbsent: processIsAbsent) == true else { return }
        do {
            try intentStore.clear(requestID: intent.request_id)
        } catch {
            phase = .unknown
            statusTextValue = "이전 요청을 안전하게 정리하지 못했습니다. 새 요청은 멈춰 있습니다. \(error.localizedDescription)"
            renderStatus()
            updateControlEnablement()
            return
        }
        pendingIntent = nil
        intentLoadError = nil
        recoveryFence = nil
        phase = .idle
        batchPlan = nil
        activeBatchIDs = []
        activePreviews = []
        confirmationField.stringValue = ""
        statusTextValue = "이전 요청이 종료되었습니다. 삭제 영향을 다시 확인한 뒤 요청할 수 있습니다."
        renderReviewDetails()
        renderStatus()
        updateControlEnablement()
    }

    @objc private func handleContinueBatch() {
        guard pendingIntent == nil,
              capability?.permitsPolicyOnlyDelete == true,
              phase == .terminal,
              var batchPlan,
              batchPlan.advanceAfterExplicitUserAction() else { return }
        self.batchPlan = batchPlan
        beginPreviewForCurrentBatch()
    }

    private func invalidatePreviewAfterSelectionChange() {
        guard pendingIntent == nil else { return }
        if phase == .previewing {
            previewRequest?.cancel()
            previewRequest = nil
            activeBatchIDs = []
            activePreviews = []
            batchPlan = nil
            phase = .idle
            statusTextValue = "선택이 바뀌었습니다. 삭제 영향을 다시 확인하세요."
            setReviewVisible(false)
        } else if phase == .reviewing || phase == .terminal {
            activeBatchIDs = []
            activePreviews = []
            batchPlan = nil
            phase = .idle
            setReviewVisible(false)
            statusTextValue = "선택이 바뀌어 확인 내용을 닫았습니다."
        }
    }

    private var missingSelectedIDs: [String] {
        let available = Set(userPolicies.map(\.policyId))
        return selection.orderedPolicyIDs.filter { !available.contains($0) }
    }

    private var allActivePreviewsStructurallyValid: Bool {
        guard !activeBatchIDs.isEmpty, activePreviews.count == activeBatchIDs.count,
              activePreviews.map(\.resource.policy_id) == activeBatchIDs else { return false }
        let validator = PolicyDeletePreviewValidator()
        return zip(activePreviews, activeBatchIDs).allSatisfy { preview, policyID in
            validator.structuralBlocker(for: preview, expectedPolicyID: policyID, at: Date()) == nil
        }
    }

    private var locallyEligiblePreviewCount: Int {
        guard allActivePreviewsStructurallyValid else { return 0 }
        let validator = PolicyDeletePreviewValidator()
        return activePreviews.filter { validator.localEligibilityBlocker(for: $0) == nil }.count
    }

    private func allowedSingleConfirmations(for policyID: String) -> Set<String> {
        PolicyDeletionConfirmation.allowedSingleValues(
            policyID: policyID,
            policyName: allPolicies.first(where: { $0.policyId == policyID })?.name,
            currentPolicyNames: allPolicies.map(\.name)
        )
    }

    private func preferredConfirmation(for ids: [String]) -> String {
        guard ids.count == 1 else { return "DELETE \(ids.count) POLICIES" }
        let allowed = allowedSingleConfirmations(for: ids[0])
        guard let policy = allPolicies.first(where: { $0.policyId == ids[0] }),
              allowed.contains(policy.name) else { return ids[0] }
        return policy.name
    }

    private func renderReviewDetails() {
        if let intent = pendingIntent {
            let readbackText = pendingReadback.map(formatReadback) ?? "요청 결과 조회 전"
            detailsText.string = """
            보류 중 request_id: \(intent.request_id)
            데이터 원본: \(intent.data_root_identity)
            대상 정책 \(intent.expected_items.count)건:
            \(intent.expected_items.map { "• \($0.policy_id) · scope \($0.scope_digest)" }.joined(separator: "\n"))

            \(readbackText)
            """
            detailsText.setAccessibilityValue(detailsText.string)
            setReviewVisible(true)
            return
        }
        if let pendingReadback {
            detailsText.string = formatReadback(pendingReadback)
            detailsText.setAccessibilityValue(detailsText.string)
            setReviewVisible(true)
            return
        }
        guard !activePreviews.isEmpty, let plan = batchPlan else {
            renderSelectionScope()
            setReviewVisible(phase == .terminal)
            return
        }
        let expected = preferredConfirmation(for: activeBatchIDs)
        let validator = PolicyDeletePreviewValidator()
        let structuralBlockers = zip(activePreviews, activeBatchIDs).compactMap { preview, id -> String? in
            validator.structuralBlocker(for: preview, expectedPolicyID: id, at: Date())
                .map { "\(id): \($0)" }
        }
        let previewIneligible = zip(activePreviews, activeBatchIDs).compactMap { preview, id -> String? in
            validator.localEligibilityBlocker(for: preview).map { "\(id): \($0)" }
        }
        let remediation = activePreviews.contains(where: { !$0.blockers.isEmpty })
            ? "\n참조 중인 정책은 유지됩니다."
            : ""
        detailsText.string = """
        선택 \(plan.totalCount)건 · 이번 묶음 \(activeBatchIDs.count)건 · 삭제 후보 \(locallyEligiblePreviewCount)건 · 남은 정책 \(plan.remainingCount)건
        확인 문구: \(expected)
        참조 중인 정책은 유지됩니다.\(remediation)
        \(structuralBlockers.isEmpty ? "확인 정보가 유효합니다." : "확인 정보가 올바르지 않아 요청을 막았습니다:\n\(structuralBlockers.joined(separator: "\n"))")
        \(previewIneligible.isEmpty ? "삭제 요청을 준비할 수 있습니다." : "확인이 필요한 정책이 포함되어 있습니다. 항목별 결과를 확인하세요:\n\(previewIneligible.joined(separator: "\n"))")

        \(activePreviews.map(\.summary).joined(separator: "\n\n────────────────────────\n\n"))
        """
        detailsText.toolTip = detailsText.string
        detailsText.setAccessibilityValue(detailsText.string)
        setReviewVisible(phase == .reviewing || phase == .submitting || phase == .terminal)
    }

    private func formatReadback(_ readback: PolicyDeletionReadback) -> String {
        guard readback.found else { return "readback: NOT_FOUND · 이전 RPC가 늦게 도착할 수 있어 fence 유지" }
        let countText = readback.counts.map {
            "DELETED \($0.deleted) · BLOCKED \($0.blocked) · STALE \($0.stale) · FAILED \($0.failed) · INTERRUPTED \($0.interrupted_not_applied) · PENDING \($0.pending) · RETAINED \($0.retained)"
        } ?? "카운트 미확인"
        let itemsText = readback.items?.map { item in
            let remediation = item.remediation_code == "ARCHIVE_DEACTIVATE_UNAVAILABLE"
                ? " · Archive/deactivation unavailable; policy retained"
                : (item.remediation_code.map { " · \($0)" } ?? "")
            return "\(item.policy_id) · \(item.status.rawValue) · \(item.effect) · retained \(item.retained) · deleted revisions \(item.deleted_revision_count)\(item.reason_code.map { " · \($0)" } ?? "")\(remediation)"
        }.joined(separator: "\n") ?? "item outcomes missing"
        return "request_id: \(readback.request_id) · state \(readback.state?.rawValue ?? "UNKNOWN") · \(countText)\n\(itemsText)"
    }

    private func setReviewVisible(_ visible: Bool) {
        if !visible, pendingIntent == nil, pendingReadback == nil, activePreviews.isEmpty {
            renderSelectionScope()
        }
        confirmationField.isHidden = !(visible && phase == .reviewing)
        confirmBatchButton.isHidden = !(visible && phase == .reviewing)
        cancelReviewButton.isHidden = !(visible && phase == .reviewing)
    }

    private func renderStatus() {
        capabilityLabel.stringValue = capability?.permitsPolicyOnlyDelete == true
            ? "삭제 요청을 사용할 수 있습니다. 참조 중인 정책은 유지됩니다."
            : "삭제 요청을 사용할 수 없습니다. 영향 확인만 가능합니다."
        capabilityLabel.setAccessibilityValue(capabilityLabel.stringValue)
        capabilityLabel.toolTip = capability?.permitsPolicyOnlyDelete == true
            ? "POLICY_ONLY_CONFIRMED · max_items \(capability?.max_items ?? 100) · archive unavailable · request readback available"
            : "policy deletion capability unavailable; preview only"
        statusLabel.stringValue = statusTextValue
        statusLabel.toolTip = statusTextValue
        statusLabel.textColor = pendingIntent == nil ? TerminalTheme.secondaryText : .systemOrange
        statusLabel.setAccessibilityLabel(statusTextValue)
        statusLabel.setAccessibilityValue(statusTextValue)
    }

    private func schedulePreviewExpiryCheck() {
        expiryTimer?.invalidate()
        expiryTimer = nil
        guard isVisibleWorkspace, phase == .reviewing,
              let earliest = activePreviews.compactMap(Self.expiryDate).min() else { return }
        let delay = max(0.1, earliest.timeIntervalSinceNow)
        expiryTimer = Timer.scheduledTimer(withTimeInterval: delay, repeats: false) { [weak self] _ in
            guard let self, self.phase == .reviewing else { return }
            self.statusTextValue = "확인 시간이 지났습니다. 삭제 영향을 다시 확인하세요."
            self.updateControlEnablement()
            self.renderStatus()
        }
    }

    private static func expiryDate(_ preview: PolicyDeletePreview) -> Date? {
        let parser = PolicyDeletePreviewValidator()
        let result = parser.expiryDate(preview.expires_at)
        return result
    }

    private var canStartPreview: Bool {
        pendingIntent == nil && intentLoadError == nil && !selection.selectedPolicyIDs.isEmpty
            && missingSelectedIDs.isEmpty && previewRequest == nil && phase != .previewing
            && phase != .reviewing && phase != .submitting && phase != .querying
    }

    private func updateControlEnablement() {
        let hasPendingFence = pendingIntent != nil || intentLoadError != nil
        selectFilteredButton.isEnabled = !filteredPolicies.isEmpty
        clearSelectionButton.isEnabled = !selection.selectedPolicyIDs.isEmpty
        previewButton.isEnabled = canStartPreview
        queryPendingButton.isHidden = pendingIntent == nil
        queryPendingButton.isEnabled = pendingIntent != nil && queryRequest == nil
            && capability?.request_readback == true
        let canContinue = pendingIntent == nil && phase == .terminal
            && capability?.permitsPolicyOnlyDelete == true && (batchPlan?.hasNextBatch ?? false)
        continueBatchButton.isHidden = !canContinue
        continueBatchButton.isEnabled = canContinue && previewRequest == nil
        previousPageButton.isEnabled = currentPage > 0
        nextPageButton.isEnabled = currentPage + 1 < max(1, (filteredPolicies.count + pageSize - 1) / pageSize)
        let confirmationOptions = activeBatchIDs.count == 1
            ? allowedSingleConfirmations(for: activeBatchIDs[0])
            : Set(["DELETE \(activeBatchIDs.count) POLICIES"])
        confirmBatchButton.isEnabled = phase == .reviewing
            && !hasPendingFence
            && capability?.permitsPolicyOnlyDelete == true
            && allActivePreviewsStructurallyValid
            && locallyEligiblePreviewCount > 0
            && confirmationOptions.contains(confirmationField.stringValue)
    }
}

