import AppKit

struct TradingControlAvailability: Equatable {
    let emergency: Bool

    static let backendControlUnavailable = TradingControlAvailability(
        emergency: false
    )
}

protocol SettingsTradingService: AnyObject {
    @discardableResult
    func fetchControlStatus(completion: @escaping (Result<TradingControlSnapshot, Error>) -> Void) -> NativeMCPRequest?
    @discardableResult
    func fetchAccounts(completion: @escaping (Result<[UpbitLiveAccount], Error>) -> Void) -> NativeMCPRequest?
    func safeControl(
        action: String,
        requestId: String,
        sequence: UInt64,
        expectedContext: NativeMCPTransportContext?,
        completion: @escaping (Result<TradingControlSnapshot, Error>) -> Void
    )
    func currentTransportContext() -> NativeMCPTransportContext?
    func isCurrentTransportContext(_ context: NativeMCPTransportContext) -> Bool
    @discardableResult
    func fetchLiveOrderControlStatus(
        expectedContext: NativeMCPTransportContext,
        completion: @escaping (Result<LiveOrderControlSnapshot, Error>) -> Void
    ) -> NativeMCPRequest
    @discardableResult
    func changeLiveOrderControl(
        action: String,
        requestID: String,
        expectedSequence: UInt64,
        expectedEpoch: String,
        confirmation: String?,
        expectedContext: NativeMCPTransportContext,
        completion: @escaping (Result<LiveOrderControlSnapshot, Error>) -> Void
    ) -> NativeMCPRequest
    @discardableResult
    func readLiveOrderControlRequest(
        requestID: String,
        expectedContext: NativeMCPTransportContext,
        completion: @escaping (Result<LiveOrderControlSnapshot, Error>) -> Void
    ) -> NativeMCPRequest
}

extension UpbitLiveService: SettingsTradingService {}

final class SettingsWindowController: NSWindowController, NSWindowDelegate {
    private let store: ConfigurationStore
    private let tradingService: SettingsTradingService
    private let now: () -> Date
    private let domainField = NSTextField()
    private let portField = NSTextField()
    private let authPopup = NSPopUpButton()
    private let tradingMcpToggle = NSButton(checkboxWithTitle: "MCP 계좌 조회·전략 제어·주문 전송 허용", target: nil, action: nil)
    private let statusLabel = NSTextField(labelWithString: "MCP 상태: 확인 전")

    private let krwBalanceDisplay = NSTextField(labelWithString: "금액 숨김")
    private let balanceDetailLabel = NSTextField(labelWithString: "계좌를 조회하기 전입니다.")
    private let cryptoHoldingsLabel = NSTextField(labelWithString: "보유 자산: 알 수 없음")
    private let liveApiBadge = NSTextField(labelWithString: "읽기 상태: 확인 전")
    private let refreshButton = NSButton()
    private let privacyButton = NSButton()
    private var unmaskedKRWBalance = "—"
    private var unmaskedBalanceDetail = "계좌를 조회하기 전입니다."
    private var unmaskedCryptoHoldings = "보유 자산: 알 수 없음"
    private var isBalanceMasked = true

    private let tradingStateLabel = NSTextField(labelWithString: "전략/전역 거래 제어: 미확인 · 서버 상태 조회 전")
    private let strategyAdmissionButton = NSButton()
    private let strategyAdmissionStatusLabel = NSTextField(wrappingLabelWithString: "계좌 전체 전략 권한: 서버 상태 확인 전")
    private let automationWorkspaceButton = NSButton()
    private let emergencyButton = NSButton()
    private let controlRefreshButton = NSButton()
    private let liveOrderControlStateLabel = NSTextField(wrappingLabelWithString: "확인 불가 · OFF로 판단하지 않습니다")
    private let liveOrderControlDetailsLabel = NSTextField(wrappingLabelWithString: "신규 주문 차단 상태: 확인 불가 · 긴급 정지 상태: 확인 불가 · 마지막 서버 확인 없음")
    private let liveOrderControlRefreshButton = NSButton()
    private let enableLiveOrderControlButton = NSButton()
    private let disableLiveOrderControlButton = NSButton()
    private let saveButton = NSButton()
    private var persistedConfiguration = AppConfiguration.default
    private var currentControlSnapshot: TradingControlSnapshot?
    private var controlRequestInFlight = false
    private var controlReadPending = false
    private var pendingStrategyAdmission: PendingStrategyAdmission?
    private var strategyAdmissionExitObserver: NSObjectProtocol?
    private var strategyAdmissionStatusMessage: String?
    private var balanceReadPending = false
    private var readGeneration: UInt64 = 0
    private var balanceReadRequest: NativeMCPRequest?
    private var controlReadRequest: NativeMCPRequest?
    private var liveOrderControlReadRequest: NativeMCPRequest?
    private var liveOrderControlMutationRequest: NativeMCPRequest?
    private var liveOrderControlRequestInFlight = false
    private var currentLiveOrderControlSnapshot: LiveOrderControlSnapshot?
    private var currentLiveOrderControlContext: NativeMCPTransportContext?
    private var lastKnownLiveOrderControl: LiveOrderControlObservation?
    private var pendingLiveOrderControlOperation: PendingLiveOrderControlOperation?
    private var liveOrderControlOperationMessage: String?
    private var freshnessTimer: Timer?

    private struct LiveOrderControlObservation {
        let snapshot: LiveOrderControlSnapshot
        let receivedAt: Date
        let context: NativeMCPTransportContext
    }

    private struct PendingStrategyAdmission {
        enum Phase {
            case sending
            case unknown
        }

        let requestID: String
        let action: String
        let expectedSequence: UInt64
        let context: NativeMCPTransportContext
        var phase: Phase
    }

    private struct PendingLiveOrderControlOperation {
        enum Stage {
            case mutation
            case requestReceipt
            case statusReadback
        }

        let requestID: String
        let action: String
        let expectedEpoch: String
        let expectedSequence: UInt64
        let context: NativeMCPTransportContext
        var stage: Stage
        var receipt: LiveOrderControlReceipt?
        var receiptLookupWasUnknown = false
    }

    var configurationDidSave: ((AppConfiguration) -> Void)?
    var onOpenDashboard: (() -> Void)?
    var onOpenAutomationWorkspace: (() -> Void)?
    var onTradingStateChanged: ((LiveTradingState) -> Void)?
    var onControlSnapshot: ((TradingControlSnapshot) -> Void)?

    init(
        store: ConfigurationStore,
        tradingService: SettingsTradingService = UpbitLiveService.shared,
        now: @escaping () -> Date = Date.init
    ) {
        self.store = store
        self.tradingService = tradingService
        self.now = now
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 680, height: 820),
            styleMask: [.titled, .closable, .resizable], backing: .buffered, defer: false
        )
        window.title = "Sky Backcraft — 설정 및 실거래 제어"
        window.backgroundColor = TerminalTheme.surface
        window.titlebarAppearsTransparent = true
        window.isReleasedWhenClosed = false
        super.init(window: window)
        window.delegate = self
        buildUI()
        populate(.default)
        updateTradingState(.standby)
        strategyAdmissionExitObserver = NotificationCenter.default.addObserver(
            forName: .nativeMCPOwnedHelperExited, object: nil, queue: .main
        ) { [weak self] notification in
            guard let self,
                  let context = notification.userInfo?["context"] as? NativeMCPTransportContext,
                  context.owned_helper != nil,
                  self.pendingStrategyAdmission?.context == context else { return }
            self.confirmStrategyAdmissionHelperExit()
        }
    }

    deinit {
        if let strategyAdmissionExitObserver { NotificationCenter.default.removeObserver(strategyAdmissionExitObserver) }
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) { nil }

    func show(configurationError suppliedError: Error? = nil) {
        var configurationError = suppliedError
        do {
            if configurationError == nil { populate(try store.load()) }
            else { populate(.default) }
        } catch {
            configurationError = error
            populate(.default)
        }
        showWindow(nil)
        startFreshnessTimer()
        if !CommandLine.arguments.contains("--gui-review") || CommandLine.arguments.contains("--gui-review-read-only") {
            refreshLiveBalances()
            refreshControlStatus()
            refreshLiveOrderControlStatus()
        }
        window?.center()
        NSApp.activate(ignoringOtherApps: true)
        if let configurationError, let window {
            NSAlert(error: configurationError).beginSheetModal(for: window)
        }
    }

    func windowWillClose(_ notification: Notification) {
        cancelVisibleReads()
    }

    func windowDidMiniaturize(_ notification: Notification) {
        cancelVisibleReads()
    }

    func windowDidDeminiaturize(_ notification: Notification) {
        startFreshnessTimer()
        refreshLiveOrderControlStatus()
    }

    private func startFreshnessTimer() {
        freshnessTimer?.invalidate()
        freshnessTimer = Timer.scheduledTimer(withTimeInterval: 1, repeats: true) { [weak self] _ in
            self?.expireControlObservation()
            self?.expireLiveOrderControlObservation()
        }
    }

    func expireControlObservation() {
        guard let snapshot = currentControlSnapshot,
              !Self.isFresh(snapshot.observed_at_utc, now: now()) else { return }
        applyControlUnavailable("실주문: 마지막 관측값 STALE · 최신 허용 근거 없음 · 서버 상태를 다시 조회하세요")
    }

    private func cancelVisibleReads() {
        readGeneration &+= 1
        balanceReadRequest?.cancel(); balanceReadRequest = nil
        controlReadRequest?.cancel(); controlReadRequest = nil
        liveOrderControlReadRequest?.cancel(); liveOrderControlReadRequest = nil
        if liveOrderControlMutationRequest == nil {
            liveOrderControlRequestInFlight = false
        }
        if balanceReadPending {
            liveApiBadge.stringValue = "읽기 조회 취소 · 이전 값 최신성 확인 불가"
        }
        balanceReadPending = false
        refreshButton.isEnabled = !CommandLine.arguments.contains("--gui-review") || CommandLine.arguments.contains("--gui-review-read-only")
        if controlReadPending { setControlRequestInFlight(false) }
        controlReadPending = false
        applyControlUnavailable("실주문: 화면을 닫거나 최소화함 · 최신 허용 근거 없음")
        currentLiveOrderControlSnapshot = nil
        currentLiveOrderControlContext = nil
        renderLiveOrderControlUnavailable("화면을 닫거나 최소화함 · 최신 허용 근거 없음")
        freshnessTimer?.invalidate(); freshnessTimer = nil
    }

    func updateStatus(_ status: RuntimeStatus) {
        statusLabel.stringValue = status.menuTitle
        switch status {
        case .running: statusLabel.textColor = .systemGreen
        case .failed, .blocked: statusLabel.textColor = .systemRed
        default: statusLabel.textColor = .secondaryLabelColor
        }
        if case .running = status {
            if window?.isVisible == true,
               !CommandLine.arguments.contains("--gui-review") {
                refreshControlStatus()
                refreshLiveOrderControlStatus()
            }
        } else {
            currentControlSnapshot = nil
            applyControlUnavailable("Backend 상태가 바뀌었습니다. 최신 상태를 조회하세요. 미확정 허가 요청은 확인된 helper 종료 전까지 재전송하지 않습니다.")
        }
    }

    private func confirmStrategyAdmissionHelperExit() {
        let pending = pendingStrategyAdmission
        if pending != nil {
            readGeneration &+= 1
            controlReadRequest?.cancel()
            controlReadRequest = nil
            controlReadPending = false
            setControlRequestInFlight(false)
            pendingStrategyAdmission = nil
            strategyAdmissionStatusMessage = "UNKNOWN fence released after the MCP helper exited. The request was not resent; a fresh LIVE status is required after restart."
        } else if !(strategyAdmissionStatusMessage?.hasPrefix("UNKNOWN fence released after the MCP helper exited") ?? false) {
            strategyAdmissionStatusMessage = nil
        }
        currentControlSnapshot = nil
        applyControlUnavailable(pending.map {
            "MCP helper가 종료되어 계좌 전략 상태를 다시 확인해야 합니다 · 이전 \($0.action.uppercased())은 자동 재전송하지 않습니다."
        } ?? "MCP helper가 실행 중이 아니어서 계좌 전략 상태를 확인할 수 없습니다.")
    }

    func updateTradingState(_ state: LiveTradingState) {
        applyControlAvailability(.backendControlUnavailable)
        updateStrategyAdmissionControls()
        emergencyButton.toolTip = currentControlSnapshot?.live_trading_enabled == false
            ? "전략/전역 거래 제어가 OFF이므로 긴급 차단 명령을 사용할 수 없습니다. 서버 상태와 이유만 조회할 수 있습니다."
            : "최근 서버 상태를 조회해야 신규 주문 긴급 차단을 요청할 수 있습니다."

        switch state {
        case .standby:
            tradingStateLabel.stringValue = "전략/전역 거래 제어: 상태 조회 전"
            tradingStateLabel.textColor = .secondaryLabelColor
        case .armed:
            tradingStateLabel.stringValue = "전략/전역 거래 제어: 확인되지 않은 실행 상태 · 적용하지 않음"
            tradingStateLabel.textColor = .systemOrange
        case .paused:
            tradingStateLabel.stringValue = "전략/전역 거래 제어: 중지 상태 서버 응답 없음"
            tradingStateLabel.textColor = .systemOrange
        case .emergencyStopped:
            tradingStateLabel.stringValue = "전략/전역 거래 제어: 긴급 정지 서버 확인 없음"
            tradingStateLabel.textColor = .systemRed
        }
    }

    @discardableResult
    func updateControlSnapshot(
        _ snapshot: TradingControlSnapshot,
        authenticated: Bool
    ) -> Bool {
        guard authenticated,
              Self.isFresh(snapshot.observed_at_utc, now: now()) else {
            currentControlSnapshot = nil
            applyControlUnavailable("실주문: 상태가 오래되었거나 인증되지 않음 · 제어 불가")
            return false
        }

        if pendingStrategyAdmission == nil {
            strategyAdmissionStatusMessage = nil
        }
        currentControlSnapshot = snapshot
        let reviewMode = CommandLine.arguments.contains("--gui-review")
        let emergencyEnabled = snapshot.live_trading_enabled
            && !reviewMode
            && !controlRequestInFlight
            && !snapshot.emergency_latched
        let availability = TradingControlAvailability(
            emergency: emergencyEnabled
        )
        applyControlAvailability(availability)
        if reviewMode {
            emergencyButton.toolTip = "GUI 검토 세션에서는 거래 제어 명령을 실행하지 않습니다."
        } else if !snapshot.live_trading_enabled {
            emergencyButton.toolTip = "전략/전역 거래 제어가 OFF이므로 긴급 차단 명령을 사용할 수 없습니다. 서버 상태와 이유만 조회할 수 있습니다."
        } else if emergencyEnabled {
            emergencyButton.toolTip = "서버에 신규 주문 차단을 요청합니다. 기존 주문 취소나 자산 청산은 수행하지 않습니다."
        } else if snapshot.emergency_latched {
            emergencyButton.toolTip = "신규 주문 긴급 차단이 이미 서버에서 확인되었습니다."
        } else {
            emergencyButton.toolTip = "최근 서버 상태가 없거나 제어 요청이 진행 중이므로 사용할 수 없습니다."
        }

        let observed = snapshot.observed_at_utc
        if !snapshot.live_trading_enabled {
            let emergency = snapshot.emergency_latched ? " · 긴급 정지 적용" : ""
            tradingStateLabel.stringValue = "전략/전역 거래 제어: OFF · 서버 상태 \(snapshot.state) · 마지막 확인 \(observed)\(emergency)"
            tradingStateLabel.textColor = snapshot.emergency_latched ? .systemRed : .secondaryLabelColor
        } else if snapshot.emergency_latched {
            tradingStateLabel.stringValue = "전략/전역 거래 제어: 긴급 정지 적용 · 서버 상태 \(snapshot.state) · 마지막 확인 \(observed)"
            tradingStateLabel.textColor = .systemRed
        } else {
            let admission = snapshot.order_admission_blocked ? "신규 전략 주문 차단" : "사용 가능"
            tradingStateLabel.stringValue = "전략/전역 거래 제어: \(admission) · 서버 상태 \(snapshot.state) · 마지막 확인 \(observed)"
            tradingStateLabel.textColor = .systemOrange
        }
        updateStrategyAdmissionControls()
        onControlSnapshot?(snapshot)
        return true
    }

    private func availableStrategyAdmissionAction() -> String? {
        guard let snapshot = currentControlSnapshot,
              persistedConfiguration.authenticationMode == .oauth,
              persistedConfiguration.tradingMcpEnabled,
              snapshot.exchange_read_mode == "LIVE_READ",
              snapshot.live_trading_enabled,
              !snapshot.emergency_latched,
              Self.isFresh(snapshot.observed_at_utc, now: now()),
              !CommandLine.arguments.contains("--gui-review"),
              !controlRequestInFlight,
              pendingStrategyAdmission == nil else { return nil }
        switch snapshot.state {
        case "STANDBY": return "start"
        case "PAUSED": return "resume"
        default: return nil
        }
    }

    private func updateStrategyAdmissionControls() {
        let action = availableStrategyAdmissionAction()
        strategyAdmissionButton.isEnabled = action != nil
        if let pendingStrategyAdmission {
            switch pendingStrategyAdmission.phase {
            case .sending:
                strategyAdmissionButton.title = "계좌 전략 권한 요청 중…"
                strategyAdmissionStatusLabel.stringValue = "\(pendingStrategyAdmission.action.uppercased()) 전송 중 · request_id \(pendingStrategyAdmission.requestID) · 응답 전까지 재전송하지 않습니다."
            case .unknown:
                strategyAdmissionButton.title = "UNKNOWN · 재요청 잠김"
                strategyAdmissionStatusLabel.stringValue = "UNKNOWN · \(pendingStrategyAdmission.action.uppercased()) · request_id \(pendingStrategyAdmission.requestID) · expected_sequence \(pendingStrategyAdmission.expectedSequence) · 재전송하지 않습니다. 새 서버 상태의 순번이 이를 넘어가거나 helper 종료가 확인될 때까지 새 요청을 잠급니다."
            }
            strategyAdmissionStatusLabel.textColor = TerminalTheme.negative
            strategyAdmissionButton.toolTip = strategyAdmissionStatusLabel.stringValue
            strategyAdmissionButton.setAccessibilityLabel(strategyAdmissionButton.title)
            strategyAdmissionButton.setAccessibilityHelp(strategyAdmissionStatusLabel.stringValue)
            return
        }

        if let action {
            strategyAdmissionButton.title = action == "start"
                ? "계좌 전략 주문 허용 (Start)"
                : "계좌 전략 재개 (Resume)"
            strategyAdmissionButton.toolTip = "현재 인증된 LIVE 계좌의 새 서버 상태를 다시 검토한 뒤 계좌 전체 전략 권한을 요청합니다."
            strategyAdmissionStatusLabel.textColor = .secondaryLabelColor
        } else if let snapshot = currentControlSnapshot {
            switch snapshot.state {
            case "RUNNING": strategyAdmissionButton.title = "계좌 전략 허용됨 (RUNNING)"
            case "EMERGENCY_STOP_ACTIVE": strategyAdmissionButton.title = "긴급 정지 상태 · Start/Resume 잠김"
            default: strategyAdmissionButton.title = "계좌 전략 권한 확인 필요"
            }
            strategyAdmissionButton.toolTip = strategyAdmissionUnavailableReason(snapshot)
            strategyAdmissionStatusLabel.textColor = .secondaryLabelColor
            strategyAdmissionStatusLabel.stringValue = strategyAdmissionStatusMessage
                ?? "계좌 전체 범위 · LIVE_READ \(snapshot.exchange_read_mode == "LIVE_READ" ? "확인" : "불일치") · state \(snapshot.state) · sequence \(snapshot.sequence) · live_trading_enabled \(snapshot.live_trading_enabled ? "ON" : "OFF") · emergency_latched \(snapshot.emergency_latched ? "ON" : "OFF") · order_admission_blocked \(snapshot.order_admission_blocked ? "YES" : "NO") · observed_at_utc \(snapshot.observed_at_utc)"
        } else {
            strategyAdmissionButton.title = "계좌 전략 권한 확인 필요"
            strategyAdmissionButton.toolTip = strategyAdmissionUnavailableReason(nil)
            strategyAdmissionStatusLabel.stringValue = strategyAdmissionStatusMessage
                ?? "계좌 전체 전략 권한: 인증된 최신 LIVE 상태를 확인해야 합니다."
            strategyAdmissionStatusLabel.textColor = .systemOrange
        }
        strategyAdmissionButton.setAccessibilityLabel("\(strategyAdmissionButton.title) · \(strategyAdmissionButton.isEnabled ? "활성" : "비활성")")
        strategyAdmissionButton.setAccessibilityHelp(strategyAdmissionButton.toolTip)
    }

    private func strategyAdmissionUnavailableReason(_ snapshot: TradingControlSnapshot?) -> String {
        if CommandLine.arguments.contains("--gui-review") { return "GUI 검토 모드에서는 전략 권한 요청을 보내지 않습니다." }
        if controlRequestInFlight { return "다른 전략/긴급 제어 요청이 확인될 때까지 기다리세요." }
        if pendingStrategyAdmission != nil { return "이전 요청 결과를 확인할 때까지 새 요청은 잠겨 있습니다." }
        guard persistedConfiguration.authenticationMode == .oauth,
              persistedConfiguration.tradingMcpEnabled else {
            return "Settings에서 OAuth 인증과 MCP 계좌·전략 제어 권한이 활성화되어야 합니다."
        }
        guard let snapshot else { return "서버에서 인증된 최신 LIVE 제어 상태를 먼저 조회하세요." }
        guard Self.isFresh(snapshot.observed_at_utc, now: now()) else { return "서버 상태가 오래되었습니다. 새로고침하세요." }
        guard snapshot.exchange_read_mode == "LIVE_READ" else { return "서버가 LIVE_READ 계좌 모드를 확인하지 않았습니다." }
        guard snapshot.live_trading_enabled else { return "계좌 전체 전략 권한이 서버에서 OFF입니다." }
        guard !snapshot.emergency_latched else { return "긴급 정지가 잠겨 있어 Start/Resume을 사용할 수 없습니다." }
        switch snapshot.state {
        case "STANDBY", "PAUSED": return "서버 상태가 바뀌었거나 다른 제어 요청이 진행 중입니다. 새로고침하세요."
        default: return "현재 서버 state \(snapshot.state)는 계좌 Start/Resume 요청을 허용하지 않습니다."
        }
    }

    private func resolveUnknownStrategyAdmission(withFreshStatus snapshot: TradingControlSnapshot) {
        guard let pending = pendingStrategyAdmission, pending.phase == .unknown else { return }
        guard snapshot.exchange_read_mode == "LIVE_READ" else {
            strategyAdmissionStatusMessage = "UNKNOWN 유지 · 새 서버 응답은 LIVE_READ 계좌 상태가 아닙니다. 재전송하지 않습니다."
            updateStrategyAdmissionControls()
            return
        }
        guard snapshot.sequence > pending.expectedSequence else {
            strategyAdmissionStatusMessage = "UNKNOWN 유지 · 새 서버 순번 \(snapshot.sequence)이 expected_sequence \(pending.expectedSequence)을 넘지 않았습니다. 재전송하지 않습니다."
            updateStrategyAdmissionControls()
            return
        }
        pendingStrategyAdmission = nil
        strategyAdmissionStatusMessage = "UNKNOWN 해제 · 새 서버 순번 \(snapshot.sequence)이 expected_sequence \(pending.expectedSequence)을 넘어 오래된 요청의 CAS 적용을 막습니다. 요청은 재전송하지 않았습니다."
        updateStrategyAdmissionControls()
    }

    private func markStrategyAdmissionUnknown(requestID: String, detail: String) {
        guard var pending = pendingStrategyAdmission, pending.requestID == requestID else { return }
        pending.phase = .unknown
        pendingStrategyAdmission = pending
        setControlRequestInFlight(false)
        strategyAdmissionStatusMessage = "UNKNOWN · \(detail) · request_id \(requestID) · status 새로고침으로 순번이 expected_sequence \(pending.expectedSequence)을 넘거나 MCP helper 종료가 확인될 때까지 재요청하지 않습니다."
        tradingStateLabel.stringValue = "계좌 전략 권한 요청 결과 UNKNOWN · 서버 상태를 새로고침하세요"
        tradingStateLabel.textColor = .systemRed
        updateStrategyAdmissionControls()
    }

    private static func isFresh(_ timestamp: String, now: Date) -> Bool {
        guard let observedAt = parseTimestamp(timestamp) else { return false }
        let age = now.timeIntervalSince(observedAt)
        return age >= 0 && age <= 30
    }

    private static func parseTimestamp(_ timestamp: String) -> Date? {
        let fractional = ISO8601DateFormatter()
        fractional.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        if let parsed = fractional.date(from: timestamp) { return parsed }
        return ISO8601DateFormatter().date(from: timestamp)
    }

    private static func liveOrderControlTimestamp(_ timestamp: String) -> Date? {
        guard timestamp.hasSuffix("Z") || timestamp.hasSuffix("+00:00") else { return nil }
        return parseTimestamp(timestamp)
    }

    private static func liveOrderControlIsFresh(_ timestamp: String, now: Date) -> Bool {
        guard let observedAt = liveOrderControlTimestamp(timestamp) else { return false }
        let age = now.timeIntervalSince(observedAt)
        return age >= 0 && age <= 30
    }

    private var liveOrderControlWindowIsVisible: Bool {
        window?.isVisible == true && window?.isMiniaturized == false
    }

    private var reviewAllowsLiveOrderControlRead: Bool {
        !CommandLine.arguments.contains("--gui-review")
            || CommandLine.arguments.contains("--gui-review-read-only")
    }

    @objc func refreshLiveOrderControlStatus() {
        guard liveOrderControlWindowIsVisible else { return }
        guard reviewAllowsLiveOrderControlRead else {
            renderLiveOrderControlUnavailable("GUI 검토 모드에서 서버 조회가 꺼져 있습니다")
            return
        }
        guard persistedConfiguration.authenticationMode == .oauth,
              persistedConfiguration.tradingMcpEnabled else {
            currentLiveOrderControlSnapshot = nil
            currentLiveOrderControlContext = nil
            renderLiveOrderControlUnavailable("앱 설정에서 OAuth와 MCP 거래 권한이 활성화되지 않았습니다")
            return
        }
        guard !liveOrderControlRequestInFlight else { return }

        if let operation = pendingLiveOrderControlOperation {
            guard tradingService.isCurrentTransportContext(operation.context) else {
                pendingLiveOrderControlOperation = nil
                liveOrderControlOperationMessage = "연결이 바뀌어 요청 결과를 확인할 수 없습니다."
                currentLiveOrderControlSnapshot = nil
                currentLiveOrderControlContext = nil
                guard let context = tradingService.currentTransportContext() else {
                    renderLiveOrderControlUnavailable("Backend 연결 세대가 변경되어 현재 상태를 확인할 수 없습니다")
                    return
                }
                beginLiveOrderControlStatusRead(context: context, generation: readGeneration, operationID: nil)
                return
            }
            switch operation.stage {
            case .mutation:
                renderLiveOrderControlUnavailable("요청 전송 중입니다. 서버 응답 전까지 현재 상태는 UNKNOWN입니다.")
            case .requestReceipt:
                beginLiveOrderControlReceiptReadback(operation, generation: readGeneration)
            case .statusReadback:
                beginLiveOrderControlStatusRead(
                    context: operation.context,
                    generation: readGeneration,
                    operationID: operation.requestID
                )
            }
            return
        }

        guard let context = tradingService.currentTransportContext() else {
            renderLiveOrderControlUnavailable("Backend 연결을 확인할 수 없습니다")
            return
        }
        beginLiveOrderControlStatusRead(context: context, generation: readGeneration, operationID: nil)
    }

    private func beginLiveOrderControlStatusRead(
        context: NativeMCPTransportContext,
        generation: UInt64,
        operationID: String?
    ) {
        guard liveOrderControlWindowIsVisible,
              generation == readGeneration,
              tradingService.isCurrentTransportContext(context),
              !liveOrderControlRequestInFlight else { return }
        liveOrderControlRequestInFlight = true
        liveOrderControlOperationMessage = operationID == nil
            ? liveOrderControlOperationMessage
            : "서버에서 현재 상태를 다시 확인하고 있습니다."
        renderCurrentLiveOrderControl()
        let request = tradingService.fetchLiveOrderControlStatus(expectedContext: context) { [weak self] result in
            self?.finishLiveOrderControlStatusRead(
                result,
                context: context,
                generation: generation,
                operationID: operationID
            )
        }
        if liveOrderControlRequestInFlight, generation == readGeneration {
            liveOrderControlReadRequest = request
        }
    }

    private func finishLiveOrderControlStatusRead(
        _ result: Result<LiveOrderControlSnapshot, Error>,
        context: NativeMCPTransportContext,
        generation: UInt64,
        operationID: String?
    ) {
        guard generation == readGeneration else { return }
        liveOrderControlReadRequest = nil
        liveOrderControlRequestInFlight = false
        guard liveOrderControlWindowIsVisible else { return }
        if operationID == nil {
            guard tradingService.isCurrentTransportContext(context) else {
                renderLiveOrderControlUnavailable("서버 상태를 확인할 수 없습니다.")
                refreshLiveOrderControlStatus()
                return
            }
            switch result {
            case .success(let snapshot):
                let accepted = updateLiveOrderControlSnapshot(
                    snapshot,
                    context: context,
                    receivedAt: now(),
                    authenticated: true
                )
                if !accepted && !tradingService.isCurrentTransportContext(context) {
                    refreshLiveOrderControlStatus()
                }
            case .failure:
                renderLiveOrderControlUnavailable("서버 상태 조회에 실패했습니다. UNKNOWN은 OFF 확인이 아닙니다.")
            }
            return
        }
        guard let operationID,
              var operation = pendingLiveOrderControlOperation,
              operation.requestID == operationID else { return }

        guard tradingService.isCurrentTransportContext(context), context == operation.context else {
            liveOrderControlOperationMessage = "연결이 바뀌어 요청 결과를 확인할 수 없습니다."
            currentLiveOrderControlSnapshot = nil
            currentLiveOrderControlContext = nil
            pendingLiveOrderControlOperation = nil
            renderLiveOrderControlUnavailable("Backend 연결 세대 변경 · 이전 작업 결과 UNKNOWN")
            refreshLiveOrderControlStatus()
            return
        }

        switch result {
        case .success(let snapshot):
            let accepted = updateLiveOrderControlSnapshot(
                snapshot,
                context: context,
                receivedAt: now(),
                authenticated: true
            )
            guard accepted else {
                if !tradingService.isCurrentTransportContext(context) {
                    pendingLiveOrderControlOperation = nil
                    liveOrderControlOperationMessage = "연결이 바뀌어 요청 결과를 확인할 수 없습니다."
                    refreshLiveOrderControlStatus()
                    return
                }
                operation.stage = .statusReadback
                pendingLiveOrderControlOperation = operation
                return
            }
            liveOrderControlOperationMessage = liveOrderControlResultMessage(operation, current: snapshot)
            pendingLiveOrderControlOperation = nil
            renderCurrentLiveOrderControl()
        case .failure:
            operation.stage = .statusReadback
            pendingLiveOrderControlOperation = operation
            liveOrderControlOperationMessage = "서버 상태를 확인할 수 없습니다."
            renderLiveOrderControlUnavailable("현재 서버 상태 readback 실패 · 재조회가 필요합니다")
        }
    }

    @discardableResult
    func updateLiveOrderControlSnapshot(
        _ snapshot: LiveOrderControlSnapshot,
        context: NativeMCPTransportContext,
        receivedAt: Date,
        authenticated: Bool
    ) -> Bool {
        guard authenticated,
              !snapshot.boot_epoch.isEmpty,
              snapshot.boot_epoch.count <= 128,
              Self.liveOrderControlTimestamp(snapshot.observed_at_utc) != nil,
              tradingService.isCurrentTransportContext(context) else {
            currentLiveOrderControlSnapshot = nil
            currentLiveOrderControlContext = nil
            renderLiveOrderControlUnavailable("응답 인증·epoch·시간 또는 Backend 연결을 검증하지 못했습니다")
            return false
        }
        let newBackendEpoch = lastKnownLiveOrderControl.map {
            $0.snapshot.boot_epoch != snapshot.boot_epoch
        } ?? false
        if newBackendEpoch,
           pendingLiveOrderControlOperation == nil,
           liveOrderControlOperationMessage == "서버에서 주문 전송 설정 변경을 확인했습니다." {
            liveOrderControlOperationMessage = nil
        }
        lastKnownLiveOrderControl = LiveOrderControlObservation(
            snapshot: snapshot,
            receivedAt: receivedAt,
            context: context
        )
        guard Self.liveOrderControlIsFresh(snapshot.observed_at_utc, now: receivedAt) else {
            currentLiveOrderControlSnapshot = nil
            currentLiveOrderControlContext = nil
            renderLiveOrderControlUnavailable("마지막 서버 관측값이 STALE입니다")
            return false
        }
        guard ["LIVE_READ", "MOCK"].contains(snapshot.exchange_read_mode) else {
            currentLiveOrderControlSnapshot = nil
            currentLiveOrderControlContext = nil
            renderLiveOrderControlUnavailable("거래소 읽기 모드를 확인할 수 없습니다")
            return false
        }

        currentLiveOrderControlSnapshot = snapshot
        currentLiveOrderControlContext = context
        renderCurrentLiveOrderControl(receivedAt: receivedAt)
        return true
    }

    private func beginLiveOrderControlReceiptReadback(
        _ operation: PendingLiveOrderControlOperation,
        generation: UInt64
    ) {
        guard liveOrderControlWindowIsVisible,
              generation == readGeneration,
              tradingService.isCurrentTransportContext(operation.context),
              !liveOrderControlRequestInFlight else { return }
        liveOrderControlRequestInFlight = true
        liveOrderControlOperationMessage = "요청 결과를 확인하고 있습니다."
        renderCurrentLiveOrderControl()
        let request = tradingService.readLiveOrderControlRequest(
            requestID: operation.requestID,
            expectedContext: operation.context
        ) { [weak self] result in
            self?.finishLiveOrderControlReceiptReadback(
                result,
                operationID: operation.requestID,
                generation: generation
            )
        }
        if liveOrderControlRequestInFlight, generation == readGeneration {
            liveOrderControlReadRequest = request
        }
    }

    private func finishLiveOrderControlReceiptReadback(
        _ result: Result<LiveOrderControlSnapshot, Error>,
        operationID: String,
        generation: UInt64
    ) {
        guard generation == readGeneration else { return }
        liveOrderControlReadRequest = nil
        liveOrderControlRequestInFlight = false
        guard var operation = pendingLiveOrderControlOperation,
              operation.requestID == operationID else { return }
        guard generation == readGeneration, liveOrderControlWindowIsVisible else {
            operation.stage = .requestReceipt
            pendingLiveOrderControlOperation = operation
            return
        }
        guard tradingService.isCurrentTransportContext(operation.context) else {
            liveOrderControlOperationMessage = "연결이 바뀌어 요청 결과를 확인할 수 없습니다."
            pendingLiveOrderControlOperation = nil
            renderLiveOrderControlUnavailable("Backend 연결 세대 변경 · 현재 상태 재조회가 필요합니다")
            refreshLiveOrderControlStatus()
            return
        }

        switch result {
        case .success(let response):
            if let receipt = response.receipt,
               Self.validLiveOrderControlReceipt(receipt, operation: operation, requireFresh: false) {
                operation.receipt = receipt
            } else {
                operation.receipt = nil
                operation.receiptLookupWasUnknown = true
                liveOrderControlOperationMessage = "요청 결과를 확인할 수 없습니다."
            }
        case .failure(let error):
            operation.receipt = nil
            if let requestError = error as? LiveOrderControlRequestError,
               case .unknownRequest = requestError {
                operation.receiptLookupWasUnknown = true
            } else {
                operation.receiptLookupWasUnknown = false
            }
            if operation.receiptLookupWasUnknown {
                liveOrderControlOperationMessage = "요청 결과를 확인할 수 없습니다."
            } else {
                liveOrderControlOperationMessage = "요청 결과를 확인할 수 없습니다."
            }
        }
        operation.stage = .statusReadback
        pendingLiveOrderControlOperation = operation
        beginLiveOrderControlStatusRead(
            context: operation.context,
            generation: generation,
            operationID: operation.requestID
        )
    }

    private static func validLiveOrderControlReceipt(
        _ receipt: LiveOrderControlReceipt,
        operation: PendingLiveOrderControlOperation,
        requireFresh: Bool,
        now: Date = Date()
    ) -> Bool {
        guard receipt.request_id == operation.requestID,
              receipt.action == operation.action,
              ["ACKNOWLEDGED", "DENIED"].contains(receipt.outcome),
              !receipt.boot_epoch.isEmpty,
              Self.liveOrderControlTimestamp(receipt.observed_at_utc) != nil else { return false }
        if requireFresh && !Self.liveOrderControlIsFresh(receipt.observed_at_utc, now: now) { return false }
        return true
    }

    private func liveOrderControlResultMessage(
        _ operation: PendingLiveOrderControlOperation,
        current snapshot: LiveOrderControlSnapshot
    ) -> String {
        guard let receipt = operation.receipt else {
            return operation.receiptLookupWasUnknown
                ? "요청 결과를 확인할 수 없습니다. 현재 상태를 다시 확인했습니다."
                : "요청 결과가 확인되지 않았습니다. 현재 상태를 다시 확인했습니다."
        }
        guard receipt.outcome == "ACKNOWLEDGED" else {
            return "서버에서 주문 전송 설정 변경을 거부했습니다."
        }
        let requestedEnabled = operation.action == "enable"
        guard snapshot.live_order_control_available,
              receipt.boot_epoch == operation.expectedEpoch,
              receipt.sequence > operation.expectedSequence,
              receipt.boot_epoch == snapshot.boot_epoch,
              snapshot.sequence >= receipt.sequence,
              snapshot.live_trading_enabled == requestedEnabled else {
            return "요청 응답과 현재 상태가 다릅니다. 아래 서버 상태를 확인하세요."
        }
        return "서버에서 주문 전송 설정 변경을 확인했습니다."
    }

    private func renderCurrentLiveOrderControl(receivedAt: Date? = nil) {
        guard let snapshot = currentLiveOrderControlSnapshot,
              let context = currentLiveOrderControlContext,
              let observation = lastKnownLiveOrderControl else {
            renderLiveOrderControlUnavailable("최신 허용 서버 관측이 없습니다")
            return
        }
        guard tradingService.isCurrentTransportContext(context),
              context == observation.context,
              Self.liveOrderControlIsFresh(snapshot.observed_at_utc, now: receivedAt ?? now()) else {
            currentLiveOrderControlSnapshot = nil
            currentLiveOrderControlContext = nil
            renderLiveOrderControlUnavailable("서버 관측이 STALE이거나 Backend 연결 세대가 바뀌었습니다")
            return
        }

        let gateValue = snapshot.live_trading_enabled ? "ON" : "OFF"
        let mockMode = snapshot.exchange_read_mode == "MOCK"
        let source = mockMode ? "모의 서버 · 실거래소 호출 없음" : "실계좌 조회 연결"
        if liveOrderControlRequestInFlight {
            liveOrderControlStateLabel.stringValue = "확인 불가 · OFF로 판단하지 않습니다 · 마지막 확인 당시 주문 전송: \(gateValue)"
            liveOrderControlStateLabel.textColor = .systemOrange
        } else if snapshot.live_order_control_available {
            liveOrderControlStateLabel.stringValue = mockMode
                ? "\(source) · 모의 주문 전송: \(gateValue)"
                : "실제 주문 전송: \(gateValue) · \(source)"
            liveOrderControlStateLabel.textColor = snapshot.live_trading_enabled ? .systemOrange : .secondaryLabelColor
        } else {
            liveOrderControlStateLabel.stringValue = "전송 설정 사용 불가 · 현재 전송 상태 확인 필요"
            liveOrderControlStateLabel.textColor = .systemOrange
        }

        let receivedText = Self.utcTimestamp(receivedAt ?? observation.receivedAt)
        liveOrderControlDetailsLabel.stringValue =
            "\(source) · 추가 주문 차단: \(snapshot.order_admission_blocked ? "적용" : "없음") · 긴급 정지: \(snapshot.emergency_latched ? "적용" : "없음") · 마지막 서버 확인: \(snapshot.observed_at_utc) · 화면 수신: \(receivedText)"

        let reviewMode = CommandLine.arguments.contains("--gui-review")
        let mockReviewControl = reviewMode
            && CommandLine.arguments.contains("--gui-review-mock-order-control")
            && snapshot.exchange_read_mode == "MOCK"
        let reviewAllowsMutation = !reviewMode || mockReviewControl
        let configured = persistedConfiguration.authenticationMode == .oauth
            && persistedConfiguration.tradingMcpEnabled
        let canMutate = configured
            && snapshot.live_order_control_available
            && ["LIVE_READ", "MOCK"].contains(snapshot.exchange_read_mode)
            && !controlRequestInFlight
            && !liveOrderControlRequestInFlight
            && Self.liveOrderControlIsFresh(snapshot.observed_at_utc, now: now())
            && tradingService.isCurrentTransportContext(context)
            && reviewAllowsMutation
        enableLiveOrderControlButton.isEnabled = canMutate
            && !snapshot.live_trading_enabled
            && snapshot.confirmation_required
        disableLiveOrderControlButton.isEnabled = canMutate && snapshot.live_trading_enabled
        if reviewMode && !mockReviewControl {
            enableLiveOrderControlButton.toolTip = "검토 화면에서는 모의 연결에서만 전송 설정을 바꿀 수 있습니다."
            disableLiveOrderControlButton.toolTip = enableLiveOrderControlButton.toolTip
        } else if !snapshot.live_order_control_available {
            enableLiveOrderControlButton.toolTip = "현재 전송 설정을 사용할 수 없습니다."
            disableLiveOrderControlButton.toolTip = enableLiveOrderControlButton.toolTip
        } else if !snapshot.confirmation_required {
            enableLiveOrderControlButton.toolTip = "현재 서버 상태에서 전송 설정을 변경할 수 없습니다."
            disableLiveOrderControlButton.toolTip = "현재 주문 전송은 OFF입니다."
        } else {
            enableLiveOrderControlButton.toolTip = "확인 문구를 정확히 입력한 뒤 주문 전송을 허용합니다."
            disableLiveOrderControlButton.toolTip = "새 전송과 미전송 대기 요청을 막습니다. 이미 전송된 주문이나 보유 자산은 유지됩니다."
        }
        enableLiveOrderControlButton.setAccessibilityLabel("주문 전송 허용, \(enableLiveOrderControlButton.isEnabled ? "활성" : "비활성")")
        disableLiveOrderControlButton.setAccessibilityLabel("주문 전송 차단, \(disableLiveOrderControlButton.isEnabled ? "활성" : "비활성")")
        liveOrderControlRefreshButton.isEnabled = !liveOrderControlRequestInFlight
            && reviewAllowsLiveOrderControlRead
            && persistedConfiguration.authenticationMode == .oauth
            && persistedConfiguration.tradingMcpEnabled
            && liveOrderControlWindowIsVisible
        liveOrderControlRefreshButton.setAccessibilityValue(
            mockMode ? "모의 주문 전송: \(gateValue)" : "실제 주문 전송: \(gateValue)"
        )
        if !liveOrderControlRequestInFlight, let message = liveOrderControlOperationMessage {
            liveOrderControlStateLabel.stringValue += " · 최근 요청: \(message)"
        }
        NSAccessibility.post(element: liveOrderControlStateLabel, notification: .valueChanged)
        NSAccessibility.post(element: liveOrderControlDetailsLabel, notification: .valueChanged)
    }

    private func renderLiveOrderControlUnavailable(_ reason: String) {
        enableLiveOrderControlButton.isEnabled = false
        disableLiveOrderControlButton.isEnabled = false
        enableLiveOrderControlButton.setAccessibilityLabel("주문 전송 허용, 비활성")
        disableLiveOrderControlButton.setAccessibilityLabel("주문 전송 차단, 비활성")
        liveOrderControlRefreshButton.isEnabled = !liveOrderControlRequestInFlight
            && reviewAllowsLiveOrderControlRead
            && persistedConfiguration.authenticationMode == .oauth
            && persistedConfiguration.tradingMcpEnabled
            && liveOrderControlWindowIsVisible
        if let observation = lastKnownLiveOrderControl {
            let snapshot = observation.snapshot
            let gate = snapshot.live_trading_enabled ? "ON" : "OFF"
            let source = snapshot.exchange_read_mode == "MOCK"
                ? "모의 서버 · 실거래소 호출 없음"
                : "실계좌 조회 연결"
            liveOrderControlStateLabel.stringValue = "확인 불가 · OFF로 판단하지 않습니다 · 마지막 확인 당시 주문 전송: \(gate)"
            liveOrderControlStateLabel.textColor = .systemOrange
            liveOrderControlDetailsLabel.stringValue =
                "마지막 확인 당시 \(source) · 추가 주문 차단: \(snapshot.order_admission_blocked ? "적용" : "없음") · 긴급 정지: \(snapshot.emergency_latched ? "적용" : "없음") · 마지막 서버 확인: \(snapshot.observed_at_utc) · 화면 수신: \(Self.utcTimestamp(observation.receivedAt))"
        } else {
            liveOrderControlStateLabel.stringValue = "확인 불가 · OFF로 판단하지 않습니다"
            liveOrderControlStateLabel.textColor = .systemOrange
            liveOrderControlDetailsLabel.stringValue = "신규 주문 차단 상태: 확인 불가 · 긴급 정지 상태: 확인 불가 · 마지막 서버 확인 없음"
        }
        NSAccessibility.post(element: liveOrderControlStateLabel, notification: .valueChanged)
        NSAccessibility.post(element: liveOrderControlDetailsLabel, notification: .valueChanged)
    }

    private func expireLiveOrderControlObservation() {
        guard let snapshot = currentLiveOrderControlSnapshot,
              !Self.liveOrderControlIsFresh(snapshot.observed_at_utc, now: now()) else { return }
        currentLiveOrderControlSnapshot = nil
        currentLiveOrderControlContext = nil
        renderLiveOrderControlUnavailable("마지막 서버 관측값이 STALE입니다")
    }

    private static func utcTimestamp(_ date: Date) -> String {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter.string(from: date)
    }

    @objc private func enableLiveOrderControlPressed() {
        guard let snapshot = currentLiveOrderControlSnapshot,
              let context = currentLiveOrderControlContext,
              canChangeLiveOrderControl(to: true, snapshot: snapshot, context: context) else {
            liveOrderControlOperationMessage = "서버 상태를 확인할 수 없어 요청하지 않았습니다."
            refreshLiveOrderControlStatus()
            return
        }

        let (alert, confirmationField) = Self.makeLiveOrderEnableAlert(
            isMock: snapshot.exchange_read_mode == "MOCK"
        )
        guard alert.runModal() == .alertFirstButtonReturn else { return }
        guard confirmationField.stringValue == UpbitLiveService.liveOrderEnableConfirmation else {
            liveOrderControlOperationMessage = "확인 문구가 일치하지 않아 요청하지 않았습니다."
            renderCurrentLiveOrderControl()
            return
        }
        guard canChangeLiveOrderControl(to: true, snapshot: snapshot, context: context) else {
            liveOrderControlOperationMessage = "확인하는 동안 서버 상태가 바뀌어 요청하지 않았습니다. 다시 확인하세요."
            refreshLiveOrderControlStatus()
            return
        }
        beginLiveOrderControlMutation(
            action: "enable",
            confirmation: confirmationField.stringValue,
            snapshot: snapshot,
            context: context
        )
    }

    static func makeLiveOrderEnableAlert(isMock: Bool) -> (alert: NSAlert, confirmationField: NSTextField) {
        let alert = NSAlert()
        alert.alertStyle = .critical
        alert.messageText = "주문 전송을 허용하시겠습니까?"
        let modeNotice = isMock ? "모의 실행이며 실거래소를 호출하지 않습니다.\n\n" : ""
        alert.informativeText = "\(modeNotice)실계좌에서는 실행 중인 전략이 실제 주문을 보낼 수 있습니다. 거래 OAuth 승인은 계좌 조회·전략 제어와 주문 전송을 함께 허용합니다. 서버 재시작 후 주문 전송은 OFF로 시작하며, 다음 새 거래 OAuth 승인 전까지 OFF를 유지합니다. OFF 후에는 이 확인 문구로 수동 재허용할 수 있습니다. 이 동작은 전략을 시작·재개하거나 긴급 정지를 해제하지 않습니다.\n\n진행하려면 아래 문구를 정확히 입력하세요: \(UpbitLiveService.liveOrderEnableConfirmation)"
        alert.addButton(withTitle: "주문 전송 허용 요청")
        alert.addButton(withTitle: "취소")

        let confirmationField = NSTextField(string: "")
        confirmationField.placeholderString = UpbitLiveService.liveOrderEnableConfirmation
        confirmationField.setAccessibilityLabel("주문 전송 허용 확인 문구")
        confirmationField.font = .monospacedSystemFont(ofSize: 13, weight: .regular)
        let prompt = NSTextField(labelWithString: "확인 문구")
        prompt.font = .systemFont(ofSize: 11, weight: .semibold)
        let phrase = NSTextField(labelWithString: UpbitLiveService.liveOrderEnableConfirmation)
        phrase.font = .monospacedSystemFont(ofSize: 13, weight: .semibold)
        let accessory = NSStackView(views: [prompt, phrase, confirmationField])
        accessory.orientation = .vertical
        accessory.alignment = .leading
        accessory.distribution = .fill
        accessory.spacing = 6
        let accessoryWidth: CGFloat = 440
        accessory.frame = NSRect(x: 0, y: 0, width: accessoryWidth, height: 78)
        confirmationField.widthAnchor.constraint(equalToConstant: accessoryWidth).isActive = true
        confirmationField.heightAnchor.constraint(equalToConstant: 26).isActive = true
        alert.accessoryView = accessory
        alert.window.initialFirstResponder = confirmationField
        return (alert, confirmationField)
    }

    @objc private func disableLiveOrderControlPressed() {
        guard let snapshot = currentLiveOrderControlSnapshot,
              let context = currentLiveOrderControlContext,
              canChangeLiveOrderControl(to: false, snapshot: snapshot, context: context) else {
            liveOrderControlOperationMessage = "서버 상태를 확인할 수 없어 차단 요청을 보내지 않았습니다."
            refreshLiveOrderControlStatus()
            return
        }
        beginLiveOrderControlMutation(action: "disable", confirmation: nil, snapshot: snapshot, context: context)
    }

    private func canChangeLiveOrderControl(
        to enabled: Bool,
        snapshot: LiveOrderControlSnapshot,
        context: NativeMCPTransportContext
    ) -> Bool {
        let reviewMode = CommandLine.arguments.contains("--gui-review")
        let mockReviewControl = reviewMode
            && CommandLine.arguments.contains("--gui-review-mock-order-control")
            && snapshot.exchange_read_mode == "MOCK"
        guard liveOrderControlWindowIsVisible,
              persistedConfiguration.authenticationMode == .oauth,
              persistedConfiguration.tradingMcpEnabled,
              snapshot.live_order_control_available,
              ["LIVE_READ", "MOCK"].contains(snapshot.exchange_read_mode),
              Self.liveOrderControlIsFresh(snapshot.observed_at_utc, now: now()),
              !liveOrderControlRequestInFlight,
              !controlRequestInFlight,
              tradingService.isCurrentTransportContext(context),
              context == currentLiveOrderControlContext,
              let current = currentLiveOrderControlSnapshot,
              current.boot_epoch == snapshot.boot_epoch,
              current.sequence == snapshot.sequence,
              current.observed_at_utc == snapshot.observed_at_utc,
              (!reviewMode || mockReviewControl),
              enabled ? !snapshot.live_trading_enabled && snapshot.confirmation_required : snapshot.live_trading_enabled else {
            return false
        }
        return true
    }

    private func beginLiveOrderControlMutation(
        action: String,
        confirmation: String?,
        snapshot: LiveOrderControlSnapshot,
        context: NativeMCPTransportContext
    ) {
        let enable = action == "enable"
        guard canChangeLiveOrderControl(to: enable, snapshot: snapshot, context: context) else {
            liveOrderControlOperationMessage = "서버 상태가 바뀌어 요청하지 않았습니다. 다시 확인하세요."
            refreshLiveOrderControlStatus()
            return
        }
        let operation = PendingLiveOrderControlOperation(
            requestID: UUID().uuidString.lowercased(),
            action: action,
            expectedEpoch: snapshot.boot_epoch,
            expectedSequence: snapshot.sequence,
            context: context,
            stage: .mutation,
            receipt: nil
        )
        pendingLiveOrderControlOperation = operation
        liveOrderControlRequestInFlight = true
        liveOrderControlOperationMessage = "주문 전송 설정 요청 중입니다. 서버 확인 전에는 상태를 바꾸지 않습니다."
        renderCurrentLiveOrderControl()
        let generation = readGeneration
        let request = tradingService.changeLiveOrderControl(
            action: action,
            requestID: operation.requestID,
            expectedSequence: operation.expectedSequence,
            expectedEpoch: operation.expectedEpoch,
            confirmation: confirmation,
            expectedContext: context
        ) { [weak self] result in
            self?.finishLiveOrderControlMutation(result, operationID: operation.requestID, generation: generation)
        }
        if liveOrderControlRequestInFlight,
           pendingLiveOrderControlOperation?.requestID == operation.requestID,
           generation == readGeneration {
            liveOrderControlMutationRequest = request
        }
    }

    private func finishLiveOrderControlMutation(
        _ result: Result<LiveOrderControlSnapshot, Error>,
        operationID: String,
        generation: UInt64
    ) {
        liveOrderControlMutationRequest = nil
        liveOrderControlRequestInFlight = false
        guard var operation = pendingLiveOrderControlOperation,
              operation.requestID == operationID else { return }
        guard tradingService.isCurrentTransportContext(operation.context) else {
            pendingLiveOrderControlOperation = nil
            liveOrderControlOperationMessage = "연결이 바뀌어 요청 결과를 확인할 수 없습니다."
            currentLiveOrderControlSnapshot = nil
            currentLiveOrderControlContext = nil
            if liveOrderControlWindowIsVisible {
                renderLiveOrderControlUnavailable("Backend 연결 세대 변경 · 이전 요청 결과 UNKNOWN")
                refreshLiveOrderControlStatus()
            }
            return
        }
        guard generation == readGeneration, liveOrderControlWindowIsVisible else {
            operation.stage = .requestReceipt
            pendingLiveOrderControlOperation = operation
            liveOrderControlOperationMessage = "요청 결과를 확인하고 있습니다."
            if liveOrderControlWindowIsVisible {
                refreshLiveOrderControlStatus()
            }
            return
        }

        switch result {
        case .success(let response):
            if let receipt = response.receipt,
               Self.validLiveOrderControlReceipt(receipt, operation: operation, requireFresh: true, now: now()),
               (receipt.outcome == "DENIED"
                    || (receipt.boot_epoch == operation.expectedEpoch
                        && receipt.sequence > operation.expectedSequence)) {
                operation.receipt = receipt
                operation.stage = .statusReadback
            } else {
                operation.stage = .requestReceipt
                liveOrderControlOperationMessage = "요청 결과를 확인할 수 없어 서버에서 다시 확인합니다."
            }
        case .failure:
            operation.stage = .requestReceipt
            liveOrderControlOperationMessage = "요청 결과를 확인할 수 없어 서버에서 다시 확인합니다."
        }
        pendingLiveOrderControlOperation = operation
        switch operation.stage {
        case .mutation:
            return
        case .requestReceipt:
            beginLiveOrderControlReceiptReadback(operation, generation: generation)
        case .statusReadback:
            liveOrderControlOperationMessage = "서버에서 현재 상태를 확인하고 있습니다."
            beginLiveOrderControlStatusRead(
                context: operation.context,
                generation: generation,
                operationID: operation.requestID
            )
        }
    }

    private func applyControlAvailability(_ availability: TradingControlAvailability) {
        emergencyButton.isEnabled = availability.emergency
        emergencyButton.setAccessibilityLabel("긴급 중단, \(availability.emergency ? "활성" : "비활성")")
        updateStrategyAdmissionControls()
    }

    private func applyControlUnavailable(_ message: String) {
        currentControlSnapshot = nil
        if pendingStrategyAdmission == nil,
           !(strategyAdmissionStatusMessage?.hasPrefix("UNKNOWN fence released after the MCP helper exited") ?? false) {
            strategyAdmissionStatusMessage = nil
        }
        applyControlAvailability(.backendControlUnavailable)
        tradingStateLabel.stringValue = message.hasPrefix("전략/전역 거래 제어")
            ? message
            : "전략/전역 거래 제어: 상태 미확인 · \(message)"
        tradingStateLabel.textColor = .systemOrange
        NSAccessibility.post(element: tradingStateLabel, notification: .valueChanged)
    }

    var controlAvailabilitySnapshot: TradingControlAvailability {
        TradingControlAvailability(emergency: emergencyButton.isEnabled)
    }

    var guiReviewSideEffectsAreDisabled: Bool {
        !saveButton.isEnabled && !refreshButton.isEnabled && !controlRefreshButton.isEnabled
            && !enableLiveOrderControlButton.isEnabled && !disableLiveOrderControlButton.isEnabled
    }

    var liveOrderControlStatusTextSnapshot: String { liveOrderControlStateLabel.stringValue }
    var liveOrderControlDetailsTextSnapshot: String { liveOrderControlDetailsLabel.stringValue }
    var liveOrderControlButtonAvailabilitySnapshot: (enable: Bool, disable: Bool) {
        (enableLiveOrderControlButton.isEnabled, disableLiveOrderControlButton.isEnabled)
    }

    var controlStatusTextSnapshot: String { tradingStateLabel.stringValue }

    private func buildUI() {
        guard let content = window?.contentView else { return }

        let dashboardBtn = NSButton(
            title: "퀀트 연구 대시보드 열기… (⌘D)",
            target: self,
            action: #selector(openDashboardPressed)
        )
        dashboardBtn.bezelStyle = .rounded
        dashboardBtn.controlSize = .large
        dashboardBtn.font = .systemFont(ofSize: 13, weight: .semibold)
        dashboardBtn.keyEquivalent = "d"
        dashboardBtn.keyEquivalentModifierMask = .command

        let balanceCard = buildRealBalancesCard()
        let liveTradingCard = buildLiveTradingCard()
        let mcpCard = buildMcpSettingsCard()

        let cancel = NSButton(title: "취소", target: self, action: #selector(cancelPressed))
        cancel.bezelStyle = .rounded
        saveButton.title = "네트워크·인증 설정 저장"
        saveButton.target = self
        saveButton.action = #selector(savePressed)
        saveButton.bezelStyle = .rounded
        saveButton.keyEquivalent = "\r"
        if CommandLine.arguments.contains("--gui-review") {
            saveButton.isEnabled = false
            saveButton.toolTip = "GUI 검토 모드에서는 설정 저장과 백엔드 재시작을 수행하지 않습니다."
        }

        let footerSpacer = NSView()
        footerSpacer.setContentHuggingPriority(.defaultLow, for: .horizontal)
        let footerButtons = NSStackView(views: [statusLabel, footerSpacer, cancel, saveButton])
        footerButtons.orientation = .horizontal
        footerButtons.spacing = 10
        footerButtons.alignment = .centerY

        let rootStack = NSStackView(views: [
            dashboardBtn,
            balanceCard,
            liveTradingCard,
            mcpCard,
            footerButtons
        ])
        rootStack.orientation = .vertical
        rootStack.alignment = .leading
        rootStack.spacing = 14
        rootStack.translatesAutoresizingMaskIntoConstraints = false
        content.addSubview(rootStack)

        NSLayoutConstraint.activate([
            rootStack.leadingAnchor.constraint(equalTo: content.leadingAnchor, constant: 20),
            rootStack.trailingAnchor.constraint(equalTo: content.trailingAnchor, constant: -20),
            rootStack.topAnchor.constraint(equalTo: content.topAnchor, constant: 18),
            rootStack.bottomAnchor.constraint(lessThanOrEqualTo: content.bottomAnchor, constant: -18),
            dashboardBtn.widthAnchor.constraint(equalTo: rootStack.widthAnchor),
            balanceCard.widthAnchor.constraint(equalTo: rootStack.widthAnchor),
            liveTradingCard.widthAnchor.constraint(equalTo: rootStack.widthAnchor),
            mcpCard.widthAnchor.constraint(equalTo: rootStack.widthAnchor),
            footerButtons.widthAnchor.constraint(equalTo: rootStack.widthAnchor)
        ])
    }

    private func buildRealBalancesCard() -> NSBox {
        let box = NSBox()
        box.boxType = .custom
        box.cornerRadius = 8
        box.borderWidth = 1
        box.borderColor = .separatorColor
        box.fillColor = TerminalTheme.panel

        let titleLabel = NSTextField(labelWithString: "Upbit 계좌 읽기 상태")
        titleLabel.font = .systemFont(ofSize: 14, weight: .semibold)

        liveApiBadge.font = .systemFont(ofSize: 11, weight: .medium)
        liveApiBadge.textColor = .secondaryLabelColor

        let headerSpacer = NSView()
        headerSpacer.setContentHuggingPriority(.defaultLow, for: .horizontal)

        refreshButton.title = "새로고침"
        refreshButton.target = self
        refreshButton.action = #selector(refreshLiveBalances)
        refreshButton.bezelStyle = .inline
        refreshButton.font = .systemFont(ofSize: 11)
        refreshButton.setAccessibilityLabel("Upbit 계좌 잔고 새로고침")

        privacyButton.title = "금액 보기"
        privacyButton.target = self
        privacyButton.action = #selector(toggleBalancePrivacy)
        privacyButton.bezelStyle = .inline
        privacyButton.font = .systemFont(ofSize: 11)
        privacyButton.setAccessibilityLabel("계좌 금액 표시 전환")
        if CommandLine.arguments.contains("--gui-review") && !CommandLine.arguments.contains("--gui-review-read-only") {
            refreshButton.isEnabled = false
            refreshButton.toolTip = "GUI 검토 모드에서는 실제 계좌 API를 호출하지 않습니다."
        }

        let headerRow = NSStackView(views: [titleLabel, liveApiBadge, headerSpacer, privacyButton, refreshButton])
        headerRow.orientation = .horizontal
        headerRow.alignment = .centerY
        headerRow.spacing = 8

        krwBalanceDisplay.font = .monospacedDigitSystemFont(ofSize: 22, weight: .bold)
        krwBalanceDisplay.textColor = .labelColor

        balanceDetailLabel.font = .monospacedDigitSystemFont(ofSize: 11, weight: .regular)
        balanceDetailLabel.textColor = .secondaryLabelColor
        balanceDetailLabel.lineBreakMode = .byWordWrapping
        balanceDetailLabel.maximumNumberOfLines = 0

        cryptoHoldingsLabel.font = .systemFont(ofSize: 11, weight: .medium)
        cryptoHoldingsLabel.textColor = .secondaryLabelColor
        cryptoHoldingsLabel.lineBreakMode = .byWordWrapping
        cryptoHoldingsLabel.maximumNumberOfLines = 0

        let stack = NSStackView(views: [
            headerRow,
            krwBalanceDisplay,
            balanceDetailLabel,
            cryptoHoldingsLabel
        ])
        stack.orientation = .vertical
        stack.alignment = .leading
        stack.spacing = 6
        stack.translatesAutoresizingMaskIntoConstraints = false
        box.addSubview(stack)

        NSLayoutConstraint.activate([
            stack.leadingAnchor.constraint(equalTo: box.leadingAnchor, constant: 14),
            stack.trailingAnchor.constraint(equalTo: box.trailingAnchor, constant: -14),
            stack.topAnchor.constraint(equalTo: box.topAnchor, constant: 12),
            stack.bottomAnchor.constraint(equalTo: box.bottomAnchor, constant: -12),
            headerRow.widthAnchor.constraint(equalTo: stack.widthAnchor),
            balanceDetailLabel.widthAnchor.constraint(equalTo: stack.widthAnchor),
            cryptoHoldingsLabel.widthAnchor.constraint(equalTo: stack.widthAnchor)
        ])

        return box
    }

    private func buildLiveTradingCard() -> NSBox {
        let box = NSBox()
        box.boxType = .custom
        box.cornerRadius = 8
        box.borderWidth = 1
        box.borderColor = .separatorColor
        box.fillColor = TerminalTheme.panel

        let heading = NSTextField(labelWithString: "주문 전송 설정")
        heading.font = .systemFont(ofSize: 14, weight: .semibold)

        let safetyBadge = NSTextField(labelWithString: "계좌 전체 권한 · 세션 제어 · 주문 전송 게이트는 별도")
        safetyBadge.font = .systemFont(ofSize: 11, weight: .semibold)
        safetyBadge.textColor = .systemOrange

        let safetyDesc = NSTextField(wrappingLabelWithString: "계좌 전체 전략 Start/Resume은 계좌 권한을 바꿉니다. 이미 RUNNING인 세션은 각자 정책·위험 한도와 별도 주문 전송 게이트가 허용하면 새 주문을 낼 수 있습니다. 대시보드에서는 개별 세션을 관리하며, 두 제어 화면은 다른 권한을 바꾸지 않습니다. 긴급 정지는 신규 주문을 차단하고 기존 주문이나 자산을 청산하지 않습니다.")
        safetyDesc.font = .systemFont(ofSize: 11)
        safetyDesc.textColor = .secondaryLabelColor

        let strategyAdmissionDescription = NSTextField(wrappingLabelWithString: "아래 Start/Resume은 현재 LIVE 계좌 전체의 전략 주문 권한만 요청합니다. 특정 전략 세션을 만들거나 재개하지 않고, 주문 전송 게이트도 켜지 않습니다.")
        strategyAdmissionDescription.font = .systemFont(ofSize: 11)
        strategyAdmissionDescription.textColor = .secondaryLabelColor

        tradingStateLabel.font = .systemFont(ofSize: 12, weight: .semibold)
        tradingStateLabel.textColor = .secondaryLabelColor
        strategyAdmissionStatusLabel.font = .monospacedSystemFont(ofSize: 10, weight: .regular)
        strategyAdmissionStatusLabel.textColor = .secondaryLabelColor
        strategyAdmissionStatusLabel.maximumNumberOfLines = 4

        strategyAdmissionButton.title = "계좌 전략 주문 허용 (Start)"
        strategyAdmissionButton.target = self
        strategyAdmissionButton.action = #selector(strategyAdmissionPressed)
        strategyAdmissionButton.bezelStyle = .rounded
        strategyAdmissionButton.contentTintColor = .systemOrange
        strategyAdmissionButton.isEnabled = false
        strategyAdmissionButton.setAccessibilityLabel("계좌 전략 권한 상태 확인 전, 비활성")

        controlRefreshButton.title = "전략 제어 상태 새로고침"
        controlRefreshButton.target = self
        controlRefreshButton.action = #selector(refreshControlStatus)
        controlRefreshButton.bezelStyle = .inline
        controlRefreshButton.font = .systemFont(ofSize: 11)
        controlRefreshButton.setAccessibilityLabel("전략 제어 서버 상태 새로고침")
        if CommandLine.arguments.contains("--gui-review") && !CommandLine.arguments.contains("--gui-review-read-only") {
            controlRefreshButton.isEnabled = false
            controlRefreshButton.toolTip = "GUI 검토 모드에서는 서버 제어 상태를 요청하지 않습니다."
        }

        automationWorkspaceButton.title = "전략 자동매매 관리…"
        automationWorkspaceButton.target = self
        automationWorkspaceButton.action = #selector(openAutomationWorkspacePressed)
        automationWorkspaceButton.bezelStyle = .rounded
        automationWorkspaceButton.setAccessibilityLabel("대시보드에서 전략 자동화 세션별 시작, 재개, 일시정지와 중지 관리")

        emergencyButton.title = "긴급 중단"
        emergencyButton.target = self
        emergencyButton.action = #selector(emergencyStopPressed)
        emergencyButton.bezelStyle = .rounded
        emergencyButton.contentTintColor = .systemRed
        emergencyButton.isEnabled = false
        emergencyButton.setAccessibilityLabel("긴급 중단, 비활성")

        let strategyButtonsRow = NSStackView(views: [strategyAdmissionButton, emergencyButton])
        strategyButtonsRow.orientation = .horizontal
        strategyButtonsRow.spacing = 8

        let workspaceButtonRow = NSStackView(views: [automationWorkspaceButton])
        workspaceButtonRow.orientation = .horizontal
        workspaceButtonRow.spacing = 8

        let orderControlHeading = NSTextField(labelWithString: "주문 전송 상태 및 허용")
        orderControlHeading.font = .systemFont(ofSize: 12, weight: .semibold)

        let orderControlDescription = NSTextField(wrappingLabelWithString:
            "OAuth 거래 권한 승인은 계좌 조회·전략 제어와 주문 전송 권한을 함께 활성화합니다. 주문 전송 ON/OFF는 즉시 서버에 적용되어 설정 저장이 필요하지 않습니다. 네트워크·인증 설정을 변경해 저장하면 백엔드가 재시작되고 주문 전송은 OFF로 초기화되며, 다음 새 거래 OAuth 승인 전까지 OFF를 유지합니다. OFF는 새 전송과 미전송 대기 주문을 막지만 거래소에 이미 보낸 주문·보유 자산은 유지합니다. 이 제어는 전략 시작·재개나 긴급 정지 해제를 하지 않습니다.")
        orderControlDescription.font = .systemFont(ofSize: 11)
        orderControlDescription.textColor = .secondaryLabelColor

        liveOrderControlStateLabel.font = .systemFont(ofSize: 12, weight: .semibold)
        liveOrderControlStateLabel.textColor = .secondaryLabelColor
        liveOrderControlDetailsLabel.font = .systemFont(ofSize: 10)
        liveOrderControlDetailsLabel.textColor = .secondaryLabelColor

        liveOrderControlRefreshButton.title = "전송 상태 새로고침"
        liveOrderControlRefreshButton.target = self
        liveOrderControlRefreshButton.action = #selector(refreshLiveOrderControlStatus)
        liveOrderControlRefreshButton.bezelStyle = .inline
        liveOrderControlRefreshButton.font = .systemFont(ofSize: 11)
        liveOrderControlRefreshButton.setAccessibilityLabel("주문 전송 설정 상태 새로고침")
        if CommandLine.arguments.contains("--gui-review") && !CommandLine.arguments.contains("--gui-review-read-only") {
            liveOrderControlRefreshButton.isEnabled = false
            liveOrderControlRefreshButton.toolTip = "이 화면에서는 서버 상태를 조회할 수 없습니다."
        }

        enableLiveOrderControlButton.title = "전송 허용 (ON)"
        enableLiveOrderControlButton.target = self
        enableLiveOrderControlButton.action = #selector(enableLiveOrderControlPressed)
        enableLiveOrderControlButton.bezelStyle = .rounded
        enableLiveOrderControlButton.setAccessibilityLabel("주문 전송 허용, 비활성")
        enableLiveOrderControlButton.isEnabled = false

        disableLiveOrderControlButton.title = "전송 차단 (OFF)"
        disableLiveOrderControlButton.target = self
        disableLiveOrderControlButton.action = #selector(disableLiveOrderControlPressed)
        disableLiveOrderControlButton.bezelStyle = .rounded
        disableLiveOrderControlButton.setAccessibilityLabel("주문 전송 차단, 비활성")
        disableLiveOrderControlButton.isEnabled = false

        let orderControlButtonsRow = NSStackView(views: [
            liveOrderControlRefreshButton,
            enableLiveOrderControlButton,
            disableLiveOrderControlButton
        ])
        orderControlButtonsRow.orientation = .horizontal
        orderControlButtonsRow.spacing = 8

        let stack = NSStackView(views: [
            heading,
            safetyBadge,
            safetyDesc,
            strategyAdmissionDescription,
            tradingStateLabel,
            controlRefreshButton,
            strategyAdmissionStatusLabel,
            strategyButtonsRow,
            workspaceButtonRow,
            orderControlHeading,
            orderControlDescription,
            liveOrderControlStateLabel,
            liveOrderControlDetailsLabel,
            orderControlButtonsRow
        ])
        stack.orientation = .vertical
        stack.alignment = .leading
        stack.spacing = 8
        stack.translatesAutoresizingMaskIntoConstraints = false
        box.addSubview(stack)

        NSLayoutConstraint.activate([
            stack.leadingAnchor.constraint(equalTo: box.leadingAnchor, constant: 14),
            stack.trailingAnchor.constraint(equalTo: box.trailingAnchor, constant: -14),
            stack.topAnchor.constraint(equalTo: box.topAnchor, constant: 12),
            stack.bottomAnchor.constraint(equalTo: box.bottomAnchor, constant: -12),
            safetyDesc.widthAnchor.constraint(equalTo: stack.widthAnchor),
            strategyAdmissionDescription.widthAnchor.constraint(equalTo: stack.widthAnchor),
            strategyAdmissionStatusLabel.widthAnchor.constraint(equalTo: stack.widthAnchor),
            orderControlDescription.widthAnchor.constraint(equalTo: stack.widthAnchor),
            liveOrderControlStateLabel.widthAnchor.constraint(equalTo: stack.widthAnchor),
            liveOrderControlDetailsLabel.widthAnchor.constraint(equalTo: stack.widthAnchor)
        ])

        return box
    }

    private func buildMcpSettingsCard() -> NSBox {
        let box = NSBox()
        box.boxType = .custom
        box.cornerRadius = 8
        box.borderWidth = 1
        box.borderColor = .separatorColor
        box.fillColor = TerminalTheme.panel

        let heading = NSTextField(labelWithString: "MCP 네트워크 및 인증 설정")
        heading.font = .systemFont(ofSize: 13, weight: .bold)

        domainField.placeholderString = "비워 두면 Quick Tunnel을 사용합니다"
        domainField.setAccessibilityLabel("MCP 도메인 (선택)")
        portField.placeholderString = String(AppConfiguration.defaultPort)
        portField.setAccessibilityLabel("MCP 로컬 포트")
        let formatter = NumberFormatter()
        formatter.minimum = 1
        formatter.maximum = 65535
        formatter.allowsFloats = false
        portField.formatter = formatter
        authPopup.addItems(withTitles: AuthenticationMode.allCases.map(\.displayName))
        authPopup.setAccessibilityLabel("MCP 인증 모드")
        authPopup.target = self
        authPopup.action = #selector(authenticationModeChanged)
        tradingMcpToggle.setAccessibilityLabel("MCP 계좌 조회·전략 제어·주문 전송 허용")
        tradingMcpToggle.setAccessibilityHelp("OAuth 거래 권한 승인은 계좌 조회·전략 제어와 주문 전송 권한을 함께 활성화합니다.")

        let form = NSGridView(views: [
            [formLabel("도메인 (선택)"), domainField],
            [formLabel("로컬 포트"), portField],
            [formLabel("인증 모드"), authPopup]
        ])
        form.column(at: 0).xPlacement = .trailing
        form.column(at: 1).xPlacement = .fill
        form.column(at: 1).width = 320
        form.rowSpacing = 10
        form.columnSpacing = 14

        let hint = NSTextField(wrappingLabelWithString: "도메인을 입력하면 기존 Cloudflare Tunnel을 사용합니다. 비워 두면 임시 Quick Tunnel을 생성합니다.")
        hint.textColor = .secondaryLabelColor
        hint.font = .systemFont(ofSize: 11)
        let orderNotice = NSTextField(wrappingLabelWithString: "OAuth 거래 권한 승인은 계좌 조회·전략 제어와 주문 전송 권한을 함께 활성화합니다.")
        orderNotice.textColor = .secondaryLabelColor
        orderNotice.font = .systemFont(ofSize: 11)

        let stack = NSStackView(views: [heading, form, tradingMcpToggle, orderNotice, hint])
        stack.orientation = .vertical
        stack.alignment = .leading
        stack.spacing = 10
        stack.translatesAutoresizingMaskIntoConstraints = false
        box.addSubview(stack)

        NSLayoutConstraint.activate([
            stack.leadingAnchor.constraint(equalTo: box.leadingAnchor, constant: 14),
            stack.trailingAnchor.constraint(equalTo: box.trailingAnchor, constant: -14),
            stack.topAnchor.constraint(equalTo: box.topAnchor, constant: 12),
            stack.bottomAnchor.constraint(equalTo: box.bottomAnchor, constant: -12),
            orderNotice.widthAnchor.constraint(equalTo: stack.widthAnchor),
            hint.widthAnchor.constraint(equalTo: stack.widthAnchor)
        ])

        return box
    }

    private func formLabel(_ text: String) -> NSTextField {
        let field = NSTextField(labelWithString: text)
        field.textColor = .secondaryLabelColor
        field.font = .systemFont(ofSize: 12)
        return field
    }

    private func populate(_ configuration: AppConfiguration) {
        persistedConfiguration = configuration
        domainField.stringValue = configuration.domain
        portField.stringValue = String(configuration.port)
        authPopup.selectItem(withTitle: configuration.authenticationMode.displayName)
        tradingMcpToggle.state = configuration.tradingMcpEnabled ? .on : .off
        updateTradingMcpAvailability()
    }

    @objc private func authenticationModeChanged() {
        updateTradingMcpAvailability()
    }

    private func updateTradingMcpAvailability() {
        let selectedIndex = authPopup.indexOfSelectedItem
        let oauthSelected = AuthenticationMode.allCases.indices.contains(selectedIndex)
            && AuthenticationMode.allCases[selectedIndex] == .oauth
        tradingMcpToggle.isEnabled = oauthSelected
        if !oauthSelected { tradingMcpToggle.state = .off }
    }

    @objc private func refreshLiveBalances() {
        guard !balanceReadPending else { return }
        balanceReadPending = true
        let generation = readGeneration
        refreshButton.isEnabled = false
        liveApiBadge.stringValue = "읽기 상태: 조회 중"
        liveApiBadge.textColor = .secondaryLabelColor

        let request = tradingService.fetchAccounts { [weak self] result in
            guard let self, generation == self.readGeneration else { return }
            self.balanceReadPending = false
            self.balanceReadRequest = nil
            self.refreshButton.isEnabled = true

            switch result {
            case .success(let accounts):
                let receivedAt = self.now().formatted(date: .omitted, time: .standard)
                self.liveApiBadge.stringValue = "읽기 조회 완료 · 수신 \(receivedAt)"
                self.liveApiBadge.textColor = .systemBlue

                var krwBalance: String?
                var krwLocked: String?
                var cryptoHoldings: [String] = []

                for acc in accounts {
                    if acc.currency == "KRW" {
                        krwBalance = acc.balance
                        krwLocked = acc.locked
                    } else {
                        let available = acc.balanceDecimal
                        let locked = acc.lockedDecimal
                        if available == nil || locked == nil {
                            cryptoHoldings.append("\(acc.currency): 잔고 형식 오류")
                        } else if let available, let locked, available > 0 || locked > 0 {
                            var detail = "\(acc.currency): 사용 가능 \(acc.balance) · 잠김 \(acc.locked)"
                            if let average = Decimal(string: acc.avg_buy_price, locale: Locale(identifier: "en_US_POSIX")),
                               average > 0 {
                                detail += " · 평균 매수가 \(acc.avg_buy_price) \(acc.unit_currency)"
                            }
                            cryptoHoldings.append(detail)
                        }
                    }
                }

                self.unmaskedKRWBalance = krwBalance.flatMap(Self.formatKRW) ?? "KRW 잔고: 알 수 없음"
                self.unmaskedBalanceDetail = Self.balanceDetail(available: krwBalance, locked: krwLocked)

                if cryptoHoldings.isEmpty {
                    self.unmaskedCryptoHoldings = accounts.isEmpty
                        ? "보유 자산: 응답에 자산 없음"
                        : "보유 가상자산: 응답에 양수 잔고 없음"
                } else {
                    self.unmaskedCryptoHoldings = "보유 가상자산: " + cryptoHoldings.joined(separator: ", ")
                }
                self.cryptoHoldingsLabel.textColor = .secondaryLabelColor
                self.applyBalancePrivacy()

            case .failure:
                self.liveApiBadge.stringValue = "읽기 상태: 조회 실패 · 값 최신성 확인 불가"
                self.liveApiBadge.textColor = .systemRed
                self.unmaskedKRWBalance = "KRW 잔고: 알 수 없음"
                self.unmaskedBalanceDetail = "API 오류와 잔고 0은 구분됩니다. 다시 조회하세요."
                self.unmaskedCryptoHoldings = "보유 자산: 알 수 없음"
                self.applyBalancePrivacy()
            }
        }
        if balanceReadPending, generation == readGeneration { balanceReadRequest = request }
    }

    @objc func refreshControlStatus() {
        guard (!CommandLine.arguments.contains("--gui-review") || CommandLine.arguments.contains("--gui-review-read-only")),
              !controlRequestInFlight else { return }
        setControlRequestInFlight(true)
        controlReadPending = true
        let generation = readGeneration
        tradingStateLabel.stringValue = "실주문 서버 상태 조회 중 · 제어 잠김"
        tradingStateLabel.textColor = .secondaryLabelColor
        let request = tradingService.fetchControlStatus { [weak self] result in
            guard let self, generation == self.readGeneration else { return }
            self.controlReadPending = false
            self.controlReadRequest = nil
            self.setControlRequestInFlight(false)
            switch result {
            case .success(let snapshot):
                if self.updateControlSnapshot(snapshot, authenticated: true) {
                    self.resolveUnknownStrategyAdmission(withFreshStatus: snapshot)
                }
            case .failure:
                self.applyControlUnavailable("전략 제어 서버 상태를 확인할 수 없습니다")
            }
        }
        if controlReadPending, generation == readGeneration { controlReadRequest = request }
    }

    private func setControlRequestInFlight(_ inFlight: Bool) {
        controlRequestInFlight = inFlight
        if inFlight { applyControlAvailability(.backendControlUnavailable) }
        controlRefreshButton.isEnabled = !inFlight && (!CommandLine.arguments.contains("--gui-review") || CommandLine.arguments.contains("--gui-review-read-only"))
        if currentLiveOrderControlSnapshot != nil {
            renderCurrentLiveOrderControl()
        }
        updateStrategyAdmissionControls()
    }

    private static func formatKRW(_ raw: String) -> String? {
        guard let value = Decimal(string: raw, locale: Locale(identifier: "en_US_POSIX")) else { return nil }
        let formatter = NumberFormatter()
        formatter.numberStyle = .currency
        formatter.currencyCode = "KRW"
        formatter.currencySymbol = "₩"
        formatter.locale = Locale(identifier: "ko_KR")
        formatter.minimumFractionDigits = 2
        formatter.maximumFractionDigits = 2
        return formatter.string(from: value as NSDecimalNumber)
    }

    private static func balanceDetail(available: String?, locked: String?) -> String {
        let availableText = available.flatMap(formatKRW) ?? "알 수 없음"
        let lockedText = locked.flatMap(formatKRW) ?? "알 수 없음"
        return "주문 가능: \(availableText)  ·  잠김: \(lockedText)"
    }

    @objc private func toggleBalancePrivacy() {
        isBalanceMasked.toggle()
        applyBalancePrivacy()
    }

    private func applyBalancePrivacy() {
        privacyButton.title = isBalanceMasked ? "금액 보기" : "금액 숨기기"
        privacyButton.setAccessibilityLabel(isBalanceMasked ? "공유 보호 해제 · 금액 표시" : "화면 공유 보호 · 금액 숨기기")
        if isBalanceMasked {
            krwBalanceDisplay.stringValue = "금액 숨김"
            balanceDetailLabel.stringValue = "계좌 금액이 화면 공유 보호를 위해 숨겨졌습니다."
            cryptoHoldingsLabel.stringValue = "보유 자산 숨김"
        } else {
            krwBalanceDisplay.stringValue = unmaskedKRWBalance
            balanceDetailLabel.stringValue = unmaskedBalanceDetail
            cryptoHoldingsLabel.stringValue = unmaskedCryptoHoldings
        }
        krwBalanceDisplay.setAccessibilityValue(krwBalanceDisplay.stringValue)
        NSAccessibility.post(element: krwBalanceDisplay, notification: .valueChanged)
    }

    @objc private func openDashboardPressed() {
        onOpenDashboard?()
    }

    @objc private func openAutomationWorkspacePressed() {
        if let onOpenAutomationWorkspace {
            onOpenAutomationWorkspace()
        } else {
            onOpenDashboard?()
        }
    }

    @objc private func strategyAdmissionPressed() {
        guard let snapshot = currentControlSnapshot,
              let action = availableStrategyAdmissionAction() else {
            updateStrategyAdmissionControls()
            return
        }
        let alert = NSAlert()
        alert.alertStyle = .critical
        alert.messageText = action == "start"
            ? "계좌 전체 전략 주문 권한을 시작하시겠습니까?"
            : "계좌 전체 전략 주문 권한을 재개하시겠습니까?"
        alert.informativeText = [
            "요청 작업: \(action.uppercased())",
            "범위: 현재 OAuth LIVE 계좌 전체 · 이 제어 응답은 account_identity를 제공하지 않습니다.",
            "검토한 서버 상태: \(snapshot.state) · sequence \(snapshot.sequence) · observed_at_utc \(snapshot.observed_at_utc)",
            "live_trading_enabled: ON · emergency_latched: OFF · order_admission_blocked: \(snapshot.order_admission_blocked ? "YES" : "NO")",
            "이 요청은 개별 전략 세션을 만들거나 재개하지 않습니다. 이미 RUNNING인 전략 세션은 각자 정책·위험 한도와 별도 주문 전송 게이트가 허용하면 새 주문을 낼 수 있습니다.",
            "이 요청은 주문 전송 게이트를 켜거나 끄지 않으며 긴급 정지를 해제하지 않습니다."
        ].joined(separator: "\n\n")
        alert.addButton(withTitle: action == "start" ? "계좌 전체 START 요청" : "계좌 전체 RESUME 요청")
        alert.addButton(withTitle: "취소")
        guard alert.runModal() == .alertFirstButtonReturn else {
            strategyAdmissionStatusMessage = "계좌 전체 \(action.uppercased()) 요청을 취소했습니다. 서버에는 변경 요청을 보내지 않았습니다."
            updateStrategyAdmissionControls()
            return
        }
        performAccountStrategyAdmissionConfirmed(
            expectedAction: action,
            expectedSequence: snapshot.sequence
        )
    }

    func performAccountStrategyAdmissionConfirmed(
        requestID: String = UUID().uuidString,
        expectedAction: String,
        expectedSequence: UInt64
    ) {
        guard !requestID.isEmpty,
              let snapshot = currentControlSnapshot,
              let context = tradingService.currentTransportContext(),
              tradingService.isCurrentTransportContext(context),
              snapshot.sequence == expectedSequence,
              availableStrategyAdmissionAction() == expectedAction else {
            strategyAdmissionStatusMessage = "검토 이후 서버 상태가 바뀌었거나 오래되었습니다. 요청을 보내지 않았습니다. 상태를 새로고침하고 다시 검토하세요."
            updateStrategyAdmissionControls()
            return
        }

        let pending = PendingStrategyAdmission(
            requestID: requestID,
            action: expectedAction,
            expectedSequence: expectedSequence,
            context: context,
            phase: .sending
        )
        pendingStrategyAdmission = pending
        strategyAdmissionStatusMessage = nil
        setControlRequestInFlight(true)
        tradingStateLabel.stringValue = "계좌 전체 \(expectedAction.uppercased()) 요청 전송 중 · 서버 응답 확인 대기"
        tradingStateLabel.textColor = .systemOrange
        updateStrategyAdmissionControls()
        tradingService.safeControl(
            action: expectedAction,
            requestId: requestID,
            sequence: expectedSequence,
            expectedContext: context
        ) { [weak self] result in
            guard let self,
                  let current = self.pendingStrategyAdmission,
                  current.phase == .sending,
                  current.requestID == requestID,
                  current.action == expectedAction,
                  current.expectedSequence == expectedSequence else { return }
            guard self.tradingService.isCurrentTransportContext(current.context) else {
                self.markStrategyAdmissionUnknown(requestID: requestID, detail: "요청한 Backend 연결이 변경되었습니다")
                return
            }
            switch result {
            case .failure(let error):
                self.markStrategyAdmissionUnknown(
                    requestID: requestID,
                    detail: "응답을 확인할 수 없습니다 (\(error.localizedDescription))"
                )
            case .success(let response):
                self.handleStrategyAdmissionResponse(
                    response,
                    requestID: requestID,
                    action: expectedAction,
                    expectedSequence: expectedSequence
                )
            }
        }
    }

    private func handleStrategyAdmissionResponse(
        _ snapshot: TradingControlSnapshot,
        requestID: String,
        action: String,
        expectedSequence: UInt64
    ) {
        guard let receipt = snapshot.receipt,
              receipt.request_id == requestID,
              receipt.action == action,
              receipt.sequence == snapshot.sequence,
              receipt.sequence >= expectedSequence,
              Self.isFresh(receipt.observed_at_utc, now: now()),
              Self.isFresh(snapshot.observed_at_utc, now: now()) else {
            markStrategyAdmissionUnknown(requestID: requestID, detail: "응답 receipt의 request_id/action/sequence가 요청과 정확히 일치하지 않습니다")
            return
        }

        if receipt.outcome == "DENIED" {
            guard updateControlSnapshot(snapshot, authenticated: true) else {
                markStrategyAdmissionUnknown(requestID: requestID, detail: "거부 응답과 함께 온 서버 상태가 오래되었거나 인증되지 않았습니다")
                return
            }
            pendingStrategyAdmission = nil
            setControlRequestInFlight(false)
            let reason = receipt.denied_code ?? snapshot.denied_code ?? "DENIED"
            strategyAdmissionStatusMessage = "\(action.uppercased()) 거부 · \(reason) · 현재 state \(snapshot.state) · sequence \(snapshot.sequence)"
            tradingStateLabel.stringValue = "계좌 전체 \(action.uppercased()) 요청 거부 · \(reason)"
            tradingStateLabel.textColor = .systemRed
            updateStrategyAdmissionControls()
            return
        }

        guard receipt.outcome == "ACKNOWLEDGED",
              receipt.sequence > expectedSequence,
              snapshot.exchange_read_mode == "LIVE_READ",
              snapshot.live_trading_enabled,
              snapshot.state == "RUNNING",
              !snapshot.order_admission_blocked,
              !snapshot.emergency_latched,
              updateControlSnapshot(snapshot, authenticated: true) else {
            markStrategyAdmissionUnknown(requestID: requestID, detail: "ACKNOWLEDGED 및 RUNNING 계좌 상태를 정확히 확인하지 못했습니다")
            return
        }
        pendingStrategyAdmission = nil
        setControlRequestInFlight(false)
        strategyAdmissionStatusMessage = "\(action.uppercased()) 확인됨 · 계좌 전체 전략 권한 RUNNING · request_id \(requestID) · sequence \(snapshot.sequence) · 주문 전송 게이트는 별도 상태입니다."
        tradingStateLabel.stringValue = "계좌 전체 전략 권한 RUNNING 확인 · \(snapshot.observed_at_utc)"
        tradingStateLabel.textColor = .systemOrange
        updateStrategyAdmissionControls()
    }

    @objc private func emergencyStopPressed() {
        guard controlAvailabilitySnapshot.emergency else {
            showUnavailableControlAlert(action: "Emergency Stop")
            return
        }
        let alert = NSAlert()
        alert.alertStyle = .critical
        alert.messageText = "신규 주문을 긴급 차단하시겠습니까?"
        alert.informativeText = "긴급 정지는 새 주문을 막습니다. 기존 미체결 주문 취소나 보유 자산 매도·청산은 하지 않습니다. 서버에서 적용을 확인한 뒤 결과를 표시합니다."
        alert.addButton(withTitle: "신규 주문 차단 요청")
        alert.addButton(withTitle: "취소")
        if alert.runModal() == .alertFirstButtonReturn {
            performEmergencyStopConfirmed()
        }
    }

    func performEmergencyStopConfirmed(requestId: String = UUID().uuidString) {
        guard !CommandLine.arguments.contains("--gui-review"),
              !controlRequestInFlight,
              let snapshot = currentControlSnapshot,
              snapshot.live_trading_enabled,
              controlAvailabilitySnapshot.emergency else { return }

        setControlRequestInFlight(true)
        tradingStateLabel.stringValue = "긴급 정지 요청 중 · 서버 확인 대기 · 결과 미확정"
        tradingStateLabel.textColor = .systemOrange
        tradingService.safeControl(
            action: "emergency_stop",
            requestId: requestId,
            sequence: snapshot.sequence,
            expectedContext: tradingService.currentTransportContext()
        ) { [weak self] result in
            guard let self else { return }
            switch result {
            case .failure:
                self.setControlRequestInFlight(false)
                self.applyControlUnavailable("긴급 정지 결과를 확인할 수 없습니다. 서버 상태를 다시 조회하세요.")
            case .success(let acknowledged):
                self.handleEmergencyAcknowledgement(acknowledged, requestId: requestId)
            }
        }
    }

    private func handleEmergencyAcknowledgement(
        _ snapshot: TradingControlSnapshot,
        requestId: String
    ) {
        guard let receipt = snapshot.receipt,
              receipt.request_id == requestId,
              receipt.action == "emergency_stop",
              receipt.sequence == snapshot.sequence,
              Self.isFresh(receipt.observed_at_utc, now: now()) else {
            setControlRequestInFlight(false)
            applyControlUnavailable("긴급 정지 응답을 확인할 수 없습니다. 서버 상태를 다시 조회하세요.")
            return
        }

        guard receipt.outcome == "ACKNOWLEDGED" else {
            setControlRequestInFlight(false)
            _ = updateControlSnapshot(snapshot, authenticated: true)
            let code = receipt.denied_code ?? snapshot.denied_code ?? "DENIED"
            tradingStateLabel.stringValue = "긴급 정지 거부 · \(code)"
            tradingStateLabel.textColor = .systemRed
            return
        }

        tradingStateLabel.stringValue = "긴급 정지 서버 확인 수신 · 현재 상태를 다시 확인 중"
        tradingService.fetchControlStatus { [weak self] result in
            guard let self else { return }
            self.setControlRequestInFlight(false)
            guard case .success(let readback) = result,
                  readback.sequence == receipt.sequence,
                  readback.emergency_latched,
                  readback.order_admission_blocked,
                  self.updateControlSnapshot(readback, authenticated: true) else {
                self.applyControlUnavailable("긴급 정지 후 서버 상태를 확인할 수 없습니다")
                return
            }
        }
    }

    private func showUnavailableControlAlert(action: String) {
        let alert = NSAlert()
        alert.alertStyle = .warning
        alert.messageText = "\(action) 명령을 보낼 수 없습니다"
        alert.informativeText = "서버에서 이 제어를 사용할 수 없거나 현재 상태를 확인하지 못했습니다. 표시된 상태는 서버 확인 전까지 바꾸지 않습니다."
        alert.addButton(withTitle: "확인")
        alert.runModal()
    }

    @objc private func cancelPressed() { close() }

    @objc private func savePressed() {
        do {
            guard let port = Int(portField.stringValue) else { throw AppError.configuration("올바른 포트를 입력하세요.") }
            let configuration = try AppConfiguration(
                domain: domainField.stringValue,
                port: port,
                authenticationMode: AuthenticationMode.allCases[authPopup.indexOfSelectedItem],
                tradingMcpEnabled: tradingMcpToggle.state == .on
            ).validated()
            if (try? store.load()) != configuration {
                try store.save(configuration)
                configurationDidSave?(configuration)
            }
            close()
        } catch { NSAlert(error: error).runModal() }
    }
}
