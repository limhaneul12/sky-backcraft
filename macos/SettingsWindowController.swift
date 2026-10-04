import AppKit

final class SettingsWindowController: NSWindowController, NSWindowDelegate {
    private let store: ConfigurationStore
    private let domainField = NSTextField()
    private let portField = NSTextField()
    private let authPopup = NSPopUpButton()
    private let statusLabel = NSTextField(labelWithString: "")
    var configurationDidSave: ((AppConfiguration) -> Void)?

    init(store: ConfigurationStore) {
        self.store = store
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 520, height: 330),
            styleMask: [.titled, .closable], backing: .buffered, defer: false
        )
        window.title = "Sky Backcraft 설정"
        window.isReleasedWhenClosed = false
        super.init(window: window)
        window.delegate = self
        buildUI()
        populate(.default)
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
        window?.center()
        NSApp.activate(ignoringOtherApps: true)
        if let configurationError, let window {
            NSAlert(error: configurationError).beginSheetModal(for: window)
        }
    }

    func updateStatus(_ status: RuntimeStatus) {
        statusLabel.stringValue = status.menuTitle
        switch status {
        case .running: statusLabel.textColor = .systemGreen
        case .failed, .blocked: statusLabel.textColor = .systemRed
        default: statusLabel.textColor = .secondaryLabelColor
        }
    }

    private func buildUI() {
        guard let content = window?.contentView else { return }
        let heading = NSTextField(labelWithString: "MCP 연결 설정")
        heading.font = .systemFont(ofSize: 22, weight: .semibold)
        domainField.placeholderString = "비워 두면 Quick Tunnel을 사용합니다"
        portField.placeholderString = String(AppConfiguration.defaultPort)
        let formatter = NumberFormatter()
        formatter.minimum = 1
        formatter.maximum = 65535
        formatter.allowsFloats = false
        portField.formatter = formatter
        authPopup.addItems(withTitles: AuthenticationMode.allCases.map(\.displayName))

        let form = NSGridView(views: [
            [formLabel("도메인 (선택)"), domainField],
            [formLabel("로컬 포트"), portField],
            [formLabel("인증 모드"), authPopup]
        ])
        form.column(at: 0).xPlacement = .trailing
        form.column(at: 1).xPlacement = .fill
        form.column(at: 1).width = 300
        form.rowSpacing = 16
        form.columnSpacing = 16

        let hint = NSTextField(wrappingLabelWithString: "도메인을 입력하면 기존 Cloudflare Tunnel을 사용합니다. 비워 두면 앱이 임시 Quick Tunnel을 만듭니다.")
        hint.textColor = .secondaryLabelColor
        hint.font = .systemFont(ofSize: 12)
        let cancel = NSButton(title: "취소", target: self, action: #selector(cancelPressed))
        let save = NSButton(title: "저장 및 재시작", target: self, action: #selector(savePressed))
        save.keyEquivalent = "\r"
        let footerSpacer = NSView()
        footerSpacer.setContentHuggingPriority(.defaultLow, for: .horizontal)
        footerSpacer.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        let buttons = NSStackView(views: [footerSpacer, cancel, save])
        buttons.orientation = .horizontal
        buttons.spacing = 10

        let stack = NSStackView(views: [heading, form, hint, statusLabel, buttons])
        stack.orientation = .vertical
        stack.alignment = .leading
        stack.spacing = 20
        stack.translatesAutoresizingMaskIntoConstraints = false
        content.addSubview(stack)
        NSLayoutConstraint.activate([
            stack.leadingAnchor.constraint(equalTo: content.leadingAnchor, constant: 32),
            stack.trailingAnchor.constraint(equalTo: content.trailingAnchor, constant: -32),
            stack.topAnchor.constraint(equalTo: content.topAnchor, constant: 28),
            buttons.widthAnchor.constraint(equalTo: stack.widthAnchor),
            hint.widthAnchor.constraint(equalTo: stack.widthAnchor)
        ])
    }

    private func formLabel(_ text: String) -> NSTextField {
        let field = NSTextField(labelWithString: text)
        field.textColor = .secondaryLabelColor
        return field
    }

    private func populate(_ configuration: AppConfiguration) {
        domainField.stringValue = configuration.domain
        portField.stringValue = String(configuration.port)
        authPopup.selectItem(withTitle: configuration.authenticationMode.displayName)
    }

    @objc private func cancelPressed() { close() }

    @objc private func savePressed() {
        do {
            guard let port = Int(portField.stringValue) else { throw AppError.configuration("올바른 포트를 입력하세요.") }
            let configuration = try AppConfiguration(
                domain: domainField.stringValue,
                port: port,
                authenticationMode: AuthenticationMode.allCases[authPopup.indexOfSelectedItem]
            ).validated()
            try store.save(configuration)
            configurationDidSave?(configuration)
            close()
        } catch { NSAlert(error: error).runModal() }
    }
}
