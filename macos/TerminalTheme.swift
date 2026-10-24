import AppKit

/// Semantic native colors stay correct under system light/dark and high contrast.
enum TerminalTheme {
    static var surface: NSColor { neutral(light: 0.965, dark: 0.105) }
    static var panel: NSColor { neutral(light: 1, dark: 0.14) }
    static var border: NSColor { .separatorColor }
    static var chart: NSColor { neutral(light: 1, dark: 0.105) }
    static var accent: NSColor { .controlAccentColor }
    static var secondaryText: NSColor { neutral(light: 0.30, dark: 0.76) }
    private static func neutral(light: CGFloat, dark: CGFloat) -> NSColor {
        NSColor(name: nil) { appearance in
            let value = appearance.bestMatch(from: [.aqua, .darkAqua]) == .darkAqua ? dark : light
            return NSColor(srgbRed: value, green: value, blue: value, alpha: 1)
        }
    }
    static var positive: NSColor {
        NSColor(name: nil) { appearance in
            appearance.bestMatch(from: [.aqua, .darkAqua]) == .darkAqua
                ? NSColor(srgbRed: 0.48, green: 0.85, blue: 0.64, alpha: 1)
                : NSColor(srgbRed: 0.08, green: 0.43, blue: 0.26, alpha: 1)
        }
    }
    static var negative: NSColor {
        NSColor(name: nil) { appearance in
            appearance.bestMatch(from: [.aqua, .darkAqua]) == .darkAqua
                ? NSColor(srgbRed: 1, green: 0.67, blue: 0.63, alpha: 1)
                : NSColor(srgbRed: 0.7, green: 0.14, blue: 0.09, alpha: 1)
        }
    }
    private static var configuredTextScale: CGFloat = {
        if let raw = ProcessInfo.processInfo.environment["SERVICE_TEXT_SCALE"],
           let value = Double(raw), value >= 1.0, value <= 2.5 {
            return CGFloat(value)
        }
        return 1.0
    }()

    static var textScale: CGFloat { configuredTextScale }

    static var screenTitle: NSFont { systemFont(ofSize: 20, weight: .semibold) }
    static var navigationItem: NSFont { systemFont(ofSize: 13, weight: .medium) }
    static var navigationGroup: NSFont { systemFont(ofSize: 10, weight: .semibold) }
    static var sectionHeading: NSFont { systemFont(ofSize: 12, weight: .semibold) }
    static var navigationRowHeight: CGFloat { max(30, ceil(30 * min(textScale, 1.5))) }
    static var navigationIconSize: CGFloat { max(14, ceil(15 * min(textScale, 1.4))) }

    static func setTextScale(_ scale: CGFloat) {
        configuredTextScale = min(max(scale, 1.0), 2.5)
    }

    static func systemFont(ofSize size: CGFloat, weight: NSFont.Weight = .regular) -> NSFont {
        NSFont.systemFont(ofSize: round(size * textScale), weight: weight)
    }

    static func monospacedDigitFont(ofSize size: CGFloat, weight: NSFont.Weight = .regular) -> NSFont {
        NSFont.monospacedDigitSystemFont(ofSize: round(size * textScale), weight: weight)
    }

    static func monospacedFont(ofSize size: CGFloat, weight: NSFont.Weight = .regular) -> NSFont {
        NSFont.monospacedSystemFont(ofSize: round(size * textScale), weight: weight)
    }

    static var body: NSFont { systemFont(ofSize: 13) }
    static var caption: NSFont { systemFont(ofSize: 11) }
    static var table: NSFont { monospacedDigitFont(ofSize: 13, weight: .regular) }
    static var tableRowHeight: CGFloat { max(32, ceil(32 * textScale)) }

    /// Hairline used between table rows; lighter than `separatorColor`, which reads as a heavy rule in dense grids.
    static var tableGrid: NSColor { neutral(light: 0.91, dark: 0.20) }

    static func prepareTable(_ table: NSTableView) {
        for column in table.tableColumns {
            let cell = TerminalTableHeaderCell(textCell: column.title)
            cell.font = systemFont(ofSize: 11, weight: .semibold)
            cell.textColor = .secondaryLabelColor
            cell.alignment = ["Return", "MDD", "PnL", "Fees", "Trades"].contains(column.identifier.rawValue) ? .right : .left
            column.headerCell = cell
        }
        table.rowHeight = tableRowHeight
        table.intercellSpacing = NSSize(width: 12, height: 0)
        table.usesAlternatingRowBackgroundColors = false
        table.columnAutoresizingStyle = .noColumnAutoresizing
        table.backgroundColor = TerminalTheme.chart
        table.enclosingScrollView?.drawsBackground = false
        table.gridStyleMask = [.solidHorizontalGridLineMask]
        table.gridColor = tableGrid
        table.allowsMultipleSelection = false
        table.selectionHighlightStyle = .regular
        table.focusRingType = .none
    }

    /// The one card style used by every workspace: 8pt radius, hairline border, panel fill.
    static func makePanelBox() -> NSBox {
        let box = NSBox()
        box.boxType = .custom
        box.titlePosition = .noTitle
        box.cornerRadius = 8
        box.borderWidth = 1
        box.borderColor = border
        box.fillColor = panel
        box.contentViewMargins = .zero
        return box
    }
}

/// Semantic state tone. Unknown is a warning, never an error; red is reserved for real stops and live order risk.
enum TerminalTone {
    case neutral
    case positive
    case warning
    case critical
    case accent

    var color: NSColor {
        switch self {
        case .neutral: return .systemGray
        case .positive: return .systemGreen
        case .warning: return .systemOrange
        case .critical: return .systemRed
        case .accent: return .controlAccentColor
        }
    }
}

/// Compact status capsule: tone dot, short domain label and the exact state text.
/// The value field keeps the full string (tooltip and accessibility value) even when it truncates visually.
final class TerminalStatusPill: NSView {
    let valueLabel: NSTextField
    private let domainLabel: NSTextField
    private let dot = TerminalStatusDot()
    var tone: TerminalTone = .neutral {
        didSet { dot.color = tone.color; needsDisplay = true }
    }

    init(domain: String, value: String, tone: TerminalTone) {
        domainLabel = NSTextField(labelWithString: domain)
        valueLabel = NSTextField(labelWithString: value)
        super.init(frame: .zero)
        domainLabel.font = TerminalTheme.systemFont(ofSize: 11, weight: .semibold)
        domainLabel.textColor = .secondaryLabelColor
        domainLabel.setAccessibilityElement(false)
        domainLabel.setContentCompressionResistancePriority(.required, for: .horizontal)
        domainLabel.setContentHuggingPriority(.required, for: .horizontal)
        valueLabel.font = TerminalTheme.systemFont(ofSize: 12, weight: .medium)
        valueLabel.textColor = .labelColor
        valueLabel.lineBreakMode = .byTruncatingTail
        valueLabel.maximumNumberOfLines = 1
        valueLabel.toolTip = value
        valueLabel.setAccessibilityLabel(domain)
        valueLabel.setAccessibilityValue(value)
        valueLabel.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        valueLabel.setContentHuggingPriority(.defaultHigh, for: .horizontal)
        let stack = NSStackView(views: [dot, domainLabel, valueLabel])
        stack.orientation = .horizontal
        stack.alignment = .centerY
        stack.spacing = 6
        stack.setCustomSpacing(8, after: domainLabel)
        stack.translatesAutoresizingMaskIntoConstraints = false
        addSubview(stack)
        let dotSize = max(7, round(7 * min(TerminalTheme.textScale, 1.6)))
        NSLayoutConstraint.activate([
            dot.widthAnchor.constraint(equalToConstant: dotSize),
            dot.heightAnchor.constraint(equalToConstant: dotSize),
            stack.leadingAnchor.constraint(equalTo: leadingAnchor, constant: 10),
            stack.trailingAnchor.constraint(equalTo: trailingAnchor, constant: -10),
            stack.topAnchor.constraint(equalTo: topAnchor, constant: 5),
            stack.bottomAnchor.constraint(equalTo: bottomAnchor, constant: -5)
        ])
        setContentHuggingPriority(.required, for: .vertical)
        setContentCompressionResistancePriority(.required, for: .vertical)
        self.tone = tone
        dot.color = tone.color
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) { nil }

    override var wantsDefaultClipping: Bool { true }

    override func draw(_ dirtyRect: NSRect) {
        let path = NSBezierPath(roundedRect: bounds.insetBy(dx: 0.5, dy: 0.5), xRadius: 7, yRadius: 7)
        tone.color.withAlphaComponent(tone == .neutral ? 0.06 : 0.10).setFill()
        path.fill()
        tone.color.withAlphaComponent(tone == .neutral ? 0.18 : 0.30).setStroke()
        path.lineWidth = 1
        path.stroke()
    }

    override func viewDidChangeEffectiveAppearance() {
        super.viewDidChangeEffectiveAppearance()
        needsDisplay = true
    }
}

/// Decorative tone dot; never an accessibility element so status text remains the single announced value.
final class TerminalStatusDot: NSView {
    var color: NSColor = .systemGray { didSet { needsDisplay = true } }

    override init(frame frameRect: NSRect) {
        super.init(frame: frameRect)
        setAccessibilityElement(false)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) { nil }

    override func draw(_ dirtyRect: NSRect) {
        color.setFill()
        NSBezierPath(ovalIn: bounds.insetBy(dx: 0.5, dy: 0.5)).fill()
    }
}

/// Centered empty state: symbol, one-line title, short explanation.
final class TerminalEmptyStateView: NSView {
    let titleLabel = NSTextField(labelWithString: "")
    let messageLabel = NSTextField(wrappingLabelWithString: "")
    private let iconView = NSImageView()

    init(symbolName: String, title: String, message: String) {
        super.init(frame: .zero)
        iconView.image = NSImage(systemSymbolName: symbolName, accessibilityDescription: nil)
        iconView.symbolConfiguration = NSImage.SymbolConfiguration(pointSize: round(34 * min(TerminalTheme.textScale, 1.6)), weight: .light)
        iconView.contentTintColor = .tertiaryLabelColor
        iconView.setAccessibilityElement(false)
        titleLabel.stringValue = title
        titleLabel.font = TerminalTheme.systemFont(ofSize: 15, weight: .semibold)
        titleLabel.alignment = .center
        messageLabel.stringValue = message
        messageLabel.font = TerminalTheme.systemFont(ofSize: 12)
        messageLabel.textColor = .secondaryLabelColor
        messageLabel.alignment = .center
        messageLabel.maximumNumberOfLines = 3
        let stack = NSStackView(views: [iconView, titleLabel, messageLabel])
        stack.orientation = .vertical
        stack.alignment = .centerX
        stack.spacing = 6
        stack.setCustomSpacing(12, after: iconView)
        stack.translatesAutoresizingMaskIntoConstraints = false
        addSubview(stack)
        NSLayoutConstraint.activate([
            stack.centerXAnchor.constraint(equalTo: centerXAnchor),
            stack.centerYAnchor.constraint(equalTo: centerYAnchor),
            stack.widthAnchor.constraint(lessThanOrEqualToConstant: round(420 * min(TerminalTheme.textScale, 1.6))),
            stack.leadingAnchor.constraint(greaterThanOrEqualTo: leadingAnchor, constant: 20),
            stack.topAnchor.constraint(greaterThanOrEqualTo: topAnchor, constant: 12),
            messageLabel.widthAnchor.constraint(lessThanOrEqualTo: stack.widthAnchor)
        ])
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) { nil }
}

enum TerminalPresentation {
    static func state(_ raw: String) -> String {
        switch raw {
        case "COMPLETED": return "완료"
        case "FAILED": return "실패"
        case "BLOCKED": return "차단"
        case "CANCELLED": return "중단"
        case "RUNNING": return "실행 중"
        case "QUEUED": return "대기"
        case "PARTIAL": return "일부 완료"
        case "HISTORICAL_REPLAY": return "과거 백테스트"
        case "INDEPENDENT_MODELS": return "개별 자본"
        case "SHARED_PORTFOLIO": return "공유 자본"
        case "BUILTIN": return "내장"
        case "USER": return "사용자"
        case "OTHER": return "기타"
        case "DISABLED_BY_CONFIG": return "실거래 사용 안 함"
        case "EMERGENCY_STOP_ACTIVE": return "신규 주문 긴급 차단"
        case "STANDBY": return "대기"
        case "PAUSED": return "일시중지"
        default: return "미확인"
        }
    }
}

/// Draw inside the effective appearance so a live mode switch never freezes a CGColor.
final class TerminalSurfaceView: NSView {
    var usesPanel = false
    override var wantsDefaultClipping: Bool { true }
    override func draw(_ dirtyRect: NSRect) {
        (usesPanel ? TerminalTheme.panel : TerminalTheme.surface).setFill()
        bounds.fill()
    }
    override func viewDidChangeEffectiveAppearance() {
        super.viewDidChangeEffectiveAppearance()
        needsDisplay = true
    }
}

final class TerminalTableHeaderCell: NSTableHeaderCell {
    override func draw(withFrame cellFrame: NSRect, in controlView: NSView) {
        TerminalTheme.panel.setFill()
        cellFrame.fill()
        drawInterior(withFrame: cellFrame.insetBy(dx: 6, dy: 0), in: controlView)
    }
}

/// Canonical flipped document container that keeps initial scroll pinned to `.zero`
/// across multi-pass Auto Layout growth while preserving manual user scrolling.
final class TerminalFlippedDocumentView: NSView {
    var usesPanel = false
    private var didPinOverflowScroll = false

    override var isFlipped: Bool { true }
    override var wantsDefaultClipping: Bool { true }

    override func draw(_ dirtyRect: NSRect) {
        guard usesPanel else { return }
        TerminalTheme.panel.setFill()
        bounds.fill()
    }

    override func viewDidChangeEffectiveAppearance() {
        super.viewDidChangeEffectiveAppearance()
        if usesPanel {
            needsDisplay = true
        }
    }

    override func setFrameSize(_ newSize: NSSize) {
        let wasAtTop = (enclosingScrollView?.contentView.bounds.origin.y ?? 0) <= 1.0
        super.setFrameSize(newSize)
        if wasAtTop || !didPinOverflowScroll {
            pinToTopIfNeeded()
        }
    }

    override func viewDidMoveToWindow() {
        super.viewDidMoveToWindow()
        if !didPinOverflowScroll {
            pinToTopIfNeeded()
        }
    }

    override func layout() {
        super.layout()
        if !didPinOverflowScroll {
            pinToTopIfNeeded()
        }
    }

    private func pinToTopIfNeeded() {
        guard let scrollView = enclosingScrollView else { return }
        let clipHeight = scrollView.contentView.bounds.height
        let docHeight = frame.height
        guard clipHeight > 0, docHeight > 0 else { return }
        if scrollView.contentView.bounds.origin != .zero {
            scrollView.contentView.scroll(to: .zero)
            scrollView.reflectScrolledClipView(scrollView.contentView)
        }
        if docHeight > clipHeight {
            didPinOverflowScroll = true
        }
    }
}

/// Scroll view that keeps an embedded read-only `NSTextView` horizontally wrapped to `contentView.bounds.width`.
final class TerminalWrappingTextScrollView: NSScrollView {
    private var didPinInitialScroll = false

    func configureWrapping(textView: NSTextView, inset: NSSize = NSSize(width: 10, height: 10)) {
        hasVerticalScroller = true
        hasHorizontalScroller = false
        autohidesScrollers = true

        textView.minSize = NSSize(width: 0, height: 0)
        textView.maxSize = NSSize(width: CGFloat.greatestFiniteMagnitude, height: CGFloat.greatestFiniteMagnitude)
        textView.isHorizontallyResizable = false
        textView.isVerticallyResizable = true
        textView.autoresizingMask = [.width]
        textView.textContainerInset = inset
        textView.textContainer?.lineFragmentPadding = 0
        textView.textContainer?.widthTracksTextView = true
        textView.textContainer?.heightTracksTextView = false
        textView.textContainer?.lineBreakMode = .byWordWrapping
        textView.textContainer?.containerSize = NSSize(width: 0, height: CGFloat.greatestFiniteMagnitude)
        documentView = textView
        syncTextViewWidth()
    }

    override func layout() {
        super.layout()
        syncTextViewWidth()
        if !didPinInitialScroll, contentView.bounds.height > 0, (documentView?.frame.height ?? 0) > 0 {
            if contentView.bounds.origin != .zero {
                contentView.scroll(to: .zero)
                reflectScrolledClipView(contentView)
            }
            didPinInitialScroll = true
        }
    }

    override func tile() {
        super.tile()
        syncTextViewWidth()
    }

    private func syncTextViewWidth() {
        guard let textView = documentView as? NSTextView else { return }
        let width = contentView.bounds.width
        guard width > 0 else { return }
        if abs(textView.frame.width - width) > 0.5 {
            textView.setFrameSize(NSSize(width: width, height: max(textView.frame.height, contentView.bounds.height)))
        }
        let insetWidth = max(0, width - textView.textContainerInset.width * 2)
        if let container = textView.textContainer, abs(container.containerSize.width - insetWidth) > 0.5 {
            container.containerSize = NSSize(width: insetWidth, height: CGFloat.greatestFiniteMagnitude)
            textView.layoutManager?.ensureLayout(for: container)
        }
    }
}
