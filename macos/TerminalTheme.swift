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

    static var body: NSFont { systemFont(ofSize: 14) }
    static var table: NSFont { monospacedDigitFont(ofSize: 13, weight: .regular) }
    static var tableRowHeight: CGFloat { max(32, ceil(32 * textScale)) }

    static func prepareTable(_ table: NSTableView) {
        for column in table.tableColumns {
            let cell = TerminalTableHeaderCell(textCell: column.title)
            cell.font = systemFont(ofSize: 12, weight: .medium)
            cell.textColor = .labelColor
            cell.alignment = ["Return", "MDD", "PnL", "Fees", "Trades"].contains(column.identifier.rawValue) ? .right : .left
            column.headerCell = cell
        }
        table.rowHeight = tableRowHeight
        table.usesAlternatingRowBackgroundColors = false
        table.columnAutoresizingStyle = .noColumnAutoresizing
        table.backgroundColor = TerminalTheme.chart
        table.enclosingScrollView?.drawsBackground = false
        table.gridStyleMask = [.solidHorizontalGridLineMask]
        table.gridColor = .separatorColor
        table.allowsMultipleSelection = false
    }
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
