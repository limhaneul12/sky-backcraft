import AppKit
import Darwin
import Foundation

private enum PolicyCatalogLayoutFailure: Error, CustomStringConvertible {
    case assertion(String)

    var description: String {
        switch self {
        case .assertion(let message): return message
        }
    }
}

@main
private enum PolicyCatalogLayoutRegression {
    static func main() {
        _ = NSApplication.shared
        let previousTextScale = TerminalTheme.textScale
        defer { TerminalTheme.setTextScale(previousTextScale) }

        do {
            try verifyInspectorLayout(textScale: 1.0)
            try verifyInspectorLayout(textScale: 2.0)
            try verifyPaperDecisionTextLayout()
            print("PASS paper decision text has nonzero document bounds and laid-out glyphs intersecting the scroll clip")
            print("PASS no-selection policy inspector remains reachable at 1200x800 and after resize")
        } catch {
            fputs("FAIL policy catalog layout: \(error)\n", stderr)
            exit(EXIT_FAILURE)
        }
    }

    private static func verifyInspectorLayout(textScale: CGFloat) throws {
        TerminalTheme.setTextScale(textScale)
        let supportDirectory = FileManager.default.temporaryDirectory
            .appendingPathComponent("sky-backcraft-policy-catalog-layout-\(UUID().uuidString)", isDirectory: true)
        let databaseURL = supportDirectory.appendingPathComponent("research/lab.sqlite")
        try expect(!FileManager.default.fileExists(atPath: databaseURL.path), "layout test support path unexpectedly contains a database")

        let workspace = PolicyInventoryView(
            reader: DatabaseReader(supportDirectory: supportDirectory),
            policies: [],
            onOpenRun: { _, _, _ in },
            onShowStrategy: { _ in }
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 1200, height: 800),
            styleMask: [.borderless],
            backing: .buffered,
            defer: true
        )
        window.isReleasedWhenClosed = false
        let root = NSView(frame: NSRect(x: 0, y: 0, width: 1200, height: 800))
        root.autoresizingMask = [.width, .height]
        workspace.frame = NSRect(x: 230, y: 0, width: 970, height: 800)
        workspace.autoresizingMask = [.width, .height]
        root.addSubview(workspace)
        window.contentView = root
        defer { window.close() }

        let views = descendants(of: workspace)
        guard let title = views.compactMap({ $0 as? NSTextField }).first(where: {
                  $0.stringValue == "정책을 선택하세요"
              }),
              let identityPrompt = views.compactMap({ $0 as? NSTextField }).first(where: {
                  $0.stringValue == "등록된 정책이 없어 identity가 없습니다."
              }),
              let inspectorScroll = title.enclosingScrollView,
              let document = inspectorScroll.documentView,
              let performanceButton = views.compactMap({ $0 as? NSButton }).first(where: {
                  $0.title == "성과에서 보기"
              }),
              let openRunButton = views.compactMap({ $0 as? NSButton }).first(where: {
                  $0.title == "선택 실행 열기"
              }) else {
            throw PolicyCatalogLayoutFailure.assertion("policy catalog inspector or navigation controls did not construct")
        }

        window.contentView?.layoutSubtreeIfNeeded()
        workspace.layoutSubtreeIfNeeded()
        window.displayIfNeeded()
        try expect(document.visibleRect.contains(rect(of: title, in: document)),
                   "no-selection inspector title is clipped at text scale \(textScale)")
        try expect(document.visibleRect.contains(rect(of: identityPrompt, in: document)),
                   "no-selection identity state is clipped at text scale \(textScale)")

        scrollInspectorToEnd(inspectorScroll, document: document, window: window)
        try expect(document.visibleRect.contains(rect(of: performanceButton, in: document)),
                   "performance navigation action is unreachable at text scale \(textScale)")
        try expect(document.visibleRect.contains(rect(of: openRunButton, in: document)),
                   "run navigation action is unreachable at text scale \(textScale)")

        window.setContentSize(NSSize(width: 1200, height: 720))
        root.setFrameSize(NSSize(width: 1200, height: 720))
        workspace.frame = NSRect(x: 230, y: 0, width: 970, height: 720)
        window.contentView?.layoutSubtreeIfNeeded()
        workspace.layoutSubtreeIfNeeded()
        window.displayIfNeeded()
        try expect(document.visibleRect.contains(rect(of: title, in: document)),
                   "inspector title is clipped after resize at text scale \(textScale)")
        try expect(!FileManager.default.fileExists(atPath: databaseURL.path),
                   "no-selection layout test opened or created a database")
    }

    private static func verifyPaperDecisionTextLayout() throws {
        TerminalTheme.setTextScale(1.0)
        let client = NativeMCPClient(session: URLSession(configuration: .ephemeral))
        let workspace = PaperTradingWorkspaceView(
            service: PaperSessionMCPService(client: client),
            realtimeFeedService: RealtimeFeedMCPService.shared,
            jobsQueueView: NSView(),
            onSummaryChanged: { _ in }
        )
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 1200, height: 800),
            styleMask: [.borderless],
            backing: .buffered,
            defer: true
        )
        window.isReleasedWhenClosed = false
        let root = NSView(frame: NSRect(x: 0, y: 0, width: 1200, height: 800))
        workspace.frame = NSRect(x: 230, y: 0, width: 970, height: 800)
        root.addSubview(workspace)
        window.contentView = root
        defer { window.close() }

        let views = descendants(of: workspace)
        guard let textView = views.compactMap({ $0 as? NSTextView }).first(where: {
                  $0.accessibilityLabel() == "엔진이 저장한 결정 행동, 조건 평가, 이유, 주문과 체결"
              }),
              let scrollView = textView.enclosingScrollView,
              let textContainer = textView.textContainer,
              let layoutManager = textView.layoutManager else {
            throw PolicyCatalogLayoutFailure.assertion("paper decision text view did not construct inside its scroll view")
        }

        textView.string = (1...32).map { "조건 \($0) · 저장 상태 TRUE · 합성 입력 123.456" }.joined(separator: "\n")
        window.contentView?.layoutSubtreeIfNeeded()
        workspace.layoutSubtreeIfNeeded()
        textView.layoutSubtreeIfNeeded()
        window.displayIfNeeded()
        layoutManager.ensureLayout(for: textContainer)

        let visibleClip = textView.convert(scrollView.contentView.bounds, from: scrollView.contentView)
        let usedGlyphRect = layoutManager.usedRect(for: textContainer)
        try expect(textView.frame.width > 0 && textView.frame.height > 0,
                   "paper decision text document view has empty bounds")
        try expect(visibleClip.width > 0 && visibleClip.height > 0,
                   "paper decision text scroll clip has empty bounds")
        try expect(usedGlyphRect.width > 0 && usedGlyphRect.height > 0
                    && usedGlyphRect.intersects(visibleClip),
                   "paper decision text has no laid-out glyphs in the visible clip")
    }

    private static func scrollInspectorToEnd(_ scrollView: NSScrollView, document: NSView, window: NSWindow) {
        let lastVisibleOrigin = max(0, document.bounds.maxY - scrollView.contentView.bounds.height)
        scrollView.contentView.scroll(to: NSPoint(x: document.bounds.minX, y: lastVisibleOrigin))
        scrollView.reflectScrolledClipView(scrollView.contentView)
        window.contentView?.layoutSubtreeIfNeeded()
        window.displayIfNeeded()
    }

    private static func descendants(of root: NSView) -> [NSView] {
        [root] + root.subviews.flatMap(descendants)
    }

    private static func rect(of view: NSView, in ancestor: NSView) -> NSRect {
        ancestor.convert(view.bounds, from: view)
    }

    private static func expect(_ condition: Bool, _ message: String) throws {
        guard condition else { throw PolicyCatalogLayoutFailure.assertion(message) }
    }
}
