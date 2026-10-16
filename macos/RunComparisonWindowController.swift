import AppKit

/// Thin native window for comparing an exact, bounded set of immutable runs.
final class RunComparisonWindowController: NSWindowController {
    private let comparisonView: StrategyPerformanceView

    init(
        reader: DatabaseReader,
        runs: [RealRun],
        policies: [RealPolicy],
        initialPolicyId: String? = nil,
        policyRevisionScopeId: String? = nil,
        onOpenRun: @escaping (RealRun, String) -> Void
    ) {
        var seen = Set<String>()
        let uniqueRuns = runs.filter { seen.insert($0.id).inserted }
        let boundedRuns = Array(uniqueRuns.prefix(4))
        let scopeIds = Set(boundedRuns.map(\.id))

        comparisonView = StrategyPerformanceView(
            reader: reader,
            policies: policies,
            initialPolicyId: initialPolicyId,
            runScopeIds: scopeIds,
            policyRevisionScopeId: policyRevisionScopeId,
            onOpenRun: onOpenRun,
            onPolicyChange: { _ in }
        )

        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 1320, height: 860),
            styleMask: [.titled, .closable, .miniaturizable, .resizable],
            backing: .buffered,
            defer: false
        )
        window.title = "선택 실행 비교 · \(boundedRuns.count)개"
        window.minSize = NSSize(width: 1_200, height: 760)
        window.isReleasedWhenClosed = false
        window.titlebarAppearsTransparent = true
        window.backgroundColor = TerminalTheme.surface
        window.contentView = comparisonView

        super.init(window: window)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) { fatalError("init(coder:) is unavailable") }

    func showComparison() {
        showWindow(nil)
        window?.center()
        window?.makeKeyAndOrderFront(nil)
    }
}
