import Foundation

enum HistoryMetricSort {
    case netReturn
    case maxDrawdown
}

struct HistoryMetricScope {
    let policyId: String
    let market: String
    let policyRevisionId: String?
}

enum HistoryMetricAvailability: Equatable {
    case available
    case missingModel
    case missingMetric(String? = nil)
    case ambiguousModels(Int)

    static var missingMetric: HistoryMetricAvailability {
        .missingMetric(nil)
    }

    var isMissingMetric: Bool {
        if case .missingMetric = self { return true }
        return false
    }

    var missingReason: String? {
        if case .missingMetric(let reason) = self { return reason }
        return nil
    }
}

struct HistoryMetricOrderingResult {
    let orderedRuns: [RealRun]
    let valuesByRunId: [String: Decimal]
    let availabilityByRunId: [String: HistoryMetricAvailability]
    let missingReasonsByRunId: [String: String]
    let unavailableCount: Int
    let ambiguousCount: Int
}

enum HistoryMetricOrderingError: Error, Equatable {
    case runLimitExceeded(actual: Int)
    case duplicateRunId(String)
    case foreignModelRunId(String)
    case invalidMetricValueReason(runId: String, modelId: String)
}

enum HistoryMetricOrdering {
    private struct RankedRun {
        let run: RealRun
        let value: Decimal?
        let availability: HistoryMetricAvailability
    }

    static func order(
        runs: [RealRun],
        models: [RealModelComparison],
        scope: HistoryMetricScope,
        metric: HistoryMetricSort,
        ascending: Bool
    ) throws -> HistoryMetricOrderingResult {
        guard runs.count <= 100 else {
            throw HistoryMetricOrderingError.runLimitExceeded(actual: runs.count)
        }

        var runIds = Set<String>()
        for run in runs {
            guard runIds.insert(run.id).inserted else {
                throw HistoryMetricOrderingError.duplicateRunId(run.id)
            }
        }
        for model in models where !runIds.contains(model.runId) {
            throw HistoryMetricOrderingError.foreignModelRunId(model.runId)
        }

        let modelsByRun = Dictionary(grouping: models, by: \.runId)
        let ranked = try runs.map { run -> RankedRun in
            let matches = (modelsByRun[run.id] ?? []).filter { model in
                model.policyId == scope.policyId &&
                    model.market == scope.market &&
                    (scope.policyRevisionId == nil || model.policyRevisionId == scope.policyRevisionId)
            }
            guard matches.count == 1, let model = matches.first else {
                return RankedRun(
                    run: run,
                    value: nil,
                    availability: matches.isEmpty ? .missingModel : .ambiguousModels(matches.count)
                )
            }
            let value: Decimal?
            let nullReason: String?
            switch metric {
            case .netReturn:
                value = model.netReturnDecimal
                nullReason = model.netReturnNullReason
            case .maxDrawdown:
                value = model.maxDrawdownDecimal
                nullReason = model.maxDrawdownNullReason
            }
            switch (value, nullReason) {
            case (.some(let metricValue), nil):
                guard !metricValue.isNaN, metricValue.isFinite else {
                    throw HistoryMetricOrderingError.invalidMetricValueReason(runId: run.id, modelId: model.modelId)
                }
                return RankedRun(run: run, value: metricValue, availability: .available)
            case (nil, .some(let reason)):
                guard DatabaseReader.allowedMetricNullReasons.contains(reason) else {
                    throw HistoryMetricOrderingError.invalidMetricValueReason(runId: run.id, modelId: model.modelId)
                }
                return RankedRun(run: run, value: nil, availability: .missingMetric(reason))
            case (nil, nil):
                return RankedRun(run: run, value: nil, availability: .missingMetric(nil))
            case (.some, .some):
                throw HistoryMetricOrderingError.invalidMetricValueReason(runId: run.id, modelId: model.modelId)
            }
        }

        let ordered = ranked.sorted { lhs, rhs in
            let lhsRank = availabilityRank(lhs.availability)
            let rhsRank = availabilityRank(rhs.availability)
            if lhsRank != rhsRank {
                return lhsRank < rhsRank
            }
            guard lhs.availability == .available,
                  let lhsValue = lhs.value,
                  let rhsValue = rhs.value else {
                return lhs.run.id < rhs.run.id
            }
            let comparison = NSDecimalNumber(decimal: lhsValue).compare(NSDecimalNumber(decimal: rhsValue))
            if comparison == .orderedSame { return lhs.run.id < rhs.run.id }
            return ascending ? comparison == .orderedAscending : comparison == .orderedDescending
        }

        let valuePairs: [(String, Decimal)] = ranked.compactMap { rankedRun -> (String, Decimal)? in
            guard rankedRun.availability == .available, let value = rankedRun.value else { return nil }
            return (rankedRun.run.id, value)
        }
        let valuesByRunId = Dictionary(uniqueKeysWithValues: valuePairs)
        let availabilityByRunId = Dictionary(
            uniqueKeysWithValues: ranked.map { ($0.run.id, $0.availability) }
        )
        let missingReasonPairs: [(String, String)] = ranked.compactMap { rankedRun in
            guard let reason = rankedRun.availability.missingReason, !reason.isEmpty else { return nil }
            return (rankedRun.run.id, reason)
        }
        let missingReasonsByRunId = Dictionary(uniqueKeysWithValues: missingReasonPairs)
        return HistoryMetricOrderingResult(
            orderedRuns: ordered.map(\.run),
            valuesByRunId: valuesByRunId,
            availabilityByRunId: availabilityByRunId,
            missingReasonsByRunId: missingReasonsByRunId,
            unavailableCount: ranked.lazy.filter {
                switch $0.availability {
                case .missingModel, .missingMetric: return true
                case .available, .ambiguousModels: return false
                }
            }.count,
            ambiguousCount: ranked.lazy.filter {
                if case .ambiguousModels = $0.availability { return true }
                return false
            }.count
        )
    }

    private static func availabilityRank(_ availability: HistoryMetricAvailability) -> Int {
        switch availability {
        case .available: return 0
        case .ambiguousModels: return 1
        case .missingModel, .missingMetric: return 2
        }
    }
}
