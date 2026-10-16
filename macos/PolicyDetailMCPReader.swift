import Foundation

enum PolicyDetailReadError: LocalizedError {
    case invalidResponse(String)
    case identityMismatch(String)
    case resourceLimit(String)

    var errorDescription: String? {
        switch self {
        case .invalidResponse(let detail): return "정책 응답 형식이 유효하지 않습니다: \(detail)"
        case .identityMismatch(let detail): return "정책 식별자가 서로 일치하지 않습니다: \(detail)"
        case .resourceLimit(let detail): return "정책 조회 범위를 초과했습니다: \(detail)"
        }
    }
}

struct PolicyRevisionReference: Equatable {
    let policyId: String
    let revisionId: String
    let definitionDigest: String

    var arguments: [String: Any] {
        ["policy_id": policyId, "revision_id": revisionId, "definition_digest": definitionDigest]
    }
}

struct PolicyRemoteHeader {
    let policyId: String
    let family: String
    let origin: String
    let createdAt: String
    let name: String
    let head: PolicyRevisionReference
    let revisionCount: Int
}

struct PolicyRevisionHistoryItem {
    let reference: PolicyRevisionReference
    let revisionNumber: Int
    let parentRevisionId: String?
    let name: String
    let createdAt: String
    let requestId: String
}

struct PolicyDefinitionSummary {
    let schemaVersion: String
    let name: String
    let description: String
    let programKind: String
    let strategyKind: String?
    let indicatorCount: Int?
    let stateCount: Int?
    let ruleCount: Int?
    let sourceIntervals: [String]
    let signalExpiry: String?
    let evidenceReferenced: Bool
    let exactJSON: String
}

struct PolicyDetailSnapshot {
    let header: PolicyRemoteHeader
    let reference: PolicyRevisionReference
    let revisionNumber: Int
    let parentRevisionId: String?
    let family: String
    let origin: String
    let createdAt: String
    let requestId: String
    let definition: PolicyDefinitionSummary
    let history: [PolicyRevisionHistoryItem]
}

/// Read-only adapter for the exact `policy_query` list/get/history contract.
final class PolicyDetailMCPReader {
    private let client: NativeMCPClient
    private let pageLimit = 100

    init(client: NativeMCPClient = .shared) { self.client = client }

    func fetchDetail(for policy: RealPolicy, completion: @escaping (Result<PolicyDetailSnapshot, Error>) -> Void) {
        fetchHeaders { [weak self] result in
            guard let self else { return }
            switch result {
            case .failure(let error): completion(.failure(error))
            case .success(let headers):
                guard let header = headers.first(where: { $0.policyId == policy.policyId }) else {
                    completion(.failure(PolicyDetailReadError.identityMismatch("로컬 정책이 Backend 목록에 없습니다: \(policy.policyId)")))
                    return
                }
                guard header.head.revisionId == policy.headRevisionId,
                      header.revisionCount == policy.revisionCount,
                      header.origin == policy.origin,
                      header.family == policy.family,
                      header.name == policy.name else {
                    completion(.failure(PolicyDetailReadError.identityMismatch("\(policy.policyId)의 head/name/origin/family/revision count")))
                    return
                }
                self.fetchRevision(reference: header.head) { revisionResult in
                    switch revisionResult {
                    case .failure(let error): completion(.failure(error))
                    case .success(let revision):
                        self.fetchHistory(policyId: policy.policyId, expectedCount: header.revisionCount) { historyResult in
                            completion(historyResult.flatMap { history in
                                do {
                                    guard let head = history.first, head.reference == header.head else {
                                        throw PolicyDetailReadError.identityMismatch("head revision/history")
                                    }
                                    return .success(try self.makeSnapshot(
                                        header: header, revision: revision, history: history, expected: head
                                    ))
                                }
                                catch { return .failure(error) }
                            })
                        }
                    }
                }
            }
        }
    }

    func fetchRevision(_ item: PolicyRevisionHistoryItem, within detail: PolicyDetailSnapshot,
                       completion: @escaping (Result<PolicyDetailSnapshot, Error>) -> Void) {
        guard detail.history.contains(where: {
            $0.reference == item.reference && $0.revisionNumber == item.revisionNumber
        }) else {
            completion(.failure(PolicyDetailReadError.identityMismatch("선택한 리비전이 검증된 history에 없습니다")))
            return
        }
        fetchRevision(reference: item.reference) { [weak self] result in
            guard let self else { return }
            completion(result.flatMap { revision in
                do {
                    return .success(try self.makeSnapshot(
                        header: detail.header, revision: revision, history: detail.history, expected: item
                    ))
                } catch { return .failure(error) }
            })
        }
    }

    private func fetchHeaders(completion: @escaping (Result<[PolicyRemoteHeader], Error>) -> Void) {
        client.call("policy_query", arguments: ["action": "list", "after_policy_id": NSNull(), "limit": pageLimit]) { result in
            completion(result.flatMap { data in
                do {
                    let root = try Self.object(data)
                    let items = try Self.pageItems(root, maximum: self.pageLimit)
                    let total = try Self.integer(root["total_count"], field: "total_count")
                    guard total == items.count, root["next_cursor"] is NSNull || root["next_cursor"] == nil else {
                        throw PolicyDetailReadError.resourceLimit("정책 목록이 단일 \(self.pageLimit)개 페이지를 초과합니다")
                    }
                    let headers = try items.map(Self.header)
                    guard Set(headers.map(\.policyId)).count == headers.count else {
                        throw PolicyDetailReadError.identityMismatch("중복 policy_id")
                    }
                    return .success(headers)
                } catch { return .failure(error) }
            })
        }
    }

    private func fetchRevision(reference: PolicyRevisionReference, completion: @escaping (Result<[String: Any], Error>) -> Void) {
        client.call("policy_query", arguments: ["action": "get", "reference": reference.arguments]) { result in
            completion(result.flatMap { data in
                do { return .success(try Self.object(data)) }
                catch { return .failure(error) }
            })
        }
    }

    private func fetchHistory(policyId: String, expectedCount: Int,
                              completion: @escaping (Result<[PolicyRevisionHistoryItem], Error>) -> Void) {
        guard expectedCount <= pageLimit else {
            completion(.failure(PolicyDetailReadError.resourceLimit("리비전 \(expectedCount)개는 화면 조회 상한 \(pageLimit)개를 초과합니다")))
            return
        }
        client.call("policy_query", arguments: [
            "action": "history", "policy_id": policyId,
            "before_revision_number": NSNull(), "limit": pageLimit
        ]) { result in
            completion(result.flatMap { data in
                do {
                    let root = try Self.object(data)
                    let items = try Self.pageItems(root, maximum: self.pageLimit)
                    let total = try Self.integer(root["total_count"], field: "total_count")
                    guard total == expectedCount, items.count == expectedCount,
                          root["next_cursor"] is NSNull || root["next_cursor"] == nil else {
                        throw PolicyDetailReadError.identityMismatch("정책 \(policyId)의 리비전 개수")
                    }
                    let history = try items.map(Self.historyItem)
                    let revisionIds = history.map { $0.reference.revisionId }
                    let requestIds = history.map(\.requestId)
                    guard Set(history.map { $0.reference.policyId }) == [policyId],
                          Set(history.map { $0.revisionNumber }).count == history.count,
                          Set(revisionIds).count == history.count,
                          Set(requestIds).count == history.count else {
                        throw PolicyDetailReadError.identityMismatch("정책 \(policyId)의 리비전 identity")
                    }
                    for (index, item) in history.enumerated() {
                        let expectedNumber = expectedCount - index
                        guard item.revisionNumber == expectedNumber else {
                            throw PolicyDetailReadError.identityMismatch("정책 \(policyId)의 revision_number 연속성")
                        }
                        if index + 1 < history.count {
                            guard item.parentRevisionId == history[index + 1].reference.revisionId else {
                                throw PolicyDetailReadError.identityMismatch("정책 \(policyId)의 parent revision 체인")
                            }
                        } else if item.parentRevisionId != nil {
                            throw PolicyDetailReadError.identityMismatch("정책 \(policyId)의 최초 revision parent")
                        }
                    }
                    return .success(history)
                } catch { return .failure(error) }
            })
        }
    }

    private func makeSnapshot(header: PolicyRemoteHeader, revision: [String: Any],
                              history: [PolicyRevisionHistoryItem],
                              expected: PolicyRevisionHistoryItem) throws -> PolicyDetailSnapshot {
        let snapshot = try Self.dictionary(revision["snapshot"], field: "snapshot")
        let reference = try Self.reference(snapshot["reference"])
        guard reference == expected.reference else {
            throw PolicyDetailReadError.identityMismatch("selected revision reference")
        }
        let family = try Self.string(snapshot["family"], field: "family")
        let origin = try Self.string(snapshot["origin"], field: "origin")
        guard family == header.family, origin == header.origin else {
            throw PolicyDetailReadError.identityMismatch("revision family/origin")
        }
        let definition = try Self.definition(snapshot["definition"])
        let revisionNumber = try Self.integer(snapshot["revision_number"], field: "revision_number")
        let parentRevisionId = snapshot["parent_revision_id"] as? String
        let createdAt = try Self.utcTimestamp(revision["created_at"], field: "created_at")
        let requestId = try Self.string(revision["request_id"], field: "request_id")
        guard definition.name == expected.name,
              revisionNumber == expected.revisionNumber,
              parentRevisionId == expected.parentRevisionId,
              createdAt == expected.createdAt,
              requestId == expected.requestId,
              history.first?.reference == header.head else {
            throw PolicyDetailReadError.identityMismatch("selected revision summary/body")
        }
        return PolicyDetailSnapshot(
            header: header,
            reference: reference,
            revisionNumber: revisionNumber,
            parentRevisionId: parentRevisionId,
            family: family,
            origin: origin,
            createdAt: createdAt,
            requestId: requestId,
            definition: definition,
            history: history
        )
    }

    private static func object(_ data: Data) throws -> [String: Any] {
        guard data.count <= 4_194_304,
              let value = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
            throw PolicyDetailReadError.invalidResponse("JSON object")
        }
        return value
    }

    private static func pageItems(_ root: [String: Any], maximum: Int) throws -> [[String: Any]] {
        guard let items = root["items"] as? [[String: Any]], items.count <= maximum else {
            throw PolicyDetailReadError.invalidResponse("items")
        }
        guard try integer(root["returned_count"], field: "returned_count") == items.count else {
            throw PolicyDetailReadError.identityMismatch("returned_count")
        }
        return items
    }

    private static func header(_ value: [String: Any]) throws -> PolicyRemoteHeader {
        let policyId = try string(value["policy_id"], field: "policy_id")
        let head = try reference(value["head"])
        guard head.policyId == policyId else { throw PolicyDetailReadError.identityMismatch("header/head policy_id") }
        let createdAt = try utcTimestamp(value["created_at"], field: "created_at")
        return PolicyRemoteHeader(
            policyId: policyId,
            family: try string(value["family"], field: "family"),
            origin: try string(value["origin"], field: "origin"),
            createdAt: createdAt,
            name: try string(value["name"], field: "name"),
            head: head,
            revisionCount: try integer(value["revision_count"], field: "revision_count")
        )
    }

    private static func historyItem(_ value: [String: Any]) throws -> PolicyRevisionHistoryItem {
        PolicyRevisionHistoryItem(
            reference: try reference(value["reference"]),
            revisionNumber: try integer(value["revision_number"], field: "revision_number"),
            parentRevisionId: value["parent_revision_id"] as? String,
            name: try string(value["name"], field: "name"),
            createdAt: try utcTimestamp(value["created_at"], field: "created_at"),
            requestId: try string(value["request_id"], field: "request_id")
        )
    }

    private static func reference(_ value: Any?) throws -> PolicyRevisionReference {
        let object = try dictionary(value, field: "reference")
        let digest = try string(object["definition_digest"], field: "definition_digest")
        guard digest.count == 64, digest.allSatisfy({ $0.isHexDigit }) else {
            throw PolicyDetailReadError.invalidResponse("definition_digest")
        }
        return PolicyRevisionReference(
            policyId: try string(object["policy_id"], field: "policy_id"),
            revisionId: try string(object["revision_id"], field: "revision_id"),
            definitionDigest: digest
        )
    }

    private static func definition(_ value: Any?) throws -> PolicyDefinitionSummary {
        let object = try dictionary(value, field: "definition")
        let program = try dictionary(object["program"], field: "program")
        let kind = try string(program["kind"], field: "program.kind")
        let sourceIntervals: [String]
        let indicatorCount: Int?
        let stateCount: Int?
        let ruleCount: Int?
        let strategyKind: String?
        let expiry: String?
        let evidenceReferenced: Bool
        if kind == "RULES" {
            let rulesProgram = try dictionary(program["program"], field: "program.program")
            let indicators = rulesProgram["indicators"] as? [[String: Any]] ?? []
            indicatorCount = indicators.count
            stateCount = (rulesProgram["states"] as? [Any])?.count
            ruleCount = (rulesProgram["rules"] as? [Any])?.count
            sourceIntervals = Array(Set(indicators.compactMap { $0["source_interval"] as? String })).sorted()
            strategyKind = nil
            if let signal = rulesProgram["signal_expiry"] {
                expiry = compactJSON(signal)
            } else { expiry = nil }
            evidenceReferenced = compactJSON(rulesProgram)?.contains("EVIDENCE_") == true
        } else if kind == "BUILTIN" {
            let strategy = try dictionary(program["strategy"], field: "program.strategy")
            strategyKind = strategy["kind"] as? String
            indicatorCount = nil; stateCount = nil; ruleCount = nil; sourceIntervals = []; expiry = nil
            evidenceReferenced = ["S5", "S1_COVERAGE_CONTROL"].contains(strategyKind ?? "")
        } else {
            throw PolicyDetailReadError.invalidResponse("unknown program kind \(kind)")
        }
        let data = try JSONSerialization.data(withJSONObject: object, options: [.prettyPrinted, .sortedKeys])
        guard data.count <= 65_536, let exactJSON = String(data: data, encoding: .utf8) else {
            throw PolicyDetailReadError.resourceLimit("정책 정의가 64 KiB를 초과합니다")
        }
        return PolicyDefinitionSummary(
            schemaVersion: try string(object["schema_version"], field: "schema_version"),
            name: try string(object["name"], field: "name"),
            description: try string(object["description"], field: "description"),
            programKind: kind,
            strategyKind: strategyKind,
            indicatorCount: indicatorCount,
            stateCount: stateCount,
            ruleCount: ruleCount,
            sourceIntervals: sourceIntervals,
            signalExpiry: expiry,
            evidenceReferenced: evidenceReferenced,
            exactJSON: exactJSON
        )
    }

    private static func compactJSON(_ value: Any) -> String? {
        guard JSONSerialization.isValidJSONObject(value),
              let data = try? JSONSerialization.data(withJSONObject: value, options: [.sortedKeys]) else { return nil }
        return String(data: data, encoding: .utf8)
    }

    private static func dictionary(_ value: Any?, field: String) throws -> [String: Any] {
        guard let value = value as? [String: Any] else { throw PolicyDetailReadError.invalidResponse(field) }
        return value
    }

    private static func string(_ value: Any?, field: String) throws -> String {
        guard let value = value as? String, !value.isEmpty else { throw PolicyDetailReadError.invalidResponse(field) }
        return value
    }

    private static func utcTimestamp(_ value: Any?, field: String) throws -> String {
        let raw = try string(value, field: field)
        let regular = ISO8601DateFormatter()
        if regular.date(from: raw) != nil { return raw }
        let fractional = ISO8601DateFormatter()
        fractional.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        guard fractional.date(from: raw) != nil else {
            throw PolicyDetailReadError.invalidResponse(field)
        }
        return raw
    }

    private static func integer(_ value: Any?, field: String) throws -> Int {
        guard let number = value as? NSNumber else { throw PolicyDetailReadError.invalidResponse(field) }
        let integer = number.intValue
        guard integer >= 0, number.doubleValue == Double(integer) else {
            throw PolicyDetailReadError.invalidResponse(field)
        }
        return integer
    }
}
