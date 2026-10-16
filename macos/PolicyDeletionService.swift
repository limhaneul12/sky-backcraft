import Foundation

enum PolicyDeletionRequestState: String, Codable, Equatable {
    case pending = "PENDING"
    case complete = "COMPLETE"
    case partial = "PARTIAL"
    case interrupted = "INTERRUPTED"
}

enum PolicyDeletionItemStatus: String, Codable, Equatable {
    case pending = "PENDING"
    case deleted = "DELETED"
    case blocked = "BLOCKED"
    case stale = "STALE"
    case failed = "FAILED"
    case interruptedNotApplied = "INTERRUPTED_NOT_APPLIED"
}

struct PolicyDeletionCounts: Codable, Equatable {
    let deleted: UInt64
    let blocked: UInt64
    let stale: UInt64
    let failed: UInt64
    let retained: UInt64
    let interrupted_not_applied: UInt64
    let pending: UInt64
}

struct PolicyDeletionReadbackItem: Codable, Equatable {
    let policy_id: String
    let status: PolicyDeletionItemStatus
    let deleted_revision_count: UInt64
    let effect: String
    let preview_digest: String
    let preview_scope_digest: String
    let reason_code: String?
    let remediation_code: String?
    let retained: Bool
}

struct PolicyDeletionReadback: Codable, Equatable {
    let request_id: String
    let found: Bool
    let request_digest: String?
    let state: PolicyDeletionRequestState?
    let requested_count: UInt64?
    let items: [PolicyDeletionReadbackItem]?
    let counts: PolicyDeletionCounts?

    func validate(
        expectedRequestID: String,
        expectedItems: [PolicyDeletionExpectedItem]
    ) throws {
        guard request_id == expectedRequestID,
              (1...100).contains(expectedItems.count),
              Set(expectedItems.map(\.policy_id)).count == expectedItems.count else {
            throw PolicyDeletionServiceError.invalidResponse("request identity or expected item set")
        }

        guard found else {
            guard request_digest == nil, state == nil, requested_count == nil, items == nil, counts == nil else {
                throw PolicyDeletionServiceError.invalidResponse("NOT_FOUND response carried receipt fields")
            }
            return
        }

        guard let request_digest, PolicyDeletePreviewValidator.isContentHash(request_digest),
              let state, let requested_count, let items, let counts,
              requested_count == expectedItems.count,
              items.count == expectedItems.count else {
            throw PolicyDeletionServiceError.invalidResponse("incomplete policy deletion readback")
        }

        let sortedExpected = expectedItems.sorted { $0.policy_id < $1.policy_id }
        guard items.map(\.policy_id) == sortedExpected.map(\.policy_id) else {
            throw PolicyDeletionServiceError.invalidResponse("readback policy IDs or ordering differ from the submitted batch")
        }

        let expectedByID = Dictionary(uniqueKeysWithValues: sortedExpected.map { ($0.policy_id, $0) })
        for item in items {
            guard let expected = expectedByID[item.policy_id],
                  item.preview_scope_digest == expected.scope_digest,
                  PolicyDeletePreviewValidator.isContentHash(item.preview_scope_digest),
                  PolicyDeletePreviewValidator.isContentHash(item.preview_digest) else {
                throw PolicyDeletionServiceError.invalidResponse("readback preview scope echo differs from the submitted batch")
            }
            if item.status == .deleted {
                guard item.effect == "APPLIED", !item.retained else {
                    throw PolicyDeletionServiceError.invalidResponse("DELETED item has an inconsistent effect or retained flag")
                }
            } else {
                guard item.effect == "NOT_APPLIED", item.deleted_revision_count == 0 else {
                    throw PolicyDeletionServiceError.invalidResponse("no-effect item reports a deletion")
                }
            }
        }

        let deleted = UInt64(items.filter { $0.status == .deleted }.count)
        let blocked = UInt64(items.filter { $0.status == .blocked }.count)
        let stale = UInt64(items.filter { $0.status == .stale }.count)
        let failed = UInt64(items.filter { $0.status == .failed }.count)
        let interrupted = UInt64(items.filter { $0.status == .interruptedNotApplied }.count)
        let pending = UInt64(items.filter { $0.status == .pending }.count)
        let retained = UInt64(items.filter(\.retained).count)
        let partition = [deleted, blocked, stale, failed, interrupted, pending]
            .reduce((total: UInt64(0), overflow: false)) { result, value in
                let addition = result.total.addingReportingOverflow(value)
                return (addition.partialValue, result.overflow || addition.overflow)
            }
        guard counts.deleted == deleted,
              counts.blocked == blocked,
              counts.stale == stale,
              counts.failed == failed,
              counts.interrupted_not_applied == interrupted,
              counts.pending == pending,
              counts.retained == retained,
              !partition.overflow,
              partition.total == requested_count else {
            throw PolicyDeletionServiceError.invalidResponse("policy deletion counts do not partition requested_count")
        }

        let expectedState: PolicyDeletionRequestState
        if pending > 0 { expectedState = .pending }
        else if interrupted > 0 { expectedState = .interrupted }
        else if deleted == requested_count { expectedState = .complete }
        else { expectedState = .partial }
        guard state == expectedState else {
            throw PolicyDeletionServiceError.invalidResponse("policy deletion request state disagrees with item outcomes")
        }
    }
}

struct PolicyDeletionCapability: Codable, Equatable {
    let mode: String
    let max_items: UInt64
    let archive_available: Bool
    let request_readback: Bool
    let confirmation_required: Bool

    var permitsPolicyOnlyDelete: Bool {
        mode == "POLICY_ONLY_CONFIRMED"
            && max_items == 100
            && !archive_available
            && request_readback
            && confirmation_required
    }
}

private struct PolicyDeletionStatusResponse: Decodable {
    let policy_deletion: PolicyDeletionCapability?
}

struct PolicyDeletionWriteItem: Codable, Equatable {
    let policy_id: String
    let preview: PolicyDeletePreviewEcho?
}

struct PolicyDeletionWriteRequest: Codable, Equatable {
    let request_id: String
    let items: [PolicyDeletionWriteItem]
    let confirmation_text: String
}

enum PolicyDeletionServiceError: LocalizedError {
    case invalidRequest(String)
    case invalidResponse(String)
    case unknownOutcome(requestID: String, message: String)

    var errorDescription: String? {
        switch self {
        case .invalidRequest(let detail):
            return "정책 삭제 요청이 유효하지 않아 전송하지 않았습니다: \(detail)"
        case .invalidResponse(let detail):
            return "정책 삭제 응답을 확인할 수 없습니다: \(detail)"
        case .unknownOutcome(let requestID, let message):
            return "정책 삭제 요청(\(requestID)) 결과 미확인 · 같은 요청 ID readback 필요 · \(message)"
        }
    }
}

/// Desktop-only policy delete transport. Rust remains the eligibility and effect authority.
final class PolicyDeletionService {
    private let client: NativeMCPClient
    private let decoder = JSONDecoder()
    private let encoder = JSONEncoder()

    init(client: NativeMCPClient = .shared) {
        self.client = client
    }

    func currentTransportContext() -> NativeMCPTransportContext? {
        client.currentTransportContext()
    }

    func ownedHelperIsAbsent(
        in context: NativeMCPTransportContext?,
        expectedDataRootIdentity: String
    ) -> Bool {
        client.ownedHelperIsAbsent(in: context, expectedDataRootIdentity: expectedDataRootIdentity)
    }

    @discardableResult
    func loadCapability(
        completion: @escaping (Result<PolicyDeletionCapability?, Error>) -> Void
    ) -> NativeMCPRequest {
        client.call("lab_status", arguments: [:]) { [decoder] result in
            completion(result.flatMap { data in
                do {
                    return .success(try decoder.decode(PolicyDeletionStatusResponse.self, from: data).policy_deletion)
                } catch {
                    return .failure(PolicyDeletionServiceError.invalidResponse("lab_status.policy_deletion"))
                }
            })
        }
    }

    @discardableResult
    func readPreviews(
        policyIDs: [String],
        completion: @escaping (Result<[PolicyDeletePreview], Error>) -> Void
    ) -> NativeMCPRequest {
        guard (1...100).contains(policyIDs.count),
              policyIDs.allSatisfy({ !$0.isEmpty }),
              Set(policyIDs).count == policyIDs.count,
              policyIDs == policyIDs.sorted() else {
            completion(.failure(PolicyDeletionServiceError.invalidRequest("preview batch must contain 1–100 unique, sorted policy IDs")))
            return .alreadyFinished()
        }

        let sequence = PolicyDeletionReadSequence()
        func readNext(index: Int, previews: [PolicyDeletePreview]) {
            guard sequence.request.isPending else { return }
            guard policyIDs.indices.contains(index) else {
                if sequence.finish() { completion(.success(previews)) }
                return
            }
            let policyID = policyIDs[index]
            let child = client.call("resource_delete_preview", arguments: ["kind": "policy", "policy_id": policyID]) { [decoder] result in
                guard sequence.request.isPending else { return }
                do {
                    let preview = try decoder.decode(PolicyDeletePreview.self, from: result.get())
                    guard preview.resource.kind == "policy", preview.resource.policy_id == policyID else {
                        throw PolicyDeletionServiceError.invalidResponse("resource_delete_preview policy identity")
                    }
                    readNext(index: index + 1, previews: previews + [preview])
                } catch {
                    if sequence.finish() {
                        completion(.failure(error))
                    }
                }
            }
            sequence.replaceCurrent(child)
        }
        readNext(index: 0, previews: [])
        return sequence.request
    }

    func makeWriteRequest(
        requestID: String,
        expectedPolicyIDs: [String],
        previews: [PolicyDeletePreview],
        confirmationText: String,
        allowedSingleConfirmations: Set<String>,
        now: Date = Date()
    ) throws -> PolicyDeletionWriteRequest {
        guard (1...100).contains(expectedPolicyIDs.count),
              previews.count == expectedPolicyIDs.count,
              previews.map(\.resource.policy_id) == expectedPolicyIDs,
              expectedPolicyIDs == expectedPolicyIDs.sorted(),
              Set(expectedPolicyIDs).count == expectedPolicyIDs.count,
              previews.map(\.resource.policy_id) == previews.map(\.resource.policy_id).sorted(),
              Set(previews.map(\.resource.policy_id)).count == previews.count else {
            throw PolicyDeletionServiceError.invalidRequest("write batch must contain 1–100 unique, policy_id-sorted previews")
        }
        let validator = PolicyDeletePreviewValidator()
        for preview in previews {
            if let blocker = validator.structuralBlocker(
                for: preview,
                expectedPolicyID: preview.resource.policy_id,
                at: now
            ) {
                throw PolicyDeletionServiceError.invalidRequest(blocker)
            }
        }
        let confirmationMatches: Bool
        if previews.count == 1 {
            confirmationMatches = allowedSingleConfirmations.contains(confirmationText)
        } else {
            confirmationMatches = confirmationText == "DELETE \(previews.count) POLICIES"
        }
        guard confirmationMatches else {
            throw PolicyDeletionServiceError.invalidRequest("typed confirmation does not exactly match this batch")
        }
        guard let requestUUID = UUID(uuidString: requestID),
              requestUUID.uuidString.lowercased() == requestID else {
            throw PolicyDeletionServiceError.invalidRequest("request_id must be a lowercase UUID")
        }
        return PolicyDeletionWriteRequest(
            request_id: requestID,
            items: previews.map { preview in
                PolicyDeletionWriteItem(policy_id: preview.resource.policy_id, preview: preview.echo)
            },
            confirmation_text: confirmationText
        )
    }

    @discardableResult
    func submit(
        _ request: PolicyDeletionWriteRequest,
        expectedItems: [PolicyDeletionExpectedItem],
        allowedSingleConfirmations: Set<String>,
        expectedTransportContext: NativeMCPTransportContext? = nil,
        completion: @escaping (Result<PolicyDeletionReadback, Error>) -> Void
    ) -> NativeMCPRequest {
        do {
            try Self.validateRequest(request, expectedItems: expectedItems, allowedSingleConfirmations: allowedSingleConfirmations)
        } catch {
            completion(.failure(error))
            return .alreadyFinished()
        }

        let requestArguments: [String: Any]
        do { requestArguments = try arguments(for: request) }
        catch {
            completion(.failure(error))
            return .alreadyFinished()
        }

        let operation = NativeMCPRequest()
        let write = client.call(
            "policy_delete_write",
            arguments: requestArguments,
            expectedTransportContext: expectedTransportContext
        ) { [weak self] result in
            guard let self, operation.isPending else { return }
            do {
                let readback = try self.decoder.decode(PolicyDeletionReadback.self, from: result.get())
                try readback.validate(expectedRequestID: request.request_id, expectedItems: expectedItems)
                guard readback.found else {
                    throw PolicyDeletionServiceError.invalidResponse("write returned found=false")
                }
                guard operation.complete() else { return }
                completion(.success(readback))
            } catch {
                self.queryOnceAfterUncertainWrite(
                    requestID: request.request_id,
                    expectedItems: expectedItems,
                    cause: error,
                    operation: operation,
                    completion: completion
                )
            }
        }
        _ = operation.addChild(write)
        return operation
    }

    @discardableResult
    func query(
        requestID: String,
        expectedItems: [PolicyDeletionExpectedItem],
        completion: @escaping (Result<PolicyDeletionReadback, Error>) -> Void
    ) -> NativeMCPRequest {
        guard let uuid = UUID(uuidString: requestID), uuid.uuidString.lowercased() == requestID,
              (1...100).contains(expectedItems.count),
              Set(expectedItems.map(\.policy_id)).count == expectedItems.count else {
            completion(.failure(PolicyDeletionServiceError.invalidRequest("query request identity or expected item set")))
            return .alreadyFinished()
        }
        return client.call("policy_delete_query", arguments: ["request_id": requestID]) { [decoder] result in
            do {
                let readback = try decoder.decode(PolicyDeletionReadback.self, from: result.get())
                try readback.validate(expectedRequestID: requestID, expectedItems: expectedItems)
                completion(.success(readback))
            } catch {
                completion(.failure(error))
            }
        }
    }

    private func queryOnceAfterUncertainWrite(
        requestID: String,
        expectedItems: [PolicyDeletionExpectedItem],
        cause: Error,
        operation: NativeMCPRequest,
        completion: @escaping (Result<PolicyDeletionReadback, Error>) -> Void
    ) {
        let readbackRequest = client.call("policy_delete_query", arguments: ["request_id": requestID]) { [decoder] result in
            guard operation.isPending else { return }
            do {
                let readback = try decoder.decode(PolicyDeletionReadback.self, from: result.get())
                try readback.validate(expectedRequestID: requestID, expectedItems: expectedItems)
                guard readback.found else {
                    throw PolicyDeletionServiceError.unknownOutcome(
                        requestID: requestID,
                        message: "query returned NOT_FOUND; an earlier RPC may still arrive"
                    )
                }
                guard operation.complete() else { return }
                completion(.success(readback))
            } catch {
                guard operation.complete() else { return }
                completion(.failure(PolicyDeletionServiceError.unknownOutcome(
                    requestID: requestID,
                    message: "write response: \(cause.localizedDescription); one query failed: \(error.localizedDescription)"
                )))
            }
        }
        _ = operation.addChild(readbackRequest)
    }

    private func arguments(for request: PolicyDeletionWriteRequest) throws -> [String: Any] {
        let data = try encoder.encode(request)
        guard let object = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
            throw PolicyDeletionServiceError.invalidRequest("write request is not a JSON object")
        }
        return object
    }

    private static func validateRequest(
        _ request: PolicyDeletionWriteRequest,
        expectedItems: [PolicyDeletionExpectedItem],
        allowedSingleConfirmations: Set<String>
    ) throws {
        guard let uuid = UUID(uuidString: request.request_id), uuid.uuidString.lowercased() == request.request_id,
              (1...100).contains(request.items.count),
              request.items.map(\.policy_id) == request.items.map(\.policy_id).sorted(),
              Set(request.items.map(\.policy_id)).count == request.items.count,
              expectedItems.map(\.policy_id) == request.items.map(\.policy_id) else {
            throw PolicyDeletionServiceError.invalidRequest("write batch identity, order, or bound")
        }
        if request.items.count == 1 {
            guard allowedSingleConfirmations.contains(request.confirmation_text) else {
                throw PolicyDeletionServiceError.invalidRequest("single-item typed confirmation mismatch")
            }
        } else {
            guard request.confirmation_text == "DELETE \(request.items.count) POLICIES" else {
                throw PolicyDeletionServiceError.invalidRequest("multi-item typed confirmation mismatch")
            }
        }
        let expectedByID = Dictionary(uniqueKeysWithValues: expectedItems.map { ($0.policy_id, $0) })
        let validator = PolicyDeletePreviewValidator()
        let now = Date()
        for item in request.items {
            guard let expected = expectedByID[item.policy_id],
                  let preview = item.preview,
                  preview.scope_digest == expected.scope_digest,
                  PolicyDeletePreviewValidator.isContentHash(preview.scope_digest ?? ""),
                  PolicyDeletePreviewValidator.isContentHash(preview.confirmation_token ?? ""),
                  let expiresAt = preview.expires_at,
                  let expiry = validator.expiryDate(expiresAt),
                  now < expiry else {
                throw PolicyDeletionServiceError.invalidRequest("write preview echo does not match the persisted UI intent")
            }
        }
    }
}

private final class PolicyDeletionReadSequence {
    let request = NativeMCPRequest()
    private let lock = NSLock()
    private var current: NativeMCPRequest?

    init() {
        request.setCancellationHandler { [weak self] in self?.cancelCurrent() }
    }

    func replaceCurrent(_ next: NativeMCPRequest) {
        lock.lock()
        let cancelNext = !request.isPending
        if !cancelNext { current = next }
        lock.unlock()
        if cancelNext { next.cancel() }
    }

    func finish() -> Bool { request.complete() }

    private func cancelCurrent() {
        lock.lock()
        let current = self.current
        self.current = nil
        lock.unlock()
        current?.cancel()
    }
}
