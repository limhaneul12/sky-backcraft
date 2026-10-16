import Foundation

struct PolicyDeletePreview: Codable, Equatable {
    struct Resource: Codable, Equatable {
        let kind: String
        let policy_id: String
    }

    struct Blocker: Codable, Equatable {
        let `class`: String
        let reference: String
        let reason: String
    }

    struct CascadeGroup: Codable, Equatable {
        let kind: String
        let count: UInt64
        let examples: [String]
    }

    struct RetainedGroup: Codable, Equatable {
        let kind: String
        let count: UInt64
    }

    let resource: Resource
    let cascade_required: Bool
    let blockers: [Blocker]
    let cascade: [CascadeGroup]
    let retained_shared: [RetainedGroup]
    let reclaimable_file_bytes: UInt64?
    let exclusive_file_count: UInt64
    let db_row_estimate: UInt64
    let scope_digest: String
    let expires_at: String
    let confirmation_token: String
    let irreversible: Bool

    var echo: PolicyDeletePreviewEcho {
        PolicyDeletePreviewEcho(
            scope_digest: scope_digest,
            expires_at: expires_at,
            confirmation_token: confirmation_token
        )
    }

    var summary: String {
        let blockerSummary = blockers.map {
            "\($0.class) · \($0.reason) · reference \($0.reference)"
        }.joined(separator: "\n")
        let rowInventorySummary = cascade.map { "\($0.kind): \($0.count)" }.joined(separator: ", ")
        let retainedSummary = retained_shared.map { "\($0.kind): \($0.count)" }.joined(separator: ", ")
        return """
        정책 ID: \(resource.policy_id)
        리소스: \(resource.kind) · 되돌릴 수 없음: \(irreversible ? "예" : "아니요")
        참조 차단: \(blockers.isEmpty ? "없음" : "\(blockers.count)건")
        Cascade 필요: \(cascade_required ? "예" : "아니요") · 삭제 row inventory: \(rowInventorySummary.isEmpty ? "미제공" : rowInventorySummary)
        삭제 예상 DB 행: \(db_row_estimate) · 전용 파일: \(exclusive_file_count) · 회수 바이트: \(reclaimable_file_bytes.map(String.init) ?? "미측정")
        유지 공유 리소스: \(retainedSummary.isEmpty ? "없음" : retainedSummary)
        만료(UTC): \(expires_at)
        Scope digest: \(scope_digest)
        Token: 보유(표시 안 함)
        \(blockerSummary.isEmpty ? "" : "Blockers:\n\(blockerSummary)")
        """
    }
}

struct PolicyDeletePreviewEcho: Codable, Equatable {
    let scope_digest: String?
    let expires_at: String?
    let confirmation_token: String?
}

struct PolicyDeletePreviewValidator {
    private let fractionalTimestamp = ISO8601DateFormatter()
    private let wholeSecondTimestamp = ISO8601DateFormatter()

    init() {
        fractionalTimestamp.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        fractionalTimestamp.timeZone = TimeZone(secondsFromGMT: 0)
        wholeSecondTimestamp.formatOptions = [.withInternetDateTime]
        wholeSecondTimestamp.timeZone = TimeZone(secondsFromGMT: 0)
    }

    func expiryDate(_ value: String) -> Date? {
        fractionalTimestamp.date(from: value) ?? wholeSecondTimestamp.date(from: value)
    }

    func structuralBlocker(
        for preview: PolicyDeletePreview,
        expectedPolicyID: String,
        at now: Date
    ) -> String? {
        guard preview.resource.kind == "policy", preview.resource.policy_id == expectedPolicyID else {
            return "서버 Preview의 정책 ID가 선택 항목과 다릅니다."
        }
        guard preview.irreversible else { return "서버 Preview가 삭제를 되돌릴 수 없다고 표시하지 않았습니다." }
        guard preview.reclaimable_file_bytes != nil else {
            return "Preview 파일 범위를 알 수 없어 정책 삭제 요청을 차단합니다."
        }
        guard Self.isContentHash(preview.scope_digest), Self.isContentHash(preview.confirmation_token) else {
            return "Preview scope digest 또는 token 형식이 유효하지 않습니다."
        }
        guard let expiry = expiryDate(preview.expires_at) else {
            return "Preview 만료 시각을 UTC로 확인할 수 없습니다."
        }
        guard now < expiry else { return "Preview가 만료되어 새 Preview가 필요합니다." }
        return nil
    }

    func localEligibilityBlocker(for preview: PolicyDeletePreview) -> String? {
        guard preview.resource.kind == "policy" else {
            return "정책 리소스 Preview가 아닙니다."
        }
        guard !preview.cascade_required else {
            return "실제 cascade가 필요한 Preview는 policy-only 삭제 후보가 아닙니다."
        }
        guard preview.blockers.isEmpty else {
            return preview.blockers.map(\.reason).joined(separator: "; ")
        }
        guard preview.exclusive_file_count == 0, preview.reclaimable_file_bytes == 0 else {
            return "전용 파일 범위가 있어 policy-only 삭제 후보가 아닙니다."
        }
        let policyGroups = preview.cascade.filter { $0.kind == "policy" }
        let revisionGroups = preview.cascade.filter { $0.kind == "policy_revision" }
        guard policyGroups.count == 1,
              revisionGroups.count == 1,
              policyGroups[0].count == 1,
              policyGroups[0].examples == [preview.resource.policy_id],
              preview.cascade.count == policyGroups.count + revisionGroups.count else {
            return "Preview row inventory가 선택한 정책과 그 리비전만으로 구성되지 않았습니다."
        }
        let revisionGroup = revisionGroups[0]
        guard revisionGroup.count > 0,
              !revisionGroup.examples.isEmpty,
              UInt64(revisionGroup.examples.count) <= revisionGroup.count,
              revisionGroup.examples.allSatisfy({ !$0.isEmpty && $0 != preview.resource.policy_id }) else {
            return "Preview의 policy_revision row inventory가 유효하지 않습니다."
        }
        let rowCount = policyGroups[0].count.addingReportingOverflow(revisionGroup.count)
        guard !rowCount.overflow, rowCount.partialValue == preview.db_row_estimate else {
            return "Preview row inventory와 DB row estimate가 일치하지 않습니다."
        }
        return nil
    }

    static func isContentHash(_ value: String) -> Bool {
        value.count == 64 && value == value.lowercased() && value.utf8.allSatisfy {
            (48...57).contains($0) || (97...102).contains($0)
        }
    }
}
