import Foundation
import AppKit
import SQLite3

private let dashboardHistorySQLiteTransient = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

@main
private enum AutomationProtocolRegression {
    static func main() throws {
        guard let fixturePath = ProcessInfo.processInfo.environment["SKY_BACKCRAFT_AUTOMATION_WIRE_FIXTURE"] else {
            throw Failure.assertion("SKY_BACKCRAFT_AUTOMATION_WIRE_FIXTURE is required")
        }
        let fixtureData = try Data(contentsOf: URL(fileURLWithPath: fixturePath))
        guard let fixture = try JSONSerialization.jsonObject(with: fixtureData) as? [String: Any] else {
            throw Failure.assertion("wire fixture root must be an object")
        }
        guard let paperEventFixturePath = ProcessInfo.processInfo.environment["SKY_BACKCRAFT_PAPER_EVENT_FIXTURE"] else {
            throw Failure.assertion("SKY_BACKCRAFT_PAPER_EVENT_FIXTURE is required")
        }

        try startRequestMatchesFixture(fixture)
        try validateLiveAutomationContracts(fixture)
        try lifecycleAndQueryRequestsMatchFixture(fixture)
        try decodeFrozenReadProjections(fixture)
        try validatePaperSessionEvents(at: URL(fileURLWithPath: paperEventFixturePath))
        try validatePaperReadOnlyReviewEvents()
        try validatePaperReviewRetryCancellationAndTransportFence()
        try validateRiskInputBoundaries()
        try validateBaselineSourceChoices()
        try validateBaselineStatusConsistency(fixture)
        try validateLegacyMockBaselineProjection(fixture)
        try validateAccountIdentityScopes()
        try validateEventDetailAmountMasking()
        try validateAutomationReviewContracts()
        try validateRecordedDecisionPresentation()
        try validatePaperRecordedDecisionReview()
        if let reviewWirePath = ProcessInfo.processInfo.environment["SKY_BACKCRAFT_AUTOMATION_REVIEW_RUST_WIRE_FIXTURE"] {
            try validateActualRustAutomationReviewWire(at: URL(fileURLWithPath: reviewWirePath))
        }
        try validateReadbackStateAndMutationIdentity(fixture)
        try validateExpandedAutomationEnums()
        try validatePaperWriteReadbackRecovery()
#if PAPER_MARKET_TIMING_REGRESSION
        try validatePaperMarketTimingDetails()
#endif
#if POLICY_DELETION_REGRESSION
        try validateLiveOrderConfirmationDialog()
        try validateSettingsSaveOnlyRestartsForChangedNetworkConfiguration()
        try validateSettingsEmergencyStopGateAndControlRace()
        try validateSettingsAccountStrategyAdmission()
        try validateDashboardHistoryColumnsAndStatusLabels()
        try validateStrategyChartReadPacing()
        try validateStrategyBatchRecordAssembly()
        try validateStrategyRefreshFailureRetainsCachedPage()
        try validateLedgerMCPReaderCancellationOwnership()
        try validateExplicitPaperFeedGapRecoveryResume()
        guard let policyDeletionFixturePath = ProcessInfo.processInfo.environment["SKY_BACKCRAFT_POLICY_DELETION_WIRE_FIXTURE"] else {
            throw Failure.assertion("SKY_BACKCRAFT_POLICY_DELETION_WIRE_FIXTURE is required")
        }
        guard let actualPolicyPreviewFixturePath = ProcessInfo.processInfo.environment["SKY_BACKCRAFT_POLICY_DELETE_PREVIEW_FIXTURE"] else {
            throw Failure.assertion("SKY_BACKCRAFT_POLICY_DELETE_PREVIEW_FIXTURE is required")
        }
        try validatePolicyDeletionSelectionState()
        try validatePolicyDeletionCheckboxRendering()
        try validatePolicyDeletionCompactLayout()
        try validateDashboardDatabaseStatusFooter()
        try validateActualPolicyPreviewRowInventory(at: URL(fileURLWithPath: actualPolicyPreviewFixturePath))
        try validatePolicyDeletionWireExamples(at: URL(fileURLWithPath: policyDeletionFixturePath))
        try validatePolicyDeletionWritePreflight()
        try validatePolicyDeletionIntentPersistence()
        try validatePolicyDeletionTransportContext()
        try validatePolicyDeletionTransport(at: URL(fileURLWithPath: policyDeletionFixturePath))
#endif
        print("PASS native automation wire contract")
    }

    private static func validateAutomationReviewContracts() throws {
        let sessionID = "automation-review-fixture"
        let intentID = "review-intent-001"
        let observationID = "review-observation-001"
        let digest = String(repeating: "a", count: 64)
        let policyReference: [String: Any] = [
            "policy_id": "review-policy",
            "revision_id": "review-revision-1",
            "definition_digest": digest
        ]
        let decisionsArguments = AutomationSessionWire.reviewDecisionsArguments(
            sessionID: sessionID,
            offset: 50,
            limit: 50
        )
        guard decisionsArguments["action"] as? String == "decisions",
              decisionsArguments["session_id"] as? String == sessionID,
              decisionsArguments["offset"] as? UInt32 == 50,
              decisionsArguments["limit"] as? UInt32 == 50,
              AutomationSessionWire.reviewDecisionArguments(sessionID: sessionID, intentID: intentID)["action"] as? String == "decision",
              AutomationSessionWire.reviewPerformanceArguments(sessionID: sessionID)["action"] as? String == "performance" else {
            throw Failure.assertion("review MCP query action or pagination arguments changed")
        }

        let pageObject: [String: Any] = [
            "session_id": sessionID,
            "mode": "MOCK",
            "policy_ref": policyReference,
            "items": [[
                "intent_id": intentID,
                "observation_id": observationID,
                "decision_time_utc": "2026-10-14T01:00:00Z",
                "action": "BUY",
                "state": "LONG"
            ]],
            "returned_count": 1,
            "total_count": 52,
            "next_offset": 51
        ]
        let page = try JSONDecoder().decode(
            AutomationReviewDecisionPage.self,
            from: JSONSerialization.data(withJSONObject: pageObject)
        )
        try AutomationSessionMCPService.validateReviewDecisionPage(page, sessionID: sessionID, offset: 50, limit: 50)
        guard page.items.first?.action == .buy, page.next_offset == 51 else {
            throw Failure.assertion("review decision timeline page did not decode its action and next offset")
        }
        var wrongPage = pageObject
        wrongPage["session_id"] = "another-session"
        let mismatchedPage = try JSONDecoder().decode(
            AutomationReviewDecisionPage.self,
            from: JSONSerialization.data(withJSONObject: wrongPage)
        )
        try expectReviewRejection("review page from another session was accepted") {
            try AutomationSessionMCPService.validateReviewDecisionPage(
                mismatchedPage,
                sessionID: sessionID,
                offset: 50,
                limit: 50
            )
        }

        func candle(_ id: String, close: String, open: String, closeTime: String) -> [String: Any] {
            [
                "id": id,
                "candle": [
                    "market": "KRW-BTC",
                    "interval": "h1",
                    "open_time_utc": closeTime == "2026-10-14T00:00:00Z" ? "2026-10-13T23:00:00Z" : "2026-10-14T00:00:00Z",
                    "close_time_utc": closeTime,
                    "open": open,
                    "high": "124.5",
                    "low": "119.5",
                    "close": close,
                    "volume": "0.25",
                    "quote_turnover": "30.75",
                    "completed": true
                ],
                "content_digest": digest,
                "raw_object_ids": [],
                "constituent_ids": []
            ]
        }
        let selectedCandle = candle(observationID, close: "123.25", open: "121.00", closeTime: "2026-10-14T01:00:00Z")
        let evaluation: [String: Any] = [
            "decision_time_utc": "2026-10-14T01:00:00Z",
            "state_before": "CASH",
            "state_after": "LONG",
            "raw_target_weight": "1",
            "constrained_target_weight": "1",
            "reasons": ["BAND_ENTRY"],
            "indicators": [
                "version": "strategy-v1",
                "ema": 122.75,
                "bar_sigma": 0.015,
                "annual_vol": 0.73,
                "rsi": NSNull(),
                "entry_high": NSNull(),
                "exit_low": NSNull(),
                "completed_bars_seen": 24,
                "policy_values": []
            ],
            "policy_trace": [
                "matched_rule_id": "entry_rule",
                "state_before": [],
                "state_after": [],
                "evidence": NSNull()
            ]
        ]
        let session: [String: Any] = [
            "session_id": sessionID,
            "mode": "MOCK",
            "state": "RUNNING",
            "policy_ref": policyReference,
            "market": "KRW-BTC",
            "decision_interval": "h1",
            "risk": [
                "max_order_notional_krw": "100000",
                "max_daily_notional_krw": "1000000",
                "max_asset_exposure_krw": "500000",
                "max_daily_loss_krw": "50000",
                "max_open_orders": 2,
                "max_orders_per_interval": 1,
                "max_balance_age_seconds": 30,
                "max_feed_age_seconds": 90
            ],
            "created_at_utc": "2026-10-14T00:00:00Z",
            "updated_at_utc": "2026-10-14T01:00:00Z"
        ]
        let detailObject: [String: Any] = [
            "session": session,
            "frozen_policy": [
                "reference": policyReference,
                "revision_number": 1,
                "parent_revision_id": NSNull(),
                "family": "S1",
                "origin": "USER",
                "definition": [
                    "schema_version": "1.0",
                    "name": "Synthetic review strategy",
                    "description": "Fixture only",
                    "program": ["kind": "BUILTIN", "strategy": ["family": "S1"]]
                ]
            ],
            "decision": [
                "session_id": sessionID,
                "intent_id": intentID,
                "observation_id": observationID,
                "decision_time_utc": "2026-10-14T01:00:00Z",
                "recorded_at_utc": "2026-10-14T01:00:02Z",
                "action": "BUY",
                "state": "LONG",
                "candle": selectedCandle,
                "evaluation": evaluation,
                "evaluation_digest": String(repeating: "b", count: 64)
            ],
            "context_candles": [
                candle("review-observation-previous", close: "121.00", open: "120.00", closeTime: "2026-10-14T00:00:00Z")
            ],
            "orders": [[
                "client_identifier": "review-order-001",
                "intent_id": intentID,
                "market": "KRW-BTC",
                "side": "BID",
                "state": "FILLED",
                "venue_order_uuid": "mock-order-001",
                "requested_notional_krw": "100",
                "reserved_fee_krw": "0.05",
                "executed_volume": "0.0008",
                "paid_fee_krw": "0.05",
                "locked_quote_krw": "0",
                "locked_base": NSNull(),
                "trade_count": 1,
                "created_at_utc": "2026-10-14T01:00:02Z",
                "updated_at_utc": "2026-10-14T01:00:03Z"
            ]],
            "events": [[
                "session_id": sessionID,
                "sequence": 1,
                "kind": "INTENT_REJECTED",
                "at_utc": "2026-10-14T01:00:02Z",
                "detail": ["reason": "RISK_LIMIT", "intent_id": intentID]
            ]],
            "recorded_decision": [
                "schema_version": "1.0",
                "policy_ref": policyReference,
                "decision_time": "2026-10-14T01:00:00Z",
                "strategy_action": "BUY",
                "constrained_action": "BUY",
                "execution_state": "REJECTED",
                "actual_weight_at_decision": "0.0000",
                "raw_target_weight": "1.0000",
                "constrained_target_weight": "0.0000",
                "matched_rule_id": "entry_rule",
                "condition_trace_capture": "CAPTURED",
                "condition_trace": [[
                    "scope": ["kind": "RULE_CONDITION", "rule_id": "entry_rule"],
                    "ast_path": "rule:entry_rule:when",
                    "role": "ENTRY_CONDITION",
                    "outcome": ["kind": "FALSE"],
                    "engine_result": false,
                    "evidence": [
                        "kind": "COMPARISON",
                        "left": ["value": "1.12", "unit": "ratio"],
                        "comparison": "GREATER_THAN",
                        "right": ["value": "1.5", "unit": "ratio"]
                    ]
                ]],
                "reasons": [["stage": "ADMISSION", "code": "RISK_LIMIT", "label_ko": "위험 한도 초과"]],
                "orders": [["order_id": "review-order-001", "state": "REJECTED", "reason": "RISK_LIMIT"]],
                "fills": []
            ]
        ]
        let detail = try JSONDecoder().decode(
            AutomationReviewDecisionDetail.self,
            from: JSONSerialization.data(withJSONObject: detailObject)
        )
        var oldDetailObject = detailObject
        oldDetailObject.removeValue(forKey: "recorded_decision")
        let oldDetail = try JSONDecoder().decode(
            AutomationReviewDecisionDetail.self,
            from: JSONSerialization.data(withJSONObject: oldDetailObject)
        )
        let recordedDecisionText = RecordedDecisionPresentation.lines(
            for: detail.recorded_decision,
            masked: false
        ).joined(separator: "\n")
        guard oldDetail.recorded_decision == nil,
              recordedDecisionText.contains("매수 (BUY)"),
              recordedDecisionText.contains("거부 (REJECTED)"),
              recordedDecisionText.contains("불충족 (FALSE)"),
              recordedDecisionText.contains("1.12"), recordedDecisionText.contains("1.5"),
              recordedDecisionText.contains("위험 한도 초과") else {
            throw Failure.assertion("automation decision wire did not preserve new captured evidence and old missing capture")
        }
        let reference = AutomationPolicyRevisionReference(
            policy_id: "review-policy",
            revision_id: "review-revision-1",
            definition_digest: digest
        )
        try AutomationSessionMCPService.validateReviewDecisionDetail(
            detail,
            sessionID: sessionID,
            intentID: intentID,
            expectedMode: .mock,
            expectedPolicyReference: reference
        )
        try AutomationSessionMCPService.validateReviewDecisionMatchesSummary(
            detail,
            summary: page.items[0]
        )
        guard detail.decision.candle.candle.close == "123.25",
              detail.orders.first?.client_identifier == "review-order-001",
              detail.orders.first?.requested_notional_krw == "100",
              detail.context_candles.count == 1,
              detail.decision.evaluation.formattedJSON.contains("BAND_ENTRY") else {
            throw Failure.assertion("review detail lost the saved candle, context, or original decision reason")
        }

        var wrongObservationDetail = detailObject
        var wrongDecision = try object(wrongObservationDetail["decision"], field: "review decision")
        wrongDecision["observation_id"] = "another-observation"
        wrongObservationDetail["decision"] = wrongDecision
        let mismatchedDetail = try JSONDecoder().decode(
            AutomationReviewDecisionDetail.self,
            from: JSONSerialization.data(withJSONObject: wrongObservationDetail)
        )
        try expectReviewRejection("review decision with a mismatched observation link was accepted") {
            try AutomationSessionMCPService.validateReviewDecisionDetail(
                mismatchedDetail,
                sessionID: sessionID,
                intentID: intentID,
                expectedMode: .mock,
                expectedPolicyReference: reference
            )
        }
        var mismatchedRecordedPolicyObject = detailObject
        var mismatchedRecordedDecision = try object(
            mismatchedRecordedPolicyObject["recorded_decision"],
            field: "recorded decision"
        )
        var mismatchedRecordedReference = try object(
            mismatchedRecordedDecision["policy_ref"],
            field: "recorded decision policy reference"
        )
        mismatchedRecordedReference["revision_id"] = "another-revision"
        mismatchedRecordedDecision["policy_ref"] = mismatchedRecordedReference
        mismatchedRecordedPolicyObject["recorded_decision"] = mismatchedRecordedDecision
        let mismatchedRecordedPolicy = try JSONDecoder().decode(
            AutomationReviewDecisionDetail.self,
            from: JSONSerialization.data(withJSONObject: mismatchedRecordedPolicyObject)
        )
        try expectReviewRejection("recorded decision from another policy revision was accepted") {
            try AutomationSessionMCPService.validateReviewDecisionDetail(
                mismatchedRecordedPolicy,
                sessionID: sessionID,
                intentID: intentID,
                expectedMode: .mock,
                expectedPolicyReference: reference
            )
        }
        let selectedSummary = page.items[0]
        let mismatchedSummary = AutomationReviewDecisionSummary(
            intent_id: selectedSummary.intent_id,
            observation_id: selectedSummary.observation_id,
            decision_time_utc: selectedSummary.decision_time_utc,
            action: selectedSummary.action,
            state: "CASH"
        )
        try expectReviewRejection("detail response that differed from the selected timeline row was accepted") {
            try AutomationSessionMCPService.validateReviewDecisionMatchesSummary(
                detail,
                summary: mismatchedSummary
            )
        }

        let privateEvaluation = detail.decision.evaluation
        let maskedEvaluation = AutomationReviewPrivacyPresentation.evaluation(privateEvaluation, masked: true)
        let visibleEvaluation = AutomationReviewPrivacyPresentation.evaluation(privateEvaluation, masked: false)
        let maskedRecordedDecision = RecordedDecisionPresentation.lines(
            for: detail.recorded_decision,
            masked: true
        ).joined(separator: "\n")
        guard AutomationReviewPrivacyPresentation.identity(sessionID, masked: true) == AutomationReviewPrivacyPresentation.hiddenText,
              AutomationReviewPrivacyPresentation.exactValue("123.25", masked: true) == AutomationReviewPrivacyPresentation.hiddenText,
              !maskedEvaluation.contains("entry_rule"), !maskedEvaluation.contains("123.25"),
              maskedRecordedDecision.contains("개인정보 숨김"), !maskedRecordedDecision.contains("review-order-001"),
              visibleEvaluation.contains("BAND_ENTRY"), visibleEvaluation.contains("122.75") else {
            throw Failure.assertion("privacy masking must hide identities, indicators, and raw evaluation while visible review preserves them")
        }

        let reportObject: [String: Any] = [
            "schema_version": "1.0",
            "session": session,
            "snapshot_digest": digest,
            "snapshot_at_utc": "2026-10-14T01:00:02Z",
            "decision_count": 1,
            "warmup_record_count": 0,
            "buy_signal_count": 1,
            "sell_signal_count": 0,
            "hold_signal_count": 0,
            "rejected_intent_count": 1,
            "order_count": 0,
            "fill_count": 0,
            "buy_funds_krw": "0",
            "sell_funds_krw": "0",
            "paid_fees_krw": "0",
            "net_quote_cashflow_krw": "0",
            "realized_pnl_krw": ["value": NSNull(), "unit": "KRW", "unavailable_reason": "no fills"],
            "unrealized_pnl_krw": ["value": NSNull(), "unit": "KRW", "unavailable_reason": "no mark"],
            "total_pnl_krw": ["value": NSNull(), "unit": "KRW", "unavailable_reason": "no attributable equity series"],
            "return_ratio": ["value": NSNull(), "unit": "ratio", "unavailable_reason": "no attributable capital"],
            "max_drawdown_ratio": ["value": NSNull(), "unit": "ratio", "unavailable_reason": "no attributable equity series"],
            "remaining_volume": NSNull(),
            "remaining_cost_krw": NSNull(),
            "mark_price_krw": NSNull(),
            "mark_at_utc": NSNull(),
            "mark_observation_id": NSNull(),
            "mark_content_digest": NSNull(),
            "opening_inventory": NSNull(),
            "methodology": "Session-owned cashflow and weighted average cost; no account-wide return is inferred.",
            "warnings": ["MOCK data only"]
        ]
        let report = try JSONDecoder().decode(
            AutomationPerformanceReport.self,
            from: JSONSerialization.data(withJSONObject: reportObject)
        )
        try AutomationSessionMCPService.validateAutomationPerformance(
            report,
            sessionID: sessionID,
            expectedMode: .mock,
            expectedPolicyReference: reference
        )
        guard report.net_quote_cashflow_krw == "0",
              report.total_pnl_krw.value == nil,
              report.total_pnl_krw.unavailable_reason == "no attributable equity series",
              report.opening_inventory == nil else {
            throw Failure.assertion("performance report must preserve exact zero cashflow, unavailable P&L, and unknown opening inventory")
        }
        var zeroEvidenceObject = reportObject
        zeroEvidenceObject["mark_price_krw"] = "123.25"
        zeroEvidenceObject["mark_at_utc"] = "2026-10-14T01:00:02Z"
        zeroEvidenceObject["mark_observation_id"] = observationID
        zeroEvidenceObject["mark_content_digest"] = digest
        zeroEvidenceObject["opening_inventory"] = [
            "snapshot_sequence": 7,
            "source_mode": "MOCK",
            "observed_at_utc": "2026-10-14T00:00:00Z",
            "base_quantity": "0",
            "balances_digest": digest
        ]
        let zeroEvidenceReport = try JSONDecoder().decode(
            AutomationPerformanceReport.self,
            from: JSONSerialization.data(withJSONObject: zeroEvidenceObject)
        )
        try AutomationSessionMCPService.validateAutomationPerformance(
            zeroEvidenceReport,
            sessionID: sessionID,
            expectedMode: .mock,
            expectedPolicyReference: reference
        )
        guard zeroEvidenceReport.opening_inventory?.base_quantity == "0",
              zeroEvidenceReport.mark_observation_id == observationID,
              zeroEvidenceReport.mark_content_digest == digest else {
            throw Failure.assertion("performance report must preserve exact-zero opening inventory and its recorded mark identity")
        }

        var knownZeroPnL = zeroEvidenceObject
        knownZeroPnL["realized_pnl_krw"] = ["value": "0", "unit": "KRW", "unavailable_reason": NSNull()]
        knownZeroPnL["unrealized_pnl_krw"] = ["value": "0", "unit": "KRW", "unavailable_reason": NSNull()]
        knownZeroPnL["total_pnl_krw"] = ["value": "0", "unit": "KRW", "unavailable_reason": NSNull()]
        let zeroPnlReport = try JSONDecoder().decode(
            AutomationPerformanceReport.self,
            from: JSONSerialization.data(withJSONObject: knownZeroPnL)
        )
        try AutomationSessionMCPService.validateAutomationPerformance(
            zeroPnlReport,
            sessionID: sessionID,
            expectedMode: .mock,
            expectedPolicyReference: reference
        )
        guard zeroPnlReport.total_pnl_krw.value == "0",
              zeroPnlReport.total_pnl_krw.unavailable_reason == nil else {
            throw Failure.assertion("exact zero P&L with known zero opening inventory was treated as unknown")
        }

        var nonzeroOpeningWithPnl = knownZeroPnL
        var openingInventory = try object(nonzeroOpeningWithPnl["opening_inventory"], field: "opening_inventory")
        openingInventory["base_quantity"] = "2"
        nonzeroOpeningWithPnl["opening_inventory"] = openingInventory
        let inheritedInventoryReport = try JSONDecoder().decode(
            AutomationPerformanceReport.self,
            from: JSONSerialization.data(withJSONObject: nonzeroOpeningWithPnl)
        )
        try expectReviewRejection("nonzero inherited inventory with a numeric session P&L was accepted") {
            try AutomationSessionMCPService.validateAutomationPerformance(
                inheritedInventoryReport,
                sessionID: sessionID,
                expectedMode: .mock,
                expectedPolicyReference: reference
            )
        }

        var missingOpeningWithFill = reportObject
        missingOpeningWithFill["fill_count"] = 1
        missingOpeningWithFill["realized_pnl_krw"] = ["value": "0", "unit": "KRW", "unavailable_reason": NSNull()]
        let missingOpeningReport = try JSONDecoder().decode(
            AutomationPerformanceReport.self,
            from: JSONSerialization.data(withJSONObject: missingOpeningWithFill)
        )
        try expectReviewRejection("recorded fills without opening inventory evidence reported numeric realized P&L") {
            try AutomationSessionMCPService.validateAutomationPerformance(
                missingOpeningReport,
                sessionID: sessionID,
                expectedMode: .mock,
                expectedPolicyReference: reference
            )
        }
        var missingOpeningWithFees = reportObject
        missingOpeningWithFees["paid_fees_krw"] = "1"
        missingOpeningWithFees["realized_pnl_krw"] = ["value": "0", "unit": "KRW", "unavailable_reason": NSNull()]
        let missingOpeningFeesReport = try JSONDecoder().decode(
            AutomationPerformanceReport.self,
            from: JSONSerialization.data(withJSONObject: missingOpeningWithFees)
        )
        try expectReviewRejection("nonzero fees without opening inventory evidence allowed numeric P&L") {
            try AutomationSessionMCPService.validateAutomationPerformance(
                missingOpeningFeesReport,
                sessionID: sessionID,
                expectedMode: .mock,
                expectedPolicyReference: reference
            )
        }

        var partialMark = zeroEvidenceObject
        partialMark["mark_content_digest"] = NSNull()
        let partialMarkReport = try JSONDecoder().decode(
            AutomationPerformanceReport.self,
            from: JSONSerialization.data(withJSONObject: partialMark)
        )
        try expectReviewRejection("mark price without its persisted observation digest was accepted") {
            try AutomationSessionMCPService.validateAutomationPerformance(
                partialMarkReport,
                sessionID: sessionID,
                expectedMode: .mock,
                expectedPolicyReference: reference
            )
        }
        var contradictoryMetric = reportObject
        contradictoryMetric["realized_pnl_krw"] = [
            "value": "0",
            "unit": "KRW",
            "unavailable_reason": "not_available"
        ]
        let contradictoryMetricReport = try JSONDecoder().decode(
            AutomationPerformanceReport.self,
            from: JSONSerialization.data(withJSONObject: contradictoryMetric)
        )
        try expectReviewRejection("metric with both an exact value and an unavailable reason was accepted") {
            try AutomationSessionMCPService.validateAutomationPerformance(
                contradictoryMetricReport,
                sessionID: sessionID,
                expectedMode: .mock,
                expectedPolicyReference: reference
            )
        }
        var unexplainedMetric = reportObject
        unexplainedMetric["realized_pnl_krw"] = [
            "value": NSNull(),
            "unit": "KRW",
            "unavailable_reason": NSNull()
        ]
        let unexplainedMetricReport = try JSONDecoder().decode(
            AutomationPerformanceReport.self,
            from: JSONSerialization.data(withJSONObject: unexplainedMetric)
        )
        try expectReviewRejection("unavailable metric without a reason was accepted") {
            try AutomationSessionMCPService.validateAutomationPerformance(
                unexplainedMetricReport,
                sessionID: sessionID,
                expectedMode: .mock,
                expectedPolicyReference: reference
            )
        }

        var generation = AutomationReviewRequestGeneration()
        let firstTicket = generation.advance()
        let currentTicket = generation.advance()
        guard !generation.accepts(firstTicket), generation.accepts(currentTicket) else {
            throw Failure.assertion("review selection changes did not fence stale asynchronous responses")
        }

        let files = AutomationReviewFile.allCases.map { file -> AutomationReviewExportFile in
            let contents = Data(file.expectedFileName.utf8)
            return AutomationReviewExportFile(
                file: file,
                file_name: file.expectedFileName,
                media_type: "application/octet-stream",
                bytes: UInt64(contents.count),
                sha256: AutomationReviewExportCodec.hexDigest(contents)
            )
        }
        let manifest = AutomationReviewExportManifest(
            schema_version: "1.0",
            session_id: sessionID,
            mode: .mock,
            snapshot_digest: digest,
            files: files
        )
        try AutomationReviewExportCodec.validate(manifest, sessionID: sessionID, mode: .mock)
        let bytes = Data("a,b\r\n".utf8)
        let completeCSV = AutomationReviewExportFile(
            file: .decisionsCSV,
            file_name: "decisions.csv",
            media_type: "text/csv; charset=utf-8",
            bytes: UInt64(bytes.count),
            sha256: AutomationReviewExportCodec.hexDigest(bytes)
        )
        let chunk = AutomationReviewExportChunk(
            session_id: sessionID,
            mode: .mock,
            snapshot_digest: digest,
            file: completeCSV.file,
            file_name: completeCSV.file_name,
            media_type: completeCSV.media_type,
            total_bytes: UInt64(bytes.count),
            sha256: AutomationReviewExportCodec.hexDigest(bytes),
            offset: 0,
            bytes: UInt64(bytes.count),
            chunk_sha256: AutomationReviewExportCodec.hexDigest(bytes),
            encoding: "HEX",
            data_hex: bytes.map { String(format: "%02x", $0) }.joined(),
            next_offset: nil
        )
        let decodedChunk = try AutomationReviewExportCodec.decodeAndValidate(
            chunk,
            file: completeCSV,
            sessionID: sessionID,
            mode: .mock,
            snapshotDigest: digest,
            expectedOffset: 0
        )
        try AutomationReviewExportCodec.validateCompleteFile(decodedChunk, manifestFile: completeCSV)
        try expectReviewRejection("export chunk from a different snapshot was accepted") {
            _ = try AutomationReviewExportCodec.decodeAndValidate(
                chunk,
                file: completeCSV,
                sessionID: sessionID,
                mode: .mock,
                snapshotDigest: String(repeating: "c", count: 64),
                expectedOffset: 0
            )
        }
        try expectReviewRejection("export chunk with a wrong offset was accepted") {
            _ = try AutomationReviewExportCodec.decodeAndValidate(
                chunk,
                file: completeCSV,
                sessionID: sessionID,
                mode: .mock,
                snapshotDigest: digest,
                expectedOffset: 1
            )
        }

        let privateRoot = FileManager.default.temporaryDirectory
            .appendingPathComponent("sky-backcraft-review-export-permissions-\(UUID().uuidString.lowercased())", isDirectory: true)
        try FileManager.default.createDirectory(at: privateRoot, withIntermediateDirectories: false)
        defer { try? FileManager.default.removeItem(at: privateRoot) }
        let staging = try AutomationReviewExportStaging.createDirectory(inside: privateRoot)
        func permissions(at url: URL) throws -> Int? {
            (try FileManager.default.attributesOfItem(atPath: url.path)[.posixPermissions] as? NSNumber)?.intValue
        }
        guard try permissions(at: staging) == Int(AutomationReviewExportStaging.directoryPermissions) else {
            throw Failure.assertion("review export staging directory is not owner-only")
        }
        try AutomationReviewExportStaging.write(bytes, file: completeCSV, inside: staging)
        let stagedFile = staging.appendingPathComponent(completeCSV.file_name)
        guard try permissions(at: stagedFile) == Int(AutomationReviewExportStaging.filePermissions) else {
            throw Failure.assertion("review export file is not owner-only before publication")
        }
        let published = privateRoot.appendingPathComponent("published-review", isDirectory: true)
        try FileManager.default.moveItem(at: staging, to: published)
        guard try permissions(at: published) == Int(AutomationReviewExportStaging.directoryPermissions),
              try permissions(at: published.appendingPathComponent(completeCSV.file_name))
                  == Int(AutomationReviewExportStaging.filePermissions) else {
            throw Failure.assertion("published review files lost owner-only permissions after directory rename")
        }
    }

    private static func validateRecordedDecisionPresentation() throws {
        func decode(_ value: [String: Any]) throws -> AutomationJSONValue {
            try JSONDecoder().decode(
                AutomationJSONValue.self,
                from: JSONSerialization.data(withJSONObject: value, options: [.sortedKeys])
            )
        }

        let unknown = try decode([
            "schema_version": "1.0",
            "strategy_action": "HOLD",
            "constrained_action": "NOT_EVALUATED",
            "execution_state": "REJECTED",
            "actual_weight_at_decision": "0.0000",
            "raw_target_weight": NSNull(),
            "constrained_target_weight": "0.0000",
            "matched_rule_id": NSNull(),
            "condition_trace_capture": "PARTIAL_CAPTURE",
            "condition_trace": [[
                "scope": ["kind": "RULE_CONDITION", "rule_id": "entry"],
                "ast_path": "rule:entry:when",
                "role": "ENTRY_CONDITION",
                "outcome": ["kind": "UNKNOWN", "value": "CROSS_HISTORY_UNAVAILABLE"],
                "engine_result": true,
                "evidence": [
                    "kind": "COMPARISON",
                    "left": ["value": "1.12", "unit": "ratio"],
                    "comparison": "GREATER_THAN",
                    "right": ["value": "1.5", "unit": "ratio"]
                ]
            ]],
            "reasons": [[
                "stage": "STRATEGY",
                "code": "CROSS_HISTORY_UNAVAILABLE",
                "label_ko": "교차 이력 부족"
            ]],
            "orders": [["order_id": "order-rejected", "state": "REJECTED", "reason": "RISK_LIMIT"]],
            "fills": []
        ])
        let unknownLines = RecordedDecisionPresentation.lines(for: unknown, masked: false)
        let unknownText = unknownLines.joined(separator: "\n")
        guard unknownText.contains("HOLD"), unknownText.contains("NOT_EVALUATED"),
              unknownText.contains("REJECTED"), unknownText.contains("CROSS_HISTORY_UNAVAILABLE"),
              unknownText.contains("1.12"), unknownText.contains("1.5"),
              unknownText.contains("교차 이력 부족"), unknownText.contains("0.0000"),
              unknownText.contains("엔진 반환 true"), !unknownText.contains("불충족 (FALSE)") else {
            throw Failure.assertion("recorded unknown/rejected decision lost its exact reason, evidence, zero, or tri-state distinction")
        }

        let legacyOutcome = try decode([
            "condition_trace_capture": "CAPTURED",
            "condition_trace": [[
                "ast_path": "legacy-rule:when", "outcome": ["kind": "FALSE"],
                "engine_result": false
            ]]
        ])
        let legacyText = RecordedDecisionPresentation.lines(for: legacyOutcome, masked: false).joined(separator: "\n")
        guard legacyText.contains("평가 결과 기록"), legacyText.contains("실제 비교 입력: 미기록"),
              !legacyText.contains("전체 기록") else {
            throw Failure.assertion("captured legacy outcomes must not imply captured comparison operands")
        }

        let skipped = try decode([
            "schema_version": "1.0",
            "strategy_action": "NOT_EVALUATED",
            "constrained_action": "NOT_EVALUATED",
            "execution_state": "NOT_EVALUATED",
            "condition_trace_capture": "PARTIAL_CAPTURE",
            "condition_trace": [[
                "scope": ["kind": "RULE_CONDITION", "rule_id": "later"],
                "ast_path": "rule:later:when",
                "role": "ENTRY_CONDITION",
                "outcome": ["kind": "NOT_EVALUATED", "value": "PRIOR_RULE_MATCHED"]
            ]],
            "reasons": [],
            "orders": [],
            "fills": []
        ])
        let skippedText = RecordedDecisionPresentation.lines(for: skipped, masked: false).joined(separator: "\n")
        guard skippedText.contains("NOT_EVALUATED"), skippedText.contains("PRIOR_RULE_MATCHED"),
              skippedText.contains("미평가"), !skippedText.contains("거짓") else {
            throw Failure.assertion("a skipped condition was presented as false instead of not evaluated")
        }

        let uncaptured = try decode([
            "schema_version": "1.0",
            "strategy_action": "NOT_CAPTURED",
            "constrained_action": "NOT_CAPTURED",
            "execution_state": "NOT_CAPTURED",
            "actual_weight_at_decision": NSNull(),
            "condition_trace_capture": "NOT_CAPTURED",
            "condition_trace": [],
            "reasons": [],
            "orders": [],
            "fills": []
        ])
        let uncapturedText = RecordedDecisionPresentation.lines(for: uncaptured, masked: false).joined(separator: "\n")
        guard uncapturedText.contains("NOT_CAPTURED"), uncapturedText.contains("미기록") else {
            throw Failure.assertion("uncaptured decision values were hidden or converted to zero")
        }

        let filled = try decode([
            "schema_version": "1.0",
            "strategy_action": "BUY",
            "constrained_action": "BUY",
            "execution_state": "FILLED",
            "actual_weight_at_decision": "0.0000",
            "raw_target_weight": "1.0000",
            "constrained_target_weight": "1.0000",
            "matched_rule_id": "entry_rule",
            "condition_trace_capture": "CAPTURED",
            "condition_trace": [],
            "reasons": [["stage": "EXECUTION", "code": "ORDER_FILLED", "label_ko": "가상 주문 체결"]],
            "orders": [["order_id": "order-filled", "state": "FILLED", "reason": NSNull()]],
            "fills": [["fill_id": "fill-001", "order_id": "order-filled"]]
        ])
        let filledText = RecordedDecisionPresentation.lines(for: filled, masked: false).joined(separator: "\n")
        guard filledText.contains("BUY"), filledText.contains("FILLED"),
              filledText.contains("fill-001"), filledText.contains("가상 주문 체결"),
              filledText.contains("0.0000"), filledText.contains("1.0000") else {
            throw Failure.assertion("filled decision did not preserve saved action, execution, fill identity, or exact weights")
        }

        let unavailable = RecordedDecisionPresentation.lines(for: nil, masked: false).joined(separator: "\n")
        let masked = RecordedDecisionPresentation.lines(for: filled, masked: true).joined(separator: "\n")
        guard unavailable.contains("저장된") && unavailable.contains("없"),
              masked.contains("개인정보 숨김"), !masked.contains("fill-001"), !masked.contains("1.0000") else {
            throw Failure.assertion("legacy missing capture or privacy masking was presented as a reconstructed decision")
        }
        let preparation = try decode([
            "status": "DEFERRED",
            "completed_bars_seen": 12,
            "required_bars": 20,
            "decision_time": "2026-10-23T01:00:00Z",
            "observation_id": "observation-preparation-001"
        ])
        let preparationText = RecordedDecisionPresentation.preparationLines(
            for: preparation,
            masked: false
        ).joined(separator: "\n")
        guard preparationText.contains("DEFERRED"), preparationText.contains("12"),
              preparationText.contains("20"), preparationText.contains("observation-preparation-001") else {
            throw Failure.assertion("stored preparation status, counts, time, or observation identity were not shown as recorded")
        }
        print("PASS deterministic recorded decision labels preserve unknown, skipped, uncaptured, rejected, filled, zero and privacy states")
    }

    private static func validatePaperRecordedDecisionReview() throws {
        let sessionID = "paper-decision-fixture"
        let signalID = "paper-signal-decision-001"
        let policyReference: [String: Any] = [
            "policy_id": "paper-policy-decision",
            "revision_id": "paper-revision-decision",
            "definition_digest": String(repeating: "d", count: 64)
        ]
        let recordedDecision: [String: Any] = [
            "schema_version": "1.0",
            "policy_ref": policyReference,
            "decision_time": "2026-10-23T01:00:00Z",
            "strategy_action": "HOLD",
            "constrained_action": "HOLD",
            "execution_state": "NO_ORDER",
            "actual_weight_at_decision": "0.0000",
            "raw_target_weight": "0.0000",
            "constrained_target_weight": "0.0000",
            "matched_rule_id": NSNull(),
            "condition_trace_capture": "CAPTURED",
            "condition_trace": [],
            "reasons": [["stage": "STRATEGY", "code": "NO_ENTRY", "label_ko": "매수 조건 불충족"]],
            "orders": [],
            "fills": []
        ]
        let response: [String: Any] = [
            "schema_version": "1.0",
            "selection": [
                "session_id": sessionID,
                "event_sequence": 42,
                "as_of_sequence": 57,
                "signal_id": signalID
            ],
            "configuration": ["policy_ref": policyReference],
            "policy": ["reference": policyReference],
            "event": [
                "sequence": 42,
                "kind": "CANDLE_PROCESSED",
                "market": "KRW-BTC",
                "preparation": [
                    "status": "DEFERRED",
                    "completed_bars_seen": 12,
                    "required_bars": 20,
                    "decision_time": "2026-10-23T01:00:00Z",
                    "observation_id": "observation-preparation-001"
                ],
                "signals": [[
                    "signal_id": signalID,
                    "strategy": "fixture",
                    "intended_side": NSNull(),
                    "state_before": "CASH",
                    "state_after": "CASH",
                    "raw_target_weight": "0.0000",
                    "constrained_target_weight": "0.0000",
                    "reasons": ["NO_ENTRY"],
                    "outcome": "HOLD"
                ]],
                "orders": [],
                "order_events": [],
                "fills": [],
                "account_marks": [],
                "episodes": []
            ],
            "original_event_digest": String(repeating: "e", count: 64),
            "selected_signal_id": signalID,
            "source_observations": [],
            "data_quality": [:],
            "non_entry_observations": NSNull(),
            "recorded_decision": recordedDecision
        ]
        let recorder = PolicyDeletionTestRecorder()
        PaperWriteRecoveryURLProtocol.install { request in
            guard let body = PaperWriteRecoveryURLProtocol.requestBody(request),
                  let envelope = try? JSONSerialization.jsonObject(with: body) as? [String: Any],
                  let messageID = envelope["id"],
                  let method = envelope["method"] as? String else {
                recorder.recordUnexpected("invalid JSON-RPC request")
                return nil
            }
            let result: [String: Any]
            if method == "initialize" {
                result = ["protocolVersion": "2025-03-26"]
            } else if method == "tools/call",
                      let params = envelope["params"] as? [String: Any],
                      let tool = params["name"] as? String,
                      let arguments = params["arguments"] as? [String: Any] {
                recorder.record(tool: tool, arguments: arguments)
                guard tool == "paper_review_query",
                      arguments["selection"] as? [String: Any] != nil,
                      let data = try? JSONSerialization.data(withJSONObject: response, options: [.sortedKeys]),
                      let text = String(data: data, encoding: .utf8) else {
                    recorder.recordUnexpected("unexpected decision review tool or arguments")
                    result = ["isError": true, "content": []]
                    return Self.paperReviewJSONRPCResponse(id: messageID, result: result)
                }
                result = ["isError": false, "content": [["type": "text", "text": text]]]
            } else {
                recorder.recordUnexpected("unexpected JSON-RPC method: \(method)")
                result = ["isError": true, "content": []]
            }
            return Self.paperReviewJSONRPCResponse(id: messageID, result: result)
        }
        defer { PaperWriteRecoveryURLProtocol.clear() }

        let urlConfiguration = URLSessionConfiguration.ephemeral
        urlConfiguration.protocolClasses = [PaperWriteRecoveryURLProtocol.self]
        let urlSession = URLSession(configuration: urlConfiguration)
        defer { urlSession.invalidateAndCancel() }
        let client = NativeMCPClient(session: urlSession)
        guard client.configure(port: 65_433, token: String(repeating: "f", count: 64)) != nil else {
            throw Failure.assertion("paper decision review mocked transport did not configure")
        }
        let service = PaperSessionMCPService(client: client)
        var outcome: Result<PaperDecisionReviewResponse, Error>?
        _ = service.reviewDecision(
            sessionId: sessionID,
            eventSequence: 42,
            asOfSequence: 57,
            signalID: signalID,
            expectedPolicyReference: PaperPolicyRevisionReference(
                policy_id: "paper-policy-decision",
                revision_id: "paper-revision-decision",
                definition_digest: String(repeating: "d", count: 64)
            )
        ) { outcome = $0 }
        let deadline = Date().addingTimeInterval(5)
        while outcome == nil, Date() < deadline {
            _ = RunLoop.current.run(mode: .default, before: Date().addingTimeInterval(0.01))
        }
        guard let review = try outcome?.get() else {
            throw Failure.assertion("paper decision review did not finish its bounded mocked read")
        }
        guard review.selection.event_sequence == 42,
              review.selection.as_of_sequence == 57,
              review.event.sequence == 42,
              review.selected_signal_id == signalID,
              review.recorded_decision != nil,
              review.event.preparation != nil else {
            throw Failure.assertion("paper decision review lost the selected saved signal, fixed cutoff, or recorded explanation")
        }
        let observed = recorder.snapshot()
        guard observed.calls.count == 1,
              observed.calls[0].tool == "paper_review_query",
              observed.unexpectedRequests.isEmpty else {
            throw Failure.assertion("paper decision explanation used a legacy event/get/poll route or another tool")
        }
        try assertJSONEqual(observed.calls[0].arguments, [
            "action": "decision",
            "selection": [
                "session_id": sessionID,
                "event_sequence": 42,
                "as_of_sequence": 57,
                "signal_id": signalID
            ]
        ], field: "paper decision review fixed saved selection")
        print("PASS paper decision review reads the selected saved event at a fixed cutoff through paper_review_query only")
    }

    private static func validateActualRustAutomationReviewWire(at url: URL) throws {
        let data = try Data(contentsOf: url)
        guard let root = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
            throw Failure.assertion("actual Rust review wire root must be an object")
        }
        let pageObject = try object(root["decisions"], field: "actual Rust review decisions page")
        let page = try JSONDecoder().decode(
            AutomationReviewDecisionPage.self,
            from: JSONSerialization.data(withJSONObject: pageObject)
        )
        try AutomationSessionMCPService.validateReviewDecisionPage(
            page,
            sessionID: page.session_id,
            offset: 0,
            limit: AutomationSessionWire.reviewDecisionPageLimit
        )

        let detailObject = try object(root["decision"], field: "actual Rust review decision detail")
        let detail = try JSONDecoder().decode(
            AutomationReviewDecisionDetail.self,
            from: JSONSerialization.data(withJSONObject: detailObject)
        )
        try AutomationSessionMCPService.validateReviewDecisionDetail(
            detail,
            sessionID: page.session_id,
            intentID: detail.decision.intent_id,
            expectedMode: page.mode,
            expectedPolicyReference: page.policy_ref
        )
        guard let summary = page.items.first(where: { $0.intent_id == detail.decision.intent_id }) else {
            throw Failure.assertion("actual Rust detail is absent from its decision summary page")
        }
        try AutomationSessionMCPService.validateReviewDecisionMatchesSummary(detail, summary: summary)

        let rawOrderObjects = try arrayOfObjects(detailObject["orders"], field: "actual Rust decision raw orders")
        guard !rawOrderObjects.isEmpty,
              let rawOrder = rawOrderObjects.first,
              rawOrder["order"] == nil,
              let order = detail.orders.first,
              let rawClientIdentifier = rawOrder["client_identifier"] as? String,
              let rawIntentID = rawOrder["intent_id"] as? String,
              let rawNotional = rawOrder["requested_notional_krw"] as? String,
              let rawExecutedVolume = rawOrder["executed_volume"] as? String,
              let rawPaidFee = rawOrder["paid_fee_krw"] as? String,
              order.client_identifier == rawClientIdentifier,
              order.intent_id == rawIntentID,
              order.requested_notional_krw == rawNotional,
              order.executed_volume == rawExecutedVolume,
              order.paid_fee_krw == rawPaidFee else {
            throw Failure.assertion("actual Rust detail orders must decode as raw AutomationOrderView values")
        }

        let snapshotObject = try object(root["snapshot"], field: "actual Rust review export snapshot")
        let wrappedSnapshotOrders = try arrayOfObjects(snapshotObject["orders"], field: "snapshot wrapped orders")
        guard !wrappedSnapshotOrders.isEmpty,
              wrappedSnapshotOrders.first?["order"] is [String: Any] else {
            throw Failure.assertion("full snapshot order metadata wrapper must remain intact")
        }

        let performanceObject = try object(root["performance"], field: "actual Rust review performance")
        let report = try JSONDecoder().decode(
            AutomationPerformanceReport.self,
            from: JSONSerialization.data(withJSONObject: performanceObject)
        )
        try AutomationSessionMCPService.validateAutomationPerformance(
            report,
            sessionID: page.session_id,
            expectedMode: page.mode,
            expectedPolicyReference: page.policy_ref
        )
        let manifestObject = try object(root["export_manifest"], field: "actual Rust review export manifest")
        let manifest = try JSONDecoder().decode(
            AutomationReviewExportManifest.self,
            from: JSONSerialization.data(withJSONObject: manifestObject)
        )
        try AutomationReviewExportCodec.validate(manifest, sessionID: page.session_id, mode: page.mode)
        guard let rawWarmupCount = performanceObject["warmup_record_count"] as? NSNumber,
              report.warmup_record_count == rawWarmupCount.uint64Value,
              report.snapshot_digest == manifest.snapshot_digest else {
            throw Failure.assertion("actual Rust warmup count or shared report/export snapshot identity changed")
        }
    }

    private static func arrayOfObjects(_ value: Any?, field: String) throws -> [[String: Any]] {
        guard let rows = value as? [[String: Any]] else {
            throw Failure.assertion("\(field) must be an array of objects")
        }
        return rows
    }

    private static func expectReviewRejection(_ message: String, operation: () throws -> Void) throws {
        do {
            try operation()
            throw Failure.assertion(message)
        } catch is AutomationSessionServiceError {
        }
    }


#if POLICY_DELETION_REGRESSION
    private static func validateLiveOrderConfirmationDialog() throws {
        _ = NSApplication.shared
        for isMock in [false, true] {
            let (alert, field) = SettingsWindowController.makeLiveOrderEnableAlert(isMock: isMock)
            alert.layout()
            guard let accessory = alert.accessoryView else {
                throw Failure.assertion("Order confirmation dialog has no input area")
            }
            accessory.layoutSubtreeIfNeeded()
            let inputRect = field.convert(field.bounds, to: accessory)
            let phrase = UpbitLiveService.liveOrderEnableConfirmation
            let phraseWidth = (phrase as NSString).size(withAttributes: [
                .font: field.font ?? NSFont.systemFont(ofSize: NSFont.systemFontSize)
            ]).width
            guard accessory.bounds.width > 0, accessory.bounds.height > 0,
                  accessory.bounds.contains(inputRect), inputRect.height >= 22,
                  inputRect.width >= phraseWidth + 12,
                  field.isEditable, field.isSelectable, field.acceptsFirstResponder,
                  alert.window.initialFirstResponder === field,
                  !field.isHiddenOrHasHiddenAncestor,
                  let content = alert.window.contentView,
                  content.bounds.contains(accessory.convert(accessory.bounds, to: content)) else {
                throw Failure.assertion("Order confirmation input is clipped, too narrow, non-editable, or lacks initial keyboard focus: accessory=\(accessory.bounds), input=\(inputRect)")
            }
            let labels = accessory.subviews.compactMap { $0 as? NSTextField }.filter { $0 !== field }
            guard labels.contains(where: { $0.stringValue.contains(phrase) }),
                  labels.allSatisfy({ label in
                      guard let parent = label.superview else { return false }
                      let rect = parent.convert(label.alignmentRect(forFrame: label.frame), to: accessory)
                      return accessory.bounds.contains(rect) && !rect.intersects(inputRect)
                  }),
                  alert.buttons.count == 2,
                  alert.buttons[1].title == "취소" else {
                throw Failure.assertion("Exact confirmation phrase or cancel action is hidden or overlaps the input: labels=\(labels.map { "\($0.stringValue):\($0.convert($0.bounds, to: accessory))" }), input=\(inputRect), buttons=\(alert.buttons.map(\.title))")
            }
        }
        print("PASS order confirmation dialog visible input, exact phrase and initial keyboard focus")
    }

    private static func validateSettingsSaveOnlyRestartsForChangedNetworkConfiguration() throws {
        _ = NSApplication.shared
        let base = FileManager.default.temporaryDirectory
            .appendingPathComponent("sky-backcraft-settings-save-\(UUID().uuidString)", isDirectory: true)
        defer { try? FileManager.default.removeItem(at: base) }

        func fileNumber(at url: URL) throws -> NSNumber {
            let attributes = try FileManager.default.attributesOfItem(atPath: url.path)
            guard let number = attributes[.systemFileNumber] as? NSNumber else {
                throw Failure.assertion("settings file has no filesystem identity")
            }
            return number
        }

        func settingsControls(in controller: SettingsWindowController) throws -> (
            window: NSWindow,
            save: NSButton,
            port: NSTextField,
            orderHint: String
        ) {
            guard let window = controller.window, let content = window.contentView else {
                throw Failure.assertion("production Settings window did not build its native content")
            }
            func descendants(of view: NSView) -> [NSView] {
                [view] + view.subviews.flatMap(descendants)
            }
            let views = descendants(of: content)
            guard let save = views.compactMap({ $0 as? NSButton })
                    .first(where: { $0.title == "네트워크·인증 설정 저장" }),
                  let port = views.compactMap({ $0 as? NSTextField })
                    .first(where: { $0.accessibilityLabel() == "MCP 로컬 포트" }),
                  let orderHint = views.compactMap({ $0 as? NSTextField })
                    .first(where: { $0.stringValue.contains("설정 저장이 필요하지 않습니다") })?.stringValue,
                  orderHint.contains("백엔드가 재시작"), orderHint.contains("OFF로 초기화") else {
                throw Failure.assertion("production Settings save button, port field or immediate-order hint is missing")
            }
            return (window, save, port, orderHint)
        }

        let unchangedStore = try ConfigurationStore(
            supportDirectory: base.appendingPathComponent("unchanged", isDirectory: true)
        )
        try unchangedStore.save(.default)
        let unchangedBytes = try Data(contentsOf: unchangedStore.settingsURL)
        let unchangedFileNumber = try fileNumber(at: unchangedStore.settingsURL)
        let unchangedController = SettingsWindowController(store: unchangedStore)
        var unchangedCallbacks: [AppConfiguration] = []
        unchangedController.configurationDidSave = { unchangedCallbacks.append($0) }
        let unchangedControls = try settingsControls(in: unchangedController)
        unchangedController.showWindow(nil)
        unchangedControls.save.performClick(nil)
        let bytesAfterUnchangedSave = try Data(contentsOf: unchangedStore.settingsURL)
        let fileNumberAfterUnchangedSave = try fileNumber(at: unchangedStore.settingsURL)
        guard unchangedCallbacks.isEmpty,
              unchangedBytes == bytesAfterUnchangedSave,
              unchangedFileNumber == fileNumberAfterUnchangedSave,
              !unchangedControls.window.isVisible else {
            throw Failure.assertion("unchanged Settings save wrote the file, requested a restart, or failed to close")
        }

        let changedStore = try ConfigurationStore(
            supportDirectory: base.appendingPathComponent("changed", isDirectory: true)
        )
        try changedStore.save(.default)
        let changedFileNumber = try fileNumber(at: changedStore.settingsURL)
        let changedController = SettingsWindowController(store: changedStore)
        var changedCallbacks: [AppConfiguration] = []
        changedController.configurationDidSave = { changedCallbacks.append($0) }
        let changedControls = try settingsControls(in: changedController)
        var expected = AppConfiguration.default
        expected.port += 1
        changedControls.port.stringValue = String(expected.port)
        changedController.showWindow(nil)
        changedControls.save.performClick(nil)
        guard changedCallbacks == [expected],
              try changedStore.load() == expected,
              changedFileNumber != (try fileNumber(at: changedStore.settingsURL)),
              !changedControls.window.isVisible else {
            throw Failure.assertion("changed network Settings did not save once, request one restart, and close")
        }

        let corruptStore = try ConfigurationStore(
            supportDirectory: base.appendingPathComponent("corrupt", isDirectory: true)
        )
        try Data("corrupt settings".utf8).write(to: corruptStore.settingsURL)
        let corruptController = SettingsWindowController(store: corruptStore)
        var repairCallbacks: [AppConfiguration] = []
        corruptController.configurationDidSave = { repairCallbacks.append($0) }
        let corruptControls = try settingsControls(in: corruptController)
        corruptController.showWindow(nil)
        corruptControls.save.performClick(nil)
        guard repairCallbacks == [.default],
              try corruptStore.load() == .default,
              !corruptControls.window.isVisible else {
            throw Failure.assertion("unchanged default Settings did not repair an invalid saved configuration")
        }

        print("PASS native Settings button skips unchanged writes, saves changed network settings once, and repairs invalid settings")
    }

    private static func validateSettingsEmergencyStopGateAndControlRace() throws {
        _ = NSApplication.shared
        let base = FileManager.default.temporaryDirectory
            .appendingPathComponent("sky-backcraft-control-ui-race-\(UUID().uuidString)", isDirectory: true)
        defer { try? FileManager.default.removeItem(at: base) }
        let store = try ConfigurationStore(supportDirectory: base.appendingPathComponent("support", isDirectory: true))
        let testNow = Date()
        let timestamp = ISO8601DateFormatter().string(from: testNow)
        let server = TradingControlRaceMockServer(observedAt: timestamp)
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [TradingControlRaceURLProtocol.self]
        let session = URLSession(configuration: configuration)
        defer {
            session.invalidateAndCancel()
            TradingControlRaceURLProtocol.clear()
        }
        TradingControlRaceURLProtocol.install(server)
        let client = NativeMCPClient(session: session)
        guard client.configure(port: 65_432, token: String(repeating: "a", count: 64)) != nil else {
            throw Failure.assertion("control UI mock transport did not configure")
        }
        let service = UpbitLiveService(client: client)
        let first = SettingsWindowController(store: store, tradingService: service, now: { testNow })
        let second = SettingsWindowController(store: store, tradingService: service, now: { testNow })
        guard let firstContent = first.window?.contentView,
              let secondContent = second.window?.contentView else {
            throw Failure.assertion("two production Settings windows did not build native content views")
        }
        firstContent.layoutSubtreeIfNeeded()
        secondContent.layoutSubtreeIfNeeded()
        let firstButtons = try settingsControlButtons(in: firstContent)
        let secondButtons = try settingsControlButtons(in: secondContent)

        let disabled = TradingControlSnapshot(
            exchange_read_mode: "LIVE_READ",
            live_trading_enabled: false,
            state: "DISABLED_BY_CONFIG",
            sequence: 17,
            observed_at_utc: timestamp,
            order_admission_blocked: true,
            emergency_latched: false,
            denied_code: nil,
            receipt: nil
        )
        guard first.updateControlSnapshot(disabled, authenticated: true),
              second.updateControlSnapshot(disabled, authenticated: true) else {
            throw Failure.assertion("fresh config-off control snapshots were not accepted for presentation")
        }
        guard !firstButtons.emergency.isEnabled,
              !secondButtons.emergency.isEnabled,
              firstButtons.emergency.toolTip?.contains("전략/전역 거래 제어가 OFF") == true,
              firstButtons.automation.isEnabled,
              secondButtons.automation.isEnabled,
              firstButtons.refresh.isEnabled,
              secondButtons.refresh.isEnabled else {
            throw Failure.assertion("LIVE-off Settings window exposed Emergency Stop or hid its read-only status refresh reason")
        }
        var navigationCount = 0
        first.onOpenAutomationWorkspace = { navigationCount += 1 }
        firstButtons.automation.performClick(nil)
        guard navigationCount == 1 else {
            throw Failure.assertion("Settings strategy management did not open its actual workspace callback")
        }
        firstButtons.emergency.performClick(nil)
        first.performEmergencyStopConfirmed(requestId: "config-off-emergency-must-not-send")
        guard server.snapshot().emergencyRequestIDs.isEmpty else {
            throw Failure.assertion("config-off Emergency Stop reached the mock MCP server")
        }
        firstButtons.refresh.performClick(nil)
        _ = try waitForPolicyDeletionResult({ () -> Bool? in
            guard server.snapshot().statusReadCount == 1
                && firstButtons.refresh.isEnabled
                && !first.controlStatusTextSnapshot.contains("조회 중") else { return nil }
            return true
        }, label: "config-off status refresh")
        guard first.controlStatusTextSnapshot.contains("전략/전역 거래 제어: OFF"),
              first.controlStatusTextSnapshot.contains("DISABLED_BY_CONFIG"),
              server.snapshot().unexpectedToolCalls.isEmpty else {
            throw Failure.assertion(
                "config-off control status refresh did not remain read-only and available: "
                    + "status=\(first.controlStatusTextSnapshot), unexpected=\(server.snapshot().unexpectedToolCalls)"
            )
        }

        server.prepareLiveRace(sequence: 23, observedAt: timestamp)
        let running = TradingControlSnapshot(
            exchange_read_mode: "LIVE_READ",
            live_trading_enabled: true,
            state: "RUNNING",
            sequence: 23,
            observed_at_utc: timestamp,
            order_admission_blocked: false,
            emergency_latched: false,
            denied_code: nil,
            receipt: nil
        )
        guard first.updateControlSnapshot(running, authenticated: true),
              second.updateControlSnapshot(running, authenticated: true),
              firstButtons.emergency.isEnabled,
              secondButtons.emergency.isEnabled else {
            throw Failure.assertion("LIVE-enabled mock state did not preserve the existing Emergency Stop control path")
        }

        let requestIDs = ["00000000-0000-4000-8000-000000000017", "00000000-0000-4000-8000-000000000018"]
        first.performEmergencyStopConfirmed(requestId: requestIDs[0])
        second.performEmergencyStopConfirmed(requestId: requestIDs[1])
        do {
            _ = try waitForPolicyDeletionResult({ () -> Bool? in
                let snapshot = server.snapshot()
                guard snapshot.emergencyRequestIDs.count == 2
                    && snapshot.statusReadCount >= 2
                    && firstButtons.refresh.isEnabled
                    && secondButtons.refresh.isEnabled else { return nil }
                return true
            }, label: "concurrent Emergency Stop CAS and first readback")
        } catch {
            let snapshot = server.snapshot()
            throw Failure.assertion(
                "concurrent Emergency Stop wait failed: requests=\(snapshot.emergencyRequestIDs.count), "
                    + "statusReads=\(snapshot.statusReadCount), applied=\(snapshot.appliedRequestIDs.count), "
                    + "stale=\(snapshot.staleRequestIDs.count), maxConcurrent=\(snapshot.maximumConcurrentWrites), "
                    + "sequence=\(snapshot.sequence), unexpected=\(snapshot.unexpectedToolCalls), "
                    + "first=\(first.controlStatusTextSnapshot), second=\(second.controlStatusTextSnapshot)"
            )
        }

        let race = server.snapshot()
        guard race.emergencyRequestIDs.sorted() == requestIDs.sorted(),
              race.expectedSequences == [23, 23],
              race.appliedRequestIDs.count == 1,
              race.staleRequestIDs.count == 1,
              race.maximumConcurrentWrites == 2,
              race.sequence == 24,
              race.state == "EMERGENCY_STOP_ACTIVE",
              race.exchangeOrderEffects == 0,
              race.unexpectedToolCalls.isEmpty else {
            throw Failure.assertion("two-window Emergency Stop race did not produce one CAS winner, one stale request, and zero exchange effects")
        }

        firstButtons.refresh.performClick(nil)
        secondButtons.refresh.performClick(nil)
        _ = try waitForPolicyDeletionResult({ () -> Bool? in
            let readback = server.snapshot()
            guard readback.statusReadCount >= 4
                && readback.sequence == 24
                && readback.state == "EMERGENCY_STOP_ACTIVE"
                && firstButtons.refresh.isEnabled
                && secondButtons.refresh.isEnabled
                && first.controlStatusTextSnapshot == second.controlStatusTextSnapshot
                && first.controlStatusTextSnapshot.contains("긴급 정지 적용")
                && first.controlStatusTextSnapshot.contains("EMERGENCY_STOP_ACTIVE") else { return nil }
            return true
        }, label: "two-window control status readback convergence")
        guard server.snapshot().exchangeOrderEffects == 0,
              first.controlStatusTextSnapshot == second.controlStatusTextSnapshot else {
            throw Failure.assertion("two Settings windows did not converge on the mock server's readback")
        }
        first.window?.close()
        second.window?.close()
        print("PASS Settings LIVE-off Emergency Stop UI gate and two-window CAS/readback race (0 exchange effects)")
    }

    private static func validateSettingsAccountStrategyAdmission() throws {
        _ = NSApplication.shared
        let testNow = Date()
        let timestamp = ISO8601DateFormatter().string(from: testNow)
        let root = FileManager.default.temporaryDirectory.appendingPathComponent("account-admission-\(UUID().uuidString)")
        defer { try? FileManager.default.removeItem(at: root) }
        let store = try ConfigurationStore(supportDirectory: root)
        try store.save(AppConfiguration(domain: "synthetic-fixture.invalid", port: 65_431, authenticationMode: .oauth, tradingMcpEnabled: true))
        let server = TradingControlRaceMockServer(observedAt: timestamp)
        TradingControlRaceURLProtocol.install(server)
        defer { TradingControlRaceURLProtocol.clear() }
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [TradingControlRaceURLProtocol.self]
        let session = URLSession(configuration: configuration)
        defer { session.invalidateAndCancel() }
        let client = NativeMCPClient(session: session)
        client.configure(port: 65_431, token: String(repeating: "a", count: 64))
        let controller = SettingsWindowController(store: store, tradingService: UpbitLiveService(client: client), now: { testNow })
        controller.show()
        controller.window?.orderOut(nil)
        defer { controller.close() }
        _ = try waitForPolicyDeletionResult({
            server.snapshot().statusReadCount >= 2 && controller.controlStatusTextSnapshot.contains("DISABLED_BY_CONFIG") ? true : nil
        }, label: "Settings loads persisted OAuth configuration and initial read-only status")
        func snapshot(_ state: String, sequence: UInt64, enabled: Bool = true) -> TradingControlSnapshot {
            TradingControlSnapshot(exchange_read_mode: "LIVE_READ", live_trading_enabled: enabled,
                state: state, sequence: sequence, observed_at_utc: timestamp,
                order_admission_blocked: true, emergency_latched: false, denied_code: nil, receipt: nil)
        }
        func buttons(_ view: NSView) -> [NSButton] {
            view.subviews.flatMap { child in ([child as? NSButton].compactMap { $0 }) + buttons(child) }
        }
        guard let content = controller.window?.contentView,
              let permit = buttons(content).first(where: { $0.title.hasPrefix("계좌 전략") }),
              let refresh = buttons(content).first(where: { $0.accessibilityLabel() == "전략 제어 서버 상태 새로고침" }) else {
            throw Failure.assertion("Settings omitted the account-wide strategy admission control")
        }
        _ = controller.updateControlSnapshot(snapshot("STANDBY", sequence: 10, enabled: false), authenticated: true)
        controller.performAccountStrategyAdmissionConfirmed(requestID: "off-must-not-send", expectedAction: "start", expectedSequence: 10)
        guard !permit.isEnabled, server.snapshot().authorizationRequestIDs.isEmpty else {
            throw Failure.assertion("account permit ignored independent order-send capability OFF")
        }
        server.prepareAuthorization(state: "STANDBY", sequence: 10, observedAt: timestamp)
        _ = controller.updateControlSnapshot(snapshot("STANDBY", sequence: 10), authenticated: true)
        guard permit.isEnabled else { throw Failure.assertion("fresh STANDBY cannot request account strategy admission") }
        controller.performAccountStrategyAdmissionConfirmed(requestID: "native-account-start", expectedAction: "start", expectedSequence: 10)
        _ = try waitForPolicyDeletionResult({ controller.controlStatusTextSnapshot.contains("RUNNING") ? true : nil }, label: "native account Start receipt")
        guard server.snapshot().authorizationRequestIDs == ["native-account-start"], !permit.isEnabled else {
            throw Failure.assertion("native account Start was duplicated or left enablement stale")
        }
        server.prepareAuthorization(state: "PAUSED", sequence: 20, observedAt: timestamp)
        _ = controller.updateControlSnapshot(snapshot("PAUSED", sequence: 20), authenticated: true)
        controller.performAccountStrategyAdmissionConfirmed(requestID: "native-account-resume", expectedAction: "resume", expectedSequence: 20)
        _ = try waitForPolicyDeletionResult({ controller.controlStatusTextSnapshot.contains("RUNNING") ? true : nil }, label: "native account Resume receipt")
        guard server.snapshot().authorizationRequestIDs == ["native-account-start", "native-account-resume"] else {
            throw Failure.assertion("native account Resume did not use its own deliberate request")
        }
        server.prepareAuthorization(state: "STANDBY", sequence: 25, observedAt: timestamp, acknowledgeBlocked: true)
        _ = controller.updateControlSnapshot(snapshot("STANDBY", sequence: 25), authenticated: true)
        controller.performAccountStrategyAdmissionConfirmed(requestID: "native-account-blocked-ack", expectedAction: "start", expectedSequence: 25)
        _ = try waitForPolicyDeletionResult({ controller.controlStatusTextSnapshot.contains("UNKNOWN") ? true : nil }, label: "blocked ACK is not usable permission")
        refresh.performClick(nil)
        _ = try waitForPolicyDeletionResult({ controller.controlStatusTextSnapshot.contains("신규 전략 주문 차단") ? true : nil }, label: "blocked authoritative status readback")
        guard !permit.isEnabled else { throw Failure.assertion("ACK/RUNNING with blocked admission was reported as permission") }
        server.prepareAuthorization(state: "STANDBY", sequence: 30, observedAt: timestamp, dropResponse: true)
        _ = controller.updateControlSnapshot(snapshot("STANDBY", sequence: 30), authenticated: true)
        controller.performAccountStrategyAdmissionConfirmed(requestID: "native-account-unknown", expectedAction: "start", expectedSequence: 30)
        _ = try waitForPolicyDeletionResult({ controller.controlStatusTextSnapshot.contains("UNKNOWN") ? true : nil }, label: "unknown native account admission")
        controller.performAccountStrategyAdmissionConfirmed(requestID: "unknown-must-not-repeat", expectedAction: "start", expectedSequence: 30)
        controller.updateStatus(.stopping)
        _ = controller.updateControlSnapshot(snapshot("STANDBY", sequence: 30), authenticated: true)
        controller.performAccountStrategyAdmissionConfirmed(requestID: "runtime-label-must-not-release-unknown", expectedAction: "start", expectedSequence: 30)
        guard !permit.isEnabled else { throw Failure.assertion("a stopping label was treated as proven helper exit") }
        refresh.performClick(nil)
        _ = try waitForPolicyDeletionResult({ controller.controlStatusTextSnapshot.contains("RUNNING") ? true : nil }, label: "native account admission status readback")
        guard server.snapshot().authorizationRequestIDs == ["native-account-start", "native-account-resume", "native-account-blocked-ack", "native-account-unknown"],
              server.snapshot().exchangeOrderEffects == 0, server.snapshot().unexpectedToolCalls.isEmpty else {
            throw Failure.assertion("unknown account admission caused another mutation or exchange effect")
        }
        print("PASS native account strategy admission Start/Resume, OFF guard and unknown-result status readback (0 exchange effects)")
    }

    private static func settingsControlButtons(in root: NSView) throws -> (
        emergency: NSButton,
        refresh: NSButton,
        automation: NSButton
    ) {
        func descendants(of view: NSView) -> [NSView] {
            [view] + view.subviews.flatMap(descendants)
        }
        let buttons = descendants(of: root).compactMap { $0 as? NSButton }
        guard let emergency = buttons.first(where: { $0.title == "긴급 중단" }),
              let refresh = buttons.first(where: { $0.accessibilityLabel() == "전략 제어 서버 상태 새로고침" }),
              let automation = buttons.first(where: { $0.title == "전략 자동매매 관리…" }),
              !buttons.contains(where: { ["실거래 시작 (Start)", "실거래 재개 (Resume)", "실거래 중단 (Pause)"].contains($0.title) }) else {
            throw Failure.assertion("production Settings view omitted its native control buttons")
        }
        return (emergency, refresh, automation)
    }

    private static func validateDashboardHistoryColumnsAndStatusLabels() throws {
        _ = NSApplication.shared
        let previousTextScale = TerminalTheme.textScale
        defer { TerminalTheme.setTextScale(previousTextScale) }
        try validateVisibleHistorySelectionAtOneX()
        TerminalTheme.setTextScale(2.0)
        let base = FileManager.default.temporaryDirectory
            .appendingPathComponent("sky-backcraft-dashboard-history-scale-\(UUID().uuidString)", isDirectory: true)
        defer { try? FileManager.default.removeItem(at: base) }
        let store = try ConfigurationStore(supportDirectory: base.appendingPathComponent("support", isDirectory: true))
        let persistedRunID = "MOCK-GUI-RUN-001"
        try createDashboardHistoryDatabase(at: store, runId: persistedRunID)
        let historyStore = HistoryWorkspaceStore(rootDirectory: store.supportDirectory)
        try historyStore.saveHistoryState(HistoryPresentationState(
            search: persistedRunID,
            pinnedRunIds: [persistedRunID],
            selectedRunIds: [persistedRunID]
        ))
        let testNow = Date()
        let timestamp = ISO8601DateFormatter().string(from: testNow)
        let session = URLSession(configuration: .ephemeral)
        defer { session.invalidateAndCancel() }
        let dashboard = DashboardWindowController(
            store: store,
            liveService: UpbitLiveService(client: NativeMCPClient(session: session)),
            wallNow: { testNow },
            monotonicNow: { 100 }
        )
        guard let window = dashboard.window, let content = window.contentView else {
            throw Failure.assertion("offscreen production Dashboard did not create its window")
        }
        func descendants(of view: NSView) -> [NSView] {
            [view] + view.subviews.flatMap(descendants)
        }
        guard let sidebar = descendants(of: content).compactMap({ $0 as? NSTableView })
            .first(where: { $0.identifier?.rawValue == "DashboardNavigationTable" }) else {
            throw Failure.assertion("production Dashboard navigation table was not available offscreen")
        }
        let expectedRoutes: Set<String> = [
            "성과 분석", "실행 이력", "정책 카탈로그", "지속 모의 · 작업",
            "전략 자동화", "실계좌 및 주문 조회", "정책 삭제"
        ]
        var routeRows: [String: Int] = [:]
        var groupRows: [Int] = []
        for row in 0..<sidebar.numberOfRows {
            if dashboard.tableView(sidebar, isGroupRow: row) {
                groupRows.append(row)
                guard !dashboard.tableView(sidebar, shouldSelectRow: row) else {
                    throw Failure.assertion("Dashboard navigation group rows must not be selectable")
                }
            } else if let cell = sidebar.view(atColumn: 0, row: row, makeIfNecessary: true) as? NSTableCellView,
                      let title = cell.textField?.stringValue {
                routeRows[title] = row
                guard dashboard.tableView(sidebar, shouldSelectRow: row) else {
                    throw Failure.assertion("Dashboard route leaf \(title) must be selectable")
                }
            }
        }
        guard groupRows.count == 4,
              sidebar.numberOfRows == expectedRoutes.count + groupRows.count,
              Set(routeRows.keys) == expectedRoutes else {
            throw Failure.assertion("Dashboard navigation must expose four structural groups and all seven real destinations")
        }
        func selectSidebarRoute(named title: String) throws {
            guard let row = routeRows[title] else {
                throw Failure.assertion("Dashboard route \(title) was not reachable by its leaf label")
            }
            sidebar.selectRowIndexes(IndexSet(integer: row), byExtendingSelection: false)
            dashboard.tableViewSelectionDidChange(Notification(name: Notification.Name("test"), object: sidebar))
            content.layoutSubtreeIfNeeded()
            guard let selected = sidebar.view(atColumn: 0, row: sidebar.selectedRow, makeIfNecessary: true) as? NSTableCellView,
                  selected.textField?.stringValue == title else {
                throw Failure.assertion("Dashboard did not preserve selected route \(title)")
            }
        }

        func assertVisibleHistoryLayout(_ stage: String) throws {
            content.layoutSubtreeIfNeeded()
            let views = descendants(of: content)
            guard window.isVisible else { throw Failure.assertion("\(stage): Dashboard window was not shown") }
            guard let workspace = views.first(where: { $0.accessibilityIdentifier() == "DashboardRunHistoryWorkspace" }) else {
                throw Failure.assertion("\(stage): History workspace container was not installed")
            }
            guard let workbench = views.compactMap({ $0 as? NSSplitView })
                .first(where: { $0.identifier?.rawValue == "DashboardRunHistoryWorkbench" }) else {
                throw Failure.assertion("\(stage): History workbench was not installed")
            }
            guard let tablePane = views.first(where: { $0.accessibilityIdentifier() == "DashboardRunHistoryTablePane" }),
                  let inspectorPane = views.first(where: { $0.accessibilityIdentifier() == "DashboardRunHistoryInspectorPane" }) else {
                throw Failure.assertion("\(stage): History split panes were not installed")
            }
            guard let tableScroll = views.compactMap({ $0 as? NSScrollView })
                .first(where: { $0.identifier?.rawValue == "DashboardRunHistoryTableScroll" }) else {
                throw Failure.assertion("\(stage): History table scroll view was not installed")
            }
            guard let table = views.compactMap({ $0 as? NSTableView }).first(where: { table in
                table.tableColumns.contains(where: { $0.identifier.rawValue == "Date" })
                    && table.tableColumns.contains(where: { $0.identifier.rawValue == "Markets" })
            }) else { throw Failure.assertion("\(stage): History table was not installed") }
            guard let search = views.compactMap({ $0 as? NSSearchField })
                .first(where: { $0.accessibilityLabel() == "실행 이력 검색" }),
                  let appliedFilters = views.compactMap({ $0 as? NSTextField })
                    .first(where: { $0.accessibilityIdentifier() == "DashboardRunHistoryAppliedFiltersSummary" }),
                  let clearFilters = views.compactMap({ $0 as? NSButton })
                    .first(where: { $0.accessibilityLabel() == "실행 이력 필터 모두 지우기" }),
                  let csv = views.compactMap({ $0 as? NSButton }).first(where: { $0.title == "검색 결과 CSV" }),
                  let inspector = views.first(where: { $0.accessibilityIdentifier() == "DashboardRunHistorySelectionInspector" }),
                  let identityRows = views.compactMap({ $0 as? NSStackView })
                    .first(where: { $0.accessibilityIdentifier() == "DashboardRunHistoryIdentity" }),
                  let runIdentityValue = descendants(of: identityRows).compactMap({ $0 as? NSTextField })
                    .first(where: { $0.stringValue == persistedRunID }),
                  let feedback = views.compactMap({ $0 as? NSTextField })
                    .first(where: { $0.accessibilityLabel() == "실행 이력 보기 상태 및 피드백" }) else {
                throw Failure.assertion("\(stage): A History search, filter, CSV, inspector, or feedback control was not installed")
            }
            func hasVisibleRect(_ view: NSView) -> Bool {
                view.visibleRect.width > 0 && view.visibleRect.height > 0
            }
            workbench.layoutSubtreeIfNeeded()
            guard workspace.window === window,
                  !workspace.isHidden,
                  hasVisibleRect(workspace), hasVisibleRect(workbench),
                  workbench.arrangedSubviews.count == 2,
                  workbench.isVertical,
                  workbench.frame.width > 600, workbench.frame.height > 200,
                  tablePane.frame.width > 400, tablePane.frame.height > 100,
                  inspectorPane.frame.width > 200, inspectorPane.frame.height > 100,
                  tableScroll.frame.width > 400, tableScroll.frame.height > 80,
                  hasVisibleRect(tablePane), hasVisibleRect(inspectorPane), hasVisibleRect(tableScroll),
                  table.window === window,
                  hasVisibleRect(table), hasVisibleRect(search), hasVisibleRect(appliedFilters),
                  hasVisibleRect(clearFilters), hasVisibleRect(csv), hasVisibleRect(inspector),
                  hasVisibleRect(identityRows), hasVisibleRect(runIdentityValue), hasVisibleRect(feedback),
                  table.numberOfRows == 1, table.selectedRow >= 0,
                  identityRows.arrangedSubviews.count == 8,
                  identityRows.arrangedSubviews.first.map({ ($0 as? NSStackView)?.orientation == .vertical }) == true,
                  search.stringValue == persistedRunID,
                  appliedFilters.stringValue.contains("검색 \(persistedRunID)"),
                  clearFilters.isEnabled,
                  csv.window === window, inspector.window === window, feedback.window === window,
                  feedback.stringValue.contains("필터·고정·선택은 이 기기에 저장됩니다.") else {
                throw Failure.assertion("\(stage): History panes collapsed or restored selection state was lost")
            }
            let restored = try historyStore.loadHistoryState()
            guard restored?.selectedRunIds == [persistedRunID], restored?.pinnedRunIds == [persistedRunID] else {
                throw Failure.assertion("\(stage): History replaced the persisted selection or pinned ID")
            }
        }

        try selectSidebarRoute(named: "전략 자동화")
        guard descendants(of: content).contains(where: { $0 is AutomationWorkspaceView }) else {
            throw Failure.assertion("selecting the automation route did not mount its existing workspace")
        }
        window.setContentSize(NSSize(width: 1200, height: 800))
        window.makeKeyAndOrderFront(nil)
        content.layoutSubtreeIfNeeded()
        try selectSidebarRoute(named: "실행 이력")
        try assertVisibleHistoryLayout("initial selection")
        guard let historyTable = descendants(of: content).compactMap({ $0 as? NSTableView }).first(where: { table in
            table.tableColumns.contains(where: { $0.identifier.rawValue == "Date" })
                && table.tableColumns.contains(where: { $0.identifier.rawValue == "Markets" })
        }),
        let dateColumn = historyTable.tableColumns.first(where: { $0.identifier.rawValue == "Date" }),
        let marketColumn = historyTable.tableColumns.first(where: { $0.identifier.rawValue == "Markets" }),
        dateColumn.width == 300,
        marketColumn.width == 220,
        historyTable.rowHeight == 64,
        historyTable.enclosingScrollView?.hasHorizontalScroller == true else {
            throw Failure.assertion("Dashboard history columns and row height did not scale with 2x text")
        }

        try selectSidebarRoute(named: "정책 카탈로그")
        window.setContentSize(NSSize(width: 1320, height: 840))
        try selectSidebarRoute(named: "실행 이력")
        try assertVisibleHistoryLayout("reselection after resize")

        try selectSidebarRoute(named: "실계좌 및 주문 조회")
        func labels() -> [String] { descendants(of: content).compactMap { ($0 as? NSTextField)?.stringValue } }
        guard let contextHost = descendants(of: content).first(where: {
            $0.identifier?.rawValue == "DashboardGlobalContextStrip"
        }), let contextContainer = contextHost.superview,
        let realtimeDisclosure = descendants(of: content).compactMap({ $0 as? NSButton })
            .first(where: { $0.accessibilityLabel() == "실시간 피드 상세 상태" }) else {
            throw Failure.assertion("Dashboard context host or downstream realtime disclosure was not mounted")
        }
        func hasContextDisclosureAnchor() -> Bool {
            contextContainer.constraints.contains { constraint in
                (constraint.firstItem as? NSView) === realtimeDisclosure
                    && (constraint.secondItem as? NSView) === contextHost
                    && constraint.firstAttribute == .top
                    && constraint.secondAttribute == .bottom
            }
        }
        guard hasContextDisclosureAnchor() else {
            throw Failure.assertion("Dashboard realtime disclosure was not anchored below the mounted context host")
        }
        let unknownLabels = labels()
        let initialLiveContext = descendants(of: contextHost).compactMap { $0 as? NSTextField }
            .first(where: { $0.accessibilityLabel() == "실거래" })?.stringValue
        guard unknownLabels.contains("잔고·주문 조회: 상태 미확인"),
              unknownLabels.contains("실주문 자동매매 상태 미확인 · 주문 기능 사용 불가"),
              !unknownLabels.contains(where: { $0.contains("실주문 자동매매: 꺼짐") }) else {
            throw Failure.assertion("Dashboard UNKNOWN state was incorrectly presented as confirmed OFF or read-connected")
        }

        let disabled = TradingControlSnapshot(
            exchange_read_mode: "LIVE_READ",
            live_trading_enabled: false,
            state: "DISABLED_BY_CONFIG",
            sequence: 31,
            observed_at_utc: timestamp,
            order_admission_blocked: true,
            emergency_latched: false,
            denied_code: nil,
            receipt: nil
        )
        dashboard.updateControlSnapshot(disabled)
        content.layoutSubtreeIfNeeded()
        let updatedContextHost = descendants(of: content).first(where: {
            $0.identifier?.rawValue == "DashboardGlobalContextStrip"
        })
        let updatedLiveContext = updatedContextHost.map { host in
            descendants(of: host).compactMap { $0 as? NSTextField }
                .first(where: { $0.accessibilityLabel() == "실거래" })?.stringValue
        } ?? nil
        guard updatedContextHost === contextHost,
              updatedLiveContext != initialLiveContext,
              updatedLiveContext?.contains("실주문 자동매매: 꺼짐") == true,
              hasContextDisclosureAnchor() else {
            throw Failure.assertion("Dashboard context update replaced the host or detached the realtime disclosure constraint")
        }
        let disabledLabels = labels()
        guard disabledLabels.contains("잔고·주문 조회: 상태 미확인"),
              disabledLabels.contains("실주문 자동매매: 꺼짐 — 실제 주문 전송 안 함") else {
            throw Failure.assertion("Dashboard did not keep provider-read status separate from confirmed real-order OFF")
        }

        let enabled = TradingControlSnapshot(
            exchange_read_mode: "LIVE_READ",
            live_trading_enabled: true,
            state: "STANDBY",
            sequence: 32,
            observed_at_utc: timestamp,
            order_admission_blocked: true,
            emergency_latched: false,
            denied_code: nil,
            receipt: nil
        )
        dashboard.updateControlSnapshot(enabled)
        let enabledLabels = labels()
        guard enabledLabels.contains("실주문 자동매매 설정: 켜짐 · 신규 주문 차단"),
              !enabledLabels.contains(where: { $0.contains("실주문 자동매매: 꺼짐") }) else {
            throw Failure.assertion("Dashboard hardcoded real-order automation OFF for a fresh enabled snapshot")
        }

        window.close()
        print("PASS visible History workbench at 1200x800 and after resize/reselection, saved selection preserved, plus separate UNKNOWN/OFF/enabled status labels")
    }

    private static func validateVisibleHistorySelectionAtOneX() throws {
        TerminalTheme.setTextScale(1.0)
        let base = FileManager.default.temporaryDirectory
            .appendingPathComponent("sky-backcraft-dashboard-history-1x-\(UUID().uuidString)", isDirectory: true)
        defer { try? FileManager.default.removeItem(at: base) }
        let store = try ConfigurationStore(supportDirectory: base.appendingPathComponent("support", isDirectory: true))
        let persistedRunID = "MOCK-GUI-RUN-001"
        try createDashboardHistoryDatabase(at: store, runId: persistedRunID)
        let historyStore = HistoryWorkspaceStore(rootDirectory: store.supportDirectory)
        try historyStore.saveHistoryState(HistoryPresentationState(
            search: persistedRunID,
            pinnedRunIds: [persistedRunID],
            selectedRunIds: [persistedRunID]
        ))
        let session = URLSession(configuration: .ephemeral)
        defer { session.invalidateAndCancel() }
        let dashboard = DashboardWindowController(
            store: store,
            liveService: UpbitLiveService(client: NativeMCPClient(session: session))
        )
        guard let window = dashboard.window, let content = window.contentView else {
            throw Failure.assertion("1x production Dashboard did not create its window")
        }
        func descendants(of view: NSView) -> [NSView] {
            [view] + view.subviews.flatMap(descendants)
        }
        guard let sidebar = descendants(of: content).compactMap({ $0 as? NSTableView })
            .first(where: { $0.identifier?.rawValue == "DashboardNavigationTable" }) else {
            throw Failure.assertion("1x Dashboard navigation table was not installed")
        }
        window.setContentSize(NSSize(width: 1200, height: 800))
        window.makeKeyAndOrderFront(nil)
        content.layoutSubtreeIfNeeded()
        guard let historyRow = (0..<sidebar.numberOfRows).first(where: { row in
            (sidebar.view(atColumn: 0, row: row, makeIfNecessary: true) as? NSTableCellView)?.textField?.stringValue == "실행 이력"
        }) else {
            throw Failure.assertion("1x History destination was not present in the real sidebar")
        }
        sidebar.selectRowIndexes(IndexSet(integer: historyRow), byExtendingSelection: false)
        dashboard.tableViewSelectionDidChange(Notification(name: Notification.Name("test-history-route"), object: sidebar))
        content.layoutSubtreeIfNeeded()
        let views = descendants(of: content)
        guard let workspace = views.first(where: { $0.accessibilityIdentifier() == "DashboardRunHistoryWorkspace" }),
              let workbench = views.compactMap({ $0 as? NSSplitView })
                .first(where: { $0.identifier?.rawValue == "DashboardRunHistoryWorkbench" }),
              let historyTable = views.compactMap({ $0 as? NSTableView }).first(where: { table in
                  table.tableColumns.contains(where: { $0.identifier.rawValue == "Date" })
                      && table.tableColumns.contains(where: { $0.identifier.rawValue == "Markets" })
              }),
              let search = views.compactMap({ $0 as? NSSearchField })
                .first(where: { $0.accessibilityLabel() == "실행 이력 검색" }),
              let appliedFilters = views.compactMap({ $0 as? NSTextField })
                .first(where: { $0.accessibilityIdentifier() == "DashboardRunHistoryAppliedFiltersSummary" }),
              let clearFilters = views.compactMap({ $0 as? NSButton })
                .first(where: { $0.accessibilityLabel() == "실행 이력 필터 모두 지우기" }),
              let inspector = views.first(where: { $0.accessibilityIdentifier() == "DashboardRunHistorySelectionInspector" }),
              let identityRows = views.compactMap({ $0 as? NSStackView })
                .first(where: { $0.accessibilityIdentifier() == "DashboardRunHistoryIdentity" }),
              let runIdentityValue = descendants(of: identityRows).compactMap({ $0 as? NSTextField })
                .first(where: { $0.stringValue == persistedRunID }),
              let csv = views.compactMap({ $0 as? NSButton }).first(where: { $0.title == "검색 결과 CSV" }),
              workspace.window === window, !workspace.isHidden,
              workspace.visibleRect.width > 0, workspace.visibleRect.height > 0,
              workbench.visibleRect.width > 0, workbench.visibleRect.height > 0,
              workbench.arrangedSubviews.count == 2, workbench.isVertical,
              workbench.frame.width > 600, workbench.frame.height > 200,
              historyTable.window === window, historyTable.numberOfRows == 1, historyTable.selectedRow >= 0,
              historyTable.visibleRect.width > 0, historyTable.visibleRect.height > 0,
              search.stringValue == persistedRunID,
              search.visibleRect.width > 0, search.visibleRect.height > 0,
              appliedFilters.stringValue.contains("검색 \(persistedRunID)"),
              appliedFilters.visibleRect.width > 0, appliedFilters.visibleRect.height > 0,
              clearFilters.isEnabled, clearFilters.visibleRect.width > 0, clearFilters.visibleRect.height > 0,
              identityRows.arrangedSubviews.count == 8,
              identityRows.arrangedSubviews.first.map({ $0.frame.width > 0 && $0.frame.height > 0 }) == true,
              identityRows.visibleRect.width > 0, identityRows.visibleRect.height > 0,
              runIdentityValue.visibleRect.width > 0, runIdentityValue.visibleRect.height > 0,
              inspector.window === window, inspector.visibleRect.width > 0, inspector.visibleRect.height > 0,
              csv.window === window, csv.visibleRect.width > 0, csv.visibleRect.height > 0 else {
            throw Failure.assertion("1x sidebar navigation failed to install visible History controls and the restored run identity")
        }
        let restored = try historyStore.loadHistoryState()
        guard restored?.selectedRunIds == [persistedRunID], restored?.pinnedRunIds == [persistedRunID] else {
            throw Failure.assertion("1x History route changed the persisted selected or pinned run ID")
        }
        window.close()
        print("PASS 1x visible History body and persisted selected-run identity after native sidebar navigation")
    }

    private static func createDashboardHistoryDatabase(at store: ConfigurationStore, runId: String) throws {
        let research = store.dataDirectory(for: .none)
        try FileManager.default.createDirectory(at: research, withIntermediateDirectories: true)
        let databaseURL = research.appendingPathComponent("lab.sqlite")
        var optionalDB: OpaquePointer?
        let openResult = sqlite3_open_v2(
            databaseURL.path, &optionalDB,
            SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, nil
        )
        guard openResult == SQLITE_OK, let db = optionalDB else {
            throw Failure.assertion("Dashboard History SQLite fixture open returned \(openResult)")
        }
        defer { sqlite3_close(db) }
        let planID = "MOCK-PLAN-001"
        let policyID = "MOCK-POLICY-001"
        let revisionID = "MOCK-REVISION-001"
        let digest = String(repeating: "a", count: 64)
        let manifest = #"{"created_at":"2025-01-02T03:04:05Z","code_revision":"mock-revision","engine_version":"mock-engine","mode":"BACKTEST"}"#
        let plan = #"{"spec":{"dataset_ids":["MOCK-DATASET-001"],"markets":["BTC"],"range":{"start":"2025-01-01T00:00:00Z","end":"2025-01-03T00:00:00Z"},"policy_selections":[{"revision_id":"MOCK-REVISION-001"}],"capital_mode":"VIRTUAL","initial_cash":"10000","costs":{"assumption_label":"MOCK-COSTS"}}}"#
        let sql = """
        CREATE TABLE runs (id TEXT PRIMARY KEY, state TEXT NOT NULL, created_at_ms INTEGER NOT NULL, plan_id TEXT NOT NULL, ledger_state TEXT NOT NULL, manifest_json TEXT NOT NULL);
        CREATE TABLE plans (id TEXT PRIMARY KEY, resolved_plan_json TEXT NOT NULL);
        CREATE TABLE run_models (run_id TEXT NOT NULL, model_id TEXT NOT NULL, market TEXT NOT NULL, policy_id TEXT, policy_revision_id TEXT);
        CREATE TABLE plan_datasets (plan_id TEXT NOT NULL, dataset_id TEXT NOT NULL);
        CREATE TABLE policies (policy_id TEXT PRIMARY KEY, family TEXT NOT NULL, origin TEXT NOT NULL, name TEXT NOT NULL, head_revision_id TEXT NOT NULL, revision_count INTEGER NOT NULL);
        CREATE TABLE policy_revisions (revision_id TEXT PRIMARY KEY, policy_id TEXT NOT NULL, revision_number INTEGER NOT NULL, name TEXT NOT NULL, definition_digest TEXT NOT NULL);
        CREATE TABLE validation_results (run_id TEXT NOT NULL, check_id TEXT NOT NULL, status TEXT NOT NULL, report_json TEXT NOT NULL);
        CREATE TABLE jobs (id TEXT NOT NULL, request_id TEXT NOT NULL, payload_kind TEXT NOT NULL, current_status TEXT NOT NULL, created_at_ms INTEGER NOT NULL);
        CREATE TABLE collection_schedules (id TEXT NOT NULL, status TEXT NOT NULL, created_at_ms INTEGER NOT NULL, next_action_at_ms INTEGER, last_success_dataset_id TEXT, last_success_boundary_ms INTEGER, failure_json TEXT);
        INSERT INTO plans (id, resolved_plan_json) VALUES ('\(planID)', '\(plan)');
        INSERT INTO runs (id, state, created_at_ms, plan_id, ledger_state, manifest_json)
          VALUES ('\(runId)', 'COMPLETED', 1735796405000, '\(planID)', 'CANONICAL', '\(manifest)');
        INSERT INTO run_models (run_id, model_id, market, policy_id, policy_revision_id)
          VALUES ('\(runId)', 'MOCK-MODEL-001', 'BTC', '\(policyID)', '\(revisionID)');
        INSERT INTO plan_datasets (plan_id, dataset_id) VALUES ('\(planID)', 'MOCK-DATASET-001');
        INSERT INTO policies (policy_id, family, origin, name, head_revision_id, revision_count)
          VALUES ('\(policyID)', 'MOCK', 'USER', 'Mock policy', '\(revisionID)', 1);
        INSERT INTO policy_revisions (revision_id, policy_id, revision_number, name, definition_digest)
          VALUES ('\(revisionID)', '\(policyID)', 1, 'Mock revision', '\(digest)');
        """
        var errorPointer: UnsafeMutablePointer<CChar>?
        let result = sqlite3_exec(db, sql, nil, nil, &errorPointer)
        guard result == SQLITE_OK else {
            let detail = errorPointer.map { String(cString: $0) } ?? String(cString: sqlite3_errmsg(db))
            if let errorPointer { sqlite3_free(errorPointer) }
            throw Failure.assertion("Dashboard History SQLite fixture failed: \(detail)")
        }
    }

    private static func validateExplicitPaperFeedGapRecoveryResume() throws {
        _ = NSApplication.shared
        let base = FileManager.default.temporaryDirectory
            .appendingPathComponent("sky-backcraft-paper-feed-gap-ui-\(UUID().uuidString)", isDirectory: true)
        defer { try? FileManager.default.removeItem(at: base) }
        let now = ISO8601DateFormatter().string(from: Date())
        let server = PaperRecoveryUIFixtureServer(observedAt: now)
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [PaperWriteRecoveryURLProtocol.self]
        let session = URLSession(configuration: configuration)
        defer {
            session.invalidateAndCancel()
            PaperWriteRecoveryURLProtocol.clear()
        }
        PaperWriteRecoveryURLProtocol.install { server.response(for: $0) }
        let client = NativeMCPClient(session: session)
        guard client.configure(port: 65_433, token: String(repeating: "b", count: 64)) != nil else {
            throw Failure.assertion("paper recovery UI mock transport did not configure")
        }
        let workspace = PaperTradingWorkspaceView(
            service: PaperSessionMCPService(client: client),
            jobsQueueView: NSView(frame: .zero),
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
        workspace.frame = root.bounds
        root.addSubview(workspace)
        window.contentView = root
        window.contentView?.layoutSubtreeIfNeeded()
        window.displayIfNeeded()
        defer {
            workspace.setWorkspaceVisible(false)
            window.close()
        }

        func descendants(of view: NSView) -> [NSView] {
            [view] + view.subviews.flatMap(descendants)
        }
        func resumeButton() -> NSButton? {
            descendants(of: workspace).compactMap { $0 as? NSButton }.first {
                $0.title == "세션 재개" || $0.title == "복구 후 재개"
            }
        }
        func summary() -> String? {
            descendants(of: workspace).compactMap { $0 as? NSTextField }
                .first(where: { $0.stringValue.contains("세션 ID:") })?.stringValue
        }

        workspace.setWorkspaceVisible(true)
        _ = try waitForPolicyDeletionResult({ () -> Bool? in
            guard resumeButton()?.title == "복구 후 재개",
                  resumeButton()?.isEnabled == true,
                  summary()?.contains("FEED_GAP") == true else { return nil }
            return true
        }, label: "explicit FEED_GAP recovery resume presentation")
        guard server.snapshot().writeCalls.isEmpty else {
            throw Failure.assertion("loading a feed-gap session triggered automatic recovery or a write")
        }

        resumeButton()?.performClick(nil)
        _ = try waitForPolicyDeletionResult({ () -> Bool? in
            let snapshot = server.snapshot()
            guard snapshot.writeCalls.count == 1,
                  snapshot.writeCalls[0].action == "resume",
                  snapshot.writeCalls[0].request_id == "paper-resume-\(PaperRecoveryUIFixtureServer.sessionID)-7",
                  snapshot.writeCalls[0].expected_sequence == 7,
                  summary()?.contains("RUNNING") == true,
                  resumeButton()?.isEnabled == false else { return nil }
            return true
        }, label: "explicit feed-gap Resume CAS receipt")

        server.setSession(state: "RECOVERY_REQUIRED", sequence: 9, recoveryCause: "CHECKPOINT_INTEGRITY")
        workspace.reloadWorkspace(preserveSelection: true)
        _ = try waitForPolicyDeletionResult({ () -> Bool? in
            guard resumeButton()?.title == "세션 재개",
                  resumeButton()?.isEnabled == false,
                  summary()?.contains("CHECKPOINT_INTEGRITY") == true else { return nil }
            return true
        }, label: "integrity recovery remains blocked with its cause")
        resumeButton()?.performClick(nil)
        guard server.snapshot().writeCalls.count == 1 else {
            throw Failure.assertion("non-feed-gap recovery reached paper_session_write")
        }

        server.setSession(state: "RECOVERY_REQUIRED", sequence: 10, recoveryCause: nil)
        workspace.reloadWorkspace(preserveSelection: true)
        _ = try waitForPolicyDeletionResult({ () -> Bool? in
            guard resumeButton()?.isEnabled == false,
                  summary()?.contains("복구 재개 차단") == true,
                  summary()?.contains("미확인") == true else { return nil }
            return true
        }, label: "missing recovery cause remains unknown and blocked")

        server.setSession(state: "PAUSED", sequence: 11, recoveryCause: nil)
        workspace.reloadWorkspace(preserveSelection: true)
        _ = try waitForPolicyDeletionResult({ () -> Bool? in
            guard resumeButton()?.title == "세션 재개",
                  resumeButton()?.isEnabled == true else { return nil }
            return true
        }, label: "ordinary PAUSED resume remains available")
        resumeButton()?.performClick(nil)
        _ = try waitForPolicyDeletionResult({ () -> Bool? in
            let snapshot = server.snapshot()
            guard snapshot.writeCalls.count == 2,
                  snapshot.writeCalls[1].action == "resume",
                  snapshot.writeCalls[1].request_id == "paper-resume-\(PaperRecoveryUIFixtureServer.sessionID)-11",
                  snapshot.writeCalls[1].expected_sequence == 11,
                  summary()?.contains("RUNNING") == true else { return nil }
            return true
        }, label: "ordinary paused Resume wire remains unchanged")

        server.setSession(state: "PAUSED", sequence: 13, recoveryCause: nil)
        workspace.reloadWorkspace(preserveSelection: true)
        _ = try waitForPolicyDeletionResult({ () -> Bool? in
            guard resumeButton()?.title == "세션 재개",
                  resumeButton()?.isEnabled == true else { return nil }
            return true
        }, label: "paused Resume available before unknown-write fence")
        server.makeNextResumeResponseUnknown()
        resumeButton()?.performClick(nil)
        _ = try waitForPolicyDeletionResult({ () -> Bool? in
            let snapshot = server.snapshot()
            guard snapshot.writeCalls.count == 3,
                  snapshot.readbackQueryCount == 1,
                  resumeButton()?.isEnabled == false else { return nil }
            return true
        }, label: "unknown Resume write persists its UI fence")
        let unknownWriteCount = server.snapshot().writeCalls.count
        resumeButton()?.performClick(nil)
        guard server.snapshot().writeCalls.count == unknownWriteCount else {
            throw Failure.assertion("Resume was resent while its prior write outcome remained unknown")
        }
        print("PASS paper FEED_GAP explicit Resume, unknown-cause fence, and ordinary PAUSED Resume")
    }

    private static func validateStrategyChartReadPacing() throws {
        let spacing = StrategyChartReadPacing.minimumInterval
        var currentTime: TimeInterval = 100
        var lastStartedAt: TimeInterval?
        var starts: [TimeInterval] = []

        for _ in 0..<12 {
            currentTime += StrategyChartReadPacing.delay(lastStartedAt: lastStartedAt, now: currentTime)
            let earlyWakeDelay = StrategyChartReadPacing.delay(lastStartedAt: lastStartedAt, now: currentTime)
            guard earlyWakeDelay <= 0.000_001 else {
                throw Failure.assertion("chart read pacing did not become ready at its monotonic deadline")
            }
            starts.append(currentTime)
            lastStartedAt = currentTime
            currentTime += 0.005 // Simulate a fast response to expose queue-drain bursts.
        }

        for (earlier, later) in zip(starts, starts.dropFirst()) {
            guard later - earlier >= spacing - 0.000_001 else {
                throw Failure.assertion("compacted chart calls started less than 200 ms apart")
            }
        }

        let postsPerChartRead = StrategyChartReadPacing.httpPostsPerChartRead
        let worstOneSecondPostCount = starts.map { windowEnd in
            starts.filter { $0 > windowEnd - 1 && $0 <= windowEnd }.count * postsPerChartRead
        }.max() ?? 0
        guard worstOneSecondPostCount <= 12, worstOneSecondPostCount < 20 else {
            throw Failure.assertion("compacted chart reads can consume the backend's 20 POST/s budget")
        }

        let firstPendingDelay = StrategyChartReadPacing.delay(lastStartedAt: 200, now: 200.05)
        let pendingDelayAfterCancellation = StrategyChartReadPacing.delay(lastStartedAt: 200, now: 200.10)
        guard abs(firstPendingDelay - 0.15) <= 0.000_001,
              abs(pendingDelayAfterCancellation - 0.10) <= 0.000_001 else {
            throw Failure.assertion("cancelled chart start wait consumed a pacing slot or changed queue order")
        }
        print("PASS strategy chart read pacing: 12 fast cards, two POSTs per call, cancellable wait and ordered start budget")
    }

    private static func validateStrategyBatchRecordAssembly() throws {
        let policyId = "policy-target"
        let revisionId = "policy-revision-target"
        let runs = [
            makeStrategyRun("run-z"),
            makeStrategyRun("run-a"),
            makeStrategyRun("run-empty"),
            makeStrategyRun("run-filtered")
        ]
        // Batch DatabaseReader rows arrive ordered by run ID, then market/model.
        let comparisons = [
            makeStrategyComparison("run-a", "model-a-btc", "KRW-BTC", policyId, revisionId),
            makeStrategyComparison("run-a", "model-a-eth", "KRW-ETH", policyId, revisionId),
            makeStrategyComparison("run-a", "model-a-old-revision", "KRW-XRP", policyId, "policy-revision-old"),
            makeStrategyComparison("run-filtered", "model-wrong-policy", "KRW-BTC", "policy-other", revisionId),
            makeStrategyComparison("run-z", "model-z-btc", "KRW-BTC", policyId, revisionId),
            makeStrategyComparison("run-z", "model-z-eth", "KRW-ETH", policyId, revisionId)
        ]

        let assembled = StrategyPerformanceView.assembleCardRecords(
            runs: runs,
            comparisons: comparisons,
            policyId: policyId,
            policyRevisionScopeId: revisionId
        )
        guard assembled.records.map({ $0.run.id }) == ["run-z", "run-z", "run-a", "run-a"],
              assembled.records.map({ $0.model.modelId }) == [
                "model-z-btc", "model-z-eth", "model-a-btc", "model-a-eth"
              ],
              assembled.unavailableRuns.map(\.id) == ["run-empty", "run-filtered"] else {
            throw Failure.assertion("batched strategy comparisons changed run order, per-run order, scope filters, or unavailable runs")
        }
        print("PASS strategy batch assembly: history order, per-run order, exact policy/revision filter, unavailable runs")
    }

    private static func validateStrategyRefreshFailureRetainsCachedPage() throws {
        let retainedPage = StrategyPerformanceView.pagerPresentation(
            currentPageIndex: 2,
            pageCount: 5,
            resultCount: 49
        )
        guard retainedPage.pageIndex == 2,
              retainedPage.label == "3 / 5 · 총 49개",
              retainedPage.previousEnabled,
              retainedPage.nextEnabled else {
            throw Failure.assertion("same-policy refresh failure moved cached page three or disabled its pager controls")
        }

        let pageSelectedWhileRefreshing = StrategyPerformanceView.pagerPresentation(
            currentPageIndex: 3,
            pageCount: 5,
            resultCount: 49
        )
        guard pageSelectedWhileRefreshing.pageIndex == 3,
              pageSelectedWhileRefreshing.label == "4 / 5 · 총 49개",
              pageSelectedWhileRefreshing.previousEnabled,
              pageSelectedWhileRefreshing.nextEnabled else {
            throw Failure.assertion("refresh completion did not preserve the page selected while the request was in flight")
        }
        print("PASS strategy refresh failure pager: cached non-first page and in-flight user page changes remain coherent")
    }

    private static func validateLedgerMCPReaderCancellationOwnership() throws {
        let reader = LedgerMCPReader(client: NativeMCPClient())
        var returned = false
        var callbackWasSynchronous = false
        var callbackCount = 0
        var succeeded = false
        let request = reader.marks(runId: "run-unconfigured", modelId: "model-unconfigured", expectedCount: 0) { result in
            callbackCount += 1
            callbackWasSynchronous = !returned
            if case .success = result { succeeded = true }
        }
        returned = true
        guard callbackWasSynchronous, callbackCount == 1, !succeeded, !request.isPending else {
            throw Failure.assertion("synchronous ledger read failure lost callback or returned a pending request")
        }
        print("PASS ledger chart read cancellation ownership: synchronous failure completes returned request once")
    }

    private static func makeStrategyRun(_ id: String) -> RealRun {
        RealRun(
            id: id,
            state: "COMPLETED",
            createdDateStr: "2026-10-21 00:00:00",
            createdAtMilliseconds: 1,
            createdAtUTC: nil,
            planId: "plan-\(id)",
            ledgerState: "CANONICAL",
            mode: "BACKTEST",
            engineVersion: nil,
            codeRevision: nil,
            datasetIds: nil,
            policyRevisionIds: nil,
            markets: ["KRW-BTC"],
            windowStartUTC: nil,
            windowEndUTC: nil,
            capitalMode: nil,
            initialCashDecimal: nil,
            costAssumptionLabel: nil,
            terminalPolicy: nil,
            f08Status: nil,
            f08CheckedFills: nil,
            f08CheckedMarks: nil,
            f08FindingsCount: nil
        )
    }

    private static func makeStrategyComparison(
        _ runId: String,
        _ modelId: String,
        _ market: String,
        _ policyId: String?,
        _ policyRevisionId: String?
    ) -> RealModelComparison {
        RealModelComparison(
            runId: runId,
            modelId: modelId,
            market: market,
            status: "COMPLETED",
            policyId: policyId,
            policyRevisionId: policyRevisionId,
            initialEquityDecimal: Decimal(100),
            finalEquityDecimal: Decimal(110),
            netPnlDecimal: Decimal(10),
            netReturnDecimal: Decimal(string: "0.1"),
            maxDrawdownDecimal: Decimal.zero,
            turnoverDecimal: Decimal.zero,
            cumulativeFeesDecimal: Decimal.zero,
            fillCount: 0,
            accountMarkCount: 1,
            episodeCount: 0,
            closedEpisodes: 0,
            f08Status: nil
        )
    }
#endif

#if PAPER_MARKET_TIMING_REGRESSION
    private static func validatePaperMarketTimingDetails() throws {
        let wireFormatter = ISO8601DateFormatter()
        wireFormatter.formatOptions = [.withInternetDateTime]
        guard let now = wireFormatter.date(from: "2026-10-15T00:00:20Z"),
              let receivedAt = wireFormatter.date(from: "2026-10-15T00:00:17Z"),
              let futureReceivedAt = wireFormatter.date(from: "2026-10-15T00:00:21Z") else {
            throw Failure.assertion("paper timing regression clock fixture is invalid")
        }

        let rows = PaperMarketTimingFormatter().rows(for: [
            timingMarket(
                "KRW-BTC",
                sourceEventAt: "2026-10-15T00:00:09Z",
                finalizedCandleAt: "2026-10-15T00:00:05Z",
                accountMarkAt: "2026-10-15T00:00:15Z",
                heartbeatAt: "2026-10-15T00:00:12Z"
            ),
            timingMarket(
                "KRW-ETH",
                sourceEventAt: nil,
                finalizedCandleAt: "2026-10-15T00:00:20Z",
                accountMarkAt: nil,
                heartbeatAt: "2026-10-15T00:00:21Z"
            )
        ], at: now)

        guard rows.count == 2,
              rows[0].market == "KRW-BTC",
              rows[0].sourceEvent == "2026-10-15T00:00:09Z · 경과 11초",
              rows[0].finalizedCandle == "2026-10-15T00:00:05Z · 경과 15초",
              rows[0].accountMark == "2026-10-15T00:00:15Z · 경과 5초",
              rows[0].engineHeartbeat == "2026-10-15T00:00:12Z · 경과 8초",
              rows[1].market == "KRW-ETH",
              rows[1].sourceEvent == "미관측 · 경과 알 수 없음",
              rows[1].finalizedCandle == "2026-10-15T00:00:20Z · 경과 0초",
              rows[1].accountMark == "미관측 · 경과 알 수 없음",
              rows[1].engineHeartbeat == "2026-10-15T00:00:21Z · 미래 시각 · 경과 알 수 없음" else {
            throw Failure.assertion("paper market clocks were merged, lost, or given a fabricated age")
        }

        let received = PaperMarketTimingFormatter().receivedLabel(receivedAt, at: now)
        guard received.contains("2026-10-15T00:00:17") && received.contains("경과 3초") else {
            throw Failure.assertion("paper screen receive time or age was not preserved")
        }
        guard PaperMarketTimingFormatter().receivedLabel(nil, at: now) == "미수신 · 경과 알 수 없음" else {
            throw Failure.assertion("missing paper screen receive time was presented as zero")
        }
        let futureReceived = PaperMarketTimingFormatter().receivedLabel(futureReceivedAt, at: now)
        guard futureReceived.contains("2026-10-15T00:00:21"),
              futureReceived.contains("미래 시각"),
              futureReceived.contains("경과 알 수 없음") else {
            throw Failure.assertion("future paper screen receive time was assigned a negative age")
        }

        let previousTextScale = TerminalTheme.textScale
        defer { TerminalTheme.setTextScale(previousTextScale) }
        TerminalTheme.setTextScale(2.0)
        guard PaperTradingWorkspaceView.scaledMarketTimingColumnWidth(95) == 190,
              PaperTradingWorkspaceView.scaledMarketTimingColumnWidth(390) == 780 else {
            throw Failure.assertion("paper timing columns did not scale with the actual table font")
        }
        print("PASS paper market timing presentation regression")
    }

    private static func timingMarket(
        _ market: String,
        sourceEventAt: String?,
        finalizedCandleAt: String?,
        accountMarkAt: String?,
        heartbeatAt: String?
    ) -> PaperMarketSnapshot {
        PaperMarketSnapshot(
            market: market,
            model_id: "paper-model-\(market)",
            initial_cash: "0",
            cash_total: "0",
            cash_free: "0",
            cash_reserved: "0",
            quantity: "0",
            mark_price: nil,
            equity: nil,
            realized_pnl: "0",
            unrealized_pnl: nil,
            total_fees: "0",
            drawdown_amount: nil,
            drawdown_fraction: nil,
            signal_count: 0,
            fill_count: 0,
            event_count: 0,
            last_source_event_at: sourceEventAt,
            last_finalized_candle_at: finalizedCandleAt,
            last_account_mark_at: accountMarkAt,
            last_heartbeat_at: heartbeatAt,
            freshness: "OFFLINE",
            benchmark: nil,
            benchmark_null_reason: nil,
            verification: PaperVerification(status: "NOT_RUN", reason: "fixture"),
            costs: PaperCostPolicy(
                buy_fee_bps: "0",
                sell_fee_bps: "0",
                maker_fee_bps: "0",
                half_spread_bps: "0",
                slippage_bps: "0",
                impact_bps: "0",
                assumption_label: "synthetic test fixture",
                dynamic: nil
            ),
            market_rules: PaperMarketRuleScenario(
                kind: "SIMULATED_SCENARIO",
                min_notional: "0",
                quantity_step: "0",
                ticks: [],
                assumption_label: "synthetic test fixture"
            )
        )
    }
#endif

#if POLICY_DELETION_REGRESSION
    private static func validatePolicyDeletionSelectionState() throws {
        var selection = PolicyDeletionSelectionState()
        selection.setSelected("user-policy-z", selected: true)
        selection.selectAllFiltered(["user-policy-a", "user-policy-b"])
        selection.setSelected("user-policy-b", selected: false)
        guard selection.orderedPolicyIDs == ["user-policy-a", "user-policy-z"],
              selection.hiddenSelectionCount(visiblePolicyIDs: ["user-policy-a"]) == 1 else {
            throw Failure.assertion("policy selection did not retain stable IDs and hidden selections across filters")
        }
        selection.selectAllFiltered(["user-policy-c", "user-policy-d"])
        guard selection.orderedPolicyIDs == [
            "user-policy-a", "user-policy-c", "user-policy-d", "user-policy-z"
        ] else {
            throw Failure.assertion("select-all-filtered did not union the complete filtered identity set")
        }

        var largeSelection = PolicyDeletionSelectionState()
        for index in 0..<101 {
            largeSelection.setSelected(String(format: "user-policy-%03d", index), selected: true)
        }
        var plan = PolicyDeletionBatchPlan(policyIDs: largeSelection.orderedPolicyIDs)
        guard plan.totalCount == 101,
              plan.currentBatchIndex == 0,
              plan.currentBatchIDs.count == 100,
              plan.remainingCount == 1 else {
            throw Failure.assertion("101 selected policies were not represented as one explicit 100-item batch plus one remaining")
        }
        guard plan.advanceAfterExplicitUserAction(),
              plan.currentBatchIndex == 1,
              plan.currentBatchIDs == ["user-policy-100"],
              plan.remainingCount == 0,
              !plan.advanceAfterExplicitUserAction() else {
            throw Failure.assertion("policy batch plan advanced without an explicit next-batch action or exceeded its selection")
        }
        print("PASS policy deletion selection and batch bounds")
    }

    private static func validatePolicyDeletionCheckboxRendering() throws {
        _ = NSApplication.shared
        let previousTextScale = TerminalTheme.textScale
        defer { TerminalTheme.setTextScale(previousTextScale) }
        TerminalTheme.setTextScale(2.0)
        let base = FileManager.default.temporaryDirectory
            .appendingPathComponent("sky-backcraft-policy-delete-checkbox-\(UUID().uuidString)", isDirectory: true)
        let supportDirectory = base.appendingPathComponent("support", isDirectory: true)
        let dataRoot = base.appendingPathComponent("research", isDirectory: true)
        try FileManager.default.createDirectory(at: dataRoot, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: base) }

        func policy(_ id: String, _ name: String) -> RealPolicy {
            RealPolicy(
                policyId: id,
                family: "RSI",
                origin: "USER",
                name: name,
                headRevisionId: "revision-\(id)",
                revisionCount: 1,
                historicalRunCount: 0,
                historicalModelCount: 0,
                lastHistoricalRunDateStr: nil,
                usageState: .unused
            )
        }
        let workspace = PolicyDeletionWorkspace(
            service: PolicyDeletionService(client: NativeMCPClient(session: URLSession(configuration: .ephemeral))),
            intentStore: PolicyDeletionIntentStore(supportDirectory: supportDirectory, dataRoot: dataRoot),
            userPolicies: [policy("user-checkbox-a", "Policy A"), policy("user-checkbox-b", "Policy B")],
            allPolicies: [policy("user-checkbox-a", "Policy A"), policy("user-checkbox-b", "Policy B")],
            onPoliciesReloaded: {}
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
        window.contentView?.layoutSubtreeIfNeeded()
        window.displayIfNeeded()
        defer { window.close() }

        func descendants(of root: NSView) -> [NSView] {
            [root] + root.subviews.flatMap(descendants)
        }
        let views = descendants(of: workspace)
        guard let table = views.compactMap({ $0 as? NSTableView }).first,
              let selectAll = views.compactMap({ $0 as? NSButton }).first(where: { $0.title == "필터 전체 선택" }),
              let clearSelection = views.compactMap({ $0 as? NSButton }).first(where: { $0.title == "선택 해제" }),
              table.numberOfRows == 2,
              let firstCheckbox = table.view(atColumn: 0, row: 0, makeIfNecessary: true) as? NSButton,
              let secondCheckbox = table.view(atColumn: 0, row: 1, makeIfNecessary: true) as? NSButton,
              firstCheckbox.state == .off,
              secondCheckbox.state == .off else {
            throw Failure.assertion("offscreen policy deletion view did not construct two initially unchecked USER rows")
        }

        selectAll.performClick(nil)
        window.contentView?.layoutSubtreeIfNeeded()
        guard (table.view(atColumn: 0, row: 0, makeIfNecessary: true) as? NSButton)?.state == .on,
              (table.view(atColumn: 0, row: 1, makeIfNecessary: true) as? NSButton)?.state == .on else {
            throw Failure.assertion("select-all-filtered count changed but visible policy checkbox controls stayed unchecked")
        }
        clearSelection.performClick(nil)
        window.contentView?.layoutSubtreeIfNeeded()
        guard (table.view(atColumn: 0, row: 0, makeIfNecessary: true) as? NSButton)?.state == .off,
              (table.view(atColumn: 0, row: 1, makeIfNecessary: true) as? NSButton)?.state == .off else {
            throw Failure.assertion("clear-selection count changed but visible policy checkbox controls stayed checked")
        }
        print("PASS offscreen AppKit policy checkbox state synchronization at 1200x800 and 2x text")
    }

    private static func validatePolicyDeletionCompactLayout() throws {
        _ = NSApplication.shared
        let previousTextScale = TerminalTheme.textScale
        defer { TerminalTheme.setTextScale(previousTextScale) }
        TerminalTheme.setTextScale(2.0)
        let base = FileManager.default.temporaryDirectory
            .appendingPathComponent("sky-backcraft-policy-delete-layout-\(UUID().uuidString)", isDirectory: true)
        let supportDirectory = base.appendingPathComponent("support", isDirectory: true)
        let dataRoot = base.appendingPathComponent("research", isDirectory: true)
        try FileManager.default.createDirectory(at: dataRoot, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: base) }

        let policy = RealPolicy(
            policyId: "user-layout-a",
            family: "RSI",
            origin: "USER",
            name: "Layout Policy",
            headRevisionId: "revision-layout-a",
            revisionCount: 1,
            historicalRunCount: 0,
            historicalModelCount: 0,
            lastHistoricalRunDateStr: nil,
            usageState: .unused
        )
        let workspace = PolicyDeletionWorkspace(
            service: PolicyDeletionService(client: NativeMCPClient(session: URLSession(configuration: .ephemeral))),
            intentStore: PolicyDeletionIntentStore(supportDirectory: supportDirectory, dataRoot: dataRoot),
            userPolicies: [policy],
            allPolicies: [policy],
            onPoliciesReloaded: {}
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
        window.contentView?.layoutSubtreeIfNeeded()
        window.displayIfNeeded()
        defer { window.close() }

        func descendants(of root: NSView) -> [NSView] {
            [root] + root.subviews.flatMap(descendants)
        }
        let views = descendants(of: workspace)
        guard let outerScroll = views.compactMap({ $0 as? NSScrollView }).first(where: {
                  $0.identifier?.rawValue == "PolicyDeletionWorkspaceScroll"
              }),
              let document = outerScroll.documentView,
              let title = views.compactMap({ $0 as? NSTextField }).first(where: { $0.stringValue == "정책 삭제" }),
              let detailsScroll = views.compactMap({ $0 as? NSScrollView }).first(where: { $0.documentView is NSTextView }),
              let confirmationField = views.compactMap({ $0 as? NSTextField }).first(where: {
                  $0.placeholderString == "표시된 확인 문구를 입력하세요"
              }),
              let confirmButton = views.compactMap({ $0 as? NSButton }).first(where: { $0.title == "삭제 요청" }) else {
            throw Failure.assertion("compact policy deletion view did not expose its scroll area, heading, or review controls")
        }
        let requiredControlTitles = ["필터 전체 선택", "선택 해제", "삭제 영향 확인", "결과 조회", "다음 묶음", "이전 페이지", "다음 페이지", "삭제 요청"]
        guard requiredControlTitles.allSatisfy({ expectedTitle in
            views.compactMap({ $0 as? NSButton }).contains(where: { $0.title == expectedTitle })
        }) else {
            throw Failure.assertion("compact policy deletion layout omitted a visible workflow control")
        }

        guard outerScroll.hasVerticalScroller,
              !outerScroll.hasHorizontalScroller,
              document.frame.width <= outerScroll.contentView.bounds.width + 1,
              document.visibleRect.intersects(document.convert(title.bounds, from: title)) else {
            throw Failure.assertion("compact policy deletion heading was clipped or the outer vertical scroll was unavailable")
        }

        // Exercise the review-state content as well as the initial list. This is an
        // offscreen layout fixture; backend preview and deletion remain untouched.
        detailsScroll.isHidden = false
        confirmationField.isHidden = false
        confirmButton.isHidden = false
        for button in views.compactMap({ $0 as? NSButton }) where ["결과 조회", "다음 묶음"].contains(button.title) {
            button.isHidden = false
        }
        window.contentView?.layoutSubtreeIfNeeded()
        document.layoutSubtreeIfNeeded()
        guard document.frame.height > outerScroll.contentView.bounds.height else {
            throw Failure.assertion("review controls did not extend the document beyond the 1200x800 viewport")
        }

        let lastControlRect = document.convert(confirmButton.bounds, from: confirmButton)
        outerScroll.contentView.scroll(to: NSPoint(
            x: 0,
            y: max(0, document.bounds.height - outerScroll.contentView.bounds.height)
        ))
        outerScroll.reflectScrolledClipView(outerScroll.contentView)
        window.contentView?.layoutSubtreeIfNeeded()
        guard document.visibleRect.contains(lastControlRect) else {
            throw Failure.assertion("confirmation control was not reachable by scrolling the compact policy deletion view")
        }
        print("PASS policy deletion header and review controls scroll at 1200x800 and 2x text")
    }

    private static func validateDashboardDatabaseStatusFooter() throws {
        func policy(_ id: String) -> RealPolicy {
            RealPolicy(
                policyId: id,
                family: "RSI",
                origin: "USER",
                name: id,
                headRevisionId: "revision-\(id)",
                revisionCount: 1,
                historicalRunCount: 0,
                historicalModelCount: 0,
                lastHistoricalRunDateStr: nil,
                usageState: .unused
            )
        }
        let beforeDelete = (0..<9).map { policy("user-footer-\($0)") }
        let afterDelete = Array(beforeDelete.dropFirst())
        let before = DashboardDatabaseStatusSummary(historyRunCount: 0, policies: beforeDelete)
        let after = DashboardDatabaseStatusSummary(historyRunCount: 0, policies: afterDelete)
        guard before.label == "원장 · 0 실행 · 9 정책",
              after.label == "원장 · 0 실행 · 8 정책" else {
            throw Failure.assertion("sidebar policy count did not follow the refreshed read projection")
        }
        print("PASS sidebar policy footer follows the refreshed policy projection")
    }

    private static func validateActualPolicyPreviewRowInventory(at fixtureURL: URL) throws {
        let fixtureData = try Data(contentsOf: fixtureURL)
        let preview = try JSONDecoder().decode(PolicyDeletePreview.self, from: fixtureData)
        guard let now = ISO8601DateFormatter().date(from: "2026-10-15T03:30:00Z") else {
            throw Failure.assertion("fixed Rust preview fixture clock could not be parsed")
        }
        let validator = PolicyDeletePreviewValidator()
        guard validator.structuralBlocker(for: preview, expectedPolicyID: preview.resource.policy_id, at: now) == nil,
              preview.cascade_required == false,
              preview.blockers.isEmpty,
              preview.exclusive_file_count == 0,
              preview.reclaimable_file_bytes == 0,
              preview.cascade.map(\.kind) == ["policy", "policy_revision"],
              preview.cascade.first?.count == 1,
              preview.cascade.first?.examples == [preview.resource.policy_id],
              preview.db_row_estimate == preview.cascade.reduce(UInt64(0), { $0 + $1.count }),
              preview.summary.contains("Cascade 필요: 아니요"),
              preview.summary.contains("삭제 row inventory: policy: 1, policy_revision: 1"),
              validator.localEligibilityBlocker(for: preview) == nil else {
            throw Failure.assertion("actual Rust policy preview row inventory was misclassified as cross-resource cascade")
        }

        let object = try JSONSerialization.jsonObject(with: fixtureData) as? [String: Any] ?? [:]
        var foreignRowObject = object
        var foreignGroups = foreignRowObject["cascade"] as? [[String: Any]] ?? []
        foreignGroups.append(["kind": "plan", "count": 1, "examples": ["plan-synthetic"]])
        foreignRowObject["cascade"] = foreignGroups
        foreignRowObject["db_row_estimate"] = 3
        let foreignRows = try JSONDecoder().decode(
            PolicyDeletePreview.self,
            from: JSONSerialization.data(withJSONObject: foreignRowObject)
        )
        guard validator.localEligibilityBlocker(for: foreignRows) != nil else {
            throw Failure.assertion("non-policy row inventory was counted as policy-only eligible")
        }

        var mismatchedPolicyObject = object
        var mismatchedGroups = mismatchedPolicyObject["cascade"] as? [[String: Any]] ?? []
        mismatchedGroups[0]["examples"] = ["different-policy"]
        mismatchedPolicyObject["cascade"] = mismatchedGroups
        let mismatchedPolicyRow = try JSONDecoder().decode(
            PolicyDeletePreview.self,
            from: JSONSerialization.data(withJSONObject: mismatchedPolicyObject)
        )
        guard validator.localEligibilityBlocker(for: mismatchedPolicyRow) != nil else {
            throw Failure.assertion("policy row inventory for a different ID was counted as eligible")
        }

        let manyRevisions = PolicyDeletePreview(
            resource: preview.resource,
            cascade_required: false,
            blockers: [],
            cascade: [
                .init(kind: "policy", count: 1, examples: [preview.resource.policy_id]),
                .init(kind: "policy_revision", count: 5, examples: [
                    "policy-revision-synthetic-1",
                    "policy-revision-synthetic-2",
                    "policy-revision-synthetic-3",
                    "policy-revision-synthetic-4"
                ])
            ],
            retained_shared: [],
            reclaimable_file_bytes: 0,
            exclusive_file_count: 0,
            db_row_estimate: 6,
            scope_digest: preview.scope_digest,
            expires_at: preview.expires_at,
            confirmation_token: preview.confirmation_token,
            irreversible: true
        )
        guard validator.localEligibilityBlocker(for: manyRevisions) == nil else {
            throw Failure.assertion("bounded policy_revision examples were incorrectly required to enumerate every revision")
        }

        var missingRevisionGroupObject = object
        let groupsWithoutRevision = (missingRevisionGroupObject["cascade"] as? [[String: Any]] ?? [])
            .filter { $0["kind"] as? String != "policy_revision" }
        missingRevisionGroupObject["cascade"] = groupsWithoutRevision
        missingRevisionGroupObject["db_row_estimate"] = 1
        let missingRevisionGroup = try JSONDecoder().decode(
            PolicyDeletePreview.self,
            from: JSONSerialization.data(withJSONObject: missingRevisionGroupObject)
        )
        guard validator.localEligibilityBlocker(for: missingRevisionGroup) != nil else {
            throw Failure.assertion("policy without its required policy_revision inventory was counted as eligible")
        }
        print("PASS actual Rust unreferenced policy row inventory eligibility")
    }

    private static func validatePolicyDeletionWireExamples(at fixtureURL: URL) throws {
        let fixture = try JSONSerialization.jsonObject(with: Data(contentsOf: fixtureURL)) as? [String: Any]
            ?? { throw Failure.assertion("policy deletion wire fixture root must be an object") }()
        guard try string(fixture["fixture_kind"], field: "fixture_kind") == "synthetic_policy_delete_wire_examples" else {
            throw Failure.assertion("policy deletion wire fixture must be explicitly synthetic")
        }
        let api = try object(fixture["api"], field: "api")
        let write = try JSONDecoder().decode(
            PolicyDeletionWriteRequest.self,
            from: JSONSerialization.data(withJSONObject: try object(api["policy_delete_write"], field: "policy_delete_write"))
        )
        guard write.items.count == 1,
              write.items[0].policy_id == "policy-example-user-001",
              write.items[0].preview?.scope_digest == String(repeating: "1", count: 64),
              write.items[0].preview?.confirmation_token == String(repeating: "2", count: 64) else {
            throw Failure.assertion("policy_delete_write item and complete preview echo no longer decode from backend examples")
        }
        let query = try object(api["policy_delete_query"], field: "policy_delete_query")
        guard try string(query["request_id"], field: "policy_delete_query.request_id") == write.request_id else {
            throw Failure.assertion("policy_delete_query must carry only the exact write request id")
        }

        let desktop = try JSONDecoder().decode(
            PolicyDeletionCapability.self,
            from: JSONSerialization.data(withJSONObject: try object(api["desktop_capability"], field: "desktop_capability"))
        )
        let nonDesktop = try JSONDecoder().decode(
            PolicyDeletionCapability.self,
            from: JSONSerialization.data(withJSONObject: try object(api["non_desktop_capability"], field: "non_desktop_capability"))
        )
        guard desktop.permitsPolicyOnlyDelete, !nonDesktop.permitsPolicyOnlyDelete else {
            throw Failure.assertion("only the exact Desktop policy deletion capability may enable requests")
        }

        let expectedTwo = [
            PolicyDeletionExpectedItem(policy_id: "policy-example-user-001", scope_digest: String(repeating: "1", count: 64)),
            PolicyDeletionExpectedItem(policy_id: "policy-example-user-002", scope_digest: String(repeating: "5", count: 64))
        ]
        let pendingObject = try object(api["policy_delete_readback_pending"], field: "policy_delete_readback_pending")
        let pending = try JSONDecoder().decode(
            PolicyDeletionReadback.self,
            from: JSONSerialization.data(withJSONObject: pendingObject)
        )
        try pending.validate(expectedRequestID: pending.request_id, expectedItems: expectedTwo)
        guard pending.state == .pending,
              pending.counts?.pending == 1,
              pending.counts?.deleted == 1,
              pending.counts?.retained == 1 else {
            throw Failure.assertion("pending readback partition or overlapping retained count changed")
        }

        let completeObject = try object(api["policy_delete_readback_complete"], field: "policy_delete_readback_complete")
        let complete = try JSONDecoder().decode(
            PolicyDeletionReadback.self,
            from: JSONSerialization.data(withJSONObject: completeObject)
        )
        try complete.validate(expectedRequestID: complete.request_id, expectedItems: [expectedTwo[0]])
        guard complete.state == .complete, complete.counts?.deleted == 1, complete.counts?.pending == 0 else {
            throw Failure.assertion("terminal COMPLETE readback did not match the backend example")
        }

        let notFoundObject = try object(api["policy_delete_readback_not_found"], field: "policy_delete_readback_not_found")
        let notFound = try JSONDecoder().decode(
            PolicyDeletionReadback.self,
            from: JSONSerialization.data(withJSONObject: notFoundObject)
        )
        try notFound.validate(
            expectedRequestID: notFound.request_id,
            expectedItems: [PolicyDeletionExpectedItem(policy_id: "policy-example-user-001", scope_digest: String(repeating: "1", count: 64))]
        )
        guard !notFound.found else { throw Failure.assertion("NOT_FOUND readback was treated as a durable receipt") }

        var wrongScope = pendingObject
        var wrongScopeItems = wrongScope["items"] as? [[String: Any]] ?? []
        wrongScopeItems[1]["preview_scope_digest"] = String(repeating: "9", count: 64)
        wrongScope["items"] = wrongScopeItems
        let wrongScopeReadback = try JSONDecoder().decode(
            PolicyDeletionReadback.self,
            from: JSONSerialization.data(withJSONObject: wrongScope)
        )
        try expectPolicyDeletionRejection("readback with a mismatched preview scope digest was accepted") {
            try wrongScopeReadback.validate(expectedRequestID: pending.request_id, expectedItems: expectedTwo)
        }

        var wrongPartition = pendingObject
        var wrongCounts = try object(wrongPartition["counts"], field: "pending counts")
        wrongCounts["pending"] = 0
        wrongPartition["counts"] = wrongCounts
        let wrongPartitionReadback = try JSONDecoder().decode(
            PolicyDeletionReadback.self,
            from: JSONSerialization.data(withJSONObject: wrongPartition)
        )
        try expectPolicyDeletionRejection("readback counts that omit a pending item were accepted") {
            try wrongPartitionReadback.validate(expectedRequestID: pending.request_id, expectedItems: expectedTwo)
        }

        var invalidEffect = pendingObject
        var invalidEffectItems = invalidEffect["items"] as? [[String: Any]] ?? []
        invalidEffectItems[1]["effect"] = "UNKNOWN_EFFECT"
        invalidEffect["items"] = invalidEffectItems
        let invalidEffectReadback = try JSONDecoder().decode(
            PolicyDeletionReadback.self,
            from: JSONSerialization.data(withJSONObject: invalidEffect)
        )
        try expectPolicyDeletionRejection("unknown item effect was accepted as a no-effect receipt") {
            try invalidEffectReadback.validate(expectedRequestID: pending.request_id, expectedItems: expectedTwo)
        }

        var negativeCount = completeObject
        var negativeCounts = try object(negativeCount["counts"], field: "complete counts")
        negativeCounts["deleted"] = -1
        negativeCount["counts"] = negativeCounts
        try expectPolicyDeletionDecodeRejection("negative unsigned policy deletion count decoded") {
            _ = try JSONDecoder().decode(PolicyDeletionReadback.self, from: JSONSerialization.data(withJSONObject: negativeCount))
        }

        var missingEcho = completeObject
        var missingItems = missingEcho["items"] as? [[String: Any]] ?? []
        missingItems[0].removeValue(forKey: "preview_scope_digest")
        missingEcho["items"] = missingItems
        try expectPolicyDeletionDecodeRejection("readback without preview_scope_digest decoded") {
            _ = try JSONDecoder().decode(PolicyDeletionReadback.self, from: JSONSerialization.data(withJSONObject: missingEcho))
        }
        print("PASS policy deletion backend wire examples and strict readback validation")
    }

    private static func validatePolicyDeletionWritePreflight() throws {
        let policyID = "policy-example-user-001"
        let scope = String(repeating: "1", count: 64)
        let token = String(repeating: "2", count: 64)
        guard let now = ISO8601DateFormatter().date(from: "2026-10-15T02:00:00Z") else {
            throw Failure.assertion("fixed policy deletion preflight clock could not be parsed")
        }
        let preview = syntheticPolicyDeletePreview(
            policyID: policyID,
            scopeDigest: scope,
            expiry: "2026-10-15T03:00:00Z",
            token: token
        )
        let uniqueNameOptions = PolicyDeletionConfirmation.allowedSingleValues(
            policyID: policyID,
            policyName: "Readable unique policy",
            currentPolicyNames: ["Readable unique policy"]
        )
        guard uniqueNameOptions == [policyID, "Readable unique policy"] else {
            throw Failure.assertion("a current unique policy name was not accepted alongside its full ID")
        }
        let duplicateNameOptions = PolicyDeletionConfirmation.allowedSingleValues(
            policyID: policyID,
            policyName: "Shared name",
            currentPolicyNames: ["Shared name", "Shared name"]
        )
        guard duplicateNameOptions == [policyID] else {
            throw Failure.assertion("duplicate current policy names did not require the exact full ID")
        }

        let service = PolicyDeletionService(client: NativeMCPClient(session: URLSession(configuration: .ephemeral)))
        let requestID = "00000000-0000-4000-8000-000000000001"
        let request = try service.makeWriteRequest(
            requestID: requestID,
            expectedPolicyIDs: [policyID],
            previews: [preview],
            confirmationText: policyID,
            allowedSingleConfirmations: duplicateNameOptions,
            now: now
        )
        guard request.request_id == requestID, request.items.first?.preview?.confirmation_token == token else {
            throw Failure.assertion("single-ID confirmation did not preserve the complete preview echo")
        }

        let referencedID = "policy-example-user-002"
        let referencedPreview = syntheticPolicyDeletePreview(
            policyID: referencedID,
            scopeDigest: String(repeating: "5", count: 64),
            expiry: "2026-10-15T03:00:00Z",
            token: String(repeating: "6", count: 64),
            blockers: [.init(class: "HISTORICAL_REFERENCE", reference: "run-reference-synthetic", reason: "referenced by synthetic history")]
        )
        let validator = PolicyDeletePreviewValidator()
        guard validator.structuralBlocker(for: preview, expectedPolicyID: policyID, at: now) == nil,
              validator.structuralBlocker(for: referencedPreview, expectedPolicyID: referencedID, at: now) == nil,
              validator.localEligibilityBlocker(for: preview) == nil,
              validator.localEligibilityBlocker(for: referencedPreview) != nil else {
            throw Failure.assertion("reference eligibility was not separated from a fresh, complete policy preview")
        }
        let mixedRequest = try service.makeWriteRequest(
            requestID: "00000000-0000-4000-8000-000000000004",
            expectedPolicyIDs: [policyID, referencedID],
            previews: [preview, referencedPreview],
            confirmationText: "DELETE 2 POLICIES",
            allowedSingleConfirmations: [],
            now: now
        )
        guard mixedRequest.items.count == 2,
              mixedRequest.confirmation_text == "DELETE 2 POLICIES",
              [preview, referencedPreview].filter({ validator.localEligibilityBlocker(for: $0) == nil }).count == 1 else {
            throw Failure.assertion("eligible and referenced policies were not submitted together with the full batch phrase")
        }

        let cascadePreview = syntheticPolicyDeletePreview(
            policyID: referencedID,
            scopeDigest: String(repeating: "5", count: 64),
            expiry: "2026-10-15T03:00:00Z",
            token: String(repeating: "6", count: 64),
            cascadeRequired: true,
            cascade: [.init(kind: "descendant", count: 1, examples: ["synthetic-descendant"])]
        )
        guard validator.structuralBlocker(for: cascadePreview, expectedPolicyID: referencedID, at: now) == nil,
              validator.localEligibilityBlocker(for: cascadePreview) != nil else {
            throw Failure.assertion("cascade preview was treated as malformed or as a policy-only eligible candidate")
        }
        let cascadeMixedRequest = try service.makeWriteRequest(
            requestID: "00000000-0000-4000-8000-000000000007",
            expectedPolicyIDs: [policyID, referencedID],
            previews: [preview, cascadePreview],
            confirmationText: "DELETE 2 POLICIES",
            allowedSingleConfirmations: [],
            now: now
        )
        let cascadeMixedObject = try JSONSerialization.jsonObject(
            with: JSONEncoder().encode(cascadeMixedRequest)
        ) as? [String: Any] ?? [:]
        guard cascadeMixedRequest.items.count == 2,
              cascadeMixedObject["cascade"] == nil,
              (cascadeMixedObject["items"] as? [[String: Any]])?.allSatisfy({ $0["cascade"] == nil }) == true else {
            throw Failure.assertion("cascade preview bypassed the policy-only request shape")
        }
        let unknownFileScopePreview = syntheticPolicyDeletePreview(
            policyID: policyID,
            scopeDigest: scope,
            expiry: "2026-10-15T03:00:00Z",
            token: token,
            reclaimableFileBytes: nil
        )
        try expectPolicyDeletionRejection("unknown preview file scope was allowed into a deletion request") {
            _ = try service.makeWriteRequest(
                requestID: "00000000-0000-4000-8000-000000000005",
                expectedPolicyIDs: [policyID],
                previews: [unknownFileScopePreview],
                confirmationText: policyID,
                allowedSingleConfirmations: [policyID],
                now: now
            )
        }
        let malformedHashPreview = syntheticPolicyDeletePreview(
            policyID: policyID,
            scopeDigest: "unknown-scope",
            expiry: "2026-10-15T03:00:00Z",
            token: token
        )
        try expectPolicyDeletionRejection("malformed preview scope digest was allowed into a deletion request") {
            _ = try service.makeWriteRequest(
                requestID: "00000000-0000-4000-8000-000000000006",
                expectedPolicyIDs: [policyID],
                previews: [malformedHashPreview],
                confirmationText: policyID,
                allowedSingleConfirmations: [policyID],
                now: now
            )
        }

        try expectPolicyDeletionRejection("duplicate policy name was accepted as a single-item confirmation") {
            _ = try service.makeWriteRequest(
                requestID: "00000000-0000-4000-8000-000000000002",
                expectedPolicyIDs: [policyID],
                previews: [preview],
                confirmationText: "Shared name",
                allowedSingleConfirmations: duplicateNameOptions,
                now: now
            )
        }
        try expectPolicyDeletionRejection("invalid request id was accepted for a policy write") {
            _ = try service.makeWriteRequest(
                requestID: "policy-delete-request-001",
                expectedPolicyIDs: [policyID],
                previews: [preview],
                confirmationText: policyID,
                allowedSingleConfirmations: [policyID],
                now: now
            )
        }
        let stalePreview = syntheticPolicyDeletePreview(
            policyID: policyID,
            scopeDigest: scope,
            expiry: "2026-10-15T01:59:59Z",
            token: token
        )
        try expectPolicyDeletionRejection("expired preview was allowed to reach the write request") {
            _ = try service.makeWriteRequest(
                requestID: "00000000-0000-4000-8000-000000000003",
                expectedPolicyIDs: [policyID],
                previews: [stalePreview],
                confirmationText: policyID,
                allowedSingleConfirmations: [policyID],
                now: now
            )
        }
        print("PASS policy deletion confirmation, UUID, complete preview echo and expiry preflight")
    }

    private static func syntheticPolicyDeletePreview(
        policyID: String,
        scopeDigest: String,
        expiry: String,
        token: String,
        blockers: [PolicyDeletePreview.Blocker] = [],
        cascadeRequired: Bool = false,
        cascade: [PolicyDeletePreview.CascadeGroup]? = nil,
        exclusiveFileCount: UInt64 = 0,
        reclaimableFileBytes: UInt64? = 0
    ) -> PolicyDeletePreview {
        PolicyDeletePreview(
            resource: .init(kind: "policy", policy_id: policyID),
            cascade_required: cascadeRequired,
            blockers: blockers,
            cascade: cascade ?? [
                .init(kind: "policy", count: 1, examples: [policyID]),
                .init(kind: "policy_revision", count: 1, examples: ["policy-revision-synthetic"])
            ],
            retained_shared: [],
            reclaimable_file_bytes: reclaimableFileBytes,
            exclusive_file_count: exclusiveFileCount,
            db_row_estimate: 2,
            scope_digest: scopeDigest,
            expires_at: expiry,
            confirmation_token: token,
            irreversible: true
        )
    }

    private static func validatePolicyDeletionIntentPersistence() throws {
        let base = FileManager.default.temporaryDirectory
            .appendingPathComponent("sky-backcraft-policy-delete-intent-\(UUID().uuidString)", isDirectory: true)
        let supportDirectory = base.appendingPathComponent("support", isDirectory: true)
        let dataRoot = base.appendingPathComponent("research", isDirectory: true)
        let otherDataRoot = base.appendingPathComponent("different-research", isDirectory: true)
        try FileManager.default.createDirectory(at: dataRoot, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: base) }

        let store = PolicyDeletionIntentStore(supportDirectory: supportDirectory, dataRoot: dataRoot)
        let ownedHelper = NativeMCPOwnedHelperIdentity(
            instanceID: UUID(uuidString: "00000000-0000-4000-8000-000000000030")!,
            processID: 42_424,
            dataRoot: dataRoot
        )
        let transportContext = NativeMCPTransportContext(generation: 7, owned_helper: ownedHelper)
        let intent = PolicyDeletionPendingIntent(
            request_id: "00000000-0000-4000-8000-000000000020",
            data_root_identity: store.dataRootIdentity,
            expected_items: [PolicyDeletionExpectedItem(
                policy_id: "policy-example-user-001",
                scope_digest: String(repeating: "1", count: 64)
            )],
            transport_context: transportContext
        )
        try store.save(intent)
        let reopened = PolicyDeletionIntentStore(supportDirectory: supportDirectory, dataRoot: dataRoot)
        guard try reopened.load() == intent else {
            throw Failure.assertion("pending UI intent did not restore exactly across a new store instance")
        }
        let persistedData = try Data(contentsOf: supportDirectory.appendingPathComponent("policy-deletion-pending.json"))
        let persistedObject = try JSONSerialization.jsonObject(with: persistedData) as? [String: Any] ?? [:]
        guard persistedObject["transport_context"] != nil,
              persistedData.range(of: Data("confirmation_token".utf8)) == nil,
              persistedData.range(of: Data(String(repeating: "2", count: 64).utf8)) == nil else {
            throw Failure.assertion("pending recovery context persisted a preview token or omitted the transport identity")
        }

        do {
            _ = try PolicyDeletionIntentStore(supportDirectory: supportDirectory, dataRoot: otherDataRoot).load()
            throw Failure.assertion("pending UI intent from another data root was accepted")
        } catch PolicyDeletionIntentStoreError.dataRootMismatch {
        }
        do {
            try reopened.clear(requestID: "00000000-0000-4000-8000-000000000021")
            throw Failure.assertion("a different request ID cleared the persisted UI intent")
        } catch PolicyDeletionIntentStoreError.requestIdentityMismatch {
        }
        guard try reopened.load() == intent else {
            throw Failure.assertion("mismatched clear attempt changed the pending UI intent")
        }
        try reopened.clear(requestID: intent.request_id)
        guard try reopened.load() == nil else {
            throw Failure.assertion("terminal exact-ID clear left a UI intent behind")
        }

        var fence = PolicyDeletionUnknownOutcomeFence(transportContext: transportContext)
        fence.recordNotFound(for: transportContext)
        guard !fence.mayRelease(processIsAbsent: false) else {
            throw Failure.assertion("NOT_FOUND alone released the pending-write fence")
        }
        let mismatchedContext = NativeMCPTransportContext(generation: 8, owned_helper: ownedHelper)
        fence.recordHelperExit(mismatchedContext)
        guard !fence.mayRelease(processIsAbsent: false) else {
            throw Failure.assertion("a different transport generation released the pending-write fence")
        }
        fence.recordHelperExit(transportContext)
        guard fence.mayRelease(processIsAbsent: false) else {
            throw Failure.assertion("matching owned-helper exit did not release a verified NOT_FOUND fence")
        }

        let externalContext = NativeMCPTransportContext(generation: 9, owned_helper: nil)
        var externalFence = PolicyDeletionUnknownOutcomeFence(transportContext: externalContext)
        externalFence.recordNotFound(for: externalContext)
        externalFence.recordHelperExit(externalContext)
        guard !externalFence.mayRelease(processIsAbsent: true) else {
            throw Failure.assertion("external transport without an owned helper was allowed to clear an unknown request")
        }

        let absentContext = NativeMCPTransportContext(
            generation: 10,
            owned_helper: NativeMCPOwnedHelperIdentity(
                instanceID: UUID(uuidString: "00000000-0000-4000-8000-000000000031")!,
                processID: Int32.max,
                dataRoot: dataRoot
            )
        )
        let client = NativeMCPClient(session: URLSession(configuration: .ephemeral))
        guard client.ownedHelperIsAbsent(in: absentContext, expectedDataRootIdentity: store.dataRootIdentity),
              !client.ownedHelperIsAbsent(in: absentContext, expectedDataRootIdentity: PolicyDeletionIntentStore(supportDirectory: supportDirectory, dataRoot: otherDataRoot).dataRootIdentity) else {
            throw Failure.assertion("read-only ESRCH proof was not restricted to the captured canonical data root")
        }
        print("PASS policy deletion pending intent restart and canonical data-root fence")
    }

    private static func expectPolicyDeletionRejection(
        _ assertion: String,
        _ operation: () throws -> Void
    ) throws {
        do {
            try operation()
            throw Failure.assertion(assertion)
        } catch is PolicyDeletionServiceError {
        }
    }

    private static func expectPolicyDeletionDecodeRejection(
        _ assertion: String,
        _ operation: () throws -> Void
    ) throws {
        do {
            try operation()
            throw Failure.assertion(assertion)
        } catch is DecodingError {
        }
    }

    private static func validatePolicyDeletionTransport(at fixtureURL: URL) throws {
        let fixture = try JSONSerialization.jsonObject(with: Data(contentsOf: fixtureURL)) as? [String: Any]
            ?? { throw Failure.assertion("policy deletion wire fixture root must be an object") }()
        let api = try object(fixture["api"], field: "api")
        let capabilityObject = try object(api["desktop_capability"], field: "desktop_capability")
        let capabilityText = String(decoding: try JSONSerialization.data(withJSONObject: ["policy_deletion": capabilityObject]), as: UTF8.self)
        var pendingObject = try object(api["policy_delete_readback_pending"], field: "policy_delete_readback_pending")
        let requestID = "00000000-0000-4000-8000-000000000010"
        pendingObject["request_id"] = requestID
        let pendingText = String(decoding: try JSONSerialization.data(withJSONObject: pendingObject), as: UTF8.self)
        var notFoundObject = try object(api["policy_delete_readback_not_found"], field: "policy_delete_readback_not_found")
        notFoundObject["request_id"] = requestID
        let notFoundText = String(decoding: try JSONSerialization.data(withJSONObject: notFoundObject), as: UTF8.self)

        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [PaperWriteRecoveryURLProtocol.self]
        let session = URLSession(configuration: configuration)
        defer {
            session.invalidateAndCancel()
            PaperWriteRecoveryURLProtocol.clear()
        }
        let client = NativeMCPClient(session: session)
        client.configure(port: 65_431, token: String(repeating: "a", count: 64))
        let service = PolicyDeletionService(client: client)

        let capabilityRecorder = PolicyDeletionTestRecorder()
        installPolicyDeletionMock(recorder: capabilityRecorder, responses: ["lab_status": capabilityText])
        var capabilityResult: Result<PolicyDeletionCapability?, Error>?
        _ = service.loadCapability { capabilityResult = $0 }
        let loadedCapability = try waitForPolicyDeletionResult({ capabilityResult }, label: "policy deletion lab_status")
        guard try loadedCapability.get()?.permitsPolicyOnlyDelete == true else {
            throw Failure.assertion("policy deletion Desktop capability did not decode from lab_status")
        }

        let oldBackendRecorder = PolicyDeletionTestRecorder()
        installPolicyDeletionMock(recorder: oldBackendRecorder, responses: ["lab_status": "{}"])
        var oldCapabilityResult: Result<PolicyDeletionCapability?, Error>?
        _ = service.loadCapability { oldCapabilityResult = $0 }
        let oldCapability = try waitForPolicyDeletionResult({ oldCapabilityResult }, label: "legacy lab_status")
        guard try oldCapability.get() == nil else {
            throw Failure.assertion("missing old-backend capability did not remain preview-only")
        }

        guard let now = ISO8601DateFormatter().date(from: "2026-10-15T02:00:00Z") else {
            throw Failure.assertion("fixed policy deletion transport clock could not be parsed")
        }
        let scope1 = String(repeating: "1", count: 64)
        let scope2 = String(repeating: "5", count: 64)
        let ids = ["policy-example-user-001", "policy-example-user-002"]
        let previews = [
            syntheticPolicyDeletePreview(policyID: ids[0], scopeDigest: scope1, expiry: "2035-10-15T03:00:00Z", token: String(repeating: "2", count: 64)),
            syntheticPolicyDeletePreview(policyID: ids[1], scopeDigest: scope2, expiry: "2035-10-15T03:00:00Z", token: String(repeating: "6", count: 64))
        ]
        let expectedItems = [
            PolicyDeletionExpectedItem(policy_id: ids[0], scope_digest: scope1),
            PolicyDeletionExpectedItem(policy_id: ids[1], scope_digest: scope2)
        ]
        let request = try service.makeWriteRequest(
            requestID: requestID,
            expectedPolicyIDs: ids,
            previews: previews,
            confirmationText: "DELETE 2 POLICIES",
            allowedSingleConfirmations: [],
            now: now
        )
        let writeRecorder = PolicyDeletionTestRecorder()
        installPolicyDeletionMock(recorder: writeRecorder, responses: ["policy_delete_write": pendingText])
        var writeResult: Result<PolicyDeletionReadback, Error>?
        _ = service.submit(request, expectedItems: expectedItems, allowedSingleConfirmations: []) { writeResult = $0 }
        let writeOutcome = try waitForPolicyDeletionResult({ writeResult }, label: "policy_delete_write")
        guard case .success(let pending) = writeOutcome,
              pending.found, pending.state == .pending, pending.counts?.pending == 1 else {
            throw Failure.assertion("valid PENDING response did not preserve the request fence")
        }
        let writeCalls = writeRecorder.snapshot()
        guard writeCalls.calls.map(\.tool) == ["policy_delete_write"],
              writeCalls.calls.first?.arguments["request_id"] as? String == requestID,
              writeCalls.calls.first?.arguments["confirmation_text"] as? String == "DELETE 2 POLICIES",
              (writeCalls.calls.first?.arguments["items"] as? [[String: Any]])?.count == 2,
              writeCalls.unexpectedRequests.isEmpty else {
            throw Failure.assertion("native write did not call policy_delete_write exactly once with its batch confirmation")
        }
        let serializedWrite = try JSONDecoder().decode(
            PolicyDeletionWriteRequest.self,
            from: JSONSerialization.data(withJSONObject: writeCalls.calls.first?.arguments ?? [:])
        )
        guard serializedWrite.items.map(\.policy_id) == ids,
              serializedWrite.items.map({ $0.preview?.scope_digest ?? "" }) == [scope1, scope2] else {
            throw Failure.assertion("policy_delete_write did not preserve every preview-scope echo")
        }

        let mixedRequestID = "00000000-0000-4000-8000-000000000011"
        var partialObject = pendingObject
        partialObject["request_id"] = mixedRequestID
        partialObject["state"] = "PARTIAL"
        var partialItems = partialObject["items"] as? [[String: Any]] ?? []
        partialItems[1]["status"] = "BLOCKED"
        partialItems[1]["effect"] = "NOT_APPLIED"
        partialItems[1]["deleted_revision_count"] = 0
        partialItems[1]["reason_code"] = "REFERENCED_HISTORICAL"
        partialItems[1]["remediation_code"] = "ARCHIVE_DEACTIVATE_UNAVAILABLE"
        partialItems[1]["retained"] = true
        partialObject["items"] = partialItems
        var partialCounts = partialObject["counts"] as? [String: Any] ?? [:]
        partialCounts["pending"] = 0
        partialCounts["blocked"] = 1
        partialObject["counts"] = partialCounts
        let partialText = String(decoding: try JSONSerialization.data(withJSONObject: partialObject), as: UTF8.self)
        let referencedPreview = syntheticPolicyDeletePreview(
            policyID: ids[1],
            scopeDigest: scope2,
            expiry: "2035-10-15T03:00:00Z",
            token: String(repeating: "6", count: 64),
            blockers: [.init(class: "HISTORICAL_REFERENCE", reference: "run-reference-synthetic", reason: "referenced by synthetic history")]
        )
        let mixedRequest = try service.makeWriteRequest(
            requestID: mixedRequestID,
            expectedPolicyIDs: ids,
            previews: [previews[0], referencedPreview],
            confirmationText: "DELETE 2 POLICIES",
            allowedSingleConfirmations: [],
            now: now
        )
        let mixedRecorder = PolicyDeletionTestRecorder()
        installPolicyDeletionMock(recorder: mixedRecorder, responses: ["policy_delete_write": partialText])
        var mixedResult: Result<PolicyDeletionReadback, Error>?
        _ = service.submit(mixedRequest, expectedItems: expectedItems, allowedSingleConfirmations: []) { mixedResult = $0 }
        let mixedOutcome = try waitForPolicyDeletionResult({ mixedResult }, label: "mixed partial policy deletion")
        guard case .success(let partial) = mixedOutcome,
              partial.state == .partial,
              partial.counts?.deleted == 1,
              partial.counts?.blocked == 1,
              partial.items?.map(\.status) == [.deleted, .blocked],
              partial.items?.last?.remediation_code == "ARCHIVE_DEACTIVATE_UNAVAILABLE" else {
            throw Failure.assertion("mixed eligible/referenced batch lost the server's per-item partial result")
        }
        guard mixedRecorder.snapshot().calls.map(\.tool) == ["policy_delete_write"],
              mixedRecorder.snapshot().calls.first?.arguments["request_id"] as? String == mixedRequestID,
              mixedRecorder.snapshot().calls.first?.arguments["confirmation_text"] as? String == "DELETE 2 POLICIES",
              (mixedRecorder.snapshot().calls.first?.arguments["items"] as? [[String: Any]])?.count == 2 else {
            throw Failure.assertion("mixed batch omitted the blocked selected row or used eligibility count in confirmation")
        }

        let uncertainRecorder = PolicyDeletionTestRecorder()
        installPolicyDeletionMock(recorder: uncertainRecorder, responses: [
            "policy_delete_write": "malformed write response",
            "policy_delete_query": notFoundText
        ])
        var uncertainResult: Result<PolicyDeletionReadback, Error>?
        _ = service.submit(request, expectedItems: expectedItems, allowedSingleConfirmations: []) { uncertainResult = $0 }
        let uncertainOutcome = try waitForPolicyDeletionResult({ uncertainResult }, label: "uncertain policy write readback")
        guard case .failure(let error) = uncertainOutcome,
              let serviceError = error as? PolicyDeletionServiceError,
              case .unknownOutcome(let failedRequestID, _) = serviceError,
              failedRequestID == requestID else {
            throw Failure.assertion("one NOT_FOUND query incorrectly resolved an uncertain policy write")
        }
        let uncertainCalls = uncertainRecorder.snapshot()
        guard uncertainCalls.calls.map(\.tool) == ["policy_delete_write", "policy_delete_query"],
              uncertainCalls.calls.last?.arguments["request_id"] as? String == requestID,
              uncertainCalls.unexpectedRequests.isEmpty else {
            throw Failure.assertion("uncertain write did not issue exactly one same-ID query and avoid resending")
        }

        var hardDeleteResult: Result<Data, Error>?
        _ = client.call("resource_hard_delete", arguments: ["kind": "policy"]) { hardDeleteResult = $0 }
        guard case .failure = hardDeleteResult,
              uncertainRecorder.snapshot().calls.map(\.tool) == ["policy_delete_write", "policy_delete_query"] else {
            throw Failure.assertion("generic resource_hard_delete bypassed the native allowlist")
        }
        print("PASS policy deletion Desktop transport, PENDING fence, single typed readback and hard-delete exclusion")
    }

    private static func validatePolicyDeletionTransportContext() throws {
        let session = URLSession(configuration: .ephemeral)
        defer { session.invalidateAndCancel() }
        let client = NativeMCPClient(session: session)
        let dataRoot = FileManager.default.temporaryDirectory.appendingPathComponent("policy-delete-transport-context")
        let firstHelper = NativeMCPOwnedHelperIdentity(
            instanceID: UUID(uuidString: "00000000-0000-4000-8000-000000000040")!,
            processID: 40_040,
            dataRoot: dataRoot
        )
        let firstContext = client.configure(
            port: 65_431,
            token: String(repeating: "a", count: 64),
            ownedHelper: firstHelper
        )
        let secondHelper = NativeMCPOwnedHelperIdentity(
            instanceID: UUID(uuidString: "00000000-0000-4000-8000-000000000041")!,
            processID: 40_041,
            dataRoot: dataRoot
        )
        let secondContext = client.configure(
            port: 65_432,
            token: String(repeating: "b", count: 64),
            ownedHelper: secondHelper
        )
        guard let firstContext, let secondContext, firstContext != secondContext else {
            throw Failure.assertion("NativeMCPClient did not rotate the actual transport generation")
        }

        var staleWriteResult: Result<Data, Error>?
        _ = client.call(
            "policy_delete_write",
            arguments: ["request_id": "synthetic"],
            expectedTransportContext: firstContext
        ) { staleWriteResult = $0 }
        guard case .failure(let error) = staleWriteResult,
              let nativeError = error as? NativeMCPError,
              case .transportContextChanged = nativeError else {
            throw Failure.assertion("a stale write context was allowed to dispatch on a newer Backend connection")
        }

        var exitedContexts: [NativeMCPTransportContext] = []
        let observer = NotificationCenter.default.addObserver(
            forName: .nativeMCPOwnedHelperExited,
            object: client,
            queue: nil
        ) { notification in
            if let context = notification.userInfo?["context"] as? NativeMCPTransportContext {
                exitedContexts.append(context)
            }
        }
        defer { NotificationCenter.default.removeObserver(observer) }
        client.confirmOwnedHelperExit(firstContext)
        guard exitedContexts.isEmpty else {
            throw Failure.assertion("a mismatched old process identity produced current-helper exit proof")
        }
        client.confirmOwnedHelperExit(secondContext)
        guard exitedContexts == [secondContext] else {
            throw Failure.assertion("exact owned-helper exit did not publish its captured transport context")
        }
        client.disconnect()
        client.confirmOwnedHelperExit(secondContext)
        guard exitedContexts == [secondContext] else {
            throw Failure.assertion("disconnect alone was treated as confirmed process exit")
        }
        print("PASS policy deletion transport-generation fence and owned-helper exit identity")
    }

    private static func installPolicyDeletionMock(
        recorder: PolicyDeletionTestRecorder,
        responses: [String: String]
    ) {
        PaperWriteRecoveryURLProtocol.install { request in
            guard let body = PaperWriteRecoveryURLProtocol.requestBody(request),
                  let envelope = try? JSONSerialization.jsonObject(with: body) as? [String: Any],
                  let messageID = envelope["id"] as? String,
                  let method = envelope["method"] as? String else {
                recorder.recordUnexpected("invalid JSON-RPC request")
                return nil
            }
            let result: [String: Any]
            if method == "initialize" {
                result = ["protocolVersion": "2025-03-26"]
            } else if method == "tools/call",
                      let params = envelope["params"] as? [String: Any],
                      let tool = params["name"] as? String,
                      let arguments = params["arguments"] as? [String: Any] {
                recorder.record(tool: tool, arguments: arguments)
                if let text = responses[tool] {
                    result = ["isError": false, "content": [["type": "text", "text": text]]]
                } else {
                    recorder.recordUnexpected(tool)
                    result = ["isError": true, "content": []]
                }
            } else {
                recorder.recordUnexpected("unexpected method \(method)")
                result = ["isError": true, "content": []]
            }
            return try? JSONSerialization.data(withJSONObject: [
                "jsonrpc": "2.0", "id": messageID, "result": result
            ])
        }
    }

    private static func waitForPolicyDeletionResult<Value>(
        _ value: () -> Value?,
        label: String,
        timeout: TimeInterval = 5
    ) throws -> Value {
        let deadline = Date().addingTimeInterval(timeout)
        while Date() < deadline {
            if let result = value() { return result }
            _ = RunLoop.current.run(mode: .default, before: Date().addingTimeInterval(0.01))
        }
        throw Failure.assertion("timed out waiting for \(label)")
    }
#endif

    private static func validatePaperWriteReadbackRecovery() throws {
        let policy = PaperPolicyRevisionReference(
            policy_id: "policy-paper-test",
            revision_id: "revision-paper-test",
            definition_digest: String(repeating: "a", count: 64)
        )
        let paperSession = PaperSessionView(
            session_id: "paper-session-test",
            origin_request_id: "paper-start-test",
            state: "STOPPED",
            sequence: 8,
            policy_ref: policy,
            decision_interval: "m1",
            execution_resolution: "m1",
            mode: "MOCK",
            created_at: "2026-10-15T00:00:00Z",
            started_at: "2026-10-15T00:00:00Z",
            paused_at: nil,
            stopped_at: "2026-10-15T00:01:00Z",
            last_evaluated_at: nil,
            event_count: 1,
            markets: [],
            recovery_cause: nil
        )

        let malformedResponse = "{malformed response"
        let mismatchedResponse = try mismatchedPaperCommandText(policy: policy, session: paperSession)
        let scenarios: [(
            requestID: String,
            writeText: String,
            readbackFound: Bool,
            malformedMCPEnvelope: Bool,
            readbackExpectedSequence: UInt64,
            expectResolved: Bool
        )] = [
            ("paper-stop-malformed", malformedResponse, false, false, 7, false),
            ("paper-stop-mismatched", mismatchedResponse, true, false, 7, true),
            ("paper-stop-context-mismatch", mismatchedResponse, true, false, 8, false),
            ("paper-stop-protocol-error", "", false, true, 7, false)
        ]

        for (requestID, writeText, readbackFound, malformedMCPEnvelope, readbackExpectedSequence, expectResolved) in scenarios {
            let recorder = PaperWriteRecoveryRecorder()
            let readback: PaperSessionRequestReadback
            if readbackFound {
                let receipt = PaperSessionReceipt(
                    request_id: requestID,
                    session_id: paperSession.session_id,
                    action: "stop",
                    outcome: "APPLIED",
                    expected_sequence: readbackExpectedSequence,
                    sequence: 8,
                    created_at: "2026-10-15T00:01:00Z",
                    cancelled_pending_orders: []
                )
                let currentSession = PaperSessionView(
                    session_id: paperSession.session_id,
                    origin_request_id: paperSession.origin_request_id,
                    state: "STOPPED",
                    sequence: 9,
                    policy_ref: paperSession.policy_ref,
                    decision_interval: paperSession.decision_interval,
                    execution_resolution: paperSession.execution_resolution,
                    mode: paperSession.mode,
                    created_at: paperSession.created_at,
                    started_at: paperSession.started_at,
                    paused_at: paperSession.paused_at,
                    stopped_at: paperSession.stopped_at,
                    last_evaluated_at: paperSession.last_evaluated_at,
                    event_count: 2,
                    markets: paperSession.markets,
                    recovery_cause: paperSession.recovery_cause
                )
                readback = PaperSessionRequestReadback(
                    request_id: requestID,
                    found: true,
                    action: "stop",
                    session_id: currentSession.session_id,
                    receipt: receipt,
                    session: currentSession
                )
            } else {
                readback = PaperSessionRequestReadback(
                    request_id: requestID,
                    found: false,
                    action: nil,
                    session_id: nil,
                    receipt: nil,
                    session: nil
                )
            }
            let readbackText = String(decoding: try JSONEncoder().encode(readback), as: UTF8.self)

            PaperWriteRecoveryURLProtocol.install { request in
                guard let body = PaperWriteRecoveryURLProtocol.requestBody(request),
                      let envelope = try? JSONSerialization.jsonObject(with: body) as? [String: Any],
                      let messageID = envelope["id"] as? String,
                      let method = envelope["method"] as? String else {
                    recorder.recordUnexpected("invalid JSON-RPC request")
                    return nil
                }

                let result: [String: Any]
                if method == "initialize" {
                    result = ["protocolVersion": "2025-03-26"]
                } else if method == "tools/call",
                          let params = envelope["params"] as? [String: Any],
                          let tool = params["name"] as? String,
                          let arguments = params["arguments"] as? [String: Any] {
                    let action = arguments["action"] as? String
                    let calledRequestID = arguments["request_id"] as? String
                    switch tool {
                    case "paper_session_write":
                        recorder.recordWrite(requestID: calledRequestID)
                        if malformedMCPEnvelope { return Data("not-json".utf8) }
                        result = [
                            "isError": false,
                            "content": [["type": "text", "text": writeText]]
                        ]
                    case "paper_session_query" where action == "request":
                        recorder.recordReadback(action: action, requestID: calledRequestID)
                        result = [
                            "isError": false,
                            "content": [["type": "text", "text": readbackText]]
                        ]
                    default:
                        recorder.recordUnexpected("\(tool):\(action ?? "missing-action")")
                        result = ["isError": true, "content": []]
                    }
                } else {
                    recorder.recordUnexpected("unexpected JSON-RPC method: \(method)")
                    result = ["isError": true, "content": []]
                }

                return try? JSONSerialization.data(withJSONObject: [
                    "jsonrpc": "2.0",
                    "id": messageID,
                    "result": result
                ])
            }
            defer { PaperWriteRecoveryURLProtocol.clear() }

            let configuration = URLSessionConfiguration.ephemeral
            configuration.protocolClasses = [PaperWriteRecoveryURLProtocol.self]
            let urlSession = URLSession(configuration: configuration)
            defer { urlSession.invalidateAndCancel() }
            let client = NativeMCPClient(session: urlSession)
            client.configure(port: 65_431, token: String(repeating: "a", count: 64))
            let service = PaperSessionMCPService(client: client)

            var outcome: Result<PaperSessionCommandResponse, Error>?
            _ = service.stopSession(
                requestId: requestID,
                sessionId: paperSession.session_id,
                expectedSequence: 7
            ) { outcome = $0 }

            let deadline = Date().addingTimeInterval(5)
            while outcome == nil, Date() < deadline {
                _ = RunLoop.current.run(mode: .default, before: Date().addingTimeInterval(0.01))
            }
            guard let outcome else {
                throw Failure.assertion("uncertain paper write did not finish its bounded readback")
            }

            let observed = recorder.snapshot()
            guard observed.writeRequestIDs == [requestID] else {
                throw Failure.assertion(
                    "uncertain paper write expected one request_id \(requestID), observed \(observed.writeRequestIDs); unexpected=\(observed.unexpectedRequests)"
                )
            }
            guard observed.readbackActions == ["request"], observed.readbackRequestIDs == [requestID] else {
                throw Failure.assertion("successful-transport decode/identity failure must issue one typed request readback")
            }
            guard observed.unexpectedRequests.isEmpty else {
                throw Failure.assertion("write recovery used an unexpected MCP tool or query action")
            }
            if expectResolved {
                guard case .success(let response) = outcome,
                      response.receipt.request_id == requestID,
                      response.session.session_id == paperSession.session_id,
                      response.session.sequence >= response.receipt.sequence else {
                    throw Failure.assertion("a valid readback must resolve a mismatched write response using the current session snapshot")
                }
            } else {
                guard case .failure(let error) = outcome,
                      let serviceError = error as? PaperSessionServiceError,
                      case .unknownMutationOutcome(let recoveredID, _) = serviceError,
                      recoveredID == requestID else {
                    throw Failure.assertion("readback absence must preserve a typed unknown-mutation outcome")
                }
            }
        }
    }

    private static func mismatchedPaperCommandText(
        policy: PaperPolicyRevisionReference,
        session: PaperSessionView
    ) throws -> String {
        let receipt = PaperSessionReceipt(
            request_id: "different-request-id",
            session_id: session.session_id,
            action: "stop",
            outcome: "APPLIED",
            expected_sequence: 7,
            sequence: session.sequence,
            created_at: "2026-10-15T00:01:00Z",
            cancelled_pending_orders: []
        )
        let mismatchedSession = PaperSessionView(
            session_id: session.session_id,
            origin_request_id: session.origin_request_id,
            state: session.state,
            sequence: session.sequence,
            policy_ref: policy,
            decision_interval: session.decision_interval,
            execution_resolution: session.execution_resolution,
            mode: session.mode,
            created_at: session.created_at,
            started_at: session.started_at,
            paused_at: session.paused_at,
            stopped_at: session.stopped_at,
            last_evaluated_at: session.last_evaluated_at,
            event_count: session.event_count,
            markets: session.markets,
            recovery_cause: session.recovery_cause
        )
        return String(
            decoding: try JSONEncoder().encode(PaperSessionCommandResponse(receipt: receipt, session: mismatchedSession)),
            as: UTF8.self
        )
    }

    private static func startRequestMatchesFixture(_ fixture: [String: Any]) throws {
        let writes = try object(fixture["write_requests"], field: "write_requests")
        let start = try object(writes["start_mock"], field: "write_requests.start_mock")
        let policy = try object(start["policy_ref"], field: "policy_ref")
        let riskObject = try object(start["risk"], field: "risk")
        let reference = AutomationPolicyRevisionReference(
            policy_id: try string(policy["policy_id"], field: "policy_id"),
            revision_id: try string(policy["revision_id"], field: "revision_id"),
            definition_digest: try string(policy["definition_digest"], field: "definition_digest")
        )
        let risk = try JSONDecoder().decode(AutomationRiskLimits.self, from: JSONSerialization.data(withJSONObject: riskObject))
        let arguments = try AutomationSessionWire.startArguments(
            requestID: try string(start["request_id"], field: "request_id"),
            policyReference: reference,
            market: try string(start["market"], field: "market"),
            decisionInterval: try string(start["decision_interval"], field: "decision_interval"),
            risk: risk
        )
        try assertJSONEqual(arguments, start, field: "start_mock")
        guard arguments["mode"] as? String == "MOCK", arguments["mode"] as? String != "LIVE" else {
            throw Failure.assertion("new native sessions must serialize explicit MOCK mode")
        }
    }

    private static func validateLiveAutomationContracts(_ fixture: [String: Any]) throws {
        func rejected(_ message: String, _ action: () throws -> Void) throws {
            do { try action() } catch is AutomationSessionServiceError { return }
            throw Failure.assertion(message)
        }
        let identity = "live:upbit:" + String(repeating: "b", count: 64)
        var responseObject = try object(fixture["command_response"], field: "command_response")
        var session = try object(responseObject["session"], field: "session")
        var riskObject = try object(session["risk"], field: "risk")
        riskObject["activation_loss_limit_krw"] = "12345.67"
        session["risk"] = riskObject
        session["mode"] = "LIVE"
        session["account_identity"] = identity
        var projection = try object(session["account_risk"], field: "account_risk")
        var liveBalance = try object(projection["balance"], field: "balance")
        liveBalance["source"] = "UPBIT_LIVE_READ"
        let currentObservation = ISO8601DateFormatter().string(from: Date())
        liveBalance["observed_at_utc"] = currentObservation
        liveBalance["received_at_utc"] = currentObservation
        projection["balance"] = liveBalance
        var capabilities = try object(projection["capabilities"], field: "capabilities")
        capabilities["live_effects_enabled"] = true
        capabilities["explicit_approval_required"] = true
        projection["capabilities"] = capabilities
        var usage = try object(projection["risk"], field: "risk")
        usage["live_grant_active"] = true
        usage["daily_loss_used_krw"] = NSNull()
        projection["risk"] = usage
        var feed = try object(projection["feed"], field: "feed")
        feed["freshness"] = "FRESH"
        feed["received_at_utc"] = currentObservation
        feed["observation_close_at_utc"] = currentObservation
        projection["feed"] = feed
        projection["day_baseline"] = NSNull()
        projection["legacy_mock_baseline"] = NSNull()
        projection["mock_assumptions"] = NSNull()
        projection["effective_loss"] = ["basis": "LIVE_ACTIVATION_OBSERVED", "limit_krw": "12000", "used_krw": "0", "admission_blocked": false, "block_reasons": []] as [String: Any]
        projection["activation_reference"] = [
            "account_identity": identity, "mode": "LIVE", "source": "CURRENT_LOCAL_OBSERVED",
            "reference_observed_at_utc": "2026-10-14T00:00:00Z", "reference_received_at_utc": "2026-10-14T00:00:00Z",
            "valuation_recorded_at_utc": "2026-10-14T00:00:00Z", "snapshot_sequence": 1, "control_sequence": 0,
            "reference_max_feed_age_seconds": 90, "balances_digest": String(repeating: "c", count: 64),
            "valuation_state": "VERIFIED", "valuation_digest": String(repeating: "d", count: 64), "reference_equity_krw": "1000000"
        ] as [String: Any]
        session["account_risk"] = projection
        responseObject["session"] = session
        let data = try JSONSerialization.data(withJSONObject: responseObject)
        let response = try JSONDecoder().decode(AutomationSessionCommandResponse.self, from: data)
        try AutomationSessionMCPService.validateSessionProjection(response.session)
        let risk = response.session.risk
        let wire = try AutomationSessionWire.startArguments(requestID: "live-start", policyReference: response.session.policy_ref, market: response.session.market, decisionInterval: response.session.decision_interval, risk: risk, mode: .live)
        guard wire["mode"] as? String == "LIVE",
              (wire["risk"] as? [String: Any])?["activation_loss_limit_krw"] as? String == "12345.67" else {
            throw Failure.assertion("LIVE mode/activation limit was lost in the start request")
        }
        try rejected("MOCK accepted an activation loss limit") { try risk.validate(for: .mock) }
        try rejected("invalid returned session identity was accepted") {
            var invalidSession = session
            invalidSession["session_id"] = "invalid session/id"
            let invalid = try JSONDecoder().decode(AutomationSessionView.self, from: JSONSerialization.data(withJSONObject: invalidSession))
            try AutomationSessionMCPService.validateSessionProjection(invalid)
        }
        let pending = AutomationPendingWrite(request_id: response.receipt.request_id, action: .start,
            policy_ref: response.session.policy_ref, market: response.session.market,
            decision_interval: response.session.decision_interval, risk: risk, mode: .live)
        try pending.validate(response)
        let roundTrip = try JSONDecoder().decode(AutomationPendingWrite.self, from: JSONEncoder().encode(pending))
        guard roundTrip == pending else { throw Failure.assertion("LIVE pending command changed during persistence") }
        var legacy = try object(JSONSerialization.jsonObject(with: JSONEncoder().encode(pending)), field: "pending")
        legacy.removeValue(forKey: "mode")
        let decodedLegacy = try JSONDecoder().decode(AutomationPendingWrite.self, from: JSONSerialization.data(withJSONObject: legacy))
        guard decodedLegacy.mode == .mock else { throw Failure.assertion("legacy pending intent silently became LIVE") }
        try rejected("legacy MOCK pending accepted a LIVE result") { try decodedLegacy.validate(response) }
        var badProjection = projection
        var badReference = try object(projection["activation_reference"], field: "activation_reference")
        badReference["account_identity"] = "live:upbit:" + String(repeating: "e", count: 64)
        badProjection["activation_reference"] = badReference
        let mismatched = try JSONDecoder().decode(AutomationAccountRiskProjection.self, from: JSONSerialization.data(withJSONObject: badProjection))
        try rejected("activation reference from another account was accepted") {
            try AutomationSessionMCPService.validateAccountRiskProjection(mismatched, accountIdentity: identity, mode: .live)
        }
        badReference = try object(projection["activation_reference"], field: "activation_reference")
        badReference["reference_observed_at_utc"] = ISO8601DateFormatter().string(from: Date().addingTimeInterval(3600))
        badProjection["activation_reference"] = badReference
        let future = try JSONDecoder().decode(AutomationAccountRiskProjection.self, from: JSONSerialization.data(withJSONObject: badProjection))
        try rejected("future local activation reference was accepted") {
            try AutomationSessionMCPService.validateAccountRiskProjection(future, accountIdentity: identity, mode: .live)
        }
        badProjection = projection
        badProjection["activation_reference"] = NSNull()
        let missing = try JSONDecoder().decode(AutomationAccountRiskProjection.self, from: JSONSerialization.data(withJSONObject: badProjection))
        try rejected("unblocked LIVE activation accepted no reference") {
            try AutomationSessionMCPService.validateAccountRiskProjection(missing, accountIdentity: identity, mode: .live)
        }
        badProjection = projection
        liveBalance["source"] = "DURABLE_MOCK_VENUE"
        badProjection["balance"] = liveBalance
        let mockEvidence = try JSONDecoder().decode(AutomationAccountRiskProjection.self, from: JSONSerialization.data(withJSONObject: badProjection))
        try rejected("MOCK account evidence was presented as LIVE") {
            try AutomationSessionMCPService.validateAccountRiskProjection(mockEvidence, accountIdentity: identity, mode: .live)
        }
        try validateLiveAutomationServiceTransport(responseObject, identity: identity)
        print("PASS LIVE start wire, activation evidence, mode/account binding and legacy pending persistence")
    }

    private static func validateLiveAutomationServiceTransport(_ responseObject: [String: Any], identity: String) throws {
        let recorder = PaperWriteRecoveryRecorder()
        PaperWriteRecoveryURLProtocol.install { request in
            guard let body = PaperWriteRecoveryURLProtocol.requestBody(request),
                  let envelope = try? JSONSerialization.jsonObject(with: body) as? [String: Any],
                  let messageID = envelope["id"] as? String else { return nil }
            let result: [String: Any]
            if envelope["method"] as? String == "initialize" {
                result = ["protocolVersion": "2025-03-26"]
            } else if let params = envelope["params"] as? [String: Any],
                      params["name"] as? String == "automation_session_write",
                      let arguments = params["arguments"] as? [String: Any] {
                recorder.recordWrite(requestID: arguments["request_id"] as? String)
                var response = responseObject
                var receipt = response["receipt"] as? [String: Any] ?? [:]
                receipt["request_id"] = arguments["request_id"]
                receipt["action"] = (arguments["action"] as? String)?.uppercased()
                response["receipt"] = receipt
                let text = (try? JSONSerialization.data(withJSONObject: response)).map { String(decoding: $0, as: UTF8.self) } ?? ""
                result = ["isError": false, "content": [["type": "text", "text": text]]]
            } else { return nil }
            return try? JSONSerialization.data(withJSONObject: ["jsonrpc": "2.0", "id": messageID, "result": result])
        }
        defer { PaperWriteRecoveryURLProtocol.clear() }
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [PaperWriteRecoveryURLProtocol.self]
        let session = URLSession(configuration: config)
        defer { session.invalidateAndCancel() }
        let client = NativeMCPClient(session: session)
        let reviewedContext = client.configure(port: 65_431, token: String(repeating: "a", count: 64))
        let service = AutomationSessionMCPService(client: client)
        let response = try JSONDecoder().decode(AutomationSessionCommandResponse.self, from: JSONSerialization.data(withJSONObject: responseObject))
        var outcome: Result<AutomationSessionCommandResponse, Error>?
        _ = try service.startSession(requestID: "live-transport-start", policyReference: response.session.policy_ref,
            market: response.session.market, decisionInterval: response.session.decision_interval,
            risk: response.session.risk, mode: .live) { outcome = $0 }
        let deadline = Date().addingTimeInterval(5)
        while outcome == nil, Date() < deadline { _ = RunLoop.current.run(mode: .default, before: Date().addingTimeInterval(0.01)) }
        guard let completedOutcome = outcome else { throw Failure.assertion("LIVE start transport did not complete") }
        _ = try completedOutcome.get()
        do {
            _ = try service.transition(command: .pause, requestID: "missing-account", sessionID: response.session.session_id,
                expectedSequence: response.session.sequence, expectedMode: .live) { _ in }
            throw Failure.assertion("LIVE transition accepted no expected account before transport")
        } catch is AutomationSessionServiceError { }
        guard recorder.snapshot().writeRequestIDs == ["live-transport-start"] else {
            throw Failure.assertion("LIVE transition without an account sent a mutation")
        }
        client.configure(port: 65_431, token: String(repeating: "b", count: 64))
        outcome = nil
        _ = try service.startSession(requestID: "stale-review-must-not-send", policyReference: response.session.policy_ref,
            market: response.session.market, decisionInterval: response.session.decision_interval,
            risk: response.session.risk, mode: .live, expectedContext: reviewedContext) { outcome = $0 }
        guard case .failure? = outcome, recorder.snapshot().writeRequestIDs == ["live-transport-start"] else {
            throw Failure.assertion("a LIVE review from another Backend generation dispatched a mutation")
        }
    }

    private static func lifecycleAndQueryRequestsMatchFixture(_ fixture: [String: Any]) throws {
        let writes = try object(fixture["write_requests"], field: "write_requests")
        for (command, key) in [
            (AutomationLifecycleCommand.pause, "pause"),
            (.resume, "resume"),
            (.stop, "stop")
        ] {
            let expected = try object(writes[key], field: "write_requests.\(key)")
            let actual = try AutomationSessionWire.lifecycleArguments(
                command: command,
                requestID: string(expected["request_id"], field: "request_id"),
                sessionID: string(expected["session_id"], field: "session_id"),
                expectedSequence: unsigned(expected["expected_sequence"], field: "expected_sequence")
            )
            try assertJSONEqual(actual, expected, field: key)
        }
        let cancellation = try object(writes["cancel_order"], field: "write_requests.cancel_order")
        try assertJSONEqual(
            AutomationSessionWire.cancelOrderArguments(
                requestID: string(cancellation["request_id"], field: "request_id"),
                sessionID: string(cancellation["session_id"], field: "session_id"),
                expectedSequence: unsigned(cancellation["expected_sequence"], field: "expected_sequence"),
                clientIdentifier: string(cancellation["client_identifier"], field: "client_identifier")
            ),
            cancellation,
            field: "cancel_order"
        )
        let recordBaseline = try object(writes["record_baseline"], field: "write_requests.record_baseline")
        let baselineInputObject = try object(recordBaseline["baseline"], field: "write_requests.record_baseline.baseline")
        let baselineInput = try JSONDecoder().decode(
            AutomationAccountDayBaselineInput.self,
            from: JSONSerialization.data(withJSONObject: baselineInputObject)
        )
        try assertJSONEqual(
            AutomationSessionWire.recordBaselineArguments(
                requestID: string(recordBaseline["request_id"], field: "request_id"),
                sessionID: string(recordBaseline["session_id"], field: "session_id"),
                expectedSequence: unsigned(recordBaseline["expected_sequence"], field: "expected_sequence"),
                mode: .mock,
                baseline: baselineInput
            ),
            recordBaseline,
            field: "record_baseline"
        )
        guard baselineInput.source == .durableMockVenueSeed else {
            throw Failure.assertion("MOCK baseline request must select the durable mock venue seed")
        }
        try baselineInput.validate(for: .mock)
        let liveIdentity = "live:upbit:" + String(repeating: "a", count: 64)
        let liveSeedInput = AutomationAccountDayBaselineInput(
            account_identity: liveIdentity,
            day_utc: baselineInput.day_utc,
            reference_time_utc: baselineInput.reference_time_utc,
            source: .durableMockVenueSeed,
            assets: baselineInput.assets,
            operator_note: nil
        )
        do {
            try liveSeedInput.validate(for: .live)
            throw Failure.assertion("MOCK venue seed was accepted for a LIVE account")
        } catch is AutomationSessionServiceError {
        }

        let queries = try object(fixture["query_requests"], field: "query_requests")
        try assertJSONEqual(AutomationSessionWire.listArguments(), try object(queries["list"], field: "list"), field: "list")
        let get = try object(queries["get"], field: "get")
        try assertJSONEqual(
            AutomationSessionWire.getArguments(sessionID: string(get["session_id"], field: "session_id")),
            get,
            field: "get"
        )
        let request = try object(queries["request"], field: "request")
        try assertJSONEqual(
            AutomationSessionWire.requestArguments(requestID: string(request["request_id"], field: "request_id")),
            request,
            field: "request"
        )
        let baselineQuery = try object(queries["baseline"], field: "baseline")
        try assertJSONEqual(
            AutomationSessionWire.baselineArguments(
                sessionID: string(baselineQuery["session_id"], field: "session_id"),
                dayUTC: string(baselineQuery["day_utc"], field: "day_utc")
            ),
            baselineQuery,
            field: "baseline"
        )
        let events = try object(queries["events"], field: "events")
        try assertJSONEqual(
            AutomationSessionWire.eventsArguments(sessionID: string(events["session_id"], field: "session_id")),
            events,
            field: "events"
        )
        let ledger = try object(queries["ledger"], field: "ledger")
        try assertJSONEqual(
            AutomationSessionWire.ledgerArguments(sessionID: string(ledger["session_id"], field: "session_id")),
            ledger,
            field: "ledger"
        )
    }

    private static func validateBaselineSourceChoices() throws {
        let mockSources = AutomationBaselineSource.selectableSources(for: .mock)
        let liveSources = AutomationBaselineSource.selectableSources(for: .live)
        guard mockSources.first == .durableMockVenueSeed,
              !liveSources.contains(.durableMockVenueSeed),
              liveSources.contains(.exchangeObservedDayStart),
              liveSources.contains(.priorDayVerifiedLedgerRollover),
              liveSources.contains(.operatorAttested) else {
            throw Failure.assertion("baseline source choices must match selected session mode")
        }
        do {
            _ = try AutomationBaselineSource.selectableSource(at: liveSources.count, for: .live)
            throw Failure.assertion("out-of-range baseline source selection fell back to a source")
        } catch is AutomationSessionServiceError {
        }
    }

    private static func validateBaselineStatusConsistency(_ fixture: [String: Any]) throws {
        let validObject = try object(fixture["baseline_status"], field: "baseline_status")
        let valid = try JSONDecoder().decode(
            AutomationAccountDayBaselineStatus.self,
            from: JSONSerialization.data(withJSONObject: validObject)
        )
        try AutomationSessionMCPService.validateBaselineStatusClaims(valid)

        var blockedObject = validObject
        var blockedBaseline = try object(blockedObject["baseline"], field: "baseline_status.baseline")
        let liveIdentity = "live:upbit:" + String(repeating: "a", count: 64)
        blockedBaseline["account_identity"] = liveIdentity
        blockedBaseline["mode"] = "LIVE"
        blockedBaseline["source"] = "EXCHANGE_OBSERVED_DAY_START"
        blockedBaseline["verification_state"] = "BLOCKED_INSUFFICIENT_EVIDENCE"
        blockedBaseline["sufficient_for_risk_admission"] = false
        blockedBaseline["block_reasons"] = ["LIVE_PROVIDER_AS_OF_EVIDENCE_UNAVAILABLE"]
        blockedObject["baseline"] = blockedBaseline
        blockedObject["account_identity"] = liveIdentity
        blockedObject["mode"] = "LIVE"
        blockedObject["admission_blocked"] = true
        blockedObject["block_reasons"] = ["LIVE_PROVIDER_AS_OF_EVIDENCE_UNAVAILABLE"]
        let blocked = try JSONDecoder().decode(
            AutomationAccountDayBaselineStatus.self,
            from: JSONSerialization.data(withJSONObject: blockedObject)
        )
        try AutomationSessionMCPService.validateBaselineStatusClaims(blocked)
        guard let blockedRecord = blocked.baseline,
              blocked.admission_blocked,
              blockedRecord.verification_state == .blockedInsufficientEvidence,
              !blockedRecord.sufficient_for_risk_admission,
              blocked.block_reasons == ["LIVE_PROVIDER_AS_OF_EVIDENCE_UNAVAILABLE"] else {
            throw Failure.assertion("explicit backend block must remain visible in the read model")
        }

        blockedObject["admission_blocked"] = false
        blockedObject["block_reasons"] = []
        let contradictory = try JSONDecoder().decode(
            AutomationAccountDayBaselineStatus.self,
            from: JSONSerialization.data(withJSONObject: blockedObject)
        )
        do {
            try AutomationSessionMCPService.validateBaselineStatusClaims(contradictory)
            throw Failure.assertion("blocked baseline was presented as admitting orders")
        } catch is AutomationSessionServiceError {
        }

        var missingObject = validObject
        missingObject["baseline"] = NSNull()
        missingObject["admission_blocked"] = false
        let missingButAllowed = try JSONDecoder().decode(
            AutomationAccountDayBaselineStatus.self,
            from: JSONSerialization.data(withJSONObject: missingObject)
        )
        do {
            try AutomationSessionMCPService.validateBaselineStatusClaims(missingButAllowed)
            throw Failure.assertion("missing baseline was presented as admitting orders")
        } catch is AutomationSessionServiceError {
        }

        missingObject["admission_blocked"] = true
        missingObject["block_reasons"] = ["DAILY_START_BASELINE_MISSING"]
        let missingAndBlocked = try JSONDecoder().decode(
            AutomationAccountDayBaselineStatus.self,
            from: JSONSerialization.data(withJSONObject: missingObject)
        )
        try AutomationSessionMCPService.validateBaselineStatusClaims(missingAndBlocked)
        guard missingAndBlocked.baseline == nil,
              missingAndBlocked.admission_blocked,
              missingAndBlocked.block_reasons == ["DAILY_START_BASELINE_MISSING"] else {
            throw Failure.assertion("missing baseline must stay unknown and blocked")
        }
    }

    private static func validateLegacyMockBaselineProjection(_ fixture: [String: Any]) throws {
        guard let now = ISO8601DateFormatter().date(from: "2026-10-14T00:01:00Z") else {
            throw Failure.assertion("fixed UTC validation time could not be parsed")
        }
        let mockIdentity = "mock:upbit-default"
        let digest = String(repeating: "a", count: 64)
        let legacy: [String: Any] = [
            "account_identity": mockIdentity,
            "day_utc": "2026-10-14",
            "baseline_equity_krw": "12345.67",
            "balances_digest": digest,
            "recorded_at_utc": "2026-10-14T00:00:00Z"
        ]

        var sessionObject = try object(fixture["session_view"], field: "session_view")
        var accountRisk = try object(sessionObject["account_risk"], field: "session_view.account_risk")
        let typedBaseline = try object(accountRisk["day_baseline"], field: "session_view.account_risk.day_baseline")
        accountRisk["day_baseline"] = NSNull()
        accountRisk["legacy_mock_baseline"] = legacy
        sessionObject["account_risk"] = accountRisk
        let legacySession = try JSONDecoder().decode(
            AutomationSessionView.self,
            from: JSONSerialization.data(withJSONObject: sessionObject)
        )
        try AutomationSessionMCPService.validateSessionProjection(legacySession, now: now)
        guard let mockProjection = legacySession.account_risk,
              mockProjection.day_baseline == nil,
              mockProjection.legacy_mock_baseline?.baseline_equity_krw == "12345.67",
              mockProjection.capabilities.can_submit_orders else {
            throw Failure.assertion("validated legacy MOCK scalar must remain exact and distinct from a typed day baseline")
        }

        var statusObject = try object(fixture["baseline_status"], field: "baseline_status")
        statusObject["baseline"] = NSNull()
        statusObject["legacy_mock_baseline"] = legacy
        statusObject["admission_blocked"] = false
        statusObject["block_reasons"] = []
        let legacyStatus = try JSONDecoder().decode(
            AutomationAccountDayBaselineStatus.self,
            from: JSONSerialization.data(withJSONObject: statusObject)
        )
        try AutomationSessionMCPService.validateBaselineStatusClaims(legacyStatus, now: now)
        guard legacyStatus.baseline == nil,
              legacyStatus.legacy_mock_baseline?.balances_digest == digest,
              !legacyStatus.admission_blocked else {
            throw Failure.assertion("valid legacy MOCK status must retain its explicit scalar projection")
        }

        func expectLegacyRejection(_ name: String, _ validate: () throws -> Void) throws {
            do {
                try validate()
                throw Failure.assertion(name)
            } catch is AutomationSessionServiceError {
            }
        }

        var futureRisk = accountRisk
        var futureLegacy = legacy
        futureLegacy["recorded_at_utc"] = "2026-10-14T00:02:00Z"
        futureRisk["legacy_mock_baseline"] = futureLegacy
        var futureSessionObject = sessionObject
        futureSessionObject["account_risk"] = futureRisk
        let futureSession = try JSONDecoder().decode(
            AutomationSessionView.self,
            from: JSONSerialization.data(withJSONObject: futureSessionObject)
        )
        try expectLegacyRejection("future local legacy timestamp was accepted") {
            try AutomationSessionMCPService.validateSessionProjection(futureSession, now: now)
        }

        var malformedRisk = accountRisk
        var malformedLegacy = legacy
        malformedLegacy["recorded_at_utc"] = "not-an-rfc3339-timestamp"
        malformedRisk["legacy_mock_baseline"] = malformedLegacy
        var malformedSessionObject = sessionObject
        malformedSessionObject["account_risk"] = malformedRisk
        let malformedSession = try JSONDecoder().decode(
            AutomationSessionView.self,
            from: JSONSerialization.data(withJSONObject: malformedSessionObject)
        )
        try expectLegacyRejection("malformed local legacy timestamp was accepted") {
            try AutomationSessionMCPService.validateSessionProjection(malformedSession, now: now)
        }

        var corruptRisk = accountRisk
        var corruptLegacy = legacy
        corruptLegacy["balances_digest"] = "invalid-digest"
        corruptRisk["legacy_mock_baseline"] = corruptLegacy
        var corruptSessionObject = sessionObject
        corruptSessionObject["account_risk"] = corruptRisk
        let corruptSession = try JSONDecoder().decode(
            AutomationSessionView.self,
            from: JSONSerialization.data(withJSONObject: corruptSessionObject)
        )
        try expectLegacyRejection("invalid legacy balances digest was accepted") {
            try AutomationSessionMCPService.validateSessionProjection(corruptSession, now: now)
        }

        var wrongDayStatusObject = statusObject
        var wrongDayLegacy = legacy
        wrongDayLegacy["day_utc"] = "2026-10-13"
        wrongDayStatusObject["legacy_mock_baseline"] = wrongDayLegacy
        let wrongDayStatus = try JSONDecoder().decode(
            AutomationAccountDayBaselineStatus.self,
            from: JSONSerialization.data(withJSONObject: wrongDayStatusObject)
        )
        try expectLegacyRejection("legacy baseline for a different UTC day was accepted") {
            try AutomationSessionMCPService.validateBaselineStatusClaims(wrongDayStatus, now: now)
        }

        var missingRisk = accountRisk
        missingRisk["legacy_mock_baseline"] = NSNull()
        var missingSessionObject = sessionObject
        missingSessionObject["account_risk"] = missingRisk
        let missingSession = try JSONDecoder().decode(
            AutomationSessionView.self,
            from: JSONSerialization.data(withJSONObject: missingSessionObject)
        )
        try expectLegacyRejection("order submission capability was accepted without either baseline projection") {
            try AutomationSessionMCPService.validateSessionProjection(missingSession, now: now)
        }

        var conflictingRisk = accountRisk
        conflictingRisk["day_baseline"] = typedBaseline
        conflictingRisk["legacy_mock_baseline"] = legacy
        var conflictingSessionObject = sessionObject
        conflictingSessionObject["account_risk"] = conflictingRisk
        let conflictingSession = try JSONDecoder().decode(
            AutomationSessionView.self,
            from: JSONSerialization.data(withJSONObject: conflictingSessionObject)
        )
        try expectLegacyRejection("typed and legacy baseline projections were both accepted") {
            try AutomationSessionMCPService.validateSessionProjection(conflictingSession, now: now)
        }

        let liveIdentity = "live:upbit:" + String(repeating: "a", count: 64)
        var liveLegacy = legacy
        liveLegacy["account_identity"] = liveIdentity
        var liveStatusObject = statusObject
        liveStatusObject["mode"] = "LIVE"
        liveStatusObject["account_identity"] = liveIdentity
        liveStatusObject["legacy_mock_baseline"] = liveLegacy
        let liveLegacyStatus = try JSONDecoder().decode(
            AutomationAccountDayBaselineStatus.self,
            from: JSONSerialization.data(withJSONObject: liveStatusObject)
        )
        try expectLegacyRejection("legacy mock projection was accepted for LIVE mode") {
            try AutomationSessionMCPService.validateBaselineStatusClaims(liveLegacyStatus, now: now)
        }
    }

    private static func decodeFrozenReadProjections(_ fixture: [String: Any]) throws {
        let sessionObject = try object(fixture["session_view"], field: "session_view")
        let session = try JSONDecoder().decode(AutomationSessionView.self, from: JSONSerialization.data(withJSONObject: sessionObject))
        guard session.mode == .mock, session.state == .running,
              session.sequence == 1,
              session.account_identity == "mock:upbit-default",
              session.policy_ref.revision_id == "revision-example-r1",
              session.account_risk?.balance.quote_free == "10000000",
              session.account_risk?.balance.quote_exchange_locked == "0",
              session.account_risk?.balance.base_total == "0",
              session.account_risk?.balance.observed_at_utc == "2026-10-14T00:00:00Z",
              session.account_risk?.balance.received_at_utc == "2026-10-14T00:00:00Z",
              session.account_risk?.risk.daily_notional_used_krw == "0",
              session.account_risk?.feed.freshness == .unavailable,
              session.account_risk?.feed.observation_close_at_utc == nil,
              session.account_risk?.capabilities.live_effects_enabled == false,
              session.account_risk?.mock_assumptions?.fee_rate == "0.0005",
              session.account_risk?.day_baseline?.verification_state == .verified,
              session.account_risk?.day_baseline?.sufficient_for_risk_admission == true,
              session.account_risk?.day_baseline?.total_equity_krw == "10000000",
              session.account_risk?.admission_block_reasons == [] else {
            throw Failure.assertion("session mode, frozen identity, exact zero/amount, unknown feed, day_baseline, or returned mock assumption changed")
        }

        let baselineStatusObject = try object(fixture["baseline_status"], field: "baseline_status")
        let baselineStatus = try JSONDecoder().decode(
            AutomationAccountDayBaselineStatus.self,
            from: JSONSerialization.data(withJSONObject: baselineStatusObject)
        )
        guard baselineStatus.session_id == session.session_id,
              baselineStatus.account_identity == session.account_identity,
              baselineStatus.mode == .mock,
              baselineStatus.day_utc == "2026-10-14",
              baselineStatus.baseline == session.account_risk?.day_baseline,
              !baselineStatus.admission_blocked,
              baselineStatus.block_reasons.isEmpty else {
            throw Failure.assertion("baseline status projection did not round trip")
        }

        let responseObject = try object(fixture["command_response"], field: "command_response")
        let command = try JSONDecoder().decode(AutomationSessionCommandResponse.self, from: JSONSerialization.data(withJSONObject: responseObject))
        guard command.receipt.request_id == "auto-start-001", command.receipt.action == .start,
              command.session.session_id == session.session_id,
              command.session.account_risk == session.account_risk else {
            throw Failure.assertion("command response identity or projection did not round trip")
        }

        let pageObject = try object(fixture["session_page"], field: "session_page")
        let page = try JSONDecoder().decode(AutomationSessionPage.self, from: JSONSerialization.data(withJSONObject: pageObject))
        guard page.returned_count == 0, page.items.isEmpty, page.next_offset == nil else {
            throw Failure.assertion("empty session page/null continuation changed")
        }

        let eventsObject = try object(fixture["event_page"], field: "event_page")
        let events = try JSONDecoder().decode(AutomationEventPage.self, from: JSONSerialization.data(withJSONObject: eventsObject))
        guard events.items.count == 1, events.items[0].session_id == session.session_id,
              events.items[0].sequence == 1, events.items[0].kind == .started,
              events.items[0].detail.formattedJSON.contains("MOCK") else {
            throw Failure.assertion("event identity/detail contract changed")
        }

        let ledgerObject = try object(fixture["ledger_page"], field: "ledger_page")
        let ledger = try JSONDecoder().decode(AutomationLedgerPage.self, from: JSONSerialization.data(withJSONObject: ledgerObject))
        guard ledger.orders.count == 1, ledger.orders[0].state == .filled,
              ledger.orders[0].requested_notional_krw == "100000",
              ledger.orders[0].reserved_fee_krw == "50",
              ledger.orders[0].locked_quote_krw == "0",
              ledger.orders[0].locked_base == nil,
              ledger.fills.count == 1,
              ledger.fills[0].price_krw == "100000000",
              ledger.fills[0].funds_krw == "100000" else {
            throw Failure.assertion("exact order/fill ledger amount contract changed")
        }

        let missingReadback = try object(fixture["request_readback"], field: "request_readback")
        let readback = try JSONDecoder().decode(AutomationSessionRequestReadback.self, from: JSONSerialization.data(withJSONObject: missingReadback))
        guard readback.request_id == "auto-start-001", readback.state == .notRecorded,
              readback.response == nil,
              readback.reason == "request is not durably recorded; outcome is unconfirmed" else {
            throw Failure.assertion("missing durable request must remain explicitly unresolved")
        }
        try AutomationSessionMCPService.validate(readback, requestID: "auto-start-001")
        var foundObject = missingReadback
        foundObject["state"] = "APPLIED"
        foundObject["response"] = responseObject
        let found = try JSONDecoder().decode(AutomationSessionRequestReadback.self, from: JSONSerialization.data(withJSONObject: foundObject))
        guard found.state == .applied, found.response?.receipt.request_id == found.request_id else {
            throw Failure.assertion("found durable request must return its original command response")
        }
        try AutomationSessionMCPService.validate(found, requestID: "auto-start-001")
        var rejectedObject = missingReadback
        rejectedObject["state"] = "REJECTED"
        rejectedObject["reason"] = "sequence changed"
        let rejected = try JSONDecoder().decode(AutomationSessionRequestReadback.self, from: JSONSerialization.data(withJSONObject: rejectedObject))
        guard rejected.state == .rejected, rejected.response == nil, rejected.reason == "sequence changed" else {
            throw Failure.assertion("durable rejected request state/reason changed")
        }
        try AutomationSessionMCPService.validate(rejected, requestID: "auto-start-001")
    }

    private static func validateRiskInputBoundaries() throws {
        let valid = AutomationRiskLimits(
            max_order_notional_krw: "100000",
            max_daily_notional_krw: "1000000",
            max_asset_exposure_krw: "500000",
            max_daily_loss_krw: "50000",
            max_open_orders: 2,
            max_orders_per_interval: 1,
            max_balance_age_seconds: 30,
            max_feed_age_seconds: 90
        )
        try valid.validate()
        let zero = AutomationRiskLimits(
            max_order_notional_krw: "0",
            max_daily_notional_krw: "1000000",
            max_asset_exposure_krw: "500000",
            max_daily_loss_krw: "50000",
            max_open_orders: 2,
            max_orders_per_interval: 1,
            max_balance_age_seconds: 30,
            max_feed_age_seconds: 90
        )
        do {
            try zero.validate()
            throw Failure.assertion("zero order limit was accepted")
        } catch is AutomationSessionServiceError {
        }
        let inverted = AutomationRiskLimits(
            max_order_notional_krw: "1000001",
            max_daily_notional_krw: "1000000",
            max_asset_exposure_krw: "500000",
            max_daily_loss_krw: "50000",
            max_open_orders: 2,
            max_orders_per_interval: 1,
            max_balance_age_seconds: 30,
            max_feed_age_seconds: 90
        )
        do {
            try inverted.validate()
            throw Failure.assertion("crossed order/daily limits were accepted")
        } catch is AutomationSessionServiceError {
        }
        let exponent = AutomationRiskLimits(
            max_order_notional_krw: "1e5",
            max_daily_notional_krw: "1000000",
            max_asset_exposure_krw: "500000",
            max_daily_loss_krw: "50000",
            max_open_orders: 2,
            max_orders_per_interval: 1,
            max_balance_age_seconds: 30,
            max_feed_age_seconds: 90
        )
        do {
            try exponent.validate()
            throw Failure.assertion("non-decimal exponent notation was accepted for exact amounts")
        } catch is AutomationSessionServiceError {
        }
    }

    private static func validateAccountIdentityScopes() throws {
        try AutomationAccountIdentityScope.validate("mock:upbit-default", mode: .mock)
        let fingerprint = String(repeating: "a", count: 64)
        let liveIdentity = "live:upbit:\(fingerprint)"
        try AutomationAccountIdentityScope.validate(liveIdentity, mode: .live)
        guard AutomationAccountIdentityScope.presentation(for: .mock) == "mock:upbit-default",
              !AutomationAccountIdentityScope.presentation(for: .live).contains(fingerprint) else {
            throw Failure.assertion("credential-derived account fingerprint was exposed")
        }
        do {
            try AutomationAccountIdentityScope.validate(liveIdentity, mode: .mock)
            throw Failure.assertion("LIVE account scope was accepted in MOCK mode")
        } catch is AutomationSessionServiceError {
        }
        do {
            try AutomationAccountIdentityScope.validate("live:upbit:not-a-fingerprint", mode: .live)
            throw Failure.assertion("malformed LIVE account fingerprint was accepted")
        } catch is AutomationSessionServiceError {
        }
        let detail = AutomationJSONValue.object([
            "account_identity": .string(liveIdentity),
            "api_token": .string("fake-token-value"),
            "policy_id": .string("policy-example")
        ])
        let display = detail.redactedForDisplay.formattedJSON
        guard !display.contains(liveIdentity), !display.contains("fake-token-value"),
              display.contains("policy-example"), display.contains("[hidden]") else {
            throw Failure.assertion("event JSON display did not hide credential-derived fields")
        }
    }

    private static func validateEventDetailAmountMasking() throws {
        let detail = AutomationJSONValue.object([
            "action": .string("ORDER_ADMITTED"),
            "evaluation_digest": .string("evaluation-reference-hash"),
            "reason": .string("RISK_LIMIT"),
            "client_identifier": .string("auto-safe-identifier"),
            "policy_id": .string("policy-safe-identifier"),
            "sequence": .number(3),
            "notional_krw": .string("100000.25"),
            "fee_delta": .string("0.0005"),
            "fill": .object([
                "trade_uuid": .string("trade-safe-identifier"),
                "price_krw": .string("99000.75"),
                "volume": .string("0.0007"),
                "locked_quote_krw": .string("25000")
            ])
        ])
        let masked = detail.redactedForDisplay(hidingMonetaryValues: true).formattedJSON
        let unmasked = detail.redactedForDisplay(hidingMonetaryValues: false).formattedJSON
        guard masked.contains("ORDER_ADMITTED"), masked.contains("RISK_LIMIT"),
              masked.contains("evaluation-reference-hash"),
              masked.contains("auto-safe-identifier"), masked.contains("policy-safe-identifier"),
              masked.contains("trade-safe-identifier"), masked.contains("sequence"),
              masked.contains("[금액 숨김]"),
              !masked.contains("100000.25"), !masked.contains("0.0005"),
              !masked.contains("99000.75"), !masked.contains("0.0007"),
              !masked.contains("25000"),
              unmasked.contains("100000.25"), unmasked.contains("0.0005"),
              unmasked.contains("99000.75"), unmasked.contains("0.0007"),
              unmasked.contains("25000") else {
            throw Failure.assertion("event detail must retain safe metadata while masking monetary values only when hidden")
        }
    }

    private static func validateReadbackStateAndMutationIdentity(_ fixture: [String: Any]) throws {
        let response = try JSONDecoder().decode(
            AutomationSessionCommandResponse.self,
            from: JSONSerialization.data(withJSONObject: try object(fixture["command_response"], field: "command_response"))
        )
        let start = AutomationPendingWrite(
            request_id: response.receipt.request_id,
            action: .start,
            policy_ref: response.session.policy_ref,
            market: response.session.market,
            decision_interval: response.session.decision_interval,
            risk: response.session.risk
        )
        try start.validate(response)

        let wrongAction = AutomationPendingWrite(
            request_id: response.receipt.request_id,
            action: .pause,
            session_id: response.session.session_id,
            expected_sequence: 1
        )
        do {
            try wrongAction.validate(response)
            throw Failure.assertion("APPLIED readback with a different action was accepted")
        } catch is AutomationSessionServiceError {
        }
        let pausedSession = AutomationSessionView(
            session_id: response.session.session_id,
            account_identity: response.session.account_identity,
            mode: response.session.mode,
            state: .paused,
            sequence: 3,
            policy_ref: response.session.policy_ref,
            market: response.session.market,
            decision_interval: response.session.decision_interval,
            risk: response.session.risk,
            created_at_utc: response.session.created_at_utc,
            updated_at_utc: response.session.updated_at_utc,
            last_finalized_candle_at: response.session.last_finalized_candle_at,
            last_balance_observed_at: response.session.last_balance_observed_at,
            unresolved_order_count: response.session.unresolved_order_count,
            fill_count: response.session.fill_count,
            recovery_reason: response.session.recovery_reason,
            account_risk: response.session.account_risk
        )
        let pausedReceipt = AutomationSessionReceipt(
            request_id: response.receipt.request_id,
            action: .pause,
            outcome: .applied,
            sequence: 3,
            observed_at_utc: response.receipt.observed_at_utc,
            reason: nil
        )
        let pausedResponse = AutomationSessionCommandResponse(session: pausedSession, receipt: pausedReceipt, order: nil)
        try AutomationPendingWrite(
            request_id: response.receipt.request_id,
            action: .pause,
            session_id: response.session.session_id,
            expected_sequence: 2
        ).validate(pausedResponse)
        let wrongSequence = AutomationPendingWrite(
            request_id: response.receipt.request_id,
            action: .pause,
            session_id: response.session.session_id,
            expected_sequence: 4
        )
        do {
            try wrongSequence.validate(pausedResponse)
            throw Failure.assertion("APPLIED readback older than the expected sequence was accepted")
        } catch is AutomationSessionServiceError {
        }

        let commandObject = try object(fixture["command_response"], field: "command_response")
        var pendingCommand = commandObject
        var pendingReceipt = try object(pendingCommand["receipt"], field: "command_response.receipt")
        pendingReceipt["outcome"] = "PENDING"
        pendingCommand["receipt"] = pendingReceipt
        var pendingObject = try object(fixture["request_readback"], field: "request_readback")
        pendingObject["state"] = "PENDING"
        pendingObject["response"] = pendingCommand
        pendingObject["reason"] = "durable effect remains pending"
        let pending = try JSONDecoder().decode(
            AutomationSessionRequestReadback.self,
            from: JSONSerialization.data(withJSONObject: pendingObject)
        )
        try AutomationSessionMCPService.validate(pending, requestID: response.receipt.request_id)
        let pendingWrite = AutomationPendingWrite(
            request_id: response.receipt.request_id,
            action: .start,
            policy_ref: response.session.policy_ref,
            market: response.session.market,
            decision_interval: response.session.decision_interval,
            risk: response.session.risk
        )
        try pendingWrite.validate(try XCTUnwrapResponse(pending.response))

        let fixtureLedger = try object(fixture["ledger_page"], field: "ledger_page")
        guard let fixtureOrders = fixtureLedger["orders"] as? [[String: Any]],
              let fixtureOrder = fixtureOrders.first else {
            throw Failure.assertion("missing order fixture for cancellation identity")
        }
        let filledOrder = try JSONDecoder().decode(
            AutomationOrderView.self,
            from: JSONSerialization.data(withJSONObject: fixtureOrder)
        )
        let openOrder = AutomationOrderView(
            client_identifier: filledOrder.client_identifier,
            intent_id: filledOrder.intent_id,
            market: filledOrder.market,
            side: filledOrder.side,
            state: .open,
            venue_order_uuid: filledOrder.venue_order_uuid,
            requested_notional_krw: filledOrder.requested_notional_krw,
            reserved_fee_krw: filledOrder.reserved_fee_krw,
            executed_volume: filledOrder.executed_volume,
            paid_fee_krw: filledOrder.paid_fee_krw,
            locked_quote_krw: "100050",
            locked_base: nil,
            trade_count: filledOrder.trade_count,
            created_at_utc: filledOrder.created_at_utc,
            updated_at_utc: filledOrder.updated_at_utc
        )
        let cancelReceipt = AutomationSessionReceipt(
            request_id: "auto-cancel-001",
            action: .cancelOrder,
            outcome: .pending,
            sequence: response.session.sequence,
            observed_at_utc: response.receipt.observed_at_utc,
            reason: "mock cancel is reconciling"
        )
        let cancelResponse = AutomationSessionCommandResponse(session: response.session, receipt: cancelReceipt, order: openOrder)
        try AutomationPendingWrite(
            request_id: "auto-cancel-001",
            action: .cancelOrder,
            session_id: response.session.session_id,
            expected_sequence: response.session.sequence,
            client_identifier: openOrder.client_identifier
        ).validate(cancelResponse)

        if let dayBaseline = response.session.account_risk?.day_baseline {
            let baselineReceipt = AutomationSessionReceipt(
                request_id: "auto-baseline-001",
                action: .recordBaseline,
                outcome: .applied,
                sequence: response.session.sequence,
                observed_at_utc: response.receipt.observed_at_utc,
                reason: nil
            )
            let baselineResponse = AutomationSessionCommandResponse(
                session: response.session,
                receipt: baselineReceipt,
                order: nil,
                baseline: dayBaseline
            )
            try AutomationPendingWrite(
                request_id: "auto-baseline-001",
                action: .recordBaseline,
                session_id: response.session.session_id,
                expected_sequence: response.session.sequence,
                baseline_day_utc: dayBaseline.day_utc,
                baseline_source: dayBaseline.source
            ).validate(baselineResponse)
            do {
                try AutomationPendingWrite(
                    request_id: "auto-baseline-001",
                    action: .recordBaseline,
                    session_id: response.session.session_id,
                    expected_sequence: response.session.sequence,
                    baseline_day_utc: "2026-10-15",
                    baseline_source: dayBaseline.source
                ).validate(baselineResponse)
                throw Failure.assertion("baseline readback with mismatched day_utc was accepted")
            } catch is AutomationSessionServiceError {
            }
        }

        var preflightObject = try object(fixture["request_readback"], field: "request_readback")
        preflightObject["state"] = "PENDING"
        preflightObject["reason"] = "command identity is durably admitted"
        let preflight = try JSONDecoder().decode(
            AutomationSessionRequestReadback.self,
            from: JSONSerialization.data(withJSONObject: preflightObject)
        )
        try AutomationSessionMCPService.validate(preflight, requestID: response.receipt.request_id)

        var appliedPendingObject = preflightObject
        appliedPendingObject["state"] = "APPLIED"
        appliedPendingObject["response"] = pendingCommand
        let appliedPending = try JSONDecoder().decode(
            AutomationSessionRequestReadback.self,
            from: JSONSerialization.data(withJSONObject: appliedPendingObject)
        )
        do {
            try AutomationSessionMCPService.validate(appliedPending, requestID: response.receipt.request_id)
            throw Failure.assertion("APPLIED readback with a pending receipt outcome was accepted")
        } catch is AutomationSessionServiceError {
        }
    }

    private static func validateExpandedAutomationEnums() throws {
        let pendingOutcome = try JSONDecoder().decode(AutomationCommandOutcome.self, from: Data("\"PENDING\"".utf8))
        let sendClaimed = try JSONDecoder().decode(AutomationEventKind.self, from: Data("\"ORDER_SEND_CLAIMED\"".utf8))
        let recordBaselineAction = try JSONDecoder().decode(AutomationCommandAction.self, from: Data("\"RECORD_BASELINE\"".utf8))
        let baselineRecorded = try JSONDecoder().decode(AutomationEventKind.self, from: Data("\"BASELINE_RECORDED\"".utf8))
        let baselineBlocked = try JSONDecoder().decode(AutomationEventKind.self, from: Data("\"BASELINE_BLOCKED\"".utf8))
        let baselineVerified = try JSONDecoder().decode(AutomationBaselineVerificationState.self, from: Data("\"VERIFIED\"".utf8))
        let baselineInsufficient = try JSONDecoder().decode(
            AutomationBaselineVerificationState.self,
            from: Data("\"BLOCKED_INSUFFICIENT_EVIDENCE\"".utf8)
        )
        let exchangeDayStart = try JSONDecoder().decode(
            AutomationBaselineSource.self,
            from: Data("\"EXCHANGE_OBSERVED_DAY_START\"".utf8)
        )
        guard pendingOutcome == .pending,
              sendClaimed == .orderSendClaimed,
              recordBaselineAction == .recordBaseline,
              baselineRecorded == .baselineRecorded,
              baselineBlocked == .baselineBlocked,
              baselineVerified == .verified,
              baselineInsufficient == .blockedInsufficientEvidence,
              exchangeDayStart == .exchangeObservedDayStart else {
            throw Failure.assertion("expanded durable effect outcome/event/baseline enums changed")
        }
    }

    private static func validatePaperSessionEvents(at fixtureURL: URL) throws {
        let data = try Data(contentsOf: fixtureURL)
        let page: PaperSessionEventPage
        do {
            page = try JSONDecoder().decode(PaperSessionEventPage.self, from: data)
        } catch let DecodingError.keyNotFound(key, context) {
            let path = (context.codingPath + [key]).map { $0.stringValue }.joined(separator: ".")
            throw Failure.assertion("paper event fixture decode RED: missing key at \(path): \(context.debugDescription)")
        }

        guard page.returned_count == UInt64(page.items.count),
              page.total_count == UInt64(page.items.count),
              page.next_cursor == nil else {
            throw Failure.assertion("paper event page counts or cursor do not match the captured readback")
        }
        guard let filledEvent = page.items.first(where: { !$0.fills.isEmpty }),
              filledEvent.kind == "CANDLE_PROCESSED",
              let fill = filledEvent.fills.first,
              fill.qty == "0.07",
              fill.fee == "7774.76000",
              filledEvent.order_events.first(where: { $0.status == "CREATED" })?.episode_id == nil,
              let mark = filledEvent.account_marks.first,
              mark.equity == "9992225.24000" else {
            throw Failure.assertion("paper event page did not retain the captured simulated fill and accounting mark")
        }
        guard let controlEvent = page.items.first(where: { $0.control?.action == "pause" }),
              controlEvent.kind == "PAUSE",
              controlEvent.market == nil,
              controlEvent.candle == nil,
              controlEvent.source_event_at == nil,
              controlEvent.finalized_at == nil else {
            throw Failure.assertion("control-only paper event must preserve explicit nulls without inventing candle data")
        }

        guard var root = try JSONSerialization.jsonObject(with: data) as? [String: Any],
              var items = root["items"] as? [[String: Any]],
              let fillIndex = items.firstIndex(where: { (($0["fills"] as? [[String: Any]])?.isEmpty == false) }),
              var fills = items[fillIndex]["fills"] as? [[String: Any]],
              !fills.isEmpty else {
            throw Failure.assertion("captured paper event fixture is missing its fill object")
        }
        fills[0].removeValue(forKey: "fee")
        items[fillIndex]["fills"] = fills
        root["items"] = items
        let malformed = try JSONSerialization.data(withJSONObject: root)
        do {
            _ = try JSONDecoder().decode(PaperSessionEventPage.self, from: malformed)
            throw Failure.assertion("paper event decoder accepted a missing required fill fee")
        } catch let DecodingError.keyNotFound(key, context) {
            guard key.stringValue == "fee",
                  context.codingPath.contains(where: { $0.stringValue == "fills" }) else {
                throw Failure.assertion("malformed paper event failed at an unexpected coding path")
            }
        }
    }

    private static func validatePaperReadOnlyReviewEvents() throws {
        let sessionID = "paper-review-fixture"
        let policyReference: [String: Any] = [
            "policy_id": "paper-policy-fixture",
            "revision_id": "paper-revision-fixture",
            "definition_digest": String(repeating: "a", count: 64)
        ]
        let configuration: [String: Any] = [
            "request_id": "paper-request-fixture",
            "policy_ref": policyReference,
            "decision_interval": "M1",
            "execution_resolution": "M1",
            "latency_ms": 150,
            "execution": [
                "kind": "MARKET",
                "participation_cap": "0.1000",
                "offset_bps": NSNull(),
                "ttl_bars": NSNull()
            ],
            "markets": [[
                "market": "KRW-BTC",
                "initial_cash": "100000.0000",
                "costs": [
                    "buy_fee_bps": "0.0000",
                    "sell_fee_bps": "0.0000",
                    "maker_fee_bps": "0.0000",
                    "half_spread_bps": "0.0000",
                    "slippage_bps": "0.0000",
                    "impact_bps": "0.0000",
                    "assumption_label": "fixture",
                    "dynamic": NSNull()
                ],
                "market_rules": [
                    "kind": "SYNTHETIC",
                    "min_notional": "5000.0000",
                    "quantity_step": "0.00000001",
                    "ticks": [["lower_bound": "0", "tick": "0.0001"]],
                    "assumption_label": "fixture"
                ]
            ]]
        ]

        func event(_ sequence: UInt64) -> [String: Any] {
            let event: [String: Any] = [
                "sequence": sequence,
                "kind": "CANDLE_PROCESSED",
                "market": "KRW-BTC",
                "candle": NSNull(),
                "source_event_at": NSNull(),
                "finalized_at": NSNull(),
                "control": NSNull(),
                "provider_recovery": NSNull(),
                "signals": [[
                    "signal_id": "paper-signal-fixture",
                    "strategy": "fixture",
                    "intended_side": NSNull(),
                    "state_before": "CASH",
                    "state_after": "CASH",
                    "raw_target_weight": "0.0000",
                    "constrained_target_weight": "0.0000",
                    "reasons": [],
                    "outcome": "HOLD",
                    "trace": [
                        "matched_rule_id": NSNull(),
                        "state_before": [],
                        "state_after": [],
                        "evidence": NSNull(),
                        "future_optional_trace_field": "additive"
                    ]
                ]],
                "orders": [],
                "order_events": [],
                "fills": [],
                "account_marks": [],
                "episodes": []
            ]
            return event
        }

        func response(
            sessionID: String = sessionID,
            asOfSequence: UInt64,
            afterSequence: UInt64,
            eventSequences: [UInt64],
            totalCount: UInt64,
            nextCursor: UInt64?,
            returnedCount: UInt64? = nil,
            configuration: [String: Any] = configuration
        ) -> [String: Any] {
            let items = eventSequences.map(event)
            let cursor: Any = nextCursor.map { ["sequence": $0] as [String: Any] } ?? NSNull()
            return [
                "schema_version": "1.0",
                "mode": "SIMULATED",
                "configuration": configuration,
                "as_of_sequence": asOfSequence,
                "after_sequence": afterSequence,
                "page": [
                    "session_id": sessionID,
                    "items": items,
                    "returned_count": returnedCount ?? UInt64(items.count),
                    "total_count": totalCount,
                    "next_cursor": cursor
                ]
            ]
        }

        let validFirstPage = response(
            asOfSequence: 2,
            afterSequence: 0,
            eventSequences: [1],
            totalCount: 2,
            nextCursor: 1
        )
        let validSecondPage = response(
            asOfSequence: 2,
            afterSequence: 1,
            eventSequences: [2],
            totalCount: 2,
            nextCursor: nil
        )
        let mismatchedSessionPage = response(
            sessionID: "paper-other-session",
            asOfSequence: 2,
            afterSequence: 0,
            eventSequences: [1],
            totalCount: 2,
            nextCursor: 1
        )
        let mismatchedCutoffPage = response(
            asOfSequence: 3,
            afterSequence: 0,
            eventSequences: [1],
            totalCount: 3,
            nextCursor: 1
        )
        let mismatchedCursorPage = response(
            asOfSequence: 2,
            afterSequence: 0,
            eventSequences: [1],
            totalCount: 2,
            nextCursor: 2
        )
        let mismatchedCountPage = response(
            asOfSequence: 2,
            afterSequence: 0,
            eventSequences: [1],
            totalCount: 1,
            nextCursor: 1
        )
        let mismatchedReturnedCountPage = response(
            asOfSequence: 2,
            afterSequence: 0,
            eventSequences: [1],
            totalCount: 2,
            nextCursor: 1,
            returnedCount: 0
        )
        var invalidPolicyConfiguration = configuration
        invalidPolicyConfiguration["policy_ref"] = [
            "policy_id": "paper-policy-fixture",
            "revision_id": "paper-revision-fixture",
            "definition_digest": "not-a-content-hash"
        ]
        let invalidPolicyPage = response(
            asOfSequence: 2,
            afterSequence: 0,
            eventSequences: [1],
            totalCount: 2,
            nextCursor: 1,
            configuration: invalidPolicyConfiguration
        )
        let retrySuccessPage = response(
            asOfSequence: 5,
            afterSequence: 0,
            eventSequences: [1, 2, 3],
            totalCount: 5,
            nextCursor: 3
        )
        let retryContinuationPage = response(
            asOfSequence: 5,
            afterSequence: 3,
            eventSequences: [4, 5],
            totalCount: 5,
            nextCursor: nil
        )
        let byteBudgetError: [String: Any] = [
            "isError": true,
            "content": [[
                "type": "text",
                "text": "RESOURCE_LIMIT: paper event page exceeds byte budget; request a smaller limit"
            ]]
        ]
        let authorizationError: [String: Any] = [
            "isError": true,
            "content": [["type": "text", "text": "UNAUTHORIZED: review is unavailable"]]
        ]
        let payloads: [[String: Any]] = [
            validFirstPage,
            validSecondPage,
            mismatchedSessionPage,
            mismatchedCutoffPage,
            mismatchedCursorPage,
            mismatchedCountPage,
            mismatchedReturnedCountPage,
            invalidPolicyPage,
            byteBudgetError,
            byteBudgetError,
            byteBudgetError,
            byteBudgetError,
            retrySuccessPage,
            authorizationError,
            byteBudgetError,
            byteBudgetError,
            byteBudgetError,
            byteBudgetError,
            byteBudgetError,
            retryContinuationPage
        ] + Array(repeating: byteBudgetError, count: 7)

        let recorder = PolicyDeletionTestRecorder()
        var responseIndex = 0
        PaperWriteRecoveryURLProtocol.install { request in
            guard let body = PaperWriteRecoveryURLProtocol.requestBody(request),
                  let envelope = try? JSONSerialization.jsonObject(with: body) as? [String: Any],
                  let messageID = envelope["id"],
                  let method = envelope["method"] as? String else {
                recorder.recordUnexpected("invalid JSON-RPC request")
                return nil
            }

            let result: [String: Any]
            if method == "initialize" {
                result = ["protocolVersion": "2025-03-26"]
            } else if method == "tools/call",
                      let params = envelope["params"] as? [String: Any],
                      let tool = params["name"] as? String,
                      let arguments = params["arguments"] as? [String: Any] {
                recorder.record(tool: tool, arguments: arguments)
                guard tool == "paper_session_query",
                      arguments["action"] as? String == "review_events",
                      responseIndex < payloads.count else {
                    recorder.recordUnexpected("\(tool):\(arguments["action"] as? String ?? "missing-action")")
                    result = ["isError": true, "content": []]
                    return Self.paperReviewJSONRPCResponse(id: messageID, result: result)
                }
                let payload = payloads[responseIndex]
                responseIndex += 1
                if payload["isError"] as? Bool == true {
                    result = payload
                    return Self.paperReviewJSONRPCResponse(id: messageID, result: result)
                }
                guard let data = try? JSONSerialization.data(withJSONObject: payload, options: [.sortedKeys]),
                      let text = String(data: data, encoding: .utf8) else {
                    recorder.recordUnexpected("review fixture JSON encoding failed")
                    result = ["isError": true, "content": []]
                    return Self.paperReviewJSONRPCResponse(id: messageID, result: result)
                }
                result = ["isError": false, "content": [["type": "text", "text": text]]]
            } else {
                recorder.recordUnexpected("unexpected JSON-RPC method: \(method)")
                result = ["isError": true, "content": []]
            }
            return Self.paperReviewJSONRPCResponse(id: messageID, result: result)
        }
        defer { PaperWriteRecoveryURLProtocol.clear() }

        let urlConfiguration = URLSessionConfiguration.ephemeral
        urlConfiguration.protocolClasses = [PaperWriteRecoveryURLProtocol.self]
        let urlSession = URLSession(configuration: urlConfiguration)
        defer { urlSession.invalidateAndCancel() }
        let client = NativeMCPClient(session: urlSession)
        guard client.configure(port: 65_432, token: String(repeating: "c", count: 64)) != nil else {
            throw Failure.assertion("paper review mocked transport did not configure")
        }
        let service = PaperSessionMCPService(client: client)

        func query(
            sessionId: String = sessionID,
            asOfSequence: UInt64 = 2,
            afterSequence: UInt64? = nil,
            limit: Int = 1
        ) throws -> Result<PaperSessionReviewResponse, Error> {
            var outcome: Result<PaperSessionReviewResponse, Error>?
            _ = service.reviewEvents(
                sessionId: sessionId,
                asOfSequence: asOfSequence,
                afterSequence: afterSequence,
                limit: limit
            ) { outcome = $0 }
            let deadline = Date().addingTimeInterval(5)
            while outcome == nil, Date() < deadline {
                _ = RunLoop.current.run(mode: .default, before: Date().addingTimeInterval(0.01))
            }
            guard let outcome else {
                throw Failure.assertion("paper review query did not finish its bounded mocked read")
            }
            return outcome
        }

        let first = try query().get()
        guard first.schema_version == "1.0",
              first.mode == .simulated,
              first.as_of_sequence == 2,
              first.after_sequence == 0,
              first.page.session_id == sessionID,
              first.page.next_cursor?.sequence == 1,
              first.configuration.policy_ref.definition_digest == policyReference["definition_digest"] as? String,
              first.configuration.markets.first?.initialCash == "100000.0000",
              first.page.items.first?.signals.first?.outcome == "HOLD" else {
            throw Failure.assertion("paper review did not preserve fixed identity, exact configuration amounts and old signal DTO fields")
        }

        let second = try query(
            asOfSequence: first.as_of_sequence,
            afterSequence: first.page.next_cursor?.sequence
        ).get()
        guard second.as_of_sequence == first.as_of_sequence,
              second.after_sequence == 1,
              second.page.items.map(\.sequence) == [2],
              second.page.next_cursor == nil,
              second.configuration == first.configuration else {
            throw Failure.assertion("paper review continuation did not retain its fixed cutoff, keyset, or immutable configuration")
        }

        for label in [
            "mismatched session identity",
            "mismatched fixed cutoff",
            "mismatched keyset cursor",
            "mismatched prefix event count",
            "mismatched returned event count",
            "invalid frozen policy reference"
        ] {
            guard case .failure = try query(asOfSequence: 2) else {
                throw Failure.assertion("paper review accepted \(label)")
            }
        }
        let invalidQueries: [(String, String, UInt64?, UInt64, Int)] = [
            ("invalid session ID", "../paper", nil, 2, 1),
            ("zero page limit", sessionID, nil, 2, 0),
            ("cursor beyond cutoff", sessionID, 3, 2, 1)
        ]
        for (label, invalidSessionID, invalidAfter, invalidCutoff, invalidLimit) in invalidQueries {
            guard case .failure = try query(
                sessionId: invalidSessionID,
                asOfSequence: invalidCutoff,
                afterSequence: invalidAfter,
                limit: invalidLimit
            ) else {
                throw Failure.assertion("paper review accepted \(label)")
            }
        }

        let retried = try query(asOfSequence: 5, limit: 50).get()
        guard retried.as_of_sequence == 5,
              retried.after_sequence == 0,
              retried.page.items.map(\.sequence) == [1, 2, 3],
              retried.page.returned_count == 3,
              retried.page.total_count == 5,
              retried.page.next_cursor?.sequence == 3 else {
            throw Failure.assertion("reduced paper review page did not preserve fixed prefix, returned count, total, and cursor")
        }
        guard case .failure(let authorizationFailure as NativeMCPError) = try query(asOfSequence: 5, limit: 50),
              case .toolRejected = authorizationFailure else {
            throw Failure.assertion("paper review retried or reclassified a non-budget tool rejection")
        }
        guard case .failure(let singleEventFailure as NativeMCPError) = try query(asOfSequence: 5, limit: 1),
              singleEventFailure.localizedDescription.contains("단일 이벤트") else {
            throw Failure.assertion("single-event paper review budget failure was not surfaced explicitly")
        }
        let retriedContinuation = try query(asOfSequence: 5, afterSequence: 3, limit: 50).get()
        guard retriedContinuation.page.session_id == sessionID,
              retriedContinuation.as_of_sequence == 5,
              retriedContinuation.after_sequence == 3,
              retriedContinuation.page.items.map(\.sequence) == [4, 5],
              retriedContinuation.page.items.allSatisfy({
                  $0.signals.first?.signal_id == "paper-signal-fixture"
              }),
              retriedContinuation.page.returned_count == 2,
              retriedContinuation.page.total_count == 5,
              retriedContinuation.page.next_cursor == nil,
              retriedContinuation.configuration == retried.configuration else {
            throw Failure.assertion("reduced continuation changed its session, signal, cutoff, saved configuration, or cursor")
        }
        guard case .failure(let boundedFailure as NativeMCPError) = try query(asOfSequence: 5, limit: 100),
              boundedFailure.localizedDescription.contains("단일 이벤트") else {
            throw Failure.assertion("paper review did not stop after its finite minimum-size attempt")
        }

        let observed = recorder.snapshot()
        guard observed.calls.count == payloads.count,
              observed.calls.allSatisfy({
                  $0.tool == "paper_session_query" && $0.arguments["action"] as? String == "review_events"
              }),
              observed.unexpectedRequests.isEmpty else {
            throw Failure.assertion("passive paper review used a mutation, poll fallback, or unexpected MCP route")
        }
        try assertJSONEqual(observed.calls[0].arguments, [
            "action": "review_events",
            "session_id": sessionID,
            "after_sequence": NSNull(),
            "as_of_sequence": 2,
            "limit": 1
        ], field: "paper review first page")
        try assertJSONEqual(observed.calls[1].arguments, [
            "action": "review_events",
            "session_id": sessionID,
            "after_sequence": 1,
            "as_of_sequence": 2,
            "limit": 1
        ], field: "paper review fixed-cutoff continuation")
        guard observed.calls[8...12].map({ $0.arguments["limit"] as? Int ?? 0 }) == [50, 25, 12, 6, 3],
              observed.calls[8...12].allSatisfy({
                  $0.tool == "paper_session_query"
                      && $0.arguments["action"] as? String == "review_events"
                      && $0.arguments["session_id"] as? String == sessionID
                      && ($0.arguments["after_sequence"] as? NSNull) != nil
                      && $0.arguments["as_of_sequence"] as? Int == 5
              }),
              observed.calls[13].arguments["limit"] as? Int == 50,
              observed.calls[14].arguments["limit"] as? Int == 1,
              observed.calls[15...19].map({ $0.arguments["limit"] as? Int ?? 0 }) == [50, 25, 12, 6, 3],
              observed.calls[15...19].allSatisfy({
                  $0.tool == "paper_session_query"
                      && $0.arguments["action"] as? String == "review_events"
                      && $0.arguments["session_id"] as? String == sessionID
                      && $0.arguments["after_sequence"] as? Int == 3
                      && $0.arguments["as_of_sequence"] as? Int == 5
              }),
              observed.calls[20...26].map({ $0.arguments["limit"] as? Int ?? 0 }) == [100, 50, 25, 12, 6, 3, 1],
              observed.calls[20...26].allSatisfy({
                  $0.tool == "paper_session_query"
                      && $0.arguments["action"] as? String == "review_events"
                      && $0.arguments["session_id"] as? String == sessionID
                      && ($0.arguments["after_sequence"] as? NSNull) != nil
                      && $0.arguments["as_of_sequence"] as? Int == 5
              }) else {
            throw Failure.assertion("paper review byte-budget retries changed the request identity, cutoff, or bounded limits")
        }
        print("PASS passive paper review fixed-cutoff wire, keyset continuation, boundary validation and query-only route")
    }

    private static func validatePaperReviewRetryCancellationAndTransportFence() throws {
        let sessionID = "paper-review-retry-fence"
        let byteBudgetError: [String: Any] = [
            "isError": true,
            "content": [[
                "type": "text",
                "text": "RESOURCE_LIMIT: paper event page exceeds byte budget; request a smaller limit"
            ]]
        ]
        let recorder = PolicyDeletionTestRecorder()
        let requestObserved = DispatchSemaphore(value: 0)
        var cancellationRequest: NativeMCPRequest?
        let urlConfiguration = URLSessionConfiguration.ephemeral
        urlConfiguration.protocolClasses = [PaperWriteRecoveryURLProtocol.self]
        let urlSession = URLSession(configuration: urlConfiguration)
        defer { urlSession.invalidateAndCancel() }
        let client = NativeMCPClient(session: urlSession)
        guard client.configure(port: 65_432, token: String(repeating: "d", count: 64)) != nil else {
            throw Failure.assertion("paper review retry fence mocked transport did not configure")
        }

        PaperWriteRecoveryURLProtocol.install { request in
            guard let body = PaperWriteRecoveryURLProtocol.requestBody(request),
                  let envelope = try? JSONSerialization.jsonObject(with: body) as? [String: Any],
                  let messageID = envelope["id"],
                  let method = envelope["method"] as? String else {
                recorder.recordUnexpected("invalid JSON-RPC request")
                return nil
            }
            if method == "initialize" {
                return Self.paperReviewJSONRPCResponse(
                    id: messageID,
                    result: ["protocolVersion": "2025-03-26"]
                )
            }
            guard method == "tools/call",
                  let params = envelope["params"] as? [String: Any],
                  let tool = params["name"] as? String,
                  let arguments = params["arguments"] as? [String: Any] else {
                recorder.recordUnexpected("unexpected JSON-RPC method: \(method)")
                return nil
            }
            recorder.record(tool: tool, arguments: arguments)
            DispatchQueue.main.async { cancellationRequest?.cancel() }
            requestObserved.signal()
            return Self.paperReviewJSONRPCResponse(id: messageID, result: byteBudgetError)
        }
        defer { PaperWriteRecoveryURLProtocol.clear() }

        let service = PaperSessionMCPService(client: client)
        let request = service.reviewEvents(
            sessionId: sessionID,
            asOfSequence: 3,
            afterSequence: nil,
            limit: 50
        ) { _ in }
        cancellationRequest = request
        guard requestObserved.wait(timeout: .now() + 5) == .success else {
            throw Failure.assertion("paper review cancellation fixture did not observe its first bounded request")
        }
        let cancelDeadline = Date().addingTimeInterval(5)
        while request.isPending, Date() < cancelDeadline {
            _ = RunLoop.current.run(mode: .default, before: Date().addingTimeInterval(0.01))
        }
        guard !request.isPending, recorder.snapshot().calls.count == 1 else {
            throw Failure.assertion("cancelled paper review continued into a reduced-page retry")
        }

        let transportRecorder = PolicyDeletionTestRecorder()
        PaperWriteRecoveryURLProtocol.install { request in
            guard let body = PaperWriteRecoveryURLProtocol.requestBody(request),
                  let envelope = try? JSONSerialization.jsonObject(with: body) as? [String: Any],
                  let messageID = envelope["id"],
                  let method = envelope["method"] as? String else {
                transportRecorder.recordUnexpected("invalid JSON-RPC request")
                return nil
            }
            if method == "initialize" {
                return Self.paperReviewJSONRPCResponse(
                    id: messageID,
                    result: ["protocolVersion": "2025-03-26"]
                )
            }
            guard method == "tools/call",
                  let params = envelope["params"] as? [String: Any],
                  let tool = params["name"] as? String,
                  let arguments = params["arguments"] as? [String: Any] else {
                transportRecorder.recordUnexpected("unexpected JSON-RPC method: \(method)")
                return nil
            }
            transportRecorder.record(tool: tool, arguments: arguments)
            _ = client.configure(port: 65_433, token: String(repeating: "d", count: 64))
            return Self.paperReviewJSONRPCResponse(id: messageID, result: byteBudgetError)
        }
        let transportOutcome = try awaitPaperReviewResult {
            service.reviewEvents(
                sessionId: sessionID,
                asOfSequence: 3,
                afterSequence: nil,
                limit: 50,
                completion: $0
            )
        }
        guard case .failure(let transportFailure as NativeMCPError) = transportOutcome,
              case .unavailable = transportFailure,
              transportRecorder.snapshot().calls.count == 1 else {
            throw Failure.assertion("paper review retried after its native transport context changed")
        }
        print("PASS paper review cancellation and transport-context retry fences")
    }

    private static func awaitPaperReviewResult(
        _ start: (@escaping (Result<PaperSessionReviewResponse, Error>) -> Void) -> NativeMCPRequest
    ) throws -> Result<PaperSessionReviewResponse, Error> {
        var outcome: Result<PaperSessionReviewResponse, Error>?
        _ = start { outcome = $0 }
        let deadline = Date().addingTimeInterval(5)
        while outcome == nil, Date() < deadline {
            _ = RunLoop.current.run(mode: .default, before: Date().addingTimeInterval(0.01))
        }
        guard let outcome else {
            throw Failure.assertion("paper review retry boundary query did not finish its bounded mocked read")
        }
        return outcome
    }

    private static func paperReviewJSONRPCResponse(id: Any, result: [String: Any]) -> Data? {
        try? JSONSerialization.data(withJSONObject: [
            "jsonrpc": "2.0",
            "id": id,
            "result": result
        ])
    }

    private static func assertJSONEqual(_ actual: [String: Any], _ expected: [String: Any], field: String) throws {
        let actualData = try JSONSerialization.data(withJSONObject: actual, options: [.sortedKeys])
        let expectedData = try JSONSerialization.data(withJSONObject: expected, options: [.sortedKeys])
        let actualObject = try JSONSerialization.jsonObject(with: actualData)
        let expectedObject = try JSONSerialization.jsonObject(with: expectedData)
        guard NSDictionary(dictionary: actualObject as? [String: Any] ?? [:])
            .isEqual(to: expectedObject as? [String: Any] ?? [:]) else {
            throw Failure.assertion("wire request mismatch for \(field)")
        }
    }

    private static func object(_ value: Any?, field: String) throws -> [String: Any] {
        guard let object = value as? [String: Any] else { throw Failure.assertion("missing object \(field)") }
        return object
    }

    private static func string(_ value: Any?, field: String) throws -> String {
        guard let string = value as? String else { throw Failure.assertion("missing string \(field)") }
        return string
    }

    private static func unsigned(_ value: Any?, field: String) throws -> UInt64 {
        guard let number = value as? NSNumber, number.doubleValue >= 0,
              number.doubleValue.rounded(.towardZero) == number.doubleValue else {
            throw Failure.assertion("missing unsigned integer \(field)")
        }
        return number.uint64Value
    }

    private static func XCTUnwrapResponse(_ response: AutomationSessionCommandResponse?) throws -> AutomationSessionCommandResponse {
        guard let response else { throw Failure.assertion("PENDING readback omitted its receipt") }
        return response
    }
}

private enum Failure: Error, CustomStringConvertible {
    case assertion(String)

    var description: String {
        switch self {
        case .assertion(let message): return message
        }
    }
}

private final class PolicyDeletionTestRecorder {
    struct Call {
        let tool: String
        let arguments: [String: Any]
    }

    struct Snapshot {
        let calls: [Call]
        let unexpectedRequests: [String]
    }

    private let lock = NSLock()
    private var calls: [Call] = []
    private var unexpectedRequests: [String] = []

    func record(tool: String, arguments: [String: Any]) {
        lock.lock(); defer { lock.unlock() }
        calls.append(Call(tool: tool, arguments: arguments))
    }

    func recordUnexpected(_ request: String) {
        lock.lock(); defer { lock.unlock() }
        unexpectedRequests.append(request)
    }

    func snapshot() -> Snapshot {
        lock.lock(); defer { lock.unlock() }
        return Snapshot(calls: calls, unexpectedRequests: unexpectedRequests)
    }
}

private final class PaperWriteRecoveryRecorder {
    struct Snapshot {
        let writeRequestIDs: [String]
        let readbackActions: [String]
        let readbackRequestIDs: [String]
        let unexpectedRequests: [String]
    }

    private let lock = NSLock()
    private var writeRequestIDs: [String] = []
    private var readbackActions: [String] = []
    private var readbackRequestIDs: [String] = []
    private var unexpectedRequests: [String] = []

    func recordWrite(requestID: String?) {
        lock.lock(); defer { lock.unlock() }
        writeRequestIDs.append(requestID ?? "missing-request-id")
    }

    func recordReadback(action: String?, requestID: String?) {
        lock.lock(); defer { lock.unlock() }
        readbackActions.append(action ?? "missing-action")
        readbackRequestIDs.append(requestID ?? "missing-request-id")
    }

    func recordUnexpected(_ request: String) {
        lock.lock(); defer { lock.unlock() }
        unexpectedRequests.append(request)
    }

    func snapshot() -> Snapshot {
        lock.lock(); defer { lock.unlock() }
        return Snapshot(
            writeRequestIDs: writeRequestIDs,
            readbackActions: readbackActions,
            readbackRequestIDs: readbackRequestIDs,
            unexpectedRequests: unexpectedRequests
        )
    }
}

private final class PaperWriteRecoveryURLProtocol: URLProtocol {
    private static let lock = NSLock()
    private static var responder: ((URLRequest) -> Data?)?

    static func install(_ response: @escaping (URLRequest) -> Data?) {
        lock.lock(); defer { lock.unlock() }
        responder = response
    }

    static func clear() {
        lock.lock(); defer { lock.unlock() }
        responder = nil
    }

    private static func response(for request: URLRequest) -> Data? {
        lock.lock(); defer { lock.unlock() }
        return responder?(request)
    }

    static func requestBody(_ request: URLRequest) -> Data? {
        if let body = request.httpBody { return body }
        guard let stream = request.httpBodyStream else { return nil }
        stream.open()
        defer { stream.close() }
        var data = Data()
        var buffer = [UInt8](repeating: 0, count: 4_096)
        while true {
            let count = buffer.withUnsafeMutableBufferPointer { bufferPointer -> Int in
                guard let baseAddress = bufferPointer.baseAddress else { return 0 }
                return stream.read(baseAddress, maxLength: bufferPointer.count)
            }
            if count < 0 { return nil }
            if count == 0 { break }
            data.append(contentsOf: buffer.prefix(count))
        }
        return data
    }

    override class func canInit(with request: URLRequest) -> Bool {
        request.url?.scheme == "http"
            && request.url?.host == "127.0.0.1"
            && request.url?.path == "/desktop/mcp"
    }

    override class func canonicalRequest(for request: URLRequest) -> URLRequest {
        request
    }

    override func startLoading() {
        guard let url = request.url,
              let data = Self.response(for: request),
              let response = HTTPURLResponse(
                url: url,
                statusCode: 200,
                httpVersion: "HTTP/1.1",
                headerFields: ["Content-Type": "application/json"]
              ) else {
            client?.urlProtocol(self, didFailWithError: URLError(.badServerResponse))
            return
        }
        client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
        client?.urlProtocol(self, didLoad: data)
        client?.urlProtocolDidFinishLoading(self)
    }

    override func stopLoading() {}
}

private final class TradingControlRaceMockServer {
    struct Snapshot {
        let statusReadCount: Int
        let emergencyRequestIDs: [String]
        let authorizationRequestIDs: [String]
        let expectedSequences: [UInt64]
        let appliedRequestIDs: [String]
        let staleRequestIDs: [String]
        let maximumConcurrentWrites: Int
        let sequence: UInt64
        let state: String
        let exchangeOrderEffects: Int
        let unexpectedToolCalls: [String]
    }

    private let lock = NSLock()
    private let writeBarrier = DispatchGroup()
    private var sequence: UInt64 = 17
    private var state = "DISABLED_BY_CONFIG"
    private var liveTradingEnabled = false
    private var orderAdmissionBlocked = true
    private var emergencyLatched = false
    private var observedAt: String
    private var statusReadCount = 0
    private var emergencyRequestIDs: [String] = []
    private var authorizationRequestIDs: [String] = []
    private var dropAuthorizationResponse = false
    private var blockAuthorizationResponse = false
    private var expectedSequences: [UInt64] = []
    private var appliedRequestIDs: [String] = []
    private var staleRequestIDs: [String] = []
    private var activeWrites = 0
    private var maximumConcurrentWrites = 0
    private var unexpectedToolCalls: [String] = []
    private var exchangeOrderEffects = 0
    private var raceBarrierArrivals = 0
    private var raceBarrierTimedOut = false
    private var idempotency: [String: [String: Any]] = [:]

    init(observedAt: String) {
        self.observedAt = observedAt
        writeBarrier.enter()
        writeBarrier.enter()
    }

    func prepareLiveRace(sequence: UInt64, observedAt: String) {
        lock.lock(); defer { lock.unlock() }
        self.sequence = sequence
        state = "RUNNING"
        liveTradingEnabled = true
        orderAdmissionBlocked = false
        emergencyLatched = false
        self.observedAt = observedAt
    }

    func prepareAuthorization(state: String, sequence: UInt64, observedAt: String, dropResponse: Bool = false, acknowledgeBlocked: Bool = false) {
        lock.lock(); defer { lock.unlock() }
        self.state = state
        self.sequence = sequence
        self.observedAt = observedAt
        liveTradingEnabled = true
        orderAdmissionBlocked = true
        emergencyLatched = false
        dropAuthorizationResponse = dropResponse
        blockAuthorizationResponse = acknowledgeBlocked
    }

    func snapshot() -> Snapshot {
        lock.lock(); defer { lock.unlock() }
        return Snapshot(
            statusReadCount: statusReadCount,
            emergencyRequestIDs: emergencyRequestIDs,
            authorizationRequestIDs: authorizationRequestIDs,
            expectedSequences: expectedSequences,
            appliedRequestIDs: appliedRequestIDs,
            staleRequestIDs: staleRequestIDs,
            maximumConcurrentWrites: maximumConcurrentWrites,
            sequence: sequence,
            state: state,
            exchangeOrderEffects: exchangeOrderEffects,
            unexpectedToolCalls: unexpectedToolCalls + (raceBarrierTimedOut ? ["race barrier timed out"] : [])
        )
    }

    func response(for request: URLRequest) -> Data? {
        guard let body = PaperWriteRecoveryURLProtocol.requestBody(request),
              let envelope = try? JSONSerialization.jsonObject(with: body) as? [String: Any],
              let method = envelope["method"] as? String else { return nil }
        let messageID = envelope["id"] ?? NSNull()
        if method == "initialize" {
            return jsonRPC(id: messageID, result: ["protocolVersion": "2025-03-26"])
        }
        guard method == "tools/call",
              let params = envelope["params"] as? [String: Any],
              let tool = params["name"] as? String,
              let arguments = params["arguments"] as? [String: Any] else {
            return jsonRPC(id: messageID, result: ["isError": true, "content": []])
        }
        guard tool == "trading_control" else {
            if ["upbit_account_balances", "live_order_control"].contains(tool) {
                return jsonRPC(id: messageID, result: ["isError": true, "content": []])
            }
            lock.lock(); unexpectedToolCalls.append(tool); lock.unlock()
            return jsonRPC(id: messageID, result: ["isError": true, "content": []])
        }
        guard let action = arguments["action"] as? String else {
            return jsonRPC(id: messageID, result: ["isError": true, "content": []])
        }
        switch action {
        case "status":
            let status = statusProjection()
            return toolResponse(id: messageID, object: status)
        case "start", "resume":
            guard let requestID = arguments["request_id"] as? String,
                  let expected = (arguments["expected_sequence"] as? NSNumber)?.uint64Value else { return nil }
            lock.lock()
            authorizationRequestIDs.append(requestID)
            guard liveTradingEnabled, !emergencyLatched, expected == sequence,
                  state == (action == "start" ? "STANDBY" : "PAUSED") else {
                unexpectedToolCalls.append("invalid authorization CAS/state")
                lock.unlock()
                return nil
            }
            sequence += 1
            state = "RUNNING"
            orderAdmissionBlocked = blockAuthorizationResponse
            let receipt: [String: Any] = ["request_id": requestID, "action": action,
                "outcome": "ACKNOWLEDGED", "sequence": sequence,
                "observed_at_utc": observedAt, "denied_code": NSNull()]
            let value = snapshotObject(receipt: receipt)
            let drop = dropAuthorizationResponse
            dropAuthorizationResponse = false
            lock.unlock()
            return drop ? nil : toolResponse(id: messageID, object: value)
        case "emergency_stop":
            guard let requestID = arguments["request_id"] as? String,
                  let expected = (arguments["expected_sequence"] as? NSNumber)?.uint64Value else {
                lock.lock(); unexpectedToolCalls.append("malformed emergency_stop"); lock.unlock()
                return jsonRPC(id: messageID, result: ["isError": true, "content": []])
            }
            let output = emergencyStop(requestID: requestID, expectedSequence: expected)
            if output.rejected {
                return jsonRPC(id: messageID, result: ["isError": true, "content": []])
            }
            return toolResponse(id: messageID, object: output.snapshot)
        default:
            lock.lock(); unexpectedToolCalls.append(action); lock.unlock()
            return jsonRPC(id: messageID, result: ["isError": true, "content": []])
        }
    }

    private func statusProjection() -> [String: Any] {
        lock.lock(); defer { lock.unlock() }
        statusReadCount += 1
        return snapshotObject(receipt: nil)
    }

    private func emergencyStop(requestID: String, expectedSequence: UInt64) -> (snapshot: [String: Any], rejected: Bool) {
        lock.lock()
        emergencyRequestIDs.append(requestID)
        expectedSequences.append(expectedSequence)
        activeWrites += 1
        maximumConcurrentWrites = max(maximumConcurrentWrites, activeWrites)
        raceBarrierArrivals += 1
        let joinsRaceBarrier = raceBarrierArrivals <= 2
        lock.unlock()

        if joinsRaceBarrier {
            writeBarrier.leave()
            if writeBarrier.wait(timeout: .now() + 5) == .timedOut {
                lock.lock(); raceBarrierTimedOut = true; lock.unlock()
            }
        }

        lock.lock(); defer {
            activeWrites -= 1
            lock.unlock()
        }
        if let priorReceipt = idempotency[requestID] {
            return (snapshotObject(receipt: priorReceipt), false)
        }
        guard liveTradingEnabled, expectedSequence == sequence else {
            staleRequestIDs.append(requestID)
            return (snapshotObject(receipt: nil), true)
        }
        sequence += 1
        state = "EMERGENCY_STOP_ACTIVE"
        orderAdmissionBlocked = true
        emergencyLatched = true
        let receipt: [String: Any] = [
            "request_id": requestID,
            "action": "emergency_stop",
            "outcome": "ACKNOWLEDGED",
            "sequence": sequence,
            "observed_at_utc": observedAt,
            "denied_code": NSNull()
        ]
        idempotency[requestID] = receipt
        appliedRequestIDs.append(requestID)
        return (snapshotObject(receipt: receipt), false)
    }

    private func snapshotObject(receipt: [String: Any]?) -> [String: Any] {
        [
            "exchange_read_mode": "LIVE_READ",
            "live_trading_enabled": liveTradingEnabled,
            "state": state,
            "sequence": sequence,
            "observed_at_utc": observedAt,
            "order_admission_blocked": orderAdmissionBlocked,
            "emergency_latched": emergencyLatched,
            "denied_code": NSNull(),
            "receipt": receipt ?? NSNull()
        ]
    }

    private func toolResponse(id: Any, object: [String: Any]) -> Data? {
        guard let body = try? JSONSerialization.data(withJSONObject: object),
              let text = String(data: body, encoding: .utf8) else { return nil }
        return jsonRPC(id: id, result: ["isError": false, "content": [["type": "text", "text": text]]])
    }

    private func jsonRPC(id: Any, result: [String: Any]) -> Data? {
        try? JSONSerialization.data(withJSONObject: ["jsonrpc": "2.0", "id": id, "result": result])
    }
}

private final class TradingControlRaceURLProtocol: URLProtocol {
    private static let lock = NSLock()
    private static var server: TradingControlRaceMockServer?

    static func install(_ value: TradingControlRaceMockServer) {
        lock.lock(); defer { lock.unlock() }
        server = value
    }

    static func clear() {
        lock.lock(); defer { lock.unlock() }
        server = nil
    }

    private static func responder() -> TradingControlRaceMockServer? {
        lock.lock(); defer { lock.unlock() }
        return server
    }

    override class func canInit(with request: URLRequest) -> Bool {
        request.url?.scheme == "http"
            && request.url?.host == "127.0.0.1"
            && request.url?.path == "/desktop/mcp"
    }

    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        let loadingRequest = request
        DispatchQueue.global(qos: .userInitiated).async { [self] in
            guard let url = loadingRequest.url,
                  let data = Self.responder()?.response(for: loadingRequest),
                  let response = HTTPURLResponse(
                    url: url,
                    statusCode: 200,
                    httpVersion: "HTTP/1.1",
                    headerFields: ["Content-Type": "application/json"]
                  ) else {
                client?.urlProtocol(self, didFailWithError: URLError(.badServerResponse))
                return
            }
            client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
            client?.urlProtocol(self, didLoad: data)
            client?.urlProtocolDidFinishLoading(self)
        }
    }

    override func stopLoading() {}
}

#if POLICY_DELETION_REGRESSION
private final class PaperRecoveryUIFixtureServer {
    static let sessionID = "paper-session-feed-gap-test-001"

    struct WriteCall {
        let action: String
        let request_id: String
        let session_id: String
        let expected_sequence: UInt64
    }

    struct Snapshot {
        let sessionState: String
        let recoveryCause: String?
        let sequence: UInt64
        let writeCalls: [WriteCall]
        let readbackQueryCount: Int
        let unexpectedCalls: [String]
    }

    private let lock = NSLock()
    private let observedAt: String
    private var session: [String: Any]
    private var writeCalls: [WriteCall] = []
    private var readbackQueryCount = 0
    private var unknownNextResumeResponse = false
    private var unexpectedCalls: [String] = []

    init(observedAt: String) {
        self.observedAt = observedAt
        session = Self.sessionObject(
            observedAt: observedAt,
            state: "RECOVERY_REQUIRED",
            sequence: 7,
            recoveryCause: "FEED_GAP"
        )
    }

    func setSession(state: String, sequence: UInt64, recoveryCause: String?) {
        lock.lock(); defer { lock.unlock() }
        session = Self.sessionObject(
            observedAt: observedAt,
            state: state,
            sequence: sequence,
            recoveryCause: recoveryCause
        )
    }

    func makeNextResumeResponseUnknown() {
        lock.lock(); defer { lock.unlock() }
        unknownNextResumeResponse = true
    }

    func snapshot() -> Snapshot {
        lock.lock(); defer { lock.unlock() }
        return Snapshot(
            sessionState: session["state"] as? String ?? "UNKNOWN",
            recoveryCause: session["recovery_cause"] as? String,
            sequence: (session["sequence"] as? NSNumber)?.uint64Value ?? 0,
            writeCalls: writeCalls,
            readbackQueryCount: readbackQueryCount,
            unexpectedCalls: unexpectedCalls
        )
    }

    func response(for request: URLRequest) -> Data? {
        guard let body = PaperWriteRecoveryURLProtocol.requestBody(request),
              let envelope = try? JSONSerialization.jsonObject(with: body) as? [String: Any],
              let method = envelope["method"] as? String else { return nil }
        let messageID = envelope["id"] ?? NSNull()
        if method == "initialize" {
            return jsonRPC(id: messageID, result: ["protocolVersion": "2025-03-26"])
        }
        guard method == "tools/call",
              let params = envelope["params"] as? [String: Any],
              let tool = params["name"] as? String,
              let arguments = params["arguments"] as? [String: Any] else {
            return jsonRPC(id: messageID, result: ["isError": true, "content": []])
        }
        if tool == "paper_session_query" {
            guard let action = arguments["action"] as? String,
                  let object = query(action: action, arguments: arguments) else {
                return jsonRPC(id: messageID, result: ["isError": true, "content": []])
            }
            return toolResponse(id: messageID, object: object)
        }
        if tool == "paper_session_write" {
            let outcome = write(arguments: arguments)
            guard !outcome.rejected else {
                return jsonRPC(id: messageID, result: ["isError": true, "content": []])
            }
            if outcome.malformedResponse { return Data("malformed response".utf8) }
            return toolResponse(id: messageID, object: outcome.object)
        }
        lock.lock(); unexpectedCalls.append("\(tool)"); lock.unlock()
        return jsonRPC(id: messageID, result: ["isError": true, "content": []])
    }

    private func query(action: String, arguments: [String: Any]) -> [String: Any]? {
        lock.lock(); defer { lock.unlock() }
        switch action {
        case "start_options":
            return Self.startOptionsObject()
        case "request":
            readbackQueryCount += 1
            guard let requestID = arguments["request_id"] as? String else {
                unexpectedCalls.append("malformed request readback")
                return nil
            }
            return [
                "request_id": requestID,
                "found": false,
                "action": NSNull(),
                "session_id": NSNull(),
                "receipt": NSNull(),
                "session": NSNull()
            ]
        case "list":
            return ["items": [session], "returned_count": 1, "total_count": 1, "next_cursor": NSNull()]
        case "events":
            guard arguments["session_id"] as? String == Self.sessionID else {
                unexpectedCalls.append("events session identity")
                return nil
            }
            return [
                "session_id": Self.sessionID,
                "items": [],
                "returned_count": 0,
                "total_count": (session["event_count"] as? NSNumber)?.uint64Value ?? 0,
                "next_cursor": NSNull()
            ]
        default:
            unexpectedCalls.append("paper_session_query:\(action)")
            return nil
        }
    }

    private func write(arguments: [String: Any]) -> (
        object: [String: Any],
        rejected: Bool,
        malformedResponse: Bool
    ) {
        lock.lock(); defer { lock.unlock() }
        guard let action = arguments["action"] as? String,
              let requestID = arguments["request_id"] as? String,
              let sessionID = arguments["session_id"] as? String,
              let expectedSequence = (arguments["expected_sequence"] as? NSNumber)?.uint64Value else {
            unexpectedCalls.append("malformed paper_session_write")
            return (["isError": true], true, false)
        }
        writeCalls.append(WriteCall(
            action: action,
            request_id: requestID,
            session_id: sessionID,
            expected_sequence: expectedSequence
        ))
        let state = session["state"] as? String ?? "UNKNOWN"
        let cause = session["recovery_cause"] as? String
        let sequence = (session["sequence"] as? NSNumber)?.uint64Value ?? 0
        let recoveryAllowed = state == "RECOVERY_REQUIRED" && cause == "FEED_GAP"
        guard action == "resume",
              sessionID == Self.sessionID,
              expectedSequence == sequence,
              state == "PAUSED" || recoveryAllowed else {
            unexpectedCalls.append("rejected paper_session_write eligibility/sequence")
            return (["isError": true], true, false)
        }
        if unknownNextResumeResponse {
            unknownNextResumeResponse = false
            return ([:], false, true)
        }
        let nextSequence = sequence + 1
        session = Self.sessionObject(
            observedAt: observedAt,
            state: "RUNNING",
            sequence: nextSequence,
            recoveryCause: nil
        )
        var responseSession = session
        responseSession["event_count"] = 2
        let response: [String: Any] = [
            "receipt": [
                "request_id": requestID,
                "session_id": sessionID,
                "action": "resume",
                "outcome": "APPLIED",
                "expected_sequence": expectedSequence,
                "sequence": nextSequence,
                "created_at": observedAt,
                "cancelled_pending_orders": []
            ],
            "session": responseSession
        ]
        session["event_count"] = 2
        return (response, false, false)
    }

    private static func sessionObject(
        observedAt: String,
        state: String,
        sequence: UInt64,
        recoveryCause: String?
    ) -> [String: Any] {
        var object: [String: Any] = [
            "session_id": sessionID,
            "origin_request_id": "paper-start-fixture-001",
            "state": state,
            "sequence": sequence,
            "policy_ref": [
                "policy_id": "policy-fixture",
                "revision_id": "revision-fixture",
                "definition_digest": String(repeating: "a", count: 64)
            ],
            "decision_interval": "m1",
            "execution_resolution": "m1",
            "mode": "MOCK",
            "created_at": observedAt,
            "started_at": observedAt,
            "paused_at": state == "PAUSED" ? observedAt as Any : NSNull(),
            "stopped_at": NSNull(),
            "last_evaluated_at": observedAt,
            "event_count": 1,
            "markets": []
        ]
        if let recoveryCause { object["recovery_cause"] = recoveryCause }
        return object
    }

    private static func startOptionsObject() -> [String: Any] {
        [
            "policy_options": [],
            "scenario_presets": [[
                "preset_id": "synthetic-paper-ui",
                "name": "Synthetic UI fixture",
                "initial_cash": "100000",
                "costs": [
                    "buy_fee_bps": "0",
                    "sell_fee_bps": "0",
                    "maker_fee_bps": "0",
                    "half_spread_bps": "0",
                    "slippage_bps": "0",
                    "impact_bps": "0",
                    "assumption_label": "SYNTHETIC_TEST_ONLY",
                    "dynamic": NSNull()
                ],
                "market_rules": [
                    "kind": "SIMULATED_SCENARIO",
                    "min_notional": "1",
                    "quantity_step": "0.0001",
                    "ticks": [["lower_bound": "0", "tick": "0.01"]],
                    "assumption_label": "SYNTHETIC_TEST_ONLY"
                ],
                "execution": ["kind": "NEXT_BAR_OPEN", "participation_cap": "1"],
                "latency_ms": 0,
                "editable": true,
                "source_note": "SYNTHETIC_TEST_ONLY"
            ]],
            "decision_intervals": ["m1"],
            "execution_resolutions": ["m1"],
            "limits": [
                "active_sessions": 1,
                "markets_per_session": 3,
                "recovery_bars_per_market_interval": 128
            ]
        ]
    }

    private func toolResponse(id: Any, object: [String: Any]) -> Data? {
        guard let body = try? JSONSerialization.data(withJSONObject: object),
              let text = String(data: body, encoding: .utf8) else { return nil }
        return jsonRPC(id: id, result: ["isError": false, "content": [["type": "text", "text": text]]])
    }

    private func jsonRPC(id: Any, result: [String: Any]) -> Data? {
        try? JSONSerialization.data(withJSONObject: ["jsonrpc": "2.0", "id": id, "result": result])
    }
}
#endif
