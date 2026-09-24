"""补样计划与补样轮次状态流的确定性测试。"""

from __future__ import annotations

import json
import sqlite3
import unittest
from datetime import datetime, timezone
from pathlib import Path

from robot_trials.clock import FrozenClock
from robot_trials.errors import Conflict, Forbidden, InvalidState, ValidationFailed
from robot_trials.jsonio import load_json
from robot_trials.service import TrialService


ROOT = Path(__file__).resolve().parents[1]


def build_service() -> tuple[TrialService, FrozenClock]:
    connection = sqlite3.connect(":memory:", isolation_level=None)
    connection.row_factory = sqlite3.Row
    clock = FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc))
    service = TrialService(connection, clock)
    for user_id, role in (
        ("operator", "operator"),
        ("stat", "statistician"),
        ("approver", "approver"),
        ("auditor", "auditor"),
    ):
        service.create_user(user_id, user_id, role)
    protocol = load_json(ROOT / "fixtures" / "demo_protocol.json")
    service.register_robot("operator", "robot-a", "A 型", "厂商")
    service.register_build("operator", "build-a", "robot-a", "1.0", "b" * 64)
    service.publish_protocol("stat", protocol)
    service.create_batch("operator", "batch-a", "demo-delivery-v1", 1, "build-a")
    return service, clock


def load_rows() -> list[dict]:
    return [
        json.loads(line)
        for line in (ROOT / "fixtures" / "demo_observations.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def run_analysis(service: TrialService) -> dict:
    job = service.claim_job("worker", 600)
    assert job is not None
    return service.complete_job("worker", job["job_id"], "stat")


def row(rows: list[dict], source_row: str) -> dict:
    return next(item for item in rows if item["source_row"] == source_row)


class SupplementTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service, self.clock = build_service()
        self.rows = load_rows()

    def _first_round_deficient(self) -> dict:
        """首采 5 条：cross-traffic 只有 2 条（004、005），缺 1 条。"""
        service = self.service
        service.start_batch("operator", "batch-a", 1)
        service.import_observations(
            "operator", "batch-a", "key-1",
            [row(self.rows, key) for key in ("001", "002", "003", "004", "005")],
        )
        service.seal_batch("stat", "batch-a", 2)
        analysis = run_analysis(service)
        self.assertEqual(analysis["result"]["conclusion"], "insufficient")
        decision = service.decide(
            "approver", "batch-a", analysis["analysis_id"], "needs_more_data", "缺分层"
        )
        return analysis, decision

    def test_full_supplement_loop_creates_new_revision_snapshot_and_analysis(self) -> None:
        service = self.service
        first_analysis, first_decision = self._first_round_deficient()
        self.clock.advance(minutes=5)
        plan = service.issue_supplement_plan(
            "approver", "batch-a", first_decision["decision_id"],
            [{"stratum_key": "cross-traffic", "minimum_count": 1}],
        )
        self.assertEqual(plan["round_seq"], 1)
        opened = service.open_supplement_round("operator", "batch-a", plan["plan_id"], 3)
        self.assertEqual(opened["state"], "running")
        self.assertEqual(opened["revision"], 4)
        self.assertEqual(opened["current_round_seq"], 1)
        imported = service.import_observations(
            "operator", "batch-a", "key-2", [row(self.rows, "006")]
        )
        self.assertEqual(imported["round_seq"], 1)
        service.seal_batch("stat", "batch-a", 4)
        second = run_analysis(service)
        self.assertEqual(second["round_seq"], 1)
        self.assertNotEqual(first_analysis["input_sha256"], second["input_sha256"])
        self.assertEqual(second["result"]["conclusion"], "pass")
        final = service.decide("approver", "batch-a", second["analysis_id"], "approved", "补齐通过")
        self.assertEqual(final["round_seq"], 1)
        report = service.report("auditor", "batch-a")
        rounds = report["rounds"]
        self.assertEqual([item["round_seq"] for item in rounds], [0, 1])
        self.assertEqual(rounds[0]["sample_increment_total"], 5)
        self.assertEqual(rounds[1]["sample_increment_total"], 1)
        self.assertEqual(
            rounds[1]["sample_increments"], {"cross-traffic": 1}
        )
        self.assertEqual(rounds[0]["decision"]["decision"], "needs_more_data")
        self.assertEqual(rounds[1]["decision"]["decision"], "approved")
        self.assertEqual(rounds[1]["plan"]["items"],
                         [{"stratum_key": "cross-traffic", "minimum_count": 1}])
        event_types = [event["event_type"] for event in report["events"]]
        self.assertEqual(event_types, [
            "batch.created", "batch.started", "observations.imported", "batch.sealed",
            "analysis.completed", "decision.recorded", "supplement.plan_issued",
            "supplement.round_opened", "observations.imported", "supplement.sealed",
            "analysis.completed", "decision.recorded",
        ])

    def test_plan_requires_needs_more_data_decision(self) -> None:
        service = self.service
        service.start_batch("operator", "batch-a", 1)
        service.import_observations("operator", "batch-a", "key-1", self.rows)
        service.seal_batch("stat", "batch-a", 2)
        analysis = run_analysis(service)
        approved = service.decide("approver", "batch-a", analysis["analysis_id"], "approved", "ok")
        with self.assertRaises(InvalidState):
            service.issue_supplement_plan(
                "approver", "batch-a", approved["decision_id"],
                [{"stratum_key": "cross-traffic", "minimum_count": 1}],
            )

    def test_only_approver_can_plan_and_only_operator_can_open(self) -> None:
        _, decision = self._first_round_deficient()
        service = self.service
        with self.assertRaises(Forbidden):
            service.issue_supplement_plan(
                "operator", "batch-a", decision["decision_id"],
                [{"stratum_key": "cross-traffic", "minimum_count": 1}],
            )
        plan = service.issue_supplement_plan(
            "approver", "batch-a", decision["decision_id"],
            [{"stratum_key": "cross-traffic", "minimum_count": 1}],
        )
        with self.assertRaises(Forbidden):
            service.open_supplement_round("approver", "batch-a", plan["plan_id"], 3)

    def test_plan_items_must_reference_declared_strata_and_positive_counts(self) -> None:
        _, decision = self._first_round_deficient()
        service = self.service
        with self.assertRaises(ValidationFailed):
            service.issue_supplement_plan(
                "approver", "batch-a", decision["decision_id"],
                [{"stratum_key": "unknown-stratum", "minimum_count": 1}],
            )
        with self.assertRaises(ValidationFailed):
            service.issue_supplement_plan(
                "approver", "batch-a", decision["decision_id"],
                [{"stratum_key": "cross-traffic", "minimum_count": 0}],
            )
        with self.assertRaises(ValidationFailed):
            service.issue_supplement_plan("approver", "batch-a", decision["decision_id"], [])

    def test_duplicate_plan_issuance_is_blocked(self) -> None:
        _, decision = self._first_round_deficient()
        service = self.service
        items = [{"stratum_key": "cross-traffic", "minimum_count": 1}]
        service.issue_supplement_plan("approver", "batch-a", decision["decision_id"], items)
        with self.assertRaises(Conflict):
            service.issue_supplement_plan("approver", "batch-a", decision["decision_id"], items)

    def test_duplicate_round_open_is_blocked(self) -> None:
        _, decision = self._first_round_deficient()
        service = self.service
        plan = service.issue_supplement_plan(
            "approver", "batch-a", decision["decision_id"],
            [{"stratum_key": "cross-traffic", "minimum_count": 1}],
        )
        service.open_supplement_round("operator", "batch-a", plan["plan_id"], 3)
        with self.assertRaises(Conflict):
            service.open_supplement_round("operator", "batch-a", plan["plan_id"], 4)

    def test_open_round_rejects_stale_revision(self) -> None:
        _, decision = self._first_round_deficient()
        service = self.service
        plan = service.issue_supplement_plan(
            "approver", "batch-a", decision["decision_id"],
            [{"stratum_key": "cross-traffic", "minimum_count": 1}],
        )
        with self.assertRaises(InvalidState):
            service.open_supplement_round("operator", "batch-a", plan["plan_id"], 2)

    def test_import_rejects_stratum_outside_plan(self) -> None:
        _, decision = self._first_round_deficient()
        service = self.service
        plan = service.issue_supplement_plan(
            "approver", "batch-a", decision["decision_id"],
            [{"stratum_key": "cross-traffic", "minimum_count": 1}],
        )
        service.open_supplement_round("operator", "batch-a", plan["plan_id"], 3)
        # 计划只要求 cross-traffic，clear-aisle 的追加观测必须被拒绝。
        extra = dict(row(self.rows, "001"))
        extra["source_row"] = "999"
        with self.assertRaises(ValidationFailed):
            service.import_observations("operator", "batch-a", "key-x", [extra])

    def test_import_cannot_exceed_plan_minimum(self) -> None:
        _, decision = self._first_round_deficient()
        service = self.service
        plan = service.issue_supplement_plan(
            "approver", "batch-a", decision["decision_id"],
            [{"stratum_key": "cross-traffic", "minimum_count": 1}],
        )
        service.open_supplement_round("operator", "batch-a", plan["plan_id"], 3)
        first_extra = dict(row(self.rows, "006"))
        second_extra = dict(row(self.rows, "004"))
        second_extra["source_row"] = "998"
        service.import_observations("operator", "batch-a", "key-a", [first_extra])
        with self.assertRaises(InvalidState):
            service.import_observations("operator", "batch-a", "key-b", [second_extra])

    def test_seal_blocked_until_plan_minimum_met(self) -> None:
        _, decision = self._first_round_deficient()
        service = self.service
        plan = service.issue_supplement_plan(
            "approver", "batch-a", decision["decision_id"],
            [{"stratum_key": "cross-traffic", "minimum_count": 2}],
        )
        service.open_supplement_round("operator", "batch-a", plan["plan_id"], 3)
        service.import_observations("operator", "batch-a", "key-a", [row(self.rows, "006")])
        with self.assertRaises(InvalidState):
            service.seal_batch("stat", "batch-a", 4)

    def test_import_blocked_after_round_sealed(self) -> None:
        _, decision = self._first_round_deficient()
        service = self.service
        plan = service.issue_supplement_plan(
            "approver", "batch-a", decision["decision_id"],
            [{"stratum_key": "cross-traffic", "minimum_count": 1}],
        )
        service.open_supplement_round("operator", "batch-a", plan["plan_id"], 3)
        service.import_observations("operator", "batch-a", "key-a", [row(self.rows, "006")])
        service.seal_batch("stat", "batch-a", 4)
        late = dict(row(self.rows, "004"))
        late["source_row"] = "997"
        with self.assertRaises(InvalidState):
            service.import_observations("operator", "batch-a", "key-late", [late])

    def test_decision_on_stale_analysis_is_blocked(self) -> None:
        first_analysis, first_decision = self._first_round_deficient()
        service = self.service
        plan = service.issue_supplement_plan(
            "approver", "batch-a", first_decision["decision_id"],
            [{"stratum_key": "cross-traffic", "minimum_count": 1}],
        )
        service.open_supplement_round("operator", "batch-a", plan["plan_id"], 3)
        service.import_observations("operator", "batch-a", "key-a", [row(self.rows, "006")])
        service.seal_batch("stat", "batch-a", 4)
        second = run_analysis(service)
        # 批次当前修订对应的是第二轮分析，回头对旧分析版本做审批必须失败。
        with self.assertRaises(InvalidState):
            service.decide("approver", "batch-a", first_analysis["analysis_id"], "rejected", "陈旧审批")
        service.decide("approver", "batch-a", second["analysis_id"], "approved", "ok")
        # 终态之后同一计划的轮次早已开启，重复开启同样必须被阻止。
        with self.assertRaises(Conflict):
            service.open_supplement_round("operator", "batch-a", plan["plan_id"], 5)

    def test_historical_analysis_decision_and_samples_remain_traceable(self) -> None:
        first_analysis, _ = self._first_round_deficient()
        service = self.service
        # 走完两轮补样后，旧行、旧分析、旧决定都必须原样保留。
        decision_rows = service.connection.execute(
            "SELECT decision FROM decisions WHERE batch_id='batch-a' ORDER BY decision_id"
        ).fetchall()
        self.assertEqual([row[0] for row in decision_rows], ["needs_more_data"])
        second_plan = service.issue_supplement_plan(
            "approver", "batch-a",
            service.connection.execute(
                "SELECT decision_id FROM decisions WHERE analysis_id=?",
                (first_analysis["analysis_id"],),
            ).fetchone()[0],
            [{"stratum_key": "cross-traffic", "minimum_count": 1}],
        )
        service.open_supplement_round("operator", "batch-a", second_plan["plan_id"], 3)
        service.import_observations("operator", "batch-a", "key-a", [row(self.rows, "006")])
        service.seal_batch("stat", "batch-a", 4)
        second = run_analysis(service)
        service.decide("approver", "batch-a", second["analysis_id"], "approved", "ok")
        round_zero = service.connection.execute(
            "SELECT count(*) FROM observations WHERE batch_id='batch-a' AND round_seq=0"
        ).fetchone()[0]
        round_one = service.connection.execute(
            "SELECT count(*) FROM observations WHERE batch_id='batch-a' AND round_seq=1"
        ).fetchone()[0]
        self.assertEqual((round_zero, round_one), (5, 1))
        stored = service.connection.execute(
            "SELECT input_sha256 FROM analyses WHERE analysis_id=?",
            (first_analysis["analysis_id"],),
        ).fetchone()[0]
        self.assertEqual(stored, first_analysis["input_sha256"])
        decisions = service.connection.execute(
            "SELECT decision FROM decisions WHERE batch_id='batch-a' ORDER BY decision_id"
        ).fetchall()
        self.assertEqual([row[0] for row in decisions], ["needs_more_data", "approved"])

    def test_two_supplement_cycles_chain_with_continuous_round_numbers(self) -> None:
        service = self.service
        # 首采：clear-aisle 3 条，cross-traffic 仅 1 条（缺 2 条）。
        service.start_batch("operator", "batch-a", 1)
        service.import_observations(
            "operator", "batch-a", "key-1",
            [row(self.rows, key) for key in ("001", "002", "003", "004")],
        )
        service.seal_batch("stat", "batch-a", 2)
        first = run_analysis(service)
        d1 = service.decide("approver", "batch-a", first["analysis_id"], "needs_more_data", "缺 2")
        # 第一轮补样只补 1 条，仍然不足。
        p1 = service.issue_supplement_plan(
            "approver", "batch-a", d1["decision_id"],
            [{"stratum_key": "cross-traffic", "minimum_count": 1}],
        )
        service.open_supplement_round("operator", "batch-a", p1["plan_id"], 3)
        service.import_observations("operator", "batch-a", "key-2", [row(self.rows, "005")])
        service.seal_batch("stat", "batch-a", 4)
        second = run_analysis(service)
        self.assertEqual(second["result"]["conclusion"], "insufficient")
        d2 = service.decide("approver", "batch-a", second["analysis_id"], "needs_more_data", "仍缺 1")
        # 第二轮补样补齐最后 1 条。
        p2 = service.issue_supplement_plan(
            "approver", "batch-a", d2["decision_id"],
            [{"stratum_key": "cross-traffic", "minimum_count": 1}],
        )
        self.assertEqual(p2["round_seq"], 2)
        service.open_supplement_round("operator", "batch-a", p2["plan_id"], 5)
        service.import_observations("operator", "batch-a", "key-3", [row(self.rows, "006")])
        service.seal_batch("stat", "batch-a", 6)
        third = run_analysis(service)
        self.assertEqual(third["round_seq"], 2)
        self.assertEqual(third["result"]["conclusion"], "pass")
        service.decide("approver", "batch-a", third["analysis_id"], "approved", "两轮补齐")
        report = service.report("auditor", "batch-a")
        self.assertEqual([item["round_seq"] for item in report["rounds"]], [0, 1, 2])
        self.assertEqual(
            [item["sample_increment_total"] for item in report["rounds"]], [4, 1, 1]
        )

    def test_exclusion_cannot_touch_historical_round_observations(self) -> None:
        _, decision = self._first_round_deficient()
        service = self.service
        plan = service.issue_supplement_plan(
            "approver", "batch-a", decision["decision_id"],
            [{"stratum_key": "cross-traffic", "minimum_count": 1}],
        )
        service.open_supplement_round("operator", "batch-a", plan["plan_id"], 3)
        historical_id = service.connection.execute(
            "SELECT observation_id FROM observations WHERE round_seq=0 LIMIT 1"
        ).fetchone()[0]
        with self.assertRaises(InvalidState):
            service.request_exclusion("operator", historical_id, "试图改动旧证据")

    def test_closed_loop_is_deterministic_with_injected_clock(self) -> None:
        def closed_loop() -> dict:
            service, clock = build_service()
            rows = load_rows()
            service.start_batch("operator", "batch-a", 1)
            service.import_observations(
                "operator", "batch-a", "key-1",
                [row(rows, key) for key in ("001", "002", "003", "004", "005")],
            )
            service.seal_batch("stat", "batch-a", 2)
            first = run_analysis(service)
            decision = service.decide(
                "approver", "batch-a", first["analysis_id"], "needs_more_data", "缺"
            )
            clock.advance(minutes=10)
            plan = service.issue_supplement_plan(
                "approver", "batch-a", decision["decision_id"],
                [{"stratum_key": "cross-traffic", "minimum_count": 1}],
            )
            service.open_supplement_round("operator", "batch-a", plan["plan_id"], 3)
            service.import_observations("operator", "batch-a", "key-2", [row(rows, "006")])
            service.seal_batch("stat", "batch-a", 4)
            second = run_analysis(service)
            service.decide("approver", "batch-a", second["analysis_id"], "approved", "ok")
            return {
                "first_input": first["input_sha256"],
                "second_input": second["input_sha256"],
                "first_result": first["result"],
                "second_result": second["result"],
            }

        self.assertEqual(closed_loop(), closed_loop())


if __name__ == "__main__":
    unittest.main()
