from __future__ import annotations

import json
import sqlite3
import unittest
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

from robot_trials.clock import FrozenClock
from robot_trials.errors import Conflict, Forbidden, InvalidState, NotFound, ValidationFailed
from robot_trials.jsonio import load_json
from robot_trials.service import TrialService


ROOT = Path(__file__).resolve().parents[1]


def make_row(source_row: str, stratum_key: str, *, robot: str = "robot-a", **metrics) -> dict:
    return {
        "source_batch": "hall-a-20260924",
        "source_row": source_row,
        "robot_id": robot,
        "protocol_id": "demo-delivery-v1",
        "protocol_version": 1,
        "stratum_key": stratum_key,
        "observed_at": "2026-09-24T10:00:00+08:00",
        "metrics": {
            "completed": metrics.get("completed", 1),
            "completion_seconds": str(metrics.get("completion_seconds", 45.0)),
            "interventions": metrics.get("interventions", 0),
        },
        "excluded_reason": None,
    }


CLEAR_ROWS = [make_row(f"c-{i}", "clear-aisle", completion_seconds=42 + i) for i in range(3)]
TRAFFIC_ROWS = [
    make_row("t-1", "cross-traffic", completed=1, completion_seconds=58.4, interventions=1),
    make_row("t-2", "cross-traffic", completed=1, completion_seconds=60.1, interventions=1),
    make_row("t-3", "cross-traffic", completed=1, completion_seconds=62.0, interventions=1),
]


class ResamplingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.clock = FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc))
        self.service = TrialService(self.connection, self.clock)
        for user_id, role in (
            ("operator", "operator"),
            ("stat", "statistician"),
            ("approver", "approver"),
            ("auditor", "auditor"),
        ):
            self.service.create_user(user_id, user_id, role)
        protocol = load_json(ROOT / "fixtures" / "demo_protocol.json")
        self.service.register_robot("operator", "robot-a", "A 型", "厂商")
        self.service.register_build("operator", "build-a", "robot-a", "1.0", "b" * 64)
        self.service.publish_protocol("stat", protocol)
        self.service.create_batch("operator", "batch-a", "demo-delivery-v1", 1, "build-a")
        self.service.start_batch("operator", "batch-a", 1)

    def tearDown(self) -> None:
        self.connection.close()

    def _first_cycle_needs_more_data(self) -> tuple[dict, dict]:
        """首批只导入 clear-aisle，封存分析后形成 needs_more_data 决定。"""

        self.service.import_observations("operator", "batch-a", "key-initial", CLEAR_ROWS)
        self.service.seal_batch("stat", "batch-a", 2)
        job = self.service.claim_job("worker", 60)
        analysis = self.service.complete_job("worker", job["job_id"], "stat")
        self.assertEqual(analysis["result"]["conclusion"], "insufficient")
        decision = self.service.decide(
            "approver", "batch-a", analysis["analysis_id"], "needs_more_data", "缺少横向人流分层"
        )
        decision_id = self.connection.execute(
            "SELECT decision_id FROM decisions WHERE analysis_id=?", (analysis["analysis_id"],)
        ).fetchone()["decision_id"]
        return analysis, {"decision_id": decision_id}

    def test_full_resampling_loop_is_deterministic_and_traceable(self) -> None:
        first_analysis, decision = self._first_cycle_needs_more_data()
        self.clock.advance(hours=1)
        plan = self.service.issue_supplement_plan(
            "approver", "batch-a", decision["decision_id"],
            [{"stratum_key": "cross-traffic", "minimum_count": 3}],
            note="补齐横向人流分层",
        )
        self.assertEqual(self.service.get_batch("batch-a")["state"], "resampling")
        self.clock.advance(hours=1)
        opened = self.service.open_resampling_round("operator", "batch-a", plan["plan_id"])
        self.assertEqual(opened["round_number"], 1)
        self.clock.advance(hours=1)
        imported = self.service.import_observations("operator", "batch-a", "key-round-1", TRAFFIC_ROWS)
        self.assertEqual(imported["round_id"], opened["round_id"])
        sealed = self.service.seal_resampling_round("stat", "batch-a", opened["round_id"])
        self.assertEqual(sealed["state"], "sealed")
        self.assertEqual(sealed["revision"], 4)
        job = self.service.claim_job("worker", 60)
        second_analysis = self.service.complete_job("worker", job["job_id"], "stat")
        self.assertNotEqual(second_analysis["input_sha256"], first_analysis["input_sha256"])
        self.assertEqual(second_analysis["result"]["conclusion"], "pass")
        # 相同快照 + 固定种子 => 确定性结果：重跑领取到的同一修订任务会复用已存分析。
        stored_result = json.loads(self.connection.execute(
            "SELECT result_json FROM analyses WHERE analysis_id=?",
            (second_analysis["analysis_id"],),
        ).fetchone()["result_json"])
        self.assertEqual(second_analysis["result"], stored_result)
        final = self.service.decide(
            "approver", "batch-a", second_analysis["analysis_id"], "approved", "补样后满足规则"
        )
        self.assertEqual(final["decision"], "approved")

        report = self.service.report("auditor", "batch-a")
        revisions = report["revisions"]
        self.assertEqual([item["batch_revision"] for item in revisions], [3, 4])
        self.assertEqual(revisions[0]["decision"]["decision"], "needs_more_data")
        self.assertEqual(revisions[0]["analysis_id"], first_analysis["analysis_id"])
        self.assertEqual(revisions[1]["decision"]["decision"], "approved")
        self.assertEqual(len(report["rounds"]), 1)
        round_info = report["rounds"][0]
        self.assertEqual(round_info["increments"], [
            {"stratum_key": "cross-traffic", "count": 3, "included": 3}
        ])
        self.assertEqual(round_info["plan_items"], [
            {"stratum_key": "cross-traffic", "minimum_count": 3}
        ])
        self.assertEqual(round_info["analysis"]["analysis_id"], second_analysis["analysis_id"])
        self.assertEqual(round_info["analysis"]["decision"]["decision"], "approved")
        self.assertEqual(round_info["sealed_revision"], 4)
        # 当时样本永久保留：首批 3 条与补样 3 条都在，且归属各自轮次。
        counts = {
            row[0]: row[1]
            for row in self.connection.execute(
                "SELECT COALESCE(round_id, 0), count(*) FROM observations GROUP BY COALESCE(round_id, 0)"
            ).fetchall()
        }
        self.assertEqual(counts, {0: 3, opened["round_id"]: 3})
        event_types = [event["event_type"] for event in report["events"]]
        self.assertIn("supplement_plan.issued", event_types)
        self.assertIn("resampling_round.opened", event_types)
        self.assertIn("resampling_round.sealed", event_types)
        # 时钟确定性：三个阶段时间戳依次推进一小时。
        self.assertEqual(round_info["opened_at"], "2026-09-24T10:00:00Z")
        self.assertEqual(round_info["sealed_at"], "2026-09-24T11:00:00Z")

    def test_duplicate_round_open_for_same_plan_is_blocked(self) -> None:
        _, decision = self._first_cycle_needs_more_data()
        plan = self.service.issue_supplement_plan(
            "approver", "batch-a", decision["decision_id"],
            [{"stratum_key": "cross-traffic", "minimum_count": 3}],
        )
        self.service.open_resampling_round("operator", "batch-a", plan["plan_id"])
        with self.assertRaises(Conflict):
            self.service.open_resampling_round("operator", "batch-a", plan["plan_id"])

    def test_over_quota_submission_is_blocked(self) -> None:
        _, decision = self._first_cycle_needs_more_data()
        plan = self.service.issue_supplement_plan(
            "approver", "batch-a", decision["decision_id"],
            [{"stratum_key": "cross-traffic", "minimum_count": 2}],
        )
        opened = self.service.open_resampling_round("operator", "batch-a", plan["plan_id"])
        self.service.import_observations("operator", "batch-a", "k1", TRAFFIC_ROWS[:2])
        with self.assertRaises(InvalidState):
            self.service.import_observations("operator", "batch-a", "k2", TRAFFIC_ROWS[2:])
        # 被阻止的整批导入不留任何数据。
        total = self.connection.execute(
            "SELECT count(*) FROM observations WHERE round_id=?", (opened["round_id"],)
        ).fetchone()[0]
        self.assertEqual(total, 2)
        # 超出计划声明范围的分层同样被阻止。
        with self.assertRaises(ValidationFailed):
            self.service.import_observations("operator", "batch-a", "k3", CLEAR_ROWS[:1])

    def test_import_after_round_sealed_is_blocked(self) -> None:
        _, decision = self._first_cycle_needs_more_data()
        plan = self.service.issue_supplement_plan(
            "approver", "batch-a", decision["decision_id"],
            [{"stratum_key": "cross-traffic", "minimum_count": 3}],
        )
        opened = self.service.open_resampling_round("operator", "batch-a", plan["plan_id"])
        self.service.import_observations("operator", "batch-a", "k1", TRAFFIC_ROWS)
        self.service.seal_resampling_round("stat", "batch-a", opened["round_id"])
        extra = [make_row("t-9", "cross-traffic")]
        with self.assertRaises(InvalidState):
            self.service.import_observations("operator", "batch-a", "k2", extra)
        with self.assertRaises(InvalidState):
            self.service.seal_resampling_round("stat", "batch-a", opened["round_id"])

    def test_stale_revision_decision_is_blocked(self) -> None:
        first_analysis, decision = self._first_cycle_needs_more_data()
        plan = self.service.issue_supplement_plan(
            "approver", "batch-a", decision["decision_id"],
            [{"stratum_key": "cross-traffic", "minimum_count": 3}],
        )
        opened = self.service.open_resampling_round("operator", "batch-a", plan["plan_id"])
        self.service.import_observations("operator", "batch-a", "k1", TRAFFIC_ROWS)
        self.service.seal_resampling_round("stat", "batch-a", opened["round_id"])
        job = self.service.claim_job("worker", 60)
        self.service.complete_job("worker", job["job_id"], "stat")
        # 批次已到修订 4，不能再对修订 3 的旧分析签发决定。
        with self.assertRaises(InvalidState):
            self.service.decide(
                "approver", "batch-a", first_analysis["analysis_id"], "approved", "陈旧审批"
            )

    def test_seal_before_plan_quota_met_is_blocked(self) -> None:
        _, decision = self._first_cycle_needs_more_data()
        plan = self.service.issue_supplement_plan(
            "approver", "batch-a", decision["decision_id"],
            [{"stratum_key": "cross-traffic", "minimum_count": 3}],
        )
        opened = self.service.open_resampling_round("operator", "batch-a", plan["plan_id"])
        self.service.import_observations("operator", "batch-a", "k1", TRAFFIC_ROWS[:2])
        with self.assertRaises(InvalidState):
            self.service.seal_resampling_round("stat", "batch-a", opened["round_id"])
        self.assertEqual(self.service.get_batch("batch-a")["state"], "resampling")

    def test_excluded_sample_consumes_no_quota_and_must_be_replaced(self) -> None:
        _, decision = self._first_cycle_needs_more_data()
        plan = self.service.issue_supplement_plan(
            "approver", "batch-a", decision["decision_id"],
            [{"stratum_key": "cross-traffic", "minimum_count": 3}],
        )
        opened = self.service.open_resampling_round("operator", "batch-a", plan["plan_id"])
        self.service.import_observations("operator", "batch-a", "k1", TRAFFIC_ROWS)
        observation_id = self.connection.execute(
            "SELECT observation_id FROM observations WHERE round_id=? ORDER BY observation_id LIMIT 1",
            (opened["round_id"],),
        ).fetchone()[0]
        requested = self.service.request_exclusion("operator", observation_id, "记录失效")
        self.service.review_exclusion("stat", requested["exclusion_id"], True, "证据充分")
        # 有效样本只剩 2 条，封存被阻止。
        with self.assertRaises(InvalidState):
            self.service.seal_resampling_round("stat", "batch-a", opened["round_id"])
        # 被排除的样本腾出名额，可以追加一条补齐。
        replacement = make_row("t-4", "cross-traffic", completion_seconds=59.0)
        self.service.import_observations("operator", "batch-a", "k2", [replacement])
        sealed = self.service.seal_resampling_round("stat", "batch-a", opened["round_id"])
        self.assertEqual(sealed["revision"], 4)

    def test_plan_only_for_needs_more_data_and_current_revision(self) -> None:
        # 完整样本直接通过的批次不能签发补样计划。
        self.service.import_observations("operator", "batch-a", "k0", CLEAR_ROWS + TRAFFIC_ROWS)
        self.service.seal_batch("stat", "batch-a", 2)
        job = self.service.claim_job("worker", 60)
        analysis = self.service.complete_job("worker", job["job_id"], "stat")
        decision = self.service.decide("approver", "batch-a", analysis["analysis_id"], "approved", "通过")
        decision_id = self.connection.execute(
            "SELECT decision_id FROM decisions WHERE analysis_id=?", (analysis["analysis_id"],)
        ).fetchone()["decision_id"]
        with self.assertRaises(InvalidState):
            self.service.issue_supplement_plan(
                "approver", "batch-a", decision_id,
                [{"stratum_key": "cross-traffic", "minimum_count": 1}],
            )
        # 未知决定被拒绝。
        with self.assertRaises(NotFound):
            self.service.issue_supplement_plan(
                "approver", "batch-a", 9999,
                [{"stratum_key": "cross-traffic", "minimum_count": 1}],
            )

    def test_plan_validation_rejects_unknown_stratum_and_bad_count(self) -> None:
        _, decision = self._first_cycle_needs_more_data()
        with self.assertRaises(ValidationFailed):
            self.service.issue_supplement_plan(
                "approver", "batch-a", decision["decision_id"],
                [{"stratum_key": "unknown-stratum", "minimum_count": 1}],
            )
        with self.assertRaises(ValidationFailed):
            self.service.issue_supplement_plan(
                "approver", "batch-a", decision["decision_id"],
                [{"stratum_key": "cross-traffic", "minimum_count": 0}],
            )
        with self.assertRaises(ValidationFailed):
            self.service.issue_supplement_plan("approver", "batch-a", decision["decision_id"], [])

    def test_role_separation_for_resampling(self) -> None:
        _, decision = self._first_cycle_needs_more_data()
        with self.assertRaises(Forbidden):
            self.service.issue_supplement_plan(
                "operator", "batch-a", decision["decision_id"],
                [{"stratum_key": "cross-traffic", "minimum_count": 1}],
            )
        plan = self.service.issue_supplement_plan(
            "approver", "batch-a", decision["decision_id"],
            [{"stratum_key": "cross-traffic", "minimum_count": 3}],
        )
        with self.assertRaises(Forbidden):
            self.service.open_resampling_round("approver", "batch-a", plan["plan_id"])
        opened = self.service.open_resampling_round("operator", "batch-a", plan["plan_id"])
        self.service.import_observations("operator", "batch-a", "k1", TRAFFIC_ROWS)
        with self.assertRaises(Forbidden):
            self.service.seal_resampling_round("operator", "batch-a", opened["round_id"])

    def test_round_observations_must_match_protocol_and_build(self) -> None:
        _, decision = self._first_cycle_needs_more_data()
        plan = self.service.issue_supplement_plan(
            "approver", "batch-a", decision["decision_id"],
            [{"stratum_key": "cross-traffic", "minimum_count": 3}],
        )
        self.service.open_resampling_round("operator", "batch-a", plan["plan_id"])
        wrong_robot = make_row("x-1", "cross-traffic", robot="robot-other")
        with self.assertRaises(ValidationFailed):
            self.service.import_observations("operator", "batch-a", "k1", [wrong_robot])
        wrong_protocol = deepcopy(TRAFFIC_ROWS[0])
        wrong_protocol["protocol_version"] = 2
        with self.assertRaises(ValidationFailed):
            self.service.import_observations("operator", "batch-a", "k2", [wrong_protocol])

    def test_two_consecutive_resampling_rounds_chain_in_report(self) -> None:
        # 第一轮：计划只补 clear-aisle（已在首批，故计划改向 cross-traffic 补 2 条，仍不足）。
        _, decision = self._first_cycle_needs_more_data()
        plan_1 = self.service.issue_supplement_plan(
            "approver", "batch-a", decision["decision_id"],
            [{"stratum_key": "cross-traffic", "minimum_count": 2}],
        )
        round_1 = self.service.open_resampling_round("operator", "batch-a", plan_1["plan_id"])
        self.service.import_observations("operator", "batch-a", "k1", TRAFFIC_ROWS[:2])
        self.service.seal_resampling_round("stat", "batch-a", round_1["round_id"])
        job = self.service.claim_job("worker", 60)
        analysis_2 = self.service.complete_job("worker", job["job_id"], "stat")
        self.assertEqual(analysis_2["result"]["conclusion"], "insufficient")
        decision_2 = self.service.decide(
            "approver", "batch-a", analysis_2["analysis_id"], "needs_more_data", "仍缺 1 条"
        )
        decision_2_id = self.connection.execute(
            "SELECT decision_id FROM decisions WHERE analysis_id=?", (analysis_2["analysis_id"],)
        ).fetchone()["decision_id"]
        plan_2 = self.service.issue_supplement_plan(
            "approver", "batch-a", decision_2_id,
            [{"stratum_key": "cross-traffic", "minimum_count": 1}],
        )
        round_2 = self.service.open_resampling_round("operator", "batch-a", plan_2["plan_id"])
        self.assertEqual(round_2["round_number"], 2)
        self.service.import_observations("operator", "batch-a", "k2", TRAFFIC_ROWS[2:])
        self.service.seal_resampling_round("stat", "batch-a", round_2["round_id"])
        job_2 = self.service.claim_job("worker", 60)
        analysis_3 = self.service.complete_job("worker", job_2["job_id"], "stat")
        self.assertEqual(analysis_3["result"]["conclusion"], "pass")
        self.service.decide(
            "approver", "batch-a", analysis_3["analysis_id"], "approved", "两轮补样后通过"
        )
        report = self.service.report("auditor", "batch-a")
        self.assertEqual([r["round_number"] for r in report["rounds"]], [1, 2])
        self.assertEqual([item["batch_revision"] for item in report["revisions"]], [3, 4, 5])
        self.assertEqual(
            [item["decision"]["decision"] for item in report["revisions"]],
            ["needs_more_data", "needs_more_data", "approved"],
        )
        self.assertEqual(report["rounds"][0]["increments"][0]["count"], 2)
        self.assertEqual(report["rounds"][1]["increments"][0]["count"], 1)
        # 三个输入快照互不相同。
        digests = [item["input_sha256"] for item in report["revisions"]]
        self.assertEqual(len(set(digests)), 3)

    def test_open_round_requires_issued_plan(self) -> None:
        _, decision = self._first_cycle_needs_more_data()
        with self.assertRaises(NotFound):
            self.service.open_resampling_round("operator", "batch-a", 9999)
        # 计划签发前批次仍是 decided，不能导入。
        with self.assertRaises(InvalidState):
            self.service.import_observations("operator", "batch-a", "late", TRAFFIC_ROWS[:1])
        self.assertIsNotNone(decision)


if __name__ == "__main__":
    unittest.main()
