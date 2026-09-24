from __future__ import annotations

import json
import sqlite3
import unittest
from datetime import datetime, timezone
from pathlib import Path

from robot_trials.api import JsonApplication
from robot_trials.clock import FrozenClock
from robot_trials.service import TrialService


ROOT = Path(__file__).resolve().parents[1]


def make_row(source_row: str, stratum_key: str) -> dict:
    return {
        "source_batch": "hall-a-20260924",
        "source_row": source_row,
        "robot_id": "robot-a",
        "protocol_id": "demo-delivery-v1",
        "protocol_version": 1,
        "stratum_key": stratum_key,
        "observed_at": "2026-09-24T10:00:00+08:00",
        "metrics": {"completed": 1, "completion_seconds": "45.0", "interventions": 0},
        "excluded_reason": None,
    }


class ResamplingApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        clock = FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc))
        self.service = TrialService(self.connection, clock)
        self.app = JsonApplication(self.service)
        for user_id, role in (
            ("operator", "operator"),
            ("stat", "statistician"),
            ("approver", "approver"),
        ):
            self.service.create_user(user_id, user_id, role)
        protocol = json.loads((ROOT / "fixtures" / "demo_protocol.json").read_text(encoding="utf-8"))
        self.service.register_robot("operator", "robot-a", "A 型", "厂商")
        self.service.register_build("operator", "build-a", "robot-a", "1.0", "b" * 64)
        self.service.publish_protocol("stat", protocol)
        self.service.create_batch("operator", "batch-a", "demo-delivery-v1", 1, "build-a")
        self.service.start_batch("operator", "batch-a", 1)
        self.service.import_observations(
            "operator", "batch-a", "init",
            [make_row(f"c-{i}", "clear-aisle") for i in range(3)],
        )
        self.service.seal_batch("stat", "batch-a", 2)
        job = self.service.claim_job("worker", 60)
        analysis = self.service.complete_job("worker", job["job_id"], "stat")
        self.service.decide(
            "approver", "batch-a", analysis["analysis_id"], "needs_more_data", "缺分层"
        )
        self.decision_id = self.connection.execute(
            "SELECT decision_id FROM decisions WHERE analysis_id=?", (analysis["analysis_id"],)
        ).fetchone()["decision_id"]

    def tearDown(self) -> None:
        self.connection.close()

    def _post(self, path: str, payload: dict, actor: str, idempotency_key: str | None = None):
        headers = {"X-Actor-Id": actor}
        if idempotency_key is not None:
            headers["Idempotency-Key"] = idempotency_key
        return self.app.handle(
            "POST", path, headers, json.dumps(payload, ensure_ascii=False).encode("utf-8")
        )

    def test_resampling_routes_close_the_loop(self) -> None:
        response = self._post(
            "/supplement-plans",
            {"batch_id": "batch-a", "decision_id": self.decision_id,
             "items": [{"stratum_key": "cross-traffic", "minimum_count": 3}]},
            "approver",
        )
        self.assertEqual(response.status, 201)
        plan_id = response.body["plan_id"]

        response = self._post(
            "/batches/batch-a/rounds", {"plan_id": plan_id}, "operator"
        )
        self.assertEqual(response.status, 201)
        round_id = response.body["round_id"]

        traffic_rows = [make_row(f"t-{i}", "cross-traffic") for i in range(3)]
        response = self._post(
            "/batches/batch-a/observations", {"observations": traffic_rows},
            "operator", idempotency_key="round-import",
        )
        self.assertEqual(response.status, 200)
        self.assertEqual(response.body["inserted"], 3)
        self.assertEqual(response.body["round_id"], round_id)

        response = self._post(
            f"/batches/batch-a/rounds/{round_id}/seal", {}, "stat"
        )
        self.assertEqual(response.status, 200)
        self.assertEqual(response.body["state"], "sealed")
        self.assertEqual(response.body["revision"], 4)

        job = self.service.claim_job("worker", 60)
        analysis = self.service.complete_job("worker", job["job_id"], "stat")
        response = self._post(
            "/decisions",
            {"batch_id": "batch-a", "analysis_id": analysis["analysis_id"],
             "decision": "approved", "reason": "补样后通过"},
            "approver",
        )
        self.assertEqual(response.status, 201)

        report = self.app.handle(
            "GET", "/batches/batch-a/report", {"X-Actor-Id": "stat"}, b""
        )
        self.assertEqual(report.status, 200)
        self.assertEqual(len(report.body["rounds"]), 1)
        self.assertEqual(
            [item["batch_revision"] for item in report.body["revisions"]], [3, 4]
        )

    def test_over_quota_returns_conflict_status(self) -> None:
        response = self._post(
            "/supplement-plans",
            {"batch_id": "batch-a", "decision_id": self.decision_id,
             "items": [{"stratum_key": "cross-traffic", "minimum_count": 1}]},
            "approver",
        )
        plan_id = response.body["plan_id"]
        self._post("/batches/batch-a/rounds", {"plan_id": plan_id}, "operator")
        rows = [make_row("t-1", "cross-traffic"), make_row("t-2", "cross-traffic")]
        response = self._post(
            "/batches/batch-a/observations", {"observations": rows},
            "operator", idempotency_key="overshoot",
        )
        self.assertEqual(response.status, 409)
        self.assertEqual(response.body["error"]["code"], "invalid_state")

    def test_wrong_role_is_forbidden(self) -> None:
        response = self._post(
            "/supplement-plans",
            {"batch_id": "batch-a", "decision_id": self.decision_id,
             "items": [{"stratum_key": "cross-traffic", "minimum_count": 1}]},
            "operator",
        )
        self.assertEqual(response.status, 403)


if __name__ == "__main__":
    unittest.main()
