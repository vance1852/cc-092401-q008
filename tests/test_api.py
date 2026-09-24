from __future__ import annotations

import json
import sqlite3
import unittest
from datetime import datetime, timezone

from robot_trials.api import JsonApplication
from robot_trials.clock import FrozenClock
from robot_trials.service import TrialService


class ApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        clock = FrozenClock(datetime(2026, 9, 24, tzinfo=timezone.utc))
        self.service = TrialService(self.connection, clock)
        self.app = JsonApplication(self.service)

    def tearDown(self) -> None:
        self.connection.close()

    def _call(self, method: str, target: str, actor: str | None, body: dict | None = None) -> object:
        headers = {} if actor is None else {"X-Actor-Id": actor}
        payload = b"" if body is None else json.dumps(body).encode()
        return self.app.handle(method, target, headers, payload)

    def test_health(self) -> None:
        response = self.app.handle("GET", "/health")
        self.assertEqual(response.status, 200)
        self.assertEqual(response.body["status"], "ok")

    def test_json_error_shape(self) -> None:
        response = self.app.handle("POST", "/users", body=b"not-json")
        self.assertEqual(response.status, 422)
        self.assertEqual(response.body["error"]["code"], "validation_failed")

    def test_user_route(self) -> None:
        payload = json.dumps({"user_id": "u1", "display_name": "操作员", "role": "operator"}).encode()
        response = self.app.handle("POST", "/users", body=payload)
        self.assertEqual(response.status, 201)
        self.assertEqual(response.body["role"], "operator")

    def test_supplement_routes_enforce_role_and_routing(self) -> None:
        for user_id, role in (
            ("operator", "operator"), ("stat", "statistician"),
            ("approver", "approver"), ("auditor", "auditor"),
        ):
            self.service.create_user(user_id, user_id, role)
        # 操作员无权签发补样计划。
        forbidden = self._call(
            "POST", "/supplement-plans", "operator",
            {"batch_id": "b", "decision_id": 1, "items": []},
        )
        self.assertEqual(forbidden.status, 403)
        # 审批人调用但决定不存在 -> 404，证明路由正确进入服务。
        not_found = self._call(
            "POST", "/supplement-plans", "approver",
            {"batch_id": "b", "decision_id": 99,
             "items": [{"stratum_key": "s", "minimum_count": 1}]},
        )
        self.assertEqual(not_found.status, 404)
        # 审批人无权开启补样轮次。
        forbidden_open = self._call(
            "POST", "/batches/b/supplement-rounds", "approver",
            {"plan_id": 1, "expected_revision": 3},
        )
        self.assertEqual(forbidden_open.status, 403)
        # 未知路由仍然 404。
        self.assertEqual(self._call("POST", "/nope", "operator", {}).status, 404)


if __name__ == "__main__":
    unittest.main()
