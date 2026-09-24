"""完整产品流程的离线验收入口。"""

from __future__ import annotations

import argparse
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from .clock import FrozenClock
from .jsonio import load_json
from .service import TrialService
from .storage import connect, inspect_schema


def run(workspace: Path) -> dict[str, object]:
    fixtures = workspace / "fixtures"
    protocol = load_json(fixtures / "demo_protocol.json")
    observation_rows = [
        json.loads(line)
        for line in (fixtures / "demo_observations.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    clear_rows = [row for row in observation_rows if row["stratum_key"] == "clear-aisle"]
    traffic_rows = [row for row in observation_rows if row["stratum_key"] == "cross-traffic"]
    clock = FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc))
    with tempfile.TemporaryDirectory(prefix="robot-trials-") as temporary:
        database = Path(temporary) / "foundation.sqlite3"
        connection = connect(database)
        try:
            service = TrialService(connection, clock)
            service.create_user("operator-1", "测试操作员", "operator")
            service.create_user("stat-1", "统计负责人", "statistician")
            service.create_user("approver-1", "准入审批人", "approver")
            service.create_user("auditor-1", "审计人员", "auditor")
            service.register_robot("operator-1", "robot-a", "A 型人形机器人", "示例厂商")
            service.register_build("operator-1", "build-a1", "robot-a", "1.0.0", "a" * 64)
            service.publish_protocol("stat-1", protocol)

            # 标准闭环：完整样本一次通过。
            service.create_batch("operator-1", "batch-demo", protocol["protocol_id"], protocol["version"], "build-a1")
            service.start_batch("operator-1", "batch-demo", 1)
            imported = service.import_observations(
                "operator-1", "batch-demo", "demo-import-1", observation_rows
            )
            service.seal_batch("stat-1", "batch-demo", 2)
            job = service.claim_job("worker-1", lease_seconds=60)
            if job is None:
                raise RuntimeError("未能领取分析任务")
            analysis = service.complete_job("worker-1", job["job_id"], "stat-1")
            decision_value = "approved" if analysis["result"]["conclusion"] == "pass" else "rejected"
            service.decide(
                "approver-1", "batch-demo", analysis["analysis_id"], decision_value, "离线验收决定"
            )
            demo_report = service.report("auditor-1", "batch-demo")

            # 补样闭环：首批只覆盖一个分层，判为需要更多数据后签发计划并补采。
            service.create_batch("operator-1", "batch-resample", protocol["protocol_id"], protocol["version"], "build-a1")
            service.start_batch("operator-1", "batch-resample", 1)
            service.import_observations("operator-1", "batch-resample", "resample-import-1", clear_rows)
            service.seal_batch("stat-1", "batch-resample", 2)
            first_job = service.claim_job("worker-1", lease_seconds=60)
            first_analysis = service.complete_job("worker-1", first_job["job_id"], "stat-1")
            if first_analysis["result"]["conclusion"] != "insufficient":
                raise RuntimeError("补样验收期望首批结论为 insufficient")
            service.decide(
                "approver-1", "batch-resample", first_analysis["analysis_id"],
                "needs_more_data", "cross-traffic 分层缺失",
            )
            decision_id = connection.execute(
                "SELECT decision_id FROM decisions WHERE batch_id=? AND analysis_id=?",
                ("batch-resample", first_analysis["analysis_id"]),
            ).fetchone()["decision_id"]
            clock.advance(hours=1)
            plan = service.issue_supplement_plan(
                "approver-1", "batch-resample", decision_id,
                [{"stratum_key": "cross-traffic", "minimum_count": 3}],
                note="补齐横向人流分层",
            )
            clock.advance(hours=1)
            opened = service.open_resampling_round("operator-1", "batch-resample", plan["plan_id"])
            service.import_observations(
                "operator-1", "batch-resample", "resample-import-2", traffic_rows
            )
            clock.advance(hours=1)
            service.seal_resampling_round("stat-1", "batch-resample", opened["round_id"])
            second_job = service.claim_job("worker-1", lease_seconds=60)
            second_analysis = service.complete_job("worker-1", second_job["job_id"], "stat-1")
            if second_analysis["input_sha256"] == first_analysis["input_sha256"]:
                raise RuntimeError("补样后输入快照应当变化")
            final_decision = service.decide(
                "approver-1", "batch-resample", second_analysis["analysis_id"],
                "approved" if second_analysis["result"]["conclusion"] == "pass" else "rejected",
                "补样后满足准入规则",
            )
            resample_report = service.report("auditor-1", "batch-resample")
            schema = inspect_schema(connection)
        finally:
            connection.close()
    if schema["missing_tables"] or schema["schema_version"] != "3":
        raise RuntimeError("SQLite 基础结构检查失败")
    rounds = resample_report["rounds"]
    if len(rounds) != 1 or rounds[0]["increments"] != [
        {"stratum_key": "cross-traffic", "count": 3, "included": 3}
    ]:
        raise RuntimeError("补样轮次增量记录不正确")
    if [item["batch_revision"] for item in resample_report["revisions"]] != [3, 4]:
        raise RuntimeError("报告必须串联每个修订版本")
    if resample_report["revisions"][0]["decision"]["decision"] != "needs_more_data":
        raise RuntimeError("旧决定必须永久可追溯")
    return {
        "status": "ok",
        "protocol": f"{protocol['protocol_id']}@{protocol['version']}",
        "observation_count": imported["inserted"],
        "analysis_id": analysis["analysis_id"],
        "input_sha256": analysis["input_sha256"],
        "conclusion": analysis["result"]["conclusion"],
        "decision": demo_report["decision"]["decision"],
        "resampling": {
            "old_analysis_id": first_analysis["analysis_id"],
            "new_analysis_id": second_analysis["analysis_id"],
            "old_input_sha256": first_analysis["input_sha256"],
            "new_input_sha256": second_analysis["input_sha256"],
            "final_decision": final_decision["decision"],
            "rounds": len(rounds),
            "revisions": [item["batch_revision"] for item in resample_report["revisions"]],
        },
        "event_count": len(resample_report["events"]),
        "schema": schema,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="执行试验数据基础工具的离线自检")
    parser.add_argument("--workspace", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)
    result = run(args.workspace.resolve())
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
