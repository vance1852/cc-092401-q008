"""完整产品流程的离线验收入口。

验收会走通一次补样闭环：首采在某个分层样本不足，审批人决定
needs_more_data 并签发结构化补样计划，操作员开启关联原批次的补样轮次，
追加观测封存后产生新的批次修订、输入快照与分析版本，最终报告串联每轮增量。
"""

from __future__ import annotations

import argparse
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from robot_trials.clock import FrozenClock
from robot_trials.jsonio import load_json
from robot_trials.service import TrialService
from robot_trials.storage import connect, inspect_schema


def run(workspace: Path) -> dict[str, object]:
    fixtures = workspace / "fixtures"
    protocol = load_json(fixtures / "demo_protocol.json")
    observation_rows = [
        json.loads(line)
        for line in (fixtures / "demo_observations.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    # 首采只导入 5 条：clear-aisle 3 条满足，cross-traffic 仅 2 条（要求 3 条）。
    initial_rows = [row for row in observation_rows if row["source_row"] != "006"]
    # 补样轮次追加剩余的 1 条，补齐 cross-traffic 分层。
    supplement_rows = [row for row in observation_rows if row["source_row"] == "006"]

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
            service.create_batch("operator-1", "batch-demo", protocol["protocol_id"], protocol["version"], "build-a1")

            # 第一轮（原始）采集与分析。
            service.start_batch("operator-1", "batch-demo", 1)
            clock.advance(minutes=1)
            service.import_observations("operator-1", "batch-demo", "demo-import-1", initial_rows)
            clock.advance(minutes=1)
            service.seal_batch("stat-1", "batch-demo", 2)
            clock.advance(minutes=1)
            first_job = service.claim_job("worker-1", lease_seconds=600)
            if first_job is None:
                raise RuntimeError("未能领取首轮分析任务")
            first_analysis = service.complete_job("worker-1", first_job["job_id"], "stat-1")
            if first_analysis["result"]["conclusion"] != "insufficient":
                raise RuntimeError("首轮分析应当判定分层样本不足")
            clock.advance(minutes=1)
            first_decision = service.decide(
                "approver-1", "batch-demo", first_analysis["analysis_id"],
                "needs_more_data", "cross-traffic 分层样本不足",
            )

            # 审批人签发结构化补样计划：cross-traffic 至少补 1 条。
            clock.advance(minutes=1)
            plan = service.issue_supplement_plan(
                "approver-1", "batch-demo", first_decision["decision_id"],
                [{"stratum_key": "cross-traffic", "minimum_count": 1}], "补齐横向人流分层",
            )

            # 操作员开启关联原批次的补样轮次，追加符合协议与构建约束的观测。
            clock.advance(minutes=1)
            service.open_supplement_round(
                "operator-1", "batch-demo", plan["plan_id"], expected_revision=3
            )
            clock.advance(minutes=1)
            service.import_observations("operator-1", "batch-demo", "demo-import-2", supplement_rows)
            clock.advance(minutes=1)
            service.seal_batch("stat-1", "batch-demo", 4)
            clock.advance(minutes=1)
            second_job = service.claim_job("worker-1", lease_seconds=600)
            if second_job is None:
                raise RuntimeError("未能领取补样轮次分析任务")
            second_analysis = service.complete_job("worker-1", second_job["job_id"], "stat-1")
            if second_analysis["round_seq"] != 1:
                raise RuntimeError("补样分析版本应归属第 1 补样轮次")
            clock.advance(minutes=1)
            service.decide(
                "approver-1", "batch-demo", second_analysis["analysis_id"],
                "approved" if second_analysis["result"]["conclusion"] == "pass" else "rejected",
                "补样后复核决定",
            )
            report = service.report("auditor-1", "batch-demo")
            schema = inspect_schema(connection)
        finally:
            connection.close()

    if schema["missing_tables"] or schema["schema_version"] != "3":
        raise RuntimeError("SQLite 基础结构检查失败")
    rounds = report["rounds"]
    if len(rounds) != 2 or [item["round_seq"] for item in rounds] != [0, 1]:
        raise RuntimeError("最终报告必须按顺序串联原始轮次与补样轮次")
    initial, supplement = rounds
    if initial["sample_increment_total"] != 5 or supplement["sample_increment_total"] != 1:
        raise RuntimeError("各轮样本增量记录不正确")
    if initial["decision"]["decision"] != "needs_more_data":
        raise RuntimeError("首轮决定必须永久保留为 needs_more_data")
    if initial["analysis"]["input_sha256"] == supplement["analysis"]["input_sha256"]:
        raise RuntimeError("两轮输入快照摘要必须不同")
    if supplement["plan"]["items"] != [{"stratum_key": "cross-traffic", "minimum_count": 1}]:
        raise RuntimeError("补样计划未在报告中完整保留")
    if report["decision"]["decision"] != "approved":
        raise RuntimeError("补样后最终决定应为 approved")
    return {
        "status": "ok",
        "protocol": f"{protocol['protocol_id']}@{protocol['version']}",
        "initial_observation_count": initial["sample_increment_total"],
        "supplement_observation_count": supplement["sample_increment_total"],
        "initial_analysis_id": first_analysis["analysis_id"],
        "supplement_analysis_id": second_analysis["analysis_id"],
        "initial_input_sha256": first_analysis["input_sha256"],
        "supplement_input_sha256": second_analysis["input_sha256"],
        "initial_conclusion": first_analysis["result"]["conclusion"],
        "supplement_conclusion": second_analysis["result"]["conclusion"],
        "final_decision": report["decision"]["decision"],
        "event_count": len(report["events"]),
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
