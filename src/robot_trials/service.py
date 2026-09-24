"""统计准入服务的领域用例。"""

from __future__ import annotations

import json
import sqlite3
from datetime import timedelta
from decimal import Decimal
from typing import Any, Iterable, Mapping

from .analysis import ALGORITHM_VERSION, analyze
from .clock import SystemClock, isoformat
from .contracts import Observation, Protocol, ValidationError
from .errors import Conflict, Forbidden, InvalidState, NotFound, ValidationFailed
from .jsonio import canonical_json, content_digest
from .storage import initialize, transaction


ROLE_PERMISSIONS = {
    "operator": {
        "catalog.write", "batch.create", "batch.start", "observation.import",
        "exclusion.request", "exclusion.revoke", "round.open",
    },
    "statistician": {"protocol.publish", "batch.seal", "exclusion.review", "analysis.run"},
    "approver": {"decision.write", "supplement.plan"},
    "auditor": {"report.read", "audit.read"},
}


class TrialService:
    """在单个 SQLite 连接上提供全部业务操作。"""

    def __init__(self, connection: sqlite3.Connection, clock=None) -> None:
        self.connection = connection
        self.clock = clock or SystemClock()
        initialize(connection)

    def _now(self) -> str:
        return isoformat(self.clock.now())

    def _user(self, user_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT user_id, display_name, role, active FROM users WHERE user_id=?", (user_id,)
        ).fetchone()
        if row is None:
            raise NotFound(f"用户不存在: {user_id}")
        if not row["active"]:
            raise Forbidden("用户已停用")
        return row

    def _require(self, user_id: str, permission: str) -> sqlite3.Row:
        user = self._user(user_id)
        if permission not in ROLE_PERMISSIONS[user["role"]]:
            raise Forbidden(f"角色 {user['role']} 无权执行 {permission}")
        return user

    def _audit(
        self, entity_type: str, entity_id: str, event_type: str, actor_id: str, payload: Mapping[str, Any]
    ) -> None:
        self.connection.execute(
            "INSERT INTO audit_events(entity_type,entity_id,event_type,actor_id,payload_json,created_at) "
            "VALUES(?,?,?,?,?,?)",
            (entity_type, entity_id, event_type, actor_id, canonical_json(payload), self._now()),
        )

    def create_user(self, user_id: str, display_name: str, role: str) -> dict[str, Any]:
        if role not in ROLE_PERMISSIONS:
            raise ValidationFailed(f"未知角色: {role}")
        if not user_id.strip() or not display_name.strip():
            raise ValidationFailed("用户编号和名称不能为空")
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO users(user_id,display_name,role) VALUES(?,?,?)",
                    (user_id.strip(), display_name.strip(), role),
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict(f"用户已存在: {user_id}") from exc
        return {"user_id": user_id.strip(), "role": role}

    def register_robot(
        self, actor_id: str, robot_id: str, model_name: str, vendor: str
    ) -> dict[str, Any]:
        self._require(actor_id, "catalog.write")
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO robots(robot_id,model_name,vendor,created_at) VALUES(?,?,?,?)",
                    (robot_id, model_name, vendor, self._now()),
                )
                self._audit("robot", robot_id, "robot.registered", actor_id, {"model_name": model_name})
        except sqlite3.IntegrityError as exc:
            raise Conflict(f"机器人已存在: {robot_id}") from exc
        return {"robot_id": robot_id, "model_name": model_name, "vendor": vendor}

    def register_build(
        self, actor_id: str, build_id: str, robot_id: str, version: str, content_sha256: str
    ) -> dict[str, Any]:
        self._require(actor_id, "catalog.write")
        if len(content_sha256) != 64:
            raise ValidationFailed("构建摘要必须是 64 位 SHA-256")
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO builds(build_id,robot_id,version,content_sha256,created_at) VALUES(?,?,?,?,?)",
                    (build_id, robot_id, version, content_sha256.lower(), self._now()),
                )
                self._audit("build", build_id, "build.registered", actor_id, {"robot_id": robot_id, "version": version})
        except sqlite3.IntegrityError as exc:
            raise Conflict("构建编号、版本或摘要冲突") from exc
        return {"build_id": build_id, "robot_id": robot_id, "version": version}

    def publish_protocol(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "protocol.publish")
        try:
            protocol = Protocol.from_dict(raw)
        except ValidationError as exc:
            raise ValidationFailed(str(exc)) from exc
        text = canonical_json(raw)
        digest = content_digest([raw])
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO protocol_catalog(protocol_id,version,title,task_family,canonical_json,content_sha256,created_at) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (
                        protocol.protocol_id,
                        protocol.version,
                        protocol.title,
                        protocol.task_family,
                        text,
                        digest,
                        self._now(),
                    ),
                )
                identity = f"{protocol.protocol_id}@{protocol.version}"
                self._audit("protocol", identity, "protocol.published", actor_id, {"sha256": digest})
        except sqlite3.IntegrityError as exc:
            raise Conflict("协议版本或内容摘要已经存在") from exc
        return {"protocol_id": protocol.protocol_id, "version": protocol.version, "sha256": digest}

    def _protocol(self, protocol_id: str, version: int) -> tuple[Protocol, str]:
        row = self.connection.execute(
            "SELECT canonical_json,content_sha256 FROM protocol_catalog WHERE protocol_id=? AND version=?",
            (protocol_id, version),
        ).fetchone()
        if row is None:
            raise NotFound("协议版本不存在")
        return Protocol.from_dict(json.loads(row["canonical_json"])), row["content_sha256"]

    def create_batch(
        self,
        actor_id: str,
        batch_id: str,
        protocol_id: str,
        protocol_version: int,
        build_id: str,
    ) -> dict[str, Any]:
        self._require(actor_id, "batch.create")
        self._protocol(protocol_id, protocol_version)
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO batches(batch_id,protocol_id,protocol_version,build_id,state,created_by,created_at) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (batch_id, protocol_id, protocol_version, build_id, "draft", actor_id, self._now()),
                )
                self._audit("batch", batch_id, "batch.created", actor_id, {"build_id": build_id})
        except sqlite3.IntegrityError as exc:
            raise Conflict("批次编号冲突或构建不存在") from exc
        return self.get_batch(batch_id)

    def get_batch(self, batch_id: str) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
        if row is None:
            raise NotFound("批次不存在")
        return dict(row)

    def start_batch(self, actor_id: str, batch_id: str, expected_revision: int) -> dict[str, Any]:
        self._require(actor_id, "batch.start")
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "UPDATE batches SET state='running',revision=revision+1,started_at=? "
                "WHERE batch_id=? AND state='draft' AND revision=?",
                (self._now(), batch_id, expected_revision),
            )
            if cursor.rowcount != 1:
                raise InvalidState("批次不是当前草稿版本")
            self._audit("batch", batch_id, "batch.started", actor_id, {"from_revision": expected_revision})
        return self.get_batch(batch_id)

    def _idempotent_response(self, scope: str, key: str, request_digest: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT request_sha256,response_json FROM idempotency_keys WHERE scope=? AND key=?",
            (scope, key),
        ).fetchone()
        if row is None:
            return None
        if row["request_sha256"] != request_digest:
            raise Conflict("同一幂等键对应了不同请求内容")
        return json.loads(row["response_json"])

    def import_observations(
        self,
        actor_id: str,
        batch_id: str,
        idempotency_key: str,
        raw_rows: Iterable[Mapping[str, Any]],
    ) -> dict[str, Any]:
        self._require(actor_id, "observation.import")
        rows = tuple(raw_rows)
        if not rows:
            raise ValidationFailed("观测数组不能为空")
        request_digest = content_digest(rows)
        scope = f"observations:{batch_id}"
        existing = self._idempotent_response(scope, idempotency_key, request_digest)
        if existing is not None:
            return existing
        batch = self.get_batch(batch_id)
        if batch["state"] != "running":
            raise InvalidState("只有运行中的批次可以导入观测")
        protocol, _ = self._protocol(batch["protocol_id"], batch["protocol_version"])
        round_seq = batch["current_round_seq"]
        plan_limits: dict[str, int] = {}
        if round_seq > 0:
            plan_limits = self._plan_limits(batch_id, round_seq)
        build_robot = self.connection.execute(
            "SELECT robot_id FROM builds WHERE build_id=?", (batch["build_id"],)
        ).fetchone()["robot_id"]
        parsed: list[Observation] = []
        incoming_counts: dict[str, int] = {}
        for raw in rows:
            try:
                item = Observation.from_dict(raw, protocol)
            except ValidationError as exc:
                raise ValidationFailed(str(exc)) from exc
            if item.robot_id != build_robot:
                raise ValidationFailed("观测机器人与批次构建不一致")
            if round_seq > 0 and item.stratum_key not in plan_limits:
                raise ValidationFailed(f"补样计划未要求该分层: {item.stratum_key}")
            incoming_counts[item.stratum_key] = incoming_counts.get(item.stratum_key, 0) + 1
            parsed.append(item)
        if round_seq > 0:
            existing = self._included_round_counts(batch_id, round_seq)
            for stratum_key, incoming in incoming_counts.items():
                total = existing.get(stratum_key, 0) + incoming
                if total > plan_limits[stratum_key]:
                    raise InvalidState(
                        f"分层 {stratum_key} 补样数量 {total} 超过计划下限 "
                        f"{plan_limits[stratum_key]}"
                    )
        response = {
            "batch_id": batch_id,
            "round_seq": round_seq,
            "inserted": len(parsed),
            "request_sha256": request_digest,
        }
        try:
            with transaction(self.connection, immediate=True):
                for item, raw in zip(parsed, rows):
                    self.connection.execute(
                        "INSERT INTO observations(batch_id,round_seq,source_batch,source_row,robot_id,"
                        "stratum_key,observed_at,metrics_json,content_sha256,imported_by,imported_at) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            batch_id,
                            round_seq,
                            item.source_batch,
                            item.source_row,
                            item.robot_id,
                            item.stratum_key,
                            item.observed_at,
                            canonical_json({key: format(value, "f") for key, value in item.metrics.items()}),
                            content_digest([raw]),
                            actor_id,
                            self._now(),
                        ),
                    )
                self.connection.execute(
                    "INSERT INTO idempotency_keys(scope,key,request_sha256,response_json,created_at) VALUES(?,?,?,?,?)",
                    (scope, idempotency_key, request_digest, canonical_json(response), self._now()),
                )
                self._audit("batch", batch_id, "observations.imported", actor_id, response)
        except sqlite3.IntegrityError as exc:
            raise Conflict("来源行重复或幂等键并发冲突") from exc
        return response

    def request_exclusion(self, actor_id: str, observation_id: int, reason: str) -> dict[str, Any]:
        self._require(actor_id, "exclusion.request")
        observation = self.connection.execute(
            "SELECT observation_id,batch_id,round_seq FROM observations WHERE observation_id=?",
            (observation_id,),
        ).fetchone()
        if observation is None:
            raise NotFound("观测不存在")
        batch = self.get_batch(observation["batch_id"])
        if batch["state"] != "running":
            raise InvalidState("批次封存后不能申请排除")
        # 补样轮次只能处理本轮新增观测，旧轮次样本是历史证据不得改动。
        if observation["round_seq"] != batch["current_round_seq"]:
            raise InvalidState("只能对当前轮次新增的观测申请排除")
        try:
            with transaction(self.connection, immediate=True):
                cursor = self.connection.execute(
                    "INSERT INTO exclusion_requests(observation_id,status,reason,requested_by,requested_at) "
                    "VALUES(?,?,?,?,?)",
                    (observation_id, "pending", reason, actor_id, self._now()),
                )
                exclusion_id = cursor.lastrowid
                self._audit("observation", str(observation_id), "exclusion.requested", actor_id, {"reason": reason})
        except sqlite3.IntegrityError as exc:
            raise Conflict("该观测已有待处理或生效排除") from exc
        return {"exclusion_id": exclusion_id, "status": "pending"}

    def review_exclusion(
        self, actor_id: str, exclusion_id: int, approve: bool, note: str
    ) -> dict[str, Any]:
        self._require(actor_id, "exclusion.review")
        row = self.connection.execute(
            "SELECT * FROM exclusion_requests WHERE exclusion_id=?", (exclusion_id,)
        ).fetchone()
        if row is None:
            raise NotFound("排除申请不存在")
        if row["status"] != "pending":
            raise InvalidState("排除申请已经处理")
        if row["requested_by"] == actor_id:
            raise Forbidden("申请人不能复核自己的排除申请")
        status = "approved" if approve else "rejected"
        with transaction(self.connection, immediate=True):
            self.connection.execute(
                "UPDATE exclusion_requests SET status=?,reviewed_by=?,reviewed_at=?,review_note=? "
                "WHERE exclusion_id=? AND status='pending'",
                (status, actor_id, self._now(), note, exclusion_id),
            )
            self._audit("exclusion", str(exclusion_id), f"exclusion.{status}", actor_id, {"note": note})
        return {"exclusion_id": exclusion_id, "status": status}

    def revoke_exclusion(self, actor_id: str, exclusion_id: int, reason: str) -> dict[str, Any]:
        self._require(actor_id, "exclusion.revoke")
        row = self.connection.execute(
            "SELECT e.*,o.batch_id,o.round_seq FROM exclusion_requests e "
            "JOIN observations o ON o.observation_id=e.observation_id WHERE e.exclusion_id=?",
            (exclusion_id,),
        ).fetchone()
        if row is None:
            raise NotFound("排除记录不存在")
        if row["status"] != "approved":
            raise InvalidState("只有已批准的排除可以撤销")
        if row["requested_by"] != actor_id:
            raise Forbidden("只有原申请人可以撤销排除")
        batch = self.get_batch(row["batch_id"])
        if batch["state"] != "running":
            raise InvalidState("批次封存后不能改变排除状态")
        if row["round_seq"] != batch["current_round_seq"]:
            raise InvalidState("只能撤销当前轮次新增观测的排除")
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "UPDATE exclusion_requests SET status='revoked',review_note=?,reviewed_at=? "
                "WHERE exclusion_id=? AND status='approved'",
                (reason, self._now(), exclusion_id),
            )
            if cursor.rowcount != 1:
                raise InvalidState("排除状态已变化")
            self._audit(
                "observation",
                str(row["observation_id"]),
                "exclusion.revoked",
                actor_id,
                {"exclusion_id": exclusion_id, "reason": reason},
            )
        return {"exclusion_id": exclusion_id, "status": "revoked"}

    def _included_round_counts(self, batch_id: str, round_seq: int) -> dict[str, int]:
        """统计指定轮次中已纳入（未批准排除）的观测分层数量。"""

        rows = self.connection.execute(
            "SELECT o.stratum_key AS stratum_key, count(*) AS total "
            "FROM observations o "
            "LEFT JOIN exclusion_requests e ON e.observation_id=o.observation_id AND e.status='approved' "
            "WHERE o.batch_id=? AND o.round_seq=? AND e.observation_id IS NULL "
            "GROUP BY o.stratum_key",
            (batch_id, round_seq),
        ).fetchall()
        return {row["stratum_key"]: row["total"] for row in rows}

    def _plan_limits(self, batch_id: str, round_seq: int) -> dict[str, int]:
        rows = self.connection.execute(
            "SELECT i.stratum_key AS stratum_key, i.minimum_count AS minimum_count "
            "FROM supplement_plan_items i JOIN supplement_plans p ON p.plan_id=i.plan_id "
            "WHERE p.batch_id=? AND p.round_seq=?",
            (batch_id, round_seq),
        ).fetchall()
        if not rows:
            raise NotFound("补样计划不存在")
        return {row["stratum_key"]: row["minimum_count"] for row in rows}

    def seal_batch(self, actor_id: str, batch_id: str, expected_revision: int) -> dict[str, Any]:
        self._require(actor_id, "batch.seal")
        with transaction(self.connection, immediate=True):
            pending = self.connection.execute(
                "SELECT count(*) FROM exclusion_requests e JOIN observations o ON o.observation_id=e.observation_id "
                "WHERE o.batch_id=? AND e.status='pending'", (batch_id,)
            ).fetchone()[0]
            if pending:
                raise InvalidState("仍有待复核的排除申请")
            row = self.connection.execute(
                "SELECT current_round_seq FROM batches WHERE batch_id=? AND state='running' AND revision=?",
                (batch_id, expected_revision),
            ).fetchone()
            if row is None:
                raise InvalidState("批次状态或版本已变化")
            round_seq = row["current_round_seq"]
            if round_seq > 0:
                limits = self._plan_limits(batch_id, round_seq)
                counts = self._included_round_counts(batch_id, round_seq)
                short = [
                    {"stratum": key, "minimum": limits[key], "actual": counts.get(key, 0)}
                    for key in sorted(limits)
                    if counts.get(key, 0) < limits[key]
                ]
                if short:
                    raise InvalidState(f"补样计划尚未补齐: {canonical_json(short)}")
            now = self._now()
            new_revision = expected_revision + 1
            self.connection.execute(
                "UPDATE batches SET state='sealed',revision=?,sealed_at=? "
                "WHERE batch_id=? AND state='running' AND revision=?",
                (new_revision, now, batch_id, expected_revision),
            )
            self.connection.execute(
                "INSERT INTO analysis_jobs(batch_id,batch_revision,round_seq,state,available_at,created_at,updated_at) "
                "VALUES(?,?,?, 'queued', ?,?,?)",
                (batch_id, new_revision, round_seq, now, now, now),
            )
            if round_seq > 0:
                self.connection.execute(
                    "UPDATE supplement_rounds SET state='sealed',batch_revision_sealed=?,sealed_by=?,sealed_at=? "
                    "WHERE batch_id=? AND round_seq=? AND state='open'",
                    (new_revision, actor_id, now, batch_id, round_seq),
                )
                self._audit(
                    "batch", batch_id, "supplement.sealed", actor_id,
                    {"round_seq": round_seq, "revision": new_revision},
                )
            else:
                self._audit("batch", batch_id, "batch.sealed", actor_id, {"revision": new_revision})
        return self.get_batch(batch_id)

    def claim_job(self, worker_id: str, lease_seconds: int = 60) -> dict[str, Any] | None:
        if lease_seconds <= 0:
            raise ValidationFailed("租约时长必须大于零")
        now = self._now()
        expires = isoformat(self.clock.now() + timedelta(seconds=lease_seconds))
        with transaction(self.connection, immediate=True):
            row = self.connection.execute(
                "SELECT job_id FROM analysis_jobs WHERE "
                "(state='queued' AND available_at<=?) OR (state='leased' AND lease_expires_at<=?) "
                "ORDER BY available_at,job_id LIMIT 1",
                (now, now),
            ).fetchone()
            if row is None:
                return None
            self.connection.execute(
                "UPDATE analysis_jobs SET state='leased',attempts=attempts+1,lease_owner=?,lease_expires_at=?,updated_at=? "
                "WHERE job_id=?",
                (worker_id, expires, now, row["job_id"]),
            )
            claimed = self.connection.execute("SELECT * FROM analysis_jobs WHERE job_id=?", (row["job_id"],)).fetchone()
        return dict(claimed)

    def _analysis_observations(self, batch_id: str, protocol: Protocol) -> tuple[tuple[Observation, int], ...]:
        rows = self.connection.execute(
            "SELECT o.*,e.reason AS excluded_reason FROM observations o "
            "LEFT JOIN exclusion_requests e ON e.observation_id=o.observation_id AND e.status='approved' "
            "WHERE o.batch_id=? ORDER BY o.observation_id",
            (batch_id,),
        ).fetchall()
        items: list[tuple[Observation, int]] = []
        for row in rows:
            metrics = json.loads(row["metrics_json"])
            items.append((Observation(
                source_batch=row["source_batch"],
                source_row=row["source_row"],
                robot_id=row["robot_id"],
                protocol_id=protocol.protocol_id,
                protocol_version=protocol.version,
                stratum_key=row["stratum_key"],
                observed_at=row["observed_at"],
                metrics={key: Decimal(str(value)) for key, value in metrics.items()},
                excluded_reason=row["excluded_reason"],
            ), row["round_seq"]))
        return tuple(items)

    def complete_job(self, worker_id: str, job_id: int, statistician_id: str) -> dict[str, Any]:
        self._require(statistician_id, "analysis.run")
        job = self.connection.execute("SELECT * FROM analysis_jobs WHERE job_id=?", (job_id,)).fetchone()
        if job is None:
            raise NotFound("分析任务不存在")
        if job["state"] != "leased" or job["lease_owner"] != worker_id:
            raise InvalidState("任务未由当前工作进程持有")
        if job["lease_expires_at"] <= self._now():
            raise InvalidState("任务租约已经过期")
        batch = self.get_batch(job["batch_id"])
        round_seq = job["round_seq"]
        protocol, protocol_digest = self._protocol(batch["protocol_id"], batch["protocol_version"])
        observations = self._analysis_observations(batch["batch_id"], protocol)
        snapshot_rows = [
            {
                "round": item[1],
                "source_batch": item[0].source_batch,
                "source_row": item[0].source_row,
                "stratum": item[0].stratum_key,
                "metrics": {key: format(value, "f") for key, value in item[0].metrics.items()},
                "excluded_reason": item[0].excluded_reason,
            }
            for item in observations
        ]
        analysis_items = tuple(item[0] for item in observations)
        input_digest = content_digest(snapshot_rows)
        result = analyze(protocol, analysis_items)
        with transaction(self.connection, immediate=True):
            existing = self.connection.execute(
                "SELECT analysis_id,result_json FROM analyses WHERE batch_id=? AND batch_revision=? AND input_sha256=?",
                (batch["batch_id"], job["batch_revision"], input_digest),
            ).fetchone()
            if existing is None:
                cursor = self.connection.execute(
                    "INSERT INTO analyses(batch_id,batch_revision,round_seq,protocol_sha256,input_sha256,"
                    "algorithm_version,seed,result_json,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        batch["batch_id"], job["batch_revision"], round_seq, protocol_digest, input_digest,
                        ALGORITHM_VERSION, protocol.seed, canonical_json(result), statistician_id, self._now(),
                    ),
                )
                analysis_id = cursor.lastrowid
            else:
                analysis_id = existing["analysis_id"]
                result = json.loads(existing["result_json"])
            self.connection.execute(
                "UPDATE analysis_jobs SET state='succeeded',lease_owner=NULL,lease_expires_at=NULL,updated_at=? "
                "WHERE job_id=? AND state='leased' AND lease_owner=?",
                (self._now(), job_id, worker_id),
            )
            self.connection.execute(
                "UPDATE batches SET state='analyzed' WHERE batch_id=? AND state IN ('sealed','analyzing')",
                (batch["batch_id"],),
            )
            self._audit(
                "batch",
                batch["batch_id"],
                "analysis.completed",
                statistician_id,
                {"analysis_id": analysis_id, "round_seq": round_seq, "input_sha256": input_digest},
            )
        return {"analysis_id": analysis_id, "round_seq": round_seq, "input_sha256": input_digest, "result": result}

    def fail_job(self, worker_id: str, job_id: int, error: str, retry_seconds: int = 0) -> dict[str, Any]:
        available = isoformat(self.clock.now() + timedelta(seconds=retry_seconds))
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "UPDATE analysis_jobs SET state='queued',available_at=?,lease_owner=NULL,lease_expires_at=NULL," 
                "last_error=?,updated_at=? WHERE job_id=? AND state='leased' AND lease_owner=?",
                (available, error[:1000], self._now(), job_id, worker_id),
            )
            if cursor.rowcount != 1:
                raise InvalidState("任务未由当前工作进程持有")
        return {"job_id": job_id, "state": "queued", "available_at": available}

    def decide(
        self, actor_id: str, batch_id: str, analysis_id: int, decision: str, reason: str
    ) -> dict[str, Any]:
        self._require(actor_id, "decision.write")
        if decision not in {"needs_more_data", "approved", "rejected"}:
            raise ValidationFailed("未知准入决定")
        analysis_row = self.connection.execute(
            "SELECT * FROM analyses WHERE analysis_id=? AND batch_id=?", (analysis_id, batch_id)
        ).fetchone()
        if analysis_row is None:
            raise NotFound("分析版本不存在")
        if analysis_row["created_by"] == actor_id:
            raise Forbidden("统计负责人不能批准自己的分析")
        batch = self.get_batch(batch_id)
        if batch["state"] != "analyzed" or batch["revision"] != analysis_row["batch_revision"]:
            raise InvalidState("分析不是批次当前可审批版本（可能已存在更新的补样修订）")
        round_seq = analysis_row["round_seq"]
        try:
            with transaction(self.connection, immediate=True):
                cursor = self.connection.execute(
                    "INSERT INTO decisions(batch_id,analysis_id,round_seq,decision,reason,decided_by,decided_at) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (batch_id, analysis_id, round_seq, decision, reason, actor_id, self._now()),
                )
                self.connection.execute("UPDATE batches SET state='decided' WHERE batch_id=?", (batch_id,))
                self._audit(
                    "batch",
                    batch_id,
                    "decision.recorded",
                    actor_id,
                    {
                        "decision_id": cursor.lastrowid,
                        "analysis_id": analysis_id,
                        "round_seq": round_seq,
                        "decision": decision,
                    },
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("该分析版本已经形成决定") from exc
        return {
            "decision_id": cursor.lastrowid,
            "batch_id": batch_id,
            "analysis_id": analysis_id,
            "round_seq": round_seq,
            "decision": decision,
        }

    def issue_supplement_plan(
        self,
        actor_id: str,
        batch_id: str,
        decision_id: int,
        items: Iterable[Mapping[str, Any]],
        note: str | None = None,
    ) -> dict[str, Any]:
        """审批人针对 needs_more_data 决定签发结构化补样计划。"""

        self._require(actor_id, "supplement.plan")
        entries = tuple(items)
        if not entries:
            raise ValidationFailed("补样计划至少包含一个分层")
        protocol, _ = self._protocol_for_batch(batch_id)
        normalized: dict[str, int] = {}
        for entry in entries:
            key = entry.get("stratum_key")
            minimum = entry.get("minimum_count")
            if not isinstance(key, str) or not key.strip():
                raise ValidationFailed("补样分层键必须是非空字符串")
            key = key.strip()
            if key not in protocol.stratum_keys:
                raise ValidationFailed(f"补样分层未在协议中声明: {key}")
            if isinstance(minimum, bool) or not isinstance(minimum, int) or minimum <= 0:
                raise ValidationFailed(f"分层 {key} 的最低补样数量必须是正整数")
            if key in normalized:
                raise ValidationFailed(f"分层 {key} 在计划中重复")
            normalized[key] = minimum
        decision = self.connection.execute(
            "SELECT * FROM decisions WHERE decision_id=? AND batch_id=?", (decision_id, batch_id)
        ).fetchone()
        if decision is None:
            raise NotFound("补样决定不存在")
        if decision["decision"] != "needs_more_data":
            raise InvalidState("只有 needs_more_data 决定可以签发补样计划")
        if decision["decided_by"] != actor_id:
            raise Forbidden("只有作出该决定的审批人可以签发补样计划")
        analysis = self.connection.execute(
            "SELECT round_seq FROM analyses WHERE analysis_id=?", (decision["analysis_id"],)
        ).fetchone()
        decided_round = analysis["round_seq"]
        batch = self.get_batch(batch_id)
        if batch["current_round_seq"] != decided_round:
            raise InvalidState("该决定对应的轮次已不是最新轮次，不能再为其签发补样计划")
        next_round = decided_round + 1
        try:
            with transaction(self.connection, immediate=True):
                # 唯一约束 (batch_id, round_seq) 会阻止重复/并发签发同一轮计划。
                cursor = self.connection.execute(
                    "INSERT INTO supplement_plans(batch_id,decision_id,analysis_id,batch_revision,round_seq,"
                    "note,created_by,created_at) VALUES(?,?,?,?,?,?,?,?)",
                    (
                        batch_id, decision_id, decision["analysis_id"], batch["revision"],
                        next_round, note, actor_id, self._now(),
                    ),
                )
                plan_id = cursor.lastrowid
                self.connection.executemany(
                    "INSERT INTO supplement_plan_items(plan_id,stratum_key,minimum_count) VALUES(?,?,?)",
                    [(plan_id, key, count) for key, count in sorted(normalized.items())],
                )
                self._audit(
                    "batch", batch_id, "supplement.plan_issued", actor_id,
                    {
                        "plan_id": plan_id,
                        "decision_id": decision_id,
                        "round_seq": next_round,
                        "items": [{"stratum_key": k, "minimum_count": c} for k, c in sorted(normalized.items())],
                    },
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("该决定已经签发过补样计划，不能重复开启") from exc
        return {
            "plan_id": plan_id,
            "batch_id": batch_id,
            "decision_id": decision_id,
            "round_seq": next_round,
            "items": [{"stratum_key": k, "minimum_count": c} for k, c in sorted(normalized.items())],
        }

    def open_supplement_round(
        self, actor_id: str, batch_id: str, plan_id: int, expected_revision: int
    ) -> dict[str, Any]:
        """操作员据计划开启关联原批次的新补样轮次。"""

        self._require(actor_id, "round.open")
        with transaction(self.connection, immediate=True):
            plan = self.connection.execute(
                "SELECT * FROM supplement_plans WHERE plan_id=? AND batch_id=?", (plan_id, batch_id)
            ).fetchone()
            if plan is None:
                raise NotFound("补样计划不存在")
            round_seq = plan["round_seq"]
            existing_round = self.connection.execute(
                "SELECT state FROM supplement_rounds WHERE plan_id=?", (plan_id,)
            ).fetchone()
            if existing_round is not None:
                raise Conflict("该补样计划已经开启过轮次，不能重复开启")
            batch = self.connection.execute(
                "SELECT * FROM batches WHERE batch_id=? AND revision=?", (batch_id, expected_revision)
            ).fetchone()
            if batch is None:
                raise InvalidState("批次修订已变化，拒绝基于陈旧修订开启轮次")
            if batch["state"] != "decided":
                raise InvalidState("只有等待补样（decided）的批次可以开启补样轮次")
            # 轮次必须严格接续：上一轮是最近一次决定对应的轮次。
            if batch["current_round_seq"] != round_seq - 1:
                raise InvalidState("补样轮次顺序不连续，可能存在已开启的更新轮次")
            new_revision = expected_revision + 1
            now = self._now()
            self.connection.execute(
                "INSERT INTO supplement_rounds(batch_id,round_seq,plan_id,batch_revision_opened,"
                "batch_revision_sealed,state,opened_by,opened_at) VALUES(?,?,?,?,?, 'open', ?,?)",
                (batch_id, round_seq, plan_id, new_revision, None, actor_id, now),
            )
            self.connection.execute(
                "UPDATE batches SET state='running',revision=?,current_round_seq=? WHERE batch_id=?",
                (new_revision, round_seq, batch_id),
            )
            self._audit(
                "batch", batch_id, "supplement.round_opened", actor_id,
                {"plan_id": plan_id, "round_seq": round_seq, "revision": new_revision},
            )
        return self.get_batch(batch_id)

    def _protocol_for_batch(self, batch_id: str) -> tuple[Protocol, str]:
        batch = self.get_batch(batch_id)
        return self._protocol(batch["protocol_id"], batch["protocol_version"])

    def report(self, actor_id: str, batch_id: str) -> dict[str, Any]:
        user = self._user(actor_id)
        if user["role"] not in {"statistician", "approver", "auditor"}:
            raise Forbidden("当前角色不能读取完整报告")
        batch = self.get_batch(batch_id)
        protocol, protocol_digest = self._protocol(batch["protocol_id"], batch["protocol_version"])
        analysis_rows = self.connection.execute(
            "SELECT * FROM analyses WHERE batch_id=? ORDER BY analysis_id", (batch_id,)
        ).fetchall()
        analyses_by_round = {row["round_seq"]: row for row in analysis_rows}
        analysis_row = analysis_rows[-1] if analysis_rows else None
        decision_row = None
        decisions_by_analysis: dict[int, sqlite3.Row] = {}
        for row in self.connection.execute(
            "SELECT * FROM decisions WHERE batch_id=? ORDER BY decision_id", (batch_id,)
        ).fetchall():
            decisions_by_analysis[row["analysis_id"]] = row
        if analysis_row is not None:
            decision_row = decisions_by_analysis.get(analysis_row["analysis_id"])
        exclusions = self.connection.execute(
            "SELECT e.exclusion_id,e.observation_id,e.status,e.reason,e.requested_by,e.reviewed_by "
            "FROM exclusion_requests e JOIN observations o ON o.observation_id=e.observation_id "
            "WHERE o.batch_id=? ORDER BY e.exclusion_id", (batch_id,)
        ).fetchall()
        events = self.connection.execute(
            "SELECT event_type,actor_id,payload_json,created_at FROM audit_events "
            "WHERE entity_type='batch' AND entity_id=? "
            "ORDER BY event_id", (batch_id,)
        ).fetchall()
        rounds = self._report_rounds(
            batch_id, analyses_by_round, decisions_by_analysis
        )
        return {
            "batch": batch,
            "protocol": {
                "protocol_id": protocol.protocol_id,
                "version": protocol.version,
                "sha256": protocol_digest,
                "seed": protocol.seed,
                "bootstrap_samples": protocol.bootstrap_samples,
            },
            "rounds": rounds,
            "analysis": None if analysis_row is None else self._analysis_view(analysis_row),
            "decision": None if decision_row is None else dict(decision_row),
            "exclusions": [dict(row) for row in exclusions],
            "events": [dict(row) | {"payload": json.loads(row["payload_json"])} for row in events],
        }

    def _analysis_view(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "analysis_id": row["analysis_id"],
            "batch_revision": row["batch_revision"],
            "round_seq": row["round_seq"],
            "input_sha256": row["input_sha256"],
            "algorithm_version": row["algorithm_version"],
            "created_by": row["created_by"],
            "created_at": row["created_at"],
            "result": json.loads(row["result_json"]),
        }

    def _report_rounds(
        self,
        batch_id: str,
        analyses_by_round: dict[int, sqlite3.Row],
        decisions_by_analysis: dict[int, sqlite3.Row],
    ) -> list[dict[str, Any]]:
        round_rows = self.connection.execute(
            "SELECT * FROM supplement_rounds WHERE batch_id=? ORDER BY round_seq", (batch_id,)
        ).fetchall()
        rounds_by_seq = {row["round_seq"]: row for row in round_rows}
        plan_rows = self.connection.execute(
            "SELECT * FROM supplement_plans WHERE batch_id=? ORDER BY round_seq", (batch_id,)
        ).fetchall()
        plans_by_round = {row["round_seq"]: row for row in plan_rows}
        plan_items: dict[int, list[dict[str, Any]]] = {}
        for plan in plan_rows:
            items = self.connection.execute(
                "SELECT stratum_key,minimum_count FROM supplement_plan_items WHERE plan_id=? "
                "ORDER BY stratum_key", (plan["plan_id"],)
            ).fetchall()
            plan_items[plan["round_seq"]] = [dict(item) for item in items]
        increments: dict[int, dict[str, int]] = {}
        for row in self.connection.execute(
            "SELECT round_seq,stratum_key,count(*) AS total FROM observations "
            "WHERE batch_id=? GROUP BY round_seq,stratum_key ORDER BY round_seq,stratum_key",
            (batch_id,),
        ).fetchall():
            increments.setdefault(row["round_seq"], {})[row["stratum_key"]] = row["total"]
        last_round = max(
            [0, *rounds_by_seq.keys(), *analyses_by_round.keys(),
             *increments.keys(), *plans_by_round.keys()]
        )
        result: list[dict[str, Any]] = []
        for seq in range(0, last_round + 1):
            entry: dict[str, Any] = {
                "round_seq": seq,
                "kind": "initial" if seq == 0 else "supplement",
                "sample_increments": increments.get(seq, {}),
                "sample_increment_total": sum(increments.get(seq, {}).values()),
            }
            if seq > 0:
                supplement_round = rounds_by_seq.get(seq)
                entry["round"] = None if supplement_round is None else {
                    "plan_id": supplement_round["plan_id"],
                    "state": supplement_round["state"],
                    "opened_by": supplement_round["opened_by"],
                    "opened_at": supplement_round["opened_at"],
                    "sealed_by": supplement_round["sealed_by"],
                    "sealed_at": supplement_round["sealed_at"],
                    "revision_opened": supplement_round["batch_revision_opened"],
                    "revision_sealed": supplement_round["batch_revision_sealed"],
                }
                plan = plans_by_round.get(seq)
                entry["plan"] = None if plan is None else {
                    "plan_id": plan["plan_id"],
                    "decision_id": plan["decision_id"],
                    "analysis_id": plan["analysis_id"],
                    "batch_revision": plan["batch_revision"],
                    "note": plan["note"],
                    "created_by": plan["created_by"],
                    "created_at": plan["created_at"],
                    "items": plan_items.get(seq, []),
                }
            analysis = analyses_by_round.get(seq)
            entry["analysis"] = None if analysis is None else self._analysis_view(analysis)
            decision = None if analysis is None else decisions_by_analysis.get(analysis["analysis_id"])
            entry["decision"] = None if decision is None else dict(decision)
            result.append(entry)
        return result

