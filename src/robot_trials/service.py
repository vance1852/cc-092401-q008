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
    "statistician": {"protocol.publish", "batch.seal", "exclusion.review", "analysis.run", "round.seal"},
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
        active_round = None
        plan_items: dict[str, int] = {}
        if batch["state"] == "resampling":
            active_round = self.connection.execute(
                "SELECT * FROM resampling_rounds WHERE batch_id=? AND state='running' "
                "ORDER BY round_number DESC LIMIT 1",
                (batch_id,),
            ).fetchone()
            if active_round is None:
                raise InvalidState("批次处于补样状态但没有运行中的补样轮次")
            plan_items = {
                row["stratum_key"]: row["minimum_count"]
                for row in self.connection.execute(
                    "SELECT stratum_key,minimum_count FROM supplement_plan_items WHERE plan_id=?",
                    (active_round["plan_id"],),
                ).fetchall()
            }
        elif batch["state"] != "running":
            raise InvalidState("只有运行中的批次或补样轮次可以导入观测")
        protocol, _ = self._protocol(batch["protocol_id"], batch["protocol_version"])
        build_row = self.connection.execute(
            "SELECT robot_id FROM builds WHERE build_id=?", (batch["build_id"],)
        ).fetchone()
        if build_row is None:
            raise NotFound("批次构建不存在")
        parsed: list[Observation] = []
        additions: dict[str, int] = {}
        for raw in rows:
            try:
                item = Observation.from_dict(raw, protocol)
            except ValidationError as exc:
                raise ValidationFailed(str(exc)) from exc
            if item.robot_id != build_row["robot_id"]:
                raise ValidationFailed("观测机器人与批次构建不一致")
            if active_round is not None:
                if item.stratum_key not in plan_items:
                    raise ValidationFailed(
                        f"分层 {item.stratum_key} 不在补样计划范围内，不能超计划提交"
                    )
                additions[item.stratum_key] = additions.get(item.stratum_key, 0) + 1
            parsed.append(item)
        response = {
            "batch_id": batch_id,
            "inserted": len(parsed),
            "request_sha256": request_digest,
        }
        if active_round is not None:
            response["round_id"] = active_round["round_id"]
        try:
            with transaction(self.connection, immediate=True):
                if active_round is not None:
                    locked_round = self.connection.execute(
                        "SELECT round_id,state FROM resampling_rounds WHERE round_id=?",
                        (active_round["round_id"],),
                    ).fetchone()
                    if locked_round is None or locked_round["state"] != "running":
                        raise InvalidState("补样轮次已封存，计划完成后不能继续写入")
                    locked_state = self.connection.execute(
                        "SELECT state FROM batches WHERE batch_id=?", (batch_id,)
                    ).fetchone()["state"]
                    if locked_state != "resampling":
                        raise InvalidState("批次已离开补样状态，不能继续写入")
                    existing_rows = self.connection.execute(
                        "SELECT o.stratum_key AS stratum_key,count(*) AS total FROM observations o "
                        "LEFT JOIN exclusion_requests e "
                        "ON e.observation_id=o.observation_id AND e.status='approved' "
                        "WHERE o.round_id=? AND e.observation_id IS NULL GROUP BY o.stratum_key",
                        (active_round["round_id"],),
                    ).fetchall()
                    existing_counts = {row["stratum_key"]: row["total"] for row in existing_rows}
                    for stratum_key, added in additions.items():
                        if existing_counts.get(stratum_key, 0) + added > plan_items[stratum_key]:
                            raise InvalidState(
                                f"分层 {stratum_key} 补样数量超过计划下限 "
                                f"{plan_items[stratum_key]}，超计划提交被阻止"
                            )
                for item, raw in zip(parsed, rows):
                    self.connection.execute(
                        "INSERT INTO observations(batch_id,round_id,source_batch,source_row,robot_id,stratum_key,observed_at,"
                        "metrics_json,content_sha256,imported_by,imported_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            batch_id,
                            None if active_round is None else active_round["round_id"],
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
            "SELECT observation_id,batch_id FROM observations WHERE observation_id=?", (observation_id,)
        ).fetchone()
        if observation is None:
            raise NotFound("观测不存在")
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
            "SELECT e.*,o.batch_id FROM exclusion_requests e "
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
        if batch["state"] not in {"running", "resampling"}:
            raise InvalidState("批次封存后不能改变排除状态")
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

    def seal_batch(self, actor_id: str, batch_id: str, expected_revision: int) -> dict[str, Any]:
        self._require(actor_id, "batch.seal")
        with transaction(self.connection, immediate=True):
            pending = self.connection.execute(
                "SELECT count(*) FROM exclusion_requests e JOIN observations o ON o.observation_id=e.observation_id "
                "WHERE o.batch_id=? AND e.status='pending'", (batch_id,)
            ).fetchone()[0]
            if pending:
                raise InvalidState("仍有待复核的排除申请")
            cursor = self.connection.execute(
                "UPDATE batches SET state='sealed',revision=revision+1,sealed_at=? "
                "WHERE batch_id=? AND state='running' AND revision=?",
                (self._now(), batch_id, expected_revision),
            )
            if cursor.rowcount != 1:
                raise InvalidState("批次状态或版本已变化")
            new_revision = expected_revision + 1
            now = self._now()
            self.connection.execute(
                "INSERT INTO analysis_jobs(batch_id,batch_revision,state,available_at,created_at,updated_at) "
                "VALUES(?,?, 'queued', ?,?,?)",
                (batch_id, new_revision, now, now, now),
            )
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

    def _analysis_observations(self, batch_id: str, protocol: Protocol) -> tuple[Observation, ...]:
        rows = self.connection.execute(
            "SELECT o.*,e.reason AS excluded_reason FROM observations o "
            "LEFT JOIN exclusion_requests e ON e.observation_id=o.observation_id AND e.status='approved' "
            "WHERE o.batch_id=? ORDER BY o.observation_id",
            (batch_id,),
        ).fetchall()
        items: list[Observation] = []
        for row in rows:
            metrics = json.loads(row["metrics_json"])
            items.append(Observation(
                source_batch=row["source_batch"],
                source_row=row["source_row"],
                robot_id=row["robot_id"],
                protocol_id=protocol.protocol_id,
                protocol_version=protocol.version,
                stratum_key=row["stratum_key"],
                observed_at=row["observed_at"],
                metrics={key: Decimal(str(value)) for key, value in metrics.items()},
                excluded_reason=row["excluded_reason"],
            ))
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
        protocol, protocol_digest = self._protocol(batch["protocol_id"], batch["protocol_version"])
        observations = self._analysis_observations(batch["batch_id"], protocol)
        snapshot_rows = [
            {
                "source_batch": item.source_batch,
                "source_row": item.source_row,
                "stratum": item.stratum_key,
                "metrics": {key: format(value, "f") for key, value in item.metrics.items()},
                "excluded_reason": item.excluded_reason,
            }
            for item in observations
        ]
        input_digest = content_digest(snapshot_rows)
        result = analyze(protocol, observations)
        with transaction(self.connection, immediate=True):
            existing = self.connection.execute(
                "SELECT analysis_id,result_json FROM analyses WHERE batch_id=? AND batch_revision=? AND input_sha256=?",
                (batch["batch_id"], job["batch_revision"], input_digest),
            ).fetchone()
            if existing is None:
                cursor = self.connection.execute(
                    "INSERT INTO analyses(batch_id,batch_revision,protocol_sha256,input_sha256,algorithm_version,seed," 
                    "result_json,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        batch["batch_id"], job["batch_revision"], protocol_digest, input_digest,
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
                {"analysis_id": analysis_id, "input_sha256": input_digest},
            )
        return {"analysis_id": analysis_id, "input_sha256": input_digest, "result": result}

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
            raise InvalidState("分析不是批次当前可审批版本")
        try:
            with transaction(self.connection, immediate=True):
                cursor = self.connection.execute(
                    "INSERT INTO decisions(batch_id,analysis_id,decision,reason,decided_by,decided_at) "
                    "VALUES(?,?,?,?,?,?)",
                    (batch_id, analysis_id, decision, reason, actor_id, self._now()),
                )
                self.connection.execute("UPDATE batches SET state='decided' WHERE batch_id=?", (batch_id,))
                self._audit(
                    "batch",
                    batch_id,
                    "decision.recorded",
                    actor_id,
                    {"decision_id": cursor.lastrowid, "analysis_id": analysis_id, "decision": decision},
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("该分析版本已经形成决定") from exc
        return {"batch_id": batch_id, "analysis_id": analysis_id, "decision": decision}

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
        raw_items = tuple(items)
        if not raw_items:
            raise ValidationFailed("补样计划至少要声明一个分层")
        decision_row = self.connection.execute(
            "SELECT * FROM decisions WHERE decision_id=? AND batch_id=?", (decision_id, batch_id)
        ).fetchone()
        if decision_row is None:
            raise NotFound("准入决定不存在")
        if decision_row["decision"] != "needs_more_data":
            raise InvalidState("只有需要更多数据的决定可以签发补样计划")
        analysis_row = self.connection.execute(
            "SELECT batch_revision FROM analyses WHERE analysis_id=?", (decision_row["analysis_id"],)
        ).fetchone()
        batch = self.get_batch(batch_id)
        if batch["state"] != "decided":
            raise InvalidState("只有已定决定的批次可以签发补样计划")
        if analysis_row["batch_revision"] != batch["revision"]:
            raise InvalidState("不能针对陈旧修订签发补样计划")
        protocol, _ = self._protocol(batch["protocol_id"], batch["protocol_version"])
        allowed = protocol.stratum_keys
        parsed_items: list[dict[str, int | str]] = []
        seen: set[str] = set()
        for raw in raw_items:
            if not isinstance(raw, Mapping):
                raise ValidationFailed("补样计划条目必须是对象")
            key = raw.get("stratum_key")
            if not isinstance(key, str) or not key.strip():
                raise ValidationFailed("补样分层键必须是非空字符串")
            key = key.strip()
            if key not in allowed:
                raise ValidationFailed(f"分层 {key} 未在协议中声明")
            if key in seen:
                raise ValidationFailed(f"分层 {key} 在补样计划中重复")
            minimum = raw.get("minimum_count")
            if isinstance(minimum, bool) or not isinstance(minimum, int) or minimum <= 0:
                raise ValidationFailed(f"分层 {key} 的 minimum_count 必须是正整数")
            seen.add(key)
            parsed_items.append({"stratum_key": key, "minimum_count": minimum})
        try:
            with transaction(self.connection, immediate=True):
                cursor = self.connection.execute(
                    "INSERT INTO supplement_plans(batch_id,decision_id,analysis_id,batch_revision,note,created_by,created_at) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (
                        batch_id, decision_id, decision_row["analysis_id"], batch["revision"],
                        note, actor_id, self._now(),
                    ),
                )
                plan_id = cursor.lastrowid
                self.connection.executemany(
                    "INSERT INTO supplement_plan_items(plan_id,stratum_key,minimum_count) VALUES(?,?,?)",
                    [(plan_id, item["stratum_key"], item["minimum_count"]) for item in parsed_items],
                )
                self.connection.execute(
                    "UPDATE batches SET state='resampling' WHERE batch_id=? AND state='decided'",
                    (batch_id,),
                )
                self._audit(
                    "batch", batch_id, "supplement_plan.issued", actor_id,
                    {"plan_id": plan_id, "decision_id": decision_id, "items": parsed_items},
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("该决定已经签发过补样计划") from exc
        return {
            "plan_id": plan_id,
            "batch_id": batch_id,
            "decision_id": decision_id,
            "batch_revision": batch["revision"],
            "items": parsed_items,
        }

    def open_resampling_round(
        self, actor_id: str, batch_id: str, plan_id: int
    ) -> dict[str, Any]:
        """操作员依据补样计划开启关联原批次的补样轮次。"""

        self._require(actor_id, "round.open")
        plan = self.connection.execute(
            "SELECT * FROM supplement_plans WHERE plan_id=? AND batch_id=?", (plan_id, batch_id)
        ).fetchone()
        if plan is None:
            raise NotFound("补样计划不存在")
        batch = self.get_batch(batch_id)
        if batch["state"] != "resampling":
            raise InvalidState("批次不在补样状态，不能开启补样轮次")
        last_round = self.connection.execute(
            "SELECT round_number FROM resampling_rounds WHERE batch_id=? ORDER BY round_number DESC LIMIT 1",
            (batch_id,),
        ).fetchone()
        round_number = 1 if last_round is None else last_round["round_number"] + 1
        now = self._now()
        try:
            with transaction(self.connection, immediate=True):
                cursor = self.connection.execute(
                    "INSERT INTO resampling_rounds(batch_id,plan_id,round_number,state,opened_by,opened_at) "
                    "VALUES(?,?,?, 'running', ?,?)",
                    (batch_id, plan_id, round_number, actor_id, now),
                )
                round_id = cursor.lastrowid
                self._audit(
                    "batch", batch_id, "resampling_round.opened", actor_id,
                    {"round_id": round_id, "round_number": round_number, "plan_id": plan_id},
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("补样计划已开启过轮次，重复开启被阻止") from exc
        return {
            "round_id": round_id,
            "batch_id": batch_id,
            "plan_id": plan_id,
            "round_number": round_number,
            "state": "running",
        }

    def _round_included_counts(self, round_id: int) -> dict[str, int]:
        rows = self.connection.execute(
            "SELECT o.stratum_key AS stratum_key, count(*) AS total "
            "FROM observations o "
            "LEFT JOIN exclusion_requests e ON e.observation_id=o.observation_id AND e.status='approved' "
            "WHERE o.round_id=? AND e.observation_id IS NULL GROUP BY o.stratum_key",
            (round_id,),
        ).fetchall()
        return {row["stratum_key"]: row["total"] for row in rows}

    def seal_resampling_round(
        self, actor_id: str, batch_id: str, round_id: int
    ) -> dict[str, Any]:
        """封存补样轮次，生成新的批次修订并排队新分析。"""

        self._require(actor_id, "round.seal")
        round_row = self.connection.execute(
            "SELECT * FROM resampling_rounds WHERE round_id=? AND batch_id=?", (round_id, batch_id)
        ).fetchone()
        if round_row is None:
            raise NotFound("补样轮次不存在")
        if round_row["state"] != "running":
            raise InvalidState("补样轮次已经封存，计划完成后不能继续写入")
        batch = self.get_batch(batch_id)
        if batch["state"] != "resampling":
            raise InvalidState("批次不在补样状态")
        pending = self.connection.execute(
            "SELECT count(*) FROM exclusion_requests e JOIN observations o ON o.observation_id=e.observation_id "
            "WHERE o.batch_id=? AND e.status='pending'",
            (batch_id,),
        ).fetchone()[0]
        if pending:
            raise InvalidState("仍有待复核的排除申请")
        plan_items = {
            row["stratum_key"]: row["minimum_count"]
            for row in self.connection.execute(
                "SELECT stratum_key,minimum_count FROM supplement_plan_items WHERE plan_id=?",
                (round_row["plan_id"],),
            ).fetchall()
        }
        included = self._round_included_counts(round_id)
        missing = {
            key: {"required": minimum, "actual": included.get(key, 0)}
            for key, minimum in plan_items.items()
            if included.get(key, 0) < minimum
        }
        if missing:
            raise InvalidState(f"补样计划最低数量尚未满足：{canonical_json(missing)}")
        with transaction(self.connection, immediate=True):
            now = self._now()
            gate = self.connection.execute(
                "UPDATE resampling_rounds SET state='sealed' "
                "WHERE round_id=? AND state='running'",
                (round_id,),
            )
            if gate.rowcount != 1:
                raise InvalidState("补样轮次已经封存，重复封存被阻止")
            cursor = self.connection.execute(
                "UPDATE batches SET state='sealed',revision=revision+1,sealed_at=? "
                "WHERE batch_id=? AND state='resampling'",
                (now, batch_id),
            )
            if cursor.rowcount != 1:
                raise InvalidState("批次状态已变化")
            new_revision = batch["revision"] + 1
            self.connection.execute(
                "UPDATE resampling_rounds SET sealed_by=?,sealed_at=?,sealed_revision=? "
                "WHERE round_id=?",
                (actor_id, now, new_revision, round_id),
            )
            self.connection.execute(
                "INSERT INTO analysis_jobs(batch_id,batch_revision,state,available_at,created_at,updated_at) "
                "VALUES(?,?, 'queued', ?,?,?)",
                (batch_id, new_revision, now, now, now),
            )
            self._audit(
                "batch", batch_id, "resampling_round.sealed", actor_id,
                {"round_id": round_id, "revision": new_revision, "counts": included},
            )
        return self.get_batch(batch_id)

    def report(self, actor_id: str, batch_id: str) -> dict[str, Any]:
        user = self._user(actor_id)
        if user["role"] not in {"statistician", "approver", "auditor"}:
            raise Forbidden("当前角色不能读取完整报告")
        batch = self.get_batch(batch_id)
        protocol, protocol_digest = self._protocol(batch["protocol_id"], batch["protocol_version"])
        analysis_row = self.connection.execute(
            "SELECT * FROM analyses WHERE batch_id=? ORDER BY analysis_id DESC LIMIT 1", (batch_id,)
        ).fetchone()
        decision_row = None
        if analysis_row is not None:
            decision_row = self.connection.execute(
                "SELECT * FROM decisions WHERE analysis_id=?", (analysis_row["analysis_id"],)
            ).fetchone()
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

        revisions = []
        for revision_row in self.connection.execute(
            "SELECT a.analysis_id,a.batch_revision,a.input_sha256,a.algorithm_version,a.created_by,"
            "a.result_json,d.decision_id,d.decision,d.reason,d.decided_by,d.decided_at "
            "FROM analyses a LEFT JOIN decisions d ON d.analysis_id=a.analysis_id "
            "WHERE a.batch_id=? ORDER BY a.batch_revision,a.analysis_id",
            (batch_id,),
        ).fetchall():
            revisions.append({
                "analysis_id": revision_row["analysis_id"],
                "batch_revision": revision_row["batch_revision"],
                "input_sha256": revision_row["input_sha256"],
                "algorithm_version": revision_row["algorithm_version"],
                "created_by": revision_row["created_by"],
                "conclusion": json.loads(revision_row["result_json"])["conclusion"],
                "result": json.loads(revision_row["result_json"]),
                "decision": None if revision_row["decision_id"] is None else {
                    "decision_id": revision_row["decision_id"],
                    "decision": revision_row["decision"],
                    "reason": revision_row["reason"],
                    "decided_by": revision_row["decided_by"],
                    "decided_at": revision_row["decided_at"],
                },
            })

        rounds = []
        round_rows = self.connection.execute(
            "SELECT r.*,p.decision_id FROM resampling_rounds r "
            "JOIN supplement_plans p ON p.plan_id=r.plan_id "
            "WHERE r.batch_id=? ORDER BY r.round_number",
            (batch_id,),
        ).fetchall()
        for round_row in round_rows:
            plan_items = [
                {"stratum_key": item_row["stratum_key"], "minimum_count": item_row["minimum_count"]}
                for item_row in self.connection.execute(
                    "SELECT stratum_key,minimum_count FROM supplement_plan_items WHERE plan_id=? "
                    "ORDER BY stratum_key",
                    (round_row["plan_id"],),
                ).fetchall()
            ]
            count_rows = self.connection.execute(
                "SELECT o.stratum_key AS stratum_key,count(*) AS total,"
                "sum(CASE WHEN e.observation_id IS NULL THEN 1 ELSE 0 END) AS included "
                "FROM observations o LEFT JOIN exclusion_requests e "
                "ON e.observation_id=o.observation_id AND e.status='approved' "
                "WHERE o.round_id=? GROUP BY o.stratum_key ORDER BY o.stratum_key",
                (round_row["round_id"],),
            ).fetchall()
            increments = [
                {"stratum_key": row["stratum_key"], "count": row["total"], "included": row["included"]}
                for row in count_rows
            ]
            round_analysis = None
            if round_row["sealed_revision"] is not None:
                analysis = self.connection.execute(
                    "SELECT analysis_id,input_sha256,result_json FROM analyses "
                    "WHERE batch_id=? AND batch_revision=? ORDER BY analysis_id",
                    (batch_id, round_row["sealed_revision"]),
                ).fetchall()
                if analysis:
                    latest = analysis[-1]
                    decision = self.connection.execute(
                        "SELECT decision_id,decision,reason,decided_by,decided_at FROM decisions "
                        "WHERE analysis_id=?",
                        (latest["analysis_id"],),
                    ).fetchone()
                    round_analysis = {
                        "analysis_id": latest["analysis_id"],
                        "input_sha256": latest["input_sha256"],
                        "conclusion": json.loads(latest["result_json"])["conclusion"],
                        "decision": None if decision is None else dict(decision),
                    }
            rounds.append({
                "round_id": round_row["round_id"],
                "round_number": round_row["round_number"],
                "plan_id": round_row["plan_id"],
                "decision_id": round_row["decision_id"],
                "state": round_row["state"],
                "opened_by": round_row["opened_by"],
                "opened_at": round_row["opened_at"],
                "sealed_by": round_row["sealed_by"],
                "sealed_at": round_row["sealed_at"],
                "sealed_revision": round_row["sealed_revision"],
                "plan_items": plan_items,
                "increments": increments,
                "analysis": round_analysis,
            })

        return {
            "batch": batch,
            "protocol": {
                "protocol_id": protocol.protocol_id,
                "version": protocol.version,
                "sha256": protocol_digest,
                "seed": protocol.seed,
                "bootstrap_samples": protocol.bootstrap_samples,
            },
            "analysis": None if analysis_row is None else {
                "analysis_id": analysis_row["analysis_id"],
                "input_sha256": analysis_row["input_sha256"],
                "algorithm_version": analysis_row["algorithm_version"],
                "created_by": analysis_row["created_by"],
                "result": json.loads(analysis_row["result_json"]),
            },
            "decision": None if decision_row is None else dict(decision_row),
            "revisions": revisions,
            "rounds": rounds,
            "exclusions": [dict(row) for row in exclusions],
            "events": [dict(row) | {"payload": json.loads(row["payload_json"])} for row in events],
        }
