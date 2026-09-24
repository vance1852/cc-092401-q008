"""统计准入服务的 SQLite 模式与事务辅助。"""

from __future__ import annotations

import contextlib
import sqlite3
from collections.abc import Iterator
from pathlib import Path


SCHEMA_VERSION = 3

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS protocol_catalog (
    protocol_id TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version > 0),
    title TEXT NOT NULL,
    task_family TEXT NOT NULL,
    canonical_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL CHECK (length(content_sha256) = 64),
    created_at TEXT NOT NULL,
    PRIMARY KEY (protocol_id, version),
    UNIQUE (content_sha256)
);

CREATE TABLE IF NOT EXISTS users (
    user_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('operator', 'statistician', 'approver', 'auditor')),
    active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1))
);

CREATE TABLE IF NOT EXISTS robots (
    robot_id TEXT PRIMARY KEY,
    model_name TEXT NOT NULL,
    vendor TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS builds (
    build_id TEXT PRIMARY KEY,
    robot_id TEXT NOT NULL REFERENCES robots(robot_id),
    version TEXT NOT NULL,
    content_sha256 TEXT NOT NULL CHECK (length(content_sha256) = 64),
    created_at TEXT NOT NULL,
    UNIQUE (robot_id, version),
    UNIQUE (content_sha256)
);

CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    protocol_id TEXT NOT NULL,
    protocol_version INTEGER NOT NULL,
    build_id TEXT NOT NULL REFERENCES builds(build_id),
    state TEXT NOT NULL CHECK (state IN ('draft', 'running', 'sealed', 'analyzing', 'analyzed', 'decided', 'resampling')),
    revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_by TEXT NOT NULL REFERENCES users(user_id),
    created_at TEXT NOT NULL,
    started_at TEXT,
    sealed_at TEXT,
    FOREIGN KEY (protocol_id, protocol_version) REFERENCES protocol_catalog(protocol_id, version)
);

CREATE TABLE IF NOT EXISTS observations (
    observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL REFERENCES batches(batch_id),
    round_id INTEGER REFERENCES resampling_rounds(round_id),
    source_batch TEXT NOT NULL,
    source_row TEXT NOT NULL,
    robot_id TEXT NOT NULL REFERENCES robots(robot_id),
    stratum_key TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    metrics_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL CHECK (length(content_sha256) = 64),
    imported_by TEXT NOT NULL REFERENCES users(user_id),
    imported_at TEXT NOT NULL,
    UNIQUE (batch_id, source_batch, source_row)
);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    scope TEXT NOT NULL,
    key TEXT NOT NULL,
    request_sha256 TEXT NOT NULL CHECK (length(request_sha256) = 64),
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (scope, key)
);

CREATE TABLE IF NOT EXISTS exclusion_requests (
    exclusion_id INTEGER PRIMARY KEY AUTOINCREMENT,
    observation_id INTEGER NOT NULL REFERENCES observations(observation_id),
    status TEXT NOT NULL CHECK (status IN ('pending', 'approved', 'rejected', 'revoked')),
    reason TEXT NOT NULL,
    requested_by TEXT NOT NULL REFERENCES users(user_id),
    requested_at TEXT NOT NULL,
    reviewed_by TEXT REFERENCES users(user_id),
    reviewed_at TEXT,
    review_note TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS one_open_exclusion_per_observation
ON exclusion_requests(observation_id)
WHERE status IN ('pending', 'approved');

CREATE TABLE IF NOT EXISTS analysis_jobs (
    job_id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL REFERENCES batches(batch_id),
    batch_revision INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('queued', 'leased', 'succeeded', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    available_at TEXT NOT NULL,
    lease_owner TEXT,
    lease_expires_at TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (batch_id, batch_revision)
);

CREATE TABLE IF NOT EXISTS analyses (
    analysis_id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL REFERENCES batches(batch_id),
    batch_revision INTEGER NOT NULL,
    protocol_sha256 TEXT NOT NULL CHECK (length(protocol_sha256) = 64),
    input_sha256 TEXT NOT NULL CHECK (length(input_sha256) = 64),
    algorithm_version TEXT NOT NULL,
    seed INTEGER NOT NULL,
    result_json TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (batch_id, batch_revision, input_sha256)
);

CREATE TABLE IF NOT EXISTS decisions (
    decision_id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL REFERENCES batches(batch_id),
    analysis_id INTEGER NOT NULL REFERENCES analyses(analysis_id),
    decision TEXT NOT NULL CHECK (decision IN ('needs_more_data', 'approved', 'rejected')),
    reason TEXT NOT NULL,
    decided_by TEXT NOT NULL REFERENCES users(user_id),
    decided_at TEXT NOT NULL,
    UNIQUE (batch_id, analysis_id)
);

CREATE TABLE IF NOT EXISTS supplement_plans (
    plan_id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL REFERENCES batches(batch_id),
    decision_id INTEGER NOT NULL REFERENCES decisions(decision_id),
    analysis_id INTEGER NOT NULL REFERENCES analyses(analysis_id),
    batch_revision INTEGER NOT NULL,
    note TEXT,
    created_by TEXT NOT NULL REFERENCES users(user_id),
    created_at TEXT NOT NULL,
    UNIQUE (decision_id)
);

CREATE TABLE IF NOT EXISTS supplement_plan_items (
    plan_id INTEGER NOT NULL REFERENCES supplement_plans(plan_id),
    stratum_key TEXT NOT NULL,
    minimum_count INTEGER NOT NULL CHECK (minimum_count > 0),
    PRIMARY KEY (plan_id, stratum_key)
);

CREATE TABLE IF NOT EXISTS resampling_rounds (
    round_id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL REFERENCES batches(batch_id),
    plan_id INTEGER NOT NULL REFERENCES supplement_plans(plan_id),
    round_number INTEGER NOT NULL CHECK (round_number > 0),
    state TEXT NOT NULL CHECK (state IN ('running', 'sealed')),
    opened_by TEXT NOT NULL REFERENCES users(user_id),
    opened_at TEXT NOT NULL,
    sealed_by TEXT REFERENCES users(user_id),
    sealed_at TEXT,
    sealed_revision INTEGER,
    UNIQUE (batch_id, round_number),
    UNIQUE (plan_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS one_running_round_per_batch
ON resampling_rounds(batch_id)
WHERE state = 'running';

CREATE TABLE IF NOT EXISTS audit_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""

REQUIRED_TABLES = frozenset({
    "schema_meta", "protocol_catalog", "users", "robots", "builds", "batches",
    "observations", "idempotency_keys", "exclusion_requests", "analysis_jobs",
    "analyses", "decisions", "supplement_plans", "supplement_plan_items",
    "resampling_rounds", "audit_events",
})


def connect(path: str | Path) -> sqlite3.Connection:
    """打开连接并启用严格的事务与外键设置。"""

    connection = sqlite3.connect(str(path), isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 5000")
    return connection


@contextlib.contextmanager
def transaction(connection: sqlite3.Connection, *, immediate: bool = False) -> Iterator[None]:
    """显式事务；异常时保证回滚。"""

    connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
    try:
        yield
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()


def _column_names(connection: sqlite3.Connection, table: str) -> frozenset[str]:
    return frozenset(row["name"] for row in connection.execute(f"PRAGMA table_info({table})").fetchall())


BATCHES_V3_COLUMNS = (
    "batch_id", "protocol_id", "protocol_version", "build_id", "state", "revision",
    "created_by", "created_at", "started_at", "sealed_at", "root_batch_id", "round_number",
)


def _migrate_batches_to_v3(connection: sqlite3.Connection) -> None:
    """旧版 batches 缺少 resampling 状态与批次链字段，按 SQLite 推荐方式重建。"""

    foreign_keys = connection.execute("PRAGMA foreign_keys").fetchone()[0]
    try:
        connection.execute("PRAGMA foreign_keys = OFF")
        connection.execute("PRAGMA legacy_alter_table = ON")
        connection.execute("ALTER TABLE batches RENAME TO batches_v2")
        connection.execute(
            "CREATE TABLE batches ("
            "batch_id TEXT PRIMARY KEY, "
            "protocol_id TEXT NOT NULL, "
            "protocol_version INTEGER NOT NULL, "
            "build_id TEXT NOT NULL REFERENCES builds(build_id), "
            "state TEXT NOT NULL CHECK (state IN ('draft','running','sealed','analyzing','analyzed','decided','resampling')), "
            "revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0), "
            "created_by TEXT NOT NULL REFERENCES users(user_id), "
            "created_at TEXT NOT NULL, "
            "started_at TEXT, "
            "sealed_at TEXT, "
            "root_batch_id TEXT REFERENCES batches(batch_id), "
            "round_number INTEGER, "
            "FOREIGN KEY (protocol_id, protocol_version) REFERENCES protocol_catalog(protocol_id, version))"
        )
        connection.execute(
            "INSERT INTO batches(batch_id,protocol_id,protocol_version,build_id,state,revision,"
            "created_by,created_at,started_at,sealed_at,root_batch_id,round_number) "
            f"SELECT {', '.join(BATCHES_V3_COLUMNS[:10])}, NULL, NULL FROM batches_v2"
        )
        connection.execute("DROP TABLE batches_v2")
    finally:
        connection.execute("PRAGMA legacy_alter_table = OFF")
        connection.execute(f"PRAGMA foreign_keys = {'ON' if foreign_keys else 'OFF'}")


def initialize(connection: sqlite3.Connection) -> None:
    """初始化基础资料表，重复执行不改变已有数据。"""

    meta_exists = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_meta'"
    ).fetchone()
    version_row = None
    if meta_exists is not None:
        version_row = connection.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()
    legacy_version = None if version_row is None else int(version_row["value"])
    connection.executescript(SCHEMA_SQL)
    if legacy_version is not None and legacy_version < 3:
        _migrate_batches_to_v3(connection)
    if "root_batch_id" not in _column_names(connection, "batches"):
        connection.execute("ALTER TABLE batches ADD COLUMN root_batch_id TEXT REFERENCES batches(batch_id)")
    if "round_number" not in _column_names(connection, "batches"):
        connection.execute("ALTER TABLE batches ADD COLUMN round_number INTEGER")
    if "round_id" not in _column_names(connection, "observations"):
        connection.execute(
            "ALTER TABLE observations ADD COLUMN round_id INTEGER REFERENCES resampling_rounds(round_id)"
        )
    with transaction(connection, immediate=True):
        connection.execute(
            "INSERT INTO schema_meta(key, value) VALUES('schema_version', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(SCHEMA_VERSION),),
        )
        connection.execute("PRAGMA foreign_key_check")


def inspect_schema(connection: sqlite3.Connection) -> dict[str, object]:
    """返回适合机器检查的数据库结构摘要。"""

    table_rows = connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    tables = tuple(row["name"] for row in table_rows)
    version_row = connection.execute(
        "SELECT value FROM schema_meta WHERE key='schema_version'"
    ).fetchone()
    missing = sorted(REQUIRED_TABLES - set(tables))
    foreign_keys = connection.execute("PRAGMA foreign_keys").fetchone()[0]
    return {
        "tables": tables,
        "missing_tables": missing,
        "schema_version": None if version_row is None else version_row["value"],
        "foreign_keys": bool(foreign_keys),
    }
