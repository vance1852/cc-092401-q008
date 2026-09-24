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
    state TEXT NOT NULL CHECK (state IN ('draft', 'running', 'sealed', 'analyzing', 'analyzed', 'decided')),
    revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_by TEXT NOT NULL REFERENCES users(user_id),
    created_at TEXT NOT NULL,
    started_at TEXT,
    sealed_at TEXT,
    current_round_seq INTEGER NOT NULL DEFAULT 0 CHECK (current_round_seq >= 0),
    FOREIGN KEY (protocol_id, protocol_version) REFERENCES protocol_catalog(protocol_id, version)
);

CREATE TABLE IF NOT EXISTS observations (
    observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL REFERENCES batches(batch_id),
    round_seq INTEGER NOT NULL DEFAULT 0 CHECK (round_seq >= 0),
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
    round_seq INTEGER NOT NULL DEFAULT 0 CHECK (round_seq >= 0),
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
    round_seq INTEGER NOT NULL DEFAULT 0 CHECK (round_seq >= 0),
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
    round_seq INTEGER NOT NULL DEFAULT 0 CHECK (round_seq >= 0),
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
    round_seq INTEGER NOT NULL CHECK (round_seq > 0),
    note TEXT,
    created_by TEXT NOT NULL REFERENCES users(user_id),
    created_at TEXT NOT NULL,
    UNIQUE (batch_id, round_seq)
);

CREATE TABLE IF NOT EXISTS supplement_plan_items (
    plan_id INTEGER NOT NULL REFERENCES supplement_plans(plan_id),
    stratum_key TEXT NOT NULL,
    minimum_count INTEGER NOT NULL CHECK (minimum_count > 0),
    PRIMARY KEY (plan_id, stratum_key)
);

CREATE TABLE IF NOT EXISTS supplement_rounds (
    batch_id TEXT NOT NULL REFERENCES batches(batch_id),
    round_seq INTEGER NOT NULL CHECK (round_seq > 0),
    plan_id INTEGER NOT NULL REFERENCES supplement_plans(plan_id),
    batch_revision_opened INTEGER NOT NULL,
    batch_revision_sealed INTEGER,
    state TEXT NOT NULL CHECK (state IN ('open', 'sealed')),
    opened_by TEXT NOT NULL REFERENCES users(user_id),
    opened_at TEXT NOT NULL,
    sealed_by TEXT REFERENCES users(user_id),
    sealed_at TEXT,
    PRIMARY KEY (batch_id, round_seq),
    UNIQUE (plan_id)
);

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
    "supplement_rounds", "audit_events",
})

# 旧版数据库上需要补齐的列：表名 -> (列名, 列定义)。
_ADDED_COLUMNS = (
    ("batches", "current_round_seq", "INTEGER NOT NULL DEFAULT 0"),
    ("observations", "round_seq", "INTEGER NOT NULL DEFAULT 0"),
    ("analyses", "round_seq", "INTEGER NOT NULL DEFAULT 0"),
    ("decisions", "round_seq", "INTEGER NOT NULL DEFAULT 0"),
    ("analysis_jobs", "round_seq", "INTEGER NOT NULL DEFAULT 0"),
)


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


def initialize(connection: sqlite3.Connection) -> None:
    """初始化基础资料表，重复执行不改变已有数据。"""

    connection.executescript(SCHEMA_SQL)
    # 对旧版数据库做仅追加列的幂等迁移，保留全部历史数据。
    with transaction(connection, immediate=True):
        for table, column, definition in _ADDED_COLUMNS:
            existing = {
                row["name"]
                for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
            }
            if column not in existing:
                connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        connection.execute(
            "CREATE INDEX IF NOT EXISTS observations_batch_round "
            "ON observations(batch_id, round_seq)"
        )
        connection.execute(
            "INSERT INTO schema_meta(key, value) VALUES('schema_version', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(SCHEMA_VERSION),),
        )


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
