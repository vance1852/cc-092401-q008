from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from robot_trials.storage import connect, initialize, inspect_schema, transaction


class StorageTests(unittest.TestCase):
    def test_initialize_is_repeatable(self) -> None:
        connection = sqlite3.connect(":memory:", isolation_level=None)
        connection.row_factory = sqlite3.Row
        try:
            initialize(connection)
            initialize(connection)
            summary = inspect_schema(connection)
        finally:
            connection.close()
        self.assertEqual(summary["missing_tables"], [])
        self.assertEqual(summary["schema_version"], "3")

    def test_transaction_rolls_back_on_error(self) -> None:
        connection = sqlite3.connect(":memory:", isolation_level=None)
        connection.execute("CREATE TABLE items(value TEXT NOT NULL)")
        with self.assertRaises(RuntimeError):
            with transaction(connection):
                connection.execute("INSERT INTO items(value) VALUES('x')")
                raise RuntimeError("stop")
        count = connection.execute("SELECT count(*) FROM items").fetchone()[0]
        connection.close()
        self.assertEqual(count, 0)

    def test_connect_enables_foreign_keys(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            connection = connect(Path(directory) / "test.sqlite3")
            try:
                self.assertEqual(connection.execute("PRAGMA foreign_keys").fetchone()[0], 1)
            finally:
                connection.close()

    def test_migrates_v2_database_without_losing_data(self) -> None:
        connection = sqlite3.connect(":memory:", isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        # 模拟 v2 时代的最小旧结构：batches/observations 不含任何轮次列。
        connection.executescript(
            """
            CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE users (
                user_id TEXT PRIMARY KEY, display_name TEXT NOT NULL,
                role TEXT NOT NULL CHECK (role IN ('operator','statistician','approver','auditor')),
                active INTEGER NOT NULL DEFAULT 1
            );
            CREATE TABLE robots (robot_id TEXT PRIMARY KEY, model_name TEXT NOT NULL,
                vendor TEXT NOT NULL, created_at TEXT NOT NULL);
            CREATE TABLE builds (build_id TEXT PRIMARY KEY, robot_id TEXT NOT NULL,
                version TEXT NOT NULL, content_sha256 TEXT NOT NULL, created_at TEXT NOT NULL);
            CREATE TABLE protocol_catalog (
                protocol_id TEXT NOT NULL, version INTEGER NOT NULL, title TEXT NOT NULL,
                task_family TEXT NOT NULL, canonical_json TEXT NOT NULL,
                content_sha256 TEXT NOT NULL, created_at TEXT NOT NULL,
                PRIMARY KEY (protocol_id, version));
            CREATE TABLE batches (
                batch_id TEXT PRIMARY KEY, protocol_id TEXT NOT NULL,
                protocol_version INTEGER NOT NULL, build_id TEXT NOT NULL,
                state TEXT NOT NULL, revision INTEGER NOT NULL DEFAULT 1,
                created_by TEXT NOT NULL, created_at TEXT NOT NULL,
                started_at TEXT, sealed_at TEXT,
                FOREIGN KEY (protocol_id, protocol_version)
                    REFERENCES protocol_catalog(protocol_id, version));
            CREATE TABLE observations (
                observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id TEXT NOT NULL, source_batch TEXT NOT NULL, source_row TEXT NOT NULL,
                robot_id TEXT NOT NULL, stratum_key TEXT NOT NULL, observed_at TEXT NOT NULL,
                metrics_json TEXT NOT NULL, content_sha256 TEXT NOT NULL,
                imported_by TEXT NOT NULL, imported_at TEXT NOT NULL,
                UNIQUE (batch_id, source_batch, source_row));
            INSERT INTO schema_meta(key,value) VALUES('schema_version','2');
            INSERT INTO users(user_id,display_name,role) VALUES('u1','操作员','operator');
            INSERT INTO robots(robot_id,model_name,vendor,created_at) VALUES('r1','m','v','t');
            INSERT INTO builds(build_id,robot_id,version,content_sha256,created_at)
                VALUES('b1','r1','1','a','t');
            INSERT INTO protocol_catalog(protocol_id,version,title,task_family,
                canonical_json,content_sha256,created_at)
                VALUES('p',1,'t','f','{}','c','t');
            INSERT INTO batches(batch_id,protocol_id,protocol_version,build_id,state,
                revision,created_by,created_at) VALUES('x','p',1,'b1','decided',3,'u1','t');
            """
        )
        try:
            initialize(connection)
            summary = inspect_schema(connection)
            self.assertEqual(summary["schema_version"], "3")
            self.assertEqual(summary["missing_tables"], [])
            batch = dict(connection.execute("SELECT * FROM batches").fetchone())
            self.assertEqual(batch["state"], "decided")
            self.assertEqual(batch["revision"], 3)
            self.assertEqual(batch["current_round_seq"], 0)
            columns = {row["name"] for row in connection.execute("PRAGMA table_info(observations)")}
            self.assertIn("round_seq", columns)
        finally:
            connection.close()


if __name__ == "__main__":
    unittest.main()
