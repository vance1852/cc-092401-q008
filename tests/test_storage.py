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

    def test_migrates_v2_database_preserving_rows(self) -> None:
        connection = sqlite3.connect(":memory:", isolation_level=None)
        connection.row_factory = sqlite3.Row
        try:
            connection.executescript(
                """
                CREATE TABLE schema_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                INSERT INTO schema_meta VALUES('schema_version', '2');
                CREATE TABLE protocol_catalog(
                    protocol_id TEXT NOT NULL, version INTEGER NOT NULL, title TEXT NOT NULL,
                    task_family TEXT NOT NULL, canonical_json TEXT NOT NULL,
                    content_sha256 TEXT NOT NULL, created_at TEXT NOT NULL,
                    PRIMARY KEY (protocol_id, version));
                CREATE TABLE users(
                    user_id TEXT PRIMARY KEY, display_name TEXT NOT NULL,
                    role TEXT NOT NULL, active INTEGER NOT NULL DEFAULT 1);
                CREATE TABLE robots(
                    robot_id TEXT PRIMARY KEY, model_name TEXT NOT NULL,
                    vendor TEXT NOT NULL, created_at TEXT NOT NULL);
                CREATE TABLE builds(
                    build_id TEXT PRIMARY KEY, robot_id TEXT NOT NULL REFERENCES robots(robot_id),
                    version TEXT NOT NULL, content_sha256 TEXT NOT NULL, created_at TEXT NOT NULL);
                CREATE TABLE batches(
                    batch_id TEXT PRIMARY KEY, protocol_id TEXT NOT NULL,
                    protocol_version INTEGER NOT NULL, build_id TEXT NOT NULL REFERENCES builds(build_id),
                    state TEXT NOT NULL CHECK (state IN ('draft','running','sealed','analyzing','analyzed','decided')),
                    revision INTEGER NOT NULL DEFAULT 1, created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL, started_at TEXT, sealed_at TEXT);
                INSERT INTO users VALUES('u1', '操作员', 'operator', 1);
                INSERT INTO robots VALUES('r1', '型号', '厂商', 't0');
                INSERT INTO builds VALUES('b1', 'r1', '1.0', 'aabb', 't0');
                INSERT INTO protocol_catalog VALUES('p1', 1, '标题', '家族', '{}', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 't0');
                INSERT INTO batches VALUES('batch-1', 'p1', 1, 'b1', 'decided', 3, 'u1', 't0', 't0', 't0');
                """
            )
            initialize(connection)
            summary = inspect_schema(connection)
            row = connection.execute("SELECT state,revision FROM batches WHERE batch_id='batch-1'").fetchone()
        finally:
            connection.close()
        self.assertEqual(summary["schema_version"], "3")
        self.assertEqual(summary["missing_tables"], [])
        self.assertEqual(tuple(row), ("decided", 3))

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


if __name__ == "__main__":
    unittest.main()
