from __future__ import annotations

import unittest
from pathlib import Path

from robot_trials.acceptance import run


ROOT = Path(__file__).resolve().parents[1]


class AcceptanceTests(unittest.TestCase):
    def test_offline_acceptance(self) -> None:
        result = run(ROOT)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["initial_observation_count"], 5)
        self.assertEqual(result["supplement_observation_count"], 1)
        self.assertEqual(result["schema"]["missing_tables"], [])
        self.assertEqual(result["initial_conclusion"], "insufficient")
        self.assertEqual(result["supplement_conclusion"], "pass")
        self.assertEqual(result["final_decision"], "approved")
        self.assertEqual(len(result["initial_input_sha256"]), 64)
        self.assertEqual(len(result["supplement_input_sha256"]), 64)
        self.assertNotEqual(result["initial_input_sha256"], result["supplement_input_sha256"])


if __name__ == "__main__":
    unittest.main()
