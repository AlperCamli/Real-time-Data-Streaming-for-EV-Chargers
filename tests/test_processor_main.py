from __future__ import annotations

import unittest

from src.processor.main import _should_record_session_mutation_skip


class ProcessorMainHelperTests(unittest.TestCase):
    def test_expected_non_mutating_events_are_not_counted_as_skips(self) -> None:
        self.assertFalse(_should_record_session_mutation_skip("no_session_mutation_for_event_type"))

    def test_unexpected_session_mutation_failures_are_counted(self) -> None:
        self.assertTrue(_should_record_session_mutation_skip("session_not_found"))


if __name__ == "__main__":
    unittest.main()
