import unittest

from rapidfuzzlib.rapidfuzzlib import fuzz_one_against_choices


class TestRapidFuzzLib(unittest.TestCase):

    def test_fuzz_one_against_choices(self):
        choices = ["apple pie", "banana bread", "cherry tart"]
        scores = fuzz_one_against_choices("apple", choices, threshold=50.0)
        assert len(scores) == 3
        assert scores["apple pie"] >= 90.0
        assert scores["banana bread"] == 0.0


if __name__ == "__main__":
    unittest.main()
