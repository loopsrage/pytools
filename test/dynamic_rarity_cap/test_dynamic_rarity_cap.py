import unittest

from dynamic_rarity_cap.dynamic_rarity_cap import DynamicRarityCap


class TestDynamicRarityCap(unittest.TestCase):

    def test_rarity_cap_bounds(self):
        # For count=10000: cap = max(2, floor(20 - 5*4)) = 2
        cap = DynamicRarityCap({"common": 10000, "rare": 1})
        assert cap.check("common") is False  # count becomes 1
        assert cap.check("common") is False  # count becomes 2
        assert cap.check("common") is True   # over cap (>= 2)

        # For count=1: cap = max(2, floor(20 - 0)) = 20
        for _ in range(20):
            assert cap.check("rare") is False
        assert cap.check("rare") is True


if __name__ == "__main__":
    unittest.main()
