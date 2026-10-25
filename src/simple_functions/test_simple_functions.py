import uuid
import unittest

from simple_functions.simple_functions import uuid5_from_args


class TestSimpleFunctions(unittest.TestCase):

    def test_uuid5_from_args(self):
        ns = str(uuid.NAMESPACE_DNS)
        u1 = uuid5_from_args(ns, "part1", 2, "part3")
        u2 = uuid5_from_args(ns, "part1", 2, "part3")
        u3 = uuid5_from_args(ns, "part1", 3, "part3")
        assert u1 == u2
        assert u1 != u3
        assert str(uuid.UUID(u1)) == u1


if __name__ == "__main__":
    unittest.main()
