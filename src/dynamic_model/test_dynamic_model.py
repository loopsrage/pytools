import unittest
from pydantic import Field, ValidationError

from dynamic_model.dynamic_model import DynamicModel, dynamic_model, reflect_type


class TestDynamicModel(unittest.TestCase):

    def test_reflect_type_and_dynamic_model(self):
        assert reflect_type("int") is int
        assert reflect_type("float") is float
        assert reflect_type("") is str
        assert reflect_type("invalid_syntax(((") is str

        M1 = dynamic_model("CustomModel", name=Field(default="bob"), count=(int, 5))
        inst = M1()
        assert inst.name == "bob" and inst.count == 5

        Generated = DynamicModel.model_validate(
            {
                "age": {"type": "int", "description": "User age", "ge": 0, "le": 120},
                "code": {"type": "str", "pattern": r"^[A-Z]{3}$"},
            }
        )
        valid_obj = Generated(age=30, code="ABC")
        assert valid_obj.age == 30 and valid_obj.code == "ABC"

        with self.assertRaises(ValidationError):
            Generated(age=-5, code="ABC")
        with self.assertRaises(ValidationError):
            Generated(age=30, code="123")


if __name__ == "__main__":
    unittest.main()
