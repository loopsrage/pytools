import typing
from typing import Any

from pydantic import create_model, BaseModel, ConfigDict
from pydantic.fields import FieldInfo, Field

def dynamic_model(model_name, **fields):
    formatted_fields = {
        name: (str, f) if isinstance(f, FieldInfo) else f
        for name, f in fields.items()
    }
    model = create_model(
        model_name,
        **formatted_fields,
        __base__=BaseModel
    )
    return model

def reflect_type(type_str: str) -> Any:
    if not type_str:
        return str
    clean = type_str.strip().strip("'").strip('"').strip()
    safe_ns = {
        "str": str, "int": int, "float": float, "bool": bool,
        "List": typing.List, "Optional": typing.Optional, "Literal": typing.Literal,
        "Union": typing.Union, "Dict": typing.Dict, "Any": Any,
        "list": list, "dict": dict
    }

    try:
        return eval(clean, {"__builtins__": {}}, safe_ns)
    except Exception:
        return str

class DynamicModel(BaseModel):
    model_config = ConfigDict(extra="allow")

    @classmethod
    def model_validate(cls, obj: Any, *args, **kwargs):
        if cls.__name__ == "GeneratedModel":
            return super().model_validate(obj, *args, **kwargs)

        pydantic_fields = {}
        for field_name, config in obj.items():
            if not isinstance(config, dict): continue
            type_str = config.get("type", "str")
            python_type = reflect_type(type_str)

            field_info = Field(
                description=config.get("description", ""),
                pattern=config.get("pattern"),
                ge=config.get("ge"),
                le=config.get("le"),
            )
            pydantic_fields[field_name] = (python_type, field_info)

        model = create_model(
            "GeneratedModel",
            **pydantic_fields,
            __base__=BaseModel
        )
        return model
