from typing import Optional

from pydantic_settings import SettingsConfigDict, BaseSettings

class Example(BaseSettings):
    key: str
    value: str

class Example2(BaseSettings):
    key: str
    value: str

class Example3(BaseSettings):
    key: str
    value: str

class Example4(BaseSettings):
    key: str
    value: str

class Level1(BaseSettings):
    model_config = SettingsConfigDict(arbitrary_types_allowed=True)
    test: str = "nested_layer"

class Level2(BaseSettings):
    model_config = SettingsConfigDict(arbitrary_types_allowed=True)
    test: str = "nested_layer1"
    nest: Level1 = Level1()

class Level3(BaseSettings):
    model_config = SettingsConfigDict(arbitrary_types_allowed=True)
    test: str = "nested_layer2"
    nest: Level2 = Level2()

class NestedSettings(BaseSettings):
    model_config = SettingsConfigDict(arbitrary_types_allowed=True)
    test: str = "not nested"
    nest: Level3 = Level3()


class AppSettings(BaseSettings):
    ex1: Optional[Example] = None
    ex2: Optional[Example2] = None
    ex3: Optional[Example3] = None
    ex4: Optional[Example4] = None
    nested: Optional[NestedSettings] = None
