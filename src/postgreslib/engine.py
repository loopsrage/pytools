import logging
from typing import Any, Dict

from pydantic_settings import BaseSettings
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

from settings.helper import unmarshal_app_settings
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)

Base = declarative_base()

class PostgresEngineSettings(BaseSettings):
    url: str = "postgresql://localhost:5432/"
    echo: bool = False
    pool_size: int = 10
    max_overflow: int = 20
    pool_timeout: int = 10

_engines: Dict[str, Any] = {}
_factories: Dict[str, sessionmaker] = {}

def named_session(name: str):
    global _engines, _factories

    if name not in _engines:
        config = unmarshal_app_settings(name, PostgresEngineSettings)
        kwargs = config.model_dump()
        if config.url.startswith("sqlite"):
            kwargs = {"url": config.url, "echo": config.echo}
        engine = create_engine(**kwargs)
        _engines[name] = engine
        _factories[name] = sessionmaker(bind=engine, expire_on_commit=False)

    return _factories[name]()