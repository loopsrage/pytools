import logging
from typing import Dict

from pydantic_settings import BaseSettings
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

from settings.helper import unmarshal_app_settings
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)

Base = declarative_base()

_engine = None
_SessionFactory = None

class PostgresEngineSettings(BaseSettings):
    url: str = "postgresql://localhost:5432/"
    echo: bool = False
    pool_size: int = 10
    max_overflow: int = 20
    pool_timeout: int = 10

def named_session(name: str):
    global _engine, _SessionFactory

    config = unmarshal_app_settings(name, PostgresEngineSettings)
    if _engine is None:
        _engine = create_engine(**config.model_dump())
        _SessionFactory = sessionmaker(bind=_engine)

    return _SessionFactory()


_engines: Dict[str, any] = {}
_factories: Dict[str, sessionmaker] = {}

def named_session(name: str):
    global _engines, _factories

    if name not in _engines:
        config = unmarshal_app_settings(name, PostgresEngineSettings)
        engine = create_engine(**config.model_dump())
        _engines[name] = engine
        _factories[name] = sessionmaker(bind=engine)

    return _factories[name]()