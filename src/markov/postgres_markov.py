"""
PostgreSQL JSONB-backed Markov chain store.

Same interface/logic as `mongo_markov.MongoMarkov`, but persisted in PostgreSQL
using SQLAlchemy + JSONB columns. All async methods run blocking SQLAlchemy
calls in a worker thread via `asyncio.to_thread`.
"""

from __future__ import annotations

import asyncio
import random
import time
from typing import Any, Optional

from sqlalchemy import (
    Column,
    Float,
    Integer,
    UniqueConstraint,
    cast,
    select,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, Session

from postgreslib.engine import named_session

Base = declarative_base()


class MarkovHistory(Base):
    __tablename__ = "markov_history"
    id = Column(Integer, primary_key=True)
    from_ri = Column(JSONB, index=True, nullable=False)
    to_ri = Column(JSONB, index=True, nullable=False)
    transition_count = Column(Integer, default=0, nullable=False)
    total_reward = Column(Float, default=0.0, nullable=False)
    avg_duration_ms = Column(Float, default=0.0, nullable=False)
    last_seen = Column(Float, default=0.0, nullable=False)

    __table_args__ = (
        UniqueConstraint("from_ri", "to_ri", name="uq_markov_transition"),
    )


def _encode_state(state: Any) -> Any:
    """JSONB does not distinguish tuples from lists; normalize tuples to lists."""
    if isinstance(state, tuple):
        return list(state)
    return state


def _decode_state(state: Any) -> Any:
    """Return stored lists back as tuples so callers get tuple keys."""
    if isinstance(state, list):
        return tuple(state)
    return state


class PostgresMarkov:
    _name: str = None
    _session: Session = None
    _indices_initialized = False

    def __init__(self, name: str, session: Optional[Session] = None):
        self._name = name
        self._session = session

    @property
    def session(self) -> Session:
        if self._session is None:
            self._session = named_session(self._name)
        return self._session

    async def record(self, from_ri: Any, to_ri: Any, reward: float = 0.0, duration_ms: float = 0.0):
        if not self._indices_initialized:
            await asyncio.to_thread(self._setup_markov_indices_sync)
            self._indices_initialized = True
        await asyncio.to_thread(self._record_sync, from_ri, to_ri, reward, duration_ms)

    def _record_sync(self, from_ri: Any, to_ri: Any, reward: float, duration_ms: float):
        from_ri_e = cast(_encode_state(from_ri), JSONB)
        to_ri_e = cast(_encode_state(to_ri), JSONB)

        row = (
            self.session.execute(
                select(MarkovHistory).where(
                    MarkovHistory.from_ri == from_ri_e,
                    MarkovHistory.to_ri == to_ri_e,
                )
            )
            .scalar_one_or_none()
        )

        if row is None:
            row = MarkovHistory(
                from_ri=_encode_state(from_ri),
                to_ri=_encode_state(to_ri),
                transition_count=0,
                total_reward=0.0,
                avg_duration_ms=0.0,
                last_seen=0.0,
            )
            self.session.add(row)

        row.transition_count += 1
        row.total_reward += reward
        row.last_seen = time.time()
        row.avg_duration_ms = (
            row.avg_duration_ms * (row.transition_count - 1) + duration_ms
        ) / row.transition_count
        self.session.commit()

    async def build(self):
        return await asyncio.to_thread(self._build_sync)

    def _build_sync(self):
        rows = self.session.execute(select(MarkovHistory)).scalars().all()

        counts = {}
        metadata = {}
        for row in rows:
            state = _decode_state(row.from_ri)
            next_state = _decode_state(row.to_ri)

            if state not in counts:
                counts[state] = {}
                metadata[state] = {}

            counts[state][next_state] = counts[state].get(next_state, 0) + row.transition_count
            prev = metadata[state].get(next_state, {"reward": 0.0, "duration": 0.0})
            metadata[state][next_state] = {
                "reward": prev["reward"] + row.total_reward,
                "duration": prev["duration"] + row.avg_duration_ms * row.transition_count,
            }

        markov_model = {}
        for state, transitions in counts.items():
            total_from_state = sum(transitions.values())
            markov_model[state] = {}
            for next_state, count in transitions.items():
                markov_model[state][next_state] = {
                    "probability": count / total_from_state,
                    "expected_reward": metadata[state][next_state]["reward"] / count,
                    "avg_duration": metadata[state][next_state]["duration"] / count,
                    "last_seen": None,
                }
        return markov_model

    async def call(self, callback, current: Any, last: Any):
        if last is not None and last != current:
            await self.record(last, current, reward=1.0)

        start_time = time.perf_counter()
        success_reward = 0.0

        selected_ri = await self.select_next(current, last)
        if selected_ri is None:
            return None

        try:
            success_reward = await callback(selected_ri, current)
        except Exception as e:
            success_reward = -1.0
            raise e
        finally:
            duration = (time.perf_counter() - start_time) * 1000
            await self.record(current, selected_ri, success_reward, duration)

        return selected_ri

    async def select_next(self, current_ri: Any, last: Any):
        rows = await asyncio.to_thread(
            self._select_rows_sync, _encode_state(current_ri)
        )

        if not rows:
            return last

        states = [_decode_state(r.to_ri) for r in rows]
        weights = [r.transition_count for r in rows]
        return random.choices(states, weights=weights, k=1)[0]

    def _select_rows_sync(self, from_ri_e: Any):
        return (
            self.session.execute(
                select(MarkovHistory).where(
                    MarkovHistory.from_ri == cast(from_ri_e, JSONB)
                )
            )
            .scalars()
            .all()
        )

    async def keys(self):
        return await asyncio.to_thread(self._keys_sync)

    def _keys_sync(self):
        rows = self.session.execute(select(MarkovHistory.from_ri).distinct()).scalars().all()
        return [_decode_state(r) for r in rows]

    async def setup_markov_indices(self):
        await asyncio.to_thread(self._setup_markov_indices_sync)

    def _setup_markov_indices_sync(self):
        bind = self.session.get_bind()
        Base.metadata.create_all(bind=bind, tables=[MarkovHistory.__table__])
