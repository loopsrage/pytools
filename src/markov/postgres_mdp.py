"""
PostgreSQL JSONB-backed Markov Decision Process store.

Same interface/logic as `mongo_mdp.MongoMDP`, but persisted in PostgreSQL
using SQLAlchemy + JSONB columns. All async methods run blocking SQLAlchemy
calls in a worker thread via `asyncio.to_thread`.

Actions are dynamic: they emerge from `record()` calls and are discovered via
`distinct("action")`. There is no separate action registry.
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

from markov.postgres_markov import MarkovHistory

Base = declarative_base()


class MDPHistory(Base):
    __tablename__ = "mdp_history"
    id = Column(Integer, primary_key=True)
    from_ri = Column(JSONB, index=True, nullable=False)
    action = Column(JSONB, index=True, nullable=False)
    to_ri = Column(JSONB, index=True, nullable=False)
    transition_count = Column(Integer, default=0, nullable=False)
    total_reward = Column(Float, default=0.0, nullable=False)
    avg_duration_ms = Column(Float, default=0.0, nullable=False)
    last_seen = Column(Float, default=0.0, nullable=False)

    __table_args__ = (
        UniqueConstraint("from_ri", "action", "to_ri", name="uq_mdp_transition"),
    )


def _encode_state(state: Any) -> Any:
    if isinstance(state, tuple):
        return list(state)
    return state


def _decode_state(state: Any) -> Any:
    if isinstance(state, list):
        return tuple(state)
    return state


class PostgresMDP:
    _name: str = None
    _session: Session = None
    _gamma: float = None
    _collection_name: str = "MDP"
    _v_table = None
    _indices_initialized = False

    def __init__(self, name: str, session: Optional[Session] = None, gamma: float = 0.9):
        self._name = name
        self._session = session
        self._gamma = gamma

    @property
    def session(self) -> Session:
        if self._session is None:
            self._session = named_session(self._name)
        return self._session

    async def record(self, from_ri: Any, to_ri: Any, action: Any, reward: float = 0.0, duration_ms: float = 0.0):
        if not self._indices_initialized:
            await asyncio.to_thread(self._setup_markov_indices_sync)
            self._indices_initialized = True
        await asyncio.to_thread(self._record_sync, from_ri, to_ri, action, reward, duration_ms)

    def _record_sync(self, from_ri: Any, to_ri: Any, action: Any, reward: float, duration_ms: float):
        from_ri_e = _encode_state(from_ri)
        to_ri_e = _encode_state(to_ri)
        action_e = _encode_state(action)

        row = (
            self.session.execute(
                select(MDPHistory).where(
                    MDPHistory.from_ri == cast(from_ri_e, JSONB),
                    MDPHistory.action == cast(action_e, JSONB),
                    MDPHistory.to_ri == cast(to_ri_e, JSONB),
                )
            )
            .scalar_one_or_none()
        )

        if row is None:
            row = MDPHistory(
                from_ri=from_ri_e,
                action=action_e,
                to_ri=to_ri_e,
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
        rows = self.session.execute(select(MDPHistory)).scalars().all()

        groups = {}
        for row in rows:
            key = (_decode_state(row.from_ri), _decode_state(row.action))
            if key not in groups:
                groups[key] = {
                    "total_count": 0,
                    "outcomes": [],
                }
            groups[key]["total_count"] += row.transition_count
            groups[key]["outcomes"].append({
                "to_s": _decode_state(row.to_ri),
                "transition_count": row.transition_count,
                "total_reward": row.total_reward,
            })

        mdp_model = {}
        for (state, action), data in groups.items():
            if state not in mdp_model:
                mdp_model[state] = {}
            possible_next = []
            for outcome in data["outcomes"]:
                possible_next.append({
                    "to_s": outcome["to_s"],
                    "prob": outcome["transition_count"] / data["total_count"],
                    "reward": outcome["total_reward"] / outcome["transition_count"],
                })
            mdp_model[state][action] = possible_next
        return mdp_model

    async def call(self, callback, current: Any, last: Any, action: Any):
        start_time = time.perf_counter()
        success_reward = 0.0

        if last is not None:
            await self.record(last, current, action, reward=1.0)

        selected_ri, action_taken = await self.select_next(current, last)
        if selected_ri is None:
            return None

        try:
            success_reward = await callback(selected_ri, current)
        except Exception as e:
            success_reward = -1.0
            raise e
        finally:
            duration = (time.perf_counter() - start_time) * 1000
            await self.record(
                from_ri=current,
                action=action_taken,
                to_ri=selected_ri,
                reward=success_reward,
                duration_ms=duration,
            )

        return selected_ri

    async def select_next(self, current_ri: Any, last: Any, epsilon: float = 0.1):
        rows = await asyncio.to_thread(
            self._select_best_sync, _encode_state(current_ri)
        )

        if not rows or random.random() < epsilon:
            active_actions = await self.actions()
            if not active_actions:
                return last, "observed_behavior"

            selected_action = random.choice(active_actions)

            action_matches = [r for r in rows if _decode_state(r.action) == selected_action]
            if action_matches:
                return _decode_state(action_matches[0].to_ri), selected_action
            return last, selected_action

        self._v_table = await self.solve_policy()

        def get_v(row):
            target = _decode_state(row.to_ri)
            return self._v_table.get(target, 0)

        best_t = max(rows, key=get_v)
        return _decode_state(best_t.to_ri), _decode_state(best_t.action)

    def _select_best_sync(self, from_ri_e: Any):
        return (
            self.session.execute(
                select(MDPHistory)
                .where(MDPHistory.from_ri == cast(from_ri_e, JSONB))
                .order_by(MDPHistory.total_reward.desc())
                .limit(1)
            )
            .scalars()
            .all()
        )

    async def keys(self):
        return await asyncio.to_thread(self._keys_sync)

    def _keys_sync(self):
        rows = self.session.execute(select(MDPHistory.from_ri).distinct()).scalars().all()
        return [_decode_state(r) for r in rows]

    async def actions(self):
        return await asyncio.to_thread(self._actions_sync)

    def _actions_sync(self):
        rows = self.session.execute(select(MDPHistory.action).distinct()).scalars().all()
        return [_decode_state(r) for r in rows]

    async def setup_markov_indices(self):
        await asyncio.to_thread(self._setup_markov_indices_sync)

    def _setup_markov_indices_sync(self):
        bind = self.session.get_bind()
        Base.metadata.create_all(bind=bind, tables=[MDPHistory.__table__])

    async def build_transition_model(self):
        return await asyncio.to_thread(self._build_transition_model_sync)

    def _build_transition_model_sync(self):
        rows = self.session.execute(select(MDPHistory)).scalars().all()

        groups = {}
        for row in rows:
            key = (_decode_state(row.from_ri), _decode_state(row.action))
            if key not in groups:
                groups[key] = {
                    "total_count": 0,
                    "outcomes": [],
                }
            groups[key]["total_count"] += row.transition_count
            groups[key]["outcomes"].append({
                "to_ri": _decode_state(row.to_ri),
                "transition_count": row.transition_count,
                "total_reward": row.total_reward,
            })

        results = []
        for (state, action), data in groups.items():
            results.append({
                "_id": {"from": state, "act": action},
                "total_act_count": data["total_count"],
                "outcomes": data["outcomes"],
            })
        return results

    async def solve_policy(self, iterations=100, theta=0.001):
        return await asyncio.to_thread(self._solve_policy_sync, iterations, theta)

    def _solve_policy_sync(self, iterations, theta):
        states = self._keys_sync()
        val = {state: 0.0 for state in states}
        model = self._build_transition_model_sync()

        for i in range(iterations):
            delta = 0
            new_val = {state: -float("inf") for state in states}
            for entry in model:
                state = entry["_id"]["from"]
                action = entry["_id"]["act"]
                if state not in new_val:
                    new_val[state] = -float("inf")

                expected_val = 0.0
                for outcome in entry["outcomes"]:
                    prob = outcome["transition_count"] / entry["total_act_count"]
                    reward = outcome["total_reward"] / outcome["transition_count"]
                    expected_val += prob * (reward + self._gamma * val.get(outcome["to_ri"], 0))

                new_val[state] = max(new_val[state], expected_val)

            for state, v_new in new_val.items():
                v_old = val.get(state, 0.0)
                actual_v = v_new if v_new != -float("inf") else v_old
                new_val[state] = actual_v
                delta = max(delta, abs(actual_v - v_old))

            val.update(new_val)

            if delta < theta:
                break

        return val

    async def read_markov_chain(self, mc):
        await asyncio.to_thread(self._read_markov_chain_sync, mc)

    def _read_markov_chain_sync(self, mc):
        rows = mc.session.execute(select(MarkovHistory)).scalars().all()
        for row in rows:
            mdp_record = {
                "from_ri": _encode_state(_decode_state(row.from_ri)),
                "to_ri": _encode_state(_decode_state(row.to_ri)),
                "action": "observed_behavior",
                "transition_count": row.transition_count,
                "total_reward": row.total_reward,
                "avg_duration_ms": row.avg_duration_ms,
                "last_seen": row.last_seen,
            }

            existing = (
                self.session.execute(
                    select(MDPHistory).where(
                        MDPHistory.from_ri == cast(mdp_record["from_ri"], JSONB),
                        MDPHistory.action == cast(mdp_record["action"], JSONB),
                        MDPHistory.to_ri == cast(mdp_record["to_ri"], JSONB),
                    )
                )
                .scalar_one_or_none()
            )

            if existing is None:
                self.session.add(MDPHistory(**mdp_record))
            else:
                existing.transition_count = mdp_record["transition_count"]
                existing.total_reward = mdp_record["total_reward"]
                existing.avg_duration_ms = mdp_record["avg_duration_ms"]
                existing.last_seen = mdp_record["last_seen"]

        self.session.commit()
