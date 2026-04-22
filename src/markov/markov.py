import json
import random
import time
from collections import defaultdict
from typing import List, Any

from pydantic import BaseModel

from thread_safe.tslist import TsList


class MarkovData(BaseModel):
    from_key: Any
    to_key: Any
    total_reward: float
    average_duration: float

class MarkovEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, TsList):
            return obj.all()
        if isinstance(obj, Markov):
            return {"history": obj.history}
        if hasattr(obj, 'dict'):
            return obj.dict()
        return super().default(obj)

class Markov:
    history: TsList

    def __init__(self):
        self.history = TsList()

    def record_many(self, markov_data: List[MarkovData]):
        self.history.add(markov_data)

    def record(self, from_ri: Any, to_ri: Any, reward: float = 0.0, duration_ms: float = 0.0):
        self.history.append(MarkovData(
            from_key=from_ri,
            to_key=to_ri,
            total_reward=reward,
            average_duration=duration_ms,
        ))

    def select_next(self, source: dict, current_ri: Any):
        model = self.build()
        options = model.get(current_ri, {})
        if not options:
            selected_ri = random.choice(list(source.keys()))
        else:
            states = list(options.keys())
            weights = [data["probability"] for data in options.values()]

            selected_ri = random.choices(states, weights=weights, k=1)[0]

        next_agent = source.get(selected_ri)
        return next_agent, selected_ri

    async def call(self, source: dict, callback, current: Any):
        start_time = time.perf_counter()
        success_reward = 0.0

        next_agent, selected_ri = self.select_next(source, current)
        try:
            success_reward = await callback(next_agent, selected_ri)
        except Exception as e:
            success_reward = -1.0
            raise e
        finally:
            duration = (time.perf_counter() - start_time) * 1000
            self.record(current, selected_ri, success_reward, duration)
        return selected_ri

    def build(self):
        counts = defaultdict(lambda: defaultdict(int))
        metadata = defaultdict(lambda: defaultdict(lambda: {"reward": 0.0, "duration": 0.0}))

        hist = self.history.all()
        for i in range(len(hist) - 1):
            curr, nxt = hist[i], hist[i+1]
            state, next_state = curr.to_key, nxt.to_key

            counts[state][next_state] += 1
            metadata[state][next_state]["reward"] += nxt.total_reward
            metadata[state][next_state]["duration"] += nxt.average_duration

        markov_model = {}
        for state, transitions in counts.items():
            total_from_state = sum(transitions.values())
            markov_model[state] = {}

            for next_state, count in transitions.items():
                markov_model[state][next_state] = {
                    "probability": count / total_from_state,
                    "avg_reward": metadata[state][next_state]["reward"] / count,
                    "avg_duration": metadata[state][next_state]["duration"] / count
                }
        return markov_model
