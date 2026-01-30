import random
import time
from typing import Any

from pymongo import AsyncMongoClient

class MongoMarkov:

    _name: str  = None
    _collection_name: str = None
    _client: AsyncMongoClient = None

    def __init__(self, name: str, client: AsyncMongoClient):
        self._name = name
        self._client = client
        self.setup_markov_indices()

    @property
    def db(self):
        return self._client[self._name]

    @property
    def collection(self):
        return self.db[f"{self._collection_name}_markov_history"]

    async def record(self, from_ri: Any, to_ri: Any, reward: float = 0.0, duration_ms: float = 0.0):
        await self.collection.update_one(
            {"from_ri": from_ri, "to_ri": to_ri},
            [
                {
                    "$set": {
                        "transition_count": {"$add": [{"$ifNull": ["$transition_count", 0]}, 1]},
                        "total_reward": {"$add": [{"$ifNull": ["$total_reward", 0]}, reward]},
                        "last_seen": time.time()
                    }
                },
                {
                    "$set": {
                        "avg_duration_ms": {
                            "$divide": [
                                {"$add": [{"$multiply": [{"$ifNull": ["$avg_duration_ms", 0]}, {"$subtract": ["$transition_count", 1]}]}, duration_ms]},
                                "$transition_count"
                            ]
                        }
                    }
                }
            ],
            upsert=True
        )

    async def build(self):
        pipeline = [
            {
                "$group": {
                    "_id": "$from_ri",
                    "total_from_count": {"$sum": "$transition_count"},
                    "transitions": {"$push": "$$ROOT"}
                }
            },
            {"$unwind": "$transitions"},
            {
                "$project": {
                    "_id": 0,
                    "from_ri": "$_id",
                    "to_ri": "$transitions.to_ri",
                    "probability": {
                        "$divide": ["$transitions.transition_count", "$total_from_count"]
                    },
                    "expected_reward": {
                        "$divide": ["$transitions.total_reward", "$transitions.transition_count"]
                    },
                    "avg_duration": "$transitions.avg_duration_ms",
                    "last_seen": "$transitions.last_seen"
                }
            }
        ]

        markov_model = {}
        cursor = await self.collection.aggregate(pipeline)
        async for doc in cursor:
            f_ri = doc['from_ri']
            t_ri = doc['to_ri']

            if f_ri not in markov_model:
                markov_model[f_ri] = {}

            markov_model[f_ri][t_ri] = {
                "probability": doc['probability'],
                "expected_reward": doc['expected_reward'],
                "avg_duration": doc['avg_duration'],
                "last_seen": doc['last_seen']
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
        cursor = self.collection.find({"from_ri": current_ri})
        transitions = await cursor.to_list(length=None)

        if not transitions:
            return last

        states = [t["to_ri"] for t in transitions]
        weights = [t["transition_count"] for t in transitions]
        return random.choices(states, weights=weights, k=1)[0]

    async def keys(self):
        return await self.collection.distinct("from_ri")

    async def setup_markov_indices(self):
        await self.collection.create_index(
            [("from_ri", 1), ("to_ri", 1)],
            unique=True
        )

