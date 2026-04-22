import random
import time
from typing import Any

from indexes.specialist_index.test_specialist_index import AsyncMongoClient
from markov.mongo_markov import MongoMarkov


class MongoMDP:

    _name: str  = None
    _gamma: float = None
    _collection_name: str = None
    _client: AsyncMongoClient = None
    _v_table = None

    def __init__(self, name: str, client: AsyncMongoClient, gamma: float):
        self._name = name
        self._client = client
        self._gamma = gamma
        self._collection_name = "MDP"
        self.setup_markov_indices()

    @property
    def db(self):
        return self._client[self._name]

    @property
    def collection(self):
        return self.db[f"{self._collection_name}_mdp_history"]

    async def record(self, from_ri: Any, to_ri: Any, action: Any, reward: float = 0.0, duration_ms: float = 0.0):
        await self.collection.update_one(
            {"from_ri": list(from_ri), "action": action, "to_ri": list(to_ri)},
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
                    "_id": {"from": "$from_ri", "act": "$action"},
                    "total_act_count": {"$sum": "$transition_count"},
                    "outcomes": {"$push": "$$ROOT"}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "from_s": "$_id.from",
                    "action": "$_id.act",
                    "possible_next": {
                        "$map": {
                            "input": "$outcomes",
                            "as": "o",
                            "in": {
                                "to_s": "$$o.to_ri",
                                "prob": {"$divide": ["$$o.transition_count", "$total_act_count"]},
                                "reward": {"$divide": ["$$o.total_reward", "$$o.transition_count"]}
                            }
                        }
                    }
                }
            }
        ]

        mdp_model = {}
        cursor = await self.collection.aggregate(pipeline)
        async for doc in cursor:
            s, a = doc['from_s'], doc['action']
            if s not in mdp_model: mdp_model[s] = {}
            mdp_model[s][a] = doc['possible_next']

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
                duration_ms=duration
            )

        return selected_ri

    async def select_next(self, current_ri: Any, last: Any, epsilon: float = 0.1):
        cursor = self.collection.find({"from_ri": current_ri}).sort([("total_reward", -1)]).limit(1)
        best_transitions = await cursor.to_list(length=1)
        if not best_transitions or random.random() < epsilon:

            active_actions = await self.actions()
            if not active_actions:
                return last, "observed_behavior"

            selected_action = random.choice(active_actions)

            # If we've seen this action before, return its common destination
            # Otherwise, return current_ri to "stay" while we try the action
            action_matches = [t for t in best_transitions if t["action"] == selected_action]
            return (random.choice(action_matches)["to_ri"] if action_matches
                    else last, selected_action)

        self._v_table = await self.solve_policy() # In production, cache this!
        def get_v(transition):
            target = transition["to_ri"]
            target_tuple = tuple(target) if isinstance(target, list) else target
            return self._v_table.get(target_tuple, 0)

        best_t = max(best_transitions, key=get_v)
        return best_t["to_ri"], best_t["action"]

    async def keys(self):
        return await self.collection.distinct("from_ri")

    async def actions(self):
        return await self.collection.distinct("action")

    async def setup_markov_indices(self):
        indices = [
            IndexModel(
                [("from_ri", 1), ("action", 1), ("to_ri", 1)],
                unique=True,
                name="idx_mdp_transition"
            ),
            IndexModel(
                [("from_ri", 1), ("total_reward", -1)],
                name="idx_reward_lookup"
            )
        ]
        await self.collection.create_indexes(indices)

    async def build_transition_model(self):
        pipeline = [
            {
                "$group": {
                    "_id": {"from": "$from_ri", "act": "$action"},
                    "total_act_count": {"$sum": "$transition_count"},
                    "outcomes": {"$push": "$$ROOT"}
                }
            }
        ]
        cursor = await self.collection.aggregate(pipeline)
        results = await cursor.to_list(length=None)

        # Normalize everything to tuples before returning to solve_policy
        for entry in results:
            entry["_id"]["from"] = tuple(entry["_id"]["from"])
            for outcome in entry["outcomes"]:
                outcome["to_ri"] = tuple(outcome["to_ri"])

        return results

    async def solve_policy(self, iterations=100, theta=0.001):
        states = await self.keys()
        val = {state: 0.0 for state in states}
        model = await self.build_transition_model()

        for i in range(iterations):
            delta = 0
            new_val = {state: -float('inf') for state in states}
            for entry in model:
                state = entry["_id"]["from"]
                action = entry["_id"]["act"]
                if state not in new_val:
                    new_val[state] = -float('inf')

                # Expected value for this specific action
                expected_val = 0
                for outcome in entry["outcomes"]:
                    prob = outcome["transition_count"] / entry["total_act_count"]
                    reward = outcome["total_reward"] / outcome["transition_count"]
                    expected_val += prob * (reward + self._gamma * val.get(outcome["to_ri"], 0))

                new_val[state] = max(new_val[state], expected_val)

            for state, v_new in new_val.items():
                v_old = val.get(state, 0.0)
                actual_v = v_new if v_new != -float('inf') else v_old
                new_val[state] = actual_v

                delta = max(delta, abs(actual_v - v_old))

            val.update(new_val)

            if delta < theta:
                print(f"MDP Converged early at iteration {i+1} (Delta: {delta:.6f})")
                break

        return val

    async def read_markov_chain(self, mc: MongoMarkov):
        cursor = self.collection.find({})
        async for doc in cursor:
            # 2. Transform into MDP triplet: (State, Action, Next State)
            # We use 'observed_behavior' as the baseline action label
            mdp_record = {
                "from_ri": doc["from_ri"],
                "to_ri": doc["to_ri"],
                "action": "observed_behavior",
                "transition_count": doc.get("transition_count", 1),
                "total_reward": doc.get("total_reward", 0.0),
                "avg_duration_ms": doc.get("avg_duration_ms", 0.0),
                "last_seen": doc.get("last_seen")
            }

            # 3. Upsert into the MDP collection
            await self.collection.update_one(
                {
                    "from_ri": mdp_record["from_ri"],
                    "action": mdp_record["action"],
                    "to_ri": mdp_record["to_ri"]
                },
                {"$set": mdp_record},
                upsert=True
            )
