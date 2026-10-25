import json
import unittest
from collections import namedtuple
import numpy as np

from markov.markov import Markov, MarkovData, MarkovEncoder

Point = namedtuple("Point", ["x", "y"])


async def norm_mag(nxt, current):
    nxt = np.array(nxt)
    current = np.array(current)
    dist = np.linalg.norm(nxt - current)
    reward = 1.0 - sigmoid(dist - 5)
    return float(reward)


def sigmoid(x):
    return 1 / (1 + np.exp(-x))


def get_action_name(last, current):
    """Calculates a string-based action based on direction."""
    dx = current.x - last.x
    dy = current.y - last.y

    if abs(dx) > abs(dy):
        return "MOVE_RIGHT" if dx > 0 else "MOVE_LEFT"
    else:
        return "MOVE_DOWN" if dy > 0 else "MOVE_UP"


class TestMarkov(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self._mk = Markov()

    async def test_markov_chain_and_encoder(self):
        self._mk.record_many(
            [
                MarkovData(from_key="A", to_key="B", total_reward=1.0, average_duration=5.0),
                MarkovData(from_key="B", to_key="C", total_reward=2.0, average_duration=10.0),
                MarkovData(from_key="C", to_key="B", total_reward=1.5, average_duration=8.0),
                MarkovData(from_key="B", to_key="C", total_reward=4.0, average_duration=12.0),
            ]
        )
        model = self._mk.build()
        assert "B" in model and "C" in model["B"]
        assert model["B"]["C"]["probability"] == 1.0
        assert model["B"]["C"]["avg_reward"] == 3.0
        assert model["B"]["C"]["avg_duration"] == 11.0

        source = {"A": (0, 0), "B": (1, 1), "C": (2, 2)}
        next_agent, selected_ri = self._mk.select_next(source, "B")
        assert selected_ri == "C" and next_agent == (2, 2)

        called_ri = await self._mk.call(source, lambda agent, ri: norm_mag(agent, (2, 2)), "B")
        assert called_ri == "C"
        assert len(self._mk.history) == 5

        encoded = json.dumps(self._mk, cls=MarkovEncoder)
        parsed = json.loads(encoded)
        assert len(parsed["history"]) == 5

    def test_direction_and_reward_helpers(self):
        assert get_action_name(Point(0, 0), Point(5, 1)) == "MOVE_RIGHT"
        assert get_action_name(Point(5, 1), Point(0, 0)) == "MOVE_LEFT"
        assert get_action_name(Point(0, 0), Point(1, 5)) == "MOVE_DOWN"
        assert get_action_name(Point(1, 5), Point(0, 0)) == "MOVE_UP"
        assert abs(sigmoid(0) - 0.5) < 1e-6


if __name__ == "__main__":
    unittest.main()
