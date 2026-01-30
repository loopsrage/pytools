import asyncio
import traceback
import unittest

import numpy as np
import pyautogui
from pymongo import AsyncMongoClient

from src.markov.markov import Markov
from src.markov.mongo_markov import MongoMarkov
from src.markov.mongo_mdp import MongoMDP


def round_mp():
    return pyautogui.position()

async def norm_mag(nxt, current):
    nxt = np.array(nxt)
    current = np.array(current)
    dist = np.linalg.norm(nxt - current)
    # Invert sigmoid so that dist=0 gives reward=1.0, and dist=large gives reward=0.0
    reward = 1.0 - sigmoid(dist - 5) # -5 shifts the curve for better sensitivity
    print(f"Next: {nxt}, Curr: {current}, Dist: {dist:.2f}, Score: {reward:.4f}")
    return float(reward)

def sigmoid(x):
    return 1 / (1 + np.exp(-x))

class Test(unittest.IsolatedAsyncioTestCase):
    _mk = None
    def setUp(self):
        self._mk = Markov()

    async def test_mouse_pred(self):
        last = (0, 0)
        while True:
            current = round_mp()
            if last == current:
                continue

            last = current
            keys = {i.from_key: i.to_key for i in self._mk.history.all()} or {pyautogui.Point(x=0, y=0): pyautogui.Point(x=0, y=0)}
            await self._mk.call(keys, norm_mag, current)


class TestMongo(unittest.IsolatedAsyncioTestCase):
    _mk = None
    def setUp(self):
        client = AsyncMongoClient("mongodb://localhost:27017")
        self._mk = MongoMarkov("mouse_test", client)

    async def test_mongo_mouse_pred(self):
        last = None
        try:
            while True:
                current = round_mp()
                if last == current:
                    continue

                await self._mk.call(norm_mag, current, last)
                last = current
        except Exception as e:
            print(str(e))

class TestMongoMDP(unittest.IsolatedAsyncioTestCase):
    _mk = None
    def setUp(self):
        client = AsyncMongoClient("mongodb://localhost:27017")
        self._mk = MongoMDP("mdp_test", client, .1)

    async def test_stuff(self):
        last = None
        try:
            while True:
                current = round_mp()
                if last is None or last == current:
                    continue

                action = get_action_name(last, current)
                curr_coords = (current.x, current.y)
                last_coords = (last.x, last.y) if last else None

                await self._mk.call(
                    callback=norm_mag,
                    current=curr_coords,
                    last=last_coords,
                    action=action
                )

                last = current
                await asyncio.sleep(0.01)

        except Exception as e:
            traceback.print_exception(e)
            print(f"Error in MDP Loop: {e}")

def get_action_name(last, current):
    """Calculates a string-based action based on direction."""
    dx = current.x - last.x
    dy = current.y - last.y

    if abs(dx) > abs(dy):
        return "MOVE_RIGHT" if dx > 0 else "MOVE_LEFT"
    else:
        return "MOVE_DOWN" if dy > 0 else "MOVE_UP"