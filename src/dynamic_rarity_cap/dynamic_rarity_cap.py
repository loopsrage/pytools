import math
from abc import ABC, abstractmethod


class RarityCap(ABC):
    @abstractmethod
    def check(self, truth):
        pass

class DynamicRarityCap(RarityCap):
    _truths = None
    _rarity_map = None

    def __init__(self, rmap):
        self._truths = {}
        self._rarity_map = rmap

    def check(self, truth) -> bool:
        try:
            total_in_data = self._rarity_map.get(str(truth), 1)
            dynamic_cap = max(2, math.floor(20 - (math.log10(total_in_data) * 5)))
            current = self._truths.get(truth, 0)
            over_cap = current >= dynamic_cap
            if over_cap:
                return over_cap

            self._truths[truth] = current + 1
        except Exception as e:
            if truth in self._truths:
                self._truths[truth] -= 1
            raise e
        finally:
            pass
