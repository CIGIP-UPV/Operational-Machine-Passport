from collections import deque
from dataclasses import dataclass
from statistics import mean, pstdev
from typing import Deque, Dict, Hashable


@dataclass(frozen=True)
class ZScoreOutcome:
    flag: bool
    score: float


class RollingZScoreDetector:
    def __init__(self, window_size: int = 20, threshold: float = 3.0, min_history: int = 5) -> None:
        self.window_size = window_size
        self.threshold = threshold
        self.min_history = min_history
        self.history: Dict[Hashable, Deque[float]] = {}

    def observe(self, key: Hashable, value: float) -> ZScoreOutcome:
        bucket = self.history.setdefault(key, deque(maxlen=self.window_size))
        if len(bucket) < self.min_history:
            bucket.append(value)
            return ZScoreOutcome(False, 0.0)

        center = mean(bucket)
        deviation = pstdev(bucket)
        score = 0.0 if deviation == 0 else abs((value - center) / deviation)
        bucket.append(value)
        return ZScoreOutcome(score >= self.threshold, score)
