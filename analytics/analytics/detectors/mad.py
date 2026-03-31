from collections import deque
from dataclasses import dataclass
from statistics import median
from typing import Deque, Dict, Hashable


@dataclass(frozen=True)
class MADOutcome:
    flag: bool
    score: float


class RollingMADDetector:
    def __init__(self, window_size: int = 20, threshold: float = 3.5, min_history: int = 5) -> None:
        self.window_size = window_size
        self.threshold = threshold
        self.min_history = min_history
        self.history: Dict[Hashable, Deque[float]] = {}

    def observe(self, key: Hashable, value: float) -> MADOutcome:
        bucket = self.history.setdefault(key, deque(maxlen=self.window_size))
        if len(bucket) < self.min_history:
            bucket.append(value)
            return MADOutcome(False, 0.0)

        center = median(bucket)
        mad = median([abs(sample - center) for sample in bucket])
        if mad == 0:
            bucket.append(value)
            return MADOutcome(False, 0.0)

        score = abs(value - center) / (1.4826 * mad)
        bucket.append(value)
        return MADOutcome(score >= self.threshold, score)
