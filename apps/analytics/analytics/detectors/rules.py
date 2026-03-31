import re
from dataclasses import dataclass
from typing import Dict, Iterable, Tuple


@dataclass(frozen=True)
class RuleOutcome:
    flag: bool
    score: float
    severity: str


class RuleDetector:
    def __init__(self, rules: Iterable[Dict[str, float]]) -> None:
        self.rules = list(rules)

    def evaluate(self, signal_name: str, value: float) -> RuleOutcome:
        for rule in self.rules:
            if not re.search(rule["pattern"], signal_name, re.IGNORECASE):
                continue

            if "critical_high" in rule and value >= float(rule["critical_high"]):
                return RuleOutcome(True, 1.0, "critical")
            if "critical_low" in rule and value <= float(rule["critical_low"]):
                return RuleOutcome(True, 1.0, "critical")
            if "warning_high" in rule and value >= float(rule["warning_high"]):
                return RuleOutcome(True, 0.6, "warning")
            if "warning_low" in rule and value <= float(rule["warning_low"]):
                return RuleOutcome(True, 0.6, "warning")
            return RuleOutcome(False, 0.0, "nominal")
        return RuleOutcome(False, 0.0, "nominal")
