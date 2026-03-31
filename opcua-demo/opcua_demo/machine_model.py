import random
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional


@dataclass
class ActiveEvent:
    end_at: int
    label: str
    mode: str
    start_at: int


class AssetScenario:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        self.asset = config["asset"]
        self.runtime = config.get("runtime", {})
        self.signals = config["signals"]
        self.events: List[ActiveEvent] = [
            ActiveEvent(
                start_at=int(event["start_at"]),
                end_at=int(event["end_at"]),
                mode=event["mode"],
                label=event.get("label", event["mode"]),
            )
            for event in config["events"]
        ]
        seed = int(self.runtime.get("seed", 42))
        self.rng = random.Random(seed)
        self._state: Dict[str, Any] = {}
        self._initialize_state()

    @property
    def update_interval_seconds(self) -> float:
        return float(self.runtime.get("update_interval_seconds", 1.0))

    @property
    def initial_delay_seconds(self) -> float:
        return float(self.runtime.get("initial_delay_seconds", 0.0))

    def _initialize_state(self) -> None:
        for name, definition in self.signals.items():
            initial = definition.get("initial")
            if initial is None:
                initial = self._sample_value(name, definition, self.events[0].mode, 0)
            self._state[name] = initial

    def signal_items(self) -> Iterable:
        return self.signals.items()

    def active_event(self, elapsed_seconds: int) -> ActiveEvent:
        for event in self.events:
            if event.start_at <= elapsed_seconds <= event.end_at:
                return event
        return self.events[-1]

    def current_values(self) -> Dict[str, Any]:
        return dict(self._state)

    def next_step(self, elapsed_seconds: int) -> Dict[str, Any]:
        event = self.active_event(elapsed_seconds)
        phase_elapsed = max(0, elapsed_seconds - event.start_at)
        for name, definition in self.signals.items():
            self._state[name] = self._sample_value(name, definition, event.mode, phase_elapsed)
        return dict(self._state)

    def _sample_value(self, name: str, definition: Dict[str, Any], mode: str, phase_elapsed: int) -> Any:
        kind = definition.get("kind", "float")
        modes = definition.get("modes", {})
        mode_config = modes.get(mode) or modes.get("nominal", {})
        previous = self._state.get(name, definition.get("initial"))

        if kind == "counter":
            increment = float(mode_config.get("increment_mean", 1.0))
            increment += self.rng.uniform(-float(mode_config.get("increment_noise", 0.0)), float(mode_config.get("increment_noise", 0.0)))
            increment = max(0.0, increment)
            return int(float(previous or 0) + increment)

        if kind == "bool":
            probability = float(mode_config.get("true_probability", 0.5))
            return self.rng.random() < probability

        if kind == "string":
            choices = mode_config.get("choices") or definition.get("choices") or [definition.get("initial", "Unknown")]
            return self.rng.choice(choices)

        baseline = float(mode_config.get("baseline", definition.get("initial", 0.0)))
        drift_per_step = float(mode_config.get("drift_per_step", 0.0))
        noise = float(mode_config.get("noise", 0.0))
        value = baseline + drift_per_step * phase_elapsed + self.rng.uniform(-noise, noise)
        minimum = mode_config.get("min")
        maximum = mode_config.get("max")
        if minimum is not None:
            value = max(float(minimum), value)
        if maximum is not None:
            value = min(float(maximum), value)
        if kind == "int":
            return int(round(value))
        return round(value, int(definition.get("precision", 3)))
