from dataclasses import dataclass


ROOT_CAUSES = ("nominal", "asset_fault", "observability_degradation", "mixed", "observability_outage")


@dataclass(frozen=True)
class CorrelationOutcome:
    confidence: float
    hint: str


def infer_root_cause(
    ot_vote_ratio: float,
    exporter_up: bool,
    cpu_rate: float,
    memory_bytes: float,
    scrape_success: float,
    scrape_duration: float,
) -> CorrelationOutcome:
    if not exporter_up or scrape_success < 1.0:
        return CorrelationOutcome(confidence=0.2, hint="observability_outage")
    if ot_vote_ratio >= 0.6 and (cpu_rate >= 0.7 or memory_bytes >= 350_000_000 or scrape_duration >= 1.0):
        return CorrelationOutcome(confidence=0.55, hint="mixed")
    if ot_vote_ratio >= 0.6:
        return CorrelationOutcome(confidence=0.85, hint="asset_fault")
    if cpu_rate >= 0.7 or memory_bytes >= 350_000_000 or scrape_duration >= 1.0:
        return CorrelationOutcome(confidence=0.6, hint="observability_degradation")
    return CorrelationOutcome(confidence=0.95, hint="nominal")
