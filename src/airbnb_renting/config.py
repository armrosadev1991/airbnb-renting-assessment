"""Business constants and the Airbnb occupancy scenarios.

Every number here was calibrated on the real data in `scratch/01` and `scratch/02`; changing one
is a business decision, so it lives in one place instead of being scattered through the code.
"""

import json
from dataclasses import dataclass

WHOLE_UNIT = "whole_unit"
ROOM = "room"
SEGMENTS: tuple[str, ...] = (WHOLE_UNIT, ROOM)

NIGHTS_PER_MONTH = 30
LOW_SAMPLE_THRESHOLD = 5

# Quarantine bounds: values outside them are treated as data errors, not as outliers.
AIRBNB_PRICE_BOUNDS_EUR = (10, 5000)  # 3 rows above: e.g. €9,000/night for 2 guests, no reviews
RENT_BOUNDS_EUR = (100, 10000)  # 4 rows below: three €1 rents, one €50


def require_conf(key: str, value: str | None) -> str:
    """Fail fast on a missing pipeline setting instead of building names like 'None.airbnb_raw'."""
    if value is None or not value.strip():
        msg = (
            f"Pipeline configuration '{key}' is missing (see resources/airbnb_renting.pipeline.yml)"
        )
        raise KeyError(msg)
    return value


@dataclass(frozen=True)
class Scenario:
    """An Airbnb occupancy assumption: either a fraction of nights or a legal yearly cap."""

    name: str
    occupancy: float | None = None
    nights_per_year: int | None = None
    segments: tuple[str, ...] = SEGMENTS

    def __post_init__(self) -> None:
        """Reject ambiguous or impossible scenarios at load time rather than as odd revenue."""
        if (self.occupancy is None) == (self.nights_per_year is None):
            msg = f"Scenario {self.name!r} needs exactly one of occupancy or nights_per_year"
            raise ValueError(msg)
        if self.occupancy is not None and not 0 < self.occupancy <= 1:
            msg = f"Scenario {self.name!r}: occupancy must be in (0, 1], got {self.occupancy}"
            raise ValueError(msg)
        if self.nights_per_year is not None and not 0 < self.nights_per_year <= 365:
            msg = f"Scenario {self.name!r}: nights_per_year must be in 1..365"
            raise ValueError(msg)
        unknown = set(self.segments) - set(SEGMENTS)
        if unknown:
            msg = f"Scenario {self.name!r}: unknown segments {sorted(unknown)}"
            raise ValueError(msg)

    def applies_to(self, segment: str) -> bool:
        """Whether this scenario is meaningful for a segment (the 30-night cap is whole-unit only)."""
        return segment in self.segments


def parse_scenarios(raw: str) -> list[Scenario]:
    """Build scenarios from the JSON string set in the pipeline configuration."""
    items = json.loads(raw)
    scenarios = [
        Scenario(
            name=item["name"],
            occupancy=item.get("occupancy"),
            nights_per_year=item.get("nights_per_year"),
            segments=tuple(item.get("segments", SEGMENTS)),
        )
        for item in items
    ]
    names = [s.name for s in scenarios]
    if len(names) != len(set(names)):
        msg = f"Scenario names must be unique, got {names}"
        raise ValueError(msg)
    return scenarios
