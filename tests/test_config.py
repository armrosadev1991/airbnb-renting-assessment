"""Tests for scenario configuration (pure Python, no Spark needed)."""

import pytest

from airbnb_renting.config import ROOM, WHOLE_UNIT, Scenario, parse_scenarios, require_conf

PIPELINE_DEFAULT = (
    '[{"name":"legal_cap_30n","nights_per_year":30,"segments":["whole_unit"]},'
    '{"name":"occ_50","occupancy":0.5},{"name":"occ_70","occupancy":0.7}]'
)


def test_parse_pipeline_default() -> None:
    """The exact JSON in the pipeline YAML must load; a typo there would only fail at runtime."""
    scenarios = parse_scenarios(PIPELINE_DEFAULT)
    assert [s.name for s in scenarios] == ["legal_cap_30n", "occ_50", "occ_70"]
    assert scenarios[0].nights_per_year == 30
    assert scenarios[1].occupancy == 0.5


def test_cap_applies_to_whole_units_only() -> None:
    """Protects A-004: the 30-night cap is a whole-home rule; rooms (B&B) have no night cap."""
    cap, occ_50, _ = parse_scenarios(PIPELINE_DEFAULT)
    assert cap.applies_to(WHOLE_UNIT)
    assert not cap.applies_to(ROOM)
    assert occ_50.applies_to(ROOM)


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"occupancy": 0.5, "nights_per_year": 30}, "exactly one"),
        ({}, "exactly one"),
        ({"occupancy": 1.5}, "occupancy must be"),
        ({"occupancy": 0}, "occupancy must be"),
        ({"nights_per_year": 400}, "nights_per_year must be"),
        ({"occupancy": 0.5, "segments": ("castle",)}, "unknown segments"),
    ],
)
def test_invalid_scenarios_rejected(kwargs: dict, message: str) -> None:
    """A bad config must fail loudly at load time, not produce plausible-looking wrong revenue."""
    with pytest.raises(ValueError, match=message):
        Scenario(name="bad", **kwargs)


def test_duplicate_names_rejected() -> None:
    """Scenario name is part of the gold grain; duplicates would double rows per PC4."""
    with pytest.raises(ValueError, match="unique"):
        parse_scenarios('[{"name":"a","occupancy":0.5},{"name":"a","occupancy":0.7}]')


def test_require_conf() -> None:
    """A missing setting must fail with its name, not silently become the string 'None'."""
    assert require_conf("bronze_schema", "bronze") == "bronze"
    for missing in (None, "", "  "):
        with pytest.raises(KeyError, match="bronze_schema"):
            require_conf("bronze_schema", missing)
