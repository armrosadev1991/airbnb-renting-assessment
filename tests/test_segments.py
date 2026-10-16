"""Tests for the property-type to segment mapping (AT-009)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airbnb_renting.segments import airbnb_segment, kamernet_segment

if TYPE_CHECKING:
    from tests.conftest import Evaluate


def test_airbnb_segment(evaluate: Evaluate) -> None:
    """A wrong mapping compares rooms with apartments, which is the bias the segments exist to fix."""
    inputs = ["Entire home/apt", "Private room", "Shared room", "Hotel room", None]
    assert evaluate(airbnb_segment, inputs) == ["whole_unit", "room", "room", None, None]


def test_kamernet_segment(evaluate: Evaluate) -> None:
    """Anti-squat must map to NULL so it is quarantined (not a market rent), not silently kept."""
    inputs = ["Apartment", "Studio", "Room", "Student residence", "Anti-squat", None]
    assert evaluate(kamernet_segment, inputs) == [
        "whole_unit",
        "whole_unit",
        "room",
        "room",
        None,
        None,
    ]
