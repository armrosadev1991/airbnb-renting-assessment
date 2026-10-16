"""Map each source's property type onto the shared segments, so comparisons are like-for-like.

Kamernet is mostly rooms and Airbnb mostly entire homes; comparing them without a common
segment would compare a room's rent with a whole apartment's revenue.
"""

from pyspark.sql import Column
from pyspark.sql import functions as F

from airbnb_renting.config import ROOM, WHOLE_UNIT

AIRBNB_SEGMENTS: dict[str, str] = {
    "Entire home/apt": WHOLE_UNIT,
    "Private room": ROOM,
    "Shared room": ROOM,
}

# 'Anti-squat' is deliberately absent: a caretaker fee, not a market rent.
KAMERNET_SEGMENTS: dict[str, str] = {
    "Apartment": WHOLE_UNIT,
    "Studio": WHOLE_UNIT,
    "Room": ROOM,
    "Student residence": ROOM,
}


def _map_values(col: Column, mapping: dict[str, str]) -> Column:
    """Exact-match lookup; anything unmapped becomes NULL (and fails `segment_known`)."""
    result = F.lit(None).cast("string")
    for source_value, segment in mapping.items():
        result = F.when(col == source_value, segment).otherwise(result)
    return result


def airbnb_segment(room_type: Column) -> Column:
    """Segment for an Airbnb `room_type`."""
    return _map_values(room_type, AIRBNB_SEGMENTS)


def kamernet_segment(property_type: Column) -> Column:
    """Segment for a Kamernet `propertyType`."""
    return _map_values(property_type, KAMERNET_SEGMENTS)
