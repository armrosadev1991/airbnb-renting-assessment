"""Silver data-quality rules: one dict per table, shared by the valid and the quarantine table.

Defining each rule once means `expect_all_or_drop(RULES)` and the quarantine filter can't drift
apart. Each rule checks ONE thing (e.g. a NULL price fails only `price_parsed`, not also
`price_in_bounds`), so `failed_rules` gives one reason per cause.
"""

from pyspark.sql import Column
from pyspark.sql import functions as F

from airbnb_renting.config import AIRBNB_PRICE_BOUNDS_EUR, RENT_BOUNDS_EUR

_PRICE_LO, _PRICE_HI = AIRBNB_PRICE_BOUNDS_EUR
_RENT_LO, _RENT_HI = RENT_BOUNDS_EUR

AIRBNB_RULES: dict[str, str] = {
    "coordinates_parsed": "latitude IS NOT NULL AND longitude IS NOT NULL",
    "pc4_not_null": "pc4 IS NOT NULL",
    "pc4_in_amsterdam": "pc4 IS NULL OR pc4_is_amsterdam",
    "price_parsed": "price_per_night_eur IS NOT NULL",
    "price_in_bounds": (
        f"price_per_night_eur IS NULL OR price_per_night_eur BETWEEN {_PRICE_LO} AND {_PRICE_HI}"
    ),
    "segment_known": "segment IS NOT NULL",
}

RENTALS_RULES: dict[str, str] = {
    "pc4_not_null": "pc4 IS NOT NULL",
    "pc4_in_amsterdam": "pc4 IS NULL OR pc4_is_amsterdam",
    "rent_parsed": "rent_eur IS NOT NULL",
    "rent_in_bounds": f"rent_eur IS NULL OR rent_eur BETWEEN {_RENT_LO} AND {_RENT_HI}",
    "segment_known": "segment IS NOT NULL",
}

# Warn-only: logged as expectation metrics, rows are kept (not needed for revenue).
AIRBNB_WARN_RULES: dict[str, str] = {
    "bedrooms_present": "bedrooms IS NOT NULL",
    "review_score_present": "review_scores_value IS NOT NULL",
}
RENTALS_WARN_RULES: dict[str, str] = {
    "area_present": "area_sqm IS NOT NULL",
}


def null_safe(rules: dict[str, str]) -> dict[str, str]:
    """Make every rule evaluate to TRUE or FALSE, never NULL.

    SQL's three-valued logic: `NULL > 10` is NULL. An expectation drops a row whose rule is not
    TRUE, while the quarantine filter `NOT rule` doesn't select NULL either, so without this the
    row would vanish from both tables without any error.
    """
    return {name: f"coalesce(({condition}), false)" for name, condition in rules.items()}


def all_rules_pass(rules: dict[str, str]) -> Column:
    """TRUE when the row passes every rule (the quarantine table keeps the rows where it's FALSE)."""
    return F.expr(" AND ".join(f"({condition})" for condition in null_safe(rules).values()))


def failed_rules(rules: dict[str, str]) -> Column:
    """ARRAY<STRING> of the names of the rules this row fails, in dict order; empty if none."""
    # One slot per rule: the rule's name if it fails, NULL if it passes. `null_safe` guarantees
    # each condition is TRUE/FALSE, so `~condition` is never NULL and no failure is missed.
    slots = [
        F.when(~F.expr(condition), F.lit(name)) for name, condition in null_safe(rules).items()
    ]
    # array_compact drops the NULL slots; dict order is preserved, and a clean row gives [].
    return F.array_compact(F.array(*slots))
