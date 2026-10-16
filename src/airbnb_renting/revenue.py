"""Gold-layer revenue model (DESIGN Decision 11): gross monthly EUR, both sources, long format.

Long format (one row per listing x scenario) means adding a scenario is a config change, not a
schema change. Medians, not means: Airbnb prices are heavily skewed (p99 €1,260, max €5,000).
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from airbnb_renting.config import Scenario

AIRBNB = "airbnb"
KAMERNET = "kamernet"
LONG_TERM = "long_term"


def scenarios_column(scenarios: list[Scenario]) -> Column:
    """The scenarios as a literal ARRAY<STRUCT>, to explode per row (no cross join, no Spark handle)."""
    return F.array(
        *[
            F.struct(
                F.lit(s.name).alias("name"),
                F.lit(s.occupancy).cast("double").alias("occupancy"),
                F.lit(s.nights_per_year).cast("int").alias("nights_per_year"),
                F.array(*[F.lit(seg) for seg in s.segments]).alias("segments"),
            )
            for s in scenarios
        ]
    )


def airbnb_monthly_revenue(
    price: Column, occupancy: Column, nights_per_year: Column, nights_per_month: int
) -> Column:
    """Gross monthly revenue: capped scenarios use nights/year / 12, others price x nights x occupancy."""
    capped = price * nights_per_year / F.lit(12)
    by_occupancy = price * F.lit(nights_per_month) * occupancy
    return F.round(F.when(nights_per_year.isNotNull(), capped).otherwise(by_occupancy), 2)


def revenue_per_property(
    airbnb: DataFrame, rentals: DataFrame, scenarios: list[Scenario], nights_per_month: int
) -> DataFrame:
    """One row per listing x applicable scenario (Airbnb) or per listing (Kamernet: long_term)."""
    airbnb_rows = (
        airbnb.select(
            "listing_id",
            "pc4",
            "segment",
            "price_per_night_eur",
            F.explode(scenarios_column(scenarios)).alias("s"),
        )
        # The 30-night cap is a whole-home rule; it doesn't apply to rooms (B&B).
        .where(F.array_contains("s.segments", F.col("segment")))
        .select(
            F.lit(AIRBNB).alias("source"),
            "listing_id",
            "pc4",
            "segment",
            F.col("s.name").alias("scenario"),
            F.col("price_per_night_eur").cast("double").alias("price_per_night_eur"),
            airbnb_monthly_revenue(
                F.col("price_per_night_eur").cast("double"),
                F.col("s.occupancy"),
                F.col("s.nights_per_year"),
                nights_per_month,
            ).alias("monthly_revenue_eur"),
        )
    )
    kamernet_rows = rentals.select(
        F.lit(KAMERNET).alias("source"),
        "listing_id",
        "pc4",
        "segment",
        F.lit(LONG_TERM).alias("scenario"),
        F.lit(None).cast("double").alias("price_per_night_eur"),
        F.col("rent_eur").cast("double").alias("monthly_revenue_eur"),
    )
    return airbnb_rows.unionByName(kamernet_rows)


def revenue_per_postcode(per_property: DataFrame) -> DataFrame:
    """Median/average monthly revenue per (source, PC4, segment, scenario)."""
    return per_property.groupBy("source", "pc4", "segment", "scenario").agg(
        F.round(F.median("monthly_revenue_eur"), 2).alias("median_monthly_revenue_eur"),
        F.round(F.avg("monthly_revenue_eur"), 2).alias("avg_monthly_revenue_eur"),
        F.round(F.median("price_per_night_eur"), 2).alias("median_price_per_night_eur"),
        F.count("*").alias("n_listings"),
    )


def breakeven_occupancy(
    kamernet_median_rent: Column, airbnb_median_price: Column, nights_per_month: int
) -> Column:
    """Occupancy at which Airbnb's gross monthly revenue equals the long-term rent.

    Rounded to 3 decimals. Above 1.0 means Airbnb can't match the rent even when fully booked.
    """
    # rent = price x nights x occupancy  =>  occupancy = rent / (price x nights).
    # NULL on either side propagates through the arithmetic, so a one-sided PC4 gets NULL
    # without an explicit `when` (AT-016). Prices are >= €10 after silver, so no division by 0.
    full_occupancy_revenue = airbnb_median_price * F.lit(nights_per_month)
    return F.round(kamernet_median_rent / full_occupancy_revenue, 3)


def investment_comparison(
    per_postcode: DataFrame,
    postcodes: DataFrame,
    nights_per_month: int,
    low_sample_threshold: int,
) -> DataFrame:
    """Kamernet vs Airbnb per (PC4, segment, scenario), with breakeven and confidence flags.

    Full outer join: a PC4 with listings on only one side stays visible (winner NULL), so the
    investor sees where data is missing instead of the postcode silently disappearing.
    """
    kamernet = per_postcode.where(F.col("source") == KAMERNET).select(
        "pc4",
        "segment",
        F.col("median_monthly_revenue_eur").alias("kamernet_median_rent_eur"),
        F.col("n_listings").alias("n_kamernet"),
    )
    airbnb = per_postcode.where(F.col("source") == AIRBNB).select(
        "pc4",
        "segment",
        "scenario",
        F.col("median_monthly_revenue_eur").alias("airbnb_median_revenue_eur"),
        F.col("median_price_per_night_eur").alias("airbnb_median_price_eur"),
        F.col("n_listings").alias("n_airbnb"),
    )
    breakeven = breakeven_occupancy(
        F.col("kamernet_median_rent_eur"), F.col("airbnb_median_price_eur"), nights_per_month
    )
    centroids = postcodes.select("pc4", "centroid_lat", "centroid_lon")
    return (
        kamernet.join(airbnb, ["pc4", "segment"], "full")
        .withColumns(
            {
                "breakeven_occupancy": breakeven,
                "airbnb_never_wins": breakeven > 1,
                "winner": F.when(
                    F.col("airbnb_median_revenue_eur") > F.col("kamernet_median_rent_eur"), AIRBNB
                ).when(
                    F.col("airbnb_median_revenue_eur") <= F.col("kamernet_median_rent_eur"),
                    KAMERNET,
                ),
                "n_kamernet": F.coalesce("n_kamernet", F.lit(0)),
                "n_airbnb": F.coalesce("n_airbnb", F.lit(0)),
            }
        )
        .withColumn(
            "low_sample",
            (F.col("n_kamernet") < low_sample_threshold)
            | (F.col("n_airbnb") < low_sample_threshold),
        )
        .join(F.broadcast(centroids), "pc4", "left")
    )
