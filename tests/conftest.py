"""Pytest configuration: one shared serverless Spark session plus a helper for Column tests."""

from collections.abc import Callable, Generator, Sequence
from pathlib import Path
from typing import Any, Protocol

import pytest
from databricks.connect import DatabricksSession
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from airbnb_renting.geo import GEOJSON_SCHEMA, explode_postcodes

FIXTURES = Path(__file__).parent / "fixtures"
ColumnFn = Callable[[Column], Column]


class Evaluate(Protocol):
    """Type of the `evaluate` fixture. A Protocol, because `Callable[...]` can't express defaults."""

    def __call__(
        self, fn: ColumnFn, inputs: Sequence[Any], input_type: str = "string"
    ) -> list[Any]:
        """Apply `fn` to every input and return the outputs in input order."""
        ...


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """Serverless session via Databricks Connect, created once: each new session costs seconds."""
    session = DatabricksSession.builder.serverless(True).getOrCreate()
    yield session
    session.stop()


@pytest.fixture(scope="session")
def evaluate(spark: SparkSession) -> Evaluate:
    """Apply a Column function to many inputs in ONE remote query and return outputs in order.

    On Databricks Connect every action is a network round-trip, so one DataFrame per test
    (instead of one per case) keeps the suite fast.
    """

    def _evaluate(fn: ColumnFn, inputs: Sequence[Any], input_type: str = "string") -> list[Any]:
        rows = [(i, value) for i, value in enumerate(inputs)]
        df = spark.createDataFrame(rows, f"idx int, value {input_type}")
        result = df.select("idx", fn(F.col("value")).alias("out")).orderBy("idx").collect()
        return [row["out"] for row in result]

    return _evaluate


@pytest.fixture(scope="session")
def postcodes(spark: SparkSession) -> DataFrame:
    """Fixture polygons (1017, 1018 Amsterdam; 1183 Amstelveen as a MultiPolygon).

    Databricks Connect executes on the server, which can't see local files, so the GeoJSON is
    read here and parsed with `from_json` using the same schema the pipeline uses.
    """
    text = (FIXTURES / "postcodes_sample.geojson").read_text()
    raw = spark.createDataFrame([(text,)], "value string")
    return explode_postcodes(
        raw.select(F.from_json("value", GEOJSON_SCHEMA).alias("j")).select("j.*")
    )
