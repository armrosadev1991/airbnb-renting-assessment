"""Turn the sources' free-text fields into typed values. Unparseable input becomes NULL.

Silver quality rules decide what to do with those NULLs; parsing never drops or guesses.
Spark 4 runs in ANSI mode, so every string-to-number step uses `try_cast`: a plain cast of
'' or 'abc' raises instead of returning NULL.
"""

from pyspark.sql import Column
from pyspark.sql import functions as F

_NULL_TOKENS = ["", "NA", "N/A", "-"]
# NL postcodes never start with 0; the lookarounds stop '12345' from matching as 1234.
_PC4_PATTERN = r"(?<![0-9])([1-9][0-9]{3})(?![0-9])"
_PC6_PATTERN = r"^[1-9][0-9]{3}[A-Z]{2}$"
_EUR_PATTERN = r"€\s*([0-9.]+)"


def strip_whitespace(col: Column) -> Column:
    """Trim spaces, tabs and newlines; Spark's `trim` only removes spaces."""
    return F.regexp_replace(col, r"^\s+|\s+$", "")


def normalise_null(col: Column) -> Column:
    """Map the sources' many spellings of 'missing' to a real NULL; otherwise return stripped text."""
    stripped = strip_whitespace(col)
    return F.when(stripped.isin(_NULL_TOKENS), None).otherwise(stripped)


def parse_pc4(col: Column) -> Column:
    """Extract a 4-digit postcode from free text ('1079 HH Amsterdam' -> 1079), else NULL."""
    return F.regexp_extract(normalise_null(col), _PC4_PATTERN, 1).try_cast("int")


def parse_pc6(col: Column) -> Column:
    """Normalise a full postcode ('1018  dw' -> '1018DW'), else NULL."""
    compact = F.upper(F.regexp_replace(col, r"\s", ""))
    return F.when(compact.rlike(_PC6_PATTERN), compact)


def parse_eur(col: Column) -> Column:
    """Euro amount from strings like '€ 600,-  Utilities incl.' or '€ 1.250,-', else NULL."""
    digits = F.regexp_extract(normalise_null(col), _EUR_PATTERN, 1)
    return F.regexp_replace(digits, r"\.", "").try_cast("decimal(10,2)")


def utilities_included(col: Column) -> Column:
    """True when the rent string says utilities are included; never NULL."""
    return F.coalesce(F.lower(col).contains("utilities incl"), F.lit(False))


def parse_area_sqm(col: Column) -> Column:
    """Floor area from '14 m2', else NULL."""
    return F.regexp_extract(normalise_null(col), r"^([0-9]+)\s*m2$", 1).try_cast("int")


def parse_decimal(col: Column) -> Column:
    """Plain numeric string ('130', '59.5') to decimal, else NULL."""
    return normalise_null(col).try_cast("decimal(10,2)")
