# Revenue model: how "more profitable" is decided

## Like for like

Revenue is compared **per 4-digit postcode and per segment**, never across segments:

| Segment | Airbnb | Kamernet |
|---------|--------|----------|
| `whole_unit` | Entire home/apt | Apartment, Studio |
| `room` | Private room, Shared room | Room, Student residence |

All figures are **gross monthly EUR**. Purchase price, mortgage, taxes, platform fees, cleaning
and utilities are out of scope (the brief asks for *potential* revenue).

## Formulas

```
Kamernet  monthly = rent (as listed)
Airbnb    monthly = price_per_night × 30 × occupancy            (occupancy scenarios)
                  = price_per_night × 30 nights per year / 12   (legal-cap scenario)

breakeven occupancy = Kamernet median rent / (Airbnb median price × 30)
```

Breakeven is the occupancy at which Airbnb earns the same as long-term rent. **It doesn't depend
on a scenario**, so it's the headline metric. Above 1.0 means Airbnb can't win even when fully
booked (`airbnb_never_wins`).

## Scenarios (pipeline configuration, not code)

| Scenario | Applies to | Meaning |
|----------|------------|---------|
| `legal_cap_30n` | `whole_unit` only | Amsterdam limits renting out a whole home to 30 nights a year (permit + notification) |
| `occ_50` | both | 50% of nights booked |
| `occ_70` | both | 70% of nights booked |

The cap doesn't apply to `room`: renting a room in a home you live in falls under B&B rules,
which need a permit but have no night limit. Third-party guides report the whole-home cap
dropping to 15 nights in some central areas from April 2026, but our data is from 2019.
Sources: [Airbnb, Amsterdam](https://www.airbnb.com/help/article/860),
[Minut](https://www.minut.com/blog/amsterdam-short-term-rental-laws),
[Keycafe](https://www.keycafe.com/s/blog/understanding-short-term-rental-regulations-in-amsterdam).
Check the City of Amsterdam's own pages before relying on this.

Changing an assumption means editing `scenarios` in
[`resources/airbnb_renting.pipeline.yml`](../resources/airbnb_renting.pipeline.yml) and redeploying.

## Result

Counting only postcodes with at least 5 listings on each side:

| Segment | Scenario | Airbnb wins | Kamernet wins | Median breakeven |
|---------|----------|-------------|---------------|------------------|
| whole_unit | legal 30-night cap | 1 | **61** | 39.5% |
| whole_unit | 50% occupancy | **51** | 11 | 39.5% |
| whole_unit | 70% occupancy | **61** | 1 | 39.5% |
| room | 50% occupancy | **64** | 0 | 25.1% |

**Reading it:** a whole flat on Airbnb needs about 40% occupancy to match long-term rent, and the
legal cap allows about 8%. **Legally, long-term renting wins almost everywhere for whole units.**
Rooms are the opposite: Airbnb needs only about 25% occupancy and has no night cap.

## Limitations

- **Different years:** Kamernet was crawled July 2019 to March 2020; the Airbnb snapshot date is
  unknown (probably 2019 or earlier). Neither reflects current prices or the 2026 rules.
- **Room comparability:** a Kamernet "room" (usually in shared housing) and an Airbnb "private
  room" aren't the same product, so the `room` comparison is weaker than `whole_unit`.
- **Listed, not achieved:** both sources show asking prices, not actual bookings or signed rents.
- **Gross only:** an Airbnb's costs (cleaning, fees, furnishing, vacancy) are higher, which would
  move breakeven up.
