# Road safety lakehouse (Databricks)

A small medallion-architecture pipeline on Databricks that turns the Department for
Transport's STATS19 collision files for Great Britain (2021–2024, 412,276 collisions) into a
tested star schema for reporting.

```
DfT open data ──► bronze_collisions ──► silver_collisions ──► fact_collision + dim_date
 (4 CSV files)    raw text + lineage    typed, validated        dim_severity, dim_area
                                        silver_dq_log           gold_msoa_year
```

| Layer | Notebook | What it does |
|---|---|---|
| Bronze | `01_bronze_ingest` | Downloads the four yearly files from data.dft.gov.uk into a Unity Catalog volume and loads them unchanged, with source file and load time on every row |
| Silver | `02_silver_clean` | Casts types, standardises small-area codes to 2021 boundaries, decodes severity, applies nine data-quality rules and logs the failure count of each to `silver_dq_log`; rows failing a hard rule are excluded |
| Gold | `03_gold_star_schema` | Builds one fact table and three dimensions, checks referential integrity, and aggregates collisions by area (MSOA) and year |
| Orchestration | `04_create_job` | Defines the three steps as a dependent job with the Databricks SDK and runs it |

## Data-quality results (run of 13 Nov 2026)

| Rule | Level | Failing rows |
|---|---|---|
| Duplicate collision ID | hard | 0 |
| Unreadable date | hard | 0 |
| Invalid severity code | hard | 0 |
| Date year differs from file year | soft | 0 |
| Missing coordinates | soft | 51 (0.01%) |
| Coordinates outside Great Britain | soft | 0 |
| Missing small-area (LSOA) code | soft | 16,480 (4.0%) |
| LSOA code from 2011 boundaries (converted) | soft | 20,457 (5.0%) |
| LSOA code not resolvable | soft | 0 |

## Referential integrity and what it found

The first run matched every fact row to a date and a severity, but 36,937 rows (9.0%) did
not match an area:

* 16,480 have no LSOA code: 16,426 are in Scotland, which does not use LSOAs, and 54 are
  English or Welsh collisions with the code left blank.
* 20,457 carried **2011** LSOA codes in the 2021–2023 files (6,599 / 6,971 / 6,887 per year),
  while the area dimension uses the **2021** boundaries; the 2024 file matched completely.

Fix: bronze now also pulls the ONS LSOA 2011 to 2021 best-fit lookup (34,753 rows, paged from
the ONS Open Geography API), and silver converts 2011 codes to 2021 codes before the join,
flagging every converted row (`lsoa_converted_from_2011`) and counting them in the
data-quality log. All 20,457 rows now match; area coverage rose from 91.0% to 96.0%, and the
only unmatched rows are the 16,480 with no code at all.

## Running it

Databricks Free Edition, serverless compute. Import the four notebooks into your home
folder, upload `lsoa_msoa_lookup.csv` (ONS LSOA 2021 to MSOA 2021) to the `raw` volume, and
run `04_create_job`.

Data: STATS19 road safety open data, Department for Transport; ONS Open Geography lookup.
Both under the Open Government Licence v3.0.
