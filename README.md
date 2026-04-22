# Final Project Sprint 10

## Data Pipeline
- Source: PostgreSQL (transactions, currencies)
- Target: Vertica (VT260112E95F7E__STAGING, VT260112E95F7E__DWH)
- Orchestration: Python scripts (load_october.py, update_metrics.py)

## Results for October 2022
- Transactions loaded: 1,144,755
- Global metrics rows: 105

## Scripts
- `load_october.py` – extracts data from PostgreSQL and loads into Vertica staging.
- `update_metrics.py` – calculates global_metrics and inserts into Vertica DWH.

## Metabase Dashboard
Link: (will be added)

## Vertica Schema
- STAGING: `VT260112E95F7E__STAGING`
- DWH (витрина): `VT260112E95F7E__DWH.global_metrics`
