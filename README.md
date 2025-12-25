# Week2-data-work

## Setup
python -m venv .venv

# activate venv
pip install -r requirements.txt
pip install -e .

## Run ETL
python scripts/run_etl.py

## Outputs
-data/processed/orders_clean.parquet- -data/processed/users.parquet- data/-processed/analytics_table.parquet- -data/processed/_run_meta.json- -reports/figures/*.png

## EDA
Open notebooks/eda.ipynb and run all cells.