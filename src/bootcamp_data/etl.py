from dataclasses import dataclass
from pathlib import Path
import sys
import pandas as pd
import re
from __future__ import annotations
import json
from dataclasses import asdict



@dataclass(frozen=True)
class ETLConfig:
      root: Path
      raw_orders: Path
      raw_users: Path
      out_orders_clean: Path
      out_users: Path
      out_analytics: Path
      run_meta: Path

import pandas as pd
from bootcamp_data.io import read_orders_csv, read_users_csv

def load_inputs(cfg: ETLConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
   orders = read_orders_csv(cfg.raw_orders)
   users = read_users_csv(cfg.raw_users)
   return orders, users


# Day2

ROOT = Path(__file__).resolve().parents[1]
src = ROOT / "src"
if str(src) not in sys.path:
    sys.path.insert(0, str(src))

from bootcamp_data.config import make_paths
from bootcamp_data.io import write_parquet
from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet
from bootcamp_data.transforms import (
    enforce_schema,
    missingness_report,
    add_missing_flags,
    normalize_text,
    apply_mapping,
)
from bootcamp_data.quality import (
    require_columns,
    assert_non_empty,
)

paths = make_paths(ROOT)

orders_raw = read_orders_csv(paths.raw / "orders.csv")
users = read_users_csv(paths.raw / "users.csv")

require_columns(orders_raw, ["order_id", "user_id", "amount", "quantity", "created_at", "status"])
require_columns(users, ["user_id", "country", "signup_date"])
assert_non_empty(orders_raw, "orders_raw")
assert_non_empty(users, "users")

orders = enforce_schema(orders_raw)

rep = missingness_report(orders)
reports_dir = ROOT / "reports"
reports_dir.mkdir(parents=True, exist_ok=True)
rep_path = reports_dir / "missingness_orders.csv"
rep.to_csv(rep_path, index=True)

status_norm = normalize_text(orders["status"])
mapping = {"paid": "paid", "refund": "refund", "refunded": "refund"}
status_clean = apply_mapping(status_norm, mapping)

orders_clean = (
    orders.assign(status_clean=status_clean)
    .pipe(add_missing_flags, cols=["amount", "quantity"])
)

write_parquet(orders_clean, paths.processed / "orders_clean.parquet")
write_parquet(users, paths.processed / "users.parquet")


# Day3 


from bootcamp_data.config import make_paths
from bootcamp_data.quality import require_columns, assert_unique_key, assert_non_empty
from bootcamp_data.transforms import parse_datetime, add_time_parts, winsorize, iqr_bounds
from bootcamp_data.joins import safe_left_join
from bootcamp_data.io import write_parquet

paths = make_paths(ROOT)

def add_outlier_flag(df: pd.DataFrame, col: str, *, k: float = 1.5) -> pd.DataFrame:
    lo, hi = iqr_bounds(df[col], k=k)
    return df.assign(**{f"{col}__is_outlier": (df[col] < lo) | (df[col] > hi)})

def main() -> None:
    p = make_paths(ROOT)

    orders = pd.read_parquet(p.processed / "orders_clean.parquet")
    users = pd.read_parquet(p.processed / "users.parquet")

    require_columns(
        orders,
        ["order_id", "user_id", "amount", "quantity", "created_at", "status_clean"],
    )
    require_columns(users, ["user_id", "country", "signup_date"])

    assert_non_empty(orders, "orders_clean")
    assert_non_empty(users, "users")
    assert_unique_key(users, "user_id")

    orders_t = (
        orders
        .pipe(parse_datetime, col="created_at", utc=True)
        .pipe(add_time_parts, ts_col="created_at")
    )

    joined = safe_left_join(
        orders_t,
        users,
        on="user_id",
        validate="many_to_one",
        suffixes=("", "_user"),
    )

    if len(joined) != len(orders_t):
        raise AssertionError("Row count changed on left join")

    joined = joined.assign(amount_winsor=winsorize(joined["amount"]))
    joined = add_outlier_flag(joined, "amount", k=1.5)

    out_path = p.processed / "analytics_table.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    joined.to_parquet(out_path, index=False)

if __name__ == "__main__":
    main()


# summary.to_csv(ROOT/"reports/revenue_by_country.csv", index=False
summary = (
joined.groupby("country", dropna=False)
      .agg(n=("order_id","size"), revenue=("amount","sum"))
      .reset_index()
      .sort_values("revenue", ascending=False)

)
print(summary)


def load_outputs(*, analytics: pd.DataFrame, users: pd.DataFrame, cfg: ETLConfig) -> None:
    """Write processed artifacts (idempotent)."""
    write_parquet(users, cfg.out_users)
    write_parquet(analytics, cfg.out_analytics)

    user_side_cols = [c for c in users.columns if c != "user_id"]
    cols_to_drop = [c for c in user_side_cols if c in analytics.columns] + [
        c for c in analytics.columns if c.endswith("_user")
    ]
    orders_clean = analytics.drop(columns=cols_to_drop, errors="ignore")
    write_parquet(orders_clean, cfg.out_orders_clean)

def write_run_meta(
        cfg: ETLConfig, *, orders_raw: pd.DataFrame, users: pd.DataFrame, analytics: pd.DataFrame
) -> None:
   missing_created_at = int(analytics["created_at"].isna().sum()) if "created_at" in analytics.columns else None
   country_match_rate = (
       1.0 - float(analytics["country"].isna().mean())
       if "country" in analytics.columns
       else None
   )

   meta = {
    "rows_in_orders_raw": int(len(orders_raw)),
    "rows_in_users": int(len(users)),
    "rows_out_analytics": int(len(analytics)),
    "missing_created_at": missing_created_at,
    "country_match_rate": country_match_rate,
    "config": {k: str(v) for k, v in asdict(cfg).items()},
}
   
cfg.run_meta.parent.mkdir(parents=True, exist_ok=True)
cfg.run_meta.write_text(json.dumps(meta, indent=2), encoding="utf-8")

# Allows: `python scripts/run_etl.py` without installing the package
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bootcamp_data.etl import ETLConfig, run_etl


def main() -> None:
  cfg = ETLConfig(
      root=ROOT,
      raw_orders=ROOT / "data" / "raw" / "orders.csv",
      raw_users=ROOT / "data" / "raw" / "users.csv",
      out_orders_clean=ROOT / "data" / "processed" / "orders_clean.parquet",
      out_users=ROOT / "data" / "processed" / "users.parquet",
      out_analytics=ROOT / "data" / "processed" / "analytics_table.parquet",
      run_meta=ROOT / "data" / "processed" / "_run_meta.json",
  )
  run_etl(cfg)

if __name__ == "__main__":
    main()


    







   













