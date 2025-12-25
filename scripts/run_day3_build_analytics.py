from __future__ import annotations

from pathlib import Path
import sys
import pandas as pd
import re

ROOT = Path(__file__).resolve().parents[1]
src = ROOT / "src"
sys.path.insert(0, str(src))

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

