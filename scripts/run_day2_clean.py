from pathlib import Path
import sys
import pandas as pd 
import re




ROOT = Path(__file__).resolve().parents[1]
src = ROOT / "src"
if str(src) not in sys.path:
    sys.path.insert(0, str(src))

from bootcamp_data.config import make_paths
from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet
from bootcamp_data.transforms import enforce_schema


def require_columns(df: pd.DataFrame, cols: list[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    assert not missing, f"Missing columns: {missing}"

def assert_non_empty(df: pd.DataFrame, name: str = "df") -> None:
    assert len(df) > 0, f"{name} has 0 rows"

def missingness_report(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.isna().sum()
        .rename("n_missing")
        .to_frame()
        .assign(p_missing=lambda t: t["n_missing"] / len(df))
        .sort_values("p_missing", ascending=False)
    )

def add_missing_flags(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        out[f"{c}__isna"] = out[c].isna()
    return out

_ws = re.compile(r"\s+")

def normalize_text(s: pd.Series) -> pd.Series:
    return (
        s.astype("string")
        .str.strip()
        .str.casefold()
        .str.replace(_ws, " ", regex=True)
    )

def apply_mapping(s: pd.Series, mapping: dict[str, str]) -> pd.Series:
    return s.map(lambda x: mapping.get(x, x))



paths = make_paths(ROOT)

orders_raw = read_orders_csv(paths.raw / "orders.csv")
users = read_users_csv(paths.raw / "users.csv")

require_columns(orders_raw, ["order_id", "user_id", "amount", "quantity", "created_at", "status"])
require_columns(users, ["user_id", "country", "signup_date"])
assert_non_empty(orders_raw, "orders_raw")
assert_non_empty(users, "users")

orders = enforce_schema(orders_raw)


rep = missingness_report(orders)


reports_dir = ROOT / "data" / "reports"
reports_dir.mkdir(parents=True, exist_ok=True)
rep.to_csv(reports_dir / "missingness_orders.csv", index=True)

status_mapping = {
    "completed": "completed",
    "complete": "completed",
    "done": "completed",
    "pending": "pending",
    "in progress": "in_progress",
    "processing": "in_progress",
    "cancelled": "cancelled",
}

orders["status_clean"] = apply_mapping(normalize_text(orders["status"]), status_mapping)

orders = add_missing_flags(orders, ["amount", "quantity"])
write_parquet(orders, paths.processed / "orders_clean.parquet")
write_parquet(users, paths.processed / "users.parquet")