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
