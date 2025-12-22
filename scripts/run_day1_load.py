from pathlib import Path
import sys 
import json 
from datetime import datetime, timezone



ROOT = Path(__file__).resolve().parents[1]
src= ROOT / "src"
if str(src) not in sys.path:
    sys.path.insert(0, str(src))

from bootcamp_data.config import make_paths
from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet
from bootcamp_data.transforms import enforce_schema




paths = make_paths(ROOT)
orders= enforce_schema(read_orders_csv(paths.raw/"orders.csv"))
users =read_users_csv(paths.raw /"users.csv")




out_orders = paths.processed / "orders.parquet"
out_users = paths.processed / "users.parquet"
write_parquet(orders, out_orders)
write_parquet(users, out_users)



