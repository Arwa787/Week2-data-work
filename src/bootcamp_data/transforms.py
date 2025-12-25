import pandas as pd
import re
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
src = ROOT / "src"
if str(src) not in sys.path:
    sys.path.insert(0, str(src))

from .config import make_paths

paths = make_paths(ROOT)

def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        order_id=df["order_id"].astype("string"),
        user_id=df["user_id"].astype("string"),
        amount=pd.to_numeric(df["amount"], errors="coerce").astype("Float64"),
        quantity=pd.to_numeric(df["quantity"], errors="coerce").astype("Int64"),
    )

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

def dedupe_keep_latest(df: pd.DataFrame, key_cols: list[str], ts_col: str) -> pd.DataFrame:
    return (
        df.sort_values(ts_col)
          .drop_duplicates(subset=key_cols, keep="last")
          .reset_index(drop=True)
    )

def parse_datetime(df: pd.DataFrame, col: str, utc: bool = True) -> pd.DataFrame:
    out = df.copy()
    out[col] = pd.to_datetime(out[col], utc=utc, errors="coerce")
    return out

def add_time_parts(df, ts_col):
    df = df.copy()
    df[ts_col] = pd.to_datetime(df[ts_col])
    df["year"] = df[ts_col].dt.year
    df["month"] = df[ts_col].dt.month
    df["day"] = df[ts_col].dt.day
    df["hour"] = df[ts_col].dt.hour
    df["dayofweek"] = df[ts_col].dt.dayofweek
    return df

def iqr_bounds(s, k=1.5):
    q1 = s.quantile(0.25)
    q3 = s.quantile(0.75)
    iqr = q3 - q1
    return float(q1 - k * iqr), float(q3 + k * iqr)

def winsorize(s: pd.Series, lo: float = 0.01, hi: float = 0.99) -> pd.Series:
    x = s.dropna()
    a, b = x.quantile(lo), x.quantile(hi)
    return s.clip(lower=a, upper=b)
