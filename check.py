
import pandas as pd


# key integrity

def assert_unique_key(df, key, allow_na=False):
     if not allow_na:
          if df[key].isnull().any:
               raise ValueError("there are missing value") 
          
          if df[key].duplicated().any:
               raise ValueError("there are duplicated value") 


df= pd.read_parquet("orders.parquet")
print(df)

def missingness_reprt(df):
     n=len(df)
     return(
          df.isin().sum()
           .rename("n_missing")
           .to_frame()
           .assign(p_missing=lambda t: t["n_missing"]/n)
           .sort_values("p_missing", ascending=False)
     )


#page 66