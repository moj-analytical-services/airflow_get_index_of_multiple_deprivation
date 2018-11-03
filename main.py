import requests
import json
import pandas as pd

from utils import get_imd_json
from utils import imd_json_to_df

IMD_YEAR = 2015

def get_all_imd():
    last_page = False
    all_results = []
    page = 1
    while not last_page:
        print(f"page is {page}")
        results = get_imd_json(page, IMD_YEAR)
        page += 1
        last_page = results["context"]["isLastPage"]
        df = imd_json_to_df(results)
        print(f"df contains {len(df)} records")
        all_results.append(df)
    all_res =  pd.concat(all_results)
    print(all_res.head(3))
    return all_res

imd = get_all_imd()

for c in imd:
    if c != "lsoa":
        imd[c] = imd[c].astype(float)





imd.to_parquet(f"s3://alpha-mojap-curated-open-data/index_of_multiple_deprivation/imd_year={IMD_YEAR}/data.parquet")

