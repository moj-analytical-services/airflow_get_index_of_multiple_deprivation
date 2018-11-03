import requests
import json
import pandas as pd

def get_imd_json(page,year):
    base_url = "http://opendatacommunities.org/slice/observations.json"

    data = {
    "dataset": "http://opendatacommunities.org/data/societal-wellbeing/imd/indices",
    "http://opendatacommunities.org/def/ontology/time/refPeriod": f"http://reference.data.gov.uk/id/year/{year}",
    "http://purl.org/linked-data/cube#measureType": "http://opendatacommunities.org/def/ontology/communities/societal_wellbeing/imd/scoreObs",
    "page": page,
    "perPage": 1000
    }

    r = requests.get(base_url, data=data)

    return json.loads(r.text)

def imd_json_to_df(results):
    df = pd.io.json.json_normalize(data = results["rows"], record_path=["content"],  meta=[["metadata","resource","value"],["metadata","resource","uri"]])
    df["metric"] = df["uri"].map(lambda x: x.split("/")[-1])
    df["lsoa"] = df["metadata.resource.uri"].map(lambda x: x.split("/")[-1])
    df = df[["metric", "value", "lsoa"]]
    df = df.pivot(index= "lsoa", columns = "metric", values="value")
    df = df.reset_index()
    return df