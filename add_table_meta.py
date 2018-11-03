import pandas as pd
import requests
import json

from utils import get_imd_json
from utils import imd_json_to_df

results = get_imd_json(1,2015)

df = imd_json_to_df(results)
cols_in_df = [c for c in df.columns]

meta_cols = {}
for col in results["rows"][0]["content"]:
    uri = col["uri"]
    colname = uri.split("/")[-1]

    j = json.loads(requests.get(uri + ".json").text)
    uri = j[0]["http://opendatacommunities.org/def/ontology/communities/societal_wellbeing/imd/indices"][0]["@id"]
    j = json.loads(requests.get(uri + ".json").text)

    label = j[0]['http://www.w3.org/2000/01/rdf-schema#label'][0]['@value']
    value = j[0]['http://www.w3.org/2000/01/rdf-schema#comment'][0]['@value']
    meta_cols[colname] = ({"name": colname, "desc": f"{label}: {value}"})

lsoas = "http://opendatacommunities.org/def/foi/collection/lsoas.json"
j = json.loads(requests.get(uri + ".json").text)
label = j[0]['http://www.w3.org/2000/01/rdf-schema#label'][0]['@value']
value = j[0]['http://www.w3.org/2000/01/rdf-schema#comment'][0]['@value']
colname= 'lsoa'
meta_cols[colname] = {"name": colname, "desc": f"{label}: {value}"}



# Create meta information for db
from etl_manager import meta

db = meta.DatabaseMeta(name="open_data", bucket="alpha-mojap-curated-open-data")

db.base_folder = ''
db.description = 'Demo database of flights data'

table = meta.TableMeta(name="index_of_multiple_deprivation", location = "index_of_multiple_deprivation",data_format='parquet' )

# Take metadata and convert into Glue Catelogue format
str_fields = ["lsoa"]

for col_name in cols_in_df:
    this_meta = meta_cols[col_name]
    this_type = "double"
    if col_name in str_fields:
        this_type = "character"
    table.add_column(name = col_name, type=this_type, description=this_meta["desc"][:255])

table.add_column(name="imd_year", type="int", description="The year that the values for index of multiple deprivation were observed")

table.partitions = ["imd_year"]

#  Import etl manager and add to glue
table.database = db
td = table.glue_table_definition()

import boto3

glue_client = boto3.client("glue" , "eu-west-1")

try:
    glue_client.delete_table(DatabaseName = 'open_data', Name = 'index_of_multiple_deprivation')
except glue_client.exceptions.EntityNotFoundException:
    pass

glue_client.create_table(DatabaseName="open_data", TableInput=td)

athena_client = boto3.client('athena', 'eu-west-1')
response = athena_client.start_query_execution(
        QueryString="MSCK REPAIR TABLE open_data.index_of_multiple_deprivation;",
        ResultConfiguration={
            'OutputLocation': "s3://alpha-mojap-curated-open-data/__athenatemp__",
      }
    )