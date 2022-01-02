from google.cloud import bigquery
import os

from google.cloud.bigquery import schema, table
from google.cloud.client import Client 
import pyarrow 
# to resolve pyarrow issue use pip install --upgrade google-cloud-bigquery==2.1.0

import pandas as pd

credential_path="E:\gcp\key.json"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=credential_path

client=bigquery.Client()

table_id="mapapi-290312.GARDENSET.GTABLE"

schema = [bigquery.SchemaField("SENSORNAME", "STRING", mode="NULLABLE"),
          bigquery.SchemaField("TEMP", "STRING", mode="NULLABLE"),
          bigquery.SchemaField("HUMIDITY", "STRING", mode="NULLABLE")]
job_config = bigquery.LoadJobConfig(schema=schema)
rows_to_insert=[
    {u'SENSORNAME':'g--001',u'TEMP':'123',u'HUMIDITY':"453"},
    {u'SENSORNAME':'g--002',u'TEMP':'123',u'HUMIDITY':"453"},
    {u'SENSORNAME':'g--003',u'TEMP':'123',u'HUMIDITY':"453"},
]
df=pd.DataFrame(rows_to_insert)
print(df)
   
#client.load_table_from_json(df, table_id, job_config=job_config).result()
client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
