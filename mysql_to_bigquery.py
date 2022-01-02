"""
autor:Surya Prakash Reddy.C
Date:2/2/2022

* service account for bq admin key
* to resolve pyarrow issue use pip install --upgrade google-cloud-bigquery==2.1.0
* streming type insertion does not works in trail account

"""
import mysql.connector
import pandas as pd
from google.cloud import bigquery
import os

from google.cloud.bigquery import schema, table
from google.cloud.client import Client 
import pyarrow 

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="Surya@8897",
  database="world"
)

mycursor = mydb.cursor()

mycursor.execute("SELECT * FROM world.city")

myresult = mycursor.fetchall()

df=pd.DataFrame(myresult,columns=["ID","Name","CountryCode","District","Population"])


credential_path="E:\gcp\key.json"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=credential_path

client=bigquery.Client()

table_id="mapapi-290312.GARDENSET.world-data"

schema = [bigquery.SchemaField("ID", "STRING",mode="NULLABLE"),
          bigquery.SchemaField("Name", "STRING",mode="NULLABLE"), 
          bigquery.SchemaField("CountryCode", "STRING",mode="NULLABLE"),
          bigquery.SchemaField("District", "STRING",mode="NULLABLE"),
          bigquery.SchemaField("Population", "STRING",mode="NULLABLE")]
job_config = bigquery.LoadJobConfig(schema=schema)

print(df)
df=df.astype(str)

client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
