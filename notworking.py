import apache_beam as beam
import json
import pandas as pd
import mysql.connector
import os
from google.cloud import bigquery
from google.cloud.client import Client

from google.cloud.bigquery import schema, table
from google.cloud.client import Client




class SplitRow(beam.DoFn):
    def __init__(self):
        print("main")
    def process(self,element):
        #self.element=element
        self.element = element
        self.path_data = open(self.element)
        self.data = json.load(self.path_data)
        self.host=self.data["host"]
        self.user = self.data["user"]
        self.pwd = self.data["password"]
        self.DB = self.data["database"]
        self.q1=self.data["q1"]

        def connect_to_sql():
            try:
                mydb = mysql.connector.connect(
                    host=self.host,
                    user=self.user,
                    password=self.pwd,
                    database=self.DB
                )

                mycursor = mydb.cursor()
                mycursor.execute(self.q1)

                myresult = mycursor.fetchall()
                df = pd.DataFrame(myresult, columns=["ID", "Name", "CountryCode", "District", "Population"])

                return  df
            except os.error as e:
                print(f"error while {e}")

        self.sql_data=connect_to_sql()

        yield self.sql_data




class FilterAccountsEmployee(beam.DoFn):
    def __init__(self,inputkey):
        self.key=inputkey
        print(self.key)
    def start_bundle(self):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.key
        self.client = bigquery.Client()
        table_id = "mapapi-290312.GARDENSET.world-data"
            #bigquery.Client()

    def process(self, element):
        self.table_data=element
        self.dataframe=pd.DataFrame(self.table_data)
        print(self.dataframe)

        """
        credential_path = "C:/Users/sprakashreddychin/Documents/mapapi-290312-8fafd696470f.json"

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credential_path

        client = bigquery.Client()

        table_id = "mapapi-290312.GARDENSET.world-data"

        schema = [bigquery.SchemaField("ID", "STRING", mode="NULLABLE"),
                  bigquery.SchemaField("Name", "STRING", mode="NULLABLE"),
                  bigquery.SchemaField("CountryCode", "STRING", mode="NULLABLE"),
                  bigquery.SchemaField("District", "STRING", mode="NULLABLE"),
                  bigquery.SchemaField("Population", "STRING", mode="NULLABLE")]
        job_config = bigquery.LoadJobConfig(schema=schema)


        df = self.dataframe.astype(str)

        client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
        """


class PairEmployees(beam.DoFn):


    def process(self, element):
        return [(element[3] + "," + element[1], 1)]


class Counting(beam.DoFn):

    def process(self, element):
        (key, values) = element  # [Marco, Accounts  [1,1,1,1....] , Rebekah, Accounts [1,1,1,1,....] ]
        return [(key, sum(values))]


p1 = beam.Pipeline()
inputpath = ["C:/Users/sprakashreddychin/PycharmProjects/gcp_ams/processgate1/sqlsourceconfig.json"]
keypath="C:/Users/sprakashreddychin/Desktop/PROJECT_AMS/gcp_codes-main/key.json"
attendance_count = (
        p1
        | beam.Create(inputpath)
        | beam.ParDo(SplitRow())
        | beam.ParDo(FilterAccountsEmployee(keypath))
        # | 'Compute WordLength' >> beam.ParDo(lambda element: [ element.split(',') ])


)

p1.run()

