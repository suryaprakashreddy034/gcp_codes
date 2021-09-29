import apache_beam as beam
import requests
from google.cloud.exceptions import NotFound
import pandas as pd
import pandas_gbq
from datetime import datetime
from pandas.io import gbq

def findingdelta(now_time,project_id):
  print(now_time)
  current_data_query="""SELECT * FROM `gcptraining-321605.king.testsurya`"""
  current_data=gbq.read_gbq(current_data_query,project_id)
  df1 = pd.DataFrame(current_data)
  options=[]
  options.append(str(now_time))
  print("options",options)
  rslt_df = df1[df1['last_updated'] .isin(options)]
  print(rslt_df)


def loadingdata(element):
    now = datetime.now()
    curr=now.strftime("%Y-%m-%d %H:%M:%S")
    now_time=curr
    response = requests.get(element)
    project_id='gcptraining-321605'
    dataset='king.'
    table_name='testsurya'
    table_id =dataset+table_name
    response = requests.get(element)
    json_response = response.json()
    df = pd.DataFrame(json_response)
    dimensions_of_table = df.shape
    z1=list(dimensions_of_table)
    x_axis1=z1[0]
    last=[]
    for i in range(0,x_axis1):
      curr=now_time
      last.append(curr)
    df['last_updated'] = last 
    pandas_gbq.to_gbq(df, table_id, project_id,if_exists='append')
    return findingdelta(now_time,project_id)

with beam.Pipeline() as pipeline:
    test = (
        pipeline
        | 'Initaiting pipeline' >> beam.Create(['https://covid19-api.com/country/all?fields=country,code'])
        | 'Make API call' >> beam.ParDo(loadingdata)
    )
