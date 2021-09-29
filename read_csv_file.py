import re
import sys
from google.cloud import storage
import pandas as pd

general_artifact=["Test_Name","Test_ID","Input","Expected_Result","Output","Test_Result"]
BUCKET = 'test_buc_for_proj' #add bucket name

# Create a Cloud Storage client.
gcs = storage.Client()
testcase1="test_buc_for_proj/incoming/medxcel/" # add test case path i.e test_buc_for_proj/incoming/medxcel
bucket = gcs.get_bucket(BUCKET)

def my_list_bucket(bucket_name,testcase1,general_artifact):
    a_bucket = gcs.lookup_bucket(bucket_name)
    bucket_iterator = a_bucket.list_blobs()
    for resource in bucket_iterator:
        resource.name
        print(resource.name)



my_list_bucket(BUCKET,testcase1,general_artifact)
