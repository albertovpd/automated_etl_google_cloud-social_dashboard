import os
import gcsfs
import pandas as pd

# CREDENTIALS
# - project = Project name in Google Cloud, NOT project ID.
# - fs.open("your bucket/name your future csv.csv")
# - fs.upload("where is temporary located in cloud function", "your bucket/name your future csv.csv")

# .ENV structure:
# TOKEN_NAME='your credentials name with object access to gcs.json'
# PROJECT_NAME='name of your project, NO name ID'
# PROJECT_PATH='your bucket in gcs/your future.csv'
# PROJECT_TMP='../tmp/name of your requested csv in pytrends_request.py .csv'

fs = gcsfs.GCSFileSystem(token=os.getenv('TOKEN_NAME') ,project=os.getenv('PROJECT_NAME'))

# upload the new final companies
with fs.open(os.getenv('PROJECT_PATH')) as mergecomp:
    df = pd.read_csv(mergecomp)
fs.upload(os.getenv('PROJECT_TMP'),os.getenv('PROJECT_PATH'))
