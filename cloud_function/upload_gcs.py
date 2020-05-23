import os
import gcsfs
import pandas as pd



# CREDENTIALS
fs = gcsfs.GCSFileSystem(token=os.getenv('TOKEN_NAME'), project=os.getenv('PROJECT_NAME'))

# upload the new final companies
with fs.open(os.getenv("PROJECT_PATH")) as mergekey:
    df = pd.read_csv(mergekey)
fs.upload("../tmp/data_pytrends.csv",os.getenv("PROJECT_PATH"))



