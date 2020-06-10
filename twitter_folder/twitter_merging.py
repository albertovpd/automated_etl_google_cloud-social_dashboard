import pandas as pd 
import datetime


twitter_deshaucio=pd.read_csv("../input/defdesa.csv")
deshaucio=twitter_deshaucio[["date","query"]].copy()
deshaucio

#print(deshaucio)

twitter_comedor=pd.read_csv("../input/defdesa.csv")

print(deshaucio[date].apply(just_date))