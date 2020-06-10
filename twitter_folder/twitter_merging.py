import pandas as pd 
import datetime

# it is really expensive to work with Twitter API for
# monitoring keywords weekly and it is out of project
# (we are simple mortals with limited budget). Nevertheless
# the code of monitoring Twitter with Python will be
# available soon, thanks to Patri


# This is just getting the right shape to use it in 
# DataStudio.

twitter_deshaucio=pd.read_csv("./input/defdesa.csv")
deshaucio=twitter_deshaucio[["date","query"]].copy()

twitter_comedor=pd.read_csv("./input/dfcom.csv")
comedor=twitter_comedor[["date","query"]].copy()

twitter_desempleo=pd.read_csv("./input/dfdes.csv")
desempleo=twitter_comedor[["date","query"]].copy()

twitter_erte=pd.read_csv("./input/dferte.csv")
erte=twitter_erte[["date","query"]].copy()

twitter_sepe=pd.read_csv("./input/dfsepe.csv")
sepe=twitter_sepe[["date","query"]].copy()

twitter_final
twitter_final.to_csv("./input/twitter_final.csv")