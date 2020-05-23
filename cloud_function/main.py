# PARA CORRER DEBUG ENTRAR EN ESTA CARPETA Y DESDE AQUÍ LLAMAR AL SCRIPT

import runpy
import os
import gcsfs
import pandas as pd
import random
import pytrends
from pytrends.request import TrendReq
import time as timer 
import datetime
from datetime import datetime, date, time, timedelta

import os
from os import listdir
from os.path import isfile, join

def main (data,context):

    #--------------------------------------
    # date
    #yesterday=(datetime.now()-timedelta(days=1)).date()
    dates="2019-01-01"+" "+str(datetime.now().date()) 
    print(dates)


    keywords=[
       "zoom","skype","hangouts",

       "refugiados","inmigracion","nacionalismo","corrupcion","estado de alarma",
       "comparecencia","independentismo","crisis política","barometro","crisis economica",
       "protesta","manifestacion",

       "vox","pp","psoe","podemos","pnv","mas pais","compromis","erc","bildu",

       "teletrabajo","remoto","cursos online","productividad","autonomo",
       "negocio online","emprendimiento", "formacion",

       "erte","paro","sepe","desempleo","deshaucio","comedor social",
       "banco alimentos", "cruz roja", "caritas",

       "ayuda alquiler","compartir piso","divorcio","embarazo","hipoteca", "idealista", "badi", "piso barato"

       "coronavirus","pandemia","infeccion","medico","residencia ancianos", "desescalada",
       
       "clases online","examenes","menu escolar","bullying",

       "netflix","hbo","steam","glovo","just eat","deliveroo","uber eats","hacer deporte","en casa",
       "yoga","meditacion","videollamada","videoconferencia","tinder","meetic", "disney","amazon", "cabify",
       "uber","taxi", "en familia"
        
    ]
        #--------------------------------------
        # the function
        #--------------------------------------
    pytrends = TrendReq(hl='ES', tz=0)
    future_dataframe={}
    c=1
    for k in keywords:   
    
        try:
            print("Requesting ",[k])
            pytrends.build_payload([k], timeframe=dates, geo='ES', gprop='')
            future_dataframe[c]=pytrends.interest_over_time() 
            future_dataframe[c].drop(['isPartial'], axis=1,inplace=True)
            c+=1
            result = pd.concat(future_dataframe, axis=1)
        except:
            print("***","\n","Error with ",k,"or not enough trending percentaje","\n","***")

    result.columns = result.columns.droplevel(0)
    df1=result.unstack(level=-1)
    df2=pd.DataFrame(df1)
    df2.reset_index(inplace=True)
    df2.columns = ["keyword","date","trend_index"]
    df2.to_csv("../tmp/data_pytrends.csv")



    processes= ("pytrends_request.py","upload_gcs.py","remove_files.py")

    for p in processes:
        exec(open(p).read())

if __name__ == "__main__":
  
    main('data','context')
 