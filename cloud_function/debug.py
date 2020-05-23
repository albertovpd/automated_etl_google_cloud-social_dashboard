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


# date

dates="2019-01-01"+" "+str(datetime.now().date()) 
print(dates)