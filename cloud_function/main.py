# PARA CORRER DEBUG ENTRAR EN ESTA CARPETA Y DESDE AQUÍ LLAMAR AL SCRIPT

import os
from os import listdir
from os.path import isfile, join

def main (data,context):
    
    processes= ("pytrends_request.py","upload_gcs_real.py","remove_files.py")

    for p in processes:
        print(p)
        exec(open(p).read())

if __name__ == "__main__":
  
    main('data','context')
 