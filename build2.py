import pandas as pd
from datetime import datetime
import re
import os
import time
import tarfile
import requests
from collections import Counter
from itertools import permutations
from thefuzz import fuzz
from thefuzz import process
import pickle
import warnings


base = "https://www.cplt.cl/transparencia_activa/datoabierto/archivos/"
deseadas =["Nombres","Paterno","Materno","organismo_nombre",'anyo', 'Mes','tipo_calificacionp']


TA_PersonalPlanta                       = f"{base}TA_PersonalPlanta.csv"
TA_PersonalContrata                     = f"{base}TA_PersonalContrata.csv"
TA_PersonalCodigotrabajo                = f"{base}TA_PersonalCodigotrabajo.csv"
TA_PersonalContratohonorarios           = f"{base}TA_PersonalContratohonorarios.csv"


PersonalPlantaDICT                = deseadas+["remuliquida_mensual",'Tipo cargo', 'remuneracionbruta_mensual']
PersonalContrataDICT              = deseadas+["remuliquida_mensual",'Tipo cargo','remuneracionbruta_mensual'] 
PersonalCodigotrabajoDICT         = deseadas+["remuliquida_mensual",'Tipo cargo', 'remuneracionbruta_mensual']
PersonalContratohonorariosDICT    = deseadas+['remuliquida_mensual','tipo_pago','num_cuotas','remuneracionbruta']


if __name__ == '__main__':
    df = pd.read_csv("TA_PersonalContrata.csv", sep=";",encoding="latin",usecols=PersonalContrataDICT)
    df["base"] = "Contrata"
    for i in df["organismo_nombre"].unique():
        #aux = df[df["organismo_nombre"] == i]
        aux = df[df["organismo_nombre"] == i]
        aux.to_csv(fr"test2/{i}.csv", compression='xz', sep='\t', index=False)

