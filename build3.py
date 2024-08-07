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

comunas = ['Corporación Municipal de Providencia',
 'Municipalidad de Antofagasta',
 'Municipalidad de Chillán',
 'Municipalidad de Concepción',
 'Municipalidad de Coquimbo',
 'Municipalidad de Curicó',
 'Municipalidad de El Bosque',
 'Municipalidad de Florida',
 'Municipalidad de Iquique',
 'Municipalidad de La Pintana',
 'Municipalidad de La Serena',
 'Municipalidad de Los Ángeles',
 'Municipalidad de Maipú',
 'Municipalidad de Peñalolén',
 'Municipalidad de Pudahuel',
 'Municipalidad de Puente Alto',
 'Municipalidad de Puerto Montt',
 'Municipalidad de Quilicura',
 'Municipalidad de Rancagua',
 'Municipalidad de San Bernardo',
 'Municipalidad de Santiago',
 'Municipalidad de Talca',
 'Municipalidad de Talcahuano',
 'Municipalidad de Valdivia',
 'Municipalidad de Valparaíso',
 'Municipalidad de Viña del Mar',
 'Municipalidad de Ñuñoa']

if __name__ == '__main__':
    df = pd.read_csv("TA_PersonalCodigotrabajo.csv", sep=";",encoding="latin",usecols=PersonalCodigotrabajoDICT)
    df["base"] = "Codigotrabajo"
    for i in df["organismo_nombre"].unique():
        #aux = df[df["organismo_nombre"] == i]
        aux = df[df["organismo_nombre"] == i]
        aux.to_csv(fr"test3/{i}.csv", compression='xz', sep='\t', index=False)

