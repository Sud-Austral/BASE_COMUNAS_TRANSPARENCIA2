import pandas as pd
from datetime import datetime
import requests
from tqdm import tqdm
import gc
import os
import shutil
import requests
from tqdm import tqdm
import gc
import logging
import locale
locale.setlocale(locale.LC_TIME, "es_ES.UTF-8")  # Cambiar a español
import numpy as np
from functools import partial
from concurrent.futures import ThreadPoolExecutor
import re

organismo = pd.read_csv(r"organismo_nombre.csv",compression='xz', sep='\t')

base = "https://www.cplt.cl/transparencia_activa/datoabierto/archivos/"
deseadas =["Nombres","Paterno","Materno","organismo_nombre",'anyo', 'Mes','tipo_calificacionp']


TA_PersonalPlanta                       = f"{base}TA_PersonalPlanta.csv"
TA_PersonalContrata                     = f"{base}TA_PersonalContrata.csv"
TA_PersonalCodigotrabajo                = f"{base}TA_PersonalCodigotrabajo.csv"
TA_PersonalContratohonorarios           = f"{base}TA_PersonalContratohonorarios.csv"


PersonalPlantaDICT                = deseadas+["remuliquida_mensual",'Tipo cargo', 'remuneracionbruta_mensual','fecha_ingreso','fecha_termino']
PersonalContrataDICT              = deseadas+["remuliquida_mensual",'Tipo cargo','remuneracionbruta_mensual','fecha_ingreso','fecha_termino'] 
PersonalCodigotrabajoDICT         = deseadas+["remuliquida_mensual",'Tipo cargo', 'remuneracionbruta_mensual','fecha_ingreso','fecha_termino']
PersonalContratohonorariosDICT    = deseadas+['remuliquida_mensual','tipo_pago','num_cuotas','remuneracionbruta','fecha_ingreso','fecha_termino']

# Definir formatos como tupla para mejor rendimiento
FORMATOS = ('%d/%m/%Y', '%Y/%m/%d %H:%M:%S.%f', '%d/%m/%y')

FECHA_DEFAULT = pd.Timestamp('1900-01-01')
CURRENT_TIME = pd.Timestamp.now()

def detect_date_format(fecha):
    if pd.isna(fecha):
        return None
    
    fecha_str = str(fecha).strip()
    
    if not re.match(r'\d{1,4}[/-]\d{1,4}[/-]\d{1,4}', fecha_str):
        return None
    
    format_cache = {}
    
    for fmt in FORMATOS:
        if fmt in format_cache and format_cache[fmt].match(fecha_str):
            return fmt
        try:
            datetime.strptime(fecha_str, fmt)
            format_cache[fmt] = re.compile(fecha_str)
            return fmt
        except ValueError:
            continue
    return None

@np.vectorize
def parse_date_fast(fecha, default_value=FECHA_DEFAULT):
    if pd.isna(fecha):
        return default_value
    
    fecha_str = str(fecha).strip()
    fmt = detect_date_format(fecha_str)
    
    if fmt:
        try:
            return pd.to_datetime(fecha_str, format=fmt)
        except ValueError:
            pass
    return default_value

def process_chunk(chunk_data, column, default_value):
    """Procesa un chunk individual de datos"""
    return parse_date_fast(chunk_data[column].values, default_value)

def process_dates(df, chunk_size=10000):
    # Convertir a categorías para reducir memoria
    df['fecha_ingreso'] = df['fecha_ingreso'].astype('category')
    df['fecha_termino'] = df['fecha_termino'].astype('category')
    
    # Calcular número de chunks
    chunks = [df[i:i+chunk_size] for i in range(0, len(df), chunk_size)]
    
    ingreso_results = []
    termino_results = []
    
    with ThreadPoolExecutor() as executor:
        # Procesar fecha_ingreso
        for chunk in chunks:
            future = executor.submit(process_chunk, chunk, 'fecha_ingreso', FECHA_DEFAULT)
            result = future.result()
            ingreso_results.append(result)
        
        # Procesar fecha_termino
        for chunk in chunks:
            future = executor.submit(process_chunk, chunk, 'fecha_termino', CURRENT_TIME)
            result = future.result()
            termino_results.append(result)
    
    # Combinar resultados manejando índices duplicados
    df['fecha_ingreso'] = np.concatenate(ingreso_results)
    df['fecha_termino'] = np.concatenate(termino_results)
    
    return df

def optimize_dates(df):
    # Verificar tipos de datos
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a pandas DataFrame")
    
    # Verificar columnas requeridas
    required_cols = ['fecha_ingreso', 'fecha_termino']
    if not all(col in df.columns for col in required_cols):
        raise ValueError(f"DataFrame must contain columns: {required_cols}")
    
    # Procesamiento optimizado
    return process_dates(df)

def descargar_archivo(url, nombre_archivo):
    try:
        # Realiza la solicitud para obtener el archivo
        with requests.get(url, stream=True) as respuesta:
            respuesta.raise_for_status()  # Verifica si hubo errores en la solicitud

            # Obtiene el tamaño total del archivo
            total_size = int(respuesta.headers.get('content-length', 0))

            # Abre un archivo para escribir los datos descargados
            with open(nombre_archivo, 'wb') as archivo:
                # Utiliza tqdm para mostrar el progreso de la descarga
                with tqdm(total=total_size, unit='B', unit_scale=True, unit_divisor=1024, desc=nombre_archivo) as barra_progreso:
                    for chunk in respuesta.iter_content(chunk_size=1024):  # Reduce chunk_size si es necesario
                        if chunk:  # Filtra los chunks vacíos
                            archivo.write(chunk)
                            barra_progreso.update(len(chunk))
        
        print(f"Descarga completada: {nombre_archivo}")

    except requests.exceptions.RequestException as e:
        print(f"Error al descargar el archivo: {e}")

    finally:
        # Forzar la liberación de memoria
        gc.collect()
        print("Recursos liberados")

# Ejemplo de uso
# descargar_archivo('https://example.com/archivo.zip', 'archivo.zip')


def separar_partes(ruta,diccionario,folder,base):
    df = pd.read_csv(ruta, sep=";",encoding="latin",usecols=diccionario)
    df = df.rename(columns={'remuneracionbruta': 'remuneracionbruta_mensual'})
    df["base"] = base
    for i in df["organismo_nombre"].unique():
        #print(i,ruta,end='\r')
        #print("",end='\r')
        file_path = f"{folder}/{i}.csv"
        aux = df[df["organismo_nombre"] == i]
        aux = optimize_dates(aux)
        aux.to_csv(file_path, compression='xz', sep='\t', index=False)
        del aux
    # Eliminar el DataFrame después de procesarlo
    del df

import pandas as pd

def asegurar_columnas(df):
    # Lista de columnas que se esperan en el DataFrame
    columnas_requeridas = ['organismo_nombre', 'anyo', 'Mes', 'Nombres', 'Paterno', 'Materno',
                           'tipo_calificacionp', 'Tipo cargo', 'remuneracionbruta_mensual',
                           'remuliquida_mensual', 'base', 'tipo_pago', 'num_cuotas']
    
    # Iterar sobre la lista de columnas requeridas
    for columna in columnas_requeridas:
        # Si la columna no está en el DataFrame, la añadimos con valores vacíos (None o NaN)
        if columna not in df.columns:
            df[columna] = None
    
    return df


def unir(organismo):

    acumulador = []
    ruta = fr"d1/{organismo}.csv"
    if(os.path.exists(ruta)):
        aux = pd.read_csv(ruta,compression='xz', sep='\t')
        acumulador.append(aux.copy())
    ruta = fr"d2/{organismo}.csv"
    if(os.path.exists(ruta)):
        aux = pd.read_csv(ruta,compression='xz', sep='\t')
        acumulador.append(aux.copy())
    ruta = fr"d3/{organismo}.csv"
    if(os.path.exists(ruta)):
        aux = pd.read_csv(ruta,compression='xz', sep='\t')
        acumulador.append(aux.copy())
    ruta = fr"d4/{organismo}.csv"
    if(os.path.exists(ruta)):
        aux = pd.read_csv(ruta,compression='xz', sep='\t')
        acumulador.append(aux.copy())
    df =  pd.concat(acumulador)
    df = asegurar_columnas(df)
    df.to_csv(fr"organismo/{organismo}.csv", index=False,compression='xz', sep='\t')
    gc.collect()
    print(f"Se guardo correctamente {organismo}" , end='\r')
    del df

def make_backup():
    current_time = datetime.now().strftime("%Y_%m_%d")
    os.makedirs(f"respaldo/{current_time}", exist_ok=True)
    source_file = 'respaldo/TA_PersonalPlanta.csv'
    # Ruta donde deseas copiar el archivo
    destination_file = f'respaldo/{current_time}/TA_PersonalPlanta.csv'
    # Copiar el archivo
    shutil.copy(source_file, destination_file)
    ##
    source_file = 'respaldo/TA_PersonalContrata.csv'
    # Ruta donde deseas copiar el archivo
    destination_file = f'respaldo/{current_time}/TA_PersonalContrata.csv'
    # Copiar el archivo
    shutil.copy(source_file, destination_file)
    ##
    source_file = 'respaldo/TA_PersonalCodigotrabajo.csv'
    # Ruta donde deseas copiar el archivo
    destination_file = f'respaldo/{current_time}/TA_PersonalCodigotrabajo.csv'
    # Copiar el archivo
    shutil.copy(source_file, destination_file)
    ##
    source_file = 'respaldo/TA_PersonalContratohonorarios.csv'
    # Ruta donde deseas copiar el archivo
    destination_file = f'respaldo/{current_time}/TA_PersonalContratohonorarios.csv'
    # Copiar el archivo
    shutil.copy(source_file, destination_file)





def GLOBAL(): 
    
    print("TA_PersonalPlanta",datetime.now())
    descargar_archivo(TA_PersonalPlanta            , "respaldo/TA_PersonalPlanta.csv")
    print("TA_PersonalContrata",datetime.now())
    descargar_archivo(TA_PersonalContrata          , "respaldo/TA_PersonalContrata.csv")
    print("TA_PersonalCodigotrabajo",datetime.now())
    descargar_archivo(TA_PersonalCodigotrabajo     , "respaldo/TA_PersonalCodigotrabajo.csv")
    print("TA_PersonalContratohonorarios",datetime.now())
    descargar_archivo(TA_PersonalContratohonorarios, "respaldo/TA_PersonalContratohonorarios.csv")
    #del TA_PersonalPlanta, TA_PersonalContrata, TA_PersonalCodigotrabajo, TA_PersonalContratohonorarios
    
    gc.collect()
    
    make_backup()
    print("deparar_partes1")
    separar_partes("respaldo/TA_PersonalPlanta.csv"            ,PersonalPlantaDICT            ,"d1","Planta")
    print("deparar_partes2")
    gc.collect()
    separar_partes("respaldo/TA_PersonalContrata.csv"          ,PersonalContrataDICT          ,"d2","Contrata")
    gc.collect()
    print("deparar_partes3")
    separar_partes("respaldo/TA_PersonalCodigotrabajo.csv"     ,PersonalCodigotrabajoDICT     ,"d3","Codigotrabajo")
    gc.collect()
    print("deparar_partes4")
    separar_partes("respaldo/TA_PersonalContratohonorarios.csv",PersonalContratohonorariosDICT,"d4","Contratohonorarios")
    gc.collect()
    
    for i in organismo["organismo"]:
        unir(i)

    """
    df1 = pd.read_csv(TA_PersonalPlanta, sep=";",encoding="latin",usecols=PersonalPlantaDICT)
    df1["base"] = "Planta"
    df2 = pd.read_csv(TA_PersonalContrata, sep=";",encoding="latin",usecols=PersonalContrataDICT)
    df2["base"] = "Contrata"
    df3 = pd.read_csv(TA_PersonalCodigotrabajo, sep=";",encoding="latin",usecols=PersonalCodigotrabajoDICT)
    df3["base"] = "Codigotrabajo"
    df4 = pd.read_csv(TA_PersonalContratohonorarios, sep=";",encoding="latin",usecols=PersonalContratohonorariosDICT)
    df4 = df.rename(columns={'remuneracionbruta': 'remuneracionbruta_mensual'})
    df4["base"] = "Contratohonorarios"
    df = pd.concat([df1,df2,df3,df4])
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = fr"respaldo/DB_{timestamp}.csv"
    # Guardar el DataFrame en un archivo CSV con compresión
    df.to_csv(filename, compression='xz', sep='\t', index=False)
    for i in df["organismo_nombre"].unique():
        aux = df[df["organismo_nombre"] == i]
        aux.to_csv(fr"organismo/{i}.csv", compression='xz', sep='\t', index=False)
    """


if __name__ == '__main__':
    GLOBAL()
    

