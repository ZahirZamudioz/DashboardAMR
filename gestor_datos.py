import pandas as pd
import pickle
import os
from categorizacion import procesar_categorizacion
from limpieza_final import procesar_limpieza_final

# Directorio para almacenar datos procesados
if os.getenv("RENDER"):  # Render define esta variable de entorno automáticamente
    DATA_DIR = "/tmp/data_pkl"
else:
    DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

os.makedirs(DATA_DIR, exist_ok=True)

def procesar_archivo_subido(contenido, anio):
    # Convertir contenido a DataFrame
    df = pd.read_excel(pd.ExcelFile(pd.io.common.BytesIO(contenido)))
    print(f"Procesando archivo para el año {anio}...")
    
    # Procesar con Categorizacion.py
    data_categorizado, _, _, _, _ = procesar_categorizacion(df)

    # Procesar con LimpiezaFinal.py
    data_limpia = procesar_limpieza_final(data_categorizado)
    
    # Guardar DataFrame procesado
    guardar_datos(data_limpia, anio)
    
    print(f"Datos guardados para el año {anio}")
    return data_limpia

def guardar_datos(df, anio):
    archivo = os.path.join(DATA_DIR, f'datos_{anio}.pkl')
    with open(archivo, 'wb') as f:
        pickle.dump(df, f)
    print(f"Archivo guardado: {archivo}")

def cargar_datos(anio):
    archivo = os.path.join(DATA_DIR, f'datos_{anio}.pkl')
    if os.path.exists(archivo):
        with open(archivo, 'rb') as f:
            df = pickle.load(f)
        print(f"Datos cargados para el año {anio}")
        return df
    print(f"No se encontraron datos para el año {anio}")
    return None

def obtener_anios_disponibles():
    if not os.path.exists(DATA_DIR):
        return []
    
    archivos = [f for f in os.listdir(DATA_DIR) if f.startswith('datos_') and f.endswith('.pkl')]
    anios = [int(f.split('_')[1].split('.')[0]) for f in archivos]
    print(f"Años disponibles: {sorted(anios)}")
    return sorted(anios)

