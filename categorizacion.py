import pandas as pd
import datetime
import re
from typing import Dict, Tuple, Optional, List
from pathlib import Path

def cargar_datos(ruta: str) -> pd.DataFrame:
    try:
        if not Path(ruta).is_file():
            raise FileNotFoundError(f"El archivo {ruta} no existe.")
        return pd.read_excel(ruta)
    except Exception as e:
        raise ValueError(f"Error al cargar el archivo {ruta}: {str(e)}")

def cargar_diccionario(ruta: str, columnas: list, llave: str, valor: str) -> Dict[str, str]:
    try:
        df = pd.read_excel(ruta, usecols=columnas).dropna()
        return dict(zip(df[llave], df[valor]))
    except Exception as e:
        raise ValueError(f"Error al cargar diccionario desde {ruta}: {str(e)}")
    
def renombrar_columnas(df: pd.DataFrame, diccionario: Dict[str, str]) -> pd.DataFrame:
    return df.rename(columns={k: v for k, v in diccionario.items() if k in df.columns})

def reemplazar_valores(df: pd.DataFrame, columna: str, diccionario: Dict[str, str]) -> pd.DataFrame:
    if columna in df.columns:
        df[columna] = df[columna].replace(diccionario)
    return df

def detectar_columna_antibioticos(df: pd.DataFrame, antibioticos: pd.DataFrame, variantes: list) -> Optional[str]:
    columnas_en_data = set(df.columns)
    for variante in variantes:
        posibles_ab = set(antibioticos[variante].dropna())
        if len(posibles_ab & columnas_en_data) > 0:
            return variante
    return None

def detectar_columna_especies(df: pd.DataFrame, especies: pd.DataFrame, columnas_codigo: list) -> Optional[str]:
    posibles_valores = set(df.values.ravel())
    for col_mapeo in columnas_codigo:
        codigos = set(especies[col_mapeo].dropna())
        if len(codigos & posibles_valores) > 0:
            return col_mapeo
    return None

def corregir_celdas_convertidas_a_fecha(df: pd.DataFrame, columnas: list) -> pd.DataFrame:
    for col in columnas:
        if col in df.columns:
            def corregir_valor(valor):
                if isinstance(valor, (pd.Timestamp, datetime.datetime)):
                    return f"{valor.day}/{valor.month}"
                return valor
            df[col] = df[col].apply(corregir_valor)
    return df

def agregar_columnas_fijas(df: pd.DataFrame, hospital: str, region:str) -> pd.DataFrame:
    df['Hospital'] = hospital
    df['Region'] = region
    return df

def eliminar_columnas_no_deseadas(df: pd.DataFrame, columnas: List[str]) -> pd.DataFrame:
    columnas_eliminadas = [col for col in columnas if col in df.columns]
    return df.drop(columns=columnas_eliminadas, errors='ignore')

def limpiar_valores_antibioticos(df: pd.DataFrame, columnas_antibioticos: List[str], valores_a_reemplazar: List[str]) -> pd.DataFrame:
    columnas_presentes = [col for col in columnas_antibioticos if col in df.columns]
    df[columnas_presentes] = df[columnas_presentes].replace(valores_a_reemplazar, pd.NA)
    return df

def agregar_columnas_mapeadas(df: pd.DataFrame, ruta_mapeo: str, columna_base: str) -> pd.DataFrame:
    mapeo_especies = cargar_datos(ruta_mapeo)
    dicc_grupo_general = dict(zip(mapeo_especies['Especie_especifica'], mapeo_especies['Grupo_general']))
    dicc_grupo_principal = dict(zip(mapeo_especies['Especie_especifica'], mapeo_especies['Grupo_principal']))
        
    if columna_base in df.columns:
        df['Grupo_general'] = df[columna_base].map(dicc_grupo_general)
        df['Grupo_principal'] = df[columna_base].map(dicc_grupo_principal)

    return df

def limpiar_valores_mic(df: pd.DataFrame, columnas_antibioticos: List[str]) -> pd.DataFrame:
    def transformar_mic(valor):
        if pd.isna(valor):
            return pd.NA
        valor = str(valor).strip()
        # Eliminar fracciones (por ejemplo: 16/4 → 16)
        if '/' in valor:
            valor = valor.split('/')[0].strip()
        try:
            if valor.startswith('>=') or valor.startswith('<=') or valor.startswith('='):
                num = float(valor.replace('>=', '').replace('<=', '').replace('=', '').replace(',', '.'))
                return num
            elif valor.startswith('>'):
                num = float(valor[1:].replace(',', '.'))
                return num * 2
            elif valor.startswith('<'):
                num = float(valor[1:].replace(',', '.'))
                return num / 2
            else:
                return float(valor.replace(',', '.'))
        except (ValueError, TypeError):
            return pd.NA

    columnas_presentes = [col for col in columnas_antibioticos if col in df.columns]
    for col in columnas_presentes:
        df[col] = df[col].apply(transformar_mic)
    return df

def cargar_puntos_corte_clsi(ruta_clsi: str) -> Tuple[Dict, Dict]:
    
    try:
        # Cargar archivo de puntos de corte
        clsi_df = pd.read_excel(
            ruta_clsi,
            usecols=['Antibiotico', 'Grupo_general', 'Grupo/Especie especifica',
                     'CLSI <=S', 'CLSI =I/SDD', 'CLSI >=R',
                     'CLSI >=S', 'CLSI =I', 'CLSI <=R']
        )
        
        # Funciones auxiliares (mantenidas exactamente como en el código original)
        def limpiar_mic(valor):
            if pd.isna(valor):
                return None
            valor = str(valor).split('/')[0].strip().replace(',', '.')
            return valor
        
        def es_numero(valor):
            if valor is None:
                return False
            return re.match(r'^\d+(\.\d+)?$', valor) is not None
        
        def limpiar_intermedio(valor):
            if pd.isna(valor):
                return None
            valor = str(valor).strip().replace(',', '.')
            if re.match(r'^\d+(\.\d+)?-\d+(\.\d+)?$', valor):
                return 'rango'
            if es_numero(valor):
                return float(valor)
            return None
        
        # Diccionarios de puntos de corte
        puntos_corte_clasico = {}
        puntos_corte_alterno = {}
        
        for _, fila in clsi_df.iterrows():
            antibiotico = str(fila['Antibiotico']).strip()
            grupo_general = str(fila['Grupo_general']).strip()
            grupo_especifico = str(fila['Grupo/Especie especifica']).strip()
            
            # Detectar si es un caso de exclusión
            modo_exclusion = False
            especies_excluidas = []
            especies_incluidas = []
            if grupo_especifico.lower().startswith("diferente:"):
                modo_exclusion = True
                especies_str = grupo_especifico.split(":", 1)[1].strip()
                especies_excluidas = [e.strip() for e in especies_str.split(",") if e.strip()]
            elif grupo_especifico.lower() != 'nan' and grupo_especifico.strip() != "":
                especies_incluidas = [e.strip() for e in grupo_especifico.split(",") if e.strip()]
            
            # --- Método clásico ---
            s = limpiar_mic(fila['CLSI <=S'])
            i = limpiar_intermedio(fila['CLSI =I/SDD'])
            r = limpiar_mic(fila['CLSI >=R'])
            if es_numero(s) and es_numero(r):
                if modo_exclusion:
                    puntos_corte_clasico[(antibiotico, grupo_general, True, tuple(especies_excluidas))] = (float(s), i, float(r))
                else:
                    for especie in especies_incluidas:
                        puntos_corte_clasico[(antibiotico, especie, False, None)] = (float(s), i, float(r))
                    puntos_corte_clasico[(antibiotico, grupo_general, False, None)] = (float(s), i, float(r))
            
            # --- Método alterno ---
            s_alt = limpiar_mic(fila['CLSI >=S'])
            i_alt = limpiar_intermedio(fila['CLSI =I'])
            r_alt = limpiar_mic(fila['CLSI <=R'])
            if es_numero(s_alt) and es_numero(r_alt):
                if modo_exclusion:
                    puntos_corte_alterno[(antibiotico, grupo_general, True, tuple(especies_excluidas))] = (float(s_alt), i_alt, float(r_alt))
                else:
                    for especie in especies_incluidas:
                        puntos_corte_alterno[(antibiotico, especie, False, None)] = (float(s_alt), i_alt, float(r_alt))
                    puntos_corte_alterno[(antibiotico, grupo_general, False, None)] = (float(s_alt), i_alt, float(r_alt))
        
        return clsi_df, puntos_corte_clasico, puntos_corte_alterno
    
    except Exception as e:
        raise ValueError(f"Error al cargar puntos de corte CLSI desde {ruta_clsi}: {str(e)}")

def obtener_puntos_corte(antibiotico: str, especie: str, metodo: str = 'clasico', clsi_df: pd.DataFrame = None,
                         puntos_corte_clasico: Dict = None, puntos_corte_alterno: Dict = None) -> Optional[tuple]:
    
    diccionario = puntos_corte_clasico if metodo == 'clasico' else puntos_corte_alterno
    # 1. Coincidencia exacta por especie (sin exclusión)
    if (antibiotico, especie, False, None) in diccionario:
        return diccionario[(antibiotico, especie, False, None)]
    # 2. Buscar reglas de exclusión aplicables
    for (ab, grupo, modo_exc, excluidas), valores in diccionario.items():
        if ab == antibiotico and modo_exc and especie not in excluidas:
            # El grupo general debe coincidir
            if clsi_df is not None and (especie in clsi_df.loc[clsi_df['Grupo_general'] == grupo, 'Grupo/Especie especifica'].values or grupo == especie):
                return valores
    # 3. Coincidencia por grupo general (sin exclusión)
    for (ab, grupo, modo_exc, _), valores in diccionario.items():
        if ab == antibiotico and not modo_exc and grupo == especie:
            return valores
    return None

def sigue_patron_dilucion_doble(valor):
    try:
        valor = float(valor)
        if valor <= 0:
            return False
        while valor < 1:
            valor *= 2
        while valor > 1:
            valor /= 2
        return abs(valor - 1) < 0.0001
    except:
        return False

def buscar_puntos_corte(diccionario: Dict, antibiotico: str, especie: str, grupo: str) -> Optional[tuple]:
    if antibiotico is None:
        return None
    ant = str(antibiotico).strip().lower()
    esp = str(especie).strip().lower() if (especie is not None) else ""
    grp = str(grupo).strip().lower() if (grupo is not None) else ""
    # 1) coincidencia exacta por especie (modo_exclusion False, k_excl is None)
    for key, val in diccionario.items():
        k_ant, k_target, k_modo, k_excl = key
        if str(k_ant).strip().lower() != ant:
            continue
        if (not bool(k_modo)) and (k_excl is None):
            if str(k_target).strip().lower() == esp:
                return val
    # 2) reglas de exclusión (modo_exclusion True) para el grupo
    for key, val in diccionario.items():
        k_ant, k_target, k_modo, k_excl = key
        if str(k_ant).strip().lower() != ant:
            continue
        if bool(k_modo) and str(k_target).strip().lower() == grp:
            # k_excl expected to be tuple/list of excluded species
            if k_excl is None:
                return val
            excluded_lower = [e.strip().lower() for e in list(k_excl)]
            if esp not in excluded_lower:
                return val
            # si esp está en excluded_lower -> no aplica esta regla de exclusión (se ignora)
    # 3) coincidencia por grupo general (modo_exclusion False)
    for key, val in diccionario.items():
        k_ant, k_target, k_modo, k_excl = key
        if str(k_ant).strip().lower() != ant:
            continue
        if (not bool(k_modo)) and (k_excl is None) and str(k_target).strip().lower() == grp:
            return val
    return None

def categorizar_mic(valor: any, antibiotico_col: str, especie: str, grupo: str, puntos_corte_clasico: Dict, puntos_corte_alterno: Dict) -> any:
    if pd.isna(valor):
        return valor
    try:
        mic = float(valor)
    except:
        return 'Inconcluyente'
    # decidir método según patrón (diluciones dobles -> clásico)
    es_clasico = sigue_patron_dilucion_doble(mic)
    if es_clasico:
        puntos = buscar_puntos_corte(puntos_corte_clasico, antibiotico_col, especie, grupo)
        if puntos is None:
            return valor  # sin puntos de corte, dejamos el valor original
        s, i, r = puntos
        # Clasificación (método clásico: MIC <= S -> S, MIC >= R -> R, entre -> I si i existe)
        if mic <= s:
            return 'S'
        elif mic >= r:
            return 'R'
        elif (i not in [None, '']):
            if isinstance(i, (int, float)):  # si es un número, comparar igualdad exacta
                if mic == i:
                    return 'I'
            else:  # si no es numérico, usar el rango original
                if s < mic < r:
                    return 'I'
        else:
            return 'Inconcluyente'
    else:
        puntos = buscar_puntos_corte(puntos_corte_alterno, antibiotico_col, especie, grupo)
        if puntos is None:
            return valor
        s, i, r = puntos
        # Clasificación (método alterno: MIC >= S -> S, MIC <= R -> R, entre -> I si i existe)
        if mic >= s:
            return 'S'
        elif mic <= r:
            return 'R'
        elif (i not in [None, '']) and (r < mic < s):
            return 'I'
        else:
            return 'Inconcluyente'

def categorizar_dataframe(data: pd.DataFrame, puntos_corte_clasico: Dict, puntos_corte_alterno: Dict) -> pd.DataFrame:
    df_categorizado = data.copy()
    columnas_ab = [
        col for col in data.columns
        if col not in ['Grupo_general', 'fecha', 'especie', 'Hospital', 'Region', 
                       'Grupo_principal', 'Tipo de localizacion', 'Tipo de muestra', 
                       'SPEC_NUM', 'Edad']
    ]
    for col in columnas_ab:
        for idx, row in data.iterrows():
            grupo = row['Grupo_general']
            especie = row['especie']
            mic_valor = row[col]
            antibiotico_col = col  # debe coincidir con los nombres usados en Lista_CLSI_test
            categoria = categorizar_mic(mic_valor, antibiotico_col, especie, grupo, puntos_corte_clasico, puntos_corte_alterno)
            df_categorizado.at[idx, col] = categoria
    return df_categorizado

def procesar_dataset(df: pd.DataFrame, ruta_diccionarios: str) -> Tuple[pd.DataFrame, Dict[str, Dict]]:
    
    # Diccionarios para almacenar resultados
    diccionarios = {}
    
    # 1. Renombrar columnas principales
    dicc_variables = cargar_diccionario(
        ruta_diccionarios, 
        columnas=['variable_original', 'variable_nueva'],
        llave='variable_original',
        valor='variable_nueva'
    )
    data = renombrar_columnas(df, dicc_variables)
    
    # 2. Reemplazar valores de tipo de muestra
    dicc_muestras = cargar_diccionario(
        ruta_diccionarios,
        columnas=['codigo_muestra', 'nombre_muestra'],
        llave='codigo_muestra',
        valor='nombre_muestra'
    )
    data = reemplazar_valores(data, 'Tipo de muestra', dicc_muestras)
    diccionarios['dicc_muestras'] = dicc_muestras
    
    # 3. Reemplazar valores de tipo de servicio
    dicc_localizacion = cargar_diccionario(
        ruta_diccionarios,
        columnas=['codigo_localizacion', 'nombre_localizacion'],
        llave='codigo_localizacion',
        valor='nombre_localizacion'
    )
    data = reemplazar_valores(data, 'Tipo de localizacion', dicc_localizacion)
    diccionarios['dicc_localizacion'] = dicc_localizacion
    
    # 4. Procesar antibióticos
    antibioticos = cargar_datos(ruta_diccionarios)[['antibiotico_1', 'antibiotico_2', 'antibiotico_3', 'antibiotico_4', 'antibiotico']]
    variantes = ['antibiotico_1', 'antibiotico_2', 'antibiotico_3', 'antibiotico_4']
    columna_usada = detectar_columna_antibioticos(data, antibioticos, variantes)
    
    dicc_antibioticos = {}
    if columna_usada:
        dicc_antibioticos = cargar_diccionario(
            ruta_diccionarios,
            columnas=[columna_usada, 'antibiotico'],
            llave=columna_usada,
            valor='antibiotico'
        )
        data = renombrar_columnas(data, dicc_antibioticos)
    diccionarios['dicc_antibioticos'] = dicc_antibioticos
    
    # 5. Procesar especies
    especies = cargar_datos(ruta_diccionarios)[['especie_1', 'especie_2', 'especie_3', 'especie']]
    columnas_codigo = ['especie_1', 'especie_2', 'especie_3']
    columna_usada = detectar_columna_especies(data, especies, columnas_codigo)
    
    dicc_especies = {}
    if columna_usada:
        dicc_especies = cargar_diccionario(
            ruta_diccionarios,
            columnas=[columna_usada, 'especie'],
            llave=columna_usada,
            valor='especie'
        )
        data = reemplazar_valores(data, 'especie', dicc_especies)
    diccionarios['dicc_especies'] = dicc_especies
    
    return data, diccionarios

# Ejecución principal
def procesar_categorizacion(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict, pd.DataFrame, Dict, Dict]:
    #RUTA_DATOS = '/Users/zahir/Documents/Bases de datos INS/Honorio Delgado  Arequipa 2023.xlsx'
    RUTA_DICCIONARIOS = 'data/Lista_antimicrobianos.xlsx'
    RUTA_MAPEO_ESPECIES = 'data/Lista_especie_especifico_general.xlsx'
    RUTA_CLSI = 'data/Lista_CLSI_completa.xlsx'

    # Procesar dataset
    data_procesado, diccionarios = procesar_dataset(df, RUTA_DICCIONARIOS)
    
    # Corregir fechas en columnas de antibióticos
    columnas_antibioticos = [c for c in diccionarios['dicc_antibioticos'].values() if c in data_procesado.columns]
    data_corregido = corregir_celdas_convertidas_a_fecha(data_procesado, columnas_antibioticos)
    
    # Filtrar columnas finales
    columnas_a_conservar = ['fecha', 'SPEC_NUM', 'Tipo de localizacion', 'Tipo de muestra', 'Edad', 'especie'] + columnas_antibioticos
    columnas_existentes = [col for col in columnas_a_conservar if col in data_corregido.columns]
    data_filtrada = data_corregido[columnas_existentes]
    
    # Agregar columnas fijas
    data_filtrada = agregar_columnas_fijas(data_filtrada, 'Hospital Honorio Delgado Arequipa', 'Arequipa')

    # Eliminar columnas no deseadas
    columnas_a_eliminar = [
        'BLEE',
        'Gentamicina de nivel alto (sinergia)',
        'Estreptomicina de nivel alto (sinergia)',
        'Resistencia inducible a clindamicina',
        'Deteccion de cefoxitina'
    ]
    data_filtrada = eliminar_columnas_no_deseadas(data_filtrada, columnas_a_eliminar)

    # Limpiar valores no deseados en columnas de antibióticos
    valores_a_reemplazar = ['TRM', 'R/N', 'NEG']
    data_filtrada = limpiar_valores_antibioticos(data_filtrada, columnas_antibioticos, valores_a_reemplazar)
    
    # Agregar columnas mapeadas
    data_filtrada = agregar_columnas_mapeadas(data_filtrada, RUTA_MAPEO_ESPECIES, 'especie')

    # Limpiar valores MIC en columnas de antibióticos
    data_filtrada = limpiar_valores_mic(data_filtrada, columnas_antibioticos)

    # Cargar puntos de corte CLSI
    clsi_df, puntos_corte_clasico, puntos_corte_alterno = cargar_puntos_corte_clsi(RUTA_CLSI)

    # Categorizar valores MIC
    data_filtrada = categorizar_dataframe(data_filtrada, puntos_corte_clasico, puntos_corte_alterno)

    return data_filtrada, diccionarios, clsi_df, puntos_corte_clasico, puntos_corte_alterno

