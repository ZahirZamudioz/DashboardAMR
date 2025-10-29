import pandas as pd
import numpy as np
import plotly.express as px
from dash import Dash, callback, dcc, html, Input, Output, dash_table
import dash_bootstrap_components as dbc
import base64
import plotly.graph_objects as go
from gestor_datos import procesar_archivo_subido, obtener_anios_disponibles, cargar_datos
from dash import State

# --- CONFIGURACIONES GLOBALES ---
app = Dash(__name__, external_stylesheets=[dbc.themes.FLATLY], suppress_callback_exceptions=True)
server = app.server   # 👈 esto es lo que Render necesita
# Componente de carga
upload_section = html.Div([
    html.H3("Cargar nuevos datos", className="mb-3"),
    dcc.Upload(
        id="upload-data",
        children=html.Div([
            html.I(className="fas fa-file-excel me-2"),
            "Seleccionar archivo Excel"
        ], className="btn btn-outline-primary"),
        multiple=False,
        style={"margin": "10px 0"}
    ),
    dbc.Row([
        dbc.Col([
            dcc.Input(
                id="input-year",
                type="number",
                placeholder="Ingrese el año (ej. 2023)",
                min=2000,
                max=2030,
                value=2023,
                className="form-control"
            )
        ], width=6),
        dbc.Col([
            dbc.Button("Procesar datos", id="btn-process", color="primary", className="w-100")
        ], width=6)
    ], className="mb-2"),
    html.Div(id="upload-status", className="alert alert-info mt-2", style={"display": "none"})
], className="card p-3 mb-4")

# Colores fijos para especies
colores_especies = {
    "Acinetobacter baumannii": "blue",
    "Klebsiella pneumoniae": "orange",
    "Escherichia coli": "red",
    "Pseudomonas aeruginosa": "green",
    "Staphylococcus aureus": "black"
}

# Especies de interés
especies_fijas = [
    "Acinetobacter baumannii", 
    "Klebsiella pneumoniae", 
    "Escherichia coli", 
    "Pseudomonas aeruginosa", 
    "Staphylococcus aureus"
]

# Ordenar meses manualmente
orden_meses = [
    "Enero-2023", "Febrero-2023", "Marzo-2023", "Abril-2023", "Mayo-2023", "Junio-2023",
    "Julio-2023", "Agosto-2023", "Septiembre-2023", "Octubre-2023", "Noviembre-2023", "Diciembre-2023"
]

# ------- CARGA DEL DATASET PRINCIPAL -------
#df_original = pd.read_excel("/Users/zahir/Downloads/BD_Categorizada_2023.xlsx")

# Carga dinámica - se actualizará con callback
df_actual = None
anio_actual = 2023
antibioticos = []
fig_resistencia = go.Figure()

def generar_todos_graficos():
    """Regenera todos los gráficos con df_actual"""
    global fig_localizacion, fig_muestra, fig_edad, fig_servicio_muestras
    global fig_muestra_especies, fig3, fig_heatmap_pos, fig_heatmap_neg, fig_resistencia
    global df_grafLineas, tabla_localizacion, tabla_muestra, tabla_servicio_muestras
    global tabla_muestra_especies, antibioticos

    if df_actual is None:
        return
    
    # ------- TRANSFORMACIONES DE DATOS -------
    # Sección 1: Transformación de datos para gráfico de lineas
    def calcular_conteos_porcentajes(data_filtrada):
        columnas_id = ['fecha', 'especie', 'Grupo_principal']
        columnas_antibioticos = [col for col in data_filtrada.columns if col not in ['fecha', 'Region', 'Hospital', 'SPEC_NUM', 'Tipo de localizacion',
                                'Tipo de muestra', 'Edad', 'especie', 'Grupo_general', 'Grupo_principal']]
        
        df_melted = pd.melt(data_filtrada,
                            id_vars=columnas_id,
                            value_vars=columnas_antibioticos,
                            var_name='antibiotico',
                            value_name='CLSI_categoria')
        
        count_table = pd.pivot_table(df_melted,
                                    index=['fecha', 'Grupo_principal','especie', 'antibiotico'],
                                    columns='CLSI_categoria',
                                    aggfunc='size',
                                    fill_value=0)
        
        count_table = count_table.reset_index()
        count_table.index.names = ['Index']
        
        for col in ['I', 'R', 'S', 'Inconcluyente']:
            if col not in count_table.columns:
                count_table[col] = 0
        
        count_table['total'] = count_table['I'] + count_table['R'] + count_table['S'] + count_table['Inconcluyente']
        count_table['I (%)'] = (count_table['I'] / count_table['total'] * 100).round(2)
        count_table['R (%)'] = (count_table['R'] / count_table['total'] * 100).round(2)
        count_table['S (%)'] = (count_table['S'] / count_table['total'] * 100).round(2)
        count_table['Inconcluyente (%)'] = (count_table['Inconcluyente'] / count_table['total'] * 100).round(2)
        
        return count_table

    count_table = calcular_conteos_porcentajes(df_actual)
    #count_table.to_excel("/Users/zahir/Downloads/CountCount.xlsx", index=False)
    df_grafLineas = count_table[count_table['total'] >= 10]
    df_grafLineas["fecha"] = pd.Categorical(df_grafLineas["fecha"], categories=orden_meses, ordered=True)
    antibioticos = sorted(df_grafLineas["antibiotico"].dropna().unique())

    # Sección 2: Transformación de datos para gráfico de barras ailados por especie
    def transformar_datos_para_aislados_barras(data_filtrada):
        # Agrupar y contar aislados por especie
        if "Grupo_principal" in data_filtrada.columns:
            conteo_especies = (
                data_filtrada.groupby(["especie", "Grupo_principal"])
                .size()
                .reset_index(name="aislados")
            )
        else:
            conteo_especies = (
                data_filtrada.groupby("especie")
                .size()
                .reset_index(name="aislados")
            )

        # Ordenar especies de mayor a menor
        conteo_especies = conteo_especies.sort_values("aislados", ascending=False)

        orden_especies = (
            conteo_especies["especie"]
            .astype(str).str.strip()
            .drop_duplicates()
            .tolist()
        )
        return conteo_especies, orden_especies

    conteo_especies, orden_especies = transformar_datos_para_aislados_barras(df_actual)

    # Sección 3: Transformación de datos para el gráfico heatmap porcentaje de resistencia por especie y antibiótico
    def transformar_datos_para_heatmap(data_filtrada):
        # Dividir los datos en Gram positivas y Gram negativas
        gram_positiva = data_filtrada[data_filtrada["Grupo_principal"] == "Gram positiva"]
        gram_negativa = data_filtrada[data_filtrada["Grupo_principal"] == "Gram negativa"]

        # Función auxiliar para procesar cada grupo
        # Agrupar por especie y antibiótico, sumar counts, y calcular todos los porcentajes globales
        def procesar_grupo(df_grupo, orden_especies):
            df_heatmap_grouped = (
                df_grupo.groupby(["especie", "antibiotico"])
                .agg({"R": "sum", "S": "sum", "I": "sum", "Inconcluyente": "sum", "total": "sum"})
                .reset_index()
            )

            # Calcular porcentajes, redondeando a 1 decimal
            df_heatmap_grouped["R (%)"] = (df_heatmap_grouped["R"] / df_heatmap_grouped["total"] * 100).round(1)
            df_heatmap_grouped["S (%)"] = (df_heatmap_grouped["S"] / df_heatmap_grouped["total"] * 100).round(1)
            df_heatmap_grouped["I (%)"] = (df_heatmap_grouped["I"] / df_heatmap_grouped["total"] * 100).round(1)
            df_heatmap_grouped["Inconcluyente (%)"] = (df_heatmap_grouped["Inconcluyente"] / df_heatmap_grouped["total"] * 100).round(1)

            # Pivotar para porcentajes
            pivot_R = df_heatmap_grouped.pivot_table(
                index="especie",
                columns="antibiotico",
                values="R (%)",
                aggfunc="first"
            ).fillna("")  # Blanks para NaN

            pivot_S = df_heatmap_grouped.pivot_table(
                index="especie",
                columns="antibiotico",
                values="S (%)",
                aggfunc="first"
            ).fillna(0)

            pivot_I = df_heatmap_grouped.pivot_table(
                index="especie",
                columns="antibiotico",
                values="I (%)",
                aggfunc="first"
            ).fillna(0)

            pivot_Inconcluyente = df_heatmap_grouped.pivot_table(
                index="especie",
                columns="antibiotico",
                values="Inconcluyente (%)",
                aggfunc="first"
            ).fillna(0)

            # Ordenar las filas del heatmap usando el mismo orden de especies del gráfico de barras
            pivot_R = pivot_R.reindex(index=orden_especies)[sorted(pivot_R.columns)]

            # Asegurar que los otros pivots estén alineados
            pivot_S = pivot_S.reindex(index=pivot_R.index, columns=pivot_R.columns).fillna(0)
            pivot_I = pivot_I.reindex(index=pivot_R.index, columns=pivot_R.columns).fillna(0)
            pivot_Inconcluyente = pivot_Inconcluyente.reindex(index=pivot_R.index, columns=pivot_R.columns).fillna(0)

            # Pivotar los conteos originales
            pivot_R_count = df_heatmap_grouped.pivot_table(
                index="especie",
                columns="antibiotico",
                values="R",
                aggfunc="first"
            ).reindex(index=pivot_R.index, columns=pivot_R.columns).fillna(0)

            pivot_S_count = df_heatmap_grouped.pivot_table(
                index="especie",
                columns="antibiotico",
                values="S",
                aggfunc="first"
            ).reindex(index=pivot_R.index, columns=pivot_R.columns).fillna(0)

            pivot_I_count = df_heatmap_grouped.pivot_table(
                index="especie",
                columns="antibiotico",
                values="I",
                aggfunc="first"
            ).reindex(index=pivot_R.index, columns=pivot_R.columns).fillna(0)

            pivot_Inconcluyente_count = df_heatmap_grouped.pivot_table(
                index="especie",
                columns="antibiotico",
                values="Inconcluyente",
                aggfunc="first"
            ).reindex(index=pivot_R.index, columns=pivot_R.columns).fillna(0)

            return (pivot_R, pivot_S, pivot_I, pivot_Inconcluyente,
                    pivot_R_count, pivot_S_count, pivot_I_count, pivot_Inconcluyente_count)
        
        # Obtener el orden de especies por grupo
        orden_positivas = conteo_especies[conteo_especies["Grupo_principal"] == "Gram positiva"]["especie"].drop_duplicates().tolist()
        orden_negativas = conteo_especies[conteo_especies["Grupo_principal"] == "Gram negativa"]["especie"].drop_duplicates().tolist()

        # Procesar ambos grupos
        pivots_positivas = procesar_grupo(gram_positiva, orden_positivas)
        pivots_negativas = procesar_grupo(gram_negativa, orden_negativas)

        return pivots_positivas, pivots_negativas

    # Ejecutar la función con count_table
    (pivots_positivas, pivots_negativas) = transformar_datos_para_heatmap(count_table)

    # Desempaquetar los pivots
    (pivot_R_pos, pivot_S_pos, pivot_I_pos, pivot_Inconcluyente_pos,
    pivot_R_count_pos, pivot_S_count_pos, pivot_I_count_pos, pivot_Inconcluyente_count_pos) = pivots_positivas

    (pivot_R_neg, pivot_S_neg, pivot_I_neg, pivot_Inconcluyente_neg,
    pivot_R_count_neg, pivot_S_count_neg, pivot_I_count_neg, pivot_Inconcluyente_count_neg) = pivots_negativas

    # Sección 4: Transformación de datos para el grafico de barras y tabla frecuencia de muestras por servicio
    def trasformar_datos_tipo_de_servicio(data_filtrada):
        # Crear una copia para no modificar el DataFrame original
        df_unicos = data_filtrada.copy()

        # Eliminar duplicados basados en SPEC_NUM, barajando aleatoriamente
        if "SPEC_NUM" in df_unicos.columns:
            df_unicos = df_unicos.sample(frac=1, random_state=42).drop_duplicates("SPEC_NUM", keep="first")

        # Calcular conteo y porcentaje para "Tipo de localización"
        if "Tipo de localizacion" in df_unicos.columns:
            conteo_servicio = df_unicos.groupby("Tipo de localizacion").size().reset_index(name="n")
            conteo_servicio["Porcentaje"] = (conteo_servicio["n"] / conteo_servicio["n"].sum() * 100).round(2)
            conteo_servicio = conteo_servicio.sort_values("Porcentaje", ascending=False)

            # Crear DataFrame para la tabla con la fila de total
            total_row = pd.DataFrame({
                "Tipo de localizacion": ["Total"],
                "n": [conteo_servicio["n"].sum()],
                "Porcentaje": [100.0]
            })
            conteo_servicio_tabla = pd.concat([conteo_servicio, total_row], ignore_index=True)
        else:
            conteo_servicio = pd.DataFrame(columns=["Tipo de localizacion", "n", "Porcentaje"])
            conteo_servicio_tabla = conteo_servicio
        
        return conteo_servicio, conteo_servicio_tabla, df_unicos

    # Ejecutar la función con df_original
    conteo_servicio, conteo_servicio_tabla, df_unicos = trasformar_datos_tipo_de_servicio(df_actual)

    # Sección 5: Transformación de datos para el gráfico de barras y tabla porcentaje por tipo de muestra
    def transformar_datos_para_frecuencia_tipo_muestra(df_unicos):
        # Calcular conteo y porcentaje para "Tipo de muestra"
        if "Tipo de muestra" in df_unicos.columns:
            conteo_muestra = df_unicos.groupby("Tipo de muestra").size().reset_index(name="n")
            conteo_muestra["Porcentaje"] = (conteo_muestra["n"] / conteo_muestra["n"].sum() * 100).round(2)
            conteo_muestra = conteo_muestra.sort_values("Porcentaje", ascending=False)

            # Identificar categorías infrecuentes (porcentaje < 1%)
            infrecuentes = conteo_muestra[(conteo_muestra["Porcentaje"] < 1.0) & (conteo_muestra["Tipo de muestra"] != "Otros")]

            if not infrecuentes.empty:
                # Calcular total de muestras y porcentaje de categorías infrecuentes
                total_infrecuentes_n = infrecuentes["n"].sum()
                total_infrecuentes_pct = infrecuentes["Porcentaje"].sum()
                
                # Crear fila para "Muestras infrecuentes"
                muestras_infrecuentes_row = pd.DataFrame({
                    "Tipo de muestra": ["Muestras infrecuentes"],
                    "n": [total_infrecuentes_n],
                    "Porcentaje": [round(total_infrecuentes_pct, 2)]
                })
                
                # Filtrar solo categorías frecuentes (>= 1%)
                categorias_frecuentes = conteo_muestra[(conteo_muestra["Porcentaje"] >= 1.0) | (conteo_muestra["Tipo de muestra"] == "Otros")]
                
                # Combinar categorías frecuentes con "Muestras infrecuentes"
                conteo_muestra = pd.concat([categorias_frecuentes, muestras_infrecuentes_row], ignore_index=True)

            # Ordenar por porcentaje descendente
            conteo_muestra = conteo_muestra.sort_values("Porcentaje", ascending=False).reset_index(drop=True)

            # Crear DataFrame para la tabla (misma lógica que para el gráfico)
            conteo_muestra_tabla = conteo_muestra.copy()

            # Crear DataFrame para la tabla con la fila de total
            total_row = pd.DataFrame({
                "Tipo de muestra": ["Total"],
                "n": [conteo_muestra["n"].sum()],
                "Porcentaje": [100.0]
            })
            conteo_muestra_tabla = pd.concat([conteo_muestra, total_row], ignore_index=True)
        else:
            conteo_muestra = pd.DataFrame(columns=["Tipo de muestra", "n", "Porcentaje"])
            conteo_muestra_tabla = conteo_muestra

        return conteo_muestra, conteo_muestra_tabla

    conteo_muestra, conteo_muestra_tabla = transformar_datos_para_frecuencia_tipo_muestra(df_unicos)

    # Sección 6: Transformación de datos para gráfico de barras de muestras por rango de edad
    def transformar_datos_para_edad(df_unicos):
    # Función para convertir edades a años
        def convertir_edad(edad):
            if pd.isna(edad) or not isinstance(edad, str):
                return None
            edad_str = str(edad).strip().upper()  # Normalizar a mayúsculas y quitar espacios
            if 'M' in edad_str:
                try:
                    meses = float(edad_str.replace('M', ''))
                    return meses / 12  # Convertir meses a años
                except ValueError:
                    return None
            elif 'D' in edad_str:
                try:
                    dias = float(edad_str.replace('D', ''))
                    return dias / 365  # Convertir días a años
                except ValueError:
                    return None
            else:
                try:
                    return float(edad_str)
                except ValueError:
                    return None

        # Aplicar la conversión a la columna "Edad"
        df_unicos['Edad_num'] = df_unicos['Edad'].apply(convertir_edad)

        # Definir rangos de edad
        rangos_edad = [
            "Neonatal", "1-5 meses", "6-11 meses", "1-2 años", "2-4 años", "5-9 años",
            "10-14 años", "15-19 años", "20-24 años", "25-29 años", "30-34 años",
            "35-39 años", "40-44 años", "45-49 años", "50-54 años", "55-59 años",
            "60-64 años", "65-69 años", "70-74 años", "75-79 años", "80-84 años",
            "85-89 años", "90-94 años", "≥95 años"
        ]

        # Función para asignar rango de edad
        def asignar_rango(edad):
            if pd.isna(edad):
                return None
            elif edad <= 1/12:  # Menos de 1 mes (aprox. 28 días)
                return "Neonatal"
            elif 1/12 < edad <= 5/12:
                return "1-5 meses"
            elif 5/12 < edad <= 11/12:
                return "6-11 meses"
            elif 1 <= edad <= 2:
                return "1-2 años"
            elif 2 < edad <= 4:
                return "2-4 años"
            elif 5 <= edad <= 9:
                return "5-9 años"
            elif 10 <= edad <= 14:
                return "10-14 años"
            elif 15 <= edad <= 19:
                return "15-19 años"
            elif 20 <= edad <= 24:
                return "20-24 años"
            elif 25 <= edad <= 29:
                return "25-29 años"
            elif 30 <= edad <= 34:
                return "30-34 años"
            elif 35 <= edad <= 39:
                return "35-39 años"
            elif 40 <= edad <= 44:
                return "40-44 años"
            elif 45 <= edad <= 49:
                return "45-49 años"
            elif 50 <= edad <= 54:
                return "50-54 años"
            elif 55 <= edad <= 59:
                return "55-59 años"
            elif 60 <= edad <= 64:
                return "60-64 años"
            elif 65 <= edad <= 69:
                return "65-69 años"
            elif 70 <= edad <= 74:
                return "70-74 años"
            elif 75 <= edad <= 79:
                return "75-79 años"
            elif 80 <= edad <= 84:
                return "80-84 años"
            elif 85 <= edad <= 89:
                return "85-89 años"
            elif 90 <= edad <= 94:
                return "90-94 años"
            elif edad >= 95:
                return "≥95 años"
            return None

        # Asignar rangos de edad
        df_unicos['Rango_edad'] = df_unicos['Edad_num'].apply(asignar_rango)

        # Calcular conteo por rango de edad
        conteo_edad = df_unicos.groupby('Rango_edad').size().reset_index(name='n')
        # Ordenar por rango de edad (de menor a mayor edad) corrigiendo el error
        conteo_edad['Orden'] = conteo_edad['Rango_edad'].apply(lambda x: rangos_edad.index(x) if x in rangos_edad else len(rangos_edad))
        conteo_edad = conteo_edad.sort_values('Orden').drop(columns=['Orden'])

        return conteo_edad

    conteo_edad = transformar_datos_para_edad(df_unicos)

    #Sección 6: Transformación de datos para gráfico de barras apiladas y tabla de tipo de muestra por servicio
    def transformar_datos_para_muestras_por_servicio(df_unicos):
        # Crear una copia para no modificar el DataFrame original
        df_temp = df_unicos.copy()

        # Identificar tipos de muestra poco frecuentes (<1%)
        conteo_muestras = df_temp.groupby('Tipo de muestra').size().reset_index(name='Conteo')
        conteo_muestras['Porcentaje'] = (conteo_muestras['Conteo'] / conteo_muestras['Conteo'].sum() * 100).round(2)
        muestras_infrecuentes = conteo_muestras[conteo_muestras['Porcentaje'] < 1]['Tipo de muestra'].tolist()
        df_temp['Tipo de muestra'] = df_temp['Tipo de muestra'].replace(muestras_infrecuentes, 'muestras infrecuentes')

        # Crear tabla pivot para conteo por servicio y tipo de muestra
        conteo_servicio_muestras = df_temp.pivot_table(
            index=['Tipo de localizacion', 'Tipo de muestra'],
            aggfunc='size'
        ).reset_index(name='Conteo')

        # Calcular porcentajes por grupo de 'Tipo de localizacion'
        totales_por_servicio = conteo_servicio_muestras.groupby('Tipo de localizacion')['Conteo'].transform('sum')
        conteo_servicio_muestras['Porcentaje'] = (conteo_servicio_muestras['Conteo'] / totales_por_servicio * 100).round(2)

        # Ordenar 'Tipo de localizacion' por conteo total
        totales_servicios = conteo_servicio_muestras.groupby('Tipo de localizacion')['Conteo'].sum().sort_values(ascending=False)
        orden_servicios = totales_servicios.index.tolist()

        # Ordenar 'Tipo de muestra' por conteo total
        totales_tipos_muestra = conteo_servicio_muestras.groupby('Tipo de muestra')['Conteo'].sum().sort_values(ascending=False)
        orden_tipos_muestra = totales_tipos_muestra.index.tolist()

        # Convertir a tipos categóricos para ordenar la tabla
        conteo_servicio_muestras['Tipo de localizacion'] = pd.Categorical(
            conteo_servicio_muestras['Tipo de localizacion'],
            categories=orden_servicios,
            ordered=True
        )
        conteo_servicio_muestras['Tipo de muestra'] = pd.Categorical(
            conteo_servicio_muestras['Tipo de muestra'],
            categories=orden_tipos_muestra,
            ordered=True
        )
        conteo_servicio_muestras = conteo_servicio_muestras.sort_values(by=['Tipo de localizacion', 'Tipo de muestra'])

        return conteo_servicio_muestras, orden_servicios, orden_tipos_muestra, muestras_infrecuentes

    # Ejecutar la función con df_unicos
    conteo_servicio_muestras, orden_servicios, orden_tipos_muestra, muestras_infrecuentes = transformar_datos_para_muestras_por_servicio(df_unicos)

    # Sección 7: Transformación de datos para gráfico de barras apiladas y tabla de perfil de especies por tipo de muestra
    def transformar_datos_para_especies_por_muestra(df_actual, especies_infrecuentes):
        # Crear una copia para no modificar el DataFrame original
        df_temp = df_actual.copy()
        print(df_temp.shape)

        # Identificar tipos de muestra poco frecuentes (<3%)
        conteo_especie = df_temp.groupby('especie').size().reset_index(name='Conteo')
        conteo_especie['Porcentaje'] = (conteo_especie['Conteo'] / conteo_especie['Conteo'].sum() * 100).round(2)
        #conteo_especies = conteo_especies.sort_values(by="Conteo", ascending=False)
        print(conteo_especies)
        especies_infrecuentes = conteo_especie[conteo_especie['Porcentaje'] < 3]['especie'].tolist()

        # Reemplazar especies poco frecuentes
        df_temp['especie'] = df_temp['especie'].replace(especies_infrecuentes, 'otras especies')

        # Reemplazar tipos de muestra poco frecuentes
        df_temp['Tipo de muestra'] = df_temp['Tipo de muestra'].replace(muestras_infrecuentes, 'muestras infrecuentes')

        # Crear tabla pivote para conteo por tipo de muestra por especies
        conteo_muestra_especies = df_temp.pivot_table(
            index=['Tipo de muestra', 'especie'],
            aggfunc='size'
        ).reset_index(name='Conteo')

        # Calcular porcentajes por grupo de 'Tipo de muestra'
        totales_por_muestra = conteo_muestra_especies.groupby('Tipo de muestra')['Conteo'].transform('sum')
        conteo_muestra_especies['Porcentaje'] = (conteo_muestra_especies['Conteo'] / totales_por_muestra * 100).round(2)

        print(conteo_muestra_especies)
        # Ordenar 'Tipo de muestra' por conteo total
        totales_muestras = conteo_muestra_especies.groupby('Tipo de muestra')['Conteo'].sum().sort_values(ascending=False)
        orden_muestras = totales_muestras.index.tolist()

        # Ordenar 'especies' por conteo total
        totales_especies = conteo_muestra_especies.groupby('especie')['Conteo'].sum().sort_values(ascending=False)
        orden_de_especies = totales_especies.index.tolist()

        # Convertir a tipos categóricos para ordenar la tabla
        conteo_muestra_especies['Tipo de muestra'] = pd.Categorical(
            conteo_muestra_especies['Tipo de muestra'],
            categories=orden_muestras,
            ordered=True
        )
        conteo_muestra_especies['especie'] = pd.Categorical(
            conteo_muestra_especies['especie'],
            categories=orden_de_especies,
            ordered=True
        )
        conteo_muestra_especies = conteo_muestra_especies.sort_values(by=['Tipo de muestra', 'Porcentaje'], ascending=[True, False])

        return conteo_muestra_especies, orden_muestras, orden_de_especies

    # Ejecutar la función con df_unicos y muestras_infrecuentes (del código anterior)
    conteo_muestra_especies, orden_muestras, orden_de_especies = transformar_datos_para_especies_por_muestra(df_actual, muestras_infrecuentes)

    # ------- GENERACIÓN DE GRÁFICOS -------
    # 1.Generación del gráfico de barras: Aislados por especie
    # Colores por categoría
    category_colors = {
        "Gram negativa": "#A5005A",
        "Gram positiva": "#F48FB1",
        "Hongo": "#81C784",
    }

    fig3 = px.bar(
        conteo_especies,
        x="especie",
        y="aislados",
        color="Grupo_principal" if "Grupo_principal" in conteo_especies.columns else None,
        color_discrete_map=category_colors if "Grupo_principal" in conteo_especies.columns else None,
        labels={"aislados": "Número de aislados", "especie": "Especie"},
        title="Número de aislados por especie"
    )

    fig3.update_layout(
        height=600,
        xaxis_tickangle=-45,
        legend_title=None
    )

    fig3.update_xaxes(categoryorder="array", categoryarray=orden_especies)
    fig3.update_yaxes(
        type="log",
        tickvals=[1, 10, 100, 1000, 10000],
        ticktext=["1", "10", "100", "1000", "10k"]
    )

    # 2.Generación del gráfico Heatmap: Resistencia por especie-antibiótico
    # Crear customdata como un array 7D para incluir porcentajes y conteos
    # Heatmap para Gram positivas
    customdata_pos = np.stack([
        pivot_S_pos.values, pivot_I_pos.values, pivot_Inconcluyente_pos.values,
        pivot_R_count_pos.values, pivot_S_count_pos.values, pivot_I_count_pos.values, pivot_Inconcluyente_count_pos.values
    ], axis=-1)

    fig_heatmap_pos = px.imshow(
        pivot_R_pos,
        text_auto=True, # Mostrar valores en celdas (solo R (%))
        aspect="auto", # Ajustar aspecto automáticamente
        color_continuous_scale="Reds", # Escala de color rojo para alto
        labels={"color": "Resistencia (%)"}
    )
    # Ajustes para mejorar legibilidad
    fig_heatmap_pos.update_layout(
        xaxis_tickangle=-45, # Rotar etiquetas de columnas
        yaxis_title="Especie (Gram Positiva)",
        xaxis_title="Antibiótico",
        height=600,  # Ajustar altura para menos especies
        coloraxis_colorbar={"title": "R (%)"},
        xaxis_side="top" # Mover etiquetas del eje X arriba
    )
    # Actualizar hover con datos adicionales incluyendo conteos
    fig_heatmap_pos.update_traces(
        customdata=customdata_pos,
        hovertemplate="Especie: %{y}<br>Antibiótico: %{x}<br>R (%): %{z:.1f}; n = %{customdata[3]:.0f}<br>S (%): %{customdata[0]:.1f}; n = %{customdata[4]:.0f}<br>I (%): %{customdata[1]:.1f}; n = %{customdata[5]:.0f}<br>Inconcluyente (%): %{customdata[2]:.1f}; n = %{customdata[6]:.0f}",
        textfont_size=14
    )

    # Heatmap para Gram negativas
    customdata_neg = np.stack([
        pivot_S_neg.values, pivot_I_neg.values, pivot_Inconcluyente_neg.values,
        pivot_R_count_neg.values, pivot_S_count_neg.values, pivot_I_count_neg.values, pivot_Inconcluyente_count_neg.values
    ], axis=-1)

    fig_heatmap_neg = px.imshow(
        pivot_R_neg,
        text_auto=True,
        aspect="auto",
        color_continuous_scale="Reds",
        labels={"color": "Resistencia (%)"}
    )

    fig_heatmap_neg.update_layout(
        xaxis_tickangle=-45,
        yaxis_title="Especie (Gram Negativa)",
        xaxis_title="Antibiótico",
        height=600,  # Ajustar altura para menos especies
        coloraxis_colorbar={"title": "R (%)"},
        xaxis_side="top"
    )

    fig_heatmap_neg.update_traces(
        customdata=customdata_neg,
        hovertemplate="Especie: %{y}<br>Antibiótico: %{x}<br>R (%): %{z:.1f}; n = %{customdata[3]:.0f}<br>S (%): %{customdata[0]:.1f}; n = %{customdata[4]:.0f}<br>I (%): %{customdata[1]:.1f}; n = %{customdata[5]:.0f}<br>Inconcluyente (%): %{customdata[2]:.1f}; n = %{customdata[6]:.0f}",
        textfont_size=14
    )

    # 3.Generación del gráfico de barras y tabla de frecuencia de muestras por servicio
    # Crear gráfico de barras
    fig_localizacion = px.bar(
        conteo_servicio,
        x="Tipo de localizacion",
        y="Porcentaje",
        labels={"Porcentaje": "Porcentaje (%)", "Tipo de localización": "Tipo de Localizacion"},
        title="Pocentajes de muestra según servicio",
        color_discrete_sequence=["#636EFA"]
    )
    fig_localizacion.update_layout(
        height=600,
        xaxis_tickangle=-45,
        showlegend=False
    )

    # Crear tabla
    tabla_localizacion = dash_table.DataTable(
        id="tabla_localizacion",
        columns=[
            {"name": "Servicio", "id": "Tipo de localizacion"},
            {"name": "n", "id": "n"},
            {"name": "Porcentaje (%)", "id": "Porcentaje"}
        ],
        data=conteo_servicio_tabla.to_dict("records"),
        style_table={"overflowX": "auto", "height": "600px",},
        style_cell={
            "textAlign": "left",
            "padding": "5px",
            "fontSize": "14px"
        },
        style_header={
            "backgroundColor": "#f8f9fa",
            "fontWeight": "bold"
        }
    )

    # 4.Generación del gráfico de barras y tabla de frecuencia por tipo de muestra
    # Crear gráfico de barras
    fig_muestra = px.bar(
        conteo_muestra,
        x="Tipo de muestra",
        y="Porcentaje",
        labels={"Porcentaje": "Porcentaje (%)", "Tipo de muestra": "Tipo de Muestra"},
        title="Tipos de muestras y su proporción (%)",
        color_discrete_sequence=["#EF553B"],
        #category_orders={"Tipo de muestra": conteo_muestra["Tipo de muestra"].tolist()}
    )
    fig_muestra.update_layout(
        height=600,
        xaxis_tickangle=-45,
        showlegend=False
    )

    # Crear tabla
    tabla_muestra = dash_table.DataTable(
        id="tabla_muestra",
        columns=[
            {"name": "Tipo de Muestra", "id": "Tipo de muestra"},
            {"name": "n", "id": "n"},
            {"name": "Porcentaje (%)", "id": "Porcentaje"}
        ],
        data=conteo_muestra_tabla.to_dict("records"),
        style_table={
            "overflowX": "auto",
            "overflowY": "auto",  # Habilita scroll vertical
            "height": "600px",    # Altura fija, ajusta según necesites
        },
        style_cell={
            "textAlign": "left",
            "padding": "5px",
            "fontSize": "14px"
        },
        style_header={
            "backgroundColor": "#f8f9fa",
            "fontWeight": "bold"
        },
        fixed_rows={"headers": True} # Fijar encabezados de tablas
    )

    # 5.Generación del gráfico de barras de muestras por edad
    # Crear gráfico de barras
    fig_edad = px.bar(
        conteo_edad,
        x="Rango_edad",
        y="n",
        labels={"n": "Conteo", "Rango_edad": "Rango de Edad"},
        title="Número de muestras según edad",
        color_discrete_sequence=["#00CC96"]
    )
    fig_edad.update_layout(
        height=400,
        xaxis_tickangle=-45,
        showlegend=False
    )

    # 6.Generación del gráfico de barras apiladas: Distribución de tipos de muestra por servicio
    fig_servicio_muestras = px.bar(
        conteo_servicio_muestras,
        x="Tipo de localizacion",
        y="Conteo",
        color="Tipo de muestra",
        title="Distribución del tipo de muestras analizadas según servicio",
        labels={"Conteo": "Conteo", "Tipo de localizacion": "Servicio", "Tipo de muestra": "Tipo de Muestra"},
        category_orders={
            "Tipo de localizacion": orden_servicios,
            "Tipo de muestra": orden_tipos_muestra
        },
        color_discrete_sequence=px.colors.qualitative.Plotly
    )
    fig_servicio_muestras.update_traces(
        marker_line_width=1,
        marker_line_color='black'
    )
    fig_servicio_muestras.update_layout(
        height=600,
        xaxis_tickangle=-45,
        showlegend=True,
        legend_title="Tipo de Muestra"
    )

    # Crear tabla para distribución de tipos de muestra por servicio
    tabla_servicio_muestras = dash_table.DataTable(
        id="tabla_servicio_muestras",
        columns=[
            {"name": "Servicio", "id": "Tipo de localizacion"},
            {"name": "Tipo de Muestra", "id": "Tipo de muestra"},
            {"name": "Conteo", "id": "Conteo"},
            {"name": "Porcentaje (%)", "id": "Porcentaje"}
        ],
        data=conteo_servicio_muestras.to_dict("records"),
        style_table={
            "overflowX": "auto",
            "overflowY": "auto", 
            "height": "600px",    
        },
        style_cell={
            "textAlign": "left",
            "padding": "5px",
            "fontSize": "14px"
        },
        style_header={
            "backgroundColor": "#f8f9fa",
            "fontWeight": "bold"
        },
    )

    # 7. Generación del gráfico de barras apiladas: Distribución de perfiles de especies por tipo de muestra
    fig_muestra_especies = px.bar(
        conteo_muestra_especies,
        x="Tipo de muestra",
        y="Conteo",
        color="especie",
        title="Aislamientos bacterianos según su origen de la muestra",
        labels={"Conteo": "Conteo", "Tipo de muestra": "Tipo de Muestra", "perfil_especies": "Perfil de Especies"},
        category_orders={
            "Tipo de muestra": orden_muestras,
            "especie": orden_de_especies
        },
        color_discrete_sequence=px.colors.qualitative.Plotly
    )
    fig_muestra_especies.update_traces(
        marker_line_width=1,
        marker_line_color='black'
    )
    fig_muestra_especies.update_layout(
        height=600,
        xaxis_tickangle=-45,
        showlegend=True,
        legend_title="Especies"
    )

    # Crear tabla para distribución de perfiles de especies por tipo de muestra
    tabla_muestra_especies = dash_table.DataTable(
        id="tabla_muestra_especies",
        columns=[
            {"name": "Tipo de Muestra", "id": "Tipo de muestra"},
            {"name": "Especies", "id": "especie"},
            {"name": "Conteo", "id": "Conteo"},
            {"name": "Porcentaje (%)", "id": "Porcentaje"}
        ],
        data=conteo_muestra_especies.to_dict("records"),
        style_table={
            "overflowX": "auto",
            "overflowY": "auto", 
            "height": "600px",    
        },
        style_cell={
            "textAlign": "left",
            "padding": "5px",
            "fontSize": "14px"
        },
        style_header={
            "backgroundColor": "#f8f9fa",
            "fontWeight": "bold"
        }
    )
    
    print(f"✅ Gráficos regenerados para {anio_actual}")


# --- LAYOUT DE LA APP ---
app.layout = dbc.Container([
    html.H1("Plataforma para el monitoreo de resistencia antimicrobiana en Arequipa", className="text-center mb-4"),
    # Nueva fila para pestañas y dropdown de años
    dbc.Row([
        dbc.Col(
            dbc.Tabs([
                dbc.Tab(label="Muestras analizadas", tab_id="tab-muestras"),
                dbc.Tab(label="Aislados analizados", tab_id="tab-aislados"),
                dcc.Tab(label="Cargar Datos", value="tab-cargar", children=upload_section)
            ], id="tabs", active_tab="tab-muestras", class_name="mb-3"),
            width=8
        ),
        dbc.Col(
            html.Div([
                html.Label("Seleccionar año:", className="me-2"),
                dcc.Dropdown(
                    id="year-selector",
                    options=[{"label": str(year), "value": year} for year in obtener_anios_disponibles()],
                    value=2023,
                    clearable=False,
                    style={"width": "150px"}
                )
            ], style={"display": "flex", "alignItems": "center"}),
            width=4,
            style={"display": "flex", "justifyContent": "flex-end"}
        )
    ], className="mb-3"),
    html.Div(id="tab-content", className="p-0")
], fluid=True, class_name="px-2")

# --- CALLBACKS ---
@callback(
    Output("tab-content", "children"),
    Input("tabs", "active_tab"),
    Input("year-selector", "value")  # Agregar dependencia del selector de año
)

def render_tab_content(active_tab, selected_year):
    global anio_actual, df_actual
    anio_actual = selected_year
    df_actual = cargar_datos(selected_year)  # Cargar datos del año seleccionado
    if df_actual is not None:
        generar_todos_graficos()  # Regenerar gráficos si hay datos
    else:
        # Actualizar orden_meses para el año seleccionado
        global orden_meses
        orden_meses = [f"{mes.split('-')[0]}-{selected_year}" for mes in [
            "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
            "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"
        ]]

    if active_tab == "tab-muestras":
        return dbc.Container([
            html.H3("Distribución de muestras según servicio", className="mt-3 mb-3"),
            dbc.Row([
                dbc.Col(
                    dcc.Graph(id="grafico_localizacion", figure=fig_localizacion, style={"height": "600px"}),
                    width=6
                ),
                dbc.Col(
                    tabla_localizacion,
                    width=6
                )
            ], className="mb-4"),
            html.H3("Distribución del tipo de muestras"),
            dbc.Row([
                dbc.Col(
                    dcc.Graph(id="grafico_muestra", figure=fig_muestra, style={"height": "600px"}),
                    width=6
                ),
                dbc.Col(
                    tabla_muestra,
                    width=6
                )
            ], className="mb-4"),
            html.H3("Distribución de muestras según edad"),
            dcc.Graph(id="grafico_edad", figure=fig_edad, style={"height": "400px"}),
            html.H3("Distribución de tipos de muestra por servicio"),
            dbc.Row([
                dbc.Col(
                    dcc.Graph(id="grafico_servicio_muestras", figure=fig_servicio_muestras, style={"height": "600px"}),
                    width=6
                ),
                dbc.Col(
                    tabla_servicio_muestras,
                    width=6
                )
            ], className="mb-4"),
            html.H3("Distribución de especies bacterianas por tipo de muestra"),
            dbc.Row([
                dbc.Col(
                    dcc.Graph(id="grafico_muestra_especies", figure=fig_muestra_especies, style={"height": "600px"}),
                    width=6
                ),
                dbc.Col(
                    tabla_muestra_especies,
                    width=6
                )
            ], className="mb-4"),
        ], className="mt-3")
    
    elif active_tab == "tab-aislados":
        return dbc.Container([
            html.H3("Especies bacterianas"),
            dcc.Graph(id="grafico_aislados", figure=fig3, style={"height": "600px"}),
            html.H3("Porcentaje de resistencia por especie y antibiótico (Gram Positivas)"),
            dcc.Graph(id="grafico_heatmap_pos", figure=fig_heatmap_pos, style={"height": "600px"}),
            html.H3("Porcentaje de resistencia por especie y antibiótico (Gram Negativas)"),
            dcc.Graph(id="grafico_heatmap_neg", figure=fig_heatmap_neg, style={"height": "1000px"}),
            html.Hr(),
            html.Label("Selecciona un antibiótico:"),
            dcc.Dropdown(
                id="abx_unico",
                options=[{"label": abx, "value": abx} for abx in antibioticos],
                value=antibioticos[0] if antibioticos else None,
                clearable=False
            ),
            dcc.Graph(id="grafico_resistencia", style={"height": "500px"}),
        ], className="mt-3")
    
    return html.P("Selecciona una pestaña")

@callback(
    Output("grafico_resistencia", "figure"),
    Input("abx_unico", "value"),
    Input("year-selector", "value")  # Agregar dependencia del selector de año
)
def actualizar_grafico(abx_1, selected_year):
    global df_grafLineas, especies_fijas, colores_especies
    if df_grafLineas is None or df_grafLineas.empty:
        return go.Figure().add_annotation(text="Sin datos para este año", showarrow=False)
    
    # Actualizar orden_meses para el año seleccionado
    global orden_meses
    orden_meses = [f"{mes.split('-')[0]}-{selected_year}" for mes in [
        "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
        "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"
    ]]
    df_grafLineas["fecha"] = pd.Categorical(df_grafLineas["fecha"], categories=orden_meses, ordered=True)
    
    df_1 = df_grafLineas[(df_grafLineas["antibiotico"] == abx_1) & (df_grafLineas["especie"].isin(especies_fijas))]
    df_1_grouped = df_1.groupby(["fecha", "especie"], as_index=False)["R (%)"].mean()
    df_1_grouped = df_1_grouped.sort_values("fecha")

    fig = px.line(
        df_1_grouped,
        x="fecha",
        y="R (%)",
        color="especie",
        markers=True,
        title=f"Resistencia a {abx_1} por especie (total)",
        labels={"fecha": "Mes", "R (%)": "Resistencia (%)", "especie": "Microorganismo"},
        color_discrete_map=colores_especies
    )
    fig.update_layout(hovermode="x unified")
    return fig

@callback(
    [Output("upload-status", "children"),
     Output("upload-status", "style"),
     Output("upload-status", "className")],
    Input("upload-data", "contents"),
    Input("upload-data", "filename"),
    Input("input-year", "value"),
    prevent_initial_call=True
)

def update_upload_status(contents, filename, year):
    if contents and filename:
        return f"📄 Archivo '{filename}' cargado. Listo para procesar para el año {year}.", {"display": "block"}, "alert alert-info"
    return "⚠️ Seleccione un archivo y un año", {"display": "block"}, "alert alert-warning"

# Procesar archivo subido
@callback(
    [Output("upload-status", "children", allow_duplicate=True),
     Output("upload-status", "style", allow_duplicate=True),
     Output("upload-status", "className", allow_duplicate=True),
     Output("year-selector", "options")],  # Actualizar opciones del dropdown
    Input("btn-process", "n_clicks"),
    [State("upload-data", "contents"),
     State("upload-data", "filename"),
     State("input-year", "value")],
    prevent_initial_call=True
)

def procesar_archivo(n_clicks, contents, filename, year):
    if n_clicks is None or not contents or not year:
        return "⚠️ Seleccione archivo y año", {"display": "block"}, "alert alert-warning", [{"label": str(y), "value": y} for y in obtener_anios_disponibles()]
    
    try:
        decoded_content = base64.b64decode(contents.split(',')[1])
        df_procesado = procesar_archivo_subido(decoded_content, year)
        global df_actual, anio_actual
        df_actual = df_procesado
        anio_actual = year
        generar_todos_graficos()
        # Actualizar opciones del dropdown con los años disponibles
        anios = obtener_anios_disponibles()
        return (f"✅ '{filename}' procesado para {year}! ({len(df_actual)} registros)",
                {"display": "block"},
                "alert alert-success",
                [{"label": str(y), "value": y} for y in anios])
    except Exception as e:
        return (f"❌ Error: {str(e)}",
                {"display": "block"},
                "alert alert-danger",
                [{"label": str(y), "value": y} for y in obtener_anios_disponibles()])

# Regenerar gráficos cuando cambian datos
@callback(
    [Output("grafico_localizacion", "figure"),
     Output("grafico_muestra", "figure"),
     Output("grafico_edad", "figure"),
     Output("grafico_servicio_muestras", "figure"),
     Output("grafico_muestra_especies", "figure"),
     Output("grafico_aislados", "figure"),
     Output("grafico_heatmap_pos", "figure"),
     Output("grafico_heatmap_neg", "figure"),
     Output("abx_unico", "options"),  # Actualizar opciones de antibióticos
     Output("abx_unico", "value")],   # Actualizar valor seleccionado
    Input("year-selector", "value"),
    prevent_initial_call=True
)
def actualizar_todos_graficos(selected_year):
    global df_actual, anio_actual, antibioticos
    anio_actual = selected_year
    df_actual = cargar_datos(selected_year)
    
    if df_actual is None:
        fig_empty = go.Figure().add_annotation(text="Sin datos para este año", showarrow=False)
        return [fig_empty] * 8 + [[], None]
    
    # Actualizar orden_meses para el año seleccionado
    global orden_meses
    orden_meses = [f"{mes.split('-')[0]}-{selected_year}" for mes in [
        "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
        "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"
    ]]
    
    generar_todos_graficos()
    return [fig_localizacion, fig_muestra, fig_edad, fig_servicio_muestras,
            fig_muestra_especies, fig3, fig_heatmap_pos, fig_heatmap_neg,
            [{"label": abx, "value": abx} for abx in antibioticos],
            antibioticos[0] if antibioticos else None]



