
"""
-------------------------------------------------------------
clean_estados.py
Script para limpiar y diagnosticar la calidad de los datos de estados históricos.

Autor: Juan Camilo Riaño Molano
Fecha: 01/08/2025

Descripción:
    - Normaliza encabezados (espacios → guion bajo)
    - Divide Fecha_Actualizacion en Fecha y Hora
    - Imputa vacíos con 'Desconocido'
    - Valida duplicados en Fecha_Actualizacion
    - Separa registros donde Fecha, Hora y Estado son 'Desconocido' simultáneamente
    - Genera tres dataframes: limpio (sin registros totalmente desconocidos), solo desconocidos, y combinado

Buenas prácticas:
    - Documentar criterios de limpieza y cambios realizados.
    - Mantener el código modular y funciones reutilizables.
    - Validar la estructura y tipos de datos antes de procesar.
    - Facilitar la trazabilidad y auditoría de los cambios.
-------------------------------------------------------------
"""

def clean_estados(input_path, output_path):

    import pandas as pd
    import unicodedata
    import numpy as np

    def normalizar_texto(val):
        if isinstance(val, str):
            val = unicodedata.normalize('NFKD', val)
            val = val.encode('ascii', 'ignore').decode('utf-8')
            val = val.replace('ñ', 'n').replace('Ñ', 'N')
        return val

    # Leer el archivo de entrada
    df = pd.read_csv(input_path)

    # Normalizar tildes y ñ en todos los valores string
    df = df.applymap(normalizar_texto)
    """
    Limpia y valida el archivo CSV de estados históricos:
    - Normaliza encabezados (espacios → guion bajo)
    - Divide la columna Fecha_Actualizacion en Fecha y Hora
    - Imputa vacíos en todas las columnas con 'Desconocido'
    - Valida duplicados exactos en Fecha_Actualizacion
    - Separa registros donde Fecha, Hora y Estado son 'Desconocido' simultáneamente (solo estos se eliminan del limpio)
    - Ordena y valida secuencia temporal en el limpio
    - Genera tres dataframes:
        1. df_limpio: sin registros con Fecha, Hora y Estado totalmente desconocidos (se guarda como CSV)
        2. df_desconocidos: solo registros con los tres campos desconocidos
        3. df_completo: todos los datos (limpio + desconocidos)
    """
    import pandas as pd

    # Leer el archivo de entrada
    import numpy as np
    df = pd.read_csv(input_path)

    # 1. Normalizar encabezados: reemplazar espacios por guion bajo
    df.columns = [col.strip().replace(' ', '_') for col in df.columns]

    # 2. Dividir columna Fecha_Actualizacion en Fecha y Hora
    if 'Fecha_Actualizacion' in df.columns:
        df['Fecha_Actualizacion'] = pd.to_datetime(df['Fecha_Actualizacion'], errors='coerce')
        df['Fecha'] = df['Fecha_Actualizacion'].dt.date.astype(str)
        df['Hora'] = df['Fecha_Actualizacion'].dt.time.astype(str)

    # 3. Imputar vacíos en todas las columnas con 'Desconocido'
    imputados_por_col = {}
    for col in df.columns:
        n_null = df[col].isnull().sum()
        n_empty = (df[col] == '').sum()
        imputados_por_col[col] = n_null + n_empty
    df = df.fillna('Desconocido')
    for col in df.columns:
        df[col] = df[col].replace('', 'Desconocido')
    print('Cantidad de registros imputados a "Desconocido" por columna:')
    for col, n in imputados_por_col.items():
        print(f"  {col}: {n}")

    # 4. Validar duplicados exactos en Fecha_Actualizacion
    if 'Fecha_Actualizacion' in df.columns:
        duplicados = df[df.duplicated(subset=['Fecha_Actualizacion'], keep=False)]
        if not duplicados.empty:
            print(f"Advertencia: Se encontraron {len(duplicados)} registros con Fecha_Actualizacion duplicada.")
            print(duplicados[['Inmueble_ID', 'Fecha_Actualizacion']])

    # 5. Separar registros donde Fecha, Hora y Estado son 'Desconocido' simultáneamente
    columnas_validar = ['Fecha', 'Hora', 'Estado']
    for col in columnas_validar:
        if col not in df.columns:
            df[col] = 'Desconocido'
    # Solo eliminar si los tres campos son 'Desconocido' simultáneamente
    mask_desconocido = (df['Fecha'] == 'Desconocido') & (df['Hora'] == 'Desconocido') & (df['Estado'] == 'Desconocido')
    df_desconocidos = df[mask_desconocido].copy()
    df_limpio = df[~mask_desconocido].copy()

    # 6. Dataframe combinado (limpio + desconocidos)
    df_completo = pd.concat([df_limpio, df_desconocidos], ignore_index=True)

    # 7. Ordenar por Inmueble_ID y Fecha_Actualizacion para asegurar secuencia lógica en el limpio
    if 'Inmueble_ID' in df_limpio.columns and 'Fecha_Actualizacion' in df_limpio.columns:
        df_limpio = df_limpio.sort_values(['Inmueble_ID', 'Fecha_Actualizacion'])
        # Asegurar tipo datetime para comparación
        df_limpio['Fecha_Actualizacion'] = pd.to_datetime(df_limpio['Fecha_Actualizacion'], errors='coerce')
        df_limpio['prev_fecha'] = df_limpio.groupby('Inmueble_ID')['Fecha_Actualizacion'].shift(1)
        df_limpio['prev_fecha'] = pd.to_datetime(df_limpio['prev_fecha'], errors='coerce')
        mask_consistente = df_limpio['prev_fecha'].isna() | (df_limpio['Fecha_Actualizacion'] >= df_limpio['prev_fecha'])
        registros_inconsistentes = df_limpio[~mask_consistente].copy()
        n_inconsistentes = len(registros_inconsistentes)
        if n_inconsistentes > 0:
            print(f"Se eliminaron {n_inconsistentes} registros por secuencia temporal inconsistente (Fecha_Actualizacion menor a la anterior para el mismo Inmueble_ID). Ejemplo:")
            print(registros_inconsistentes[['Inmueble_ID', 'Fecha_Actualizacion', 'prev_fecha']].head())
        df_limpio = df_limpio[mask_consistente]
        df_limpio = df_limpio.drop(columns=['prev_fecha'])

    # 8. Guardar solo el limpio (sin registros totalmente desconocidos) como CSV
    import os
    clean_dir = os.path.join(os.path.dirname(os.path.dirname(input_path)), 'cleanData')
    os.makedirs(clean_dir, exist_ok=True)
    clean_path = os.path.join(clean_dir, 'CLESTADOS.csv')
    df_limpio.to_csv(clean_path, index=False)
    print(f'Limpieza de estados finalizada. Archivo limpio generado en {clean_path} (sin registros con Fecha, Hora o Estado desconocidos).')
    print(f'Registros en df_limpio (sin totalmente desconocidos): {len(df_limpio)}')
    print(f'Registros en df_desconocidos (solo totalmente desconocidos): {len(df_desconocidos)}')
    print(f'Registros en df_completo (todos): {len(df_completo)}')

    # Retornar los tres dataframes
    return df_limpio, df_desconocidos, df_completo

# Punto de entrada del script
if __name__ == '__main__':
    clean_estados(
        '../data/processedData/estados.csv',
        '../data/processedData/estados.csv'
    )
