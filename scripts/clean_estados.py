# -------------------------------------------------------------
# clean_estados.py
# Script para limpiar y diagnosticar la calidad de los datos de estados históricos.
# Ordena, valida secuencia temporal y elimina inconsistencias.
# -------------------------------------------------------------

def clean_estados(input_path, output_path):
    """
    Limpia el archivo CSV de estados históricos:
    - Ordena los registros por Inmueble ID y Fecha de Actualización.
    - Elimina registros donde la secuencia temporal es inconsistente (fecha decreciente).
    - Guarda el archivo limpio para análisis posterior.
    """
    import pandas as pd

    # Leer el archivo de entrada y parsear fechas
    df = pd.read_csv(input_path, parse_dates=['Fecha_Actualizacion'])

    # Separar la columna de fecha y hora en dos columnas nuevas
    df['Fecha'] = df['Fecha_Actualizacion'].dt.date
    df['Hora'] = df['Fecha_Actualizacion'].dt.time

    # Ordenar por Inmueble_ID y Fecha para asegurar secuencia lógica
    df = df.sort_values(['Inmueble_ID', 'Fecha_Actualizacion'])
    # Eliminar registros donde la fecha de actualización es menor a la anterior
    df['prev_fecha'] = df.groupby('Inmueble_ID')['Fecha_Actualizacion'].shift(1)
    df = df[df['prev_fecha'].isna() | (df['Fecha_Actualizacion'] >= df['prev_fecha'])]
    df = df.drop(columns=['prev_fecha'])
    # Guardar el archivo limpio en la carpeta cleanData con nombre CLESTADOS.csv
    import os
    clean_dir = os.path.join(os.path.dirname(os.path.dirname(input_path)), 'cleanData')
    os.makedirs(clean_dir, exist_ok=True)
    clean_path = os.path.join(clean_dir, 'CLESTADOS.csv')
    df.to_csv(clean_path, index=False)
    print(f'Limpieza de estados finalizada. Archivo limpio generado en {clean_path} con columnas Fecha y Hora.')

# Punto de entrada del script
if __name__ == '__main__':
    clean_estados(
        '../data/processedData/estados.csv',
        '../data/processedData/estados.csv'
    )
