# Script para extraer datos desde un archivo Excel fuente y exportarlos a CSV para su posterior limpieza y análisis.
# -------------------------------------------------------------
# obtain_data.py
# Script para la obtención inicial de datos desde un archivo Excel
# y su conversión a archivos CSV para análisis posterior.
#
# Autor: Juan Camilo Riaño Molano
# Fecha de creación: 30/07/2025
# Descripción:
#   Este script extrae datos de un archivo Excel fuente, aplica tipos de datos estrictos
#   y exporta los resultados a archivos CSV para su posterior limpieza y análisis.
#   Además, normaliza los encabezados de los archivos CSV generados, reemplazando espacios por guion bajo y cambiando
#   'Antiguedad (Años)' por 'Antiguedad_Annos' para asegurar consistencia en el procesamiento.
# -------------------------------------------------------------

# =======================
# Importación de librerías
# =======================
import pandas as pd
import os

# =======================
# Definición de rutas absolutas
# =======================
# BASE_DIR apunta a la raíz del proyecto, independientemente de dónde se ejecute el script
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Ruta al archivo Excel fuente
RAW_PATH = os.path.join(BASE_DIR, "data", "sourceData", "Muestra_Prueba_BI_Jr_FW.xlsx")
# Carpeta donde se guardarán los archivos procesados
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processedData")

# =======================
# Especificación de tipos de datos esperados para cada columna
# =======================
DTYPE_MAP = {
    "Id": str,
    "Fuente": str,
    "Ciudad": str,
    "Zona": str,
    "Estrato": str,
    "Lote Id": str,
    "Tipo Inmueble": str,
    "Nombre Contacto": str,
    "Telefono Contacto": str,
    "Precio Solicitado": float,
    "Área": float,
    "Piso": str,
    "Garajes": str,
    "Ascensores": str,
    "Antiguedad (Años)": float,
}


def obtain_data():
    """
    Extrae y transforma datos desde un archivo Excel fuente y los exporta a CSV.

    Proceso:
    1. Lee la hoja 'Muestra' aplicando los tipos de datos definidos en DTYPE_MAP.
    2. Lee la hoja 'Historico_Estados' y convierte la columna de fechas a tipo datetime.
    3. Crea la carpeta de archivos procesados si no existe.
    4. Exporta los DataFrames a archivos CSV para su posterior limpieza y análisis.

    Recomendaciones profesionales:
    - Mantener la integridad de los datos fuente, no modificar el Excel original.
    - Validar la existencia de las hojas y columnas esperadas antes de procesar.
    - Documentar cualquier cambio en la estructura de los datos fuente.

    Manejo de errores:
        FileNotFoundError: Si el archivo Excel fuente no existe.
        ValueError: Si las hojas requeridas no existen en el archivo Excel.
    """
    # Validar existencia del archivo fuente
    if not os.path.exists(RAW_PATH):
        raise FileNotFoundError(f"No se encontró el archivo fuente: {RAW_PATH}")

    # Leer la hoja 'Muestra' con los tipos de datos definidos
    try:
        df_muestra = pd.read_excel(RAW_PATH, sheet_name="Muestra", dtype=DTYPE_MAP, engine="openpyxl")
    except ValueError as e:
        raise ValueError("No se encontró la hoja 'Muestra' en el archivo Excel.") from e

    # Leer la hoja 'Historico_Estados' y convertir la columna de fechas
    try:
        df_estados = pd.read_excel(
            RAW_PATH, sheet_name="Historico_Estados", parse_dates=["Fecha Actualización"], engine="openpyxl"
        )
    except ValueError as e:
        raise ValueError("No se encontró la hoja 'Historico_Estados' en el archivo Excel.") from e

    # Crear la carpeta de archivos procesados si no existe
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    # Normalizar encabezados: reemplazar espacios por guion bajo y renombrar 'Antiguedad (Años)'
    def normalizar_columnas(df):
        cols = [
            (
                col.replace(" ", "_")
                .replace("á", "a")
                .replace("Á", "A")
                .replace("é", "e")
                .replace("í", "i")
                .replace("ó", "o")
                .replace("ú", "u")
                .replace("ñ", "n")
                if col != "Antiguedad (Años)"
                else "Antiguedad_Annos"
            )
            for col in df.columns
        ]
        # Si hay 'Antiguedad (Años)', renombrar
        df.columns = cols
        return df

    df_muestra = normalizar_columnas(df_muestra)
    df_estados = normalizar_columnas(df_estados)

    # Guardar los DataFrames como CSV para su posterior limpieza y análisis
    df_muestra.to_csv(os.path.join(PROCESSED_DIR, "muestra.csv"), index=False)
    df_estados.to_csv(os.path.join(PROCESSED_DIR, "estados.csv"), index=False)
    print(f"Archivos procesados guardados en {PROCESSED_DIR} (muestra.csv, estados.csv)")


if __name__ == "__main__":
    obtain_data()
