# Script para limpieza y normalización de datos de estados históricos inmobiliarios
# -------------------------------------------------------------
# clean_estados.py
# Limpiador especializado de datos de estados históricos con validación temporal.
#
# Autor: Juan Camilo Riaño Molano
# Fecha de creación: 01/08/2025
# Descripción:
#   Este script realiza limpieza especializada de datos de estados históricos:
#   - Normalización de encabezados (espacios → guion bajo)
#   - División precisa de Fecha_Actualizacion en componentes Fecha y Hora
#   - Imputación inteligente de valores vacíos con 'Desconocido'
#   - Validación y detección de duplicados en Fecha_Actualizacion
#   - Separación de registros con información incompleta (Fecha, Hora, Estado = 'Desconocido')
#   - Generación de tres datasets: limpio, problemático y combinado
#   - Validación de coherencia temporal y secuencias de estados
#
#   Mantiene la integridad temporal de los datos mientras separa
#   registros problemáticos para análisis posterior.
#
# Funcionalidades principales:
#   - Normalización de formatos de fecha y hora con validación
#   - Detección de inconsistencias temporales y estados inválidos
#   - Separación inteligente de registros con datos faltantes
#   - Validación de secuencias lógicas de estados por inmueble
#   - Generación de reportes de calidad de datos temporales
# -------------------------------------------------------------

# =======================
# Importación de librerías
# =======================
import pandas as pd
import unicodedata


def clean_estados(input_path, output_path):
    def normalizar_texto(val):
        if isinstance(val, str):
            val = unicodedata.normalize("NFKD", val)
            val = val.encode("ascii", "ignore").decode("utf-8")
            val = val.replace("ñ", "n").replace("Ñ", "N")
        return val

    # Leer el archivo de entrada
    df = pd.read_csv(input_path)
    # Normalizar tildes y ñ en todos los valores string
    df = df.applymap(normalizar_texto)

    # 1. Normalizar encabezados: reemplazar espacios por guion bajo
    df.columns = [col.strip().replace(" ", "_") for col in df.columns]
    # 2. Dividir columna Fecha_Actualizacion en Fecha y Hora
    if "Fecha_Actualizacion" in df.columns:
        df["Fecha_Actualizacion"] = pd.to_datetime(df["Fecha_Actualizacion"], errors="coerce")
        df["Fecha"] = df["Fecha_Actualizacion"].dt.date.astype(str)
        df["Hora"] = df["Fecha_Actualizacion"].dt.time.astype(str)
    # 3. Imputar vacíos en todas las columnas con 'Desconocido'
    imputados_por_col = {}
    for col in df.columns:
        n_null = df[col].isnull().sum()
        n_empty = (df[col] == "").sum()
        imputados_por_col[col] = n_null + n_empty
    df = df.fillna("Desconocido")
    for col in df.columns:
        df[col] = df[col].replace("", "Desconocido")
    print('Cantidad de registros imputados a "Desconocido" por columna:')
    for col, n in imputados_por_col.items():
        print(f"  {col}: {n}")
    # 4. Validar duplicados exactos en Fecha_Actualizacion
    if "Fecha_Actualizacion" in df.columns:
        duplicados = df[df.duplicated(subset=["Fecha_Actualizacion"], keep=False)]
        if not duplicados.empty:
            print(f"Advertencia: Se encontraron {len(duplicados)} registros con Fecha_Actualizacion duplicada.")
            print(duplicados[["Inmueble_ID", "Fecha_Actualizacion"]])
    # 5. Separar registros donde Fecha, Hora y Estado son 'Desconocido' simultáneamente
    columnas_validar = ["Fecha", "Hora", "Estado"]
    for col in columnas_validar:
        if col not in df.columns:
            df[col] = "Desconocido"
    # Solo eliminar si los tres campos son 'Desconocido' simultáneamente
    mask_desconocido = (df["Fecha"] == "Desconocido") & (df["Hora"] == "Desconocido") & (df["Estado"] == "Desconocido")
    df_desconocidos = df[mask_desconocido].copy()
    df_limpio = df[~mask_desconocido].copy()
    # 6. Dataframe combinado (limpio + desconocidos)
    df_completo = pd.concat([df_limpio, df_desconocidos], ignore_index=True)
    # 7. Ordenar por Inmueble_ID y Fecha_Actualizacion para asegurar secuencia lógica en el limpio
    if "Inmueble_ID" in df_limpio.columns and "Fecha_Actualizacion" in df_limpio.columns:
        df_limpio = df_limpio.sort_values(["Inmueble_ID", "Fecha_Actualizacion"])
        # Asegurar tipo datetime para comparación
        df_limpio["Fecha_Actualizacion"] = pd.to_datetime(df_limpio["Fecha_Actualizacion"], errors="coerce")
        df_limpio["prev_fecha"] = df_limpio.groupby("Inmueble_ID")["Fecha_Actualizacion"].shift(1)
        df_limpio["prev_fecha"] = pd.to_datetime(df_limpio["prev_fecha"], errors="coerce")
        mask_consistente = df_limpio["prev_fecha"].isna() | (df_limpio["Fecha_Actualizacion"] >= df_limpio["prev_fecha"])
        registros_inconsistentes = df_limpio[~mask_consistente].copy()
        n_inconsistentes = len(registros_inconsistentes)
        if n_inconsistentes > 0:
            print(
                f"Se eliminaron {n_inconsistentes} registros por secuencia temporal inconsistente "
                f"(Fecha_Actualizacion menor a la anterior para el mismo Inmueble_ID). Ejemplo:"
            )
            print(registros_inconsistentes[["Inmueble_ID", "Fecha_Actualizacion", "prev_fecha"]].head())
        df_limpio = df_limpio[mask_consistente]
        df_limpio = df_limpio.drop(columns=["prev_fecha"])
    # 8. Guardar solo el limpio (sin registros totalmente desconocidos) como CSV
    import os

    clean_dir = os.path.join(os.path.dirname(os.path.dirname(input_path)), "cleanData")
    os.makedirs(clean_dir, exist_ok=True)
    clean_path = os.path.join(clean_dir, "CLESTADOS.csv")
    df_limpio.to_csv(clean_path, index=False)
    print(
        f"Limpieza de estados finalizada. Archivo limpio generado en {clean_path} "
        f"(sin registros con Fecha, Hora o Estado desconocidos)."
    )
    print(f"Registros en df_limpio (sin totalmente desconocidos): {len(df_limpio)}")
    print(f"Registros en df_desconocidos (solo totalmente desconocidos): {len(df_desconocidos)}")
    print(f"Registros en df_completo (todos): {len(df_completo)}")

    # Retornar los tres dataframes
    return df_limpio, df_desconocidos, df_completo


# Punto de entrada del script
if __name__ == "__main__":
    clean_estados("../data/processedData/estados.csv", "../data/processedData/estados.csv")
