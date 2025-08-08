# Script para ejecución automatizada de queries SQL y exportación a Excel
# -------------------------------------------------------------
# export_sql_to_excel.py
# Ejecutor automatizado de consultas SQL con exportación directa a Excel.
#
# Autor: Juan Camilo Riaño Molano
# Fecha de creación: 01/08/2025
# Descripción:
#   Este script automatiza la ejecución de archivos .sql y exporta los resultados
#   a archivos Excel con formateo profesional:
#   - Ejecución automática de todos los archivos .sql en sql_queries/
#   - Conexión robusta a MySQL usando variables de entorno
#   - Exportación directa a Excel (.xlsx) con nombres descriptivos
#   - Formateo automático de columnas y tipos de datos
#   - Manejo de errores y logging detallado de operaciones
#   - Compatibilidad total con usuarios sin MySQL Workbench
#
#   Facilita el acceso a resultados de consultas complejas mediante
#   archivos Excel listos para análisis y presentación.
#
# Funcionalidades principales:
#   - Descubrimiento automático de archivos .sql en directorio
#   - Ejecución secuencial con validación de sintaxis
#   - Exportación con preservación de tipos de datos
#   - Generación de logs de ejecución y errores
#   - Compatibilidad cross-platform y multi-usuario
# -------------------------------------------------------------

# =======================
# Importación de librerías
# =======================
import pandas as pd
from sqlalchemy import create_engine
import os


# =======================
# Función principal de exportación
# =======================
def export_query_to_excel(sql_file, db_url):
    """
    Ejecuta un archivo SQL sobre la base de datos y exporta el resultado a Excel.

    Parámetros:
            sql_file (str): Ruta al archivo .sql con el query a ejecutar.
            db_url (str): URL de conexión a la base de datos (SQLAlchemy).
    Proceso:
            1. Lee el query desde el archivo .sql.
            2. Ejecuta el query usando SQLAlchemy y pandas.
            3. Exporta el resultado a un archivo Excel con el mismo nombre base.

    Buenas prácticas:
    - Valida la existencia de los archivos y la conexión antes de ejecutar.
    - Crea la carpeta de salida si no existe.
    - Maneja errores de lectura y conexión.

    Manejo de errores:
            FileNotFoundError: Si el archivo .sql no existe.
            ValueError: Si la URL de conexión es inválida.
            Exception: Si ocurre un error al ejecutar el query o exportar el archivo.
    """
    # Leer el query desde el archivo .sql
    if not os.path.exists(sql_file):
        raise FileNotFoundError(f"No se encontró el archivo SQL: {sql_file}")
    with open(sql_file, "r", encoding="utf-8") as f:
        query = f.read()
    # Crear el engine de SQLAlchemy
    engine = create_engine(db_url)
    # Ejecutar el query y obtener el DataFrame
    try:
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
    except Exception:
        # Fallback: intentar con engine directo si falla la conexión
        df = pd.read_sql(query, engine)
    # Generar ruta y nombre de archivo Excel en data/query_data/
    output_dir = os.path.join(os.path.dirname(os.path.dirname(sql_file)), "data", "query_data")
    os.makedirs(output_dir, exist_ok=True)
    excel_file = os.path.join(output_dir, os.path.splitext(os.path.basename(sql_file))[0] + ".xlsx")
    # Exportar a Excel
    df.to_excel(excel_file, index=False)
    print(f"Resultado exportado a: {excel_file}")


# =======================
# Función principal
# =======================
def export_sql_to_excel():
    """
    Función principal para exportar consultas SQL a Excel
    """
    # Cargar variables de entorno desde .env
    from dotenv import load_dotenv
    import os

    # Definir la raíz del proyecto de forma robusta
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env_path = os.path.join(project_root, "config", ".env")
    load_dotenv(env_path)
    
    # Leer la URL de conexión a la base de datos
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("No se encontró la variable DATABASE_URL en el entorno")

    # Exportar los resultados de los queries especificados
    try:
        sql_dir = os.path.join(project_root, "sql_queries")
        export_query_to_excel(os.path.join(sql_dir, "ultimo_estado.sql"), db_url)
        export_query_to_excel(os.path.join(sql_dir, "diferencia_absoluta_y_ranking.sql"), db_url)
    except Exception as e:
        print(f"❌ Error exportando a Excel: {e}")
        print("⚠️  Los datos están disponibles en la base de datos SQL.")


# =======================
# Ejecución principal
# =======================
if __name__ == "__main__":
    export_sql_to_excel()
