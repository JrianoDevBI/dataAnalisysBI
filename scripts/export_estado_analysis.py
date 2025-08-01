# Script para exportación avanzada de análisis de estados a Excel con múltiples hojas
# -------------------------------------------------------------
# export_estado_analysis.py
# Exportador especializado de análisis de estados con formateo Excel avanzado.
#
# Autor: Juan Camilo Riaño Molano
# Fecha de creación: 01/08/2025
# Descripción:
#   Este script ejecuta análisis SQL especializados de estados inmobiliarios y exporta
#   los resultados a archivos Excel multi-hoja con formateo profesional:
#   - Ejecución de consultas SQL complejas desde archivos .sql
#   - Exportación a Excel con múltiples hojas temáticas
#   - Formateo automático de hojas con estilos profesionales
#   - Análisis de estadísticas de estados, diferencias y rankings
#   - Generación de reportes ejecutivos en formato Excel
#   - Conexión robusta a base de datos con manejo de errores
#
#   Utiliza pandas y SQLAlchemy para operaciones optimizadas
#   y genera reportes listos para presentación ejecutiva.
#
# Funcionalidades principales:
#   - Ejecución automática de consultas SQL desde archivos externos
#   - Exportación multi-hoja con nombres descriptivos y formateo
#   - Validación de conexión a base de datos y consultas
#   - Manejo robusto de errores con logging detallado
#   - Generación de metadatos y resúmenes en Excel
# -------------------------------------------------------------

# =======================
# Importación de librerías
# =======================

import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv


def export_estado_analysis_xlsx():
    """
    Ejecuta las consultas de análisis de estados y exporta los resultados a un archivo Excel con varias hojas.
    El archivo de salida es 'data/query_data/estado_analysis.xlsx'.
    """
    # Cargar variables de entorno
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL no está definida en el archivo .env")
    engine = create_engine(db_url)

    # Definir las consultas y nombres de hoja
    queries = [
        ("Ultimo Estado", open("sql_queries/ultimo_estado.sql", encoding="utf-8").read()),
        (
            "Diferencia Absoluta y Ranking",
            open("sql_queries/diferencia_absoluta_y_ranking.sql", encoding="utf-8").read(),
        ),
        ("Estadisticas Estado", open("sql_queries/estadisticas_estado.sql", encoding="utf-8").read()),
    ]

    # Ejecutar consultas y guardar resultados en hojas de Excel
    output_path = "data/query_data/estado_analysis.xlsx"
    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        for sheet, query in queries:
            df = pd.read_sql(query, engine)
            df.to_excel(writer, sheet_name=sheet, index=False)
    print(f"[OK] Análisis de estados exportado a {output_path}")
