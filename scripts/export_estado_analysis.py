# Script para exportar el análisis de estados a un archivo Excel con varias hojas, usando pandas y SQLAlchemy.
"""
-------------------------------------------------------------
export_estado_analysis.py
Exporta el análisis de estados a un archivo Excel con varias hojas.

Autor: Juan Camilo Riaño Molano
Fecha: 01/08/2025

Descripción:
        Ejecuta consultas SQL y exporta los resultados a un archivo Excel con varias hojas.
        Utiliza pandas y SQLAlchemy para la conexión y manipulación de datos.

Buenas prácticas:
        - Modularidad y reutilización de funciones.
        - Manejo de errores y mensajes claros para el usuario.
-------------------------------------------------------------
"""

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
