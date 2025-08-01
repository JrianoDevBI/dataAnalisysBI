"""
-------------------------------------------------------------
main.py
Script principal para orquestar el pipeline de procesamiento y análisis de datos.

Autor: Juan Camilo Riaño Molano
Fecha: 01/08/2025

Descripción:
    Ejecuta el pipeline completo: limpieza, backup, carga, análisis y exportación de resultados.
    Orquesta la ejecución de scripts modulares y asegura la correcta secuencia de pasos.

Buenas prácticas:
    - Modularidad y reutilización de funciones.
    - Validación de rutas, archivos y variables de entorno.
    - Manejo de errores y mensajes claros para el usuario.
-------------------------------------------------------------
"""


import os
from scripts.clean_and_backup_data import clean_and_backup_data
from scripts.export_sql_to_excel import export_query_to_excel
from scripts.export_estado_analysis import export_estado_analysis_xlsx
from scripts.clean_muestra import clean_muestra
from scripts.clean_estados import clean_estados
from scripts.obtain_data import obtain_data
from scripts.test_db_connection import probar_conexion_db
from scripts.load_to_sql import main as load_to_sql_main
from dotenv import load_dotenv

import subprocess
import sys
import pandas as pd
from sqlalchemy import create_engine

# QUERIES para exportación múltiple
QUERIES = [
    ("Ultimo Estado", open('sql_queries/ultimo_estado.sql', encoding='utf-8').read()),
    ("Diferencia Absoluta y Ranking", open('sql_queries/diferencia_absoluta_y_ranking.sql', encoding='utf-8').read()),
    ("Estadisticas Estado", open('sql_queries/estadisticas_estado.sql', encoding='utf-8').read()),
]

# =======================
# Función principal del pipeline
# =======================
def run_pipeline():
    """
    Ejecuta el pipeline de procesamiento de datos.
    Permite al usuario seleccionar entre:
      1. Obtener datos desde Excel y generar CSVs procesados.
      2. Limpiar datos de muestra.
      3. Limpiar datos de estados históricos.
      4. Cargar datos limpios a la base de datos SQL.
    El flujo es interactivo y asegura que los pasos previos se hayan realizado antes de continuar.

    Proceso:
    1. Solicita limpieza y backup de datos antes de iniciar el pipeline.
    2. Ejecuta cada etapa del pipeline de forma secuencial y validada.
    3. Exporta resultados y muestra resúmenes en consola.

    Buenas prácticas:
    - Modularidad: cada proceso está en un script independiente.
    - Validación de rutas y archivos antes de procesar.
    - Claridad en el flujo de usuario y mensajes descriptivos.
    - Uso de funciones y variables descriptivas.
    - Comentarios detallados para facilitar el mantenimiento y la colaboración.

    Manejo de errores:
        FileNotFoundError: Si algún archivo requerido no existe.
        ValueError: Si faltan variables de entorno o parámetros críticos.
        Exception: Si ocurre un error inesperado en alguna etapa.
    """
    # Solicitar limpieza y backup de datos antes de iniciar el pipeline
    clean_and_backup_data()
    processed_dir = os.path.join(os.getcwd(), 'data', 'processedData')
    muestra_path = os.path.join(processed_dir, 'muestra.csv')
    estados_path = os.path.join(processed_dir, 'estados.csv')
    clean_dir = os.path.join(os.getcwd(), 'data', 'cleanData')
    clmuestra_path = os.path.join(clean_dir, 'CLMUESTRA.csv')
    clestados_path = os.path.join(clean_dir, 'CLESTADOS.csv')

    # Paso 1: Obtener datos si no existen
    if not (os.path.exists(muestra_path) and os.path.exists(estados_path)):
        print("[1] Obteniendo datos desde Excel y generando CSVs procesados...")
        obtain_data()
    else:
        print("[Datos ya importados: muestra.csv y estados.csv]")

    # Paso 2: Limpiar datos de muestra
    print("[2] Limpiando datos de muestra (muestra.csv)...")
    clean_muestra(
        './data/processedData/muestra.csv',
        './data/cleanData/CLMUESTRA.csv',
        './data/processedData/outliers_log.csv'
    )

    # Paso 3: Limpiar datos de estados
    print("[3] Limpiando datos de estados (estados.csv)...")
    clean_estados(
        './data/processedData/estados.csv',
        './data/cleanData/CLESTADOS.csv'
    )

    # Confirmar antes de cargar a SQL
    print("\n¿Los datos están listos para cargarse a la base de datos SQL?")
    confirm = input('Escriba "Si" para continuar con la carga, o "No" para finalizar: ').strip().lower()
    if confirm == 'si':
        print("Probando conexión a la base de datos...")
        if probar_conexion_db():
            print("Cargando datos limpios a la base de datos SQL...")
            load_to_sql_main()
            print("Datos cargados exitosamente a la base de datos.")
            # Exportar resultados de queries a Excel
            from dotenv import load_dotenv
            load_dotenv()
            db_url = os.getenv('DATABASE_URL')
            if not db_url:
                print('No se encontró la variable DATABASE_URL en el entorno. No se exportaron los resultados de queries.')
                return
            print("Exportando resultados de queries a archivos Excel...")
            export_query_to_excel('sql_queries/ultimo_estado.sql', db_url)
            print("[OK] Query 'ultimo_estado.sql' exportado correctamente.")
            export_query_to_excel('sql_queries/diferencia_absoluta_y_ranking.sql', db_url)
            print("[OK] Query 'diferencia_absoluta_y_ranking.sql' exportado correctamente.")

            # Exportar análisis de estados a un solo Excel con varias hojas
            print("Exportando análisis de estados a 'estado_analysis.xlsx'...")
            export_estado_analysis_xlsx()
            print("[OK] Análisis de estados exportado correctamente.")

            # Mostrar resultados principales de cada consulta en consola
            from sqlalchemy import create_engine
            engine = create_engine(db_url)
            import pandas as pd
            print("\nResumen de análisis de estados:")
            for sheet, query in QUERIES:
                try:
                    df = pd.read_sql(query, engine)
                    print(f"\n[{sheet}]")
                    if not df.empty:
                        print(df.head(10).to_string(index=False))
                    else:
                        print("Sin resultados.")
                except Exception as e:
                    print(f"Error ejecutando consulta '{sheet}': {e}")
        else:
            print("No se pudo conectar a la base de datos. Revise la configuración y vuelva a intentar.")
    else:
        print("Ejecución finalizada. No se cargaron los datos a la base de datos.")


# =======================
# Función para análisis exploratorio
# =======================
def preguntar_analisis_exploratorio():
    """
    Pregunta al usuario si desea realizar un análisis exploratorio de los datos
    y ejecuta main_analysis.py si la respuesta es afirmativa.
    """
    print("\n¿Desea realizar un análisis exploratorio de los datos?")
    respuesta = input('Escriba "Si" para ejecutar el análisis, o cualquier otra tecla para finalizar: ').strip().lower()
    if respuesta == 'si':
        subprocess.run([sys.executable, 'main_analysis.py'])
    else:
        print("Ejecución finalizada.")

# =======================
# Punto de entrada del script principal
# =======================
if __name__ == '__main__':
    run_pipeline()
    preguntar_analisis_exploratorio()
