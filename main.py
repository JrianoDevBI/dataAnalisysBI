# main.py
# Script principal para ejecutar los procesos del pipeline de datos.
#
# Buenas prácticas implementadas:
# - Modularidad: cada proceso está en un script independiente.
# - Validación de rutas y archivos antes de procesar.
# - Claridad en el flujo de usuario y mensajes descriptivos.
# - Uso de funciones y variables descriptivas.
# - Comentarios detallados para facilitar el mantenimiento y la colaboración.

import sys
import os
# Agregar la carpeta scripts al path para importar módulos personalizados
sys.path.append(os.path.join(os.path.dirname(__file__), 'scripts'))

# Importar funciones principales del pipeline
from scripts.obtain_data import obtain_data
from scripts.clean_muestra import clean_muestra
from scripts.clean_estados import clean_estados

from scripts.load_to_sql import main as load_to_sql_main
from scripts.test_db_connection import probar_conexion_db


def run_pipeline():
    """
    Ejecuta el pipeline de procesamiento de datos.
    Permite al usuario seleccionar entre:
      1. Obtener datos desde Excel y generar CSVs procesados.
      2. Limpiar datos de muestra.
      3. Limpiar datos de estados históricos.
      4. Cargar datos limpios a la base de datos SQL.
    El flujo es interactivo y asegura que los pasos previos se hayan realizado antes de continuar.
    """
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
        else:
            print("No se pudo conectar a la base de datos. Revise la configuración y vuelva a intentar.")
    else:
        print("Ejecución finalizada. No se cargaron los datos a la base de datos.")

# Punto de entrada del script principal
if __name__ == '__main__':
    run_pipeline()
