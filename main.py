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
    # Definir rutas de archivos procesados y limpios
    processed_dir = os.path.join(os.getcwd(), 'data', 'processedData')
    muestra_path = os.path.join(processed_dir, 'muestra.csv')
    estados_path = os.path.join(processed_dir, 'estados.csv')
    clean_dir = os.path.join(os.getcwd(), 'data', 'cleanData')
    clmuestra_path = os.path.join(clean_dir, 'CLMUESTRA.csv')
    clestados_path = os.path.join(clean_dir, 'CLESTADOS.csv')

    # Verificar si los archivos procesados existen
    archivos_listos = os.path.exists(muestra_path) and os.path.exists(estados_path)

    while True:
        # Menú interactivo para el usuario
        print("Seleccione el proceso que desea realizar:")
        opciones = []
        if not archivos_listos:
            # Solo se permite obtener datos si aún no existen los archivos procesados
            print("1. Obtener datos desde Excel y generar CSVs limpios")
            opciones.append('1')
        else:
            print("[Datos ya importados: muestra.csv y estados.csv]")
        print("2. Limpiar datos de muestra (muestra.csv)")
        print("3. Limpiar datos de estados (estados.csv)")
        print("4. Cargar datos limpios a la base de datos SQL")
        opcion = input(f"Ingrese el número de la opción ({'/'.join(opciones + ['2','3','4'])}): ").strip()

        # Ejecutar la opción seleccionada
        match opcion:
            case '1' if not archivos_listos:
                # Obtención y procesamiento inicial de datos desde Excel
                obtain_data()
                archivos_listos = os.path.exists(muestra_path) and os.path.exists(estados_path)
                break
            case '2':
                # Limpieza de datos de muestra y guardado en cleanData
                clean_muestra(
                    './data/processedData/muestra.csv',
                    './data/cleanData/CLMUESTRA.csv',
                    './data/processedData/outliers_log.csv'
                )
                break
            case '3':
                # Limpieza de datos de estados históricos y guardado en cleanData
                clean_estados(
                    './data/processedData/estados.csv',
                    './data/cleanData/CLESTADOS.csv'
                )
                break
            case '4':
                # Carga de datos limpios a la base de datos SQL
                load_to_sql_main()
                break
            case _:
                print("Opción no válida. Intente de nuevo.")

# Punto de entrada del script principal
if __name__ == '__main__':
    run_pipeline()
