

# -------------------------------------------------------------
# main_analysis.py
# Script interactivo para ejecutar análisis exploratorio sobre los datos limpios del proyecto.
# Permite seleccionar y ejecutar las funciones de análisis del archivo analisis_exploratorio.py
# y retorna al menú tras cerrar cada gráfico.
#
# Autor: Juan Camilo Riaño Molano
# Fecha de creación: 01/08/2025
# Descripción:
#   Este script permite al usuario seleccionar y ejecutar análisis exploratorio sobre los datos limpios,
#   facilitando la visualización y detección de inconsistencias en los datos inmobiliarios.
# -------------------------------------------------------------

# =======================
# Importación de librerías
# =======================
import sys
import os
import pandas as pd

# =======================
# Configuración de rutas y módulos
# =======================
# Asegura que la carpeta scripts esté en el path
sys.path.append(os.path.join(os.path.dirname(__file__), 'scripts'))
from scripts.analisis_exploratorio import analizar_relaciones, detectar_inconsistencias

default_path = os.path.join('data', 'cleanData', 'CLMUESTRA.csv')

# =======================
# Función principal interactiva
# =======================
def main():
    """
    Menú interactivo para ejecutar análisis exploratorio sobre los datos limpios.

    Proceso:
    1. Solicita la ruta del archivo limpio a analizar (por defecto usa CLMUESTRA.csv).
    2. Permite seleccionar entre análisis de relaciones, detección de inconsistencias o ambos.
    3. Ejecuta la función correspondiente y espera confirmación para volver al menú.

    Buenas prácticas:
    - Valida la existencia del archivo antes de procesar.
    - Proporciona mensajes claros y descriptivos al usuario.
    - Permite repetir análisis sin reiniciar el script.

    Manejo de errores:
        FileNotFoundError: Si el archivo de entrada no existe.
        Exception: Si ocurre un error inesperado durante el análisis.
    """
    print("\n--- Análisis Exploratorio de Datos Inmobiliarios ---")
    ruta = input(f"Ingrese la ruta al archivo limpio a analizar (ENTER para usar '{default_path}'): ").strip()
    if not ruta:
        ruta = default_path
    if not os.path.exists(ruta):
        print(f"No se encontró el archivo {ruta}. Ejecuta primero el pipeline de limpieza.")
        return
    df = pd.read_csv(ruta)
    print(f"\nArchivo cargado: {ruta} (registros: {len(df)})")
    while True:
        print("\nSeleccione el análisis a ejecutar:")
        print("1. Analizar relaciones entre características (correlación, gráficos, tablas)")
        print("2. Detectar inconsistencias y outliers por agrupación")
        print("3. Ejecutar ambos análisis")
        print("4. Salir")
        opcion = input("Opción: ").strip()
        match opcion:
            case '1':
                analizar_relaciones(df)
                input("Presione ENTER para volver al menú...")
            case '2':
                detectar_inconsistencias(df)
                input("Presione ENTER para volver al menú...")
            case '3':
                analizar_relaciones(df)
                input("Presione ENTER para continuar...")
                detectar_inconsistencias(df)
                input("Presione ENTER para volver al menú...")
            case '4':
                print("Análisis finalizado.")
                break
            case _:
                print("Opción no válida. Intente de nuevo.")

# =======================
# Punto de entrada del script
# =======================
if __name__ == "__main__":
    main()
