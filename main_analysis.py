# main_analysis.py
# Script interactivo para ejecutar análisis exploratorio sobre los datos limpios del proyecto.
# Permite seleccionar y ejecutar las funciones de análisis del archivo analisis_exploratorio.py
# y retorna al menú tras cerrar cada gráfico.

import sys
import os
import pandas as pd

# Asegura que la carpeta scripts esté en el path
sys.path.append(os.path.join(os.path.dirname(__file__), 'scripts'))

from scripts.analisis_exploratorio import analizar_relaciones, detectar_inconsistencias

default_path = os.path.join('data', 'cleanData', 'CLMUESTRA.csv')

def main():
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

if __name__ == "__main__":
    main()
