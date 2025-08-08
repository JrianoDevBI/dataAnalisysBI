"""
Exploratory Analysis Package

This package contains modular components for exploratory data analysis
of real estate data.

Author: Juan Camilo Riaño Molano
Date: 06/08/2025
"""

# Import all main classes and functions for easy access
from .correlation_analysis import CorrelationAnalyzer
from .outlier_detection import OutlierDetector
from .indicators_calculator import KeyIndicatorsCalculator, InconsistencyAnalyzer
from .visualization_generator import VisualizationGenerator

# Backward compatibility imports - main functions from original analisis_exploratorio.py
from .correlation_analysis import (
    analizar_relaciones,
    analizar_relaciones_sin_graficos,
    analizar_correlaciones,
    mostrar_matriz_correlacion
)

from .outlier_detection import (
    detectar_outliers,
    analizar_outliers_precio,
    detectar_inconsistencias
)

from .indicators_calculator import (
    calcular_indicadores_clave,
    calcular_inconsistencias_especificas
)

from .visualization_generator import (
    generar_graficos_basicos,
    generar_graficos_avanzados,
    generar_dashboard_interactivo
)

# Main execution function for backward compatibility
def ejecutar_analisis_completo(
    ruta_muestra="data/cleanData/CLMUESTRA.csv", 
    ruta_estados="data/cleanData/CLESTADOS.csv", 
    mostrar_graficos=True
):
    """
    Main function to execute complete analysis from main.py
    
    Args:
        ruta_muestra (str): Path to sample file
        ruta_estados (str): Path to status file
        mostrar_graficos (bool): Whether to show interactive graphics or text analysis only
    """
    import pandas as pd
    import traceback
    
    # Determine data stage based on file path
    data_stage = "limpios" if "cleanData" in ruta_muestra else "procesados"
    stage_emoji = "✨" if data_stage == "limpios" else "📊"
    stage_description = "LIMPIOS (post-procesamiento)" if data_stage == "limpios" else "ORIGINALES (pre-limpieza)"
    
    print("\n--- Análisis Exploratorio de Datos Inmobiliarios ---")
    print(f"Iniciando análisis con correlación Estrato vs Precio e indicadores clave...")
    print(f"{stage_emoji} TIPO DE DATOS: {stage_description}")

    try:
        # Load sample file
        df_muestra = pd.read_csv(ruta_muestra)
        print(f"\n✓ Archivo muestra cargado: {ruta_muestra} (registros: {len(df_muestra)})")

        # Load status file if exists
        df_estados = None
        try:
            df_estados = pd.read_csv(ruta_estados)
            print(f"✓ Archivo estados cargado: {ruta_estados} (registros: {len(df_estados)})")
        except FileNotFoundError:
            print(f"⚠️  Archivo estados no encontrado: {ruta_estados}")
            print("ℹ️  Continuando análisis solo con datos de muestra...")

        # 1. Calculate key indicators FIRST (with data stage info)
        print("\n" + "=" * 60)
        print("1. CALCULANDO INDICADORES CLAVE")
        print("=" * 60)
        calcular_indicadores_clave(df_muestra, df_estados, data_stage)

        # 2. Relationship analysis (including Estrato vs Precio)
        print("\n" + "=" * 60)
        print("2. ANÁLISIS DE RELACIONES Y CORRELACIONES")
        print("=" * 60)
        if mostrar_graficos:
            analizar_relaciones(df_muestra)
        else:
            analizar_relaciones_sin_graficos(df_muestra)

        # 3. Calculate specific inconsistencies (with data stage info)
        print("\n" + "=" * 60)
        print("3. INCONSISTENCIAS ESPECÍFICAS POR AGRUPACIONES")
        print("=" * 60)
        calcular_inconsistencias_especificas(df_muestra, data_stage)

        # 4. General inconsistency detection (with data stage info)
        print("\n" + "=" * 60)
        print("4. DETECCIÓN DE OUTLIERS GENERALES")
        print("=" * 60)
        detectar_inconsistencias(df_muestra, data_stage)

        print("\n" + "=" * 60)
        print("ANÁLISIS COMPLETADO EXITOSAMENTE")
        print("=" * 60)

        return True

    except FileNotFoundError as e:
        print(f"✗ No se encontró un archivo requerido: {e}")
        if "cleanData" in str(e):
            print("ℹ️  Los archivos limpios no existen aún.")
            print("ℹ️  Esto es normal si es la primera ejecución o estás en fase de pre-análisis.")
            print("ℹ️  El pipeline continuará con los siguientes pasos.")
        else:
            print("ℹ️  Verifica que los archivos de datos estén disponibles.")
        return False
    except Exception as e:
        print(f"✗ Error durante el análisis: {e}")
        traceback.print_exc()
        return False

__all__ = [
    # Classes
    'CorrelationAnalyzer',
    'OutlierDetector', 
    'KeyIndicatorsCalculator',
    'InconsistencyAnalyzer',
    'VisualizationGenerator',
    
    # Backward compatibility functions
    'analizar_relaciones',
    'analizar_relaciones_sin_graficos',
    'analizar_correlaciones',
    'mostrar_matriz_correlacion',
    'detectar_outliers',
    'analizar_outliers_precio',
    'detectar_inconsistencias',
    'calcular_indicadores_clave',
    'calcular_inconsistencias_especificas',
    'generar_graficos_basicos',
    'generar_graficos_avanzados',
    'generar_dashboard_interactivo',
    'ejecutar_analisis_completo'
]
