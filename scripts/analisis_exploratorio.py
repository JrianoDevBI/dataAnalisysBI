"""
Legacy Exploratory Analysis Module

This module is kept for backward compatibility. All functionality has been
modularized into the scripts.exploratory package.

Author: Juan Camilo Riaño Molano
Date: 06/08/2025
"""

# Import all functions from the new modular structure for backward compatibility
from scripts.exploratory import (
    analizar_relaciones,
    analizar_relaciones_sin_graficos,
    calcular_indicadores_clave,
    detectar_inconsistencias,
    calcular_inconsistencias_especificas,
    ejecutar_analisis_completo
)

# This ensures that existing imports like:
# from scripts.analisis_exploratorio import ejecutar_analisis_completo
# will continue to work without modification

if __name__ == "__main__":
    # Execute complete analysis when run directly
    ejecutar_analisis_completo()
