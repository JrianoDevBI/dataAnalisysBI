"""
Pipeline Optimizado - Modular Version

Este módulo ha sido modularizado. Ahora importa componentes de scripts/optimization/
para mantener compatibilidad hacia atrás mientras permite mejor mantenimiento.

Author: Juan Camilo Riaño Molano  
Date: 06/08/2025
"""

import logging
import time
from typing import Any, Dict

# Set up logging
logger = logging.getLogger(__name__)

# Try to import optimization modules with error handling
_import_success = False
_import_error = None

try:
    from scripts.optimization import (
        DataValidator,
        OptimizedDataLoader,
        OptimizedParallelCleaner,
        OptimizedSQLExecutor,
        OptimizedPipelineOrchestrator,
        ejecutar_pipeline_optimizado_completo,
        ejecutar_pipeline_con_optimizaciones,
        obtener_estado_sistema_optimizado
    )
    _import_success = True
    logger.info("✅ Módulos de optimización importados correctamente")
except ImportError as e:
    _import_error = str(e)
    logger.warning(f"⚠️ Error importando módulos de optimización: {e}")
    _import_success = False
except Exception as e:
    _import_error = str(e)
    logger.error(f"❌ Error crítico importando optimización: {e}")
    _import_success = False

# Backward compatibility - export main functions at module level
def ejecutar_pipeline_optimizado(database_url: str = None) -> Dict[str, Any]:
    """
    Función principal para ejecutar el pipeline optimizado.
    Mantiene compatibilidad hacia atrás.
    
    Args:
        database_url (str): URL de base de datos (opcional)
        
    Returns:
        Dict[str, Any]: Resultado del pipeline o error si no están disponibles los módulos
    """
    if not _import_success:
        return {
            "success": False, 
            "error": f"Módulos de optimización no disponibles: {_import_error}",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
    
    try:
        return ejecutar_pipeline_optimizado_completo(database_url)
    except Exception as e:
        logger.error(f"❌ Error ejecutando pipeline optimizado: {e}")
        return {
            "success": False,
            "error": str(e),
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }

def obtener_estado_modularizacion() -> Dict[str, Any]:
    """
    Obtiene el estado de la modularización.
    
    Returns:
        Dict[str, Any]: Estado de importación y disponibilidad de módulos
    """
    return {
        "modularizacion_exitosa": _import_success,
        "error_importacion": _import_error,
        "modulos_disponibles": [
            "DataValidator",
            "OptimizedDataLoader", 
            "OptimizedParallelCleaner",
            "OptimizedSQLExecutor",
            "OptimizedPipelineOrchestrator"
        ] if _import_success else [],
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }

# Conditional exports based on import success
if _import_success:
    # Full export if optimization modules are available
    __all__ = [
        # Classes
        'DataValidator',
        'OptimizedDataLoader',
        'OptimizedParallelCleaner',
        'OptimizedSQLExecutor',
        'OptimizedPipelineOrchestrator',
        
        # Functions
        'ejecutar_pipeline_optimizado_completo',
        'ejecutar_pipeline_con_optimizaciones',
        'obtener_estado_sistema_optimizado',
        'ejecutar_pipeline_optimizado',  # Backward compatibility
        'obtener_estado_modularizacion'
    ]
else:
    # Limited export if optimization modules are not available
    __all__ = [
        'ejecutar_pipeline_optimizado',
        'obtener_estado_modularizacion'
    ]

# Log module status
if _import_success:
    logger.info("🎯 Pipeline optimizado modularizado - Todos los componentes disponibles")
else:
    logger.warning(f"⚠️ Pipeline optimizado modularizado - Modo degradado: {_import_error}")
