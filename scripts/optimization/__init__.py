"""
Optimization Package

This package contains modular components for optimized pipeline execution
with parallel processing, caching, and performance improvements.

Author: Juan Camilo Riaño Molano
Date: 06/08/2025
"""

# Import all main classes and functions for easy access
from .data_validator import DataValidator
from .optimized_data_loader import OptimizedDataLoader
from .parallel_cleaner import OptimizedParallelCleaner
from .sql_executor import OptimizedSQLExecutor
from .pipeline_orchestrator import (
    OptimizedPipelineOrchestrator,
    ejecutar_pipeline_optimizado_completo,
    ejecutar_pipeline_con_optimizaciones,
    obtener_estado_sistema_optimizado
)

__all__ = [
    # Classes
    'DataValidator',
    'OptimizedDataLoader', 
    'OptimizedParallelCleaner',
    'OptimizedSQLExecutor',
    'OptimizedPipelineOrchestrator',
    
    # Backward compatibility functions
    'ejecutar_pipeline_optimizado_completo',
    'ejecutar_pipeline_con_optimizaciones',
    'obtener_estado_sistema_optimizado'
]
