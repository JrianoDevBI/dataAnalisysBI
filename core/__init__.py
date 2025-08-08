"""
Core module for pipeline orchestration and business logic.

This module contains the core functionality for the data analysis pipeline,
including orchestration, execution modes, indicators calculation, and 
executive diagrams generation.

Author: Juan Camilo Riaño Molano
Date: 06/08/2025
"""

# Import classes
from .pipeline_orchestrator import PipelineOrchestrator
from .execution_modes import ExecutionModes
from .indicators_calculator import IndicatorsCalculator
from .executive_diagrams import ExecutiveDiagrams

# Import backward compatibility functions
from .pipeline_orchestrator import run_pipeline, ejecutar_pipeline_unificado
from .execution_modes import (
    ejecutar_modo_automatico_completo, 
    ejecutar_modo_interactivo, 
    ejecutar_pipeline_optimizado
)
from .indicators_calculator import calcular_indicadores_clave
from .executive_diagrams import (
    crear_diagrama_flujo_ejecutivo,
    generar_metricas_rendimiento,
    generar_comparacion_optimizacion,
    generar_todos_los_diagramas
)

__all__ = [
    # Classes
    'PipelineOrchestrator',
    'ExecutionModes', 
    'IndicatorsCalculator',
    'ExecutiveDiagrams',
    # Backward compatibility functions
    'run_pipeline',
    'ejecutar_pipeline_unificado',
    'ejecutar_modo_automatico_completo',
    'ejecutar_modo_interactivo',
    'ejecutar_pipeline_optimizado',
    'calcular_indicadores_clave',
    'crear_diagrama_flujo_ejecutivo',
    'generar_metricas_rendimiento',
    'generar_comparacion_optimizacion',
    'generar_todos_los_diagramas'
]
