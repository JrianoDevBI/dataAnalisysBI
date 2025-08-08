"""
Utilities module for performance metrics and interactive components.

This module contains utility functions for performance monitoring,
interactive menus, and helper functions for the pipeline.

Author: Juan Camilo Riaño Molano
Date: 06/08/2025
"""

# Import classes
from .performance_metrics import PerformanceMetrics
from .interactive_menu import InteractiveMenu

# Import backward compatibility functions
from .performance_metrics import (
    mostrar_metricas_performance,
    get_system_metrics,
    calculate_performance_score
)
from .interactive_menu import (
    show_main_menu,
    show_help,
    mostrar_analisis_optimizaciones
)

__all__ = [
    # Classes
    'PerformanceMetrics',
    'InteractiveMenu',
    # Backward compatibility functions
    'mostrar_metricas_performance',
    'get_system_metrics',
    'calculate_performance_score',
    'show_main_menu',
    'show_help',
    'mostrar_analisis_optimizaciones'
]
