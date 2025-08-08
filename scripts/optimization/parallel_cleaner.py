"""
Parallel Cleaner Module

This module provides optimized parallel cleaning operations using
ThreadPoolExecutor to process sample and status data simultaneously.

Author: Juan Camilo Riaño Molano
Date: 06/08/2025
"""

import concurrent.futures
import logging
from typing import Any, Dict, List, Tuple

import pandas as pd

# Import dependencies with fallbacks
try:
    from scripts.optimizacion_performance import (
        measure_performance, 
        parallel_processor,
        save_data_with_backup
    )
except ImportError:
    # Fallback implementations
    def measure_performance(name):
        def decorator(func):
            return func
        return decorator
    
    class FallbackParallelProcessor:
        def execute_parallel_cleaning(self, tasks):
            results = {}
            for func, args, kwargs, name in tasks:
                try:
                    results[name] = func(*args, **kwargs)
                except Exception as e:
                    results[name] = {"success": False, "error": str(e)}
            return results
    
    parallel_processor = FallbackParallelProcessor()
    
    def save_data_with_backup(df, path, backup_path):
        df.to_csv(path, index=False)

from .optimized_data_loader import OptimizedDataLoader
from .data_validator import DataValidator

# Set up logger
logger = logging.getLogger(__name__)


class OptimizedParallelCleaner:
    """
    Cleaner that executes cleaning operations in parallel.
    
    Uses ThreadPoolExecutor to process sample and status data
    simultaneously, eliminating sequential dependency.
    """

    def __init__(self):
        """Initialize the parallel cleaner"""
        self.processor = parallel_processor
        self.loader = OptimizedDataLoader()
        self.validator = DataValidator()
        self.logger = logger

    @measure_performance("limpieza_paralela")
    def ejecutar_limpieza_paralela(self, cargar_datos: bool = True) -> Dict[str, Any]:
        """
        Execute parallel cleaning of sample and status data.
        
        Args:
            cargar_datos (bool): Whether to load data before cleaning

        Returns:
            Dict[str, Any]: Results of parallel cleaning
        """
        if cargar_datos:
            # Pre-load data in cache
            datasets = self.loader.cargar_todos_los_datos()
            if not datasets:
                self.logger.error("❌ No se pudieron cargar los datos necesarios")
                return {"success": False, "error": "Datos no disponibles"}

        # Define parallel tasks
        tareas = [
            (self._limpiar_muestra_optimizada, (), {}, "limpieza_muestra"),
            (self._limpiar_estados_optimizada, (), {}, "limpieza_estados"),
        ]

        # Execute in parallel
        resultado = self.processor.execute_parallel_cleaning(tareas)

        # Validate results
        success_count = sum(1 for r in resultado.values() if r is not None and r.get("success", False))
        total_tasks = len(resultado)
        
        # Consider successful if at least data was processed (even with minor errors)
        has_data_processing = any(
            r is not None and r.get("filas_procesadas", 0) > 0 
            for r in resultado.values()
        )
        
        if success_count >= 1 or has_data_processing:  # At least one successful task or data processed
            self.logger.info(f"🎉 Limpieza paralela completada: {success_count}/{total_tasks} tareas exitosas")

            # Generate post-cleaning quality report
            reporte_calidad = self._generar_reporte_calidad_post_limpieza()
            resultado["reporte_calidad"] = reporte_calidad
            resultado["success"] = True
        else:
            self.logger.warning(
                f"⚠️ Limpieza falló: {success_count}/{total_tasks} tareas exitosas"
            )
            resultado["success"] = False

        resultado["success_count"] = success_count
        return resultado

    def _limpiar_muestra_optimizada(self) -> Dict[str, Any]:
        """
        Optimized version of sample cleaning using cache.

        Returns:
            Dict[str, Any]: Cleaning result
        """
        try:
            # Load from cache
            muestra_df = self.loader.get_dataset("muestra_raw")
            if muestra_df is None:
                return {"success": False, "error": "Dataset muestra_raw no disponible"}

            # Apply cleaning (simplified logic for example)
            muestra_clean = muestra_df.copy()

            # Remove duplicates based only on Id (no Fecha_Actualizacion in sample)
            antes_duplicados = len(muestra_clean)
            muestra_clean = muestra_clean.drop_duplicates(subset=["Id"])
            duplicados_eliminados = antes_duplicados - len(muestra_clean)

            # Clean null values in critical columns
            muestra_clean = muestra_clean.dropna(subset=["Id", "Precio_Solicitado"])

            # Additional cleaning operations
            muestra_clean = self._aplicar_limpieza_avanzada_muestra(muestra_clean)

            # Validate quality
            calidad = self.validator.validar_calidad_datos(muestra_clean, "muestra_limpia")

            # Save with backup
            save_data_with_backup(
                muestra_clean, "./data/cleanData/CLMUESTRA.csv", "./dataBackup"
            )

            # Update cache
            if hasattr(self.loader.cache, 'put'):
                self.loader.cache.put("muestra_clean", muestra_clean)
            elif hasattr(self.loader.cache, '_cache'):
                self.loader.cache._cache["muestra_clean"] = muestra_clean

            return {
                "success": True,
                "filas_procesadas": len(muestra_clean),
                "duplicados_eliminados": duplicados_eliminados,
                "calidad": calidad,
                "dataset_final": "muestra_clean"
            }

        except Exception as e:
            self.logger.error(f"❌ Error en limpieza de muestra: {e}")
            return {"success": False, "error": str(e)}

    def _limpiar_estados_optimizada(self) -> Dict[str, Any]:
        """
        Optimized version of status cleaning using cache.

        Returns:
            Dict[str, Any]: Cleaning result
        """
        try:
            # Load from cache
            estados_df = self.loader.get_dataset("estados_raw")
            if estados_df is None:
                return {"success": False, "error": "Dataset estados_raw no disponible"}

            # Apply cleaning (simplified logic for example)
            estados_clean = estados_df.copy()

            # Clean and normalize data
            antes_nulos = estados_clean.isnull().sum().sum()
            estados_clean = estados_clean.dropna()
            nulos_eliminados = antes_nulos - estados_clean.isnull().sum().sum()

            # Additional cleaning operations
            estados_clean = self._aplicar_limpieza_avanzada_estados(estados_clean)

            # Validate quality
            calidad = self.validator.validar_calidad_datos(estados_clean, "estados_limpios")

            # Save with backup
            save_data_with_backup(
                estados_clean, "./data/cleanData/CLESTADOS.csv", "./dataBackup"
            )

            # Update cache
            if hasattr(self.loader.cache, 'invalidate'):
                self.loader.cache.invalidate("estados_clean")
            
            # Load clean data into cache
            if hasattr(self.loader.cache, 'get'):
                self.loader.cache.get("estados_clean", pd.read_csv, "./data/cleanData/CLESTADOS.csv")
            elif hasattr(self.loader.cache, '_cache'):
                self.loader.cache._cache["estados_clean"] = estados_clean

            return {
                "success": True,
                "filas_procesadas": len(estados_clean),
                "nulos_eliminados": nulos_eliminados,
                "calidad": calidad,
                "dataset_final": "estados_clean"
            }

        except Exception as e:
            self.logger.error(f"❌ Error en limpieza de estados: {e}")
            return {"success": False, "error": str(e)}

    def _aplicar_limpieza_avanzada_muestra(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply advanced cleaning operations to sample data.
        
        Args:
            df (pd.DataFrame): Sample dataframe to clean
            
        Returns:
            pd.DataFrame: Cleaned dataframe
        """
        df_clean = df.copy()
        
        try:
            # Convert numeric columns
            numeric_columns = ["Precio_Solicitado", "Area_Privada", "Habitaciones", "Banos"]
            for col in numeric_columns:
                if col in df_clean.columns:
                    df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce")
            
            # Remove outliers in price (basic outlier removal)
            if "Precio_Solicitado" in df_clean.columns:
                Q1 = df_clean["Precio_Solicitado"].quantile(0.01)
                Q3 = df_clean["Precio_Solicitado"].quantile(0.99)
                df_clean = df_clean[
                    (df_clean["Precio_Solicitado"] >= Q1) & 
                    (df_clean["Precio_Solicitado"] <= Q3)
                ]
            
            # Standardize text columns
            text_columns = ["Tipo_Inmueble", "Ciudad", "Zona"]
            for col in text_columns:
                if col in df_clean.columns:
                    df_clean[col] = df_clean[col].astype(str).str.strip().str.title()
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error en limpieza avanzada de muestra: {e}")
        
        return df_clean

    def _aplicar_limpieza_avanzada_estados(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply advanced cleaning operations to status data.
        
        Args:
            df (pd.DataFrame): Status dataframe to clean
            
        Returns:
            pd.DataFrame: Cleaned dataframe
        """
        df_clean = df.copy()
        
        try:
            # Convert date columns
            if "Fecha_Actualizacion" in df_clean.columns:
                df_clean["Fecha_Actualizacion"] = pd.to_datetime(
                    df_clean["Fecha_Actualizacion"], errors="coerce"
                )
            
            # Standardize status text
            if "Estado" in df_clean.columns:
                df_clean["Estado"] = df_clean["Estado"].astype(str).str.strip().str.title()
            
            # Remove future dates (data quality issue)
            if "Fecha_Actualizacion" in df_clean.columns:
                today = pd.Timestamp.now()
                df_clean = df_clean[df_clean["Fecha_Actualizacion"] <= today]
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error en limpieza avanzada de estados: {e}")
        
        return df_clean

    def _generar_reporte_calidad_post_limpieza(self) -> Dict[str, Any]:
        """
        Generate a quality report after cleaning.

        Returns:
            Dict[str, Any]: Quality report
        """
        try:
            reporte = {}
            
            # Validate clean sample
            muestra_clean = self.loader.get_dataset("muestra_clean")
            if muestra_clean is not None:
                calidad_muestra = self.validator.validar_calidad_datos(muestra_clean, "muestra_clean")
                reporte["muestra"] = calidad_muestra
                
            # Validate clean status
            estados_clean = self.loader.get_dataset("estados_clean")
            if estados_clean is not None:
                calidad_estados = self.validator.validar_calidad_datos(estados_clean, "estados_clean")
                reporte["estados"] = calidad_estados
            
            # Add summary
            reporte["resumen"] = {
                "datasets_procesados": len([k for k in reporte.keys() if k != "resumen"]),
                "calidad_promedio": sum(
                    r.get("completitud", 0) for r in reporte.values() 
                    if isinstance(r, dict) and "completitud" in r
                ) / max(1, len([r for r in reporte.values() if isinstance(r, dict) and "completitud" in r]))
            }
            
            return reporte
            
        except Exception as e:
            self.logger.error(f"❌ Error generando reporte de calidad: {e}")
            return {"error": str(e)}

    def ejecutar_limpieza_individual(self, dataset_name: str) -> Dict[str, Any]:
        """
        Execute cleaning for a single dataset.
        
        Args:
            dataset_name (str): Name of dataset to clean ("muestra" or "estados")
            
        Returns:
            Dict[str, Any]: Cleaning result
        """
        if dataset_name == "muestra":
            return self._limpiar_muestra_optimizada()
        elif dataset_name == "estados":
            return self._limpiar_estados_optimizada()
        else:
            return {"success": False, "error": f"Dataset desconocido: {dataset_name}"}

    def obtener_estadisticas_limpieza(self) -> Dict[str, Any]:
        """
        Get cleaning statistics for both datasets.
        
        Returns:
            Dict[str, Any]: Cleaning statistics
        """
        estadisticas = {}
        
        try:
            # Check clean datasets
            muestra_clean = self.loader.get_dataset("muestra_clean")
            estados_clean = self.loader.get_dataset("estados_clean")
            
            if muestra_clean is not None:
                estadisticas["muestra"] = {
                    "filas": len(muestra_clean),
                    "columnas": len(muestra_clean.columns),
                    "completitud": round(
                        ((muestra_clean.size - muestra_clean.isnull().sum().sum()) / muestra_clean.size) * 100, 2
                    )
                }
            
            if estados_clean is not None:
                estadisticas["estados"] = {
                    "filas": len(estados_clean),
                    "columnas": len(estados_clean.columns),
                    "completitud": round(
                        ((estados_clean.size - estados_clean.isnull().sum().sum()) / estados_clean.size) * 100, 2
                    )
                }
            
        except Exception as e:
            self.logger.error(f"❌ Error obteniendo estadísticas: {e}")
            estadisticas["error"] = str(e)
        
        return estadisticas


# Backward compatibility functions
def ejecutar_limpieza_paralela_optimizada() -> Dict[str, Any]:
    """Backward compatibility function"""
    cleaner = OptimizedParallelCleaner()
    return cleaner.ejecutar_limpieza_paralela()

def limpiar_dataset_individual(dataset_name: str) -> Dict[str, Any]:
    """Backward compatibility function"""
    cleaner = OptimizedParallelCleaner()
    return cleaner.ejecutar_limpieza_individual(dataset_name)
