"""
Optimized Data Loader Module

This module provides optimized data loading with intelligent caching 
to eliminate multiple I/O operations on the same files.

Author: Juan Camilo Riaño Molano
Date: 06/08/2025
"""

import logging
import os
from typing import Dict, Optional

import pandas as pd

# Import performance measurement and cache if available
try:
    from scripts.optimizacion_performance import measure_performance, data_cache
except ImportError:
    # Fallback implementations
    def measure_performance(name):
        def decorator(func):
            return func
        return decorator
    
    class FallbackCache:
        def __init__(self):
            self._cache = {}
        
        def get(self, key, loader_func=None, *args, **kwargs):
            if key in self._cache:
                return self._cache[key]
            if loader_func:
                try:
                    result = loader_func(*args, **kwargs)
                    self._cache[key] = result
                    return result
                except Exception:
                    return None
            return None
    
    data_cache = FallbackCache()

from .data_validator import DataValidator

# Set up logger
logger = logging.getLogger(__name__)


class OptimizedDataLoader:
    """
    Optimized loader that uses cache to eliminate multiple reads.
    
    Implements an intelligent caching system that keeps DataFrames
    in memory to avoid multiple I/O operations on the same files.
    """

    def __init__(self):
        """Initialize the optimized data loader"""
        self.cache = data_cache
        self.validator = DataValidator()
        self.logger = logger

    def get_dataset(self, dataset_key: str) -> Optional[pd.DataFrame]:
        """
        Get a dataset from cache.
        
        Args:
            dataset_key (str): Dataset key
            
        Returns:
            Optional[pd.DataFrame]: Dataset or None if not exists
        """
        return self.cache.get(dataset_key)

    @measure_performance("carga_datos_completa")
    def cargar_todos_los_datos(self) -> Dict[str, pd.DataFrame]:
        """
        Load all necessary datasets using intelligent cache.
        
        Returns:
            Dict[str, pd.DataFrame]: Dictionary with all datasets
        """
        datasets = {}
        
        # Define dataset paths and keys
        rutas_datasets = {
            "muestra_raw": "./data/processedData/muestra.csv",
            "estados_raw": "./data/processedData/estados.csv",
            "muestra_clean": "./data/cleanData/CLMUESTRA.csv", 
            "estados_clean": "./data/cleanData/CLESTADOS.csv"
        }
        
        for key, path in rutas_datasets.items():
            if os.path.exists(path):
                # Use cache to load (avoids multiple reads)
                df = self.cache.get(key, pd.read_csv, path)
                if df is not None:
                    datasets[key] = df
                    self.logger.info(f"📂 Cargado {key}: {len(df)} filas")
                    
                    # Validate structure only for raw datasets
                    if "raw" in key:
                        issues = self.validator.validar_estructura_basica(df, key)
                        if issues:
                            self.logger.warning(f"⚠️ Issues en estructura de {key.split('_')[0]}: {issues}")
                else:
                    self.logger.error(f"❌ Error cargando {key} desde {path}")
            else:
                self.logger.warning(f"⚠️ Archivo no encontrado: {path}")
        
        self.logger.info(f"🎯 Carga completa: {len(datasets)} datasets en cache")
        return datasets
    
    def cargar_dataset_individual(self, ruta: str, dataset_key: str, validar: bool = True) -> Optional[pd.DataFrame]:
        """
        Load a single dataset with caching.
        
        Args:
            ruta (str): File path
            dataset_key (str): Cache key for the dataset
            validar (bool): Whether to validate the dataset structure
            
        Returns:
            Optional[pd.DataFrame]: Loaded dataset or None if error
        """
        if not os.path.exists(ruta):
            self.logger.error(f"❌ Archivo no encontrado: {ruta}")
            return None
        
        try:
            # Use cache to load
            df = self.cache.get(dataset_key, pd.read_csv, ruta)
            
            if df is not None:
                self.logger.info(f"📂 Cargado {dataset_key}: {len(df)} filas, {len(df.columns)} columnas")
                
                if validar:
                    issues = self.validator.validar_estructura_basica(df, dataset_key)
                    if issues:
                        self.logger.warning(f"⚠️ Issues estructurales en {dataset_key}: {issues}")
                
                return df
            else:
                self.logger.error(f"❌ Error cargando {dataset_key} desde {ruta}")
                return None
                
        except Exception as e:
            self.logger.error(f"❌ Excepción cargando {dataset_key}: {e}")
            return None
    
    def cargar_datasets_alternativos(self) -> Dict[str, pd.DataFrame]:
        """
        Load alternative dataset paths if primary ones are not available.
        
        Returns:
            Dict[str, pd.DataFrame]: Dictionary with available datasets
        """
        datasets = {}
        
        # Alternative paths
        rutas_alternativas = {
            "muestra_source": "./data/sourceData/Muestra_Prueba_BI_Jr_FW.xlsx",
            "muestra_processed": "./data/processedData/muestra.csv",
            "estados_processed": "./data/processedData/estados.csv"
        }
        
        for key, path in rutas_alternativas.items():
            if os.path.exists(path):
                try:
                    if path.endswith('.xlsx'):
                        df = self.cache.get(key, pd.read_excel, path)
                    else:
                        df = self.cache.get(key, pd.read_csv, path)
                    
                    if df is not None:
                        datasets[key] = df
                        self.logger.info(f"📂 Cargado (alternativo) {key}: {len(df)} filas")
                        
                except Exception as e:
                    self.logger.error(f"❌ Error cargando {key}: {e}")
        
        return datasets
    
    def verificar_datasets_disponibles(self) -> Dict[str, bool]:
        """
        Check which datasets are available in the system.
        
        Returns:
            Dict[str, bool]: Availability status for each dataset
        """
        rutas_verificar = {
            "muestra_raw": "./data/processedData/muestra.csv",
            "estados_raw": "./data/processedData/estados.csv",
            "muestra_clean": "./data/cleanData/CLMUESTRA.csv", 
            "estados_clean": "./data/cleanData/CLESTADOS.csv",
            "muestra_source": "./data/sourceData/Muestra_Prueba_BI_Jr_FW.xlsx"
        }
        
        disponibilidad = {}
        for key, path in rutas_verificar.items():
            disponibilidad[key] = os.path.exists(path)
        
        datasets_disponibles = sum(disponibilidad.values())
        total_datasets = len(disponibilidad)
        
        self.logger.info(f"📊 Disponibilidad datasets: {datasets_disponibles}/{total_datasets}")
        
        return disponibilidad
    
    def obtener_estadisticas_cache(self) -> Dict[str, any]:
        """
        Get cache statistics if available.
        
        Returns:
            Dict[str, any]: Cache statistics
        """
        try:
            if hasattr(self.cache, 'get_stats'):
                return self.cache.get_stats()
            elif hasattr(self.cache, '_cache'):
                return {
                    "items_en_cache": len(self.cache._cache),
                    "claves": list(self.cache._cache.keys())
                }
            else:
                return {"cache_info": "No disponible"}
        except Exception:
            return {"cache_info": "Error obteniendo estadísticas"}
    
    def limpiar_cache(self, keys: Optional[list] = None):
        """
        Clear cache entries.
        
        Args:
            keys (Optional[list]): Specific keys to clear, or None to clear all
        """
        try:
            if hasattr(self.cache, 'clear'):
                if keys:
                    for key in keys:
                        self.cache.clear(key)
                        self.logger.info(f"🧹 Cache limpiado para: {key}")
                else:
                    self.cache.clear()
                    self.logger.info("🧹 Cache completamente limpiado")
            elif hasattr(self.cache, '_cache'):
                if keys:
                    for key in keys:
                        if key in self.cache._cache:
                            del self.cache._cache[key]
                            self.logger.info(f"🧹 Cache limpiado para: {key}")
                else:
                    self.cache._cache.clear()
                    self.logger.info("🧹 Cache completamente limpiado")
        except Exception as e:
            self.logger.error(f"❌ Error limpiando cache: {e}")


# Backward compatibility functions
def cargar_datasets_optimizado() -> Dict[str, pd.DataFrame]:
    """Backward compatibility function"""
    loader = OptimizedDataLoader()
    return loader.cargar_todos_los_datos()

def obtener_dataset_desde_cache(key: str) -> Optional[pd.DataFrame]:
    """Backward compatibility function"""
    loader = OptimizedDataLoader()
    return loader.get_dataset(key)
