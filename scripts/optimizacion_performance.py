#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Módulo de Optimización de Performance
=====================================

Autor: Juan Camilo Riaño Molano
Fecha: 01/08/2025
Descripción: Sistema de optimización de performance para el pipeline de datos inmobiliarios.

Este módulo implementa un sistema completo de optimización que reduce significativamente
los tiempos de procesamiento mediante cache, paralelización, pools de conexión y 
monitoreo de métricas en tiempo real.
"""

import pandas as pd
import time
import threading
import logging
from contextlib import contextmanager
from functools import wraps
from typing import Dict, Any, Optional, Callable
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps
import psutil
import os
from contextlib import contextmanager

# Crear directorio de logs si no existe
os.makedirs("logs", exist_ok=True)

# Configurar logging sin emojis para compatibilidad Windows
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/pipeline_performance.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DataCache:
    """Cache singleton para almacenar DataFrames en memoria."""
    
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(DataCache, cls).__new__(cls)
                    cls._instance._data = {}
                    cls._instance._access_times = {}
        return cls._instance

    def get(self, key: str, loader_func: Callable = None, *args, **kwargs) -> Optional[pd.DataFrame]:
        """Obtiene un DataFrame del cache o lo carga usando la función provista."""
        with self._lock:
            if key in self._data:
                self._access_times[key] = time.time()
                logger.info(f"Cache HIT: {key} (memoria: {self.get_memory_usage():.1f}MB)")
                return self._data[key]

            if loader_func:
                try:
                    start_time = time.time()
                    data = loader_func(*args, **kwargs)
                    load_time = time.time() - start_time

                    self._data[key] = data
                    self._access_times[key] = time.time()

                    logger.info(f"Cache MISS: {key} cargado en {load_time:.2f}s (memoria: {self.get_memory_usage():.1f}MB)")
                    return data
                except Exception as e:
                    logger.error(f"Error cargando datos para cache {key}: {e}")
                    return None
        return None

    def put(self, key: str, data: pd.DataFrame) -> None:
        """Almacena un DataFrame en el cache."""
        with self._lock:
            self._data[key] = data
            self._access_times[key] = time.time()

    def clear(self) -> None:
        """Limpia completamente el cache."""
        with self._lock:
            self._data.clear()
            self._access_times.clear()

    def invalidate(self, key: str) -> None:
        """Invalida una entrada específica del cache."""
        with self._lock:
            if key in self._data:
                del self._data[key]
            if key in self._access_times:
                del self._access_times[key]

    def get_memory_usage(self) -> float:
        """Calcula el uso de memoria del cache en MB."""
        total_memory = 0
        for df in self._data.values():
            total_memory += df.memory_usage(deep=True).sum()
        return total_memory / (1024 * 1024)


class ConnectionPoolManager:
    """Administrador de pool de conexiones SQL."""
    
    _pools: Dict[str, Engine] = {}
    _lock = threading.Lock()

    @classmethod
    def get_engine(cls, database_url: str, **kwargs) -> Engine:
        """Obtiene o crea un engine con pool de conexiones."""
        with cls._lock:
            if database_url not in cls._pools:
                default_config = {
                    "poolclass": QueuePool,
                    "pool_size": 10,
                    "max_overflow": 20,
                    "pool_pre_ping": True,
                    "pool_recycle": 3600,
                }
                config = {**default_config, **kwargs}
                cls._pools[database_url] = create_engine(database_url, **config)
                logger.info(f"Pool de conexiones creado para: {database_url[:50]}...")
            return cls._pools[database_url]

    @classmethod
    @contextmanager
    def get_connection(cls, database_url: str):
        """Context manager para obtener una conexión del pool."""
        engine = cls.get_engine(database_url)
        conn = None
        try:
            conn = engine.connect()
            yield conn
        finally:
            if conn:
                conn.close()


class ParallelProcessor:
    """Procesador paralelo para ejecutar múltiples tareas de forma concurrente."""
    
    def __init__(self, max_workers: int = 4):
        self.max_workers = max_workers
        self.logger = logger

    def execute_parallel_cleaning(self, tasks: list) -> Dict[str, Any]:
        """Ejecuta múltiples tareas de limpieza en paralelo."""
        results = {}
        start_time = time.time()

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_task = {}
            for func, args, kwargs, task_name in tasks:
                future = executor.submit(func, *args, **kwargs)
                future_to_task[future] = task_name

            for future in as_completed(future_to_task):
                task_name = future_to_task[future]
                try:
                    result = future.result()
                    results[task_name] = result
                    self.logger.info(f"Tarea paralela completada: {task_name}")
                except Exception as e:
                    self.logger.error(f"Error en tarea paralela {task_name}: {e}")
                    results[task_name] = None

        total_time = time.time() - start_time
        self.logger.info(f"Procesamiento paralelo completado en {total_time:.2f}s")
        return results


class MetricsCollector:
    """Recolector de métricas de performance del sistema."""
    
    def __init__(self):
        self.metrics = {}
        self.timers = {}
        self.logger = logger
        self._monitoring = False

    def start_monitoring(self) -> None:
        """Inicia el monitoreo de métricas."""
        self._monitoring = True
        self.logger.info("Monitoreo de métricas iniciado")

    def stop_monitoring(self) -> None:
        """Detiene el monitoreo de métricas."""
        self._monitoring = False
        self.logger.info("Monitoreo de métricas detenido")

    def start_timer(self, operation: str) -> None:
        """Inicia un timer para una operación."""
        self.timers[operation] = time.time()

    def end_timer(self, operation: str) -> float:
        """Termina un timer y registra el tiempo transcurrido."""
        if operation in self.timers:
            elapsed = time.time() - self.timers[operation]
            self.metrics[operation] = elapsed
            self.logger.info(f"Timer {operation}: {elapsed:.2f}s")
            del self.timers[operation]
            return elapsed
        return 0.0

    @contextmanager
    def timer(self, operation: str):
        """Context manager para medir tiempo de operaciones."""
        self.start_timer(operation)
        try:
            yield
        finally:
            self.end_timer(operation)

    def get_metrics(self) -> Dict[str, float]:
        """Obtiene las métricas registradas."""
        return self.metrics.copy()

    def record_metric(self, name: str, value: Any) -> None:
        """Registra una métrica personalizada."""
        self.metrics[name] = value

    def get_all_metrics(self) -> Dict[str, Any]:
        """Obtiene todas las métricas recolectadas."""
        return self.metrics

    def get_performance_summary(self) -> Dict[str, Any]:
        """
        Obtiene un resumen de las métricas de performance.
        
        Returns:
            Dict[str, Any]: Resumen de métricas de performance
        """
        total_time = sum(v for k, v in self.metrics.items() if isinstance(v, (int, float)))
        
        return {
            'total_execution_time': total_time,
            'operations_count': len(self.metrics),
            'average_operation_time': total_time / len(self.metrics) if self.metrics else 0,
            'detailed_metrics': self.metrics.copy(),
            'performance_score': min(100, max(0, 100 - (total_time * 2)))  # Score basado en tiempo
        }


def measure_performance(operation_name: str = None):
    """Decorador para medir automáticamente el tiempo de ejecución de funciones."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            name = operation_name or func.__name__
            metrics_collector.start_timer(name)
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                metrics_collector.end_timer(name)
        return wrapper
    return decorator


def load_data_with_cache(file_path: str, cache_key: str = None) -> pd.DataFrame:
    """Carga datos con cache automático."""
    cache = DataCache()
    key = cache_key or Path(file_path).stem
    return cache.get(key, pd.read_csv, file_path)


@measure_performance("save_with_backup")
def save_data_with_backup(data: pd.DataFrame, output_path: str, backup_dir: str = "./dataBackup") -> None:
    """Guarda datos con backup automático."""
    import datetime
    
    # Crear backup con timestamp único para evitar conflictos
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = Path(output_path).stem
    extension = Path(output_path).suffix
    backup_filename = f"{filename}_backup_{timestamp}{extension}"
    backup_path = Path(backup_dir) / backup_filename
    
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    data.to_csv(backup_path, index=False, encoding="utf-8")
    logger.info(f"Backup creado: {backup_path}")

    # Guardar archivo principal
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    data.to_csv(output_path, index=False, encoding="utf-8")
    logger.info(f"Archivo guardado: {output_path}")


def execute_sql_with_pool(query: str, database_url: str, params: Dict = None) -> pd.DataFrame:
    """Ejecuta una query SQL usando el pool de conexiones."""
    try:
        with ConnectionPoolManager.get_connection(database_url) as conn:
            return pd.read_sql(query, conn, params=params)
    except Exception as e:
        logger.error(f"Error ejecutando SQL: {e}")
        return pd.DataFrame()


def validate_data_with_cache(data: pd.DataFrame, validation_key: str = None) -> Dict[str, Any]:
    """Valida datos con cache de resultados."""
    results = {
        "total_rows": len(data),
        "null_counts": data.isnull().sum().to_dict(),
        "duplicates": data.duplicated().sum(),
        "memory_usage_mb": data.memory_usage(deep=True).sum() / (1024 * 1024)
    }
    
    logger.info(f"Validación {validation_key}: {len(data)} filas, {results['duplicates']} duplicados")
    return results


# Instancias globales
cache = DataCache()
metrics = MetricsCollector()
processor = ParallelProcessor(max_workers=4)

# Alias para compatibilidad con pipeline_optimizado
data_cache = cache
pool_manager = ConnectionPoolManager()
metrics_collector = metrics

# Alias para retrocompatibilidad - CacheManager apunta a DataCache
CacheManager = DataCache
