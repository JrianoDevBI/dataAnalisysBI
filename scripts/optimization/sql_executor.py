"""
SQL Executor Module

This module provides optimized SQL operations using connection pooling
to eliminate overhead from repeated connection creation/destruction.

Author: Juan Camilo Riaño Molano
Date: 06/08/2025
"""

import logging
from typing import Any, Dict, Optional

import pandas as pd

# Import dependencies with fallbacks
try:
    from scripts.optimizacion_performance import pool_manager
    from sqlalchemy import create_engine
except ImportError:
    # Fallback implementations
    class FallbackPoolManager:
        def __init__(self):
            self._pools = {}
        
        def get_connection(self, database_url):
            # Simple context manager that returns a basic connection
            return FallbackConnection(database_url)
    
    class FallbackConnection:
        def __init__(self, database_url):
            self.database_url = database_url
            self.engine = None
            self.connection = None
            if database_url:
                try:
                    from sqlalchemy import create_engine
                    self.engine = create_engine(database_url)
                except ImportError:
                    pass
        
        def __enter__(self):
            if self.engine:
                try:
                    self.connection = self.engine.connect()
                    return self.connection
                except Exception:
                    return self.engine  # Fallback to engine if connection fails
            return None
        
        def __exit__(self, exc_type, exc_val, exc_tb):
            if self.connection:
                try:
                    self.connection.close()
                except Exception:
                    pass
    
    pool_manager = FallbackPoolManager()

# Set up logger
logger = logging.getLogger(__name__)


class OptimizedSQLExecutor:
    """
    SQL executor that uses connection pool to eliminate overhead.
    
    Reuses connections through a centrally managed pool,
    eliminating repetitive connection creation/destruction.
    """

    def __init__(self, database_url: str = None):
        """
        Initialize the SQL executor.
        
        Args:
            database_url (str): Database connection URL
        """
        self.database_url = database_url
        self.pool_manager = pool_manager
        self.logger = logger

    def cargar_datos_optimizado(self, datasets: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Load multiple datasets to SQL in an optimized way.

        Args:
            datasets (Dict[str, pd.DataFrame]): Dictionary of datasets to load

        Returns:
            Dict[str, Any]: Loading result
        """
        # Check if database connection pool is initialized
        if not hasattr(self.pool_manager, '_pools') or not self.pool_manager._pools:
            if not self.database_url:
                return {"success": False, "error": "URL de base de datos no configurada"}
            
            # Try to initialize basic connection
            try:
                self._verificar_conexion_basica()
            except Exception as e:
                return {"success": False, "error": f"No se pudo establecer conexión: {e}"}

        resultados = {}

        # Use connection pool (eliminating multiple connections)
        if self.database_url:
            try:
                with self.pool_manager.get_connection(self.database_url) as conn:
                    if conn is None:
                        return {"success": False, "error": "No se pudo obtener conexión del pool"}
                    
                    for nombre, df in datasets.items():
                        try:
                            # Normalize table name
                            tabla_nombre = nombre.replace("_clean", "").replace("_raw", "")
                            
                            # Load using pooled connection
                            df.to_sql(tabla_nombre, conn, if_exists="replace", index=False)
                            
                            resultados[nombre] = {
                                "success": True,
                                "filas_cargadas": len(df),
                                "tabla": tabla_nombre
                            }
                            
                            self.logger.info(f"✅ {nombre} cargado a tabla '{tabla_nombre}': {len(df)} filas")
                            
                        except Exception as e:
                            self.logger.error(f"❌ Error cargando {nombre}: {e}")
                            resultados[nombre] = {"success": False, "error": str(e)}
                            
            except Exception as e:
                self.logger.error(f"❌ Error de conexión SQL: {e}")
                return {"success": False, "error": f"Error de conexión: {e}"}
        else:
            return {"success": False, "error": "URL de base de datos no configurada"}
        
        total_exitosos = sum(1 for r in resultados.values() if r.get("success", False))
        
        return {
            "success": total_exitosos > 0,
            "tablas_cargadas": total_exitosos,
            "total_tablas": len(datasets),
            "resultados": resultados
        }

    def ejecutar_consulta_optimizada(self, query: str, nombre_consulta: str) -> Optional[pd.DataFrame]:
        """
        Execute a SQL query using the connection pool.

        Args:
            query (str): SQL query to execute
            nombre_consulta (str): Descriptive name for the query

        Returns:
            Optional[pd.DataFrame]: Query result
        """
        # Check BD configuration
        if not hasattr(self.pool_manager, '_pools') or not self.pool_manager._pools:
            if not self.database_url:
                self.logger.warning(f"⚠️ URL de BD no configurada para: {nombre_consulta}")
                return None
        
        if not self.database_url:
            self.logger.warning(f"⚠️ URL de BD no configurada para: {nombre_consulta}")
            return None

        try:
            with self.pool_manager.get_connection(self.database_url) as conn:
                if conn is None:
                    self.logger.error(f"❌ No se pudo obtener conexión para: {nombre_consulta}")
                    return None
                
                resultado = pd.read_sql(query, conn)
                self.logger.info(f"✅ Consulta '{nombre_consulta}' ejecutada: {len(resultado)} filas")
                return resultado
                
        except Exception as e:
            self.logger.error(f"❌ Error en consulta '{nombre_consulta}': {e}")
            return None

    def ejecutar_consultas_batch(self, consultas: Dict[str, str]) -> Dict[str, Optional[pd.DataFrame]]:
        """
        Execute multiple SQL queries in batch using the same connection.
        
        Args:
            consultas (Dict[str, str]): Dictionary of query names and SQL strings
            
        Returns:
            Dict[str, Optional[pd.DataFrame]]: Results for each query
        """
        resultados = {}
        
        if not self.database_url:
            self.logger.warning("⚠️ URL de BD no configurada para consultas batch")
            return {nombre: None for nombre in consultas.keys()}
        
        try:
            with self.pool_manager.get_connection(self.database_url) as conn:
                if conn is None:
                    self.logger.error("❌ No se pudo obtener conexión para consultas batch")
                    return {nombre: None for nombre in consultas.keys()}
                
                for nombre_consulta, query in consultas.items():
                    try:
                        resultado = pd.read_sql(query, conn)
                        resultados[nombre_consulta] = resultado
                        self.logger.info(f"✅ Consulta batch '{nombre_consulta}': {len(resultado)} filas")
                    except Exception as e:
                        self.logger.error(f"❌ Error en consulta batch '{nombre_consulta}': {e}")
                        resultados[nombre_consulta] = None
                        
        except Exception as e:
            self.logger.error(f"❌ Error de conexión en consultas batch: {e}")
            return {nombre: None for nombre in consultas.keys()}
        
        return resultados

    def verificar_conexion(self) -> Dict[str, Any]:
        """
        Verify database connection status.
        
        Returns:
            Dict[str, Any]: Connection status information
        """
        if not self.database_url:
            return {
                "conectado": False,
                "error": "URL de base de datos no configurada"
            }
        
        try:
            with self.pool_manager.get_connection(self.database_url) as conn:
                if conn is None:
                    return {
                        "conectado": False,
                        "error": "No se pudo obtener conexión del pool"
                    }
                
                # Try a simple query to test connection
                test_result = pd.read_sql("SELECT 1 as test", conn)
                
                return {
                    "conectado": True,
                    "database_url": self.database_url,
                    "test_query": len(test_result) > 0
                }
                
        except Exception as e:
            return {
                "conectado": False,
                "error": str(e),
                "database_url": self.database_url
            }

    def _verificar_conexion_basica(self):
        """Verify basic connection without pool"""
        try:
            from sqlalchemy import create_engine
            engine = create_engine(self.database_url)
            with engine.connect() as conn:
                conn.execute("SELECT 1")
        except Exception as e:
            raise Exception(f"Conexión básica falló: {e}")

    def obtener_esquema_tablas(self) -> Dict[str, Any]:
        """
        Get schema information for available tables.
        
        Returns:
            Dict[str, Any]: Schema information
        """
        if not self.database_url:
            return {"error": "URL de base de datos no configurada"}
        
        try:
            with self.pool_manager.get_connection(self.database_url) as conn:
                if conn is None:
                    return {"error": "No se pudo obtener conexión"}
                
                # Get table names (this query works for most SQL databases)
                try:
                    tablas = pd.read_sql(
                        "SELECT table_name FROM information_schema.tables WHERE table_schema='public'", 
                        conn
                    )
                except Exception:
                    # Fallback for SQLite
                    try:
                        tablas = pd.read_sql(
                            "SELECT name as table_name FROM sqlite_master WHERE type='table'", 
                            conn
                        )
                    except Exception:
                        return {"error": "No se pudo obtener información de esquema"}
                
                esquema = {
                    "tablas_disponibles": tablas["table_name"].tolist() if not tablas.empty else [],
                    "total_tablas": len(tablas)
                }
                
                return esquema
                
        except Exception as e:
            return {"error": str(e)}

    def limpiar_tabla(self, nombre_tabla: str) -> Dict[str, Any]:
        """
        Clean/drop a specific table.
        
        Args:
            nombre_tabla (str): Table name to clean
            
        Returns:
            Dict[str, Any]: Operation result
        """
        if not self.database_url:
            return {"success": False, "error": "URL de base de datos no configurada"}
        
        try:
            with self.pool_manager.get_connection(self.database_url) as conn:
                if conn is None:
                    return {"success": False, "error": "No se pudo obtener conexión"}
                
                # Execute DROP TABLE
                conn.execute(f"DROP TABLE IF EXISTS {nombre_tabla}")
                
                return {
                    "success": True,
                    "tabla_eliminada": nombre_tabla
                }
                
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "tabla": nombre_tabla
            }


# Backward compatibility functions
def cargar_datos_sql_optimizado(datasets: Dict[str, pd.DataFrame], database_url: str = None) -> Dict[str, Any]:
    """Backward compatibility function"""
    executor = OptimizedSQLExecutor(database_url)
    return executor.cargar_datos_optimizado(datasets)

def ejecutar_consulta_sql_optimizada(query: str, nombre: str, database_url: str = None) -> Optional[pd.DataFrame]:
    """Backward compatibility function"""
    executor = OptimizedSQLExecutor(database_url)
    return executor.ejecutar_consulta_optimizada(query, nombre)
