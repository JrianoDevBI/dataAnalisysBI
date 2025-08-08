"""
Data Validator Module

This module provides centralized data validation to eliminate duplicate 
validation logic across the pipeline.

Author: Juan Camilo Riaño Molano
Date: 06/08/2025
"""

import logging
from typing import Any, Dict, List

import numpy as np
import pandas as pd

# Import performance measurement if available
try:
    from scripts.optimizacion_performance import measure_performance
except ImportError:
    # Fallback decorator if performance module not available
    def measure_performance(name):
        def decorator(func):
            return func
        return decorator

# Set up logger
logger = logging.getLogger(__name__)


class DataValidator:
    """
    Centralized validator to eliminate duplicate validations.
    
    Provides a common set of data quality validations that can be
    reused by all pipeline components.
    """

    def __init__(self):
        """Initialize the data validator"""
        self.logger = logger

    def validar_estructura_basica(self, df: pd.DataFrame, nombre_dataset: str) -> List[str]:
        """
        Validate basic structure of a DataFrame.
        
        Args:
            df (pd.DataFrame): DataFrame to validate
            nombre_dataset (str): Dataset name for logging
            
        Returns:
            List[str]: List of issues found
        """
        issues = []
        
        if df.empty:
            issues.append("Dataset vacío")
            return issues
            
        # Specific validations by dataset type
        columnas_esperadas = {
            "muestra": ["Id", "Precio_Solicitado", "Tipo_Inmueble", "Area", "Piso", "Garajes"],
            "estados": ["Inmueble_ID", "Estado", "Fecha_Actualizacion"]
        }
        
        # Detect dataset type
        tipo_detectado = None
        for tipo, cols in columnas_esperadas.items():
            if any(col in df.columns for col in cols):
                tipo_detectado = tipo
                break
                
        if tipo_detectado:
            cols_faltantes = [col for col in columnas_esperadas[tipo_detectado] if col not in df.columns]
            if cols_faltantes:
                issues.append(f"Columnas faltantes: {cols_faltantes}")
        
        return issues

    @measure_performance("validacion_estructura")
    def validar_calidad_datos(self, df: pd.DataFrame, nombre_dataset: str) -> Dict[str, Any]:
        """
        Execute complete data quality validation.
        
        Args:
            df (pd.DataFrame): DataFrame to validate
            nombre_dataset (str): Dataset name
            
        Returns:
            Dict[str, Any]: Quality metrics
        """
        # Basic metrics
        total_cells = df.size
        total_nulls = df.isnull().sum().sum()
        completitud = ((total_cells - total_nulls) / total_cells * 100) if total_cells > 0 else 0
        
        # Outlier detection (only for numerical columns)
        outliers_por_columna = {}
        for col in df.select_dtypes(include=[np.number]).columns:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            outliers = ((df[col] < lower_bound) | (df[col] > upper_bound)).sum()
            outliers_por_columna[col] = outliers
            
        total_outliers = sum(outliers_por_columna.values())
        
        # Duplicates
        duplicados = df.duplicated().sum()
        
        calidad = {
            "completitud": round(completitud, 1),
            "duplicados": duplicados,
            "outliers": total_outliers,
            "outliers_por_columna": outliers_por_columna,
            "filas_totales": len(df),
            "columnas_totales": len(df.columns)
        }
        
        self.logger.info(f"📊 Calidad de {nombre_dataset}: {completitud:.1f}% completitud, {duplicados} duplicados, {total_outliers} outliers")
        
        return calidad
    
    def validar_columnas_criticas(self, df: pd.DataFrame, columnas_criticas: List[str]) -> Dict[str, Any]:
        """
        Validate critical columns in the dataset.
        
        Args:
            df (pd.DataFrame): DataFrame to validate
            columnas_criticas (List[str]): List of critical column names
            
        Returns:
            Dict[str, Any]: Validation results
        """
        resultados = {}
        
        for col in columnas_criticas:
            if col not in df.columns:
                resultados[col] = {
                    "existe": False,
                    "nulos": 0,
                    "porcentaje_nulos": 0
                }
            else:
                nulos = df[col].isnull().sum()
                porcentaje_nulos = (nulos / len(df)) * 100 if len(df) > 0 else 0
                resultados[col] = {
                    "existe": True,
                    "nulos": nulos,
                    "porcentaje_nulos": round(porcentaje_nulos, 2)
                }
        
        return resultados
    
    def validar_tipos_datos(self, df: pd.DataFrame) -> Dict[str, str]:
        """
        Validate and report data types in the DataFrame.
        
        Args:
            df (pd.DataFrame): DataFrame to analyze
            
        Returns:
            Dict[str, str]: Column names and their data types
        """
        tipos_datos = {}
        
        for col in df.columns:
            dtype = str(df[col].dtype)
            
            # Classify data types
            if dtype.startswith('int') or dtype.startswith('float'):
                tipo_clasificado = 'numerico'
            elif dtype == 'object':
                # Check if it could be converted to numeric
                sample_non_null = df[col].dropna().iloc[:100] if not df[col].dropna().empty else []
                
                if len(sample_non_null) > 0:
                    try:
                        pd.to_numeric(sample_non_null)
                        tipo_clasificado = 'numerico_como_texto'
                    except (ValueError, TypeError):
                        # Check if it could be datetime
                        try:
                            pd.to_datetime(sample_non_null.iloc[:10])
                            tipo_clasificado = 'fecha_como_texto'
                        except (ValueError, TypeError, IndexError):
                            tipo_clasificado = 'texto'
                else:
                    tipo_clasificado = 'texto'
            elif dtype.startswith('datetime'):
                tipo_clasificado = 'fecha'
            else:
                tipo_clasificado = 'otro'
            
            tipos_datos[col] = tipo_clasificado
        
        return tipos_datos
    
    def generar_reporte_validacion(self, df: pd.DataFrame, nombre_dataset: str) -> Dict[str, Any]:
        """
        Generate comprehensive validation report.
        
        Args:
            df (pd.DataFrame): DataFrame to validate
            nombre_dataset (str): Dataset name
            
        Returns:
            Dict[str, Any]: Comprehensive validation report
        """
        reporte = {
            "nombre_dataset": nombre_dataset,
            "timestamp": pd.Timestamp.now().isoformat(),
            "estructura_basica": self.validar_estructura_basica(df, nombre_dataset),
            "calidad_datos": self.validar_calidad_datos(df, nombre_dataset),
            "tipos_datos": self.validar_tipos_datos(df)
        }
        
        # Add summary
        reporte["resumen"] = {
            "filas": len(df),
            "columnas": len(df.columns),
            "issues_estructura": len(reporte["estructura_basica"]),
            "completitud": reporte["calidad_datos"]["completitud"],
            "tiene_issues": len(reporte["estructura_basica"]) > 0 or reporte["calidad_datos"]["completitud"] < 90
        }
        
        return reporte


# Backward compatibility functions
def validar_estructura_dataset(df: pd.DataFrame, nombre: str) -> List[str]:
    """Backward compatibility function"""
    validator = DataValidator()
    return validator.validar_estructura_basica(df, nombre)

def validar_calidad_dataset(df: pd.DataFrame, nombre: str) -> Dict[str, Any]:
    """Backward compatibility function"""
    validator = DataValidator()
    return validator.validar_calidad_datos(df, nombre)
