"""
Outlier Detection and Analysis Module

This module handles outlier detection and analysis for numerical variables
in the real estate dataset using various statistical methods.

Author: Juan Camilo Riaño Molano
Date: 06/08/2025
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns


class OutlierDetector:
    """
    Class responsible for outlier detection and analysis
    """
    
    def __init__(self):
        """Initialize the outlier detector"""
        self.outliers = {}
        self.statistics = {}
    
    def detect_outliers_iqr(self, df, column):
        """
        Detect outliers using IQR method
        
        Args:
            df: DataFrame with the data
            column: Column name to analyze
            
        Returns:
            dict: Dictionary with outlier statistics and data
        """
        if column not in df.columns:
            print(f"❌ Column '{column}' not found in DataFrame.")
            return None
        
        df_temp = df.copy()
        df_temp[column] = pd.to_numeric(df_temp[column], errors="coerce")
        df_temp = df_temp.dropna(subset=[column])
        
        if df_temp.empty:
            print(f"❌ No valid numerical data in column '{column}'.")
            return None
        
        Q1 = df_temp[column].quantile(0.25)
        Q3 = df_temp[column].quantile(0.75)
        IQR = Q3 - Q1
        
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        outliers = df_temp[(df_temp[column] < lower_bound) | (df_temp[column] > upper_bound)]
        
        outlier_stats = {
            "num_outliers": len(outliers),
            "percentage": (len(outliers) / len(df_temp)) * 100,
            "Q1": Q1,
            "Q3": Q3,
            "IQR": IQR,
            "lower_bound": lower_bound,
            "upper_bound": upper_bound,
            "outliers_data": outliers
        }
        
        print(f"📊 Outliers en '{column}': {len(outliers)} ({outlier_stats['percentage']:.2f}%)")
        print(f"   Rangos: Q1={Q1:.2f}, Q3={Q3:.2f}, IQR={IQR:.2f}")
        print(f"   Límites: [{lower_bound:.2f}, {upper_bound:.2f}]")
        
        return outlier_stats
    
    def detect_outliers_zscore(self, df, column, threshold=3):
        """
        Detect outliers using Z-score method
        
        Args:
            df: DataFrame with the data
            column: Column name to analyze
            threshold: Z-score threshold (default: 3)
            
        Returns:
            dict: Dictionary with outlier statistics and data
        """
        if column not in df.columns:
            print(f"❌ Column '{column}' not found in DataFrame.")
            return None
        
        df_temp = df.copy()
        df_temp[column] = pd.to_numeric(df_temp[column], errors="coerce")
        df_temp = df_temp.dropna(subset=[column])
        
        if df_temp.empty:
            print(f"❌ No valid numerical data in column '{column}'.")
            return None
        
        mean_val = df_temp[column].mean()
        std_val = df_temp[column].std()
        
        df_temp['z_score'] = np.abs((df_temp[column] - mean_val) / std_val)
        outliers = df_temp[df_temp['z_score'] > threshold]
        
        outlier_stats = {
            "num_outliers": len(outliers),
            "percentage": (len(outliers) / len(df_temp)) * 100,
            "mean": mean_val,
            "std": std_val,
            "threshold": threshold,
            "outliers_data": outliers
        }
        
        print(f"📊 Outliers Z-score en '{column}': {len(outliers)} ({outlier_stats['percentage']:.2f}%)")
        print(f"   Media: {mean_val:.2f}, Desv. Estándar: {std_val:.2f}")
        print(f"   Umbral Z-score: {threshold}")
        
        return outlier_stats
    
    def analyze_price_outliers(self, df):
        """
        Comprehensive analysis of price outliers
        
        Args:
            df: DataFrame with price data
            
        Returns:
            dict: Comprehensive outlier analysis
        """
        print("\n--- ANÁLISIS DETALLADO DE OUTLIERS DE PRECIOS ---")
        
        outlier_analysis = {}
        
        # IQR method for prices
        if "Precio_Solicitado" in df.columns:
            print("\n1. Método IQR para Precios:")
            iqr_results = self.detect_outliers_iqr(df, "Precio_Solicitado")
            outlier_analysis["iqr"] = iqr_results
        
        # Z-score method for prices
        if "Precio_Solicitado" in df.columns:
            print("\n2. Método Z-Score para Precios:")
            zscore_results = self.detect_outliers_zscore(df, "Precio_Solicitado")
            outlier_analysis["zscore"] = zscore_results
        
        # Price per m² analysis
        if "Area" in df.columns and "Precio_Solicitado" in df.columns:
            print("\n3. Análisis de Precio por m²:")
            df_temp = df.copy()
            df_temp["Area"] = pd.to_numeric(df_temp["Area"], errors="coerce")
            df_temp["Precio_Solicitado"] = pd.to_numeric(df_temp["Precio_Solicitado"], errors="coerce")
            df_temp = df_temp.dropna(subset=["Area", "Precio_Solicitado"])
            df_temp = df_temp[(df_temp["Area"] > 0) & (df_temp["Precio_Solicitado"] > 0)]
            
            if not df_temp.empty:
                df_temp["precio_por_m2"] = df_temp["Precio_Solicitado"] / df_temp["Area"]
                precio_m2_results = self.detect_outliers_iqr(df_temp, "precio_por_m2")
                outlier_analysis["precio_m2"] = precio_m2_results
        
        return outlier_analysis
    
    def detectar_inconsistencias_generales(self, df, data_stage="procesados"):
        """
        Detect general inconsistencies in the data
        
        Args:
            df: DataFrame to analyze
            data_stage: Stage of data being analyzed ('procesados' or 'limpios')
            
        Returns:
            dict: Dictionary with inconsistency statistics
        """
        print("\n" + "=" * 60)
        print("          DETECCIÓN DE INCONSISTENCIAS")
        print("=" * 60)
        
        # Add data stage clarification
        stage_emoji = "📊" if data_stage == "procesados" else "✨"
        stage_description = "DATOS ORIGINALES (esperadas)" if data_stage == "procesados" else "DATOS LIMPIOS (deben ser menores)"
        print(f"{stage_emoji} ANÁLISIS DE: {stage_description}")
        print("=" * 60)
        
        inconsistencias = {}
        
        # 1. Check for negative or zero prices
        if "Precio_Solicitado" in df.columns:
            df_temp = df.copy()
            df_temp["Precio_Solicitado"] = pd.to_numeric(df_temp["Precio_Solicitado"], errors="coerce")
            
            precios_negativos = df_temp[df_temp["Precio_Solicitado"] <= 0]
            inconsistencias["precios_negativos"] = len(precios_negativos)
            
            print(f"• Precios negativos o cero: {len(precios_negativos)}")
        
        # 2. Check for negative or zero areas
        if "Area" in df.columns:
            df_temp = df.copy()
            df_temp["Area"] = pd.to_numeric(df_temp["Area"], errors="coerce")
            
            areas_negativas = df_temp[df_temp["Area"] <= 0]
            inconsistencias["areas_negativas"] = len(areas_negativas)
            
            print(f"• Áreas negativas o cero: {len(areas_negativas)}")
        
        # 3. Check for unrealistic price per m2
        if "Area" in df.columns and "Precio_Solicitado" in df.columns:
            df_temp = df.copy()
            df_temp["Area"] = pd.to_numeric(df_temp["Area"], errors="coerce")
            df_temp["Precio_Solicitado"] = pd.to_numeric(df_temp["Precio_Solicitado"], errors="coerce")
            
            df_temp = df_temp.dropna(subset=["Area", "Precio_Solicitado"])
            df_temp = df_temp[(df_temp["Area"] > 0) & (df_temp["Precio_Solicitado"] > 0)]
            
            if not df_temp.empty:
                df_temp["precio_por_m2"] = df_temp["Precio_Solicitado"] / df_temp["Area"]
                
                # Define unrealistic thresholds (adjust based on local market)
                precio_min_m2 = 500000  # 500k COP/m²
                precio_max_m2 = 20000000  # 20M COP/m²
                
                precios_irreales = df_temp[
                    (df_temp["precio_por_m2"] < precio_min_m2) | 
                    (df_temp["precio_por_m2"] > precio_max_m2)
                ]
                
                inconsistencias["precios_m2_irreales"] = len(precios_irreales)
                print(f"• Precios por m² irreales (<${precio_min_m2:,.0f} o >${precio_max_m2:,.0f}): {len(precios_irreales)}")
        
        # 4. Check for missing key information
        columnas_importantes = ["Ciudad", "Zona", "Tipo_Inmueble"]
        for columna in columnas_importantes:
            if columna in df.columns:
                faltantes = df[df[columna].isna() | (df[columna] == "")].shape[0]
                inconsistencias[f"{columna.lower()}_faltantes"] = faltantes
                print(f"• {columna} faltantes: {faltantes}")
        
        # 5. Check for duplicates
        if "Id" in df.columns:
            duplicados = df[df.duplicated(subset=["Id"], keep=False)]
            inconsistencias["duplicados"] = len(duplicados)
            print(f"• Registros duplicados: {len(duplicados)}")
        
        print("=" * 60)
        self.inconsistencies = inconsistencias
        return inconsistencias
    
    def generate_outlier_report(self, df, output_file=None):
        """
        Generate comprehensive outlier report
        
        Args:
            df: DataFrame to analyze
            output_file: Optional file path to save report
            
        Returns:
            str: Report content
        """
        report_lines = []
        report_lines.append("REPORTE DETALLADO DE OUTLIERS")
        report_lines.append("=" * 50)
        report_lines.append(f"Dataset: {len(df)} registros")
        report_lines.append(f"Fecha: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append("")
        
        # Analyze numeric columns
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        
        for column in numeric_columns:
            report_lines.append(f"ANÁLISIS: {column}")
            report_lines.append("-" * 30)
            
            outlier_stats = self.detect_outliers_iqr(df, column)
            if outlier_stats:
                report_lines.append(f"Outliers: {outlier_stats['num_outliers']} ({outlier_stats['percentage']:.2f}%)")
                report_lines.append(f"Q1: {outlier_stats['Q1']:.2f}")
                report_lines.append(f"Q3: {outlier_stats['Q3']:.2f}")
                report_lines.append(f"IQR: {outlier_stats['IQR']:.2f}")
            
            report_lines.append("")
        
        report_content = "\n".join(report_lines)
        
        if output_file:
            try:
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(report_content)
                print(f"✅ Reporte guardado en: {output_file}")
            except Exception as e:
                print(f"❌ Error guardando reporte: {e}")
        
        return report_content


# Backward compatibility functions
def detectar_outliers(df, column="Precio_Solicitado"):
    """Backward compatibility function for outlier detection"""
    detector = OutlierDetector()
    return detector.detect_outliers_iqr(df, column)

def analizar_outliers_precio(df):
    """Backward compatibility function for price outlier analysis"""
    detector = OutlierDetector()
    return detector.analyze_price_outliers(df)

def detectar_inconsistencias(df, data_stage="procesados"):
    """Backward compatibility function for inconsistency detection"""
    detector = OutlierDetector()
    return detector.detectar_inconsistencias_generales(df, data_stage)