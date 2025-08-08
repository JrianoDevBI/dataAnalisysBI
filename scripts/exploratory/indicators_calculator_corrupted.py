"""
Key Indicators Calculator Module

This module calculates key business indicators specific to the real estate project,
including price per m², reliability rate, outliers, lead analysis, and status tracking.

Author: Juan Camilo Riaño Molano
Date: 06/08/2025
"""

import pandas as pd
import numpy as np


class KeyIndicatorsCalculator:
    """
    Class responsible for calculating key business # Funciones de retrocompatibilidad
def calcular_indicadores_clave(df_muestra, df_estados=None, data_stage="procesados"):
    """Función de retrocompatibilidad"""
    calculator = KeyIndicatorsCalculator()
    return calculator.calcular_indicadores_clave(df_muestra, df_estados, data_stage)

def detectar_inconsistencias(df):
    """Función de retrocompatibilidad"""
    analyzer = InconsistencyAnalyzer()
    analyzer.detectar_inconsistencias_por_zona(df)
    return analyzer.inconsistencies

def calcular_inconsistencias_especificas(df_muestra, data_stage="procesados"):
    """Función de retrocompatibilidad"""  """
    
    def __init__(self):
        """Inicializar la calculadora de indicadores"""
        self.indicators = {}
        self.statistics = {}
    
    def calculate_price_per_m2(self, df_muestra):
        """
        Calculate average price per square meter
        
        Args:
            df_muestra: DataFrame with sample data
            
        Returns:
            float: Average price per m²
        """
        if "Area" in df_muestra.columns and "Precio_Solicitado" in df_muestra.columns:
            df_temp = df_muestra.copy()
            df_temp["Area"] = pd.to_numeric(df_temp["Area"], errors="coerce")
            df_temp["Precio_Solicitado"] = pd.to_numeric(df_temp["Precio_Solicitado"], errors="coerce")
            df_temp = df_temp.dropna(subset=["Area", "Precio_Solicitado"])
            df_temp = df_temp[df_temp["Area"] > 0]  # Evitar división por cero
            
            if not df_temp.empty:
                df_temp["precio_por_m2"] = df_temp["Precio_Solicitado"] / df_temp["Area"]
                precio_promedio_m2 = df_temp["precio_por_m2"].mean()
                
                self.indicators["precio_promedio_m2"] = precio_promedio_m2
                print(f"• Precio promedio por m²: ${precio_promedio_m2:,.0f} COP/m²")
                return precio_promedio_m2
            else:
                self.indicators["precio_promedio_m2"] = "N/A"
                print("• Precio promedio por m²: N/A (no hay datos válidos)")
                return "N/A"
        else:
            self.indicators["precio_promedio_m2"] = "N/A"
            print("• Precio promedio por m²: N/A (faltan columnas Area o Precio_Solicitado)")
            return "N/A"
    
    def calculate_reliability_rate(self, df_muestra):
        """
        Calculate reliability rate (records without imputations)
        
        Args:
            df_muestra: DataFrame with sample data
            
        Returns:
            float: Reliability rate percentage
        """
        columnas_importantes = ["Ciudad", "Zona", "Estrato", "Tipo_Inmueble", "Precio_Solicitado", "Area"]
        columnas_existentes = [col for col in columnas_importantes if col in df_muestra.columns]

        if columnas_existentes:
            registros_completos = df_muestra[columnas_existentes].dropna().shape[0]
            total_registros = len(df_muestra)
            tasa_confiabilidad = (registros_completos / total_registros) * 100
            
            self.indicators["tasa_confiabilidad"] = tasa_confiabilidad
            print(f"• Tasa de confiabilidad: {tasa_confiabilidad:.1f}%")
            return tasa_confiabilidad
        else:
            self.indicators["tasa_confiabilidad"] = "N/A"
            print("• Tasa de confiabilidad: N/A (no se encontraron columnas clave)")
            return "N/A"
    
    def calculate_outliers_statistics(self, df_muestra):
        """
        Calculate outlier statistics for price
        
        Args:
            df_muestra: DataFrame with sample data
            
        Returns:
            dict: Outlier statistics
        """
        if "Precio_Solicitado" in df_muestra.columns:
            df_temp = df_muestra.copy()
            df_temp["Precio_Solicitado"] = pd.to_numeric(df_temp["Precio_Solicitado"], errors="coerce")
            df_temp = df_temp.dropna(subset=["Precio_Solicitado"])

            Q1 = df_temp["Precio_Solicitado"].quantile(0.25)
            Q3 = df_temp["Precio_Solicitado"].quantile(0.75)
            IQR = Q3 - Q1
            limite_inferior = Q1 - 1.5 * IQR
            limite_superior = Q3 + 1.5 * IQR

            outliers = df_temp[
                (df_temp["Precio_Solicitado"] < limite_inferior) | 
                (df_temp["Precio_Solicitado"] > limite_superior)
            ]
            num_outliers = len(outliers)

            outlier_stats = {
                "num_outliers": num_outliers,
                "limite_inferior": limite_inferior,
                "limite_superior": limite_superior,
                "Q1": Q1,
                "Q3": Q3,
                "IQR": IQR,
                "percentage": (num_outliers / len(df_temp)) * 100
            }
            
            self.indicators.update(outlier_stats)
            print(f"• Outliers identificados: {num_outliers} (rangos: ${limite_inferior:,.0f} – ${limite_superior:,.0f} COP)")
            return outlier_stats
        else:
            outlier_stats = {
                "num_outliers": "N/A",
                "limite_inferior": "N/A",
                "limite_superior": "N/A"
            }
            self.indicators.update(outlier_stats)
            print("• Outliers identificados: N/A (falta columna Precio_Solicitado)")
            return outlier_stats
    
    def analyze_leads_in_review(self, df_estados):
        """
        Analyze leads in "Revisar Dirección" status
        
        Args:
            df_estados: DataFrame with status data
            
        Returns:
            dict: Lead analysis statistics
        """
        if df_estados is None:
            print("• Leads en 'Revisar Dirección': N/A (no se proporcionó archivo de estados)")
            return {"num_revisar_direccion": "N/A", "tiempo_promedio": "N/A"}
        
        if "Estado" not in df_estados.columns:
            print("• Leads en 'Revisar Dirección': N/A (falta columna Estado)")
            return {"num_revisar_direccion": "N/A", "tiempo_promedio": "N/A"}
        
        revisar_direccion = df_estados[
            df_estados["Estado"].str.contains("Revisar Dirección", case=False, na=False)
        ]
        num_revisar_direccion = (
            len(revisar_direccion["Inmueble_ID"].unique()) 
            if "Inmueble_ID" in df_estados.columns 
            else len(revisar_direccion)
        )

        # Calcular tiempo promedio en "Revisar Dirección"
        tiempo_promedio = 0
        if "Fecha_Actualizacion" in df_estados.columns and "Inmueble_ID" in df_estados.columns:
            df_temp = df_estados.copy()
            df_temp["Fecha_Actualizacion"] = pd.to_datetime(df_temp["Fecha_Actualizacion"], errors="coerce")
            df_temp = df_temp.sort_values(["Inmueble_ID", "Fecha_Actualizacion"])

            # Calcular tiempo en cada estado
            tiempos_revisar = []
            for inmueble_id in revisar_direccion["Inmueble_ID"].unique():
                estados_inmueble = df_temp[df_temp["Inmueble_ID"] == inmueble_id]
                for idx, row in estados_inmueble.iterrows():
                    if "Revisar Dirección" in str(row["Estado"]):
                        # Buscar siguiente estado
                        siguiente = estados_inmueble[estados_inmueble.index > idx]
                        if not siguiente.empty:
                            tiempo_diff = (
                                siguiente.iloc[0]["Fecha_Actualizacion"] - 
                                row["Fecha_Actualizacion"]
                            ).days
                            if tiempo_diff > 0:
                                tiempos_revisar.append(tiempo_diff)

            tiempo_promedio = sum(tiempos_revisar) / len(tiempos_revisar) if tiempos_revisar else 0
        
        lead_stats = {
            "num_revisar_direccion": num_revisar_direccion,
            "tiempo_promedio": tiempo_promedio
        }
        
        print(f"• Leads en 'Revisar Dirección': {num_revisar_direccion} (tiempo promedio: {tiempo_promedio:.1f} días)")
        return lead_stats
    
    def calculate_discard_percentage(self, df_estados):
        """
        Calculate percentage of discarded leads
        
        Args:
            df_estados: DataFrame with status data
            
        Returns:
            float: Discard percentage
        """
        if df_estados is None or "Estado" not in df_estados.columns:
            print("• Porcentaje descartados: N/A (falta información de estados)")
            return "N/A"
        
        descartados = df_estados[
            df_estados["Estado"].str.contains("Descartado|Rechazado", case=False, na=False)
        ]
        num_descartados = (
            len(descartados["Inmueble_ID"].unique()) 
            if "Inmueble_ID" in df_estados.columns 
            else len(descartados)
        )
        total_inmuebles = (
            len(df_estados["Inmueble_ID"].unique()) 
            if "Inmueble_ID" in df_estados.columns 
            else len(df_estados)
        )
        porcentaje_descartados = (num_descartados / total_inmuebles) * 100 if total_inmuebles > 0 else 0
        
        self.indicators["porcentaje_descartados"] = porcentaje_descartados
        print(f"• Porcentaje descartados: {porcentaje_descartados:.1f}%")
        return porcentaje_descartados
    
    def find_most_frequent_initial_status(self, df_estados):
        """
        Find most frequent initial status
        
        Args:
            df_estados: DataFrame with status data
            
        Returns:
            dict: Most frequent status statistics
        """
        if df_estados is None or "Estado" not in df_estados.columns:
            print("• Estado inicial más frecuente: N/A (falta información de estados)")
            return {"estado_mas_frecuente": "N/A", "porcentaje_estado": "N/A"}
        
        if "Inmueble_ID" in df_estados.columns:
            primeros_estados = df_estados.groupby("Inmueble_ID")["Estado"].first()
            estado_mas_frecuente = primeros_estados.mode()[0] if not primeros_estados.empty else "N/A"
            frecuencia_estado = (primeros_estados == estado_mas_frecuente).sum()
            porcentaje_estado = (frecuencia_estado / len(primeros_estados)) * 100 if len(primeros_estados) > 0 else 0
        else:
            estado_mas_frecuente = df_estados["Estado"].mode()[0] if not df_estados["Estado"].empty else "N/A"
            frecuencia_estado = (df_estados["Estado"] == estado_mas_frecuente).sum()
            porcentaje_estado = (frecuencia_estado / len(df_estados)) * 100 if len(df_estados) > 0 else 0
        
        status_stats = {
            "estado_mas_frecuente": estado_mas_frecuente,
            "porcentaje_estado": porcentaje_estado
        }
        
        self.indicators.update(status_stats)
        print(f"• Estado inicial más frecuente: {estado_mas_frecuente} ({porcentaje_estado:.1f}%)")
        return status_stats
    
    def calcular_indicadores_clave(self, df_muestra, df_estados=None, data_stage="procesados"):
        """
        Main method to calculate all key indicators
        
        Args:
            df_muestra: DataFrame with sample data
            df_estados: DataFrame with status data (optional)
            data_stage: Stage of data being analyzed ('procesados' or 'limpios')
            
        Returns:
            dict: All calculated indicators
        """
        print("\n" + "=" * 60)
        print("          INDICADORES CLAVE DEL PROYECTO")
        print("=" * 60)
        
        # Agregar aclaración de etapa de datos
        stage_emoji = "📊" if data_stage == "procesados" else "✨"
        stage_description = "DATOS ORIGINALES (antes de limpieza)" if data_stage == "procesados" else "DATOS LIMPIOS (después de procesamiento)"
        print(f"{stage_emoji} ANÁLISIS DE: {stage_description}")
        print("=" * 60)

        # 1. Price per m²
        self.calculate_price_per_m2(df_muestra)
        
        # 2. Reliability rate
        self.calculate_reliability_rate(df_muestra)
        
        # 3. Outliers statistics
        self.calculate_outliers_statistics(df_muestra)
        
        # 4. Status analysis (if provided)
        if df_estados is not None:
            self.analyze_leads_in_review(df_estados)
            self.calculate_discard_percentage(df_estados)
            self.find_most_frequent_initial_status(df_estados)
        else:
            print("• Leads en 'Revisar Dirección': N/A (no se proporcionó archivo de estados)")
            print("• Porcentaje descartados: N/A (no se proporcionó archivo de estados)")
            print("• Estado inicial más frecuente: N/A (no se proporcionó archivo de estados)")

        print("=" * 60)
        return self.indicators
    
    def get_indicators_summary(self):
        """Obtener resumen de todos los indicadores calculados"""
        return self.indicators


class InconsistencyAnalyzer:
    """
    Class responsible for analyzing data inconsistencies
    """
    
    def __init__(self):
        """Inicializar el analizador de inconsistencias"""
        self.inconsistencies = {}
    
    def detectar_inconsistencias_por_zona(self, df):
        """
        Detect outliers by zone using statistical analysis
        
        Args:
            df: DataFrame to analyze
        """
        print("\n--- DETECCIÓN DE OUTLIERS POR ZONA ---")
        if "Zona" in df.columns and "Precio_Solicitado" in df.columns:
            df = df.copy()
            df["Precio_Solicitado"] = pd.to_numeric(df["Precio_Solicitado"], errors="coerce")
            stats = df.groupby("Zona")["Precio_Solicitado"].agg(["mean", "std"])
            df = df.join(stats, on="Zona", rsuffix="_zona")
            outliers = df[
                (df["Precio_Solicitado"] > df["mean"] + 3 * df["std"]) | 
                (df["Precio_Solicitado"] < df["mean"] - 3 * df["std"])
            ]
            print(f"Registros con precio fuera de 3 desviaciones estándar por zona: {len(outliers)}")
            if not outliers.empty:
                outliers_fmt = outliers.copy()
                outliers_fmt["Precio_Solicitado"] = outliers_fmt["Precio_Solicitado"].apply(
                    lambda x: "$ {:,.0f}".format(x) if pd.notnull(x) else "-"
                )
                if "Id" in outliers_fmt.columns:
                    print(outliers_fmt[["Id", "Zona", "Precio_Solicitado"]])
                else:
                    print(outliers_fmt[["Zona", "Precio_Solicitado"]].head(10))
            else:
                print("No se encontraron outliers.")
    
    def calcular_inconsistencias_especificas(self, df, data_stage="procesados"):
        """
        Calculate specific inconsistencies by groupings
        
        Args:
            df: DataFrame to analyze
            data_stage: Stage of data being analyzed ('procesados' or 'limpios')
            
        Returns:
            dict: Inconsistency statistics
        """
        print("\n" + "=" * 60)
        print("     INCONSISTENCIAS ESPECÍFICAS POR AGRUPACIONES")
        print("=" * 60)
        
        # Add data stage clarification
        stage_emoji = "📊" if data_stage == "procesados" else "✨"
        stage_description = "DATOS ORIGINALES (esperadas)" if data_stage == "procesados" else "DATOS LIMPIOS (deben ser menores)"
        print(f"{stage_emoji} ANÁLISIS DE: {stage_description}")
        print("=" * 60)

        df_temp = df.copy()
        inconsistencias = {}

        # 1. Areas out of range (< 20 m² or > 300 m²)
        if "Area" in df_temp.columns:
            df_temp["Area"] = pd.to_numeric(df_temp["Area"], errors="coerce")
            areas_fuera_rango = df_temp[(df_temp["Area"] < 20) | (df_temp["Area"] > 300)]
            areas_fuera_rango = areas_fuera_rango.dropna(subset=["Area"])
            num_areas_fuera = len(areas_fuera_rango)
            inconsistencias["areas_fuera_rango"] = num_areas_fuera
            print(f"• Áreas fuera de rango (< 20 m² o > 300 m²): {num_areas_fuera}")

        # 2. Prices out of range (< 0 or > 1,000 M)
        if "Precio_Solicitado" in df_temp.columns:
            df_temp["Precio_Solicitado"] = pd.to_numeric(df_temp["Precio_Solicitado"], errors="coerce")
            precios_fuera_rango = df_temp[
                (df_temp["Precio_Solicitado"] < 0) | (df_temp["Precio_Solicitado"] > 1000000000)
            ]
            precios_fuera_rango = precios_fuera_rango.dropna(subset=["Precio_Solicitado"])
            num_precios_fuera = len(precios_fuera_rango)
            inconsistencias["precios_fuera_rango"] = num_precios_fuera
            print(f"• Precios fuera de rango (< 0 o > 1,000 M): {num_precios_fuera}")

        # 3. Non-standardized "Fuente" values
        if "Fuente" in df_temp.columns:
            valores_fuente = df_temp["Fuente"].value_counts()
            valores_no_estandarizados = len(valores_fuente[valores_fuente == 1])
            inconsistencias["fuentes_no_estandarizadas"] = valores_no_estandarizados
            print(f"• Valores de 'Fuente' no estandarizados (únicos): {valores_no_estandarizados}")

        # 4. Check for logical inconsistencies
        if "Tipo_Inmueble" in df_temp.columns and "Pisos" in df_temp.columns:
            casas_con_muchos_pisos = df_temp[
                (df_temp["Tipo_Inmueble"].str.contains("casa", case=False, na=False)) &
                (pd.to_numeric(df_temp["Pisos"], errors="coerce") > 3)
            ]
            num_casas_inconsistentes = len(casas_con_muchos_pisos)
            inconsistencias["casas_con_muchos_pisos"] = num_casas_inconsistentes
            print(f"• Casas con más de 3 pisos: {num_casas_inconsistentes}")

        self.inconsistencies = inconsistencias
        print("=" * 60)
        return inconsistencias


# Backward compatibility functions
def calcular_indicadores_clave(df_muestra, df_estados=None, data_stage="procesados"):
    """Backward compatibility function"""
    calculator = KeyIndicatorsCalculator()
    return calculator.calcular_indicadores_clave(df_muestra, df_estados, data_stage)

def detectar_inconsistencias(df):
    """Backward compatibility function"""
    analyzer = InconsistencyAnalyzer()
    analyzer.detectar_inconsistencias_por_zona(df)
    return analyzer.inconsistencies

def calcular_inconsistencias_especificas(df, data_stage="procesados"):
    """Backward compatibility function"""
    analyzer = InconsistencyAnalyzer()
    return analyzer.calcular_inconsistencias_especificas(df, data_stage)
