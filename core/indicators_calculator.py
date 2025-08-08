"""
Business Indicators Calculator Module

This module contains all the business key indicators calculation logic
for the executive report.

Author: Juan Camilo Riaño Molano
Date: 06/08/2025
"""

import pandas as pd
import numpy as np
from pathlib import Path


class IndicatorsCalculator:
    """
    Class responsible for calculating all business key indicators
    """
    
    def __init__(self):
        """Initialize the calculator with data paths"""
        self.muestra_path = "data/cleanData/CLMUESTRA.csv"
        self.estados_path = "data/cleanData/CLESTADOS.csv"
        
    def load_data(self):
        """Load clean data for analysis"""
        try:
            self.df_muestra = pd.read_csv(self.muestra_path)
            self.df_estados = pd.read_csv(self.estados_path)
            print(f"✓ Datos cargados: {len(self.df_muestra)} inmuebles, {len(self.df_estados)} estados")
            return True
        except Exception as e:
            print(f"❌ Error cargando datos: {e}")
            return False
    
    def calculate_price_per_m2(self):
        """Calculate average price per square meter"""
        # Convert to numeric
        self.df_muestra["Precio_Solicitado_num"] = pd.to_numeric(
            self.df_muestra["Precio_Solicitado"], errors="coerce"
        )
        self.df_muestra["Area_num"] = pd.to_numeric(
            self.df_muestra["Area"], errors="coerce"
        )
        
        # Filter valid data
        df_valido = self.df_muestra[
            (self.df_muestra["Precio_Solicitado_num"].notna()) & 
            (self.df_muestra["Area_num"].notna()) & 
            (self.df_muestra["Area_num"] > 0)
        ].copy()
        
        df_valido["precio_por_m2"] = df_valido["Precio_Solicitado_num"] / df_valido["Area_num"]
        price_avg = df_valido["precio_por_m2"].mean()
        
        return {
            "precio_promedio_m2": price_avg,
            "df_valido": df_valido
        }
    
    def calculate_reliability_rate(self):
        """Calculate data reliability rate (completeness percentage)"""
        total_records = len(self.df_muestra)
        critical_fields = ["Precio_Solicitado", "Area", "Ciudad", "Zona", "Tipo_Inmueble"]
        complete_records = 0
        
        for _, row in self.df_muestra.iterrows():
            complete = True
            for field in critical_fields:
                if pd.isna(row[field]) or str(row[field]).lower() in ["desconocido", "nan", ""]:
                    complete = False
                    break
            if complete:
                complete_records += 1
        
        # Almacenar para uso posterior
        self.complete_records_count = complete_records
        self.total_records_count = total_records
        
        reliability_rate = (complete_records / total_records) * 100
        return reliability_rate
    
    def detect_outliers(self, df_valido):
        """Detect outliers using IQR method"""
        Q1 = df_valido["precio_por_m2"].quantile(0.25)
        Q3 = df_valido["precio_por_m2"].quantile(0.75)
        IQR = Q3 - Q1
        lower_limit = Q1 - 1.5 * IQR
        upper_limit = Q3 + 1.5 * IQR
        
        outliers = df_valido[
            (df_valido["precio_por_m2"] < lower_limit) | 
            (df_valido["precio_por_m2"] > upper_limit)
        ]
        
        return {
            "num_outliers": len(outliers),
            "total_registros_analizados": len(df_valido),
            "limite_inferior": lower_limit,
            "limite_superior": upper_limit
        }
    
    def analyze_review_address_states(self):
        """Analyze leads in 'Review Address' state"""
        self.df_estados["Fecha_Actualizacion"] = pd.to_datetime(
            self.df_estados["Fecha_Actualizacion"], errors="coerce"
        )
        
        review_address = self.df_estados[
            self.df_estados["Estado"].str.contains("Revisar", case=False, na=False) |
            self.df_estados["Estado"].str.contains("Dirección", case=False, na=False)
        ]
        
        num_review_address = len(review_address["Inmueble_ID"].unique())
        
        # Calculate average time in this state
        if len(review_address) > 0:
            review_times = []
            for property_id in review_address["Inmueble_ID"].unique():
                property_states = self.df_estados[
                    self.df_estados["Inmueble_ID"] == property_id
                ].sort_values("Fecha_Actualizacion")
                
                for i, row in property_states.iterrows():
                    if "Revisar" in str(row["Estado"]) or "Dirección" in str(row["Estado"]):
                        next_states = property_states[
                            property_states["Fecha_Actualizacion"] > row["Fecha_Actualizacion"]
                        ]
                        if len(next_states) > 0:
                            time_days = (
                                next_states.iloc[0]["Fecha_Actualizacion"] - 
                                row["Fecha_Actualizacion"]
                            ).days
                            if time_days > 0 and time_days < 365:
                                review_times.append(time_days)
            
            avg_review_time = np.mean(review_times) if review_times else 0
        else:
            avg_review_time = 0
        
        return {
            "num_revisar_direccion": num_review_address,
            "tiempo_promedio_revisar": avg_review_time
        }
    
    def calculate_discard_percentage(self):
        """Calculate percentage of discarded properties"""
        discard_states = self.df_estados[
            self.df_estados["Estado"].str.contains("Descart|Rechaz|Cancel", case=False, na=False)
        ]
        discarded_properties = len(discard_states["Inmueble_ID"].unique())
        total_properties_with_states = len(self.df_estados["Inmueble_ID"].unique())
        discard_percentage = (discarded_properties / total_properties_with_states) * 100
        
        # Almacenar también el número de descartados para uso posterior
        self.discarded_count = discarded_properties
        self.total_properties_count = total_properties_with_states
        
        return discard_percentage
    
    def find_most_frequent_initial_state(self):
        """Find the most frequent initial state"""
        first_states = self.df_estados.groupby("Inmueble_ID")["Estado"].first()
        most_frequent_initial_state = first_states.value_counts().index[0]
        frequency = first_states.value_counts().iloc[0]
        percentage = (frequency / len(first_states)) * 100
        
        return {
            "estado_inicial_mas_frecuente": most_frequent_initial_state,
            "porcentaje_estado_inicial": percentage
        }
    
    def calcular_indicadores_clave(self):
        """
        Calculate all key business indicators for executive report.

        Returns:
            dict: Dictionary with all calculated indicators
        """
        print("\n" + "=" * 70)
        print("📊 CALCULANDO INDICADORES CLAVE DE NEGOCIO")
        print("=" * 70)
        print("✨ ANÁLISIS DE: DATOS LIMPIOS (después de procesamiento completo)")
        print("💡 Comparar con análisis PRE-limpieza para ver mejoras")
        print("=" * 70)
        
        if not self.load_data():
            return {}
        
        indicators = {}
        
        # 1. Average price per m²
        price_data = self.calculate_price_per_m2()
        indicators.update(price_data)
        print(f"1. 💰 Precio promedio por m²: ${price_data['precio_promedio_m2']:,.0f} COP/m²")
        
        # 2. Reliability rate
        reliability_rate = self.calculate_reliability_rate()
        indicators["tasa_confiabilidad"] = reliability_rate
        print(f"2. 🎯 Tasa de confiabilidad: {reliability_rate:.1f}% ({self.complete_records_count:,} registros completos de {self.total_records_count:,})")
        
        # 3. Outliers detection
        outliers_data = self.detect_outliers(price_data["df_valido"])
        indicators.update(outliers_data)
        print(f"3. 🔍 Outliers identificados: {outliers_data['num_outliers']} de {outliers_data['total_registros_analizados']:,} registros "
              f"(rangos: ${outliers_data['limite_inferior']:,.0f} – "
              f"${outliers_data['limite_superior']:,.0f} COP/m²)")
        
        # 4. Review address analysis
        review_data = self.analyze_review_address_states()
        indicators.update(review_data)
        print(f"4. 📋 Leads en 'Revisar Dirección': {review_data['num_revisar_direccion']} "
              f"(tiempo promedio: {review_data['tiempo_promedio_revisar']:.1f} días)")
        
        # 5. Discard percentage
        discard_percentage = self.calculate_discard_percentage()
        indicators["porcentaje_descartados"] = discard_percentage
        print(f"5. ❌ Porcentaje descartados: {discard_percentage:.1f}% ({self.discarded_count:,} registros de {self.total_properties_count:,})")
        
        # 6. Most frequent initial state
        initial_state_data = self.find_most_frequent_initial_state()
        indicators.update(initial_state_data)
        print(f"6. 🚀 Estado inicial más frecuente: {initial_state_data['estado_inicial_mas_frecuente']} "
              f"({initial_state_data['porcentaje_estado_inicial']:.1f}%)")
        
        # Show final summary
        self._show_summary(indicators)
        
        return indicators
    
    def _show_summary(self, indicators):
        """Show final summary of indicators"""
        print("\n" + "=" * 50)
        print("📈 RESUMEN DE INDICADORES CLAVE")
        print("=" * 50)
        print(f"💰 Precio promedio por m²: ${indicators['precio_promedio_m2']:,.0f} COP/m²")
        print(f"🎯 Tasa de confiabilidad: {indicators['tasa_confiabilidad']:.1f}% ({getattr(self, 'complete_records_count', 0):,} registros completos)")
        print(f"🔍 Outliers identificados: {indicators['num_outliers']} de {indicators.get('total_registros_analizados', 0):,} registros")
        print(f"📋 Leads en 'Revisar Dirección': {indicators['num_revisar_direccion']} "
              f"({indicators['tiempo_promedio_revisar']:.1f} días)")
        print(f"❌ Porcentaje descartados: {indicators['porcentaje_descartados']:.1f}% ({getattr(self, 'discarded_count', 0):,} registros)")
        print(f"🚀 Estado inicial más frecuente: {indicators['estado_inicial_mas_frecuente']} "
              f"({indicators['porcentaje_estado_inicial']:.1f}%)")


# Function for backward compatibility
def calcular_indicadores_clave():
    """Backward compatibility function"""
    calculator = IndicatorsCalculator()
    return calculator.calcular_indicadores_clave()
