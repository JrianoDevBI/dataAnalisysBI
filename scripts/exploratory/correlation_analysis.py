"""
Correlation Analysis Module

This module handles correlation analysis between numerical and categorical
variables in the real estate dataset.

Author: Juan Camilo Riaño Molano
Date: 06/08/2025
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path


class CorrelationAnalyzer:
    """
    Class responsible for correlation analysis and visualization
    """
    
    def __init__(self):
        """Initialize the correlation analyzer"""
        self.correlation_matrix = None
        self.insights = []
    
    def calculate_correlation_matrix(self, df):
        """
        Calculate correlation matrix for numerical variables
        
        Args:
            df: DataFrame with the data
            
        Returns:
            pd.DataFrame: Correlation matrix
        """
        print("\n--- MATRIZ DE CORRELACIÓN ---")
        
        # Find numerical columns
        numerical_columns = []
        for col in ["Area", "Precio_Solicitado", "Estrato"]:
            if col in df.columns:
                numerical_columns.append(col)
        
        if len(numerical_columns) >= 2:
            df_corr = df[numerical_columns].copy()
            for col in numerical_columns:
                df_corr[col] = pd.to_numeric(df_corr[col], errors="coerce")
            
            self.correlation_matrix = df_corr.corr()
            print(f"Variables numéricas comparadas: {', '.join(numerical_columns)}")
            print(self.correlation_matrix)
            
            # Generate automatic insights
            self._generate_correlation_insights(numerical_columns)
            
            return self.correlation_matrix
        else:
            print("No hay suficientes variables numéricas para comparar.")
            return None
    
    def _generate_correlation_insights(self, columns):
        """Generate automatic insights from correlation matrix"""
        print("\n--- Conclusiones automáticas de la matriz de correlación ---")
        
        if "Area" in columns and "Precio_Solicitado" in columns:
            corr_area_precio = self.correlation_matrix.loc["Area", "Precio_Solicitado"]
            print(f"• Correlación Área vs Precio: {corr_area_precio:.2f}")
            
            if abs(corr_area_precio) > 0.7:
                insight = "Relación fuerte entre área y precio solicitado."
            elif abs(corr_area_precio) > 0.4:
                insight = "Relación moderada entre área y precio solicitado."
            else:
                insight = "Relación débil entre área y precio solicitado."
            
            print(f"  {insight}")
            self.insights.append(f"Área-Precio: {insight}")
        
        if "Estrato" in columns and "Precio_Solicitado" in columns:
            corr_estrato_precio = self.correlation_matrix.loc["Estrato", "Precio_Solicitado"]
            print(f"• Correlación Estrato vs Precio: {corr_estrato_precio:.2f}")
            
            if abs(corr_estrato_precio) > 0.7:
                insight = "Relación fuerte entre estrato y precio solicitado."
            elif abs(corr_estrato_precio) > 0.4:
                insight = "Relación moderada entre estrato y precio solicitado."
            else:
                insight = "Relación débil entre estrato y precio solicitado."
            
            print(f"  {insight}")
            self.insights.append(f"Estrato-Precio: {insight}")
    
    def show_correlation_heatmap(self):
        """Show correlation heatmap visualization"""
        if self.correlation_matrix is not None:
            plt.figure(figsize=(8, 6))
            sns.heatmap(self.correlation_matrix, annot=True, cmap="coolwarm", vmin=-1, vmax=1)
            plt.title("Matriz de correlación (Área, Precio Solicitado, Estrato)")
            plt.tight_layout()
            plt.show()
        else:
            print("❌ No hay matriz de correlación disponible.")
    
    def create_interactive_bubble_plot(self, df):
        """Create interactive bubble plot with Plotly"""
        if not all(col in df.columns for col in ["Area", "Precio_Solicitado", "Zona"]):
            print("❌ Columnas requeridas no disponibles para bubble plot.")
            return
        
        try:
            import plotly.express as px
            
            df = df.copy()
            df["Area"] = pd.to_numeric(df["Area"], errors="coerce")
            df["Precio_Solicitado"] = pd.to_numeric(df["Precio_Solicitado"], errors="coerce")
            
            # Group by Zone and calculate averages
            resumen = (
                df.groupby("Zona")
                .agg({"Area": "mean", "Precio_Solicitado": "mean", "Zona": "count"})
                .rename(columns={"Zona": "Cantidad"})
            )
            resumen = resumen.reset_index()
            
            # Create bubble plot
            fig = px.scatter(
                resumen,
                x="Zona",
                y="Precio_Solicitado",
                size="Cantidad",
                color="Area",
                hover_name="Zona",
                hover_data={"Cantidad": True, "Area": True, "Precio_Solicitado": True},
                size_max=60,
                height=800,
            )
            
            fig.update_traces(marker=dict(line=dict(width=1, color="DarkSlateGrey")))
            fig.update_layout(
                title=(
                    "Bubble plot interactivo: Precio Solicitado promedio por Zona "
                    "(tamaño=frecuencia, color=Área promedio)"
                ),
                xaxis_title="Zona",
                yaxis_title="Precio Solicitado promedio",
                xaxis_tickangle=45,
                showlegend=True,
                margin=dict(l=40, r=40, t=80, b=200),
            )
            fig.update_xaxes(tickfont=dict(size=10))
            fig.show()
            
            print(
                "\n--- Bubble plot interactivo generado: "
                "Precio Solicitado promedio por Zona (tamaño=frecuencia, color=Área promedio) ---"
            )
            
        except ImportError:
            print("❌ Plotly no está instalado. Ejecuta 'pip install plotly' para gráficos interactivos.")
    
    def create_violin_plot(self, df):
        """Create interactive violin plot by zone"""
        if not all(col in df.columns for col in ["Zona", "Precio_Solicitado"]):
            print("❌ Columnas requeridas no disponibles para violin plot.")
            return
        
        try:
            import plotly.express as px
            
            df = df.copy()
            df["Precio_Solicitado"] = pd.to_numeric(df["Precio_Solicitado"], errors="coerce")
            
            # Order zones by average price
            orden_zonas = df.groupby("Zona")["Precio_Solicitado"].mean().sort_values().index
            
            fig = px.violin(
                df,
                x="Zona",
                y="Precio_Solicitado",
                category_orders={"Zona": list(orden_zonas)},
                box=True,
                points="all",
                hover_data=["Zona", "Precio_Solicitado"],
                height=700,
            )
            
            fig.update_layout(
                title=("Distribución interactiva (Violin Plot) de Precio Solicitado por Zona (ordenado)"),
                xaxis_title="Zona",
                yaxis_title="Precio Solicitado",
                xaxis_tickangle=45,
                margin=dict(l=40, r=40, t=80, b=200),
            )
            fig.update_xaxes(tickfont=dict(size=10))
            fig.show()
            
            # Generate automatic insights
            zona_mas_cara = df.groupby("Zona")["Precio_Solicitado"].mean().idxmax()
            zona_mas_barata = df.groupby("Zona")["Precio_Solicitado"].mean().idxmin()
            
            print("\n--- Conclusión automática del violin plot Precio Solicitado por Zona ---")
            print(f"La zona con mayor precio promedio es '{zona_mas_cara}'.")
            print(f"La zona con menor precio promedio es '{zona_mas_barata}'.")
            
            self.insights.append(f"Zona más cara: {zona_mas_cara}")
            self.insights.append(f"Zona más barata: {zona_mas_barata}")
            
        except ImportError:
            print("❌ Plotly no está instalado. Ejecuta 'pip install plotly' para gráficos interactivos.")
    
    def analyze_price_by_property_type(self, df):
        """Analyze average price by property type"""
        if not all(col in df.columns for col in ["Tipo_Inmueble", "Precio_Solicitado"]):
            print("❌ Columnas requeridas no disponibles.")
            return
        
        df = df.copy()
        df["Precio_Solicitado"] = pd.to_numeric(df["Precio_Solicitado"], errors="coerce")
        
        # Normalize property type names
        df["Tipo_Inmueble_norm"] = (
            df["Tipo_Inmueble"]
            .astype(str)
            .str.lower()
            .str.normalize("NFKD")
            .str.encode("ascii", errors="ignore")
            .str.decode("utf-8")
            .str.strip()
        )
        
        resumen = df.groupby("Tipo_Inmueble_norm")["Precio_Solicitado"].mean().sort_values(ascending=False)
        conteo = df["Tipo_Inmueble_norm"].value_counts().reindex(resumen.index)
        
        # Format as COP currency
        resumen_moneda = resumen.apply(lambda x: "$ {:,.0f}".format(x) if pd.notnull(x) else "-")
        
        print("\n--- Precio promedio por Tipo de Inmueble (agrupado, normalizado y en moneda local) ---")
        print(resumen_moneda)
        
        # Create bar chart for main property types
        tipos_principales = ["casa", "apartamento"]
        resumen_principales = resumen[resumen.index.isin(tipos_principales)]
        conteo_principales = conteo[resumen_principales.index]
        
        # Mostrar información de tipos desconocidos en consola
        tipos_desconocidos = resumen[~resumen.index.isin(tipos_principales)]
        if not tipos_desconocidos.empty:
            print("\n--- Información de Tipos Desconocidos ---")
            for tipo in tipos_desconocidos.index:
                precio_avg = tipos_desconocidos[tipo]
                cantidad = conteo[tipo]
                print(f"• {tipo.title()}: ${precio_avg:,.0f} COP promedio ({cantidad:,} registros)")
        
        if not resumen_principales.empty:
            plt.figure(figsize=(10, 6))
            plt.bar(resumen_principales.index, resumen_principales.values, 
                    color="skyblue", edgecolor="black")
            plt.title("Precio promedio por Tipo de Inmueble (COP)\n(Casas y Apartamentos)")
            plt.xlabel("Tipo de Inmueble")
            plt.ylabel("Precio promedio (COP)")
            plt.xticks(rotation=30, ha="right")
            
            for i, (v, c) in enumerate(zip(resumen_principales.values, conteo_principales.values)):
                plt.text(i, v, f"$ {v:,.0f}\n({c} registros)", 
                        ha="center", va="bottom", fontsize=9, fontweight="bold")
            
            plt.tight_layout()
            plt.show()
    
    def analizar_relaciones(self, df):
        """
        Main method to analyze relationships with visualizations
        """
        # Calculate correlation matrix
        self.calculate_correlation_matrix(df)
        
        # Show correlation heatmap
        if self.correlation_matrix is not None:
            self.show_correlation_heatmap()
        
        # Create interactive visualizations
        self.create_interactive_bubble_plot(df)
        self.create_violin_plot(df)
        
        # Analyze price by property type
        self.analyze_price_by_property_type(df)
    
    def analizar_relaciones_sin_graficos(self, df):
        """
        Analyze relationships without showing graphics - only calculations and text conclusions
        """
        return self.calculate_correlation_matrix(df)
    
    def get_insights(self):
        """Get generated insights"""
        return self.insights


# Backward compatibility functions
def analizar_correlaciones(df):
    """Backward compatibility function"""
    analyzer = CorrelationAnalyzer()
    return analyzer.calculate_correlation_matrix(df)

def mostrar_matriz_correlacion(df):
    """Backward compatibility function"""
    analyzer = CorrelationAnalyzer()
    analyzer.calculate_correlation_matrix(df)
    analyzer.show_correlation_heatmap()
    return analyzer.correlation_matrix

def analizar_relaciones(df):
    """Backward compatibility function"""
    analyzer = CorrelationAnalyzer()
    analyzer.analizar_relaciones(df)
    return analyzer.get_insights()

def analizar_relaciones_sin_graficos(df):
    """Backward compatibility function"""
    analyzer = CorrelationAnalyzer()
    return analyzer.analizar_relaciones_sin_graficos(df)
