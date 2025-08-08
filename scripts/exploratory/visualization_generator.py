"""
Visualization Generator Module

This module handles the generation of various visualizations and charts
for the real estate data analysis.

Author: Juan Camilo Riaño Molano
Date: 06/08/2025
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path


class VisualizationGenerator:
    """
    Class responsible for generating various visualizations
    """
    
    def __init__(self):
        """Initialize the visualization generator"""
        self.figures_path = Path("reports/figures")
        self.figures_path.mkdir(parents=True, exist_ok=True)
    
    def create_price_distribution_histogram(self, df):
        """
        Create histogram for price distribution
        
        Args:
            df: DataFrame with the data
        """
        if "Precio_Solicitado" not in df.columns:
            print("❌ Column 'Precio_Solicitado' not found.")
            return
        
        df_temp = df.copy()
        df_temp["Precio_Solicitado"] = pd.to_numeric(df_temp["Precio_Solicitado"], errors="coerce")
        df_temp = df_temp.dropna(subset=["Precio_Solicitado"])
        
        if df_temp.empty:
            print("❌ No valid price data available.")
            return
        
        plt.figure(figsize=(12, 6))
        
        # Main histogram
        plt.subplot(1, 2, 1)
        plt.hist(df_temp["Precio_Solicitado"], bins=50, alpha=0.7, color='skyblue', edgecolor='black')
        plt.title('Distribución de Precios Solicitados')
        plt.xlabel('Precio Solicitado (COP)')
        plt.ylabel('Frecuencia')
        plt.ticklabel_format(style='plain', axis='x')
        
        # Log scale histogram
        plt.subplot(1, 2, 2)
        plt.hist(df_temp["Precio_Solicitado"], bins=50, alpha=0.7, color='lightcoral', edgecolor='black')
        plt.title('Distribución de Precios (Escala Log)')
        plt.xlabel('Precio Solicitado (COP)')
        plt.ylabel('Frecuencia')
        plt.yscale('log')
        plt.ticklabel_format(style='plain', axis='x')
        
        plt.tight_layout()
        
        # Save figure
        output_path = self.figures_path / "price_distribution.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.show()
        
        print(f"📊 Histograma de distribución de precios guardado en: {output_path}")
    
    def create_area_vs_price_scatter(self, df):
        """
        Create scatter plot for area vs price relationship
        
        Args:
            df: DataFrame with the data
        """
        if not all(col in df.columns for col in ["Area", "Precio_Solicitado"]):
            print("❌ Required columns not found for scatter plot.")
            return
        
        df_temp = df.copy()
        df_temp["Area"] = pd.to_numeric(df_temp["Area"], errors="coerce")
        df_temp["Precio_Solicitado"] = pd.to_numeric(df_temp["Precio_Solicitado"], errors="coerce")
        df_temp = df_temp.dropna(subset=["Area", "Precio_Solicitado"])
        
        if df_temp.empty:
            print("❌ No valid data for scatter plot.")
            return
        
        plt.figure(figsize=(10, 6))
        
        # Create scatter plot with color by zone if available
        if "Zona" in df_temp.columns:
            zones = df_temp["Zona"].unique()
            colors = plt.cm.Set3(range(len(zones)))
            
            for i, zone in enumerate(zones):
                zone_data = df_temp[df_temp["Zona"] == zone]
                plt.scatter(zone_data["Area"], zone_data["Precio_Solicitado"], 
                           alpha=0.6, c=[colors[i]], label=zone, s=30)
            
            plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        else:
            plt.scatter(df_temp["Area"], df_temp["Precio_Solicitado"], alpha=0.6, s=30)
        
        plt.title('Relación Área vs Precio Solicitado')
        plt.xlabel('Área (m²)')
        plt.ylabel('Precio Solicitado (COP)')
        plt.grid(True, alpha=0.3)
        
        # Add trend line
        z = np.polyfit(df_temp["Area"], df_temp["Precio_Solicitado"], 1)
        p = np.poly1d(z)
        plt.plot(df_temp["Area"], p(df_temp["Area"]), "r--", alpha=0.8, linewidth=2)
        
        plt.tight_layout()
        
        # Save figure
        output_path = self.figures_path / "area_vs_price_scatter.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.show()
        
        print(f"📊 Gráfico de dispersión Área vs Precio guardado en: {output_path}")
    
    def create_zone_price_boxplot(self, df):
        """
        Create box plot for price distribution by zone
        
        Args:
            df: DataFrame with the data
        """
        if not all(col in df.columns for col in ["Zona", "Precio_Solicitado"]):
            print("❌ Required columns not found for box plot.")
            return
        
        df_temp = df.copy()
        df_temp["Precio_Solicitado"] = pd.to_numeric(df_temp["Precio_Solicitado"], errors="coave")
        df_temp = df_temp.dropna(subset=["Zona", "Precio_Solicitado"])
        
        if df_temp.empty:
            print("❌ No valid data for box plot.")
            return
        
        plt.figure(figsize=(14, 8))
        
        # Order zones by median price
        zone_order = df_temp.groupby("Zona")["Precio_Solicitado"].median().sort_values(ascending=False).index
        
        sns.boxplot(data=df_temp, x="Zona", y="Precio_Solicitado", order=zone_order)
        plt.title('Distribución de Precios por Zona')
        plt.xlabel('Zona')
        plt.ylabel('Precio Solicitado (COP)')
        plt.xticks(rotation=45, ha='right')
        plt.grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        
        # Save figure
        output_path = self.figures_path / "zone_price_boxplot.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.show()
        
        print(f"📊 Box plot de precios por zona guardado en: {output_path}")
    
    def create_property_type_analysis(self, df):
        """
        Create analysis visualization for property types
        
        Args:
            df: DataFrame with the data
        """
        if not all(col in df.columns for col in ["Tipo_Inmueble", "Precio_Solicitado"]):
            print("❌ Required columns not found for property type analysis.")
            return
        
        df_temp = df.copy()
        df_temp["Precio_Solicitado"] = pd.to_numeric(df_temp["Precio_Solicitado"], errors="coerce")
        
        # Normalize property type names
        df_temp["Tipo_Inmueble_norm"] = (
            df_temp["Tipo_Inmueble"]
            .astype(str)
            .str.lower()
            .str.normalize("NFKD")
            .str.encode("ascii", errors="ignore")
            .str.decode("utf-8")
            .str.strip()
        )
        
        # Create subplots
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Análisis por Tipo de Inmueble', fontsize=16)
        
        # 1. Count by property type
        type_counts = df_temp["Tipo_Inmueble_norm"].value_counts().head(10)
        axes[0, 0].bar(range(len(type_counts)), type_counts.values, color='skyblue')
        axes[0, 0].set_title('Cantidad por Tipo de Inmueble')
        axes[0, 0].set_xlabel('Tipo de Inmueble')
        axes[0, 0].set_ylabel('Cantidad')
        axes[0, 0].set_xticks(range(len(type_counts)))
        axes[0, 0].set_xticklabels(type_counts.index, rotation=45, ha='right')
        
        # 2. Average price by property type
        avg_prices = df_temp.groupby("Tipo_Inmueble_norm")["Precio_Solicitado"].mean().sort_values(ascending=False).head(10)
        
        # Mostrar información de tipos desconocidos en consola
        tipos_principales = ["casa", "apartamento"]
        tipos_desconocidos = avg_prices[~avg_prices.index.isin(tipos_principales)]
        if not tipos_desconocidos.empty:
            print("\n--- Información de Tipos Desconocidos (Gráfico de Análisis) ---")
            conteo_temp = df_temp["Tipo_Inmueble_norm"].value_counts()
            for tipo in tipos_desconocidos.index:
                precio_avg = tipos_desconocidos[tipo]
                cantidad = conteo_temp[tipo]
                print(f"• {tipo.title()}: ${precio_avg:,.0f} COP promedio ({cantidad:,} registros)")
        
        # Filtrar solo casas y apartamentos para el gráfico
        avg_prices_filtered = avg_prices[avg_prices.index.isin(tipos_principales)]
        if avg_prices_filtered.empty:
            avg_prices_filtered = avg_prices.head(5)  # Fallback si no hay casas/apartamentos
        
        axes[0, 1].bar(range(len(avg_prices_filtered)), avg_prices_filtered.values, color='lightcoral')
        axes[0, 1].set_title('Precio Promedio por Tipo (Casas y Apartamentos)')
        axes[0, 1].set_xlabel('Tipo de Inmueble')
        axes[0, 1].set_ylabel('Precio Promedio (COP)')
        axes[0, 1].set_xticks(range(len(avg_prices_filtered)))
        axes[0, 1].set_xticklabels(avg_prices_filtered.index, rotation=45, ha='right')
        axes[0, 1].ticklabel_format(style='plain', axis='y')
        
        # 3. Price distribution for main types
        main_types = type_counts.head(5).index
        df_main_types = df_temp[df_temp["Tipo_Inmueble_norm"].isin(main_types)]
        
        if not df_main_types.empty:
            df_main_types.boxplot(column="Precio_Solicitado", by="Tipo_Inmueble_norm", ax=axes[1, 0])
            axes[1, 0].set_title('Distribución de Precios por Tipo Principal')
            axes[1, 0].set_xlabel('Tipo de Inmueble')
            axes[1, 0].set_ylabel('Precio Solicitado (COP)')
            plt.sca(axes[1, 0])
            plt.xticks(rotation=45, ha='right')
        
        # 4. Price per m² by property type (if area available)
        if "Area" in df_temp.columns:
            df_temp["Area"] = pd.to_numeric(df_temp["Area"], errors="coerce")
            df_temp = df_temp.dropna(subset=["Area", "Precio_Solicitado"])
            df_temp = df_temp[df_temp["Area"] > 0]
            df_temp["precio_por_m2"] = df_temp["Precio_Solicitado"] / df_temp["Area"]
            
            avg_price_m2 = df_temp.groupby("Tipo_Inmueble_norm")["precio_por_m2"].mean().sort_values(ascending=False).head(8)
            
            # Filtrar solo casas y apartamentos para el gráfico de precio por m²
            avg_price_m2_filtered = avg_price_m2[avg_price_m2.index.isin(tipos_principales)]
            if avg_price_m2_filtered.empty:
                avg_price_m2_filtered = avg_price_m2.head(5)  # Fallback si no hay casas/apartamentos
            
            axes[1, 1].bar(range(len(avg_price_m2_filtered)), avg_price_m2_filtered.values, color='lightgreen')
            axes[1, 1].set_title('Precio Promedio por m² (Casas y Apartamentos)')
            axes[1, 1].set_xlabel('Tipo de Inmueble')
            axes[1, 1].set_ylabel('Precio por m² (COP/m²)')
            axes[1, 1].set_xticks(range(len(avg_price_m2_filtered)))
            axes[1, 1].set_xticklabels(avg_price_m2_filtered.index, rotation=45, ha='right')
            axes[1, 1].ticklabel_format(style='plain', axis='y')
        else:
            axes[1, 1].text(0.5, 0.5, 'Datos de área\nno disponibles', 
                           ha='center', va='center', transform=axes[1, 1].transAxes)
            axes[1, 1].set_title('Precio por m² - No disponible')
        
        plt.tight_layout()
        
        # Save figure
        output_path = self.figures_path / "property_type_analysis.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.show()
        
        print(f"📊 Análisis por tipo de inmueble guardado en: {output_path}")
    
    def create_correlation_heatmap_advanced(self, df):
        """
        Create advanced correlation heatmap with more variables
        
        Args:
            df: DataFrame with the data
        """
        # Find all numerical columns
        numerical_cols = []
        potential_cols = ["Area", "Precio_Solicitado", "Estrato", "Habitaciones", "Banos", "Pisos", "Antiguedad"]
        
        for col in potential_cols:
            if col in df.columns:
                numerical_cols.append(col)
        
        if len(numerical_cols) < 2:
            print("❌ Not enough numerical columns for correlation analysis.")
            return
        
        df_corr = df[numerical_cols].copy()
        for col in numerical_cols:
            df_corr[col] = pd.to_numeric(df_corr[col], errors="coerce")
        
        correlation_matrix = df_corr.corr()
        
        plt.figure(figsize=(10, 8))
        
        # Create heatmap with custom styling
        mask = np.triu(np.ones_like(correlation_matrix, dtype=bool))
        sns.heatmap(correlation_matrix, annot=True, cmap="RdBu_r", vmin=-1, vmax=1,
                   center=0, square=True, linewidths=0.5, cbar_kws={"shrink": 0.8},
                   mask=mask)
        
        plt.title('Matriz de Correlación - Variables Numéricas', fontsize=14, pad=20)
        plt.tight_layout()
        
        # Save figure
        output_path = self.figures_path / "correlation_heatmap_advanced.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.show()
        
        print(f"📊 Matriz de correlación avanzada guardada en: {output_path}")
        return correlation_matrix
    
    def create_interactive_dashboard_html(self, df):
        """
        Create an interactive HTML dashboard with multiple visualizations
        
        Args:
            df: DataFrame with the data
        """
        try:
            import plotly.express as px
            import plotly.graph_objects as go
            from plotly.subplots import make_subplots
            import plotly.offline as pyo
            
            # Prepare data
            df_temp = df.copy()
            for col in ["Area", "Precio_Solicitado", "Estrato"]:
                if col in df_temp.columns:
                    df_temp[col] = pd.to_numeric(df_temp[col], errors="coerce")
            
            # Create subplots
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=('Distribución de Precios', 'Precio por Zona', 
                              'Área vs Precio', 'Precio por Tipo de Inmueble'),
                specs=[[{"secondary_y": False}, {"secondary_y": False}],
                       [{"secondary_y": False}, {"secondary_y": False}]]
            )
            
            # 1. Price distribution
            if "Precio_Solicitado" in df_temp.columns:
                prices = df_temp["Precio_Solicitado"].dropna()
                fig.add_trace(
                    go.Histogram(x=prices, name="Distribución Precios", showlegend=False),
                    row=1, col=1
                )
            
            # 2. Price by zone (box plot)
            if "Zona" in df_temp.columns and "Precio_Solicitado" in df_temp.columns:
                for zone in df_temp["Zona"].unique():
                    zone_prices = df_temp[df_temp["Zona"] == zone]["Precio_Solicitado"].dropna()
                    fig.add_trace(
                        go.Box(y=zone_prices, name=zone, showlegend=False),
                        row=1, col=2
                    )
            
            # 3. Area vs Price scatter
            if "Area" in df_temp.columns and "Precio_Solicitado" in df_temp.columns:
                scatter_data = df_temp.dropna(subset=["Area", "Precio_Solicitado"])
                fig.add_trace(
                    go.Scatter(x=scatter_data["Area"], y=scatter_data["Precio_Solicitado"],
                              mode='markers', name="Área vs Precio", showlegend=False),
                    row=2, col=1
                )
            
            # 4. Price by property type
            if "Tipo_Inmueble" in df_temp.columns and "Precio_Solicitado" in df_temp.columns:
                type_avg = df_temp.groupby("Tipo_Inmueble")["Precio_Solicitado"].mean().head(10)
                
                # Normalizar nombres para filtrar
                type_avg_norm = df_temp.copy()
                type_avg_norm["Tipo_Inmueble_norm"] = (
                    type_avg_norm["Tipo_Inmueble"]
                    .astype(str)
                    .str.lower()
                    .str.normalize("NFKD")
                    .str.encode("ascii", errors="ignore")
                    .str.decode("utf-8")
                    .str.strip()
                )
                type_avg_filtered = type_avg_norm.groupby("Tipo_Inmueble_norm")["Precio_Solicitado"].mean()
                tipos_principales = ["casa", "apartamento"]
                type_avg_main = type_avg_filtered[type_avg_filtered.index.isin(tipos_principales)]
                
                if type_avg_main.empty:
                    type_avg_main = type_avg_filtered.head(5)  # Fallback
                
                fig.add_trace(
                    go.Bar(x=type_avg_main.index, y=type_avg_main.values, 
                          name="Precio por Tipo (Casas y Apartamentos)", showlegend=False),
                    row=2, col=2
                )
            
            # Update layout
            fig.update_layout(
                title_text="Dashboard Interactivo - Análisis Inmobiliario",
                height=800,
                showlegend=False
            )
            
            # Save as HTML
            output_path = self.figures_path / "interactive_dashboard.html"
            pyo.plot(fig, filename=str(output_path), auto_open=False)
            
            print(f"📊 Dashboard interactivo guardado en: {output_path}")
            return str(output_path)
            
        except ImportError:
            print("❌ Plotly no está instalado. Ejecuta 'pip install plotly' para generar dashboards interactivos.")
            return None
    
    def generate_all_visualizations(self, df):
        """
        Generate all available visualizations
        
        Args:
            df: DataFrame with the data
        """
        print("\n" + "=" * 60)
        print("          GENERANDO VISUALIZACIONES")
        print("=" * 60)
        
        # Static visualizations
        self.create_price_distribution_histogram(df)
        self.create_area_vs_price_scatter(df)
        self.create_zone_price_boxplot(df)
        self.create_property_type_analysis(df)
        self.create_correlation_heatmap_advanced(df)
        
        # Interactive dashboard
        self.create_interactive_dashboard_html(df)
        
        print("\n✅ Todas las visualizaciones han sido generadas y guardadas.")
        print(f"📁 Revisa la carpeta: {self.figures_path}")


# Import numpy for trend line calculation
import numpy as np

# Backward compatibility functions
def generar_graficos_basicos(df):
    """Backward compatibility function"""
    generator = VisualizationGenerator()
    generator.create_price_distribution_histogram(df)
    generator.create_area_vs_price_scatter(df)

def generar_graficos_avanzados(df):
    """Backward compatibility function"""
    generator = VisualizationGenerator()
    generator.create_zone_price_boxplot(df)
    generator.create_property_type_analysis(df)
    generator.create_correlation_heatmap_advanced(df)

def generar_dashboard_interactivo(df):
    """Backward compatibility function"""
    generator = VisualizationGenerator()
    return generator.create_interactive_dashboard_html(df)
