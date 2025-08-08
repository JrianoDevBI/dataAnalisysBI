"""
Executive Diagrams Generator Module

This module contains all the executive diagrams and visualization
generation logic for management presentations.

Author: Juan Camilo Riaño Molano
Date: 06/08/2025
"""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, ConnectionPatch
import numpy as np
from pathlib import Path


class ExecutiveDiagrams:
    """
    Class responsible for generating executive diagrams and visualizations
    """
    
    def __init__(self):
        """Initialize the diagrams generator"""
        self.output_dir = Path("docs")
        self.output_dir.mkdir(exist_ok=True)
        
        # Corporate colors
        self.colors = {
            "input": "#2E86AB",      # Blue
            "process": "#A23B72",    # Magenta
            "decision": "#F18F01",   # Orange
            "output": "#C73E1D",     # Red
            "analysis": "#592E83",   # Purple
        }
    
    def _create_box(self, ax, x, y, width, height, text, color, text_size=9):
        """Create a styled box with text"""
        box = FancyBboxPatch(
            (x - width / 2, y - height / 2),
            width,
            height,
            boxstyle="round,pad=0.1",
            facecolor=color,
            edgecolor="black",
            linewidth=1.5,
        )
        ax.add_patch(box)
        ax.text(x, y, text, ha="center", va="center", fontsize=text_size, 
                fontweight="bold", color="white", wrap=True)
    
    def _create_arrow(self, ax, x1, y1, x2, y2, color="black"):
        """Create an arrow between two points"""
        arrow = ConnectionPatch(
            (x1, y1),
            (x2, y2),
            "data",
            "data",
            arrowstyle="->",
            shrinkA=5,
            shrinkB=5,
            mutation_scale=20,
            fc=color,
            ec=color,
            linewidth=2,
        )
        ax.add_patch(arrow)
    
    def crear_diagrama_flujo_ejecutivo(self):
        """
        Generate a professional visual flow diagram for management presentation.
        """
        print("\n🎨 Generando diagrama de flujo ejecutivo...")
        
        # Chart configuration
        fig, ax = plt.subplots(1, 1, figsize=(16, 12))
        ax.set_xlim(0, 10)
        ax.set_ylim(0, 12)
        ax.axis("off")
        
        # TITLE
        ax.text(5, 11.5, "PIPELINE DE PROCESAMIENTO DE DATOS INMOBILIARIOS", 
                ha="center", va="center", fontsize=18, fontweight="bold")
        ax.text(5, 11, "Sistema de Business Intelligence - Análisis Avanzado", 
                ha="center", va="center", fontsize=12, style="italic")
        
        # PHASE 1: INPUT
        self._create_box(ax, 1.5, 10, 2.5, 0.8, 
                        "📁 DATOS SOURCE\nArchivo Excel\n52K+ registros", 
                        self.colors["input"])
        
        # PHASE 2: EXTRACTION
        self._create_box(ax, 1.5, 8.5, 2.5, 0.8, 
                        "🔄 EXTRACCIÓN\nobtain_data.py\n8K registros/seg", 
                        self.colors["process"])
        self._create_arrow(ax, 1.5, 9.6, 1.5, 8.9)
        
        # PHASE 3: PRE-ANALYSIS
        self._create_box(ax, 5, 10, 2.5, 0.8, 
                        "🔍 PRE-ANÁLISIS\nDetección de Inconsistencias\n391 outliers detectados", 
                        self.colors["analysis"])
        self._create_arrow(ax, 2.75, 8.5, 3.75, 9.6)
        
        # DECISION 1
        self._create_box(ax, 5, 8.5, 2, 0.6, "❓ CALIDAD OK?", self.colors["decision"], 8)
        self._create_arrow(ax, 5, 9.6, 5, 8.8)
        
        # PHASE 4: ADVANCED TREATMENT
        self._create_box(ax, 8.5, 10, 2.5, 0.8, 
                        "⚗️ TRATAMIENTO\nWinsorización\n349 outliers tratados", 
                        self.colors["process"])
        self._create_arrow(ax, 6, 8.5, 7.5, 9.6)
        
        # PHASE 5: CLEANING
        self._create_box(ax, 8.5, 8.5, 2.5, 0.8, 
                        "🧼 LIMPIEZA\nParalela: Muestra + Estados\n99.7% completitud", 
                        self.colors["process"])
        self._create_arrow(ax, 8.5, 9.6, 8.5, 8.9)
        
        # PHASE 6: SQL VALIDATION
        self._create_box(ax, 5, 7, 2, 0.6, "🔌 CONEXIÓN SQL", self.colors["decision"], 8)
        self._create_arrow(ax, 8.5, 8.1, 6, 7.3)
        
        # PHASE 7: LOAD
        self._create_box(ax, 2, 5.5, 2.5, 0.8, 
                        "🗄️ CARGA SQL\nload_to_sql.py\n2K registros/seg", 
                        self.colors["process"])
        self._create_arrow(ax, 4, 7, 3.25, 5.9)
        
        # PHASE 8: QUERIES
        self._create_box(ax, 5, 5.5, 2.5, 0.8, 
                        "📊 CONSULTAS SQL\n3 Queries Críticas\nRanking + Estados", 
                        self.colors["analysis"])
        self._create_arrow(ax, 3.25, 5.5, 3.75, 5.5)
        
        # PHASE 9: ANALYSIS
        self._create_box(ax, 8, 5.5, 2.5, 0.8, 
                        "🔬 ANÁLISIS\nCorrelaciones + KPIs\nOutliers por zona", 
                        self.colors["analysis"])
        self._create_arrow(ax, 6.25, 5.5, 6.75, 5.5)
        
        # OUTPUTS
        self._create_box(ax, 2, 3.5, 2.5, 0.8, 
                        "📋 REPORTES EXCEL\nultimo_estado.xlsx\ndiferencia_ranking.xlsx", 
                        self.colors["output"])
        self._create_arrow(ax, 3.25, 5.1, 2.75, 3.9)
        
        self._create_box(ax, 5, 3.5, 2.5, 0.8, 
                        "📈 DASHBOARDS\nIndicadores Clave\nVisualizaciones", 
                        self.colors["output"])
        self._create_arrow(ax, 5, 5.1, 5, 3.9)
        
        self._create_box(ax, 8, 3.5, 2.5, 0.8, 
                        "🎯 INSIGHTS\nDetección Outliers\nPatrones de Negocio", 
                        self.colors["output"])
        self._create_arrow(ax, 8, 5.1, 8, 3.9)
        
        # KEY METRICS
        self._add_key_metrics(ax)
        
        # PROPOSED IMPROVEMENTS
        self._add_proposed_improvements(ax)
        
        # Legend
        self._add_legend(ax)
        
        plt.tight_layout()
        output_path = self.output_dir / "diagrama_flujo_ejecutivo.png"
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        plt.show()
        
        print(f"✅ Diagrama de flujo ejecutivo generado: {output_path}")
        return output_path
    
    def _add_key_metrics(self, ax):
        """Add key metrics to the diagram"""
        ax.text(1, 2, "MÉTRICAS CLAVE:", fontsize=12, fontweight="bold")
        ax.text(1, 1.6, "⏱️ Tiempo Total: 89-134 seg", fontsize=10)
        ax.text(1, 1.3, "🎯 Calidad Final: 99.7%", fontsize=10)
        ax.text(1, 1.0, "⚡ Throughput: 390-588 reg/seg", fontsize=10)
        ax.text(1, 0.7, "🔄 Duplicados críticos: 0", fontsize=10)
    
    def _add_proposed_improvements(self, ax):
        """Add proposed improvements to the diagram"""
        ax.text(6, 2, "OPTIMIZACIONES PROPUESTAS:", fontsize=12, fontweight="bold")
        ax.text(6, 1.6, "📊 Cache DataFrames: -75% tiempo lectura", fontsize=10)
        ax.text(6, 1.3, "⚡ Procesamiento paralelo: -46% limpieza", fontsize=10)
        ax.text(6, 1.0, "🔗 Pool conexiones: -75% overhead SQL", fontsize=10)
        ax.text(6, 0.7, "🎯 ROI Total: 47-48% reducción tiempo", 
                fontsize=10, fontweight="bold", color="red")
    
    def _add_legend(self, ax):
        """Add legend to the diagram"""
        legend_elements = [
            mpatches.Patch(color=self.colors["input"], label="Datos de Entrada"),
            mpatches.Patch(color=self.colors["process"], label="Procesamiento"),
            mpatches.Patch(color=self.colors["decision"], label="Validación"),
            mpatches.Patch(color=self.colors["analysis"], label="Análisis"),
            mpatches.Patch(color=self.colors["output"], label="Resultados"),
        ]
        ax.legend(handles=legend_elements, loc="upper right", bbox_to_anchor=(0.98, 0.25))
    
    def generar_metricas_rendimiento(self):
        """
        Generate performance metrics chart by module.
        """
        print("📊 Generando métricas de rendimiento...")
        
        modules = ["obtain_data", "pre_analysis", "treatment", "cleaning", "sql_load", "analysis"]
        throughput = [8054, 5235, 799, 555, 2094, 500]  # records/second
        time = [6.5, 10.0, 12.5, 18.0, 25.0, 20.0]  # seconds
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        # Throughput chart
        bars1 = ax1.bar(modules, throughput, color=self.colors["analysis"], alpha=0.8)
        ax1.set_title("Throughput por Módulo (registros/seg)", fontsize=14, fontweight="bold")
        ax1.set_ylabel("Registros/segundo")
        ax1.tick_params(axis='x', rotation=45)
        
        # Add values on bars
        for bar, value in zip(bars1, throughput):
            ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 100,
                    f'{value:,}', ha='center', va='bottom', fontweight='bold')
        
        # Time chart
        bars2 = ax2.bar(modules, time, color=self.colors["process"], alpha=0.8)
        ax2.set_title("Tiempo de Ejecución por Módulo", fontsize=14, fontweight="bold")
        ax2.set_ylabel("Tiempo (segundos)")
        ax2.tick_params(axis='x', rotation=45)
        
        # Add values on bars
        for bar, value in zip(bars2, time):
            ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                    f'{value:.1f}s', ha='center', va='bottom', fontweight='bold')
        
        plt.tight_layout()
        output_path = self.output_dir / "metricas_rendimiento.png"
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        plt.show()
        
        print(f"✅ Métricas de rendimiento generadas: {output_path}")
        return output_path
    
    def generar_comparacion_optimizacion(self):
        """
        Generate optimization comparison chart (before/after).
        """
        print("⚡ Generando comparación de optimización...")
        
        stages = ["Carga Datos", "Limpieza", "SQL Queries", "Análisis", "Total"]
        before = [45, 38, 28, 22, 133]  # seconds
        after = [12, 20, 7, 18, 70]     # seconds
        improvement = [(b-a)/b*100 for b, a in zip(before, after)]
        
        x = np.arange(len(stages))
        width = 0.35
        
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
        
        # Time comparison
        bars1 = ax1.bar(x - width/2, before, width, label='Antes', 
                       color=self.colors["output"], alpha=0.8)
        bars2 = ax1.bar(x + width/2, after, width, label='Después', 
                       color=self.colors["analysis"], alpha=0.8)
        
        ax1.set_title("Comparación de Tiempos: Antes vs Después", 
                     fontsize=14, fontweight="bold")
        ax1.set_ylabel("Tiempo (segundos)")
        ax1.set_xticks(x)
        ax1.set_xticklabels(stages)
        ax1.legend()
        
        # Add values on bars
        for bars in [bars1, bars2]:
            for bar in bars:
                height = bar.get_height()
                ax1.text(bar.get_x() + bar.get_width()/2., height + 1,
                        f'{height:.0f}s', ha='center', va='bottom')
        
        # Improvement percentage
        bars3 = ax2.bar(stages, improvement, color=self.colors["input"], alpha=0.8)
        ax2.set_title("Porcentaje de Mejora por Etapa", fontsize=14, fontweight="bold")
        ax2.set_ylabel("Mejora (%)")
        ax2.tick_params(axis='x', rotation=45)
        
        # Add values on bars
        for bar, value in zip(bars3, improvement):
            ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                    f'{value:.1f}%', ha='center', va='bottom', fontweight='bold')
        
        plt.tight_layout()
        output_path = self.output_dir / "comparacion_optimizacion.png"
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        plt.show()
        
        print(f"✅ Comparación de optimización generada: {output_path}")
        return output_path
    
    def generar_todos_los_diagramas(self):
        """
        Generate all executive diagrams.
        """
        print("\n" + "=" * 60)
        print("🎨 GENERANDO TODOS LOS DIAGRAMAS EJECUTIVOS")
        print("=" * 60)
        
        results = {}
        
        try:
            results["flujo"] = self.crear_diagrama_flujo_ejecutivo()
            results["metricas"] = self.generar_metricas_rendimiento()
            results["comparacion"] = self.generar_comparacion_optimizacion()
            
            print("\n✅ Todos los diagramas ejecutivos generados exitosamente!")
            print(f"📁 Archivos guardados en: {self.output_dir.absolute()}")
            
        except Exception as e:
            print(f"❌ Error generando diagramas: {e}")
            
        return results


# Functions for backward compatibility
def crear_diagrama_flujo_ejecutivo():
    """Backward compatibility function"""
    diagrams = ExecutiveDiagrams()
    return diagrams.crear_diagrama_flujo_ejecutivo()

def generar_metricas_rendimiento():
    """Backward compatibility function"""
    diagrams = ExecutiveDiagrams()
    return diagrams.generar_metricas_rendimiento()

def generar_comparacion_optimizacion():
    """Backward compatibility function"""
    diagrams = ExecutiveDiagrams()
    return diagrams.generar_comparacion_optimizacion()

def generar_todos_los_diagramas():
    """Backward compatibility function"""
    diagrams = ExecutiveDiagrams()
    return diagrams.generar_todos_los_diagramas()
