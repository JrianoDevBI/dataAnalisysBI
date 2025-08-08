"""
Main Application Entry Point - Modularized Version

Pipeline Integrado Completo para Análisis de Datos Inmobiliarios
==================================================================

Este es el punto de entrada principal del sistema modularizado que orquesta
todos los componentes del pipeline de procesamiento de datos inmobiliarios.

Autor: Juan Camilo Riaño Molano
Fecha de creación: 01/08/2025
Fecha de modularización: 08/08/2025
Versión: 4.1 - Pipeline Unificado Optimizado

ARQUITECTURA MODULAR:
====================
• core/ - Lógica de negocio principal
  - pipeline_orchestrator.py: Orquestación del pipeline
  - execution_modes.py: Modos de ejecución (automático, interactivo, optimizado)
  - indicators_calculator.py: Cálculo de indicadores clave
  - executive_diagrams.py: Generación de diagramas ejecutivos

• utils/ - Utilidades y herramientas
  - performance_metrics.py: Métricas de rendimiento y monitoreo
  - interactive_menu.py: Sistema de menús interactivos

• scripts/ - Módulos de procesamiento específicos
  - (Mantiene la misma estructura modular existente)

BENEFICIOS DE LA MODULARIZACIÓN:
===============================
✓ Mantenibilidad: Código más fácil de entender y mantener
✓ Escalabilidad: Fácil agregar nuevas funcionalidades
✓ Testabilidad: Tests unitarios más específicos y rápidos
✓ Reutilización: Componentes reutilizables en diferentes contextos
✓ Colaboración: Múltiples desarrolladores pueden trabajar simultáneamente
✓ Responsabilidad única: Cada módulo tiene una función específica

MODOS DE EJECUCIÓN:
==================
• MODO UNIFICADO: Pipeline optimizado con control de calidad (RECOMENDADO)
• MODO AUTOMÁTICO: Ejecuta todo el flujo sin intervención
• MODO INTERACTIVO: Permite seleccionar módulos específicos
• MODO OPTIMIZADO: Pipeline con mejoras de performance
• MODO ANÁLISIS: Solo análisis e indicadores
• MODO DIAGRAMAS: Solo generación de visualizaciones
"""

import sys
import os
import argparse
from pathlib import Path

# Agregar raíz del proyecto al path de Python para importaciones
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Importaciones principales
from core.pipeline_orchestrator import PipelineOrchestrator
from core.execution_modes import ExecutionModes
from core.indicators_calculator import IndicatorsCalculator
from core.executive_diagrams import ExecutiveDiagrams

# Importaciones de utilidades
from utils.performance_metrics import PerformanceMetrics
from utils.interactive_menu import InteractiveMenu

# Importaciones de scripts para funcionalidades específicas
from scripts.analisis_exploratorio import ejecutar_analisis_completo


class MainApplication:
    """
    Main application class that coordinates all pipeline components
    """
    
    def __init__(self):
        """Inicializar la aplicación principal"""
        self.orchestrator = PipelineOrchestrator()
        self.execution_modes = ExecutionModes()
        self.indicators_calculator = IndicatorsCalculator()
        self.executive_diagrams = ExecutiveDiagrams()
        self.performance_metrics = PerformanceMetrics()
        self.interactive_menu = InteractiveMenu()
    
    def run(self):
        """Punto de entrada principal de la aplicación"""
        try:
            self.interactive_menu.show_header()
            
            while True:
                selected_option = self.interactive_menu.show_main_menu()
                
                if selected_option == "salir":
                    self._show_exit_message()
                    break
                
                self._handle_menu_selection(selected_option)
                
        except KeyboardInterrupt:
            print("\n\n👋 Programa interrumpido por el usuario.")
            self._show_exit_message()
        except Exception as e:
            print(f"\n❌ Error inesperado en la aplicación: {e}")
            print("🔧 Por favor, reporte este error al equipo de desarrollo.")
    
    def _handle_menu_selection(self, option: str):
        """Manejar selección de menú del usuario"""
        try:
            if option == "pipeline_unificado":
                self._execute_unified_pipeline()
            elif option == "pipeline_optimizado":
                self._execute_optimized_pipeline()
            elif option == "modo_interactivo":
                self._execute_interactive_mode()
            elif option == "modo_automatico":
                self._execute_automatic_mode()
            elif option == "solo_analisis":
                self._execute_analysis_only()
            elif option == "solo_diagramas":
                self._execute_diagrams_only()
            elif option == "metricas":
                self._show_performance_metrics()
            elif option == "optimizaciones":
                self._show_optimizations_analysis()
            elif option == "ayuda":
                self._show_help()
            else:
                print(f"❌ Opción no implementada: {option}")
                
        except Exception as e:
            print(f"\n❌ Error ejecutando opción '{option}': {e}")
            print("🔄 Volviendo al menú principal...")
    
    def _execute_unified_pipeline(self):
        """Ejecutar pipeline unificado (recomendado)"""
        print("\n🚀 Iniciando Pipeline Unificado...")
        try:
            # Establecer modo CLI si estamos en ejecución de línea de comandos
            if hasattr(self, '_is_cli_mode') and self._is_cli_mode:
                self.orchestrator.set_cli_mode(True)
                
            metrics = self.orchestrator.ejecutar_pipeline_unificado()
            if metrics:
                self.performance_metrics.metrics.update(metrics)
                print("\n✅ Pipeline Unificado completado exitosamente!")
        except Exception as e:
            print(f"❌ Error en Pipeline Unificado: {e}")
            print("🔄 Verifique la configuración y intente nuevamente.")
    
    def _execute_optimized_pipeline(self):
        """Ejecutar pipeline optimizado"""
        print("\n⚡ Iniciando Pipeline Optimizado...")
        try:
            # Establecer modo CLI si estamos en ejecución de línea de comandos
            if hasattr(self, '_is_cli_mode') and self._is_cli_mode:
                # Para pipeline optimizado, ejecutamos directamente sin interacción del usuario
                from scripts.pipeline_optimizado import ejecutar_pipeline_optimizado
                resultado = ejecutar_pipeline_optimizado()
                if resultado:
                    print("✅ Pipeline Optimizado completado exitosamente!")
                else:
                    print("⚠️ Pipeline Optimizado completado con advertencias.")
            else:
                self.execution_modes.ejecutar_pipeline_optimizado()
        except Exception as e:
            print(f"❌ Error en Pipeline Optimizado: {e}")
    
    def _execute_interactive_mode(self):
        """Ejecutar modo interactivo"""
        print("\n🛠️ Iniciando Modo Interactivo...")
        try:
            self.execution_modes.ejecutar_modo_interactivo()
        except Exception as e:
            print(f"❌ Error en Modo Interactivo: {e}")
    
    def _execute_automatic_mode(self):
        """Ejecutar modo automático completo"""
        print("\n🎯 Iniciando Modo Automático Completo...")
        try:
            # Establecer modo CLI si estamos en ejecución de línea de comandos
            if hasattr(self, '_is_cli_mode') and self._is_cli_mode:
                self.orchestrator.set_cli_mode(True)
                
            self.execution_modes.ejecutar_modo_automatico_completo()
        except Exception as e:
            print(f"❌ Error en Modo Automático: {e}")
    
    def _execute_analysis_only(self):
        """Ejecutar solo análisis exploratorio"""
        print("\n🔬 Ejecutando Solo Análisis Exploratorio...")
        try:
            # Verificar primero datos limpios, luego datos procesados como respaldo
            if os.path.exists("data/cleanData/CLMUESTRA.csv"):
                data_source = "cleanData"
                print("📂 Usando datos limpios para análisis...")
            elif os.path.exists("data/processedData/muestra.csv"):
                data_source = "processedData"
                print("📂 Usando datos procesados para análisis (fallback)...")
            else:
                print("❌ No se encontraron datos para análisis.")
                print("💡 Ejecute primero el pipeline de procesamiento.")
                return
            
            # Para ejecución de línea de comandos, por defecto no mostrar gráficos
            if hasattr(self, '_is_cli_mode') and self._is_cli_mode:
                show_graphics = False
                print("🖥️ Modo línea de comandos: gráficos deshabilitados")
            else:
                show_graphics = self.interactive_menu.confirm_action(
                    "¿Desea mostrar gráficos interactivos?", default=True
                )
            
            ejecutar_analisis_completo(mostrar_graficos=show_graphics)
            
            # Calcular indicadores después del análisis
            calculate_indicators = True
            if not (hasattr(self, '_is_cli_mode') and self._is_cli_mode):
                calculate_indicators = self.interactive_menu.confirm_action(
                    "¿Desea calcular también los indicadores clave?", default=True
                )
            
            if calculate_indicators:
                print("📊 Calculando indicadores clave...")
                self.indicators_calculator.calcular_indicadores_clave()
                
        except Exception as e:
            print(f"❌ Error en análisis exploratorio: {e}")
            import traceback
            traceback.print_exc()
    
    def _execute_diagrams_only(self):
        """Ejecutar solo generación de diagramas ejecutivos"""
        print("\n🎨 Generando Solo Diagramas Ejecutivos...")
        try:
            self.executive_diagrams.generar_todos_los_diagramas()
        except Exception as e:
            print(f"❌ Error generando diagramas: {e}")
    
    def _show_performance_metrics(self):
        """Mostrar métricas de rendimiento actuales"""
        print("\n📊 Mostrando Métricas de Performance...")
        try:
            self.performance_metrics.show_current_metrics()
            
            # Ofrecer exportar métricas
            if self.interactive_menu.confirm_action(
                "¿Desea exportar las métricas a un archivo?", default=False
            ):
                report_path = self.performance_metrics.export_metrics_report()
                print(f"✅ Métricas exportadas a: {report_path}")
                
        except Exception as e:
            print(f"❌ Error mostrando métricas: {e}")
    
    def _show_optimizations_analysis(self):
        """Mostrar análisis de optimizaciones"""
        self.interactive_menu.show_optimizations_analysis()
    
    def _show_help(self):
        """Mostrar ayuda y documentación"""
        self.interactive_menu.show_help()
    
    def _show_exit_message(self):
        """Mostrar mensaje de salida"""
        print("\n" + "=" * 60)
        print("👋 ¡Gracias por usar el Sistema de Business Intelligence!")
        print("=" * 60)
        print("📊 Sistema desarrollado por Juan Camilo Riaño Molano")
        print("🏢 Pipeline Integrado para Análisis Inmobiliario")
        print("📅 Versión 4.1 - Pipeline Unificado Optimizado")
        print("=" * 60)


def main():
    """Función principal con análisis de argumentos"""
    args = parse_arguments()
    app = MainApplication()
    
    # Si se proporcionan argumentos de línea de comandos, ejecutar directamente
    if args.unified:
        print("🚀 Ejecutando Pipeline Unificado desde línea de comandos...")
        app._execute_unified_pipeline()
    elif args.optimized:
        print("⚡ Ejecutando Pipeline Optimizado desde línea de comandos...")
        app._execute_optimized_pipeline()
    elif args.automatic:
        print("🎯 Ejecutando Modo Automático desde línea de comandos...")
        app._execute_automatic_mode()
    elif args.analysis_only:
        print("� Ejecutando Solo Análisis desde línea de comandos...")
        app._execute_analysis_only()
    elif args.diagrams_only:
        print("🎨 Ejecutando Solo Diagramas desde línea de comandos...")
        app._execute_diagrams_only()
    else:
        # Ejecutar modo interactivo
        app.run()


# Funciones de retrocompatibilidad
def ejecutar_analisis_exploratorio_interactivo():
    """Función de retrocompatibilidad para análisis exploratorio"""
    try:
        if not os.path.exists("data/cleanData/CLMUESTRA.csv"):
            print("❌ No se encontraron datos limpios.")
            print("💡 Ejecute primero el pipeline de procesamiento.")
            return
        
        menu = InteractiveMenu()
        show_graphics = menu.confirm_action(
            "¿Desea mostrar gráficos interactivos?", default=True
        )
        
        ejecutar_analisis_completo(mostrar_graficos=show_graphics)
        print("✅ Análisis exploratorio completado.")
        
    except Exception as e:
        print(f"❌ Error en análisis exploratorio: {e}")


def parse_arguments():
    """Analizar argumentos de línea de comandos"""
    parser = argparse.ArgumentParser(
        description="Pipeline Integrado para Análisis de Datos Inmobiliarios",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:
  python main.py                    # Modo interactivo (menú)
  python main.py --unified           # Ejecutar pipeline unificado
  python main.py --optimized         # Ejecutar pipeline optimizado
  python main.py --analysis-only     # Solo análisis exploratorio
  python main.py --diagrams-only     # Solo diagramas ejecutivos
        """
    )
    
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--unified", "-u",
        action="store_true",
        help="Ejecutar pipeline unificado (recomendado)"
    )
    group.add_argument(
        "--optimized", "-o",
        action="store_true",
        help="Ejecutar pipeline optimizado"
    )
    group.add_argument(
        "--automatic", "-a",
        action="store_true",
        help="Ejecutar modo automático completo"
    )
    group.add_argument(
        "--analysis-only", "-an",
        action="store_true",
        help="Solo ejecutar análisis exploratorio"
    )
    group.add_argument(
        "--diagrams-only", "-d",
        action="store_true",
        help="Solo generar diagramas ejecutivos"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Mostrar información detallada de ejecución"
    )
    
    return parser.parse_args()


# Backward compatibility functions
def ejecutar_analisis_exploratorio_interactivo():
    """Backward compatibility function for exploratory analysis"""
    try:
        if not os.path.exists("data/cleanData/CLMUESTRA.csv"):
            print("❌ No se encontraron datos limpios.")
            print("💡 Ejecute primero el pipeline de procesamiento.")
            return
        
        menu = InteractiveMenu()
        show_graphics = menu.confirm_action(
            "¿Desea mostrar gráficos interactivos?", default=True
        )
        
        ejecutar_analisis_completo(mostrar_graficos=show_graphics)
        print("✅ Análisis exploratorio completado.")
        
    except Exception as e:
        print(f"❌ Error en análisis exploratorio: {e}")


def main():
    """Main function with argument parsing"""
    args = parse_arguments()
    app = MainApplication()
    
    # Set CLI mode flag for non-interactive execution
    if any([args.unified, args.optimized, args.automatic, args.analysis_only, args.diagrams_only]):
        app._is_cli_mode = True
    
    # If command line arguments are provided, execute directly
    if args.unified:
        print("🚀 Ejecutando Pipeline Unificado desde línea de comandos...")
        app._execute_unified_pipeline()
    elif args.optimized:
        print("⚡ Ejecutando Pipeline Optimizado desde línea de comandos...")
        app._execute_optimized_pipeline()
    elif args.automatic:
        print("🎯 Ejecutando Modo Automático desde línea de comandos...")
        app._execute_automatic_mode()
    elif args.analysis_only:
        print("🔬 Ejecutando Solo Análisis desde línea de comandos...")
        app._execute_analysis_only()
    elif args.diagrams_only:
        print("🎨 Ejecutando Solo Diagramas desde línea de comandos...")
        app._execute_diagrams_only()
    else:
        # Run interactive mode
        app.run()


# Legacy function imports for backward compatibility
try:
    from scripts.clean_and_backup_data import clean_and_backup_data
    from scripts.obtain_data import obtain_data
    from scripts.tratamiento_inconsistencias import ejecutar_tratamiento_inconsistencias
    from scripts.clean_muestra import clean_muestra
    from scripts.clean_estados import clean_estados
    from scripts.test_db_connection import probar_conexion_db
    from scripts.load_to_sql import main as load_to_sql_main
    from scripts.export_sql_to_excel import export_sql_to_excel
except ImportError as e:
    print(f"⚠️ Advertencia: No se pudieron importar algunos módulos legacy: {e}")


if __name__ == "__main__":
    main()
