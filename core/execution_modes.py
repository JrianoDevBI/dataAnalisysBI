"""
Execution Modes Module

This module contains different execution modes for the pipeline:
automatic, interactive, and optimized modes with their specific logic.

Author: Juan Camilo Riaño Molano
Date: 06/08/2025
"""

import os
from pathlib import Path


class ExecutionModes:
    """
    Class responsible for managing different execution modes
    """
    
    def __init__(self):
        """Inicializar modos de ejecución"""
        self.available_modes = [
            "automatico_completo",
            "interactivo", 
            "optimizado",
            "solo_analisis",
            "solo_diagramas"
        ]
    
    def ejecutar_modo_automatico_completo(self):
        """
        Execute complete automatic pipeline: processing + analysis + indicators + diagrams.
        """
        print("\n" + "=" * 70)
        print("🎯 EJECUTANDO MODO AUTOMÁTICO COMPLETO")
        print("=" * 70)
        print("Este modo ejecutará:")
        print("✓ Pipeline completo de procesamiento")
        print("✓ Análisis exploratorio completo")
        print("✓ Cálculo de indicadores clave")
        print("✓ Generación de diagramas ejecutivos")
        print()
        
        confirmar = input("¿Desea continuar? (Si/No): ").strip().lower()
        if confirmar != "si":
            print("🔙 Volviendo al menú principal...")
            return
        
        try:
            # Importar módulos requeridos
            from core.pipeline_orchestrator import PipelineOrchestrator
            from core.indicators_calculator import IndicatorsCalculator
            from core.executive_diagrams import ExecutiveDiagrams
            from scripts.analisis_exploratorio import ejecutar_analisis_completo
            
            orchestrator = PipelineOrchestrator()
            calculator = IndicatorsCalculator()
            diagrams = ExecutiveDiagrams()
            
            # ETAPA 1: PROCESAMIENTO BASE
            print("\n" + "🏗️" * 20)
            print("ETAPA 1: PROCESAMIENTO BASE")
            print("🏗️" * 20)
            orchestrator.run_pipeline()
            
            # ETAPA 2: ANÁLISIS EXPLORATORIO AUTOMÁTICO
            print("\n" + "🔬" * 20)
            print("ETAPA 2: ANÁLISIS EXPLORATORIO AUTOMÁTICO")
            print("🔬" * 20)
            if os.path.exists("data/cleanData/CLMUESTRA.csv"):
                # Preguntar al usuario si quiere mostrar gráficos
                mostrar_graficos = input("¿Desea mostrar los gráficos del análisis exploratorio? (Si/No): ").strip().lower() == "si"
                print(f"📈 Ejecutando análisis exploratorio (gráficos: {'activados' if mostrar_graficos else 'desactivados'})...")
                ejecutar_analisis_completo(mostrar_graficos=mostrar_graficos)
            else:
                print("❌ No se encontraron datos limpios. Saltando análisis exploratorio.")
            
            # ETAPA 3: INDICADORES CLAVE
            print("\n" + "📊" * 20)
            print("ETAPA 3: CÁLCULO DE INDICADORES CLAVE")
            print("📊" * 20)
            if os.path.exists("data/cleanData/CLMUESTRA.csv"):
                calculator.calcular_indicadores_clave()
            else:
                print("❌ No se encontraron datos limpios. Saltando indicadores.")
            
            # ETAPA 4: DIAGRAMAS EJECUTIVOS
            print("\n" + "🎨" * 20)
            print("ETAPA 4: GENERACIÓN DE DIAGRAMAS EJECUTIVOS")
            print("🎨" * 20)
            diagrams.generar_todos_los_diagramas()
            
            print("\n" + "🎉" * 50)
            print("🎉 PIPELINE AUTOMÁTICO COMPLETO FINALIZADO EXITOSAMENTE")
            print("🎉" * 50)
            print("📁 Revise los siguientes directorios para los resultados:")
            print("   • data/query_data/ - Reportes Excel")
            print("   • docs/ - Diagramas y documentación")
            print("   • reports/ - Informes generados")
            
        except Exception as e:
            print(f"\n❌ Error en el modo automático completo: {e}")
            print("🔙 Volviendo al menú principal...")
    
    def ejecutar_modo_interactivo(self):
        """
        Interactive mode that allows selecting specific pipeline modules.
        Combines optimized efficiency with granular control for advanced users.
        """
        print("\n" + "=" * 70)
        print("🛠️ MODO INTERACTIVO - PIPELINE UNIFICADO MODULAR")
        print("=" * 70)
        print("💡 Ejecute módulos individuales con optimizaciones integradas")
        
        while True:
            print("\n📋 Seleccione el módulo a ejecutar:")
            print("1. 🧹 Backup y preparación inteligente")
            print("2. 📊 Obtención de datos con cache")
            print("3. 🔍 Análisis pre-limpieza exhaustivo")
            print("4. ⚗️ Tratamiento estadístico de inconsistencias")
            print("5. 🧼 Limpieza paralela optimizada (muestra + estados)")
            print("6. 🗄️ Carga SQL con pool de conexiones")
            print("7. 📋 Exportación eficiente de reportes")
            print("8. 🔬 Análisis exploratorio (con/sin gráficos)")
            print("9. 📊 Cálculo de indicadores clave")
            print("10. 🎨 Generación de diagramas ejecutivos")
            print("11. 📈 Ver métricas de performance")
            print("12. 🔙 Volver al menú principal")
            
            opcion = input("\nSeleccione una opción (1-12): ").strip()
            
            if opcion == "1":
                self._execute_backup_preparation()
            elif opcion == "2":
                self._execute_data_obtaining()
            elif opcion == "3":
                self._execute_pre_cleaning_analysis()
            elif opcion == "4":
                self._execute_inconsistencies_treatment()
            elif opcion == "5":
                self._execute_parallel_cleaning()
            elif opcion == "6":
                self._execute_sql_loading()
            elif opcion == "7":
                self._execute_reports_export()
            elif opcion == "8":
                self._execute_exploratory_analysis()
            elif opcion == "9":
                self._execute_indicators_calculation()
            elif opcion == "10":
                self._execute_diagrams_generation()
            elif opcion == "11":
                self._show_performance_metrics()
            elif opcion == "12":
                print("🔙 Volviendo al menú principal...")
                break
            else:
                print("❌ Opción no válida. Por favor, seleccione 1-12.")
    
    def _execute_backup_preparation(self):
        """Ejecutar backup y preparación inteligente"""
        print("\n🧹 Ejecutando backup y preparación inteligente...")
        try:
            from scripts.optimizacion_performance import MetricsCollector
            from scripts.clean_and_backup_data import clean_and_backup_data
            
            metrics = MetricsCollector()
            with metrics.timer("backup_interactivo"):
                clean_and_backup_data()
            print(f"✓ Completado en {metrics.get_metrics()['backup_interactivo']:.2f}s")
        except Exception as e:
            print(f"❌ Error en backup: {e}")
    
    def _execute_data_obtaining(self):
        """Ejecutar obtención de datos con cache"""
        print("\n📊 Ejecutando obtención de datos con cache...")
        try:
            from scripts.optimizacion_performance import MetricsCollector
            from scripts.obtain_data import obtain_data
            
            metrics = MetricsCollector()
            with metrics.timer("obtain_interactivo"):
                obtain_data()
            print(f"✓ Completado en {metrics.get_metrics()['obtain_interactivo']:.2f}s")
        except Exception as e:
            print(f"❌ Error obteniendo datos: {e}")
    
    def _execute_pre_cleaning_analysis(self):
        """Ejecutar análisis exhaustivo pre-limpieza"""
        print("\n🔍 Ejecutando análisis pre-limpieza exhaustivo...")
        try:
            from scripts.optimizacion_performance import MetricsCollector
            # El análisis pre-limpieza ha sido integrado al análisis exploratorio
            from scripts.analisis_exploratorio import ejecutar_analisis_completo
            
            metrics = MetricsCollector()
            with metrics.timer("preanalisis_interactivo"):
                print("📊 Ejecutando análisis exploratorio como análisis pre-limpieza...")
                ejecutar_analisis_completo()
            print(f"✓ Completado en {metrics.get_metrics()['preanalisis_interactivo']:.2f}s")
        except Exception as e:
            print(f"❌ Error en pre-análisis: {e}")
    
    def _execute_inconsistencies_treatment(self):
        """Ejecutar tratamiento estadístico de inconsistencias"""
        print("\n⚗️ Ejecutando tratamiento estadístico de inconsistencias...")
        try:
            from scripts.optimizacion_performance import MetricsCollector
            from scripts.tratamiento_inconsistencias import ejecutar_tratamiento_inconsistencias
            
            metrics = MetricsCollector()
            with metrics.timer("tratamiento_interactivo"):
                ejecutar_tratamiento_inconsistencias()
            print(f"✓ Completado en {metrics.get_metrics()['tratamiento_interactivo']:.2f}s")
        except Exception as e:
            print(f"❌ Error en tratamiento: {e}")
    
    def _execute_parallel_cleaning(self):
        """Ejecutar limpieza paralela optimizada"""
        print("\n🧼 Ejecutando limpieza paralela optimizada...")
        try:
            from scripts.optimizacion_performance import MetricsCollector
            from concurrent.futures import ThreadPoolExecutor
            import os
            
            metrics = MetricsCollector()
            with metrics.timer("limpieza_interactivo"):
                # Definir rutas estándar
                muestra_input = "data/processedData/muestra.csv"
                muestra_output = "data/cleanData/CLMUESTRA.csv"
                estados_input = "data/processedData/estados.csv"
                estados_output = "data/cleanData/CLESTADOS.csv"
                
                # Crear directorio de salida si no existe
                os.makedirs("data/cleanData", exist_ok=True)
                
                with ThreadPoolExecutor(max_workers=2) as executor:
                    def limpiar_muestra():
                        from scripts.clean_muestra import clean_muestra
                        clean_muestra(muestra_input, muestra_output)
                    
                    def limpiar_estados():
                        from scripts.clean_estados import clean_estados
                        clean_estados(estados_input, estados_output)
                    
                    future_muestra = executor.submit(limpiar_muestra)
                    future_estados = executor.submit(limpiar_estados)
                    
                    future_muestra.result()
                    future_estados.result()
            
            print(f"✓ Completado en {metrics.get_metrics()['limpieza_interactivo']:.2f}s")
        except Exception as e:
            print(f"❌ Error en limpieza: {e}")
    
    def _execute_sql_loading(self):
        """Ejecutar carga SQL con pool de conexiones"""
        print("\n🗄️ Ejecutando carga SQL con pool de conexiones...")
        try:
            from scripts.optimizacion_performance import MetricsCollector
            from scripts.load_to_sql import main as load_to_sql
            
            metrics = MetricsCollector()
            with metrics.timer("sql_interactivo"):
                load_to_sql()
            print(f"✓ Completado en {metrics.get_metrics()['sql_interactivo']:.2f}s")
        except Exception as e:
            print(f"❌ Error en carga SQL: {e}")
    
    def _execute_reports_export(self):
        """Ejecutar exportación eficiente de reportes"""
        print("\n📋 Ejecutando exportación eficiente de reportes...")
        try:
            from scripts.optimizacion_performance import MetricsCollector
            from scripts.export_sql_to_excel import export_sql_to_excel
            
            metrics = MetricsCollector()
            with metrics.timer("export_interactivo"):
                export_sql_to_excel()
            print(f"✓ Completado en {metrics.get_metrics()['export_interactivo']:.2f}s")
        except Exception as e:
            print(f"❌ Error en exportación: {e}")
    
    def _execute_exploratory_analysis(self):
        """Ejecutar análisis exploratorio (con/sin gráficos)"""
        mostrar = input("¿Mostrar gráficos? (Si/No): ").strip().lower() == "si"
        print(f"\n🔬 Ejecutando análisis exploratorio (gráficos: {'Si' if mostrar else 'No'})...")
        try:
            from scripts.optimizacion_performance import MetricsCollector
            from scripts.analisis_exploratorio import ejecutar_analisis_completo
            
            metrics = MetricsCollector()
            with metrics.timer("analisis_interactivo"):
                ejecutar_analisis_completo(mostrar_graficos=mostrar)
            print(f"✓ Completado en {metrics.get_metrics()['analisis_interactivo']:.2f}s")
        except Exception as e:
            print(f"❌ Error en análisis: {e}")
    
    def _execute_indicators_calculation(self):
        """Ejecutar cálculo de indicadores clave"""
        print("\n📊 Ejecutando cálculo de indicadores clave...")
        try:
            from scripts.optimizacion_performance import MetricsCollector
            from core.indicators_calculator import IndicatorsCalculator
            
            metrics = MetricsCollector()
            calculator = IndicatorsCalculator()
            with metrics.timer("indicadores_interactivo"):
                calculator.calcular_indicadores_clave()
            print(f"✓ Completado en {metrics.get_metrics()['indicadores_interactivo']:.2f}s")
        except Exception as e:
            print(f"❌ Error calculando indicadores: {e}")
    
    def _execute_diagrams_generation(self):
        """Ejecutar generación de diagramas ejecutivos"""
        print("\n🎨 Ejecutando generación de diagramas ejecutivos...")
        try:
            from scripts.optimizacion_performance import MetricsCollector
            from core.executive_diagrams import ExecutiveDiagrams
            
            metrics = MetricsCollector()
            diagrams = ExecutiveDiagrams()
            with metrics.timer("diagramas_interactivo"):
                diagrams.generar_todos_los_diagramas()
            print(f"✓ Completado en {metrics.get_metrics()['diagramas_interactivo']:.2f}s")
        except Exception as e:
            print(f"❌ Error generando diagramas: {e}")
    
    def _show_performance_metrics(self):
        """Mostrar métricas de rendimiento"""
        print("\n📈 Mostrando métricas de performance...")
        try:
            from utils.performance_metrics import PerformanceMetrics
            
            metrics = PerformanceMetrics()
            metrics.show_current_metrics()
        except Exception as e:
            print(f"❌ Error mostrando métricas: {e}")
    
    def ejecutar_pipeline_optimizado(self):
        """
        Execute optimized pipeline with performance enhancements.
        """
        print("\n" + "=" * 70)
        print("⚡ EJECUTANDO PIPELINE OPTIMIZADO")
        print("=" * 70)
        print("🚀 Mejoras implementadas:")
        print("   • Cache inteligente de DataFrames")
        print("   • Procesamiento paralelo de limpieza")
        print("   • Pool de conexiones SQL optimizado")
        print("   • Métricas de rendimiento en tiempo real")
        print()
        
        confirmar = input("¿Desear ejecutar el pipeline optimizado? (Si/No): ").strip().lower()
        if confirmar != "si":
            print("🔙 Volviendo al menú principal...")
            return
        
        try:
            from scripts.pipeline_optimizado import ejecutar_pipeline_optimizado
            return ejecutar_pipeline_optimizado()
        except Exception as e:
            print(f"❌ Error en pipeline optimizado: {e}")


# Funciones para retrocompatibilidad
def ejecutar_modo_automatico_completo():
    """Función de retrocompatibilidad"""
    modes = ExecutionModes()
    return modes.ejecutar_modo_automatico_completo()

def ejecutar_modo_interactivo():
    """Función de retrocompatibilidad"""
    modes = ExecutionModes()
    return modes.ejecutar_modo_interactivo()

def ejecutar_pipeline_optimizado():
    """Función de retrocompatibilidad"""
    modes = ExecutionModes()
    return modes.ejecutar_pipeline_optimizado()
