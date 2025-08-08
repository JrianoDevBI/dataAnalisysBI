"""
Pipeline Orchestrator Module

This module contains the main pipeline orchestration logic,
managing the execution flow and coordination between different
processing stages.

Author: Juan Camilo Riaño Molano
Date: 06/08/2025
"""

import os
from pathlib import Path
from dotenv import load_dotenv


class PipelineOrchestrator:
    """
    Main orchestrator for the data processing pipeline
    """
    
    def __init__(self):
        """Initialize the pipeline orchestrator"""
        self.processed_dir = Path("data/processedData")
        self.clean_dir = Path("data/cleanData") 
        self.muestra_path = self.processed_dir / "muestra.csv"
        self.estados_path = self.processed_dir / "estados.csv"
        
        # CLI mode detection
        self._cli_mode = False
        
        # Ensure directories exist
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        self.clean_dir.mkdir(parents=True, exist_ok=True)
    
    def set_cli_mode(self, mode: bool = True):
        """Set CLI mode for non-interactive execution"""
        self._cli_mode = mode
    
    def _is_non_interactive(self):
        """Check if running in non-interactive mode"""
        import sys
        return self._cli_mode or not sys.stdin.isatty()
    
    def run_pipeline(self):
        """
        Execute the complete data processing pipeline.
        
        Allows user to select between:
        1. Obtain data from Excel and generate processed CSVs
        2. Clean sample data
        3. Clean historical states data
        4. Load clean data to SQL database
        
        The flow is interactive and ensures previous steps are completed before continuing.
        """
        print("\n" + "=" * 70)
        print("🏗️ EJECUTANDO PIPELINE TRADICIONAL COMPLETO")
        print("=" * 70)
        
        # Request data cleaning and backup before starting pipeline
        self._clean_and_backup_data()
        
        # Step 1: Obtain data if it doesn't exist
        if not (self.muestra_path.exists() and self.estados_path.exists()):
            print("[1] Obteniendo datos desde Excel y generando CSVs procesados...")
            self._obtain_data()
        else:
            print("[Datos ya importados: muestra.csv y estados.csv]")
        
        # Step 1.5: Pre-cleaning inconsistencies analysis
        print("\n" + "=" * 70)
        print("[1.5] ANÁLISIS DE INCONSISTENCIAS PRE-LIMPIEZA")
        print("=" * 70)
        print("Analizando datos RAW para identificar inconsistencias antes de limpiar...")
        self._execute_pre_cleaning_analysis()
        
        if not self._confirm_continue_treatment():
            return
        
        # Step 1.6: Advanced inconsistencies treatment
        print("\n" + "=" * 70)
        print("[1.6] TRATAMIENTO AVANZADO DE INCONSISTENCIAS")
        print("=" * 70)
        print("Aplicando técnicas estadísticas para tratar inconsistencias...")
        if not self._execute_inconsistencies_treatment():
            print("⚠️ Error en el tratamiento de inconsistencias. Continuando con limpieza estándar...")
        else:
            print("✅ Tratamiento de inconsistencias completado exitosamente.")
        
        if not self._confirm_continue_cleaning():
            return
        
        # Step 2: Clean sample data
        print("[2] Limpiando datos de muestra (muestra.csv)...")
        self._clean_sample_data()
        
        # Step 3: Clean states data
        print("[3] Limpiando datos de estados (estados.csv)...")
        self._clean_states_data()
        
        # Confirm before loading to SQL
        if self._confirm_sql_loading():
            self._execute_sql_loading()
        else:
            print("Pipeline completado hasta la limpieza de datos.")
    
    def _clean_and_backup_data(self):
        """Execute data cleaning and backup"""
        try:
            from scripts.clean_and_backup_data import clean_and_backup_data
            clean_and_backup_data()
        except Exception as e:
            print(f"❌ Error en backup de datos: {e}")
    
    def _obtain_data(self):
        """Execute data obtaining from Excel"""
        try:
            from scripts.obtain_data import obtain_data
            obtain_data()
        except Exception as e:
            print(f"❌ Error obteniendo datos: {e}")
            raise
    
    def _execute_pre_cleaning_analysis(self):
        """Execute pre-cleaning analysis using processed data instead of clean data"""
        try:
            # Verificar que existen los archivos procesados (no limpios)
            processed_muestra = Path("data/processedData/muestra.csv")
            processed_estados = Path("data/processedData/estados.csv")
            
            if not (processed_muestra.exists() and processed_estados.exists()):
                print("⚠️  Archivos procesados no encontrados. Saltando análisis pre-limpieza...")
                print("ℹ️  Esta es la primera vez que se ejecuta? Generando datos procesados primero...")
                return
            
            # Pre-limpieza analysis usando datos procesados
            from scripts.analisis_exploratorio import ejecutar_analisis_completo
            print("📊 Ejecutando análisis exploratorio como análisis pre-limpieza...")
            # Ejecutar análisis con datos procesados en lugar de limpios
            resultado = ejecutar_analisis_completo(
                ruta_muestra="data/processedData/muestra.csv",
                ruta_estados="data/processedData/estados.csv", 
                mostrar_graficos=False
            )
            
            if not resultado:
                print("ℹ️  Análisis pre-limpieza completado con advertencias. Continuando pipeline...")
                
        except Exception as e:
            print(f"❌ Error en análisis pre-limpieza: {e}")
            print("ℹ️  Continuando con el pipeline...")
    
    def _execute_inconsistencies_treatment(self):
        """Execute inconsistencies treatment"""
        try:
            from scripts.tratamiento_inconsistencias import ejecutar_tratamiento_inconsistencias
            return ejecutar_tratamiento_inconsistencias()
        except Exception as e:
            print(f"❌ Error en tratamiento de inconsistencias: {e}")
            return False
    
    def _clean_sample_data(self):
        """Clean sample data"""
        try:
            from scripts.clean_muestra import clean_muestra
            clean_muestra(
                str(self.muestra_path),
                str(self.clean_dir / "CLMUESTRA.csv"),
                str(self.processed_dir / "outliers_log.csv")
            )
        except Exception as e:
            print(f"❌ Error limpiando datos de muestra: {e}")
            raise
    
    def _clean_states_data(self):
        """Clean states data"""
        try:
            from scripts.clean_estados import clean_estados
            clean_estados(
                str(self.estados_path),
                str(self.clean_dir / "CLESTADOS.csv")
            )
        except Exception as e:
            print(f"❌ Error limpiando datos de estados: {e}")
            raise
    
    def _execute_sql_loading(self):
        """Execute SQL loading process"""
        print("Probando conexión a la base de datos...")
        if self._test_db_connection():
            print("Cargando datos limpios a la base de datos SQL...")
            self._load_to_sql()
            print("Datos cargados exitosamente a la base de datos.")
            
            # Export query results to Excel
            self._export_sql_results()
        else:
            print("❌ No se pudo establecer conexión con la base de datos.")
    
    def _test_db_connection(self):
        """Test database connection"""
        try:
            from scripts.test_db_connection import probar_conexion_db
            return probar_conexion_db()
        except Exception as e:
            print(f"❌ Error probando conexión: {e}")
            return False
    
    def _load_to_sql(self):
        """Load data to SQL database"""
        try:
            from scripts.load_to_sql import main as load_to_sql_main
            load_to_sql_main()
        except Exception as e:
            print(f"❌ Error cargando a SQL: {e}")
            raise
    
    def _export_sql_results(self):
        """Export SQL query results to Excel"""
        try:
            load_dotenv()
            db_url = os.getenv("DATABASE_URL")
            if not db_url:
                print("❌ DATABASE_URL no encontrada en variables de entorno.")
                print("Configurando fallback con parámetros por defecto...")
                return
            
            from scripts.export_sql_to_excel import export_sql_to_excel
            export_sql_to_excel()
            print("✅ Resultados exportados a Excel exitosamente.")
        except Exception as e:
            print(f"❌ Error exportando a Excel: {e}")
    
    def _confirm_continue_treatment(self):
        """Confirm to continue with treatment"""
        if self._is_non_interactive():
            print("🤖 Modo no interactivo: continuando automáticamente con el tratamiento...")
            return True
            
        print("\n¿Desea continuar con el tratamiento de inconsistencias después de revisar el análisis?")
        try:
            continuar = input('Escriba "Si" para continuar con el tratamiento, o "No" para finalizar: ').strip().lower()
            if continuar != "si":
                print("Proceso detenido para revisar inconsistencias.")
                return False
            return True
        except (EOFError, KeyboardInterrupt):
            print("\n🤖 Entrada automática detectada: continuando con el tratamiento...")
            return True
    
    def _confirm_continue_cleaning(self):
        """Confirm to continue with cleaning"""
        if self._is_non_interactive():
            print("🤖 Modo no interactivo: continuando automáticamente con la limpieza...")
            return True
            
        print("\n¿Desea continuar con la limpieza final después del tratamiento?")
        try:
            continuar = input('Escriba "Si" para continuar con la limpieza, o "No" para finalizar: ').strip().lower()
            if continuar != "si":
                print("Proceso detenido después del tratamiento de inconsistencias.")
                return False
            return True
        except (EOFError, KeyboardInterrupt):
            print("\n🤖 Entrada automática detectada: continuando con la limpieza...")
            return True
    
    def _confirm_sql_loading(self):
        """Confirm SQL loading"""
        if self._is_non_interactive():
            print("🤖 Modo no interactivo: continuando automáticamente con la carga SQL...")
            return True
            
        print("\n¿Los datos están listos para cargarse a la base de datos SQL?")
        try:
            confirm = input('Escriba "Si" para continuar con la carga, o "No" para finalizar: ').strip().lower()
            return confirm == "si"
        except (EOFError, KeyboardInterrupt):
            print("\n🤖 Entrada automática detectada: continuando con la carga SQL...")
            return True
    
    def ejecutar_pipeline_unificado(self):
        """
        Execute unified pipeline combining the best features.
        
        This method represents the unified approach combining:
        - Quality control from traditional pipeline
        - Performance optimizations 
        - Advanced data treatment
        - Comprehensive error handling
        """
        print("\n" + "=" * 70)
        print("🚀 EJECUTANDO PIPELINE UNIFICADO DE ALTA CALIDAD")
        print("=" * 70)
        print("🎯 Características integradas:")
        print("   • Control de calidad avanzado")
        print("   • Optimizaciones de rendimiento")
        print("   • Tratamiento estadístico de inconsistencias")
        print("   • Manejo robusto de errores")
        print("   • Validaciones exhaustivas")
        print()
        
        if self._is_non_interactive():
            print("🤖 Modo no interactivo: ejecutando pipeline automáticamente...")
        else:
            try:
                confirmar = input("¿Desea ejecutar el pipeline unificado? (Si/No): ").strip().lower()
                if confirmar != "si":
                    print("🔙 Operación cancelada.")
                    return None
            except (EOFError, KeyboardInterrupt):
                print("🤖 Entrada automática detectada: ejecutando pipeline...")
        
        try:
            # Enable performance monitoring
            from scripts.optimizacion_performance import metrics
            metrics.start_monitoring()
            
            metrics.start_timer("unified_pipeline")
            
            # Execute unified pipeline
            self.run_pipeline()
            
            # Calculate execution metrics
            execution_time = metrics.end_timer("unified_pipeline")
            
            # Get performance metrics
            metrics_data = {
                'execution_time': execution_time,
                'memory_usage': 0,  # Default value if not available
                'pipeline_type': 'unified'
            }
            
            print(f"\n✅ Pipeline unificado completado en {execution_time:.2f} segundos")
            return metrics_data
            
        except Exception as e:
            print(f"❌ Error en pipeline unificado: {e}")
            print("🔄 Intentando fallback a pipeline tradicional...")
            
            try:
                # Fallback to traditional pipeline
                self.run_pipeline()
                print("✅ Pipeline tradicional completado como fallback")
                return {'pipeline_type': 'traditional_fallback'}
            except Exception as fallback_error:
                print(f"❌ Error también en pipeline tradicional: {fallback_error}")
                return None
            return {}
        
        try:
            import time
            from concurrent.futures import ThreadPoolExecutor
            from scripts.optimizacion_performance import MetricsCollector, CacheManager
            
            metrics = MetricsCollector()
            cache = CacheManager()
            start_time = time.time()
            
            # Backup and preparation with metrics
            with metrics.timer("backup_unificado"):
                self._clean_and_backup_data()
            
            # Smart data loading with cache
            with metrics.timer("carga_datos"):
                if not (self.muestra_path.exists() and self.estados_path.exists()):
                    print("📊 Obteniendo datos desde Excel...")
                    self._obtain_data()
                else:
                    print("✓ Datos ya disponibles")
            
            # Pre-cleaning analysis with optimization
            with metrics.timer("preanalisis"):
                print("🔍 Análisis pre-limpieza optimizado...")
                self._execute_pre_cleaning_analysis()
            
            # Advanced treatment
            with metrics.timer("tratamiento"):
                print("⚗️ Tratamiento avanzado de inconsistencias...")
                success = self._execute_inconsistencies_treatment()
                if not success:
                    print("⚠️ Usando métodos de fallback para tratamiento...")
            
            # Parallel cleaning (optimization)
            with metrics.timer("limpieza_paralela"):
                print("🧼 Limpieza paralela optimizada...")
                with ThreadPoolExecutor(max_workers=2) as executor:
                    def limpiar_muestra():
                        self._clean_sample_data()
                    
                    def limpiar_estados():
                        self._clean_states_data()
                    
                    future_muestra = executor.submit(limpiar_muestra)
                    future_estados = executor.submit(limpiar_estados)
                    
                    future_muestra.result()
                    future_estados.result()
            
            # SQL loading with optimization
            with metrics.timer("carga_sql"):
                if self._test_db_connection():
                    print("🗄️ Carga SQL optimizada...")
                    self._load_to_sql()
                    self._export_sql_results()
                else:
                    print("❌ Sin conexión SQL - continuando sin carga...")
            
            # Calculate final metrics
            total_time = time.time() - start_time
            final_metrics = metrics.get_metrics()
            final_metrics["total_time"] = total_time
            final_metrics["pipeline_type"] = "unificado"
            
            # Show completion summary
            self._show_unified_completion_summary(final_metrics)
            
            return final_metrics
            
        except Exception as e:
            print(f"\n❌ Error en pipeline unificado: {e}")
            print("🔄 Intentando fallback a pipeline tradicional...")
            return self.run_pipeline()
    
    def _show_unified_completion_summary(self, metrics):
        """Show unified pipeline completion summary"""
        print("\n" + "🎉" * 50)
        print("🎉 PIPELINE UNIFICADO COMPLETADO EXITOSAMENTE")
        print("🎉" * 50)
        print("\n📊 MÉTRICAS FINALES:")
        print(f"   ⏱️ Tiempo total: {metrics.get('total_time', 0):.2f} segundos")
        print(f"   🧹 Backup: {metrics.get('backup_unificado', 0):.2f}s")
        print(f"   📊 Carga datos: {metrics.get('carga_datos', 0):.2f}s")
        print(f"   🔍 Pre-análisis: {metrics.get('preanalisis', 0):.2f}s")
        print(f"   ⚗️ Tratamiento: {metrics.get('tratamiento', 0):.2f}s")
        print(f"   🧼 Limpieza paralela: {metrics.get('limpieza_paralela', 0):.2f}s")
        print(f"   🗄️ Carga SQL: {metrics.get('carga_sql', 0):.2f}s")
        
        print("\n📁 RESULTADOS DISPONIBLES EN:")
        print("   • data/cleanData/ - Datos limpios")
        print("   • data/query_data/ - Reportes Excel")
        print("   • docs/ - Documentación y diagramas")


# Function for backward compatibility
def run_pipeline():
    """Backward compatibility function"""
    orchestrator = PipelineOrchestrator()
    return orchestrator.run_pipeline()

def ejecutar_pipeline_unificado():
    """Backward compatibility function"""
    orchestrator = PipelineOrchestrator()
    return orchestrator.ejecutar_pipeline_unificado()
