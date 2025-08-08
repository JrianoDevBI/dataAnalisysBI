"""
Interactive Menu Module

This module contains the interactive menu system for the pipeline,
providing user-friendly navigation and option selection.

Author: Juan Camilo Riaño Molano
Date: 08/08/2025
"""

import os
import sys
from pathlib import Path


class InteractiveMenu:
    """
    Class responsible for managing interactive menus and user input
    """
    
    def __init__(self):
        """Inicializar el menú interactivo"""
        self.menu_options = {
            "main": {
                "title": "🏠 MENÚ PRINCIPAL - PIPELINE INTEGRADO",
                "subtitle": "Sistema de Business Intelligence para Análisis Inmobiliario",
                "options": [
                    ("🚀 Ejecutar Pipeline Unificado (Recomendado)", "pipeline_unificado"),
                    ("⚡ Ejecutar Pipeline Optimizado", "pipeline_optimizado"),
                    ("🛠️ Modo Interactivo (Módulos individuales)", "modo_interactivo"),
                    ("🎯 Modo Automático Completo", "modo_automatico"),
                    ("🔬 Solo Análisis Exploratorio", "solo_analisis"),
                    ("🎨 Solo Diagramas Ejecutivos", "solo_diagramas"),
                    ("📊 Ver Métricas de Performance", "metricas"),
                    ("🔧 Análisis de Optimizaciones", "optimizaciones"),
                    ("❓ Ayuda y Documentación", "ayuda"),
                    ("🚪 Salir", "salir")
                ]
            }
        }
    
    def is_interactive_terminal(self):
        """Verificar si se ejecuta en terminal interactivo"""
        return sys.stdin.isatty() and sys.stdout.isatty()
    
    def get_non_interactive_choice(self):
        """Devolver opción por defecto para ejecución no interactiva"""
        print("🤖 Modo no interactivo detectado. Ejecutando Pipeline Unificado por defecto...")
        return "pipeline_unificado"
    
    def show_header(self):
        """Mostrar encabezado de la aplicación"""
        print("\n" + "=" * 80)
        print("🏢 SISTEMA DE BUSINESS INTELLIGENCE - ANÁLISIS INMOBILIARIO")
        print("=" * 80)
        print("📊 Pipeline Integrado para Procesamiento de Datos Inmobiliarios")
        print("🎯 Versión 4.1 - Pipeline Unificado Optimizado")
        print("👨‍💻 Autor: Juan Camilo Riaño Molano")
        print("📅 Fecha: 08/08/2025")
        print("=" * 80)
    
    def show_main_menu(self):
        """Mostrar menú principal y devolver selección del usuario"""
        # Verificar si se ejecuta en modo no interactivo
        if not self.is_interactive_terminal():
            return self.get_non_interactive_choice()
        
        menu = self.menu_options["main"]
        
        print(f"\n{menu['title']}")
        print("=" * len(menu['title']))
        print(f"📋 {menu['subtitle']}")
        print()
        
        # PARTE 1: Procesamiento del Pipeline
        print("🔧 PARTE 1: Ejecución de Pipeline para procesamiento y limpieza de datos")
        print("=" * 70)
        print(" 1. 🚀 Ejecutar Pipeline Unificado (Recomendado)")
        print(" 2. ⚡ Ejecutar Pipeline Optimizado")
        print()
        
        # PARTE 2: Modo Interactivo
        print("🛠️ PARTE 2: Ejecución de Pipeline de Manera interactiva para validación de errores")
        print("=" * 75)
        print("📝 NOTA: Es importante haber antes ejecutado como mínimo un pipeline (unificado u optimizado)")
        print(" 3. 🛠️ Modo Interactivo (Módulos individuales)")
        print()
        
        # PARTE 3: Proceso Completo
        print("🎯 PARTE 3: Ejecución completa de proceso de pipeline, análisis y métricas clave")
        print("=" * 78)
        print(" 4. 🎯 Modo Automático Completo")
        print()
        
        # PARTE 4: Procesos Individuales
        print("🔬 PARTE 4: Ejecución de procesos individuales")
        print("=" * 50)
        print("📝 NOTA: Es necesario haber ejecutado como mínimo alguno de los dos pipelines")
        print(" 5. 🔬 Solo Análisis Exploratorio")
        print(" 6. 🎨 Solo Diagramas Ejecutivos")
        print(" 7. 📊 Ver Métricas de Performance")
        print(" 8. 🔧 Análisis de Optimizaciones")
        print()
        
        # PARTE 5: Ayuda y Salida
        print("❓ PARTE 5: Ejecución de ayuda y finalización")
        print("=" * 50)
        print(" 9. ❓ Ayuda y Documentación")
        print("10. 🚪 Salir")
        print()
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                selection = input("👉 Seleccione una opción (1-10): ").strip()
                
                if selection.isdigit():
                    option_num = int(selection)
                    if 1 <= option_num <= 10:
                        # Mapear los números mostrados a las opciones correctas del menú
                        option_mapping = {
                            1: "pipeline_unificado",
                            2: "pipeline_optimizado", 
                            3: "modo_interactivo",
                            4: "modo_automatico",
                            5: "solo_analisis",
                            6: "solo_diagramas",
                            7: "metricas",
                            8: "optimizaciones",
                            9: "ayuda",
                            10: "salir"
                        }
                        return option_mapping[option_num]
                
                print("❌ Opción no válida. Por favor, seleccione un número del 1 al 10.")
                retry_count += 1
                
            except (KeyboardInterrupt, EOFError):
                print("\n\n👋 Saliendo del programa...")
                return "salir"
            except Exception as e:
                print(f"❌ Error en la entrada: {e}")
                retry_count += 1
        
        # Si se alcanzó el máximo de reintentos, usar pipeline unificado por defecto
        print("🤖 Demasiados intentos fallidos. Ejecutando Pipeline Unificado por defecto...")
        return "pipeline_unificado"
    
    def show_help(self):
        """Mostrar ayuda y documentación"""
        print("\n" + "=" * 70)
        print("📚 AYUDA Y DOCUMENTACIÓN")
        print("=" * 70)
        
        print("\n🎯 DESCRIPCIÓN DEL SISTEMA:")
        print("Este sistema procesa datos inmobiliarios desde archivos Excel,")
        print("los limpia, analiza y genera reportes ejecutivos.")
        
        print("\n🚀 MODOS DE EJECUCIÓN DISPONIBLES:")
        print()
        print("1️⃣ Pipeline Unificado (RECOMENDADO):")
        print("   • Combina lo mejor del pipeline tradicional y optimizado")
        print("   • Control de calidad + optimizaciones de rendimiento") 
        print("   • Tratamiento avanzado de inconsistencias")
        print("   • Manejo robusto de errores con fallbacks")
        
        print("\n2️⃣ Pipeline Optimizado:")
        print("   • Procesamiento paralelo y cache inteligente")
        print("   • Pool de conexiones SQL optimizado")
        print("   • Reducción del 47-48% en tiempo de ejecución")
        print("   • Métricas de rendimiento en tiempo real")
        
        print("\n3️⃣ Modo Interactivo:")
        print("   • Ejecute módulos individuales según necesidad")
        print("   • Control granular del flujo de procesamiento")
        print("   • Ideal para desarrollo y troubleshooting")
        
        print("\n4️⃣ Modo Automático Completo:")
        print("   • Ejecuta todo el flujo sin intervención")
        print("   • Procesamiento + análisis + indicadores + diagramas")
        print("   • Ideal para ejecuciones programadas")
        
        print("\n📊 ESTRUCTURA DE DATOS:")
        print("   • data/sourceData/ - Archivos Excel originales")
        print("   • data/processedData/ - Datos intermedios procesados")
        print("   • data/cleanData/ - Datos limpios finales")
        print("   • data/query_data/ - Reportes Excel exportados")
        print("   • docs/ - Diagramas y documentación")
        print("   • reports/ - Informes ejecutivos")
        
        print("\n🔧 REQUISITOS TÉCNICOS:")
        print("   • Python 3.11+")
        print("   • Base de datos SQL configurada")
        print("   • Variables de entorno en .env")
        print("   • Memoria RAM recomendada: 8GB+")
        
        print("\n❓ SOLUCIÓN DE PROBLEMAS:")
        print("   • Verifique conectividad SQL antes de ejecutar")
        print("   • Asegúrese que los archivos Excel estén disponibles")
        print("   • Revise logs en caso de errores")
        print("   • Use modo interactivo para debug paso a paso")
        
        input("\n📖 Presione Enter para volver al menú principal...")
    
    def show_optimizations_analysis(self):
        """Mostrar análisis de optimizaciones"""
        print("\n" + "=" * 70)
        print("🔧 ANÁLISIS DE OPTIMIZACIONES IMPLEMENTADAS")
        print("=" * 70)
        
        print("\n📈 MEJORAS DE RENDIMIENTO:")
        print()
        print("1️⃣ Cache Inteligente de DataFrames:")
        print("   • Reducción: 75% en tiempo de lectura repetida")
        print("   • Beneficio: Evita re-cargar datos ya procesados")
        print("   • Impacto: -15-20 segundos en ejecuciones múltiples")
        
        print("\n2️⃣ Procesamiento Paralelo:")
        print("   • Reducción: 46% en tiempo de limpieza")
        print("   • Beneficio: Limpieza simultánea muestra + estados")
        print("   • Impacto: -8-12 segundos en fase de limpieza")
        
        print("\n3️⃣ Pool de Conexiones SQL:")
        print("   • Reducción: 75% en overhead de conexiones")
        print("   • Beneficio: Reutilización eficiente de conexiones")
        print("   • Impacto: -5-8 segundos en operaciones SQL")
        
        print("\n4️⃣ Validaciones Centralizadas:")
        print("   • Reducción: 30% en validaciones redundantes")
        print("   • Beneficio: Validación una sola vez por dataset")
        print("   • Impacto: -3-5 segundos en validaciones")
        
        print("\n🎯 RESULTADOS CONSOLIDADOS:")
        print(f"   • Tiempo tradicional: ~133 segundos")
        print(f"   • Tiempo optimizado: ~70 segundos")
        print(f"   • Mejora total: 47.4% más rápido")
        print(f"   • Ahorro absoluto: 63 segundos")
        
        print("\n💡 RECOMENDACIONES:")
        print("   • Use Pipeline Unificado para mejor balance")
        print("   • Pipeline Optimizado para máximo rendimiento")
        print("   • Modo Interactivo para desarrollo/debug")
        print("   • Monitoree métricas para optimizaciones futuras")
        
        input("\n🔧 Presione Enter para volver al menú principal...")
    
    def confirm_action(self, message: str, default: bool = False) -> bool:
        """
        Confirm an action with the user
        
        Args:
            message: Message to show to user
            default: Default response if user just presses Enter
            
        Returns:
            bool: True if user confirms, False otherwise
        """
        default_text = "Si" if default else "No"
        prompt = f"{message} (Si/No) [{default_text}]: "
        
        while True:
            response = input(prompt).strip().lower()
            
            if not response:  # El usuario solo presionó Enter
                return default
            elif response in ['si', 's', 'sí', 'yes', 'y']:
                return True
            elif response in ['no', 'n']:
                return False
            else:
                print("❌ Por favor responda 'Si' o 'No'")
    
    def get_user_choice(self, options: list, prompt: str = "Seleccione una opción") -> int:
        """
        Get user choice from a list of options
        
        Args:
            options: List of option strings
            prompt: Prompt message
            
        Returns:
            int: Index of selected option (0-based)
        """
        print(f"\n{prompt}:")
        for i, option in enumerate(options, 1):
            print(f"{i}. {option}")
        
        while True:
            try:
                choice = input(f"\nSeleccione (1-{len(options)}): ").strip()
                if choice.isdigit():
                    choice_num = int(choice)
                    if 1 <= choice_num <= len(options):
                        return choice_num - 1
                
                print(f"❌ Opción no válida. Seleccione entre 1 y {len(options)}")
            except KeyboardInterrupt:
                print("\n❌ Operación cancelada por el usuario")
                return -1
            except Exception as e:
                print(f"❌ Error: {e}")
    
    def show_progress_bar(self, current: int, total: int, description: str = ""):
        """
        Show a simple progress bar
        
        Args:
            current: Current progress value
            total: Total value
            description: Optional description
        """
        if total == 0:
            return
        
        percentage = min(100, (current / total) * 100)
        bar_length = 40
        filled_length = int(bar_length * current / total)
        
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        
        print(f"\r{description} [{bar}] {percentage:.1f}% ({current}/{total})", end='', flush=True)
        
        if current >= total:
            print()  # Nueva línea cuando se complete
    
    def wait_for_user(self, message: str = "Presione Enter para continuar..."):
        """Esperar entrada del usuario antes de continuar"""
        try:
            input(f"\n{message}")
        except KeyboardInterrupt:
            print("\n❌ Operación interrumpida")


# Funciones de retrocompatibilidad
def show_main_menu():
    """Mostrar menú principal para retrocompatibilidad"""
    menu = InteractiveMenu()
    menu.show_header()
    return menu.show_main_menu()

def show_help():
    """Mostrar ayuda para retrocompatibilidad"""
    menu = InteractiveMenu()
    return menu.show_help()

def mostrar_analisis_optimizaciones():
    """Mostrar análisis de optimizaciones para retrocompatibilidad"""
    menu = InteractiveMenu()
    return menu.show_optimizations_analysis()
