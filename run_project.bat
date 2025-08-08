@echo off
REM ============================================================================
REM Pipeline Integrado para Análisis de Datos Inmobiliarios - Launcher Script
REM ============================================================================
REM
REM Script mejorado para ejecutar el pipeline con opciones de línea de comandos
REM
REM Autor: Juan Camilo Riaño Molano
REM Fecha: 08/08/2025
REM Version: 4.1 - Pipeline Unificado Optimizado
REM ============================================================================

setlocal enabledelayedexpansion

REM Cambiar al directorio del script
cd /d %~dp0

REM Mostrar banner
echo.
echo ============================================================================
echo                    PIPELINE DE ANALISIS INMOBILIARIO
echo ============================================================================
echo  Version 4.1 - Pipeline Unificado Optimizado
echo  Autor: Juan Camilo Riaño Molano
echo  Fecha: 08/08/2025
echo ============================================================================
echo.

REM Verificar si el virtual environment existe
if not exist "venv\Scripts\python.exe" (
    echo [ERROR] Virtual environment no encontrado en venv\
    echo Por favor, ejecute: python -m venv venv
    pause
    exit /b 1
)

REM Mostrar menú de opciones
echo Seleccione el modo de ejecucion:
echo.
echo  1. Pipeline Unificado (Recomendado)
echo  2. Pipeline Optimizado  
echo  3. Modo Automatico Completo
echo  4. Solo Analisis Exploratorio
echo  5. Solo Diagramas Ejecutivos
echo  6. Modo Interactivo (Menu completo)
echo  7. Ayuda
echo  8. Salir
echo.

set /p choice="Ingrese su opcion (1-8): "

if "%choice%"=="1" (
    echo [INFO] Ejecutando Pipeline Unificado...
    venv\Scripts\python.exe main.py --unified
) else if "%choice%"=="2" (
    echo [INFO] Ejecutando Pipeline Optimizado...
    venv\Scripts\python.exe main.py --optimized  
) else if "%choice%"=="3" (
    echo [INFO] Ejecutando Modo Automatico...
    venv\Scripts\python.exe main.py --automatic
) else if "%choice%"=="4" (
    echo [INFO] Ejecutando Solo Analisis...
    venv\Scripts\python.exe main.py --analysis-only
) else if "%choice%"=="5" (
    echo [INFO] Ejecutando Solo Diagramas...
    venv\Scripts\python.exe main.py --diagrams-only
) else if "%choice%"=="6" (
    echo [INFO] Iniciando Modo Interactivo...
    venv\Scripts\python.exe main.py
) else if "%choice%"=="7" (
    echo [INFO] Mostrando ayuda...
    venv\Scripts\python.exe main.py --help
) else if "%choice%"=="8" (
    echo [INFO] Saliendo...
    goto :eof
) else (
    echo [ERROR] Opcion invalida. Ejecutando Pipeline Unificado por defecto...
    venv\Scripts\python.exe main.py --unified
)

echo.
echo ============================================================================
echo EJECUCION COMPLETADA
echo ============================================================================
echo.
pause
