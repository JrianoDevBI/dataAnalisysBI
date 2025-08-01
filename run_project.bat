@echo off
REM Activar el entorno virtual y ejecutar main.py usando el entorno de desarrollo
cd /d %~dp0
call venv\Scripts\activate.bat
python main.py
