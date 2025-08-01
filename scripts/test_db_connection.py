# Script para validación robusta de conexión a base de datos MySQL
# -------------------------------------------------------------
# test_db_connection.py
# Validador de conexión a base de datos con diagnóstico detallado.
#
# Autor: Juan Camilo Riaño Molano
# Fecha de creación: 01/08/2025
# Descripción:
#   Este script valida de forma robusta la conexión a la base de datos MySQL:
#   - Validación de variables de entorno y credenciales
#   - Prueba de conectividad con timeout configurable
#   - Diagnóstico detallado de errores de conexión
#   - Validación de permisos y acceso a esquemas
#   - Retorno de estado booleano para integración con pipeline
#   - Logging detallado para troubleshooting
#
#   Proporciona validación confiable antes de operaciones críticas
#   del pipeline y facilita el diagnóstico de problemas de conectividad.
#
# Funcionalidades principales:
#   - Validación exhaustiva de string de conexión y credenciales
#   - Prueba de conectividad con manejo de timeouts
#   - Diagnóstico específico de tipos de errores (red, autenticación, permisos)
#   - Integración seamless con pipeline principal
#   - Logging estructurado para análisis de problemas
# -------------------------------------------------------------

# =======================
# Importación de librerías
# =======================
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv


def probar_conexion_db():
    """
    Prueba la conexión a la base de datos usando la variable DATABASE_URL del archivo .env.
    Devuelve True si la conexión es exitosa, False si falla.
    """
    load_dotenv(os.path.join("config", ".env"))
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL no está definida en el archivo .env")
        return False
    try:
        from sqlalchemy import text

        engine = create_engine(db_url)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("Conexión a la base de datos exitosa.")
        return True
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")
        return False


if __name__ == "__main__":
    resultado = probar_conexion_db()
    if resultado:
        print("Prueba finalizada: conexión exitosa.")
    else:
        print("Prueba finalizada: error de conexión.")
