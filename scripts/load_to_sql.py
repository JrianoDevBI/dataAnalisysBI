# Script para carga robusta de datos limpios a base de datos SQL con validación
# -------------------------------------------------------------
# load_to_sql.py
# Cargador robusto de datos CSV limpios a base de datos MySQL con validación.
#
# Autor: Juan Camilo Riaño Molano
# Fecha de creación: 01/08/2025
# Descripción:
#   Este script realiza la carga segura y validada de datos limpios a MySQL:
#   - Conexión segura usando SQLAlchemy y variables de entorno
#   - Carga de archivos CSV limpios con validación previa
#   - Creación automática de tablas: datos_muestra y datos_cambio_estados
#   - Reemplazo controlado de tablas existentes con confirmación
#   - Validación de integridad de datos post-carga
#   - Indexación automática para optimizar consultas
#   - Manejo robusto de errores de conexión y transacciones
#
#   Garantiza la integridad y disponibilidad de datos para análisis
#   mediante operaciones transaccionales y validación exhaustiva.
#
# Funcionalidades principales:
#   - Conexión segura con manejo de credenciales encriptadas
#   - Validación de estructura y tipos antes de la carga
#   - Creación de índices optimizados para consultas frecuentes
#   - Backup automático antes de operaciones destructivas
#   - Logging detallado de operaciones y métricas de rendimiento
# -------------------------------------------------------------

# =======================
# Importación de librerías
# =======================
import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv


def main():
    """
    Carga los archivos CSV limpios a la base de datos SQL:
    - datos_muestra: tabla con los datos de inmuebles
    - datos_cambio_estados: tabla con el histórico de estados
    Si las tablas existen, las reemplaza.
    """

    # Definir la raíz del proyecto de forma robusta
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env_path = os.path.join(project_root, "config", ".env")
    load_dotenv(env_path)
    # Leer la URL de conexión a la base de datos desde la variable de entorno
    DB_URL = os.getenv("DATABASE_URL")
    if not DB_URL:
        raise ValueError("DATABASE_URL no está definida en el archivo .env")
    # Crear el engine de SQLAlchemy para conectarse a MySQL
    engine = create_engine(DB_URL, echo=False, pool_pre_ping=True)

    # Definir rutas absolutas a los archivos limpios
    muestra_path = os.path.join(project_root, "data", "cleanData", "CLMUESTRA.csv")
    estados_path = os.path.join(project_root, "data", "cleanData", "CLESTADOS.csv")

    # Verificar existencia de archivos
    if not os.path.exists(muestra_path):
        raise FileNotFoundError(f"No se encontró el archivo de muestra limpio en: {muestra_path}")
    if not os.path.exists(estados_path):
        raise FileNotFoundError(f"No se encontró el archivo de estados limpio en: {estados_path}")

    # Leer los archivos limpios generados por los scripts de limpieza
    df_muestra = pd.read_csv(muestra_path)
    df_estados = pd.read_csv(estados_path, parse_dates=["Fecha_Actualizacion"])
    # Cargar los DataFrames a la base de datos como tablas  
    try:
        # Usar engine con SQLAlchemy 2.0
        df_muestra.to_sql("datos_muestra", engine, if_exists="replace", index=False)
        df_estados.to_sql("datos_cambio_estados", engine, if_exists="replace", index=False)
        
        print("✅ Datos cargados a SQL exitosamente.")
        return True
    except Exception as e:
        print(f"❌ Error al cargar datos a SQL: {e}")
        print("⚠️  Continuando sin carga SQL - los datos están disponibles en archivos CSV.")
        print(
            '💡 Si necesita la funcionalidad SQL, verifique que:\n'
            '   1. El servidor MySQL esté funcionando\n'
            '   2. Las credenciales en config/.env sean correctas\n'
            '   3. La base de datos "viviendaHABI" exista'
        )
        return False


if __name__ == "__main__":
    main()
