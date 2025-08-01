# Script para cargar los archivos CSV limpios a la base de datos SQL definida en config/.env.
"""
-------------------------------------------------------------
load_to_sql.py
Script para cargar los datos limpios a una base de datos SQL (MySQL).

Autor: Juan Camilo Riaño Molano
Fecha: 01/08/2025

Descripción:
    Utiliza SQLAlchemy y dotenv para manejo seguro de credenciales.
    Carga los archivos CSV limpios a la base de datos SQL:
        - datos_muestra: tabla con los datos de inmuebles
        - datos_cambio_estados: tabla con el histórico de estados
    Si las tablas existen, las reemplaza.

Buenas prácticas:
    - Uso de variables de entorno para credenciales.
    - Validación de existencia de archivos antes de cargar.
    - Manejo de errores y mensajes claros para el usuario.
-------------------------------------------------------------
"""


def main():
    """
    Carga los archivos CSV limpios a la base de datos SQL:
    - datos_muestra: tabla con los datos de inmuebles
    - datos_cambio_estados: tabla con el histórico de estados
    Si las tablas existen, las reemplaza.
    """
    from dotenv import load_dotenv
    import os
    from sqlalchemy import create_engine
    import pandas as pd

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
        df_muestra.to_sql("datos_muestra", engine, if_exists="replace", index=False)
        df_estados.to_sql("datos_cambio_estados", engine, if_exists="replace", index=False)
        print("Datos cargados a SQL.")
    except AttributeError as e:
        print("Error al cargar datos a SQL:", e)
        print(
            'Si el error menciona "cursor" o "DBAPI2", pruebe instalar el paquete pymysql '
            'y/o use el dialecto mysql+pymysql en su DATABASE_URL.'
        )
        print("Ejecute: pip install pymysql")
        raise


if __name__ == "__main__":
    main()
