
# -------------------------------------------------------------
# load_to_sql.py
# Script para cargar los datos limpios a una base de datos SQL (MySQL).
# Utiliza SQLAlchemy y dotenv para manejo seguro de credenciales.
# -------------------------------------------------------------


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

    # Cargar variables de entorno desde el archivo .env
    load_dotenv('../config/.env')
    # Leer la URL de conexión a la base de datos desde la variable de entorno
    DB_URL = os.getenv('DATABASE_URL')
    if not DB_URL:
        raise ValueError('DATABASE_URL no está definida en el archivo .env')
    # Crear el engine de SQLAlchemy para conectarse a MySQL
    engine = create_engine(DB_URL, echo=False, pool_pre_ping=True)


    # Leer los archivos limpios generados por los scripts de limpieza
    df_muestra = pd.read_csv('../data/cleanData/CLMUESTRA.csv')
    df_estados = pd.read_csv('../data/cleanData/CLESTADOS.csv', parse_dates=['Fecha_Actualizacion'])
    # Cargar los DataFrames a la base de datos como tablas
    df_muestra.to_sql('datos_muestra', engine, if_exists='replace', index=False)
    df_estados.to_sql('datos_cambio_estados', engine, if_exists='replace', index=False)
    print('Datos cargados a SQL.')

if __name__ == '__main__':
    main()
