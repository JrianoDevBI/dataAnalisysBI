import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

def probar_conexion_db():
    """
    Prueba la conexión a la base de datos usando la variable DATABASE_URL del archivo .env.
    Devuelve True si la conexión es exitosa, False si falla.
    """
    load_dotenv(os.path.join('config', '.env'))
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        print('DATABASE_URL no está definida en el archivo .env')
        return False
    try:
        from sqlalchemy import text
        engine = create_engine(db_url)
        with engine.connect() as conn:
            conn.execute(text('SELECT 1'))
        print('Conexión a la base de datos exitosa.')
        return True
    except Exception as e:
        print(f'Error al conectar a la base de datos: {e}')
        return False

if __name__ == "__main__":
    resultado = probar_conexion_db()
    if resultado:
        print("Prueba finalizada: conexión exitosa.")
    else:
        print("Prueba finalizada: error de conexión.")
