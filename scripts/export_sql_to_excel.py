# -------------------------------------------------------------
# export_sql_to_excel.py
# Script para ejecutar queries SQL sobre una base de datos MySQL
# y exportar los resultados a archivos Excel con el mismo nombre
# que el query fuente.
#
# Autor: Juan Camilo Riaño Molano
# Fecha de creación: 01/08/2025
# Descripción:
#   Este script permite ejecutar archivos .sql sobre la base de datos
#   definida en la variable de entorno DATABASE_URL y exportar los
#   resultados a archivos Excel (.xlsx) para su análisis o entrega.
#   Es útil para usuarios que no cuentan con MySQL Workbench y desean
#   visualizar los resultados de queries complejos de manera sencilla.
# -------------------------------------------------------------

# =======================
# Importación de librerías
# =======================
import pandas as pd
from sqlalchemy import create_engine
import os

# =======================
# Función principal de exportación
# =======================
def export_query_to_excel(sql_file, db_url):
	"""
	Ejecuta un archivo SQL sobre la base de datos y exporta el resultado a Excel.

	Parámetros:
		sql_file (str): Ruta al archivo .sql con el query a ejecutar.
		db_url (str): URL de conexión a la base de datos (SQLAlchemy).
	Proceso:
		1. Lee el query desde el archivo .sql.
		2. Ejecuta el query usando SQLAlchemy y pandas.
		3. Exporta el resultado a un archivo Excel con el mismo nombre base.

	Buenas prácticas:
	- Valida la existencia de los archivos y la conexión antes de ejecutar.
	- Crea la carpeta de salida si no existe.
	- Maneja errores de lectura y conexión.

	Manejo de errores:
		FileNotFoundError: Si el archivo .sql no existe.
		ValueError: Si la URL de conexión es inválida.
		Exception: Si ocurre un error al ejecutar el query o exportar el archivo.
	"""
	# Leer el query desde el archivo .sql
	if not os.path.exists(sql_file):
		raise FileNotFoundError(f"No se encontró el archivo SQL: {sql_file}")
	with open(sql_file, 'r', encoding='utf-8') as f:
		query = f.read()
	# Crear el engine de SQLAlchemy
	engine = create_engine(db_url)
	# Ejecutar el query y obtener el DataFrame
	df = pd.read_sql(query, engine)
	# Generar ruta y nombre de archivo Excel en data/query_data/
	output_dir = os.path.join(os.path.dirname(os.path.dirname(sql_file)), 'data', 'query_data')
	os.makedirs(output_dir, exist_ok=True)
	excel_file = os.path.join(output_dir, os.path.splitext(os.path.basename(sql_file))[0] + '.xlsx')
	# Exportar a Excel
	df.to_excel(excel_file, index=False)
	print(f'Resultado exportado a: {excel_file}')

# =======================
# Ejecución principal
# =======================
if __name__ == "__main__":
	# Cargar variables de entorno desde .env
	from dotenv import load_dotenv
	load_dotenv()
	# Leer la URL de conexión a la base de datos
	db_url = os.getenv('DATABASE_URL')
	if not db_url:
		raise ValueError('No se encontró la variable DATABASE_URL en el entorno')

	# Exportar los resultados de los queries especificados
	export_query_to_excel('sql_queries/ultimo_estado.sql', db_url)
	export_query_to_excel('sql_queries/diferencia_absoluta_y_ranking.sql', db_url)
