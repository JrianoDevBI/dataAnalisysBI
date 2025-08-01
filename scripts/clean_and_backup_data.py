# -------------------------------------------------------------
# clean_and_backup_data.py
# Script utilitario para limpiar los datos previos y respaldar la carpeta data.
#
# Autor: Juan Camilo Riaño Molano
# Fecha de creación: 01/08/2025
# Descripción:
#   Este script permite, bajo confirmación del usuario, eliminar los archivos
#   de las carpetas data/cleanData, data/processedData y data/queryData,
#   realizando previamente un backup completo de la carpeta data en dataBackup/data.
#   El backup se sobreescribe cada vez que se ejecuta el proceso.
# -------------------------------------------------------------

# =======================
# Importación de librerías
# =======================
import os
import shutil

# =======================
# Función principal de limpieza y backup
# =======================
def clean_and_backup_data():
	"""
	Solicita confirmación al usuario para eliminar los archivos de las carpetas
	data/cleanData, data/processedData y data/queryData. Si el usuario acepta,
	realiza un backup de la carpeta data en dataBackup/data y elimina los archivos
	de las carpetas indicadas.

	Proceso:
	1. Solicita confirmación al usuario antes de eliminar archivos.
	2. Realiza backup completo de la carpeta data en dataBackup/data (sobrescribe backup anterior).
	3. Elimina archivos y subcarpetas de las carpetas cleanData, processedData y queryData.

	Buenas prácticas:
	- Nunca elimina datos sin respaldo previo.
	- Solicita confirmación explícita al usuario.
	- Maneja errores de acceso a archivos y permisos.

	Manejo de errores:
		FileNotFoundError: Si alguna carpeta no existe, se ignora y continúa.
		Exception: Si ocurre un error al eliminar archivos, se muestra el error y continúa con los demás archivos.
	"""
	base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
	data_dir = os.path.join(base_dir, 'data')
	backup_dir = os.path.join(base_dir, 'dataBackup')
	backup_data_dir = os.path.join(backup_dir, 'data')
	clean_dirs = [
		os.path.join(data_dir, 'cleanData'),
		os.path.join(data_dir, 'processedData'),
		os.path.join(data_dir, 'queryData')
	]

	print("¿Desea eliminar los archivos previos de cleanData, processedData y queryData?")
	print("Se realizará un backup de la carpeta 'data' en 'dataBackup/data' antes de eliminar.")
	confirm = input('Escriba "Si" para eliminar y respaldar, o "No" para continuar sin borrar: ').strip().lower()
	if confirm == 'si':
		# Crear carpeta de backup si no existe
		os.makedirs(backup_dir, exist_ok=True)
		# Eliminar backup anterior si existe
		if os.path.exists(backup_data_dir):
			shutil.rmtree(backup_data_dir)
		# Copiar carpeta data a dataBackup/data
		shutil.copytree(data_dir, backup_data_dir)
		print(f"Backup realizado en: {backup_data_dir}")
		# Eliminar archivos de las carpetas indicadas
		for d in clean_dirs:
			if os.path.exists(d):
				for f in os.listdir(d):
					file_path = os.path.join(d, f)
					try:
						if os.path.isfile(file_path) or os.path.islink(file_path):
							os.unlink(file_path)
						elif os.path.isdir(file_path):
							shutil.rmtree(file_path)
					except Exception as e:
						print(f"No se pudo eliminar {file_path}: {e}")
		print("Archivos previos eliminados correctamente.")
	else:
		print("No se eliminaron archivos. Se continuará con los datos existentes.")
