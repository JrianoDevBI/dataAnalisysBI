
# RealEstate_BI_Project

Este proyecto implementa la metodología OSEMN para análisis de datos inmobiliarios, integrando Python, SQL y herramientas de visualización ejecutiva.

## Estructura
- `data/sourceData/`: Datos originales (Excel)
- `data/processedData/`: Datos limpios y logs de outliers
- `data/cleanData/`: Datos limpios finales para análisis
- `data/query_data/`: Resultados de queries y análisis exportados a Excel
- `scripts/`: Obtención de datos, limpieza, carga a SQL y análisis exploratorio
- `sql_queries/`: Consultas clave para análisis
- `config/`: Variables de entorno y credenciales
- `run_project.bat`: Script para ejecutar todo el pipeline automáticamente

## Ejecución rápida del pipeline

1. **Instala las dependencias:**
	 ```
	 pip install -r requirements.txt
	 ```

2. **Configura la base de datos (MySQL):**
	 - Crea la base de datos en tu servidor MySQL:
		 ```sql
		 CREATE DATABASE realestate_db CHARACTER SET utf8mb4;
		 ```
	 - Ajusta usuario, contraseña y host en `config/.env`:
		 ```
		 DATABASE_URL=mysql+mysqlconnector://usuario:password@localhost:3306/realestate_db
		 ```

3. **Ejecuta el pipeline completo:**
	 - Haz doble clic en `run_project.bat` o ejecútalo desde la terminal:
		 ```
		 run_project.bat
		 ```
	 - El script activará el entorno virtual, ejecutará `main.py` y te guiará paso a paso:
		 - Limpieza y backup de datos
		 - Obtención y procesamiento de datos
		 - Limpieza de muestra y estados
		 - Carga a base de datos SQL
		 - Exportación de resultados y análisis a Excel
		 - Opción de ejecutar análisis exploratorio interactivo (`main_analysis.py`)

4. **Análisis exploratorio (opcional):**
	 - Al finalizar el pipeline, puedes elegir realizar un análisis exploratorio interactivo de los datos limpios.
	 - El sistema ejecutará automáticamente `main_analysis.py` usando el entorno virtual, permitiendo visualizar correlaciones, outliers y relaciones clave.

## Notas importantes
- El pipeline es completamente automatizado y modular.
- Todos los scripts usan el entorno virtual para evitar conflictos de dependencias.
- El archivo `run_project.bat` garantiza que siempre se use el entorno correcto.
- Los resultados de queries y análisis se exportan automáticamente a la carpeta `data/query_data/`.
- El análisis exploratorio requiere tener instalados los paquetes `seaborn` y `plotly` (ya incluidos en requirements.txt si es necesario).

## Seguridad
Las credenciales de la base de datos deben ir en `config/.env` (no subir a repositorios públicos).

## Integración continua y calidad de código

Este proyecto incluye integración continua (CI) con **GitHub Actions** y herramientas de calidad de código (linters):

- **GitHub Actions:** Ejecuta automáticamente los linters y verifica el formato del código en cada push o pull request.
- **flake8:** Linter para detectar errores de estilo y código en Python.
- **black:** Formateador automático de código Python.

### Uso de linters y formateadores

**Para ejecutar flake8 manualmente:**

```
flake8 .
```

**Para formatear el código automáticamente con black:**

```
black .
```

**Configuración:**
- Las reglas de flake8 están en el archivo `.flake8`.
- La configuración de black está en `pyproject.toml`.

**Recomendación:**
Antes de hacer commit, ejecuta ambos comandos para asegurar que el código cumple con los estándares del proyecto.

### CI/CD

Cada push o pull request activa automáticamente el workflow de GitHub Actions, que:
- Instala dependencias.
- Ejecuta flake8 y black.
- Falla si hay errores de estilo o formato.

Puedes ver la configuración en `.github/workflows/python-lint.yml`.
