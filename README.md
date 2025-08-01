# RealEstate_BI_Project

Este proyecto implementa la metodología OSEMN para análisis de datos inmobiliarios, integrando Python, SQL y herramientas de visualización ejecutiva.

## Estructura
- `data/sourceData/`: Datos originales (Excel)
- `data/processedData/`: Datos limpios y logs de outliers
- `notebooks/`: Exploración y visualización
- `scripts/`: Obtención de datos, limpieza y carga a SQL
- `sql_queries/`: Consultas clave para análisis
- `config/`: Variables de entorno y credenciales


## Flujo sugerido
Ahora puedes ejecutar todo el flujo desde un solo punto de entrada:

1. Ejecuta `main.py` y selecciona la opción del proceso que deseas realizar:
	- **1:** Obtener datos desde Excel y generar CSVs limpios (`scripts/obtain_data.py`)
	- **2:** Limpiar datos de muestra (`scripts/clean_muestra.py`)
	- **3:** Limpiar datos de estados (`scripts/clean_estados.py`)
	- **4:** Cargar datos limpios a la base SQL (`scripts/load_to_sql.py`)
2. Usa los notebooks y dashboards para análisis y visualización.

## Requisitos
Instala dependencias con:
```
pip install -r requirements.txt
```

## Configuración de base de datos (MySQL)
1. Crea la base de datos en tu servidor MySQL:
	```sql
	CREATE DATABASE realestate_db CHARACTER SET utf8mb4;
	```
2. Ajusta usuario, contraseña y host en `config/.env`:
	```
	DATABASE_URL=mysql+mysqlconnector://usuario:password@localhost:3306/realestate_db
	```
3. Usa MySQL Workbench para administrar y consultar la base de datos.

## Seguridad
Las credenciales de la base de datos deben ir en `config/.env` (no subir a repositorios públicos).
