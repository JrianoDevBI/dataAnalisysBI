# RealEstate_BI_Project

Este proyecto implementa la metodología OSEMN para análisis de datos inmobiliarios, integrando Python, SQL y herramientas de visualización ejecutiva. Incluye análisis pre-limpieza, validación automática de datos y generación de reportes ejecutivos.

## Estructura del Proyecto

- `data/sourceData/`: Datos originales (Excel)
- `data/processedData/`: Datos limpios y logs de outliers
- `data/cleanData/`: Datos limpios finales para análisis
- `data/query_data/`: Resultados de queries y análisis exportados a Excel
- `dataBackup/`: Respaldos automáticos de datos con timestamp
- `scripts/`: Obtención de datos, limpieza, carga a SQL y análisis exploratorio
  - `analisis_pre_limpieza.py`: **NUEVO** - Análisis de inconsistencias antes de limpieza
  - `tratamiento_inconsistencias.py`: **NUEVO** - Tratamiento estadístico avanzado de inconsistencias
  - `clean_muestra.py`: **MEJORADO** - Validación y corrección automática de datos
  - `analisis_exploratorio.py`: **MEJORADO** - Indicadores clave y correlaciones
  - `generar_informe_ejecutivo.py`: **MEJORADO** - Reportes PDF/Word con manejo de errores
- `sql_queries/`: Consultas clave para análisis
- `config/`: Variables de entorno y credenciales
- `reports/`: **NUEVO** - Informes ejecutivos generados (PDF, Word, LaTeX)
- `run_project.bat`: Script para ejecutar todo el pipeline automáticamente

## Nuevas Características Implementadas

### 🔍 Análisis Pre-Limpieza

- **Detección automática de inconsistencias** antes de procesar los datos
- **Análisis de valores faltantes, duplicados y outliers**
- **Validación de rangos** para áreas (20-1000 m²) y precios
- **Verificación de consistencia** en categorías y formatos
- **Reporte detallado** de problemas encontrados

### 🛠️ Limpieza Inteligente de Datos

- **Corrección automática** de errores comunes
- **Normalización** de tipos de inmuebles y ciudades
- **Validación de estratos** (1-6) con corrección automática
- **Manejo inteligente** de pisos y áreas fuera de rango
- **Backup automático** antes de realizar cambios

### 📈 Tratamiento Avanzado de Inconsistencias

- **Eliminación inteligente de duplicados** con múltiples criterios y umbrales de similitud
- **Imputación de precios faltantes por mediana zonal** para preservar patrones geográficos
- **Winsorización de outliers al 1%** para manejo robusto sin distorsionar distribuciones
- **Validación de mejoras en calidad** con métricas before/after automatizadas
- **Backup específico y logging detallado** de todas las transformaciones aplicadas
- **Integración seamless** con el pipeline sin afectar el flujo existente

### 📊 Análisis Exploratorio Mejorado

- **Indicadores clave de negocio** calculados automáticamente:
  - Precio promedio por m²
  - Tasa de confianza de precios
  - Detección de outliers
  - Distribución por estratos y tipos
  - Análisis temporal de datos
- **Correlación Estrato vs Precio** con visualización
- **Detección de inconsistencias específicas**
- **Resultados mostrados por consola** para revisión inmediata

### 📄 Generación de Reportes Ejecutivos

- **Informes PDF** usando LaTeX con manejo robusto de errores
- **Fallback a archivos .tex** si LaTeX no está disponible
- **Informes Word** como alternativa
- **Gráficos integrados** y análisis estadísticos

## Ejecución rápida del pipeline

1. **Instala las dependencias:**

   ```bash
   pip install -r requirements.txt
   ```

2. **Configura la base de datos (MySQL):**

   - Crea la base de datos en tu servidor MySQL:

     ```sql
     CREATE DATABASE realestate_db CHARACTER SET utf8mb4;
     ```

   - Ajusta usuario, contraseña y host en `config/.env`:

     ```env
     DATABASE_URL=mysql+mysqlconnector://usuario:password@localhost:3306/realestate_db
     ```

3. **Ejecuta el pipeline completo:**

   - Haz doble clic en `run_project.bat` o ejecútalo desde la terminal:

     ```bash
     run_project.bat
     ```

   - El script activará el entorno virtual, ejecutará `main.py` y te guiará paso a paso:
     - **Análisis pre-limpieza**: Detecta inconsistencias antes de procesar
     - **Tratamiento avanzado de inconsistencias**: Aplica técnicas estadísticas (duplicados, imputación zonal, winsorización)
     - **Limpieza inteligente y backup** de datos con corrección automática
     - **Obtención y procesamiento** de datos
     - **Limpieza mejorada** de muestra y estados con validación
     - **Carga a base de datos SQL** con verificación
     - **Exportación de resultados** y análisis a Excel
     - **Análisis exploratorio automático** con indicadores clave
     - **Opción de generar informe ejecutivo** (PDF/Word)

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

```bash
flake8 .
```

**Para formatear el código automáticamente con black:**

```bash
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
