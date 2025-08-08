# 🏗️ Pipeline de Análisis de Datos Inmobiliarios

**Autor:** Juan Camilo Riaño Molano  
**Fecha:** 08 de Agosto de 2025  
**Versión:** 4.2 - Pipeline Unificado Optimizado + UX Mejorada  
**Repositorio:** dataAnalisysBI/Prueba1  

---

## 📋 RESUMEN EJECUTIVO

Sistema de Business Intelligence profesional para análisis de datos inmobiliarios con pipeline unificado optimizado. Procesa ~52K registros con 99.7% de confiabilidad y 47% de mejora en eficiencia.

### 🎯 Indicadores Clave Alcanzados
- **52,353 registros** procesados exitosamente
- **99.7% confiabilidad** de datos (vs. 94.18% inicial)
- **47% optimización** en tiempo total de procesamiento
- **78-108 segundos** tiempo total (vs. 147-203 seg. anterior)
- **484-671 registros/segundo** throughput promedio

---

## 🏢 ARQUITECTURA DEL PROYECTO

```
pruebaHabiBI/
├── 📁 main.py                 # Orquestador principal unificado
├── 📁 scripts/                # Módulos especializados del pipeline
│   ├── obtain_data.py         # Extracción de datos Excel → CSV
│   ├── clean_and_backup_data.py  # Backup inteligente y preparación
│   ├── tratamiento_inconsistencias.py  # Tratamiento estadístico avanzado
│   ├── clean_muestra.py       # Limpieza optimizada de muestra
│   ├── clean_estados.py       # Limpieza optimizada de estados
│   ├── load_to_sql.py         # Carga optimizada a base de datos
│   ├── export_sql_to_excel.py # Exportación de reportes Excel
│   ├── analisis_exploratorio.py  # Análisis exploratorio completo
│   ├── test_db_connection.py  # Validación de conexión DB
│   ├── pipeline_optimizado.py # Optimizaciones de performance
│   └── optimizacion_performance.py  # Métricas y cache avanzado
├── 📁 data/                   # Datos del proyecto
│   ├── sourceData/           # Datos fuente (Excel)
│   ├── cleanData/           # Datos limpios (CSV)
│   ├── processedData/       # Datos procesados
│   └── query_data/          # Resultados de consultas (Excel)
├── 📁 sql_queries/           # Consultas SQL especializadas
├── 📁 docs/                  # Documentación y diagramas
│   ├── assets/              # Recursos CSS/JS
│   ├── diagrama_flujo_interactivo.html  # Diagrama interactivo
│   ├── reporte_ejecutivo_completo.md    # Reporte ejecutivo
│   ├── GLOSARIO_TECNICO.md  # Glosario completo (295+ términos)
│   └── errores_solucionados.md         # Log de errores resueltos
├── 📁 reports/              # Reportes generados e informes
├── 📁 utils/                # Utilidades del sistema (menú, métricas)
├── 📁 core/                 # Módulos centrales del pipeline
└── 📁 dataBackup/           # Backups automáticos
```

---

## 🔄 PIPELINE UNIFICADO OPTIMIZADO

### **FASE 1: 📁 Entrada de Datos (5-8 seg)**
- **Script:** `obtain_data.py`
- **Función:** Extracción y conversión Excel → CSV
- **Output:** `muestra.csv` + `estados.csv` (~52,353 registros)
- **Eficiencia:** 6,544-10,471 registros/segundo

### **FASE 2: 🧹 Preparación de Datos (2-3 seg)**
- **Script:** `clean_and_backup_data.py`
- **Función:** Backup inteligente y inicialización
- **Output:** Cache optimizado y entorno preparado
- **Eficiencia:** 92% cache hit rate

### **FASE 3: 🔍 Análisis Exploratorio (8-12 seg)**
- **Script:** `analisis_exploratorio.py`
- **Función:** Análisis estadístico descriptivo y exploratorio
- **Output:** Correlaciones, outliers y visualizaciones
- **Eficiencia:** 4,363-6,544 registros/segundo

### **FASE 4: ⚗️ Tratamiento Estadístico (10-15 seg)**
- **Script:** `tratamiento_inconsistencias.py`
- **Función:** Deduplicación + Imputación + Winsorización
- **Output:** 349 outliers controlados, calidad mejorada
- **Eficiencia:** Tratamiento estadístico avanzado

### **FASE 5: 🧼 Limpieza Paralela (18-25 seg)**
- **Scripts:** `clean_muestra.py` + `clean_estados.py`
- **Función:** Procesamiento paralelo con ThreadPool
- **Output:** `CLMUESTRA.csv` + `CLESTADOS.csv` (99.7% calidad)
- **Eficiencia:** Paralelización + fallback automático

### **FASE 6: 🗄️ Base de Datos (20-30 seg)**
- **Script:** `load_to_sql.py`
- **Función:** Carga optimizada con pool de conexiones
- **Output:** Datos cargados + índices optimizados
- **Eficiencia:** 1,745-2,618 registros/segundo

### **FASE 7: 📊 Reportería y Análisis (15-25 seg)**
- **Scripts:** `export_sql_to_excel.py` + `analisis_exploratorio.py`
- **Función:** Generación de reportes + análisis exploratorio
- **Output:** 3 reportes Excel + visualizaciones interactivas
- **Eficiencia:** Múltiples reportes en batch

---

## 🚀 INSTRUCCIONES DE USO

### **Ejecución Principal**
```bash
python main.py
```

### **Menú Interactivo**
```text
==========================================
🏗️ PIPELINE DE ANÁLISIS DE DATOS INMOBILIARIOS 🏗️
==========================================

⚠️ MENÚ REORGANIZADO PARA MAYOR CLARIDAD ⚠️

Seleccione una opción:

1. 🔄 Pipeline Completo Automático (Recomendado)
   └── Ejecuta todo el proceso sin intervención

2. 🔍 Análisis Exploratorio únicamente
   └── Análisis estadístico y reportes (datos ya procesados)

3. 🧹 Limpieza y Procesamiento únicamente
   └── Solo ETL sin análisis ni reportes

4. 🔧 Modo Interactivo Avanzado
   └── Selección manual de módulos individuales

5. 🎯 Salir

Ingrese su opción [1-5]: 
```

### **Ejecución Automatizada (Recomendada)**
- Seleccione **opción 1** para ejecutar todo el pipeline optimizado
- Tiempo total: **78-108 segundos**
- Sin intervención manual requerida
- Genera todos los reportes automáticamente

### **Modo Interactivo (Avanzado)**
- Seleccione **opción 2** para control granular
- Permite ejecutar fases específicas
- Útil para debugging o análisis específicos

---

## 📁 OUTPUTS GENERADOS

### **Datos Procesados**
- `data/cleanData/CLMUESTRA.csv` - Datos de muestra limpios
- `data/cleanData/CLESTADOS.csv` - Datos de estados limpios

### **Reportes Excel**
- `data/query_data/ultimo_estado.xlsx` - Estado actual de propiedades
- `data/query_data/diferencia_absoluta_y_ranking.xlsx` - Análisis comparativo
- `data/query_data/estado_analysis.xlsx` - Análisis detallado por estado

### **Análisis y Visualizaciones**
- `reports/grafico1.png` - Gráficos de correlación
- `reports/grafico2.html` - Visualizaciones interactivas Plotly
- `reports/Informe_Ejecutivo_BI_Jr.pdf` - Informe profesional LaTeX

### **Documentación**
- `docs/diagrama_flujo_interactivo.html` - Diagrama interactivo del pipeline
- `docs/reporte_ejecutivo_completo.md` - Reporte ejecutivo completo
- `docs/GLOSARIO_TECNICO.md` - **NUEVO:** Glosario completo con 295+ términos técnicos
- `docs/errores_solucionados.md` - Log detallado de errores resueltos

---

## 🆕 NUEVAS FUNCIONALIDADES V4.2

### **🎯 Mejoras de UX y Claridad**
- **Menú reorganizado** con opciones más claras y específicas
- **Clarificaciones automáticas** sobre datos pre/post-limpieza en consola
- **Toggle de gráficos** en modo automático (usuario elige si ver visualizaciones)
- **Reportes detallados** que incluyen tanto porcentaje como cantidad de registros descartados

### **📊 Optimizaciones de Análisis**
- **Gráficos mejorados:** Solo "casas" y "apartamentos" en títulos de precio promedio
- **Reporte de desconocidos:** Cantidad y valor de inmuebles "desconocidos" en consola
- **Indicadores robustos:** Corrección del bug de DataFrame truth value
- **Análisis contextual:** Cada módulo especifica claramente el estado de los datos que procesa

### **📚 Documentación Técnica**
- **Glosario completo:** 295+ términos técnicos organizados alfabéticamente
- **Definiciones especializadas:** Incluye términos de BI, inmobiliario, estadística y programación
- **Referencias cruzadas:** Enlaces navegables entre conceptos relacionados
- **Actualizaciones de versión:** Información unificada en todo el proyecto

---

## ⚙️ REQUISITOS TÉCNICOS

### **Python y Dependencias**
```bash
# Python 3.11+
pip install -r requirements.txt

# Librerías principales:
pandas>=2.0.0
numpy>=1.24.0
openpyxl>=3.1.0
matplotlib>=3.7.0
seaborn>=0.12.0
plotly>=5.14.0
sqlalchemy>=2.0.0
python-dotenv>=1.0.0
psutil>=5.9.0
```

### **Base de Datos**
- SQLite (incluido) para desarrollo
- PostgreSQL/MySQL para producción
- Configuración en `.env` (ver `.env.example`)

### **Sistema Operativo**
- ✅ Windows 10/11 (Probado)
- ✅ Linux Ubuntu 20.04+ 
- ✅ macOS 12+

---

## 🏗️ FUNCIONALIDADES AVANZADAS

### **Optimizaciones Implementadas**
- 🚀 **Cache inteligente** con 92% hit rate
- 🔄 **Procesamiento paralelo** en fases críticas
- 💾 **Pool de conexiones** DB reutilizable
- 🎯 **Fallback automático** en caso de errores
- 📊 **Métricas en tiempo real** de rendimiento

### **Análisis Estadístico Avanzado**
- 🔍 **Detección automática** de outliers
- 📈 **Winsorización** de valores extremos (P1-P99)
- 🎯 **Imputación zonal** por mediana
- 📊 **Análisis de correlaciones** multivariado
- 🔬 **Deduplicación inteligente** automática

### **Reportería Ejecutiva**
- 📋 **5 apartados especializados** de análisis
- 📊 **Indicadores clave** de negocio
- 🔄 **Diagrama de flujo** vertical organizado
- 💡 **3 conclusiones** con 3 detalles cada una
- 🚀 **3 recomendaciones** estratégicas detalladas

---

## 🛠️ TROUBLESHOOTING

### **Errores Comunes**

1. **Error de conexión DB**
   ```bash
   python scripts/test_db_connection.py
   ```

2. **Error de dependencias**
   ```bash
   pip install -r requirements.txt --upgrade
   ```

3. **Error de permisos**
   ```bash
   # En Windows, ejecutar como administrador
   # En Linux/macOS: sudo python main.py
   ```

### **Limpieza Manual**
```bash
# Limpiar datos temporales
python scripts/clean_and_backup_data.py

# Limpiar cache
python -c "from scripts.optimizacion_performance import DataCache; DataCache.clear_all()"
```

---

## 📞 SOPORTE TÉCNICO

**Desarrollador:** Juan Camilo Riaño Molano  
**Repositorio:** https://github.com/JrianoDevBI/dataAnalisysBI  
**Branch:** Prueba1  
**Issues:** Reportar en GitHub Issues  

### **Documentación Adicional**
- 📖 [Reporte Ejecutivo Completo](docs/reporte_ejecutivo_completo.md)
- 🔄 [Diagrama Interactivo](docs/diagrama_flujo_interactivo.html)
- � **[Glosario Técnico Completo](docs/GLOSARIO_TECNICO.md)** - 295+ términos
- �📊 [Análisis de Performance](docs/pipeline_unificado_v4_0.md)

---

## 📜 LICENCIA Y CHANGELOG

### **Versión 4.2 (08/08/2025) - ACTUAL**
- ✅ **UX Mejorada:** Menú reorganizado con 5 opciones claras
- ✅ **Clarificaciones automáticas:** Contexto pre/post-limpieza en consola
- ✅ **Toggle de gráficos:** Usuario elige mostrar visualizaciones en modo automático
- ✅ **Reportes detallados:** Porcentaje + cantidad de registros descartados
- ✅ **Gráficos optimizados:** Solo casas/apartamentos en títulos de precio
- ✅ **Bug fixes:** Corrección de DataFrame truth value error
- ✅ **Documentación completa:** Glosario técnico con 295+ términos
- ✅ **Comentarios en español:** Toda la documentación técnica unificada

### **Versión 4.1 (04/08/2025)**
- ✅ Pipeline unificado optimizado
- ✅ Separación CSS/JS/HTML en archivos independientes
- ✅ Reporte ejecutivo completo (5 apartados)
- ✅ Limpieza de archivos innecesarios
- ✅ Optimización del 47% vs. versión anterior

### **Versión 3.0 (03/08/2025)**
- ✅ Integración completa de todos los flujos
- ✅ Tratamiento estadístico avanzado
- ✅ Optimizaciones de performance

### **Versión 2.0 (02/08/2025)**
- ✅ Pipeline modularizado
- ✅ Análisis exploratorio automatizado

### **Versión 1.0 (01/08/2025)**
- ✅ Pipeline básico funcional
- ✅ Limpieza de datos implementada

---

*Sistema desarrollado con estándares profesionales de Business Intelligence para análisis de datos inmobiliarios a gran escala.*
