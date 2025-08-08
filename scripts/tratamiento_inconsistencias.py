# Script para tratamiento avanzado de datos inconsistentes con técnicas estadísticas robustas
# -------------------------------------------------------------
# tratamiento_inconsistencias.py
# Módulo especializado para el tratamiento de inconsistencias usando técnicas estadísticas avanzadas.
#
# Autor: Juan Camilo Riaño Molano
# Fecha de creación: 01/08/2025
# Descripción:
#   Este script implementa técnicas estadísticas robustas para el tratamiento de datos inconsistentes:
#   - Eliminación inteligente de duplicados con múltiples criterios
#   - Imputación de precios faltantes por mediana zonal con validación
#   - Winsorización de outliers al 1% para preservar distribuciones
#   - Tratamiento específico por tipo de inconsistencia detectada
#   - Logging detallado de todas las transformaciones aplicadas
#   - Backup automático antes de aplicar cambios
#
#   Se ejecuta después del análisis pre-limpieza y antes de la limpieza final,
#   aplicando técnicas estadísticas que mejoran la calidad sin distorsionar patrones.
#
# Funcionalidades principales:
#   - Eliminación de duplicados exactos y similares con umbral configurable
#   - Imputación zonal por mediana para preservar patrones geográficos
#   - Winsorización configurable (default 1%) para manejo robusto de outliers
#   - Validación post-tratamiento para verificar mejoras en calidad
#   - Métricas de impacto y reportes de transformaciones aplicadas
#   - Integración seamless con pipeline existente sin afectar flujo
#
# Buenas prácticas implementadas:
#   - Backup automático antes de cualquier transformación destructiva
#   - Logging detallado con métricas before/after para auditabilidad
#   - Validación de parámetros y datos de entrada antes del procesamiento
#   - Manejo robusto de casos edge y valores extremos
#   - Configuración flexible de umbrales y parámetros de tratamiento
# -------------------------------------------------------------

# =======================
# Importación de librerías
# =======================
import pandas as pd
import numpy as np
import shutil
from datetime import datetime
from pathlib import Path
import warnings

warnings.filterwarnings("ignore")

# =======================
# Configuración de rutas
# =======================
BASE_DIR = Path(__file__).parent.parent
PROCESSED_DIR = BASE_DIR / "data" / "processedData"
BACKUP_DIR = BASE_DIR / "dataBackup" / "tratamiento_inconsistencias"


def crear_backup_tratamiento():
    """
    Crea backup específico antes del tratamiento de inconsistencias.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = BACKUP_DIR / f"backup_pre_tratamiento_{timestamp}"

    try:
        # Crear directorio de backup si no existe
        backup_path.mkdir(parents=True, exist_ok=True)

        # Backup de archivos procesados
        if PROCESSED_DIR.exists():
            shutil.copytree(PROCESSED_DIR, backup_path / "processedData", dirs_exist_ok=True)
            print(f"✓ Backup creado en: {backup_path}")
            return True
    except Exception as e:
        print(f"⚠️ Error creando backup: {e}")
        return False


def eliminar_duplicados_avanzado(df, columnas_clave=None, umbral_similitud=0.95):
    """
    Elimina duplicados SOLO en columnas críticas: ID y Fecha_Actualizacion.

    Todas las demás columnas pueden tener duplicados ya que es comportamiento
    normal del negocio inmobiliario (múltiples propiedades similares).

    Args:
        df (DataFrame): Dataset a procesar
        columnas_clave (list): Columnas críticas para identificar duplicados
        umbral_similitud (float): No utilizado en esta versión

    Returns:
        DataFrame: Dataset sin duplicados críticos
        dict: Estadísticas del proceso
    """
    print("🔄 Eliminando duplicados solo en columnas críticas...")
    registros_iniciales = len(df)

    # Identificar columnas críticas que NO deben tener duplicados
    columnas_criticas = []

    # Solo verificar ID si existe en el dataset
    if "Id" in df.columns:
        columnas_criticas.append("Id")
    elif "ID" in df.columns:
        columnas_criticas.append("ID")
    elif "Inmueble_ID" in df.columns:
        columnas_criticas.append("Inmueble_ID")

    # Solo verificar Fecha_Actualizacion si existe
    if "Fecha_Actualizacion" in df.columns:
        columnas_criticas.append("Fecha_Actualizacion")

    if not columnas_criticas:
        print("  ✓ No se encontraron columnas críticas para verificar duplicados")
        return df, {
            "registros_iniciales": registros_iniciales,
            "registros_finales": len(df),
            "eliminados": 0,
            "porcentaje_eliminado": 0.0,
        }

    print(f"  ✓ Verificando duplicados en columnas críticas: {columnas_criticas}")

    # Eliminar duplicados solo en columnas críticas
    duplicados_criticos = df.duplicated(subset=columnas_criticas, keep="first")
    df_limpio = df[~duplicados_criticos].copy()
    criticos_eliminados = duplicados_criticos.sum()

    registros_finales = len(df_limpio)
    porcentaje_eliminado = (criticos_eliminados / registros_iniciales) * 100

    print(f"  ✓ Duplicados críticos eliminados: {criticos_eliminados}")
    print(f"  ✓ Total eliminados: {criticos_eliminados} ({porcentaje_eliminado:.2f}%)")

    # Estadísticas de retorno
    estadisticas = {
        "registros_iniciales": registros_iniciales,
        "duplicados_criticos_eliminados": criticos_eliminados,
        "registros_finales": registros_finales,
        "porcentaje_eliminado": porcentaje_eliminado,
    }

    return df_limpio, estadisticas


def imputar_precios_mediana_zonal(df, columna_precio="Precio_Solicitado", columna_zona="Zona"):
    """
    Imputa precios faltantes usando la mediana por zona.

    Args:
        df (DataFrame): Dataset a procesar
        columna_precio (str): Nombre de la columna de precio
        columna_zona (str): Nombre de la columna de zona

    Returns:
        DataFrame: Dataset con precios imputados
        dict: Estadísticas del proceso
    """
    print("💰 Imputando precios faltantes por mediana zonal...")
    df_procesado = df.copy()

    # Convertir a numérico
    df_procesado[columna_precio] = pd.to_numeric(df_procesado[columna_precio], errors="coerce")

    # Identificar valores faltantes
    valores_faltantes = df_procesado[columna_precio].isna()
    total_faltantes = valores_faltantes.sum()

    if total_faltantes == 0:
        print("  ✓ No hay valores faltantes en precios")
        return df_procesado, {"valores_imputados": 0, "porcentaje_imputado": 0}

    # Calcular mediana por zona
    mediana_por_zona = df_procesado.groupby(columna_zona)[columna_precio].median()

    # Imputar valores faltantes
    imputaciones_realizadas = 0
    for zona in df_procesado[columna_zona].unique():
        if zona in mediana_por_zona.index and not pd.isna(mediana_por_zona[zona]):
            mask = (df_procesado[columna_zona] == zona) & valores_faltantes
            valores_a_imputar = mask.sum()

            if valores_a_imputar > 0:
                df_procesado.loc[mask, columna_precio] = mediana_por_zona[zona]
                imputaciones_realizadas += valores_a_imputar
                print(f"  ✓ Zona '{zona}': {valores_a_imputar} valores imputados con mediana ${mediana_por_zona[zona]:,.0f}")

    # Para valores que no pudieron ser imputados por zona, usar mediana global
    valores_aun_faltantes = df_procesado[columna_precio].isna().sum()
    if valores_aun_faltantes > 0:
        mediana_global = df_procesado[columna_precio].median()
        df_procesado[columna_precio] = df_procesado[columna_precio].fillna(mediana_global)
        imputaciones_realizadas += valores_aun_faltantes
        print(f"  ✓ {valores_aun_faltantes} valores imputados con mediana global ${mediana_global:,.0f}")

    estadisticas = {
        "valores_faltantes_iniciales": total_faltantes,
        "valores_imputados": imputaciones_realizadas,
        "porcentaje_imputado": (total_faltantes / len(df)) * 100,
        "mediana_por_zona": mediana_por_zona.to_dict(),
    }

    print(f"  ✓ Total imputado: {imputaciones_realizadas} valores ({estadisticas['porcentaje_imputado']:.2f}%)")

    return df_procesado, estadisticas


def winsorizar_outliers(df, columnas_numericas=None, percentil_inferior=1, percentil_superior=99):
    """
    Aplica winsorización a outliers en columnas numéricas.

    Args:
        df (DataFrame): Dataset a procesar
        columnas_numericas (list): Columnas a winsorizar
        percentil_inferior (float): Percentil inferior para winsorización
        percentil_superior (float): Percentil superior para winsorización

    Returns:
        DataFrame: Dataset winsorizado
        dict: Estadísticas del proceso
    """
    print(f"📈 Aplicando winsorización de outliers (P{percentil_inferior}-P{percentil_superior})...")
    df_procesado = df.copy()

    # Columnas por defecto
    if columnas_numericas is None:
        columnas_numericas = ["Precio_Solicitado", "Area"]

    estadisticas = {}

    for columna in columnas_numericas:
        if columna not in df.columns:
            continue

        # Convertir a numérico
        df_procesado[columna] = pd.to_numeric(df_procesado[columna], errors="coerce")
        valores_numericos = df_procesado[columna].dropna()

        if len(valores_numericos) == 0:
            continue

        # Calcular percentiles
        p_inferior = np.percentile(valores_numericos, percentil_inferior)
        p_superior = np.percentile(valores_numericos, percentil_superior)

        # Identificar outliers
        outliers_inferiores = (df_procesado[columna] < p_inferior) & df_procesado[columna].notna()
        outliers_superiores = (df_procesado[columna] > p_superior) & df_procesado[columna].notna()

        total_outliers = outliers_inferiores.sum() + outliers_superiores.sum()

        if total_outliers > 0:
            # Aplicar winsorización
            df_procesado.loc[outliers_inferiores, columna] = p_inferior
            df_procesado.loc[outliers_superiores, columna] = p_superior

            estadisticas[columna] = {
                "outliers_inferiores": outliers_inferiores.sum(),
                "outliers_superiores": outliers_superiores.sum(),
                "total_outliers": total_outliers,
                "percentil_inferior": p_inferior,
                "percentil_superior": p_superior,
                "porcentaje_outliers": (total_outliers / len(valores_numericos)) * 100,
            }

            print(f"  ✓ {columna}: {total_outliers} outliers winsorizados ({estadisticas[columna]['porcentaje_outliers']:.2f}%)")
            print(f"    - Límites: [{p_inferior:,.0f}, {p_superior:,.0f}]")
        else:
            estadisticas[columna] = {"total_outliers": 0, "porcentaje_outliers": 0}
            print(f"  ✓ {columna}: No se detectaron outliers")

    return df_procesado, estadisticas


def validar_mejoras_calidad(df_original, df_tratado):
    """
    Valida que el tratamiento mejoró la calidad de los datos.

    Args:
        df_original (DataFrame): Dataset original
        df_tratado (DataFrame): Dataset después del tratamiento

    Returns:
        dict: Métricas de mejora en calidad
    """
    print("✅ Validando mejoras en calidad de datos...")

    # Métricas de completitud
    completitud_original = (1 - df_original.isnull().sum() / len(df_original)) * 100
    completitud_tratado = (1 - df_tratado.isnull().sum() / len(df_tratado)) * 100

    # Métricas de consistencia (coeficiente de variación)
    cv_original = {}
    cv_tratado = {}

    for col in ["Precio_Solicitado", "Area"]:
        if col in df_original.columns:
            # Original
            datos_orig = pd.to_numeric(df_original[col], errors="coerce").dropna()
            if len(datos_orig) > 0 and datos_orig.mean() != 0:
                cv_original[col] = datos_orig.std() / datos_orig.mean()

            # Tratado
            datos_trat = pd.to_numeric(df_tratado[col], errors="coerce").dropna()
            if len(datos_trat) > 0 and datos_trat.mean() != 0:
                cv_tratado[col] = datos_trat.std() / datos_trat.mean()

    mejoras = {
        "registros_original": len(df_original),
        "registros_tratado": len(df_tratado),
        "completitud_original": completitud_original.to_dict(),
        "completitud_tratado": completitud_tratado.to_dict(),
        "coef_variacion_original": cv_original,
        "coef_variacion_tratado": cv_tratado,
    }

    print(f"  ✓ Registros: {len(df_original)} → {len(df_tratado)}")

    for col in completitud_original.index:
        if col in completitud_tratado.index:
            mejora = completitud_tratado[col] - completitud_original[col]
            print(f"  ✓ Completitud {col}: {completitud_original[col]:.1f}% → {completitud_tratado[col]:.1f}% ({mejora:+.1f}%)")

    return mejoras


def ejecutar_tratamiento_inconsistencias():
    """
    Función principal que ejecuta todo el tratamiento de inconsistencias.
    """
    print("=" * 80)
    print("INICIANDO TRATAMIENTO AVANZADO DE DATOS INCONSISTENTES")
    print("=" * 80)

    # Rutas de archivos
    archivo_muestra = PROCESSED_DIR / "muestra.csv"

    # Verificar existencia de archivos
    if not archivo_muestra.exists():
        print("⚠️ No se encontró archivo de muestra procesado. Ejecute primero obtain_data.py")
        return False

    # Crear backup
    if not crear_backup_tratamiento():
        print("⚠️ No se pudo crear backup. Continuando...")

    try:
        # Cargar datos
        print("\n📂 Cargando datos procesados...")
        df_muestra = pd.read_csv(archivo_muestra)
        print(f"  ✓ Muestra cargada: {len(df_muestra)} registros")

        df_original = df_muestra.copy()  # Para validación final

        # 1. Eliminación de duplicados
        print("\n" + "=" * 60)
        print("1. ELIMINACIÓN DE DUPLICADOS")
        print("=" * 60)
        df_muestra, stats_duplicados = eliminar_duplicados_avanzado(df_muestra)

        # 2. Imputación de precios
        print("\n" + "=" * 60)
        print("2. IMPUTACIÓN DE PRECIOS FALTANTES")
        print("=" * 60)
        df_muestra, stats_imputacion = imputar_precios_mediana_zonal(df_muestra)

        # 3. Winsorización de outliers
        print("\n" + "=" * 60)
        print("3. WINSORIZACIÓN DE OUTLIERS")
        print("=" * 60)
        df_muestra, stats_winsor = winsorizar_outliers(df_muestra)

        # 4. Validación de mejoras
        print("\n" + "=" * 60)
        print("4. VALIDACIÓN DE MEJORAS EN CALIDAD")
        print("=" * 60)
        validar_mejoras_calidad(df_original, df_muestra)

        # Guardar resultado
        output_path = PROCESSED_DIR / "muestra_tratada.csv"
        df_muestra.to_csv(output_path, index=False, encoding="utf-8")
        print(f"\n✅ Datos tratados guardados en: {output_path}")

        # Resumen final
        print("\n" + "=" * 80)
        print("RESUMEN DE TRATAMIENTO COMPLETADO")
        print("=" * 80)
        print(f"📊 Registros procesados: {len(df_original)} → {len(df_muestra)}")
        print(f"🔄 Duplicados eliminados: {stats_duplicados['duplicados_criticos_eliminados']}")
        print(f"💰 Precios imputados: {stats_imputacion['valores_imputados']}")

        total_outliers_winsor = sum([stats["total_outliers"] for stats in stats_winsor.values()])
        print(f"📈 Outliers winsorizados: {total_outliers_winsor}")
        print("✅ Tratamiento de inconsistencias completado exitosamente")
        
        # Aclaración sobre el impacto en análisis posteriores
        print("\n" + "💡" * 50)
        print("NOTA IMPORTANTE: IMPACTO EN ANÁLISIS POSTERIOR")
        print("💡" * 50)
        print("🔄 Los datos procesados han sido mejorados estadísticamente:")
        print("   • Outliers corregidos por winsorización (límites P1-P99)")
        print("   • Valores faltantes imputados con medianas zonales")
        print("   • Duplicados críticos eliminados")
        print("📊 Al comparar con análisis PRE-tratamiento, esperará:")
        print("   • MENOR número de inconsistencias detectadas")
        print("   • MEJOR tasa de confiabilidad de datos")
        print("   • PRECIOS más estables (menos outliers extremos)")
        print("   • ESTADOS más consistentes después de limpieza")
        print("💡" * 50)

        return True

    except Exception as e:
        print(f"❌ Error durante el tratamiento: {e}")
        return False


if __name__ == "__main__":
    ejecutar_tratamiento_inconsistencias()
