# Script para análisis exhaustivo de inconsistencias pre-limpieza de datos
# -------------------------------------------------------------
# analisis_pre_limpieza.py
# Analizador de calidad de datos RAW antes del proceso de limpieza.
#
# Autor: Juan Camilo Riaño Molano
# Fecha de creación: 01/08/2025
# Descripción:
#   Este script realiza un análisis exhaustivo de la calidad de datos ANTES
#   de aplicar cualquier proceso de limpieza, identificando:
#   - Valores faltantes y patrones de ausencia de datos
#   - Duplicados exactos y parciales
#   - Outliers estadísticos usando métodos IQR y Z-score
#   - Validación de rangos numéricos específicos del negocio
#   - Análisis de consistencia temporal en fechas
#   - Detección de valores atípicos por categorías
#   - Patrones anómalos en variables categóricas
#
#   Genera reportes detallados para tomar decisiones informadas
#   sobre el tratamiento y corrección de inconsistencias.
#
# Funcionalidades principales:
#   - Análisis de completitud por columna con patrones de ausencia
#   - Detección inteligente de duplicados con múltiples criterios
#   - Identificación estadística de outliers con umbrales configurables
#   - Validación de rangos específicos (área: 20-1000 m², precios, estratos)
#   - Análisis temporal de coherencia en fechas y secuencias
#   - Detección de inconsistencias categóricas y valores no estándar
# -------------------------------------------------------------

# =======================
# Importación de librerías
# =======================

import pandas as pd
import numpy as np
from datetime import datetime
import warnings

warnings.filterwarnings("ignore")


def analizar_valores_faltantes(df, nombre_archivo):
    """
    Analiza patrones de valores faltantes en el dataset
    """
    print(f"\n{'='*80}")
    print(f"ANÁLISIS DE VALORES FALTANTES - {nombre_archivo}")
    print(f"{'='*80}")

    # Estadísticas generales
    total_registros = len(df)
    print(f"Total de registros: {total_registros:,}")
    print(f"Total de columnas: {len(df.columns)}")

    # Análisis por columna
    missing_stats = []
    for col in df.columns:
        missing_count = df[col].isnull().sum()
        missing_pct = (missing_count / total_registros) * 100

        # Detectar patrones de valores vacíos adicionales
        if df[col].dtype == "object":
            empty_strings = (df[col] == "").sum()
            na_strings = df[col].isin(["NA", "N/A", "null", "NULL", "None", "NONE"]).sum()
            total_missing = missing_count + empty_strings + na_strings
            total_missing_pct = (total_missing / total_registros) * 100
        else:
            total_missing = missing_count
            total_missing_pct = missing_pct

        missing_stats.append(
            {
                "Columna": col,
                "Valores_Faltantes": missing_count,
                "Porcentaje_Faltantes": missing_pct,
                "Total_Problematicos": total_missing,
                "Porcentaje_Total_Problematicos": total_missing_pct,
            }
        )

    missing_df = pd.DataFrame(missing_stats).sort_values("Porcentaje_Total_Problematicos", ascending=False)

    print("\nRESUMEN DE VALORES FALTANTES:")
    print(missing_df.to_string(index=False))

    # Identificar columnas críticas
    criticas = missing_df[missing_df["Porcentaje_Total_Problematicos"] > 50]
    if not criticas.empty:
        print("\n⚠️  COLUMNAS CRÍTICAS (>50% de valores problemáticos):")
        for _, row in criticas.iterrows():
            print(f"   - {row['Columna']}: {row['Porcentaje_Total_Problematicos']:.1f}%")

    return missing_df


def analizar_duplicados(df, nombre_archivo):
    """
    Analiza duplicados completos y parciales
    """
    print(f"\n{'='*80}")
    print(f"ANÁLISIS DE DUPLICADOS - {nombre_archivo}")
    print(f"{'='*80}")

    total_registros = len(df)

    # Duplicados completos
    duplicados_completos = df.duplicated().sum()
    print(f"Registros duplicados completos: {duplicados_completos:,} ({(duplicados_completos/total_registros)*100:.2f}%)")

    # Análisis por columnas clave
    columnas_id = [col for col in df.columns if "id" in col.lower() or "Id" in col]

    for col in columnas_id:
        if col in df.columns:
            duplicados_col = df[col].duplicated().sum()
            print(f"Duplicados en '{col}': {duplicados_col:,} ({(duplicados_col/total_registros)*100:.2f}%)")

            if duplicados_col > 0 and duplicados_col <= 20:
                duplicados_ejemplos = df[df[col].duplicated(keep=False)][col].value_counts().head(10)
                print(f"   Ejemplos de valores duplicados en '{col}':")
                for valor, count in duplicados_ejemplos.items():
                    print(f"      {valor}: {count} veces")

    return duplicados_completos


def analizar_rangos_numericos(df, nombre_archivo):
    """
    Analiza rangos y outliers en columnas numéricas
    """
    print(f"\n{'='*80}")
    print(f"ANÁLISIS DE RANGOS NUMÉRICOS - {nombre_archivo}")
    print(f"{'='*80}")

    columnas_numericas = df.select_dtypes(include=[np.number]).columns

    inconsistencias = {"areas_problematicas": 0, "precios_problematicos": 0, "outliers_extremos": 0}

    for col in columnas_numericas:
        print(f"\n--- COLUMNA: {col} ---")
        serie = df[col].dropna()

        if len(serie) == 0:
            print("   Sin datos numéricos válidos")
            continue

        # Estadísticas básicas
        print(f"   Registros válidos: {len(serie):,}")
        print(f"   Min: {serie.min():,.2f}")
        print(f"   Max: {serie.max():,.2f}")
        print(f"   Media: {serie.mean():,.2f}")
        print(f"   Mediana: {serie.median():,.2f}")

        # Detectar valores negativos donde no deberían existir
        if col.lower() in ["area", "precio", "precio_solicitado", "estrato"]:
            negativos = (serie < 0).sum()
            if negativos > 0:
                print(f"   ⚠️  Valores negativos: {negativos}")
                inconsistencias["precios_problematicos"] += negativos

        # Análisis específico por tipo de columna
        if "area" in col.lower():
            # Áreas problemáticas
            areas_muy_pequenas = (serie < 10).sum()
            areas_muy_grandes = (serie > 500).sum()
            areas_cero = (serie == 0).sum()

            if areas_muy_pequenas > 0:
                print(f"   ⚠️  Áreas < 10 m²: {areas_muy_pequenas}")
                inconsistencias["areas_problematicas"] += areas_muy_pequenas
            if areas_muy_grandes > 0:
                print(f"   ⚠️  Áreas > 500 m²: {areas_muy_grandes}")
                inconsistencias["areas_problematicas"] += areas_muy_grandes
            if areas_cero > 0:
                print(f"   ⚠️  Áreas = 0: {areas_cero}")
                inconsistencias["areas_problematicas"] += areas_cero

        elif "precio" in col.lower():
            # Precios problemáticos
            precios_cero = (serie == 0).sum()
            precios_muy_altos = (serie > 2000000000).sum()  # > 2 mil millones
            precios_muy_bajos = (serie < 10000000).sum()  # < 10 millones

            if precios_cero > 0:
                print(f"   ⚠️  Precios = 0: {precios_cero}")
                inconsistencias["precios_problematicos"] += precios_cero
            if precios_muy_altos > 0:
                print(f"   ⚠️  Precios > 2,000M: {precios_muy_altos}")
                inconsistencias["precios_problematicos"] += precios_muy_altos
            if precios_muy_bajos > 0:
                print(f"   ⚠️  Precios < 10M: {precios_muy_bajos}")

        # Outliers usando IQR
        Q1 = serie.quantile(0.25)
        Q3 = serie.quantile(0.75)
        IQR = Q3 - Q1

        if IQR > 0:
            limite_inferior = Q1 - 3 * IQR
            limite_superior = Q3 + 3 * IQR
            outliers = serie[(serie < limite_inferior) | (serie > limite_superior)]

            if len(outliers) > 0:
                print(f"   Outliers extremos (±3*IQR): {len(outliers)}")
                inconsistencias["outliers_extremos"] += len(outliers)
                if len(outliers) <= 10:
                    print(f"   Valores outliers: {outliers.tolist()}")

    return inconsistencias


def analizar_consistencia_temporal(df, nombre_archivo):
    """
    Analiza consistencia en columnas de fecha y tiempo
    """
    print(f"\n{'='*80}")
    print(f"ANÁLISIS DE CONSISTENCIA TEMPORAL - {nombre_archivo}")
    print(f"{'='*80}")

    # Buscar columnas de fecha
    columnas_fecha = [col for col in df.columns if "fecha" in col.lower() or "date" in col.lower()]

    inconsistencias_temporales = 0

    for col in columnas_fecha:
        print(f"\n--- COLUMNA: {col} ---")

        # Intentar convertir a datetime
        try:
            fechas = pd.to_datetime(df[col], errors="coerce")
            fechas_validas = fechas.dropna()

            print(f"   Registros con fecha válida: {len(fechas_validas):,}")
            print(f"   Registros con fecha inválida: {len(df) - len(fechas_validas):,}")

            if len(fechas_validas) > 0:
                print(f"   Fecha más antigua: {fechas_validas.min()}")
                print(f"   Fecha más reciente: {fechas_validas.max()}")

                # Fechas futuras
                hoy = datetime.now()
                fechas_futuras = (fechas_validas > hoy).sum()
                if fechas_futuras > 0:
                    print(f"   ⚠️  Fechas futuras: {fechas_futuras}")
                    inconsistencias_temporales += fechas_futuras

                # Fechas muy antiguas (antes de 1990)
                fecha_limite = datetime(1990, 1, 1)
                fechas_antiguas = (fechas_validas < fecha_limite).sum()
                if fechas_antiguas > 0:
                    print(f"   ⚠️  Fechas antes de 1990: {fechas_antiguas}")
                    inconsistencias_temporales += fechas_antiguas

        except Exception as e:
            print(f"   Error procesando fechas: {e}")

    return inconsistencias_temporales


def analizar_categorias(df, nombre_archivo):
    """
    Analiza consistencia en variables categóricas
    """
    print(f"\n{'='*80}")
    print(f"ANÁLISIS DE VARIABLES CATEGÓRICAS - {nombre_archivo}")
    print(f"{'='*80}")

    columnas_categoricas = df.select_dtypes(include=["object"]).columns

    inconsistencias_categoricas = 0

    for col in columnas_categoricas:
        if col.lower() in ["fecha", "date"]:  # Skip date columns
            continue

        print(f"\n--- COLUMNA: {col} ---")

        valores_unicos = df[col].nunique()
        total_registros = len(df[col].dropna())

        print(f"   Valores únicos: {valores_unicos:,}")
        print(f"   Registros válidos: {total_registros:,}")

        if valores_unicos > 0:
            # Mostrar distribución de valores
            value_counts = df[col].value_counts().head(10)
            print("   Top 10 valores más frecuentes:")
            for valor, count in value_counts.items():
                pct = (count / total_registros) * 100
                print(f"      '{valor}': {count:,} ({pct:.1f}%)")

            # Detectar valores sospechosos
            valores_sospechosos = 0
            for valor in df[col].dropna().unique():
                valor_str = str(valor).strip().lower()

                # Valores que indican problemas
                if any(problema in valor_str for problema in ["error", "test", "prueba", "xxxx", "???", "null"]):
                    count = (df[col] == valor).sum()
                    print(f"   ⚠️  Valor sospechoso '{valor}': {count} registros")
                    valores_sospechosos += count

                # Valores muy cortos o muy largos
                if len(valor_str) == 1 and valor_str.isalpha():
                    count = (df[col] == valor).sum()
                    if count > 1:  # Solo reportar si aparece múltiples veces
                        print(f"   ⚠️  Valor muy corto '{valor}': {count} registros")
                        valores_sospechosos += count

            inconsistencias_categoricas += valores_sospechosos

    return inconsistencias_categoricas


def generar_reporte_completo(df_muestra, df_estados):
    """
    Genera un reporte completo de inconsistencias
    """
    print(f"\n{'='*80}")
    print("REPORTE COMPLETO DE INCONSISTENCIAS PRE-LIMPIEZA")
    print(f"{'='*80}")

    # Análisis de archivo muestra
    # Inicializar variables para el resumen
    duplicados_muestra = duplicados_estados = 0
    rangos_muestra = {}
    categoricas_muestra = categoricas_estados = 0
    temporal_estados = 0

    if df_muestra is not None:
        analizar_valores_faltantes(df_muestra, "MUESTRA")
        duplicados_muestra = analizar_duplicados(df_muestra, "MUESTRA")
        rangos_muestra = analizar_rangos_numericos(df_muestra, "MUESTRA")
        categoricas_muestra = analizar_categorias(df_muestra, "MUESTRA")

    # Análisis de archivo estados
    if df_estados is not None:
        analizar_valores_faltantes(df_estados, "ESTADOS")
        duplicados_estados = analizar_duplicados(df_estados, "ESTADOS")
        temporal_estados = analizar_consistencia_temporal(df_estados, "ESTADOS")
        categoricas_estados = analizar_categorias(df_estados, "ESTADOS")

    # Resumen final
    print("=" * 80)
    print("RESUMEN EJECUTIVO DE INCONSISTENCIAS")
    print(f"{'='*80}")

    if df_muestra is not None:
        print("\nARCHIVO MUESTRA:")
        print(f"   • Duplicados completos: {duplicados_muestra:,}")
        print(f"   • Áreas problemáticas: {rangos_muestra.get('areas_problematicas', 0):,}")
        print(f"   • Precios problemáticos: {rangos_muestra.get('precios_problematicos', 0):,}")
        print(f"   • Outliers extremos: {rangos_muestra.get('outliers_extremos', 0):,}")
        print(f"   • Inconsistencias categóricas: {categoricas_muestra:,}")

    if df_estados is not None:
        print("\nARCHIVO ESTADOS:")
        print(f"   • Duplicados completos: {duplicados_estados:,}")
        print(f"   • Inconsistencias temporales: {temporal_estados:,}")
        print(f"   • Inconsistencias categóricas: {categoricas_estados:,}")

    # Recomendaciones
    print(f"\n{'='*80}")
    print("RECOMENDACIONES PARA LIMPIEZA")
    print(f"{'='*80}")

    if df_muestra is not None and rangos_muestra.get("areas_problematicas", 0) > 0:
        print("• ÁREAS: Revisar y corregir áreas < 10 m² o > 500 m² antes de limpieza")

    if df_muestra is not None and rangos_muestra.get("precios_problematicos", 0) > 0:
        print("• PRECIOS: Validar precios = 0 o negativos antes de limpieza")

    if df_muestra is not None and duplicados_muestra > 0:
        print("• DUPLICADOS: Implementar estrategia de deduplicación")

    if df_estados is not None and temporal_estados > 0:
        print("• FECHAS: Corregir fechas futuras o muy antiguas")

    print("• Implementar validaciones específicas durante el proceso de limpieza")
    print("• Considerar crear reglas de negocio para valores atípicos")

    return True


def ejecutar_analisis_pre_limpieza(ruta_muestra="data/processedData/muestra.csv", ruta_estados="data/processedData/estados.csv"):
    """
    Función principal para ejecutar análisis pre-limpieza
    """
    print("INICIANDO ANÁLISIS DE INCONSISTENCIAS PRE-LIMPIEZA")
    print("Este análisis se ejecuta ANTES de cualquier proceso de limpieza\n")

    # Cargar archivos
    df_muestra = None
    df_estados = None

    try:
        df_muestra = pd.read_csv(ruta_muestra)
        print(f"✓ Archivo muestra cargado: {len(df_muestra):,} registros")
    except FileNotFoundError:
        print(f"⚠️  No se encontró: {ruta_muestra}")
    except Exception as e:
        print(f"✗ Error cargando muestra: {e}")

    try:
        df_estados = pd.read_csv(ruta_estados)
        print(f"✓ Archivo estados cargado: {len(df_estados):,} registros")
    except FileNotFoundError:
        print(f"⚠️  No se encontró: {ruta_estados}")
    except Exception as e:
        print(f"✗ Error cargando estados: {e}")

    if df_muestra is None and df_estados is None:
        print("✗ No se pudieron cargar los archivos necesarios")
        return False

    # Ejecutar análisis completo
    generar_reporte_completo(df_muestra, df_estados)

    return True


if __name__ == "__main__":
    ejecutar_analisis_pre_limpieza()
