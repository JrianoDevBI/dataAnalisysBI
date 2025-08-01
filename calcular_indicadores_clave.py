#!/usr/bin/env python3
# Script para el cálculo de indicadores clave del negocio inmobiliario
# -------------------------------------------------------------
# calcular_indicadores_clave.py
# Calculadora de indicadores de negocio específicos para análisis inmobiliario.
#
# Autor: Juan Camilo Riaño Molano
# Fecha de creación: 01/08/2025
# Descripción:
#   Este script calcula los indicadores clave de negocio solicitados para el informe ejecutivo:
#   - Precio promedio por metro cuadrado
#   - Tasa de confiabilidad de datos (completitud)
#   - Detección y cuantificación de outliers usando método IQR
#   - Análisis de estados: leads en "Revisar Dirección" y tiempo promedio
#   - Porcentaje de inmuebles descartados
#   - Estado inicial más frecuente en el pipeline
#
#   Los indicadores se calculan usando datos limpios y validados,
#   aplicando filtros de calidad y métodos estadísticos robustos.
# -------------------------------------------------------------

# =======================
# Importación de librerías
# =======================

import pandas as pd
import numpy as np


def calcular_indicadores():
    """Calcula todos los indicadores clave solicitados"""

    print("=== Calculando Indicadores Clave ===\n")

    # Cargar datos limpios
    try:
        df_muestra = pd.read_csv("data/cleanData/CLMUESTRA.csv")
        df_estados = pd.read_csv("data/cleanData/CLESTADOS.csv")
        print(f"✓ Datos cargados: {len(df_muestra)} inmuebles, {len(df_estados)} estados")
    except Exception as e:
        print(f"Error cargando datos: {e}")
        return

    # 1. Precio promedio por m²
    df_muestra["Precio_Solicitado_num"] = pd.to_numeric(df_muestra["Precio_Solicitado"], errors="coerce")
    df_muestra["Area_num"] = pd.to_numeric(df_muestra["Area"], errors="coerce")

    # Filtrar datos válidos para el cálculo
    df_valido = df_muestra[
        (df_muestra["Precio_Solicitado_num"].notna()) & (df_muestra["Area_num"].notna()) & (df_muestra["Area_num"] > 0)
    ].copy()

    df_valido["precio_por_m2"] = df_valido["Precio_Solicitado_num"] / df_valido["Area_num"]
    precio_promedio_m2 = df_valido["precio_por_m2"].mean()

    print(f"1. Precio promedio por m²: ${precio_promedio_m2:,.0f} COP/m²")

    # 2. Tasa de confiabilidad (porcentaje de datos completos)
    total_registros = len(df_muestra)
    # Contar registros con datos completos en campos críticos
    campos_criticos = ["Precio_Solicitado", "Area", "Ciudad", "Zona", "Tipo_Inmueble"]
    registros_completos = 0

    for _, row in df_muestra.iterrows():
        completo = True
        for campo in campos_criticos:
            if pd.isna(row[campo]) or str(row[campo]).lower() in ["desconocido", "nan", ""]:
                completo = False
                break
        if completo:
            registros_completos += 1

    tasa_confiabilidad = (registros_completos / total_registros) * 100
    print(f"2. Tasa de confiabilidad: {tasa_confiabilidad:.1f}%")

    # 3. Outliers identificados usando IQR
    Q1 = df_valido["precio_por_m2"].quantile(0.25)
    Q3 = df_valido["precio_por_m2"].quantile(0.75)
    IQR = Q3 - Q1
    limite_inferior = Q1 - 1.5 * IQR
    limite_superior = Q3 + 1.5 * IQR

    outliers = df_valido[(df_valido["precio_por_m2"] < limite_inferior) | (df_valido["precio_por_m2"] > limite_superior)]

    num_outliers = len(outliers)
    print(f"3. Outliers identificados: {num_outliers} (rangos: ${limite_inferior:,.0f} – ${limite_superior:,.0f} COP/m²)")

    # 4. Análisis de estados - Leads en "Revisar Dirección"
    df_estados["Fecha_Actualizacion"] = pd.to_datetime(df_estados["Fecha_Actualizacion"], errors="coerce")

    # Filtrar registros en "Revisar Dirección"
    revisar_direccion = df_estados[
        df_estados["Estado"].str.contains("Revisar", case=False, na=False)
        | df_estados["Estado"].str.contains("Dirección", case=False, na=False)
    ]

    num_revisar_direccion = len(revisar_direccion["Inmueble_ID"].unique())

    # Calcular tiempo promedio en este estado
    if len(revisar_direccion) > 0:
        # Agrupar por inmueble y calcular tiempo en estado
        tiempos_revisar = []
        for inmueble_id in revisar_direccion["Inmueble_ID"].unique():
            estados_inmueble = df_estados[df_estados["Inmueble_ID"] == inmueble_id].sort_values("Fecha_Actualizacion")

            # Buscar períodos en "Revisar Dirección"
            for i, row in estados_inmueble.iterrows():
                if "Revisar" in str(row["Estado"]) or "Dirección" in str(row["Estado"]):
                    # Buscar siguiente estado
                    siguientes = estados_inmueble[estados_inmueble["Fecha_Actualizacion"] > row["Fecha_Actualizacion"]]
                    if len(siguientes) > 0:
                        tiempo_dias = (siguientes.iloc[0]["Fecha_Actualizacion"] - row["Fecha_Actualizacion"]).days
                        if tiempo_dias > 0 and tiempo_dias < 365:  # Filtrar valores razonables
                            tiempos_revisar.append(tiempo_dias)

        tiempo_promedio_revisar = np.mean(tiempos_revisar) if tiempos_revisar else 0
    else:
        tiempo_promedio_revisar = 0

    print(f"4. Leads en 'Revisar Dirección': {num_revisar_direccion} (tiempo promedio: {tiempo_promedio_revisar:.1f} días)")

    # 5. Porcentaje descartados
    estados_descarte = df_estados[df_estados["Estado"].str.contains("Descart|Rechaz|Cancel", case=False, na=False)]
    inmuebles_descartados = len(estados_descarte["Inmueble_ID"].unique())
    total_inmuebles_con_estados = len(df_estados["Inmueble_ID"].unique())
    porcentaje_descartados = (inmuebles_descartados / total_inmuebles_con_estados) * 100

    print(f"5. Porcentaje descartados: {porcentaje_descartados:.1f}%")

    # 6. Estado inicial más frecuente
    # Obtener primer estado de cada inmueble
    primeros_estados = df_estados.groupby("Inmueble_ID")["Estado"].first()
    estado_inicial_mas_frecuente = primeros_estados.value_counts().index[0]
    frecuencia_estado_inicial = primeros_estados.value_counts().iloc[0]
    porcentaje_estado_inicial = (frecuencia_estado_inicial / len(primeros_estados)) * 100

    print(f"6. Estado inicial más frecuente: {estado_inicial_mas_frecuente} ({porcentaje_estado_inicial:.1f}%)")

    print("\n=== Resumen de Indicadores Clave ===")
    print(f"• Precio promedio por m²: ${precio_promedio_m2:,.0f} COP/m²")
    print(f"• Tasa de confiabilidad: {tasa_confiabilidad:.1f}%")
    print(f"• Outliers identificados: {num_outliers} (rangos: ${limite_inferior:,.0f} – ${limite_superior:,.0f} COP/m²)")
    print(f"• Leads en 'Revisar Dirección': {num_revisar_direccion} (tiempo promedio: {tiempo_promedio_revisar:.1f} días)")
    print(f"• Porcentaje descartados: {porcentaje_descartados:.1f}%")
    print(f"• Estado inicial más frecuente: {estado_inicial_mas_frecuente} ({porcentaje_estado_inicial:.1f}%)")


if __name__ == "__main__":
    calcular_indicadores()
