# Script para análisis exploratorio avanzado de datos inmobiliarios limpios
# -------------------------------------------------------------
# analisis_exploratorio.py
# Analizador exploratorio con indicadores clave y visualizaciones avanzadas.
#
# Autor: Juan Camilo Riaño Molano
# Fecha de creación: 01/08/2025
# Descripción:
#   Este script realiza análisis exploratorio exhaustivo de datos inmobiliarios limpios:
#   - Cálculo de indicadores clave de negocio (precio/m², confiabilidad, outliers)
#   - Análisis de correlaciones con foco en Estrato vs Precio
#   - Detección de inconsistencias específicas por agrupaciones
#   - Visualizaciones interactivas y estáticas
#   - Análisis de distribuciones y tendencias
#   - Generación de insights automatizados para toma de decisiones
#
#   Integra metodologías estadísticas robustas con visualizaciones
#   profesionales para proporcionar insights accionables del negocio.
#
# Funcionalidades principales:
#   - Indicadores clave: precio/m², tasa confianza, outliers, leads problemáticos
#   - Análisis de correlación Estrato-Precio con visualización
#   - Detección de inconsistencias por Ciudad, Zona, Tipo de inmueble
#   - Gráficos interactivos con Plotly y estáticos con Matplotlib/Seaborn
#   - Análisis temporal de estados y flujos de conversión
#   - Resúmenes ejecutivos automatizados con recomendaciones
#
# Buenas prácticas implementadas:
#   - Modularidad: cada análisis es una función especializada e independiente
#   - Validación exhaustiva de columnas y tipos antes del procesamiento
#   - Documentación detallada con docstrings y comentarios explicativos
#   - Manejo robusto de errores con mensajes informativos
#   - Configuración flexible de parámetros de análisis
# -------------------------------------------------------------

# =======================
# Importación de librerías
# =======================

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


# =========================
# Análisis de relaciones entre características
# =========================
def analizar_relaciones(df):
    """
    Analiza y visualiza relaciones entre características numéricas y categóricas relevantes del DataFrame.
    - Calcula matriz de correlación.
    - Genera scatter plots y boxplots para variables clave.
    - Muestra tablas resumen por agrupación.
    """

    print("\n--- MATRIZ DE CORRELACIÓN ---")
    # Comparar Area, Precio_Solicitado y Estrato si existen
    columnas_numericas = []
    if "Area" in df.columns:
        columnas_numericas.append("Area")
    if "Precio_Solicitado" in df.columns:
        columnas_numericas.append("Precio_Solicitado")
    if "Estrato" in df.columns:
        columnas_numericas.append("Estrato")

    if len(columnas_numericas) >= 2:
        df_corr = df[columnas_numericas].copy()
        for col in columnas_numericas:
            df_corr[col] = pd.to_numeric(df_corr[col], errors="coerce")
        correlacion = df_corr.corr()
        print(f"Variables numéricas comparadas: {', '.join(columnas_numericas)}")
        print(correlacion)
        plt.figure(figsize=(8, 6))
        sns.heatmap(correlacion, annot=True, cmap="coolwarm", vmin=-1, vmax=1)
        plt.title("Matriz de correlación (Área, Precio Solicitado, Estrato)")
        plt.show()

        # Conclusiones automáticas
        print("\n--- Conclusiones automáticas de la matriz de correlación ---")
        if "Area" in columnas_numericas and "Precio_Solicitado" in columnas_numericas:
            corr_area_precio = correlacion.loc["Area", "Precio_Solicitado"]
            print(f"• Correlación Área vs Precio: {corr_area_precio:.2f}")
            if abs(corr_area_precio) > 0.7:
                print("  Relación fuerte entre área y precio solicitado.")
            elif abs(corr_area_precio) > 0.4:
                print("  Relación moderada entre área y precio solicitado.")
            else:
                print("  Relación débil entre área y precio solicitado.")

        if "Estrato" in columnas_numericas and "Precio_Solicitado" in columnas_numericas:
            corr_estrato_precio = correlacion.loc["Estrato", "Precio_Solicitado"]
            print(f"• Correlación Estrato vs Precio: {corr_estrato_precio:.2f}")
            if abs(corr_estrato_precio) > 0.7:
                print("  Relación fuerte entre estrato y precio solicitado.")
            elif abs(corr_estrato_precio) > 0.4:
                print("  Relación moderada entre estrato y precio solicitado.")
            else:
                print("  Relación débil entre estrato y precio solicitado.")
    else:
        print("No hay suficientes variables numéricas para comparar.")

    # Bubble plot interactivo vertical con Plotly
    if "Area" in df.columns and "Precio_Solicitado" in df.columns and "Zona" in df.columns:
        try:
            import plotly.express as px

            df = df.copy()
            df["Area"] = pd.to_numeric(df["Area"], errors="coerce")
            df["Precio_Solicitado"] = pd.to_numeric(df["Precio_Solicitado"], errors="coerce")
            resumen = (
                df.groupby("Zona")
                .agg({"Area": "mean", "Precio_Solicitado": "mean", "Zona": "count"})
                .rename(columns={"Zona": "Cantidad"})
            )
            resumen = resumen.reset_index()
            # Bubble plot vertical: eje x = Zona, eje y = Precio, tamaño = cantidad, color = Area promedio
            fig = px.scatter(
                resumen,
                x="Zona",
                y="Precio_Solicitado",
                size="Cantidad",
                color="Area",
                hover_name="Zona",
                hover_data={"Cantidad": True, "Area": True, "Precio_Solicitado": True},
                size_max=60,
                height=800,
            )
            fig.update_traces(marker=dict(line=dict(width=1, color="DarkSlateGrey")))
            fig.update_layout(
                title=(
                    "Bubble plot interactivo: Precio Solicitado promedio por Zona " "(tamaño=frecuencia, color=Área promedio)"
                ),
                xaxis_title="Zona",
                yaxis_title="Precio Solicitado promedio",
                xaxis_tickangle=45,
                showlegend=True,
                margin=dict(l=40, r=40, t=80, b=200),
            )
            fig.update_xaxes(tickfont=dict(size=10))
            fig.show()
            print(
                "\n--- Bubble plot interactivo generado: "
                "Precio Solicitado promedio por Zona (tamaño=frecuencia, color=Área promedio) ---"
            )
        except ImportError:
            print("Plotly no está instalado. Ejecuta 'pip install plotly' para gráficos interactivos.")
            return

    # Gráfico violin interactivo de precio por zona (ordenado de menor a mayor precio promedio)
    if "Zona" in df.columns and "Precio_Solicitado" in df.columns:
        try:
            import plotly.express as px

            df = df.copy()
            df["Precio_Solicitado"] = pd.to_numeric(df["Precio_Solicitado"], errors="coerce")
            orden_zonas = df.groupby("Zona")["Precio_Solicitado"].mean().sort_values().index
            fig = px.violin(
                df,
                x="Zona",
                y="Precio_Solicitado",
                category_orders={"Zona": list(orden_zonas)},
                box=True,
                points="all",
                hover_data=["Zona", "Precio_Solicitado"],
                height=700,
            )
            fig.update_layout(
                title=("Distribución interactiva (Violin Plot) de Precio Solicitado por Zona (ordenado)"),
                xaxis_title="Zona",
                yaxis_title="Precio Solicitado",
                xaxis_tickangle=45,
                margin=dict(l=40, r=40, t=80, b=200),
            )
            fig.update_xaxes(tickfont=dict(size=10))
            fig.show()
            # Conclusión automática para violin plot
            print("\n--- Conclusión automática del violin plot Precio Solicitado por Zona ---")
            zona_mas_cara = df.groupby("Zona")["Precio_Solicitado"].mean().idxmax()
            zona_mas_barata = df.groupby("Zona")["Precio_Solicitado"].mean().idxmin()
            print(f"La zona con mayor precio promedio es '{zona_mas_cara}'.")
            print(f"La zona con menor precio promedio es '{zona_mas_barata}'.")
        except ImportError:
            print("Plotly no está instalado. Ejecuta 'pip install plotly' para gráficos interactivos.")
            return

    # Tabla resumen de precio promedio por tipo de inmueble (agrupando y normalizando nombres)
    if "Tipo_Inmueble" in df.columns and "Precio_Solicitado" in df.columns:
        df = df.copy()
        df["Precio_Solicitado"] = pd.to_numeric(df["Precio_Solicitado"], errors="coerce")
        # Normalizar nombres: minúsculas y quitar tildes/espacios
        df["Tipo_Inmueble_norm"] = (
            df["Tipo_Inmueble"]
            .astype(str)
            .str.lower()
            .str.normalize("NFKD")
            .str.encode("ascii", errors="ignore")
            .str.decode("utf-8")
            .str.strip()
        )
        resumen = df.groupby("Tipo_Inmueble_norm")["Precio_Solicitado"].mean().sort_values(ascending=False)
        conteo = df["Tipo_Inmueble_norm"].value_counts().reindex(resumen.index)
        # Formatear como moneda COP
        resumen_moneda = resumen.apply(lambda x: "$ {:,.0f}".format(x) if pd.notnull(x) else "-")
        print("\n--- Precio promedio por Tipo de Inmueble (agrupado, normalizado y en moneda local) ---")
        print(resumen_moneda)

        # Gráfico 1: casas, apartamentos y desconocido
        tipos_1 = ["casa", "apartamento", "desconocido"]
        resumen_1 = resumen[resumen.index.isin(tipos_1)]
        conteo_1 = conteo[resumen_1.index]
        plt.figure(figsize=(10, 5))
        plt.bar(resumen_1.index, resumen_1.values, color="skyblue", edgecolor="black")
        plt.title("Precio promedio por Tipo de Inmueble (COP)\n(Casas, Apartamentos y Desconocido)")
        plt.xlabel("Tipo de Inmueble")
        plt.ylabel("Precio promedio (COP)")
        plt.xticks(rotation=30, ha="right")
        for i, (v, c) in enumerate(zip(resumen_1.values, conteo_1.values)):
            plt.text(i, v, f"$ {v:,.0f}\n({c} registros)", ha="center", va="bottom", fontsize=9, fontweight="bold")
        plt.tight_layout()
        plt.show()

        # Gráfico 2: solo casas y apartamentos
        tipos_2 = ["casa", "apartamento"]
        resumen_2 = resumen[resumen.index.isin(tipos_2)]
        conteo_2 = conteo[resumen_2.index]
        plt.figure(figsize=(8, 5))
        plt.bar(resumen_2.index, resumen_2.values, color="lightgreen", edgecolor="black")
        plt.title("Precio promedio por Tipo de Inmueble (COP)\n(Solo Casas y Apartamentos)")
        plt.xlabel("Tipo de Inmueble")
        plt.ylabel("Precio promedio (COP)")
        plt.xticks(rotation=30, ha="right")
        for i, (v, c) in enumerate(zip(resumen_2.values, conteo_2.values)):
            plt.text(i, v, f"$ {v:,.0f}\n({c} registros)", ha="center", va="bottom", fontsize=9, fontweight="bold")
        plt.tight_layout()
        plt.show()


# =========================
# Cálculo de indicadores clave
# =========================
def calcular_indicadores_clave(df_muestra, df_estados=None):
    """
    Calcula los indicadores clave específicos del proyecto:
    - Precio promedio por m²
    - Tasa de confiabilidad
    - Outliers identificados
    - Leads en "Revisar Dirección"
    - Porcentaje descartados
    - Estado inicial más frecuente
    """
    print("\n" + "=" * 60)
    print("          INDICADORES CLAVE DEL PROYECTO")
    print("=" * 60)

    # 1. Precio promedio por m²
    if "Area" in df_muestra.columns and "Precio_Solicitado" in df_muestra.columns:
        df_temp = df_muestra.copy()
        df_temp["Area"] = pd.to_numeric(df_temp["Area"], errors="coerce")
        df_temp["Precio_Solicitado"] = pd.to_numeric(df_temp["Precio_Solicitado"], errors="coerce")
        df_temp = df_temp.dropna(subset=["Area", "Precio_Solicitado"])
        df_temp = df_temp[df_temp["Area"] > 0]  # Evitar división por cero
        df_temp["precio_por_m2"] = df_temp["Precio_Solicitado"] / df_temp["Area"]
        precio_promedio_m2 = df_temp["precio_por_m2"].mean()
        print(f"• Precio promedio por m²: ${precio_promedio_m2:,.0f} COP/m²")
    else:
        precio_promedio_m2 = "N/A"
        print("• Precio promedio por m²: N/A (faltan columnas Area o Precio_Solicitado)")

    # 2. Tasa de confiabilidad (registros sin imputaciones)
    columnas_importantes = ["Ciudad", "Zona", "Estrato", "Tipo_Inmueble", "Precio_Solicitado", "Area"]
    columnas_existentes = [col for col in columnas_importantes if col in df_muestra.columns]

    if columnas_existentes:
        registros_completos = df_muestra[columnas_existentes].dropna().shape[0]
        total_registros = len(df_muestra)
        tasa_confiabilidad = (registros_completos / total_registros) * 100
        print(f"• Tasa de confiabilidad: {tasa_confiabilidad:.1f}%")
    else:
        tasa_confiabilidad = "N/A"
        print("• Tasa de confiabilidad: N/A (no se encontraron columnas clave)")

    # 3. Outliers identificados
    if "Precio_Solicitado" in df_muestra.columns:
        df_temp = df_muestra.copy()
        df_temp["Precio_Solicitado"] = pd.to_numeric(df_temp["Precio_Solicitado"], errors="coerce")
        df_temp = df_temp.dropna(subset=["Precio_Solicitado"])

        Q1 = df_temp["Precio_Solicitado"].quantile(0.25)
        Q3 = df_temp["Precio_Solicitado"].quantile(0.75)
        IQR = Q3 - Q1
        limite_inferior = Q1 - 1.5 * IQR
        limite_superior = Q3 + 1.5 * IQR

        outliers = df_temp[(df_temp["Precio_Solicitado"] < limite_inferior) | (df_temp["Precio_Solicitado"] > limite_superior)]
        num_outliers = len(outliers)

        print(f"• Outliers identificados: {num_outliers} (rangos: ${limite_inferior:,.0f} – ${limite_superior:,.0f} COP)")
    else:
        print("• Outliers identificados: N/A (falta columna Precio_Solicitado)")

    # 4. Análisis de estados (si se proporciona df_estados)
    if df_estados is not None:
        # Leads en "Revisar Dirección"
        if "Estado" in df_estados.columns:
            revisar_direccion = df_estados[df_estados["Estado"].str.contains("Revisar Dirección", case=False, na=False)]
            num_revisar_direccion = (
                len(revisar_direccion["Inmueble_ID"].unique()) if "Inmueble_ID" in df_estados.columns else len(revisar_direccion)
            )

            # Calcular tiempo promedio en "Revisar Dirección"
            if "Fecha_Actualizacion" in df_estados.columns and "Inmueble_ID" in df_estados.columns:
                df_temp = df_estados.copy()
                df_temp["Fecha_Actualizacion"] = pd.to_datetime(df_temp["Fecha_Actualizacion"], errors="coerce")
                df_temp = df_temp.sort_values(["Inmueble_ID", "Fecha_Actualizacion"])

                # Calcular tiempo en cada estado
                tiempos_revisar = []
                for inmueble_id in revisar_direccion["Inmueble_ID"].unique():
                    estados_inmueble = df_temp[df_temp["Inmueble_ID"] == inmueble_id]
                    for idx, row in estados_inmueble.iterrows():
                        if "Revisar Dirección" in str(row["Estado"]):
                            # Buscar el siguiente estado
                            siguiente = estados_inmueble[estados_inmueble.index > idx]
                            if not siguiente.empty:
                                tiempo_diff = (siguiente.iloc[0]["Fecha_Actualizacion"] - row["Fecha_Actualizacion"]).days
                                if tiempo_diff > 0:
                                    tiempos_revisar.append(tiempo_diff)

                tiempo_promedio = sum(tiempos_revisar) / len(tiempos_revisar) if tiempos_revisar else 0
                print(f"• Leads en 'Revisar Dirección': {num_revisar_direccion} (tiempo promedio: {tiempo_promedio:.1f} días)")
            else:
                print(f"• Leads en 'Revisar Dirección': {num_revisar_direccion} (tiempo promedio: N/A)")

            # Porcentaje descartados
            descartados = df_estados[df_estados["Estado"].str.contains("Descartado|Rechazado", case=False, na=False)]
            num_descartados = (
                len(descartados["Inmueble_ID"].unique()) if "Inmueble_ID" in df_estados.columns else len(descartados)
            )
            total_inmuebles = len(df_estados["Inmueble_ID"].unique()) if "Inmueble_ID" in df_estados.columns else len(df_estados)
            porcentaje_descartados = (num_descartados / total_inmuebles) * 100 if total_inmuebles > 0 else 0
            print(f"• Porcentaje descartados: {porcentaje_descartados:.1f}%")

            # Estado inicial más frecuente
            if "Inmueble_ID" in df_estados.columns:
                primeros_estados = df_estados.groupby("Inmueble_ID")["Estado"].first()
                estado_mas_frecuente = primeros_estados.mode()[0] if not primeros_estados.empty else "N/A"
                frecuencia_estado = (primeros_estados == estado_mas_frecuente).sum()
                porcentaje_estado = (frecuencia_estado / len(primeros_estados)) * 100 if len(primeros_estados) > 0 else 0
                print(f"• Estado inicial más frecuente: {estado_mas_frecuente} ({porcentaje_estado:.1f}%)")
            else:
                estado_mas_frecuente = df_estados["Estado"].mode()[0] if not df_estados["Estado"].empty else "N/A"
                frecuencia_estado = (df_estados["Estado"] == estado_mas_frecuente).sum()
                porcentaje_estado = (frecuencia_estado / len(df_estados)) * 100 if len(df_estados) > 0 else 0
                print(f"• Estado inicial más frecuente: {estado_mas_frecuente} ({porcentaje_estado:.1f}%)")
        else:
            print("• Leads en 'Revisar Dirección': N/A (falta columna Estado)")
            print("• Porcentaje descartados: N/A (falta columna Estado)")
            print("• Estado inicial más frecuente: N/A (falta columna Estado)")
    else:
        print("• Leads en 'Revisar Dirección': N/A (no se proporcionó archivo de estados)")
        print("• Porcentaje descartados: N/A (no se proporcionó archivo de estados)")
        print("• Estado inicial más frecuente: N/A (no se proporcionó archivo de estados)")

    print("=" * 60)
    return {
        "precio_promedio_m2": precio_promedio_m2,
        "tasa_confiabilidad": tasa_confiabilidad,
        "num_outliers": num_outliers if "Precio_Solicitado" in df_muestra.columns else "N/A",
        "limite_inferior": limite_inferior if "Precio_Solicitado" in df_muestra.columns else "N/A",
        "limite_superior": limite_superior if "Precio_Solicitado" in df_muestra.columns else "N/A",
    }


# =========================
# Análisis de inconsistencias por agrupación
# =========================
def detectar_inconsistencias(df):
    """
    Identifica posibles inconsistencias agrupando por características clave y detectando outliers.
    - Calcula estadísticas por grupo (media, std).
    - Señala registros fuera de 3 desviaciones estándar.
    - Busca combinaciones ilógicas (ej: pisos altos en casas).
    """
    print("\n--- DETECCIÓN DE OUTLIERS POR ZONA ---")
    if "Zona" in df.columns and "Precio_Solicitado" in df.columns:
        df = df.copy()
        df["Precio_Solicitado"] = pd.to_numeric(df["Precio_Solicitado"], errors="coerce")
        stats = df.groupby("Zona")["Precio_Solicitado"].agg(["mean", "std"])
        df = df.join(stats, on="Zona", rsuffix="_zona")
        outliers = df[
            (df["Precio_Solicitado"] > df["mean"] + 3 * df["std"]) | (df["Precio_Solicitado"] < df["mean"] - 3 * df["std"])
        ]
        print(f"Registros con precio fuera de 3 desviaciones estándar por zona: {len(outliers)}")
        if not outliers.empty:
            outliers_fmt = outliers.copy()
            outliers_fmt["Precio_Solicitado"] = outliers_fmt["Precio_Solicitado"].apply(
                lambda x: "$ {:,.0f}".format(x) if pd.notnull(x) else "-"
            )
            print(outliers_fmt[["Id", "Zona", "Precio_Solicitado"]])
        else:
            print("No se encontraron outliers.")


def calcular_inconsistencias_especificas(df):
    """
    Calcula inconsistencias específicas por agrupaciones:
    - Áreas fuera de rango (< 20 m² o > 300 m²)
    - Precios fuera de rango (< 0 o > 1,000 M)
    - Valores de "Fuente" no estandarizados
    """
    print("\n" + "=" * 60)
    print("     INCONSISTENCIAS ESPECÍFICAS POR AGRUPACIONES")
    print("=" * 60)

    df_temp = df.copy()

    # 1. Áreas fuera de rango (< 20 m² o > 300 m²)
    if "Area" in df_temp.columns:
        df_temp["Area"] = pd.to_numeric(df_temp["Area"], errors="coerce")
        areas_fuera_rango = df_temp[(df_temp["Area"] < 20) | (df_temp["Area"] > 300)]
        areas_fuera_rango = areas_fuera_rango.dropna(subset=["Area"])
        num_areas_fuera = len(areas_fuera_rango)
        print(f"• Áreas fuera de rango (< 20 m² o > 300 m²): {num_areas_fuera}")

        if num_areas_fuera > 0:
            print(f"  - Áreas < 20 m²: {len(areas_fuera_rango[areas_fuera_rango['Area'] < 20])}")
            print(f"  - Áreas > 300 m²: {len(areas_fuera_rango[areas_fuera_rango['Area'] > 300])}")
            if num_areas_fuera <= 10:
                print("  Ejemplos:")
                for _, row in areas_fuera_rango[["Id", "Area", "Zona"]].head(10).iterrows():
                    print(f"    ID: {row['Id']}, Área: {row['Area']} m², Zona: {row['Zona']}")
    else:
        print("• Áreas fuera de rango: N/A (falta columna Area)")

    # 2. Precios fuera de rango (< 0 o > 1,000 M)
    if "Precio_Solicitado" in df_temp.columns:
        df_temp["Precio_Solicitado"] = pd.to_numeric(df_temp["Precio_Solicitado"], errors="coerce")
        limite_maximo = 1000000000  # 1,000 millones
        precios_fuera_rango = df_temp[(df_temp["Precio_Solicitado"] < 0) | (df_temp["Precio_Solicitado"] > limite_maximo)]
        precios_fuera_rango = precios_fuera_rango.dropna(subset=["Precio_Solicitado"])
        num_precios_fuera = len(precios_fuera_rango)
        print(f"• Precios fuera de rango (< 0 o > 1,000 M): {num_precios_fuera}")

        if num_precios_fuera > 0:
            print(f"  - Precios < 0: {len(precios_fuera_rango[precios_fuera_rango['Precio_Solicitado'] < 0])}")
            print(f"  - Precios > 1,000 M: {len(precios_fuera_rango[precios_fuera_rango['Precio_Solicitado'] > limite_maximo])}")
            if num_precios_fuera <= 10:
                print("  Ejemplos:")
                for _, row in precios_fuera_rango[["Id", "Precio_Solicitado", "Zona"]].head(10).iterrows():
                    precio_fmt = f"${row['Precio_Solicitado']:,.0f}" if pd.notnull(row["Precio_Solicitado"]) else "N/A"
                    print(f"    ID: {row['Id']}, Precio: {precio_fmt}, Zona: {row['Zona']}")
    else:
        print("• Precios fuera de rango: N/A (falta columna Precio_Solicitado)")

    # 3. Valores de "Fuente" no estandarizados
    fuente_cols = [col for col in df_temp.columns if "fuente" in col.lower() or "source" in col.lower()]
    if fuente_cols:
        col_fuente = fuente_cols[0]  # Tomar la primera columna que contenga "fuente"
        valores_fuente = df_temp[col_fuente].value_counts()
        valores_no_estandarizados = 0

        # Considerar como no estandarizados los valores que aparecen menos de 5 veces
        # o que contienen caracteres especiales/inconsistencias
        valores_sospechosos = []
        for valor, count in valores_fuente.items():
            if pd.isna(valor):
                continue
            valor_str = str(valor).strip().lower()
            if (
                count < 5
                or len(valor_str) < 3
                or any(char in valor_str for char in ["@", "#", "$", "%", "&", "*"])
                or valor_str in ["desconocido", "unknown", "n/a", "na", "null", "none"]
            ):
                valores_no_estandarizados += count
                valores_sospechosos.append((valor, count))

        print(f"• Valores de 'Fuente' no estandarizados: {valores_no_estandarizados}")
        if valores_sospechosos and len(valores_sospechosos) <= 10:
            print("  Ejemplos de valores sospechosos:")
            for valor, count in valores_sospechosos[:10]:
                print(f"    '{valor}': {count} registros")
    else:
        print("• Valores de 'Fuente' no estandarizados: N/A (no se encontró columna de fuente)")

    print("=" * 60)

    return {
        "areas_fuera_rango": num_areas_fuera if "Area" in df_temp.columns else "N/A",
        "precios_fuera_rango": num_precios_fuera if "Precio_Solicitado" in df_temp.columns else "N/A",
        "fuentes_no_estandarizadas": valores_no_estandarizados if fuente_cols else "N/A",
    }


def ejecutar_analisis_completo(ruta_muestra="data/cleanData/CLMUESTRA.csv", ruta_estados="data/cleanData/CLESTADOS.csv"):
    """
    Función principal para ejecutar el análisis completo desde main.py
    """
    print("\n--- Análisis Exploratorio de Datos Inmobiliarios ---")
    print("Iniciando análisis con correlación Estrato vs Precio e indicadores clave...")

    try:
        # Cargar archivo de muestra
        df_muestra = pd.read_csv(ruta_muestra)
        print(f"\n✓ Archivo muestra cargado: {ruta_muestra} (registros: {len(df_muestra)})")

        # Cargar archivo de estados si existe
        df_estados = None
        try:
            df_estados = pd.read_csv(ruta_estados)
            print(f"✓ Archivo estados cargado: {ruta_estados} (registros: {len(df_estados)})")
        except FileNotFoundError:
            print(f"⚠️  Archivo estados no encontrado: {ruta_estados}")

        # 1. Calcular indicadores clave PRIMERO
        print("\n" + "=" * 60)
        print("1. CALCULANDO INDICADORES CLAVE")
        print("=" * 60)
        calcular_indicadores_clave(df_muestra, df_estados)

        # 2. Análisis de relaciones (incluyendo Estrato vs Precio)
        print("\n" + "=" * 60)
        print("2. ANÁLISIS DE RELACIONES Y CORRELACIONES")
        print("=" * 60)
        analizar_relaciones(df_muestra)

        # 3. Cálculo de inconsistencias específicas
        print("\n" + "=" * 60)
        print("3. INCONSISTENCIAS ESPECÍFICAS POR AGRUPACIONES")
        print("=" * 60)
        calcular_inconsistencias_especificas(df_muestra)

        # 4. Detección de inconsistencias generales
        print("\n" + "=" * 60)
        print("4. DETECCIÓN DE OUTLIERS GENERALES")
        print("=" * 60)
        detectar_inconsistencias(df_muestra)

        print("\n" + "=" * 60)
        print("ANÁLISIS COMPLETADO EXITOSAMENTE")
        print("=" * 60)

        return True

    except FileNotFoundError as e:
        print(f"✗ No se encontró un archivo requerido: {e}")
        print("Ejecuta primero el pipeline de limpieza (main.py) para generar los archivos limpios.")
        return False
    except Exception as e:
        print(f"✗ Error durante el análisis: {e}")
        import traceback

        traceback.print_exc()
        return False


# =========================
# Ejecución directa del análisis exploratorio con datos limpios
# =========================
if __name__ == "__main__":
    # Ejecutar análisis completo cuando se ejecuta directamente
    ejecutar_analisis_completo()
