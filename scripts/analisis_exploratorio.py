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
    # Solo comparar Area y Precio_Solicitado si existen ambas columnas
    if 'Area' in df.columns and 'Precio_Solicitado' in df.columns:
        df_corr = df[['Area', 'Precio_Solicitado']].copy()
        df_corr['Area'] = pd.to_numeric(df_corr['Area'], errors='coerce')
        df_corr['Precio_Solicitado'] = pd.to_numeric(df_corr['Precio_Solicitado'], errors='coerce')
        correlacion = df_corr.corr()
        variables = list(correlacion.columns)
        print(f"Variables numéricas comparadas: {', '.join(variables)}")
        print(correlacion)
        plt.figure(figsize=(6, 5))
        sns.heatmap(correlacion, annot=True, cmap='coolwarm', vmin=-1, vmax=1)
        plt.title('Matriz de correlación (Área vs Precio Solicitado)')
        plt.show()

        # Conclusión automática simple
        print("\n--- Conclusión automática de la matriz de correlación ---")
        corr_val = correlacion.loc['Area', 'Precio_Solicitado']
        print(f"La correlación entre 'Area' y 'Precio_Solicitado' es de {corr_val:.2f}.")
        if abs(corr_val) > 0.7:
            print("Existe una relación fuerte entre el área y el precio solicitado.")
        elif abs(corr_val) > 0.4:
            print("Existe una relación moderada entre el área y el precio solicitado.")
        else:
            print("No se observa una relación fuerte entre el área y el precio solicitado.")
    else:
        print("No hay suficientes variables numéricas para comparar (se requieren 'Area' y 'Precio_Solicitado').")



    # Bubble plot interactivo vertical con Plotly
    if 'Area' in df.columns and 'Precio_Solicitado' in df.columns and 'Zona' in df.columns:
        try:
            import plotly.express as px
        except ImportError:
            print("Plotly no está instalado. Ejecuta 'pip install plotly' para gráficos interactivos.")
            return
        df = df.copy()
        df['Area'] = pd.to_numeric(df['Area'], errors='coerce')
        df['Precio_Solicitado'] = pd.to_numeric(df['Precio_Solicitado'], errors='coerce')
        resumen = df.groupby('Zona').agg({'Area': 'mean', 'Precio_Solicitado': 'mean', 'Zona': 'count'}).rename(columns={'Zona': 'Cantidad'})
        resumen = resumen.reset_index()
        # Bubble plot vertical: eje x = Zona, eje y = Precio, tamaño = cantidad, color = Area promedio
        fig = px.scatter(
            resumen,
            x='Zona',
            y='Precio_Solicitado',
            size='Cantidad',
            color='Area',
            hover_name='Zona',
            hover_data={'Cantidad': True, 'Area': True, 'Precio_Solicitado': True},
            size_max=60,
            height=800,
        )
        fig.update_traces(marker=dict(line=dict(width=1, color='DarkSlateGrey')))
        fig.update_layout(
            title='Bubble plot interactivo: Precio Solicitado promedio por Zona (tamaño=frecuencia, color=Área promedio)',
            xaxis_title='Zona',
            yaxis_title='Precio Solicitado promedio',
            xaxis_tickangle=45,
            showlegend=True,
            margin=dict(l=40, r=40, t=80, b=200),
        )
        fig.update_xaxes(tickfont=dict(size=10))
        fig.show()
        print(f"\n--- Bubble plot interactivo generado: Precio Solicitado promedio por Zona (tamaño=frecuencia, color=Área promedio) ---")


    # Gráfico violin interactivo de precio por zona (ordenado de menor a mayor precio promedio)
    if 'Zona' in df.columns and 'Precio_Solicitado' in df.columns:
        try:
            import plotly.express as px
        except ImportError:
            print("Plotly no está instalado. Ejecuta 'pip install plotly' para gráficos interactivos.")
            return
        df = df.copy()
        df['Precio_Solicitado'] = pd.to_numeric(df['Precio_Solicitado'], errors='coerce')
        orden_zonas = df.groupby('Zona')['Precio_Solicitado'].mean().sort_values().index
        fig = px.violin(
            df,
            x='Zona',
            y='Precio_Solicitado',
            category_orders={'Zona': list(orden_zonas)},
            box=True,
            points='all',
            hover_data=['Zona', 'Precio_Solicitado'],
            height=700,
        )
        fig.update_layout(
            title='Distribución interactiva (Violin Plot) de Precio Solicitado por Zona (ordenado)',
            xaxis_title='Zona',
            yaxis_title='Precio Solicitado',
            xaxis_tickangle=45,
            margin=dict(l=40, r=40, t=80, b=200),
        )
        fig.update_xaxes(tickfont=dict(size=10))
        fig.show()
        # Conclusión automática para violin plot
        print("\n--- Conclusión automática del violin plot Precio Solicitado por Zona ---")
        zona_mas_cara = df.groupby('Zona')['Precio_Solicitado'].mean().idxmax()
        zona_mas_barata = df.groupby('Zona')['Precio_Solicitado'].mean().idxmin()
        print(f"La zona con mayor precio promedio es '{zona_mas_cara}'.")
        print(f"La zona con menor precio promedio es '{zona_mas_barata}'.")

    # Tabla resumen de precio promedio por tipo de inmueble (agrupando y normalizando nombres)
    if 'Tipo_Inmueble' in df.columns and 'Precio_Solicitado' in df.columns:
        df = df.copy()
        df['Precio_Solicitado'] = pd.to_numeric(df['Precio_Solicitado'], errors='coerce')
        # Normalizar nombres: minúsculas y quitar tildes/espacios
        df['Tipo_Inmueble_norm'] = (
            df['Tipo_Inmueble']
            .astype(str)
            .str.lower()
            .str.normalize('NFKD')
            .str.encode('ascii', errors='ignore')
            .str.decode('utf-8')
            .str.strip()
        )
        resumen = df.groupby('Tipo_Inmueble_norm')['Precio_Solicitado'].mean().sort_values(ascending=False)
        conteo = df['Tipo_Inmueble_norm'].value_counts().reindex(resumen.index)
        # Formatear como moneda COP
        resumen_moneda = resumen.apply(lambda x: "$ {:,.0f}".format(x) if pd.notnull(x) else "-")
        print("\n--- Precio promedio por Tipo de Inmueble (agrupado, normalizado y en moneda local) ---")
        print(resumen_moneda)

        # Gráfico 1: casas, apartamentos y desconocido
        tipos_1 = ['casa', 'apartamento', 'desconocido']
        resumen_1 = resumen[resumen.index.isin(tipos_1)]
        conteo_1 = conteo[resumen_1.index]
        plt.figure(figsize=(10, 5))
        bars1 = plt.bar(resumen_1.index, resumen_1.values, color='skyblue', edgecolor='black')
        plt.title('Precio promedio por Tipo de Inmueble (COP)\n(Casas, Apartamentos y Desconocido)')
        plt.xlabel('Tipo de Inmueble')
        plt.ylabel('Precio promedio (COP)')
        plt.xticks(rotation=30, ha='right')
        for i, (v, c) in enumerate(zip(resumen_1.values, conteo_1.values)):
            plt.text(i, v, f"$ {v:,.0f}\n({c} registros)", ha='center', va='bottom', fontsize=9, fontweight='bold')
        plt.tight_layout()
        plt.show()

        # Gráfico 2: solo casas y apartamentos
        tipos_2 = ['casa', 'apartamento']
        resumen_2 = resumen[resumen.index.isin(tipos_2)]
        conteo_2 = conteo[resumen_2.index]
        plt.figure(figsize=(8, 5))
        bars2 = plt.bar(resumen_2.index, resumen_2.values, color='lightgreen', edgecolor='black')
        plt.title('Precio promedio por Tipo de Inmueble (COP)\n(Solo Casas y Apartamentos)')
        plt.xlabel('Tipo de Inmueble')
        plt.ylabel('Precio promedio (COP)')
        plt.xticks(rotation=30, ha='right')
        for i, (v, c) in enumerate(zip(resumen_2.values, conteo_2.values)):
            plt.text(i, v, f"$ {v:,.0f}\n({c} registros)", ha='center', va='bottom', fontsize=9, fontweight='bold')
        plt.tight_layout()
        plt.show()

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
    if 'Zona' in df.columns and 'Precio_Solicitado' in df.columns:
        df = df.copy()
        df['Precio_Solicitado'] = pd.to_numeric(df['Precio_Solicitado'], errors='coerce')
        stats = df.groupby('Zona')['Precio_Solicitado'].agg(['mean', 'std'])
        df = df.join(stats, on='Zona', rsuffix='_zona')
        outliers = df[(df['Precio_Solicitado'] > df['mean'] + 3*df['std']) |
                      (df['Precio_Solicitado'] < df['mean'] - 3*df['std'])]
        print(f"Registros con precio fuera de 3 desviaciones estándar por zona: {len(outliers)}")
        if not outliers.empty:
            outliers_fmt = outliers.copy()
            outliers_fmt['Precio_Solicitado'] = outliers_fmt['Precio_Solicitado'].apply(lambda x: "$ {:,.0f}".format(x) if pd.notnull(x) else "-")
            print(outliers_fmt[['Id', 'Zona', 'Precio_Solicitado']])
        else:
            print("No se encontraron outliers.")


# =========================
# Ejecución directa del análisis exploratorio con datos limpios
# =========================
if __name__ == "__main__":
    # Ruta al archivo limpio generado por el pipeline
    ruta = "data/cleanData/CLMUESTRA.csv"
    try:
        df = pd.read_csv(ruta)
        print(f"\nAnálisis exploratorio sobre {ruta} (registros: {len(df)})")
        analizar_relaciones(df)
        detectar_inconsistencias(df)
    except FileNotFoundError:
        print(f"No se encontró el archivo {ruta}. Ejecuta primero el pipeline de limpieza.")
