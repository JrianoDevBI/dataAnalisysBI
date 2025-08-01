"""
-------------------------------------------------------------
clean_muestra.py
Script para limpiar y validar la calidad de los datos de muestra según reglas específicas.

Autor: Juan Camilo Riaño
Fecha: 2025-07-31

Descripción:
    Este script toma un archivo CSV de datos de muestra y realiza un proceso de limpieza basado en reglas de negocio específicas:
    - Imputación de valores nulos o vacíos.
    - Validación de campos clave según criterios definidos.
    - Generación de archivo limpio para análisis posterior.

Buenas prácticas:
    - Documentar criterios de limpieza y cambios realizados.
    - Mantener el código modular y funciones reutilizables.
    - Validar la estructura y tipos de datos antes de procesar.
    - Facilitar la trazabilidad y auditoría de los cambios.
-------------------------------------------------------------
"""



# Librerías principales
import pandas as pd
import os
import re


def imputar_vacios(df):
    """
    Imputa todos los valores vacíos o nulos en cualquier columna a 'Desconocido'.
    Esto ayuda a evitar errores en análisis posteriores y mantiene la consistencia de los datos.
    """
    df = df.fillna('Desconocido')
    df = df.replace('', 'Desconocido')
    return df


def validar_nombre_contacto(valor):
    """
    Valida la columna 'Nombre Contacto'.
    Si el campo contiene únicamente caracteres especiales o números, se imputa a 'Desconocido'.
    Esto previene registros con nombres inválidos o no informativos.
    """
    if valor == 'Desconocido':
        return valor
    # Si solo tiene caracteres especiales o números
    if re.fullmatch(r'[^a-zA-ZáéíóúÁÉÍÓÚñÑ]+', str(valor)):
        return 'Desconocido'
    return valor


def validar_telefono_contacto(valor):
    """
    Valida la columna 'Telefono Contacto'.
    El campo solo es válido si contiene tanto números como asteriscos (*).
    Si solo tiene números o solo asteriscos, se imputa a 'Desconocido'.
    """
    if valor == 'Desconocido':
        return valor
    tel_str = str(valor)
    tiene_num = bool(re.search(r'\d', tel_str))
    tiene_ast = '*' in tel_str
    # Solo válido si tiene ambos: números y asteriscos
    if (tiene_num and tiene_ast):
        return valor
    else:
        return 'Desconocido'


def validar_precio_solicitado(valor):
    """
    Valida la columna 'Precio Solicitado'.
    Si el valor es inferior a 50,000,000 se imputa a 'Desconocido'.
    Esto ayuda a filtrar registros con precios atípicos o erróneos.
    """
    try:
        return valor if float(valor) >= 50000000 else 'Desconocido'
    except:
        return 'Desconocido'


def validar_piso(valor):
    """
    Valida la columna 'Piso'.
    Si el valor es mayor a 67 o menor a 1 se imputa a 'Desconocido'.
    Esto previene registros con pisos fuera de rango razonable.
    """
    try:
        piso = int(float(valor))
        return valor if 1 <= piso <= 67 else 'Desconocido'
    except:
        return 'Desconocido'


def validar_antiguedad(valor):
    """
    Valida la columna 'Antiguedad (Años)'.
    Si el valor es mayor a 100 se imputa a 'Desconocido'.
    Esto ayuda a evitar antigüedades poco realistas en los registros.
    """
    try:
        return valor if float(valor) <= 100 else 'Desconocido'
    except:
        return 'Desconocido'


def clean_muestra(input_path, output_path, outliers_log_path=None):
    """
    Limpia la muestra de datos aplicando reglas de imputación y validación específicas.

    Proceso:
        1. Validar existencia del archivo de entrada.
        2. Leer el archivo y validar columnas clave.
        3. Imputar valores vacíos o nulos a 'Desconocido'.
        4. Validar y limpiar columnas según reglas de negocio.
        5. Guardar el archivo limpio para análisis posterior.

    Manejo de errores:
        - FileNotFoundError: Si el archivo de entrada no existe.
        - ValueError: Si faltan columnas clave en el archivo.
    """
    # 1. Validar existencia del archivo de entrada
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"No se encontró el archivo de entrada: {input_path}")

    # 2. Leer el archivo de entrada
    df = pd.read_csv(input_path)

    # 3. Validar columnas clave

    columnas_requeridas = [
        'Id', 'Telefono_Contacto', 'Precio_Solicitado', 'Zona', 'Tipo_Inmueble', 'Fuente',
        'Area', 'Nombre_Contacto', 'Piso', 'Antiguedad_Annos', 'Ciudad', 'Lote_Id',
        'Garajes', 'Ascensores', 'Estrato'
    ]
    for col in columnas_requeridas:
        if col not in df.columns:
            raise ValueError(f"Falta la columna requerida: {col}")

    # 4. Imputar vacíos o nulos
    df = imputar_vacios(df)


    # 5. Validar y limpiar columnas según reglas de negocio
    # Reemplazar Ñ/ñ por N/n en Nombre_Contacto
    df['Nombre_Contacto'] = df['Nombre_Contacto'].apply(lambda x: x.replace('Ñ', 'N').replace('ñ', 'n') if isinstance(x, str) else x)
    # Validar Nombre_Contacto
    df['Nombre_Contacto'] = df['Nombre_Contacto'].apply(validar_nombre_contacto)
    df['Telefono_Contacto'] = df['Telefono_Contacto'].apply(validar_telefono_contacto)
    df['Precio_Solicitado'] = df['Precio_Solicitado'].apply(validar_precio_solicitado)
    df['Piso'] = df['Piso'].apply(validar_piso)
    df['Antiguedad_Annos'] = df['Antiguedad_Annos'].apply(validar_antiguedad)

    # Reemplazar tildes por vocales simples en Zona, Ciudad y Nombre_Contacto para evitar errores de codificación
    def quitar_tildes(texto):
        if not isinstance(texto, str):
            return texto
        reemplazos = (
            ("á", "a"), ("é", "e"), ("í", "i"), ("ó", "o"), ("ú", "u"),
            ("Á", "A"), ("É", "E"), ("Í", "I"), ("Ó", "O"), ("Ú", "U")
        )
        for orig, repl in reemplazos:
            texto = texto.replace(orig, repl)
        return texto

    for col in ['Zona', 'Ciudad', 'Nombre_Contacto']:
        if col in df.columns:
            df[col] = df[col].apply(quitar_tildes)



    # Reportar cantidad de imputaciones por columna
    print('\nResumen de imputaciones por columna:')
    for col in df.columns:
        imputados = (df[col] == 'Desconocido').sum()
        if imputados > 0:
            print(f'  {col}: {imputados} imputaciones')

    # Reportar cantidad de registros totales y registros con 1 o más imputaciones
    total_registros = len(df)
    registros_con_imputacion = (df == 'Desconocido').any(axis=1).sum()
    print(f'\nTotal de registros: {total_registros}')
    if total_registros > 0:
        porcentaje = (registros_con_imputacion / total_registros) * 100
    else:
        porcentaje = 0
    print(f'Registros con 1 o más imputaciones: {registros_con_imputacion} lo cual representa el {porcentaje:.2f}%')

    # Guardar el archivo limpio en la ruta de salida indicada
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f'Limpieza de muestra finalizada. Archivo limpio generado en {output_path} con imputaciones a "Desconocido".')


# Punto de entrada del script
if __name__ == '__main__':
    # Rutas de entrada y salida (ajustar según sea necesario)
    clean_muestra(
        '../data/processedData/muestra.csv',
        '../data/processedData/muestra.csv'
    )
