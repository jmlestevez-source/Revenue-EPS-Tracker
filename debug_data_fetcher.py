import sys
import os
import pandas as pd
import numpy as np

# Añadir el directorio principal al path para poder importar los módulos
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Importar las funciones necesarias
from modules.importhtml import get_stockanalysis_table
from modules.data_fetcher import fetch_quarterly_financials, _parse_financial_value

def debug_data_extraction(ticker):
    """Función para depurar el proceso de extracción de datos."""
    print(f"\n=== Depurando extracción de datos para {ticker} ===\n")
    
    # Paso 1: Obtener la tabla completa
    print("Paso 1: Obteniendo tabla completa...")
    df_raw = get_stockanalysis_table(ticker, quarterly=True)
    
    if df_raw.empty:
        print("ERROR: La tabla extraída está vacía.")
        return
    
    print(f"Tabla extraída exitosamente. Dimensiones: {df_raw.shape}")
    print("\nPrimeras 5 filas:")
    print(df_raw.head())
    print("\nColumnas:")
    print(df_raw.columns.tolist())
    
    # Paso 2: Identificar las filas de Revenue y EPS
    print("\nPaso 2: Buscando filas de Revenue y EPS...")
    
    # Establecer la primera columna como índice
    first_col = df_raw.columns[0]
    print(f"Primera columna (índice): {first_col}")
    
    try:
        df = df_raw.set_index(first_col)
        print("Índice establecido correctamente.")
    except Exception as e:
        print(f"Error al establecer el índice: {e}")
        df = df_raw.copy()
    
    # Mostrar todos los índices para inspección
    print("\nÍndices disponibles:")
    for idx in df.index:
        print(f"  - {idx}")
    
    # Buscar las filas que contienen Revenue y EPS
    revenue_row = None
    eps_row = None
    revenue_keywords = ["revenue", "total revenue", "net revenue", "sales", "net sales"]
    eps_keywords = ["eps", "eps (diluted)", "diluted eps", "earnings per share", "diluted earnings per share"]
    
    print("\nBuscando coincidencias de palabras clave:")
    for row_name in df.index:
        row_name_str = str(row_name).lower()
        
        # Buscar fila de Revenue
        for keyword in revenue_keywords:
            if keyword in row_name_str and "cost" not in row_name_str:
                print(f"ENCONTRADO REVENUE: '{row_name}' contiene '{keyword}'")
                if revenue_row is None:
                    revenue_row = row_name
                    print(f"  → Seleccionado como revenue_row")
        
        # Buscar fila de EPS
        for keyword in eps_keywords:
            if keyword in row_name_str:
                print(f"ENCONTRADO EPS: '{row_name}' contiene '{keyword}'")
                if eps_row is None:
                    eps_row = row_name
                    print(f"  → Seleccionado como eps_row")
    
    print(f"\nResultado final de búsqueda:")
    print(f"Revenue row: {revenue_row}")
    print(f"EPS row: {eps_row}")
    
    if revenue_row is None or eps_row is None:
        print("ERROR: No se encontraron filas de Revenue o EPS.")
        print("\nSolución propuesta: Modificar las palabras clave de búsqueda o la lógica de detección.")
        return
    
    # Paso 3: Extraer fechas y valores
    print("\nPaso 3: Extrayendo fechas y valores...")
    
    dates = df.columns.tolist()
    revenue_values = df.loc[revenue_row].tolist()
    eps_values = df.loc[eps_row].tolist()
    
    print(f"Fechas (primeras 5): {dates[:5]}")
    print(f"Valores de Revenue (primeros 5): {revenue_values[:5]}")
    print(f"Valores de EPS (primeros 5): {eps_values[:5]}")
    
    # Paso 4: Crear DataFrame con los datos extraídos
    print("\nPaso 4: Creando DataFrame con los datos extraídos...")
    
    result_df = pd.DataFrame({
        'date': dates,
        'totalRevenue': revenue_values,
        'epsDiluted': eps_values
    })
    
    print(f"DataFrame creado. Dimensiones: {result_df.shape}")
    print("\nPrimeras 5 filas:")
    print(result_df.head())
    
    # Paso 5: Convertir fechas y valores financieros
    print("\nPaso 5: Convirtiendo fechas y valores financieros...")
    
    # Convertir fechas a formato datetime
    print("Convirtiendo fechas...")
    result_df['date'] = pd.to_datetime(result_df['date'], errors='coerce')
    
    # Mostrar fechas convertidas
    print("Fechas convertidas (primeras 5):")
    print(result_df['date'].head())
    
    # Verificar valores nulos en fechas
    null_dates = result_df['date'].isnull().sum()
    print(f"Valores nulos en fechas: {null_dates} de {len(result_df)}")
    
    # Convertir valores financieros a números
    print("\nConvirtiendo valores financieros...")
    result_df['totalRevenue'] = result_df['totalRevenue'].apply(_parse_financial_value)
    result_df['epsDiluted'] = result_df['epsDiluted'].apply(_parse_financial_value)
    
    # Mostrar valores convertidos
    print("Valores convertidos (primeras 5):")
    print(result_df[['totalRevenue', 'epsDiluted']].head())
    
    # Verificar valores nulos en datos financieros
    null_revenue = result_df['totalRevenue'].isnull().sum()
    null_eps = result_df['epsDiluted'].isnull().sum()
    print(f"Valores nulos en Revenue: {null_revenue} de {len(result_df)}")
    print(f"Valores nulos en EPS: {null_eps} de {len(result_df)}")
    
    # Paso 6: Eliminar filas con valores faltantes
    print("\nPaso 6: Eliminando filas con valores faltantes...")
    
    before_dropna = len(result_df)
    result_df.dropna(subset=['date', 'totalRevenue', 'epsDiluted'], inplace=True)
    after_dropna = len(result_df)
    
    print(f"Filas antes de eliminar valores nulos: {before_dropna}")
    print(f"Filas después de eliminar valores nulos: {after_dropna}")
    print(f"Filas eliminadas: {before_dropna - after_dropna}")
    
    if result_df.empty:
        print("ERROR: Todas las filas fueron eliminadas al quitar valores nulos.")
        print("\nSolución propuesta: Revisar la conversión de fechas y valores financieros.")
        return
    
    print("\nDataFrame final:")
    print(result_df)
    
    # Paso 7: Comparar con la función fetch_quarterly_financials
    print("\nPaso 7: Comparando con fetch_quarterly_financials...")
    
    df_fetch = fetch_quarterly_financials(ticker)
    
    if df_fetch.empty:
        print("ERROR: fetch_quarterly_financials devolvió un DataFrame vacío.")
    else:
        print(f"fetch_quarterly_financials devolvió un DataFrame con dimensiones: {df_fetch.shape}")
        print("\nPrimeras 5 filas:")
        print(df_fetch.head())

# Ejecutar la depuración con el ticker problemático
if __name__ == "__main__":
    ticker = "ADP"  # Ticker problemático
    debug_data_extraction(ticker)