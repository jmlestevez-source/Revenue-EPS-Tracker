import sys
import os
import pandas as pd

# Añadir el directorio principal al path para poder importar los módulos
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importar directamente las funciones que necesitamos probar
from modules.importhtml import get_stockanalysis_table, importhtml
from modules.data_fetcher import scrape_financial_data, fetch_quarterly_financials

def test_importhtml_functions():
    """Prueba las funciones de importhtml de forma independiente"""
    print("\n=== Probando funciones de importhtml ===\n")
    
    ticker = "AAPL"  # Usar un ticker conocido para la prueba
    
    # Probar la función importhtml directamente
    print(f"Probando importhtml con {ticker}...")
    url = f"https://stockanalysis.com/stocks/{ticker.lower()}/financials/?p=quarterly"
    df_importhtml = importhtml(url)
    
    if not df_importhtml.empty:
        print(f"✓ importhtml funcionó correctamente. Dimensiones: {df_importhtml.shape}")
        print(f"Primeras columnas: {df_importhtml.columns.tolist()[:5]}")
    else:
        print("✗ importhtml falló - DataFrame vacío")
    
    # Probar la función get_stockanalysis_table
    print(f"\nProbando get_stockanalysis_table con {ticker}...")
    df_stockanalysis = get_stockanalysis_table(ticker, quarterly=True)
    
    if not df_stockanalysis.empty:
        print(f"✓ get_stockanalysis_table funcionó correctamente. Dimensiones: {df_stockanalysis.shape}")
        print(f"Primeras columnas: {df_stockanalysis.columns.tolist()[:5]}")
    else:
        print("✗ get_stockanalysis_table falló - DataFrame vacío")

def test_data_fetcher_functions():
    """Prueba las funciones de data_fetcher que utilizan importhtml"""
    print("\n=== Probando funciones de data_fetcher ===\n")
    
    ticker = "AAPL"  # Usar un ticker conocido para la prueba
    
    # Probar la función scrape_financial_data
    print(f"Probando scrape_financial_data con {ticker}...")
    df_scrape = scrape_financial_data(ticker)
    
    if not df_scrape.empty:
        print(f"✓ scrape_financial_data funcionó correctamente. Dimensiones: {df_scrape.shape}")
        print(f"Columnas: {df_scrape.columns.tolist()}")
        print(f"Últimas filas:\n{df_scrape.tail(3)}")
    else:
        print("✗ scrape_financial_data falló - DataFrame vacío")
    
    # Probar la función fetch_quarterly_financials
    print(f"\nProbando fetch_quarterly_financials con {ticker}...")
    df_fetch = fetch_quarterly_financials(ticker)
    
    if not df_fetch.empty:
        print(f"✓ fetch_quarterly_financials funcionó correctamente. Dimensiones: {df_fetch.shape}")
        print(f"Columnas: {df_fetch.columns.tolist()}")
        print(f"Últimas filas:\n{df_fetch.tail(3)}")
    else:
        print("✗ fetch_quarterly_financials falló - DataFrame vacío")

if __name__ == "__main__":
    print("=== INICIANDO PRUEBAS DE IMPORTHTML Y DATA_FETCHER ===\n")
    
    # Ejecutar las pruebas
    test_importhtml_functions()
    test_data_fetcher_functions()
    
    print("\n=== PRUEBAS COMPLETADAS ===\n")