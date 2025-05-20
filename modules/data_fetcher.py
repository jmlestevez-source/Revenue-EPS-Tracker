import sys
import os
import pandas as pd
import numpy as np
from curl_cffi import requests  # Usar curl_cffi.requests en lugar del estándar
from bs4 import BeautifulSoup
from datetime import datetime
import re # Import regex for cleaning numbers
import time
import random
from typing import List, Dict, Optional, Union, Tuple

# Importar la nueva función que imita IMPORTHTML
from .importhtml import get_stockanalysis_table, importhtml


def _parse_financial_value(value_str):
    """Helper function to parse financial values like '123.45B' or '(67.89M)' into numbers."""
    if not isinstance(value_str, str):
        return None # Return None if not a string
    value_str = value_str.strip()
    # Handle em-dash, hyphen, common NA strings, or empty string
    if value_str in ['\u2014', '-', 'N/A', 'n/a', ''] or not value_str: # Added 'or not value_str'
        return None # Changed from 0.0 to None for better NA handling

    multiplier = 1
    # Ensure case-insensitivity for B, M, K
    temp_value_str = value_str.upper()
    if temp_value_str.endswith('B'):
        multiplier = 1_000_000_000
        value_str = value_str[:-1]
    elif temp_value_str.endswith('M'):
        multiplier = 1_000_000
        value_str = value_str[:-1]
    elif temp_value_str.endswith('K'):
        multiplier = 1_000
        value_str = value_str[:-1]

    # Remove parentheses for negative numbers and commas
    value_str = value_str.replace('(', '-').replace(')', '').replace(',', '')

    try:
        return float(value_str) * multiplier
    except ValueError:
        return None # Return None if conversion fails

def _calculate_ttm(df: pd.DataFrame) -> pd.DataFrame:
    """Calculates Trailing Twelve Months (TTM) for Revenue and EPS."""
    df = df.sort_values(by='date').copy()
    # Ensure numeric types before rolling sum
    df['totalRevenue'] = pd.to_numeric(df['totalRevenue'], errors='coerce')
    df['epsDiluted'] = pd.to_numeric(df['epsDiluted'], errors='coerce')

    # Calculate TTM by summing the last 4 quarters
    df['ttmRevenue'] = df['totalRevenue'].rolling(window=4, min_periods=4).sum()
    df['ttmEpsDiluted'] = df['epsDiluted'].rolling(window=4, min_periods=4).sum()
    return df

def fetch_quarterly_financials(ticker: str) -> pd.DataFrame:
    """Fetches quarterly or semi-annual financials using StockAnalysis.com data.

    Args:
        ticker (str): The stock ticker symbol. Can be in format 'AAPL' for US stocks or 'epa/RMS' for European stocks.
                     Supported European markets: etr (Alemania), epa (Francia), ams (Holanda), lon (Reino Unido),
                     bit (Italia), bme (España), eli (Portugal), sto (Suecia).
                     Handles both quarterly (Q1, Q2, Q3, Q4) and semi-annual (H1, H2) reporting formats.

    Returns:
        pd.DataFrame: DataFrame with columns ['date', 'totalRevenue', 'epsDiluted', 'ttmRevenue', 'ttmEpsDiluted', 'last_updated', 'company_name'].
                      Returns an empty DataFrame if data is not available.
    """
    # Validar el ticker antes de intentar obtener datos
    if not ticker or not isinstance(ticker, str) or len(ticker) > 20:  # Aumentado a 20 para permitir formato mercado/ticker
        print(f"Formato de ticker inválido: {ticker}")
        return pd.DataFrame(columns=["date", "totalRevenue", "epsDiluted", "ttmRevenue", "ttmEpsDiluted", "last_updated", "company_name"])
    
    # Normalizar el ticker (eliminar espacios y convertir a mayúsculas)
    ticker = ticker.strip().upper()
    
    # Detectar si el ticker incluye un prefijo de mercado (formato: mercado/símbolo)
    market_prefix = None
    ticker_symbol = ticker
    if '/' in ticker:
        parts = ticker.split('/')
        if len(parts) == 2:
            market_prefix = parts[0].lower()  # El mercado siempre en minúsculas (epa, xetr, etc.)
            ticker_symbol = parts[1]          # El símbolo mantiene el formato original
    
    print(f"Obteniendo datos financieros para {ticker_symbol}{' en mercado ' + market_prefix if market_prefix else ''}")
    try:
        # Usar la función get_stockanalysis_table para obtener los datos
        # Si hay un prefijo de mercado, se lo pasamos a la función
        df_raw = get_stockanalysis_table(ticker_symbol, quarterly=True, market=market_prefix)
        
        if df_raw.empty:
            print(f"No se encontraron datos para {ticker}")
            return pd.DataFrame(columns=["date", "totalRevenue", "epsDiluted", "ttmRevenue", "ttmEpsDiluted", "last_updated", "company_name"])
        
        # Procesar la tabla obtenida
        # Verificar si la tabla tiene la estructura esperada
        if len(df_raw.columns) < 2:
            print(f"La tabla no tiene suficientes columnas para {ticker}")
            return pd.DataFrame(columns=["date", "totalRevenue", "epsDiluted", "ttmRevenue", "ttmEpsDiluted", "last_updated", "company_name"])
        
        # Establecer la primera columna como índice si no lo está ya
        first_col = df_raw.columns[0]
        if first_col == '' or first_col == 'Column_0':
            # Si la primera columna no tiene nombre, intentar con la segunda columna
            if len(df_raw.columns) > 1:
                first_col = df_raw.columns[1]
            else:
                print(f"No se encontraron columnas válidas para {ticker}")
                return pd.DataFrame(columns=["date", "totalRevenue", "epsDiluted", "ttmRevenue", "ttmEpsDiluted", "last_updated", "company_name"])
        
        try:
            df = df_raw.set_index(first_col)
        except Exception as e:
            print(f"Error al establecer el índice: {e}. Intentando con índice numérico")
            df = df_raw.copy()
        
        # Buscar las filas que contienen Revenue y EPS
        revenue_row = None
        eps_row = None
        revenue_keywords = ["revenue", "total revenue", "net revenue", "sales", "net sales"]
        eps_keywords = ["eps", "eps (diluted)", "diluted eps", "earnings per share", "diluted earnings per share"]
        
        # Buscar con prioridad las filas específicas
        for row_name in df.index:
            row_name_str = str(row_name).lower()
            
            # Buscar fila de Revenue
            if revenue_row is None:
                for keyword in revenue_keywords:
                    if keyword in row_name_str and "cost" not in row_name_str:
                        revenue_row = row_name
                        break
            
            # Buscar fila de EPS
            if eps_row is None:
                for keyword in eps_keywords:
                    if keyword in row_name_str:
                        eps_row = row_name
                        break
            
            # Si ya encontramos ambas filas, salir del bucle
            if revenue_row is not None and eps_row is not None:
                break
        
        if revenue_row is None or eps_row is None:
            print(f"No se encontraron filas de Revenue o EPS para {ticker}")
            return pd.DataFrame(columns=["date", "totalRevenue", "epsDiluted", "ttmRevenue", "ttmEpsDiluted", "last_updated", "company_name"])
        
        # Extraer las fechas (columnas) y los valores
        dates = df.columns.tolist()
        revenue_values = df.loc[revenue_row].tolist()
        eps_values = df.loc[eps_row].tolist()
        
        # Crear DataFrame con los datos extraídos
        result_df = pd.DataFrame({
            'date': dates,
            'totalRevenue': revenue_values,
            'epsDiluted': eps_values
        })
        
        # Convertir fechas de formato 'Q1 2023' o 'H1 2023' a formato datetime
        def convert_quarter_to_date(quarter_str):
            try:
                if not isinstance(quarter_str, str):
                    return None
                    
                # Limpiar y normalizar el formato
                quarter_str = quarter_str.strip().upper()
                
                # Manejar formatos como 'Q1 2023' (trimestral)
                if quarter_str.startswith('Q'):
                    parts = quarter_str.split()
                    if len(parts) == 2:
                        quarter = parts[0][1]  # Extraer el número después de 'Q'
                        year = parts[1]
                        
                        # Mapear trimestre a mes
                        quarter_to_month = {'1': '03', '2': '06', '3': '09', '4': '12'}
                        month = quarter_to_month.get(quarter)
                        
                        if month and year.isdigit():
                            return pd.Timestamp(f"{year}-{month}-30")
                
                # Manejar formatos como 'H1 2023' o 'H2 2023' (semestral)
                elif quarter_str.startswith('H'):
                    parts = quarter_str.split()
                    if len(parts) == 2:
                        half = parts[0][1]  # Extraer el número después de 'H'
                        year = parts[1]
                        
                        # Mapear semestre a mes (H1 = junio, H2 = diciembre)
                        half_to_month = {'1': '06', '2': '12'}
                        month = half_to_month.get(half)
                        
                        if month and year.isdigit():
                            return pd.Timestamp(f"{year}-{month}-30")
                
                # Intentar convertir directamente si no coincide con los formatos anteriores
                return pd.to_datetime(quarter_str, errors='coerce')
            except Exception:
                return None
        
        # Aplicar la conversión de fechas
        result_df['date'] = result_df['date'].apply(convert_quarter_to_date)
        
        # Convertir valores financieros a números
        result_df['totalRevenue'] = result_df['totalRevenue'].apply(_parse_financial_value)
        result_df['epsDiluted'] = result_df['epsDiluted'].apply(_parse_financial_value)
        
        # Eliminar filas con valores faltantes
        result_df.dropna(subset=['date', 'totalRevenue', 'epsDiluted'], inplace=True)
        
        # Verificar si hay datos después de la limpieza
        if result_df.empty:
            print(f"No hay datos válidos después de la limpieza para {ticker}")
            # Intentar un enfoque alternativo si todas las filas fueron eliminadas
            try:
                # Crear fechas artificiales basadas en los trimestres
                dates_alt = []
                for date_str in dates:
                    date_obj = convert_quarter_to_date(date_str)
                    if date_obj is not None:
                        dates_alt.append(date_obj)
                    else:
                        dates_alt.append(None)
                
                # Crear un nuevo DataFrame con las fechas convertidas
                result_df_alt = pd.DataFrame({
                    'date': dates_alt,
                    'totalRevenue': revenue_values,
                    'epsDiluted': eps_values
                })
                
                # Convertir valores financieros a números
                result_df_alt['totalRevenue'] = result_df_alt['totalRevenue'].apply(_parse_financial_value)
                result_df_alt['epsDiluted'] = result_df_alt['epsDiluted'].apply(_parse_financial_value)
                
                # Eliminar filas con valores faltantes
                result_df_alt.dropna(subset=['date', 'totalRevenue', 'epsDiluted'], inplace=True)
                
                if not result_df_alt.empty:
                    print(f"Se recuperaron {len(result_df_alt)} filas usando el enfoque alternativo")
                    result_df = result_df_alt
            except Exception as e:
                print(f"Error en el enfoque alternativo: {e}")
                pass
        
        if result_df.empty:
            print(f"No hay datos válidos después de la limpieza para {ticker}")
            return pd.DataFrame(columns=["date", "totalRevenue", "epsDiluted", "ttmRevenue", "ttmEpsDiluted", "last_updated", "company_name"])
        
        # Calcular TTM
        result_df = _calculate_ttm(result_df)
        
        # Añadir información adicional
        result_df['last_updated'] = pd.Timestamp.now()
        
        # Obtener el nombre de la compañía
        try:
            # Construir la URL correcta según si es un ticker con mercado o no
            # Lista de mercados europeos conocidos
            european_markets = ['etr', 'epa', 'ams', 'LON', 'bit', 'bme', 'eli', 'sto']
            
            if market_prefix and market_prefix.lower() in european_markets:
                url = f"https://stockanalysis.com/quote/{market_prefix.lower()}/{ticker_symbol}/"
            elif market_prefix:
                # Para otros mercados no reconocidos, intentar con el formato de mercado
                url = f"https://stockanalysis.com/quote/{market_prefix.lower()}/{ticker_symbol}/"
            else:
                # URL para mercado estadounidense
                url = f"https://stockanalysis.com/stocks/{ticker_symbol.lower()}/"
                
            session = requests.Session(impersonate="chrome")
            response = session.get(url, headers={'User-Agent': get_random_user_agent()}, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')
            title_tag = soup.find('title')
            company_name = ticker  # Usar el ticker original como valor predeterminado
            
            if title_tag and title_tag.string:
                match = re.search(r"^(.*?)\s*\(", title_tag.string)
                if match:
                    company_name = match.group(1).strip()
                else:
                    name_part = title_tag.string.split('(')[0].strip()
                    if name_part and "not found" not in name_part.lower() and "error" not in name_part.lower():
                        company_name = name_part
        except Exception as e:
            print(f"Error al obtener el nombre de la compañía: {e}")
            company_name = ticker  # Usar el ticker original como valor predeterminado
        
        # Asegurarse de que el nombre de la compañía no incluya el ticker duplicado
        # Por ejemplo, evitar que aparezca como "V-V" y en su lugar mostrar "V-Visa Inc."
        if company_name.upper() == ticker or (ticker_symbol and company_name.upper() == ticker_symbol):
            # Si solo tenemos el ticker, lo dejamos tal cual
            result_df['company_name'] = company_name
        else:
            # Determinar qué ticker usar para la limpieza (el símbolo sin el mercado)
            clean_ticker = ticker_symbol if '/' in ticker else ticker
            
            # Eliminar el ticker del nombre si aparece al principio
            ticker_pattern = re.compile(f'^{re.escape(clean_ticker)}\\s*[-:.]?\\s*', re.IGNORECASE)
            clean_name = re.sub(ticker_pattern, '', company_name).strip()
            
            # Si después de la limpieza el nombre está vacío, usar el nombre original
            if not clean_name:
                clean_name = company_name
            
            # Para tickers europeos, añadir el prefijo del mercado para mayor claridad
            if market_prefix:
                result_df['company_name'] = f"{market_prefix.upper()}/{clean_name}"
            else:
                result_df['company_name'] = clean_name
        
        # Ordenar por fecha y reiniciar índice
        result_df = result_df.sort_values(by='date').reset_index(drop=True)
        
        # Asegurar que todas las columnas esperadas estén presentes
        final_cols = ["date", "totalRevenue", "epsDiluted", "ttmRevenue", "ttmEpsDiluted", "last_updated", "company_name"]
        for col in final_cols:
            if col not in result_df.columns:
                result_df[col] = pd.NA
        
        return result_df[final_cols]
        
    except Exception as e:
        print(f"Error al procesar datos para {ticker}: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame(columns=["date", "totalRevenue", "epsDiluted", "ttmRevenue", "ttmEpsDiluted", "last_updated", "company_name"])

# Lista de user agents para rotación
USER_AGENTS: List[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"
]

def get_random_user_agent() -> str:
    """Devuelve un user agent aleatorio de la lista."""
    return random.choice(USER_AGENTS)

def exponential_backoff(retry_count: int, base_delay: float = 2.0, max_delay: float = 60.0) -> float:
    """Calcula el tiempo de espera con retroceso exponencial.
    
    Args:
        retry_count: Número actual de reintentos
        base_delay: Tiempo base de espera en segundos
        max_delay: Tiempo máximo de espera en segundos
        
    Returns:
        Tiempo de espera en segundos
    """
    delay = min(max_delay, base_delay * (2 ** retry_count))
    # Añadir un poco de aleatoriedad (jitter) para evitar sincronización
    jitter = random.uniform(0.8, 1.2)
    return delay * jitter

# Ensure this block is the only __main__ block and is at the very end of the file.
if __name__ == "__main__":
    debug_log_file_path = os.path.join(os.path.dirname(__file__), "debug_data_fetcher.txt") # Changed filename
    with open(debug_log_file_path, "w", encoding="utf-8") as f_debug:
        f_debug.write(f"--- DEBUG LOG: MAIN BLOCK STARTED AT {datetime.now()} ---\n\n")

    print(f"--- MAIN BLOCK STARTED (stdout) AT {datetime.now()} ---", flush=True)
    print(f"--- MAIN BLOCK STARTED (stderr) AT {datetime.now()} ---", file=sys.stderr, flush=True)

    tickers_to_test = ["AAPL", "ADP", "MSFT", "GOOGL", "EPA/RMS", "ETR/VOW3", "NONEXISTENTTICKER"] # Incluidos tickers europeos
    functions_to_test = {
        "fetch_quarterly_financials": fetch_quarterly_financials,
    }

    for ticker in tickers_to_test:
        for func_name, func in functions_to_test.items():
            print(f"\n--- TESTING {func_name} with {ticker} ---", flush=True)
            try:
                result = func(ticker)
                if isinstance(result, pd.DataFrame):
                    print(f"Result shape: {result.shape}", flush=True)
                    if not result.empty:
                        print("\nFirst 3 rows:", flush=True)
                        print(result.head(3), flush=True)
                        print("\nLast 3 rows:", flush=True)
                        print(result.tail(3), flush=True)
                    else:
                        print("Result is empty DataFrame", flush=True)
                else:
                    print(f"Result: {result}", flush=True)
            except Exception as e:
                print(f"Error testing {func_name} with {ticker}: {e}", flush=True)
                import traceback
                traceback.print_exc()

    print(f"\n--- MAIN BLOCK COMPLETED AT {datetime.now()} ---", flush=True)