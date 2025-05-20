import pandas as pd
import numpy as np
from curl_cffi import requests
from bs4 import BeautifulSoup
import random
import time
import re

# Lista de user agents para rotación
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"
]

def get_random_user_agent():
    """Devuelve un user agent aleatorio de la lista."""
    return random.choice(USER_AGENTS)

def exponential_backoff(retry_count, base_delay=2.0, max_delay=60.0):
    """Calcula el tiempo de espera con retroceso exponencial."""
    delay = min(max_delay, base_delay * (2 ** retry_count))
    jitter = random.uniform(0.8, 1.2)
    return delay * jitter

def importhtml(url, table_index=0):
    """Función que imita el comportamiento de IMPORTHTML en hojas de cálculo.
    
    Args:
        url (str): URL de la página web que contiene la tabla.
        table_index (int): Índice de la tabla a extraer (0 para la primera tabla).
        
    Returns:
        pd.DataFrame: DataFrame con el contenido de la tabla.
    """
    print(f"--- DEBUG [importhtml]: Extrayendo tabla {table_index} de {url} ---")
    
    # Implementar sistema de reintentos mejorado
    max_retries = 4
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # Crear una sesión nueva con curl_cffi que imita a Chrome
            session = requests.Session(impersonate="chrome")
            
            # Mejorar los headers para evitar bloqueos
            headers = {
                'User-Agent': get_random_user_agent(),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Cache-Control': 'max-age=0'
            }
            
            # Hacer la solicitud
            response = session.get(url, headers=headers, timeout=20)
            response.raise_for_status()
            break
        except Exception as e:
            retry_count += 1
            if retry_count >= max_retries:
                print(f"--- DEBUG [importhtml]: Todos los reintentos fallaron: {e} ---")
                return pd.DataFrame()
            
            wait_time = exponential_backoff(retry_count)
            print(f"--- DEBUG [importhtml]: Reintento {retry_count}/{max_retries} debido a: {e}. Esperando {wait_time:.2f}s ---")
            time.sleep(wait_time)
    
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Buscar todas las tablas en la página
    tables = soup.find_all('table')
    
    if not tables:
        print(f"--- DEBUG [importhtml]: No se encontraron tablas en la página ---")
        return pd.DataFrame()
    
    if table_index >= len(tables):
        print(f"--- DEBUG [importhtml]: Índice de tabla {table_index} fuera de rango. Solo hay {len(tables)} tablas ---")
        return pd.DataFrame()
    
    # Seleccionar la tabla según el índice
    selected_table = tables[table_index]
    
    # Extraer encabezados
    headers = []
    header_row = None
    
    # Buscar el thead primero
    thead = selected_table.find('thead')
    if thead:
        header_row = thead.find('tr')
    
    # Si no hay thead o no tiene tr, buscar en el primer tr de la tabla
    if not header_row:
        header_row = selected_table.find('tr')
    
    if header_row:
        headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]
    
    # Extraer filas de datos
    rows = []
    tbody = selected_table.find('tbody')
    
    # Si hay tbody, extraer filas de ahí
    if tbody:
        data_rows = tbody.find_all('tr')
    else:
        # Si no hay tbody, extraer todas las filas excepto la primera (que sería el encabezado)
        all_rows = selected_table.find_all('tr')
        data_rows = all_rows[1:] if all_rows and len(all_rows) > 1 else []
    
    for tr in data_rows:
        row = [td.get_text(strip=True) for td in tr.find_all(['th', 'td'])]
        if row:  # Asegurarse de que la fila no esté vacía
            rows.append(row)
    
    # Crear DataFrame
    if not rows:
        print(f"--- DEBUG [importhtml]: No se encontraron filas de datos en la tabla ---")
        return pd.DataFrame()
    
    # Asegurarse de que todas las filas tengan la misma longitud que los encabezados
    max_len = max(len(headers) if headers else 0, max(len(row) for row in rows))
    
    # Si no hay encabezados, crear encabezados genéricos
    if not headers:
        headers = [f'Column_{i}' for i in range(max_len)]
    else:
        headers = headers + [''] * (max_len - len(headers))
    
    rows = [row + [''] * (max_len - len(row)) for row in rows]
    
    df = pd.DataFrame(rows, columns=headers)
    
    print(f"--- DEBUG [importhtml]: Tabla extraída exitosamente. Dimensiones: {df.shape} ---")
    return df

def get_stockanalysis_table(ticker, quarterly=True, market=None):
    """Obtiene la tabla de datos financieros de StockAnalysis.com.
    
    Args:
        ticker (str): Símbolo del ticker.
        quarterly (bool): True para datos trimestrales, False para anuales.
        market (str, optional): Prefijo del mercado para acciones europeas (ej. 'epa' para París).
        
    Returns:
        pd.DataFrame: DataFrame con la tabla completa.
    """
    ticker = ticker.strip()
    
    # Construir la URL según el mercado
    if market:
        # URL para mercados internacionales (formato: stockanalysis.com/quote/mercado/TICKER/)
        url = f"https://stockanalysis.com/quote/{market.lower()}/{ticker}/financials/"
    else:
        # URL para mercado estadounidense (formato: stockanalysis.com/stocks/TICKER/)
        url = f"https://stockanalysis.com/stocks/{ticker.lower()}/financials/"
        
    # Añadir parámetro para datos trimestrales si es necesario
    if quarterly:
        url += "?p=quarterly"
    
    print(f"--- DEBUG [importhtml]: Obteniendo tabla para {ticker} desde {url} ---")
    
    # Implementar sistema de reintentos mejorado
    max_retries = 4
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # Crear una sesión nueva con curl_cffi que imita a Chrome
            session = requests.Session(impersonate="chrome")
            
            # Mejorar los headers para evitar bloqueos
            headers = {
                'User-Agent': get_random_user_agent(),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Cache-Control': 'max-age=0'
            }
            
            # Hacer la solicitud
            response = session.get(url, headers=headers, timeout=20)
            response.raise_for_status()
            break
        except Exception as e:
            retry_count += 1
            if retry_count >= max_retries:
                print(f"--- DEBUG [importhtml]: Todos los reintentos fallaron: {e} ---")
                return pd.DataFrame()
            
            wait_time = exponential_backoff(retry_count)
            print(f"--- DEBUG [importhtml]: Reintento {retry_count}/{max_retries} debido a: {e}. Esperando {wait_time:.2f}s ---")
            time.sleep(wait_time)
    
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Buscar todas las tablas en la página
    tables = soup.find_all('table')
    
    if not tables:
        print(f"--- DEBUG [importhtml]: No se encontraron tablas en la página ---")
        return pd.DataFrame()
    
    # Encontrar la tabla financiera principal (generalmente la más grande)
    main_table = None
    max_rows = 0
    
    for i, table in enumerate(tables):
        # Contar filas en la tabla
        rows = table.find_all('tr')
        if len(rows) > max_rows:
            max_rows = len(rows)
            main_table = table
            main_table_index = i
    
    if not main_table:
        print(f"--- DEBUG [importhtml]: No se pudo identificar la tabla principal ---")
        return pd.DataFrame()
    
    print(f"--- DEBUG [importhtml]: Tabla principal identificada con índice {main_table_index} ({max_rows} filas) ---")
    
    # Extraer la tabla usando importhtml con el índice correcto
    df = importhtml(url, table_index=main_table_index)
    
    # Si la tabla está vacía o es muy pequeña, intentar con todas las tablas
    if df.empty or df.shape[0] < 5 or df.shape[1] < 5:
        print(f"--- DEBUG [importhtml]: Tabla principal vacía o muy pequeña, intentando con todas las tablas ---")
        
        for i in range(len(tables)):
            if i != main_table_index:  # Evitar repetir la tabla principal
                print(f"--- DEBUG [importhtml]: Probando con tabla índice {i} ---")
                temp_df = importhtml(url, table_index=i)
                
                # Si encontramos una tabla con suficientes datos, usarla
                if not temp_df.empty and temp_df.shape[0] >= 5 and temp_df.shape[1] >= 5:
                    df = temp_df
                    print(f"--- DEBUG [importhtml]: Tabla válida encontrada con índice {i} ---")
                    break
    
    return df