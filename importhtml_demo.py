import pandas as pd
import requests
from bs4 import BeautifulSoup
import sys

def importhtml_stockanalysis(ticker):
    """
    Función que imita el comportamiento de IMPORTHTML en hojas de cálculo
    para extraer tablas financieras de StockAnalysis.com.
    
    Args:
        ticker (str): Símbolo del ticker (ej. TSCO, AAPL, MSFT)
        
    Returns:
        pd.DataFrame: DataFrame con la tabla completa de datos financieros
    """
    ticker = ticker.strip().lower()
    url = f"https://stockanalysis.com/stocks/{ticker}/financials/?p=quarterly"
    
    print(f"Obteniendo datos para {ticker.upper()} desde {url}")
    
    # Configurar headers para evitar bloqueos
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
    }
    
    try:
        # Hacer la solicitud HTTP
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        # Parsear el HTML
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Encontrar todas las tablas
        tables = soup.find_all('table')
        
        if not tables:
            print("No se encontraron tablas en la página")
            return pd.DataFrame()
        
        # La tabla principal suele ser la más grande
        main_table = tables[0]
        
        # Extraer encabezados
        headers = []
        header_row = main_table.find('thead').find('tr')
        if header_row:
            headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]
        
        # Extraer filas de datos
        rows = []
        for tr in main_table.find('tbody').find_all('tr'):
            row = [td.get_text(strip=True) for td in tr.find_all(['th', 'td'])]
            if row:  # Asegurarse de que la fila no esté vacía
                rows.append(row)
        
        # Crear DataFrame
        if not rows:
            print("No se encontraron filas de datos en la tabla")
            return pd.DataFrame()
        
        # Asegurarse de que todas las filas tengan la misma longitud que los encabezados
        max_len = max(len(headers), max(len(row) for row in rows))
        headers = headers + [''] * (max_len - len(headers))
        rows = [row + [''] * (max_len - len(row)) for row in rows]
        
        df = pd.DataFrame(rows, columns=headers)
        
        print(f"Tabla extraída exitosamente. Dimensiones: {df.shape}")
        return df
        
    except Exception as e:
        print(f"Error al obtener datos: {e}")
        return pd.DataFrame()

# Ejecutar la función si se llama directamente
if __name__ == "__main__":
    # Obtener el ticker desde los argumentos de línea de comandos o usar TSCO por defecto
    ticker = sys.argv[1] if len(sys.argv) > 1 else "TSCO"
    
    print(f"\n=== Extrayendo tabla financiera para {ticker} ===\n")
    
    # Obtener la tabla
    df = importhtml_stockanalysis(ticker)
    
    # Mostrar los resultados
    if not df.empty:
        print("\nPrimeras 10 filas de la tabla:")
        print(df.head(10))
        
        # Guardar a CSV si se desea
        csv_file = f"{ticker}_financials.csv"
        df.to_csv(csv_file, index=False)
        print(f"\nTabla guardada en {csv_file}")
    else:
        print("\nNo se pudo obtener la tabla de datos financieros.")
    
    print("\n=== Proceso completado ===\n")
    print("Para usar en tu aplicación, simplemente importa y llama a la función importhtml_stockanalysis con el ticker deseado.")