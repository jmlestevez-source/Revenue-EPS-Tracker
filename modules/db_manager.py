import sqlite3
import pandas as pd
import os
from datetime import datetime
import json

DB_DIR = "data"
DB_PATH = os.path.join(DB_DIR, "finance.db")

def _ensure_db_dir():
    """Ensures the database directory exists."""
    os.makedirs(DB_DIR, exist_ok=True)

def _get_db_connection():
    """Establishes a connection to the SQLite database."""
    _ensure_db_dir()
    conn = sqlite3.connect(DB_PATH)
    return conn

def _create_table_if_not_exists(ticker: str):
    """Creates a table for the ticker if it doesn't exist."""
    # Validar el ticker antes de crear la tabla
    if not ticker or not isinstance(ticker, str):
        print(f"Error: No se puede crear tabla para ticker inválido: {ticker}")
        return
    
    # Normalizar y sanitizar el ticker para el nombre de la tabla
    ticker = ticker.strip().upper()
    # Sanitizar de manera consistente para evitar problemas con caracteres especiales
    # Añadir '/' a la lista de caracteres a reemplazar para manejar tickers europeos como EPA/MC
    sanitized_ticker = ticker.replace('-', '_').replace('^', '_').replace('.', '_').replace(' ', '_').replace('/', '_')
    table_name = f"ticker_{sanitized_ticker}" # Sanitize ticker for table name
    
    try:
        conn = _get_db_connection()
        cursor = conn.cursor()
        
        # Check existing columns
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [info[1] for info in cursor.fetchall()]

        # Create table if not exists
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                date TEXT PRIMARY KEY,
                totalRevenue REAL,
                epsDiluted REAL,
                ttmRevenue REAL,          -- Added TTM Revenue
                ttmEpsDiluted REAL,       -- Added TTM EPS
                last_updated TEXT
            )
        """)

        # Add columns if they don't exist (for existing tables)
        if 'ttmRevenue' not in columns:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN ttmRevenue REAL")
            print(f"Added ttmRevenue column to {table_name}")
        if 'ttmEpsDiluted' not in columns:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN ttmEpsDiluted REAL")
            print(f"Added ttmEpsDiluted column to {table_name}")

        conn.commit()
    except sqlite3.Error as e:
        print(f"Error creating/altering table {table_name}: {e}")
    finally:
        conn.close()

def _create_watchlists_table():
    """Creates the watchlists table if it doesn't exist."""
    try:
        conn = _get_db_connection()
        cursor = conn.cursor()
        
        # Create watchlists table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS watchlists (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                tickers TEXT NOT NULL,
                created_at TEXT,
                updated_at TEXT
            )
        """)
        conn.commit()
    except sqlite3.Error as e:
        print(f"Error creating watchlists table: {e}")
    finally:
        conn.close()

def save_financials(df: pd.DataFrame, ticker: str):
    """Saves the financial DataFrame to the SQLite database for the given ticker.

    Args:
        df (pd.DataFrame): DataFrame with columns including ['date', 'totalRevenue', 'epsDiluted', 'ttmRevenue', 'ttmEpsDiluted'].
        ticker (str): The stock ticker symbol.
    """
    # Validar el ticker antes de intentar guardar datos
    if not ticker or not isinstance(ticker, str):
        print(f"Error: Ticker inválido o vacío: {ticker}. No se guardarán datos.")
        return
    
    # Normalizar el ticker
    ticker = ticker.strip().upper()
    
    if df.empty:
        print(f"DataFrame for {ticker} is empty. Nothing to save.")
        return
    
    # Sanitizar el nombre de la tabla de manera consistente
    # Añadir '/' a la lista de caracteres a reemplazar para manejar tickers europeos como EPA/MC
    sanitized_ticker = ticker.replace('-', '_').replace('^', '_').replace('.', '_').replace(' ', '_').replace('/', '_')
    table_name = f"ticker_{sanitized_ticker}"
    
    _create_table_if_not_exists(ticker)
    conn = _get_db_connection()
    
    try:
        # Ensure required columns exist, add if missing (with None/NaN)
        required_cols = ['date', 'totalRevenue', 'epsDiluted', 'ttmRevenue', 'ttmEpsDiluted']
        for col in required_cols:
            if col not in df.columns:
                df[col] = None # Or pd.NA

        # Add last updated timestamp
        df['last_updated'] = datetime.now().isoformat()

        # Convert date to string for SQLite compatibility
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')

        # Select only the columns that exist in the table schema (important for ALTER TABLE cases)
        # Fetch current columns from the table to ensure compatibility
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        table_columns = [info[1] for info in cursor.fetchall()]
        df_to_save = df[[col for col in df.columns if col in table_columns]].copy()

        # Use INSERT OR REPLACE to handle updates
        df_to_save.to_sql(table_name, conn, if_exists='append', index=False, method=_insert_or_replace)
        print(f"Successfully saved/updated data for {ticker} in table {table_name}.")

    except sqlite3.Error as e:
        print(f"Error saving data for {ticker} to table {table_name}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred while saving data for {ticker}: {e}")
    finally:
        conn.close()

# def _insert_or_replace(table, conn, keys, data_iter): # Old signature
def _insert_or_replace(table, cursor, keys, data_iter): # New signature: Rename conn to cursor
    """Custom method for pandas to_sql assuming 'conn' passed is actually a cursor.
       Includes explicit error handling.
    """
    # cursor = None # Remove initialization
    conn = cursor.connection # Get the actual connection object from the cursor
    try:
        # cursor = conn.cursor() # Remove this line - we already have the cursor
        placeholders = ', '.join('?' * len(keys))
        # Ensure keys match the columns being inserted
        sql = f"INSERT OR REPLACE INTO {table.name} ({', '.join(keys)}) VALUES ({placeholders})"
        # Use the passed cursor directly
        cursor.executemany(sql, data_iter)
        # Commit using the actual connection
        conn.commit() # Commit on the connection obtained from the cursor
    except sqlite3.Error as e:
        print(f"Error during INSERT OR REPLACE in table {table.name}: {e}")
        # Optionally rollback: conn.rollback()
        raise # Re-raise the exception so pandas/caller knows it failed
    # finally: # Remove the finally block that closes the cursor
        # If pandas passed the cursor, it might manage closing it.
        # pass

def load_financials(ticker: str) -> pd.DataFrame:
    """Loads financial data for the given ticker from the SQLite database.

    Args:
        ticker (str): The stock ticker symbol.

    Returns:
        pd.DataFrame: DataFrame with financial data, including TTM and 'last_updated'.
                      Returns an empty DataFrame if the table doesn't exist or an error occurs.
    """

# --- Funciones para gestionar watchlists ---

def save_watchlist(name: str, tickers: list):
    """Guarda una watchlist en la base de datos.
    
    Args:
        name (str): Nombre de la watchlist
        tickers (list): Lista de tickers en la watchlist
    
    Returns:
        bool: True si se guardó correctamente, False en caso contrario
    """
    _create_watchlists_table()
    
    try:
        conn = _get_db_connection()
        cursor = conn.cursor()
        
        # Convertir lista de tickers a JSON string
        tickers_json = json.dumps(tickers)
        now = datetime.now().isoformat()
        
        # Verificar si ya existe una watchlist con ese nombre
        cursor.execute("SELECT id FROM watchlists WHERE name = ?", (name,))
        existing = cursor.fetchone()
        
        if existing:
            # Actualizar watchlist existente
            cursor.execute(
                "UPDATE watchlists SET tickers = ?, updated_at = ? WHERE name = ?",
                (tickers_json, now, name)
            )
        else:
            # Crear nueva watchlist
            cursor.execute(
                "INSERT INTO watchlists (name, tickers, created_at, updated_at) VALUES (?, ?, ?, ?)",
                (name, tickers_json, now, now)
            )
        
        conn.commit()
        return True
    except sqlite3.Error as e:
        print(f"Error guardando watchlist {name}: {e}")
        return False
    finally:
        conn.close()

def get_watchlist(name: str) -> list:
    """Obtiene los tickers de una watchlist específica.
    
    Args:
        name (str): Nombre de la watchlist
    
    Returns:
        list: Lista de tickers en la watchlist, o lista vacía si no existe
    """
    _create_watchlists_table()
    
    try:
        conn = _get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT tickers FROM watchlists WHERE name = ?", (name,))
        result = cursor.fetchone()
        
        if result:
            return json.loads(result[0])
        return []
    except sqlite3.Error as e:
        print(f"Error obteniendo watchlist {name}: {e}")
        return []
    finally:
        conn.close()

def get_all_watchlists() -> dict:
    """Obtiene todas las watchlists disponibles.
    
    Returns:
        dict: Diccionario con nombres de watchlists como claves y listas de tickers como valores
    """
    _create_watchlists_table()
    
    try:
        conn = _get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT name, tickers FROM watchlists ORDER BY name")
        results = cursor.fetchall()
        
        watchlists = {}
        for name, tickers_json in results:
            watchlists[name] = json.loads(tickers_json)
        
        return watchlists
    except sqlite3.Error as e:
        print(f"Error obteniendo todas las watchlists: {e}")
        return {}
    finally:
        conn.close()

def delete_watchlist(name: str) -> bool:
    """Elimina una watchlist de la base de datos.
    
    Args:
        name (str): Nombre de la watchlist a eliminar
    
    Returns:
        bool: True si se eliminó correctamente, False en caso contrario
    """
    try:
        conn = _get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("DELETE FROM watchlists WHERE name = ?", (name,))
        conn.commit()
        return cursor.rowcount > 0
    except sqlite3.Error as e:
        print(f"Error eliminando watchlist {name}: {e}")
        return False
    finally:
        conn.close()

def load_financials(ticker: str) -> pd.DataFrame:
    """Loads financial data for the given ticker from the SQLite database.

    Args:
        ticker (str): The stock ticker symbol.

    Returns:
        pd.DataFrame: DataFrame with financial data, including TTM and 'last_updated'.
                      Returns an empty DataFrame if the table doesn't exist or an error occurs.
    """
    # Validar el ticker antes de intentar cargar datos
    if not ticker or not isinstance(ticker, str):
        print(f"Error: Ticker inválido o vacío: {ticker}")
        return pd.DataFrame(columns=['date', 'totalRevenue', 'epsDiluted', 'ttmRevenue', 'ttmEpsDiluted', 'last_updated'])
    
    # Normalizar el ticker
    ticker = ticker.strip().upper()
    
    try:
        # Sanitizar el nombre de la tabla de manera consistente
        # Añadir '/' a la lista de caracteres a reemplazar para manejar tickers europeos como EPA/MC
        sanitized_ticker = ticker.replace('-', '_').replace('^', '_').replace('.', '_').replace(' ', '_').replace('/', '_')
        table_name = f"ticker_{sanitized_ticker}"
        expected_columns = ['date', 'totalRevenue', 'epsDiluted', 'ttmRevenue', 'ttmEpsDiluted', 'last_updated']
        
        conn = _get_db_connection()
        # Check if table exists first
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        if cursor.fetchone() is None:
            print(f"Table {table_name} does not exist for ticker {ticker}.")
            return pd.DataFrame(columns=expected_columns)

        # Check existing columns in the table
        cursor.execute(f"PRAGMA table_info({table_name})")
        available_columns = [info[1] for info in cursor.fetchall()]
        columns_to_select = [col for col in expected_columns if col in available_columns]
        
        if not columns_to_select:
            print(f"No hay columnas válidas en la tabla {table_name} para el ticker {ticker}.")
            return pd.DataFrame(columns=expected_columns)
            
        select_statement = f"SELECT {', '.join(columns_to_select)} FROM {table_name}"

        # Load data if table exists
        df = pd.read_sql(select_statement, conn)

        # Add missing expected columns with None/NaN if they weren't in the table
        for col in expected_columns:
            if col not in df.columns:
                df[col] = pd.NA # Use pandas NA for better type handling

        # Asegurar que la columna 'date' se convierta correctamente a objetos Timestamp
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        # Eliminar filas con fechas inválidas
        df = df.dropna(subset=['date'])
        # Convert numeric columns, coercing errors
        for col in ['totalRevenue', 'epsDiluted', 'ttmRevenue', 'ttmEpsDiluted']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        df = df.sort_values(by='date').reset_index(drop=True)
        print(f"Successfully loaded data for {ticker} from table {table_name}.")
        return df[expected_columns] # Return with consistent column order

    except sqlite3.Error as e:
        print(f"Error loading data for {ticker} from table {table_name}: {e}")
        return pd.DataFrame(columns=expected_columns)
    except Exception as e:
        print(f"An unexpected error occurred while loading data for {ticker}: {e}")
        return pd.DataFrame(columns=expected_columns)
    finally:
        conn.close()

# Example usage (optional, for testing)
if __name__ == '__main__':
    # Create dummy data including TTM
    data = {
        'date': pd.to_datetime(['2023-03-31', '2023-06-30', '2023-09-30', '2023-12-31', '2024-03-31']),
        'totalRevenue': [1000, 1100, 1200, 1300, 1400],
        'epsDiluted': [1.0, 1.1, 1.2, 1.3, 1.4],
        'ttmRevenue': [None, None, None, 4600, 5000], # Example TTM values
        'ttmEpsDiluted': [None, None, None, 4.6, 5.0] # Example TTM values
    }
    dummy_df = pd.DataFrame(data)

    # Test saving
    save_financials(dummy_df, "TEST")

    # Test loading
    loaded_df = load_financials("TEST")
    print("\nLoaded TEST Financials:")
    print(loaded_df)
    print("\nLoaded TEST Financials Info:")
    print(loaded_df.info())

    # Test loading non-existent ticker
    non_existent_df = load_financials("NONEXISTENT")
    print("\nLoaded NONEXISTENT Financials:")
    print(non_existent_df)
    print("\nLoaded NONEXISTENT Financials Info:")
    print(non_existent_df.info())