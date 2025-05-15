import sqlite3
import pandas as pd
import os
from datetime import datetime

# Importar el módulo db_manager para probar las funciones
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), 'modules'))
from db_manager import load_financials, save_financials, _create_table_if_not_exists

# Verificar la base de datos
DB_DIR = "data"
DB_PATH = os.path.join(DB_DIR, "finance.db")

print(f"Verificando base de datos en: {DB_PATH}")
print(f"¿Existe la base de datos? {os.path.exists(DB_PATH)}")

# Asegurar que el directorio existe
os.makedirs(DB_DIR, exist_ok=True)

# Conectar a la base de datos
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Listar todas las tablas
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = cursor.fetchall()
print("\nTablas en la base de datos:")
for table in tables:
    print(f"- {table[0]}")

# Probar con el ticker V que causa problemas
print("\nProbando con el ticker V:")

# Crear tabla para V si no existe
_create_table_if_not_exists("V")

# Verificar que la tabla se creó correctamente
cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ticker_V'")
if cursor.fetchone():
    print("La tabla ticker_V se creó correctamente")
else:
    print("Error: La tabla ticker_V no se creó")

# Crear datos de prueba para V
data = {
    'date': pd.to_datetime(['2023-03-31', '2023-06-30', '2023-09-30', '2023-12-31', '2024-03-31']),
    'totalRevenue': [8000, 8200, 8400, 8600, 8800],
    'epsDiluted': [2.0, 2.1, 2.2, 2.3, 2.4],
    'ttmRevenue': [None, None, None, 33200, 34000],
    'ttmEpsDiluted': [None, None, None, 8.6, 9.0]
}
test_df = pd.DataFrame(data)

# Guardar datos de prueba
print("\nGuardando datos de prueba para V...")
save_financials(test_df, "V")

# Cargar datos para verificar
print("\nCargando datos para V...")
loaded_df = load_financials("V")
print("\nDatos cargados para V:")
print(loaded_df)

# Cerrar conexión
conn.close()

print("\nPrueba completada.")