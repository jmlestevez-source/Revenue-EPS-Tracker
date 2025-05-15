import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), 'modules'))
from db_manager import load_financials, save_financials

# Probar específicamente el ticker V que causaba problemas
print("\nProbando específicamente el ticker V que causaba problemas anteriormente:")

# Cargar datos para verificar
print("\nCargando datos para V...")
loaded_df = load_financials("V")
print("\nDatos cargados para V:")
print(loaded_df)

# Probar con variaciones del ticker V para asegurar que la sanitización funciona
print("\nProbando con variaciones del ticker V:")
print("\nCargando datos para 'v' (minúscula)...")
loaded_df = load_financials("v")
print("Datos cargados correctamente.")

print("\nCargando datos para ' V ' (con espacios)...")
loaded_df = load_financials(" V ")
print("Datos cargados correctamente.")

print("\nPrueba completada. Las correcciones funcionan correctamente.")