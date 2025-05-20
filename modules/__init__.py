# Este archivo permite que Python reconozca el directorio 'modules' como un paquete
# y facilita la importación de los módulos contenidos en él.

# Importar explícitamente los módulos y funciones principales para facilitar su uso
# Solo se incluyen las funciones relacionadas con StockAnalysis
from .importhtml import get_stockanalysis_table, importhtml
from .data_fetcher import fetch_quarterly_financials