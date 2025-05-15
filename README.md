# Revenue & EPS Tracker

Una aplicación web simple construida con Streamlit para rastrear los ingresos trimestrales (Total Revenue) y las ganancias por acción (EPS Diluted) de tickers de acciones utilizando datos de yfinance.

## Características

*   Obtiene datos financieros trimestrales de yfinance.
*   Almacena los datos localmente en una base de datos SQLite para acceso rápido.
*   Permite ingresar múltiples tickers separados por comas.
*   Muestra los datos en tablas y gráficos interactivos (Plotly).
*   Interfaz de usuario adaptable a dispositivos móviles.
*   Botón para forzar la actualización de datos desde yfinance.

## Instalación

1.  Clona este repositorio o descarga los archivos.
2.  Abre una terminal en el directorio del proyecto (`revenue_eps_tracker`).
3.  Crea un entorno virtual (recomendado):
    ```bash
    python -m venv venv
    source venv/bin/activate  # En Linux/macOS
    .\venv\Scripts\activate    # En Windows
    ```
4.  Instala las dependencias:
    ```bash
    pip install -r requirements.txt
    ```

## Ejecución

1.  Asegúrate de estar en el directorio raíz del proyecto (`revenue_eps_tracker`) en tu terminal.
2.  Ejecuta la aplicación Streamlit:
    ```bash
    streamlit run app.py --server.address=0.0.0.0 --server.port=8501
    ```
    *   `--server.address=0.0.0.0`: Permite el acceso desde otros dispositivos en tu red local.
    *   `--server.port=8501`: Especifica el puerto (puedes cambiarlo si es necesario).

3.  Abre tu navegador web:
    *   **En tu PC:** Ve a `http://localhost:8501`
    *   **En tu smartphone u otro dispositivo en la misma red:**
        *   Primero, necesitas encontrar la dirección IP local de tu PC.
            *   **Windows:** Abre CMD o PowerShell y escribe `ipconfig`. Busca la dirección IPv4 de tu adaptador de red activo (Wi-Fi o Ethernet).
            *   **macOS/Linux:** Abre la terminal y escribe `ifconfig` o `ip addr`. Busca la dirección IP (inet) de tu interfaz de red activa.
        *   Abre el navegador en tu smartphone y ve a `http://<TU_IP_LOCAL>:8501` (reemplaza `<TU_IP_LOCAL>` con la IP que encontraste).

        *Nota: Asegúrate de que tu PC y tu smartphone estén conectados a la misma red Wi-Fi y que ningún firewall esté bloqueando el puerto 8501.*