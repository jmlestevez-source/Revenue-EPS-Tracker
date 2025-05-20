import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
import numpy as np
from .data_fetcher import fetch_quarterly_financials # Use the combined fetcher
from . import db_manager as load_financials  # Renombrado para evitar conflictos

# Helper function to format large numbers
def format_large_number(num):
    if pd.isna(num):
        return "N/A"
    num = float(num)
    if abs(num) >= 1_000_000_000:
        return f"{num / 1_000_000_000:.2f}B"
    elif abs(num) >= 1_000_000:
        return f"{num / 1_000_000:.2f}M"
    elif abs(num) >= 1_000:
        return f"{num / 1_000:.2f}K"
    else:
        return f"{num:.2f}"

def calculate_growth_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Calculates QoQ, YoY growth, streaks, and ATH flags."""
    df = df.sort_values(by='date').copy()

    # Ensure numeric types
    for col in ['totalRevenue', 'epsDiluted', 'ttmRevenue', 'ttmEpsDiluted']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # QoQ Growth (Quarter over Quarter)
    df['revenue_qoq'] = df['totalRevenue'].pct_change(periods=1) * 100
    df['eps_qoq'] = df['epsDiluted'].pct_change(periods=1) * 100

    # YoY Growth (Year over Year - comparing to same quarter last year)
    df['revenue_yoy'] = df['totalRevenue'].pct_change(periods=4) * 100
    df['eps_yoy'] = df['epsDiluted'].pct_change(periods=4) * 100

    # Growth Streaks (Consecutive Quarters)
    def calculate_streak(series):
        positive_streak = 0
        negative_streak = 0
        streaks = []
        for x in series.dropna(): # Iterate through non-NaN growth values
            if x > 0:
                positive_streak += 1
                negative_streak = 0
                streaks.append(positive_streak)
            elif x < 0:
                negative_streak += 1
                positive_streak = 0
                streaks.append(-negative_streak)
            else: # x == 0 or NaN
                positive_streak = 0
                negative_streak = 0
                streaks.append(0)
        # Pad with NaN for initial periods where growth couldn't be calculated
        return pd.Series([np.nan] * (len(series) - len(streaks)) + streaks, index=series.index)

    df['revenue_streak'] = calculate_streak(df['revenue_qoq'])
    df['eps_streak'] = calculate_streak(df['eps_qoq'])

    # All-Time High (ATH) based on available data
    df['revenue_ath'] = df['totalRevenue'] == df['totalRevenue'].cummax()
    df['eps_ath'] = df['epsDiluted'] == df['epsDiluted'].cummax()
    # Mark only the *first* occurrence of a new max as ATH if needed, or keep as is (marks all equal to max)
    # Example: Keep only first ATH
    # df['revenue_ath'] = (df['totalRevenue'] == df['totalRevenue'].cummax()) & (df['totalRevenue'] != df['totalRevenue'].shift(1))
    # df['eps_ath'] = (df['epsDiluted'] == df['epsDiluted'].cummax()) & (df['epsDiluted'] != df['epsDiluted'].shift(1))
    
    # Calcular el número de trimestres sin alcanzar un máximo histórico de EPS
    df['quarters_since_eps_ath'] = 0
    if not df.empty and 'eps_ath' in df.columns:
        # Crear una serie temporal con los índices de los ATH
        ath_indices = df.index[df['eps_ath']].tolist()
        
        if ath_indices:
            # Para cada fila, calcular cuántos trimestres han pasado desde el último ATH
            for i, idx in enumerate(df.index):
                # Encontrar el último ATH antes de este índice
                prev_aths = [ath_idx for ath_idx in ath_indices if ath_idx <= idx]
                if prev_aths:
                    last_ath_idx = max(prev_aths)
                    # Contar cuántas filas hay entre el último ATH y la fila actual
                    quarters_since = df.index.get_loc(idx) - df.index.get_loc(last_ath_idx)
                    df.loc[idx, 'quarters_since_eps_ath'] = quarters_since
    
    # Detectar caídas consecutivas ACTUALES en EPS (más de 5 trimestres)
    df['eps_consecutive_decline'] = False
    # Verificamos si el último valor de eps_streak es negativo con valor absoluto mayor o igual a 5
    if not df.empty and 'eps_streak' in df.columns:
        # Obtenemos el último valor de streak (el más reciente)
        latest_streak = df.iloc[-1]['eps_streak'] if not df.empty else None
        # Si el último streak es -5 o menor, marcamos la alerta
        if pd.notna(latest_streak) and latest_streak <= -5:  # Streak negativo actual de 5 o más
            df.loc[df.index[-1], 'eps_consecutive_decline'] = True

    return df

# --- Chart Display Functions ---
def display_revenue_chart(ticker, df_filtered):
    # Obtener el nombre completo de la empresa si está disponible
    company_name = df_filtered['company_name'].iloc[-1] if 'company_name' in df_filtered.columns and not df_filtered.empty else ticker
    # Formatear el título del gráfico para evitar duplicación del ticker
    st.subheader(f"Gráfico de Evolución de Ingresos - {ticker} - {company_name}")
    if df_filtered.empty or 'totalRevenue' not in df_filtered.columns:
        st.warning("No hay datos de ingresos suficientes para mostrar el gráfico.")
        return

    # Solo mostrar datos trimestrales, eliminando TTM
    df_chart_rev = df_filtered.melt(id_vars=['date'], value_vars=['totalRevenue'],
                               var_name='Metrica', value_name='Valor')
    df_chart_rev['Metrica'] = df_chart_rev['Metrica'].replace({'totalRevenue': 'Ingresos Trimestrales'})
    # Formatear el título del gráfico para evitar duplicación del ticker
    chart_title = f"Ingresos para {ticker} - {company_name}"
    fig_rev = px.line(df_chart_rev, x='date', y='Valor', color='Metrica',
                  title=chart_title,
                  labels={'date': 'Fecha', 'Valor': 'Valor (USD)', 'Metrica': 'Métrica'},
                  markers=True)
    fig_rev.update_layout(hovermode="x unified")
    st.plotly_chart(fig_rev, use_container_width=True)

def display_eps_chart(ticker, df_filtered):
    # Obtener el nombre completo de la empresa si está disponible
    company_name = df_filtered['company_name'].iloc[-1] if 'company_name' in df_filtered.columns and not df_filtered.empty else ticker
    # Formatear el título del gráfico para evitar duplicación del ticker
    st.subheader(f"Gráfico de Evolución de EPS - {ticker} - {company_name}")
    if df_filtered.empty or 'epsDiluted' not in df_filtered.columns:
        st.warning("No hay datos de EPS suficientes para mostrar el gráfico.")
        return

    # Solo mostrar datos trimestrales, eliminando TTM
    df_chart_eps = df_filtered.melt(id_vars=['date'], value_vars=['epsDiluted'],
                               var_name='Metrica', value_name='Valor')
    df_chart_eps['Metrica'] = df_chart_eps['Metrica'].replace({'epsDiluted': 'EPS Trimestral'})
    # Formatear el título del gráfico para evitar duplicación del ticker
    chart_title = f"EPS Diluido para {ticker} - {company_name}"
    fig_eps = px.line(df_chart_eps, x='date', y='Valor', color='Metrica',
                  title=chart_title,
                  labels={'date': 'Fecha', 'Valor': 'Valor (USD)', 'Metrica': 'Métrica'},
                  markers=True)
    fig_eps.update_layout(hovermode="x unified")
    st.plotly_chart(fig_eps, use_container_width=True)

# --- Callback Functions for Navigation ---
def set_view(view, ticker=None, chart_type=None):
    st.session_state.current_view = view
    st.session_state.chart_ticker = ticker
    st.session_state.chart_type = chart_type

def main_ui():
    """Main function to build the Streamlit UI."""

    st.set_page_config(layout="wide", page_title="Revenue & EPS Tracker") # Use wide layout and set title

    # --- Initialize Session State --- 
    if 'current_view' not in st.session_state:
        st.session_state.current_view = 'main'
    if 'chart_ticker' not in st.session_state:
        st.session_state.chart_ticker = None
    if 'chart_type' not in st.session_state:
        st.session_state.chart_type = None
    if 'all_data' not in st.session_state:
        st.session_state.all_data = {}
    if 'last_updated_times' not in st.session_state:
        st.session_state.last_updated_times = {}

    # --- Custom CSS (Inspired by Lovable) ---
    st.markdown('''
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
            /* General Styles */
            .main .block-container {
                padding-top: 2rem;
                padding-bottom: 2rem;
                padding-left: 2rem;
                padding-right: 2rem;
            }
            h1 {
                text-align: center;
                color: #2c3e50; /* Dark blue-gray */
            }
            h3 {
                 color: #34495e; /* Slightly lighter blue-gray */
                 border-bottom: 2px solid #ecf0f1; /* Light gray border */
                 padding-bottom: 5px;
                 margin-top: 2rem;
                 margin-bottom: 1rem;
            }
            /* Metric Styles */
            div[data-testid="stMetric"] {
                background-color: #ecf0f1; /* Light gray background */
                border-radius: 8px;
                padding: 15px;
                border-left: 5px solid #3498db; /* Blue left border */
                box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
            }
            div[data-testid="stMetric"] label {
                font-weight: bold;
                color: #7f8c8d; /* Grayish color for label */
            }
            div[data-testid="stMetric"] div.stMetricValue {
                font-size: 1.5em;
                color: #2c3e50;
            }
            div[data-testid="stMetric"] div.stMetricDelta {
                font-size: 0.9em;
            }
            /* Positive/Negative Delta Colors */
            div[data-testid="stMetric"] div.stMetricDelta > div[data-testid="metric-container"] > div {
                 color: #27ae60 !important; /* Green for positive */
            }
             div[data-testid="stMetric"] div.stMetricDelta > div[data-testid="metric-container"] > div[style*="color: rgb(255, 43, 43)"] {
                 color: #c0392b !important; /* Red for negative */
            }
            /* Table Styles */
            table {
                width: 100% !important;
                overflow-x: auto;
                display: block;
                border-collapse: collapse;
            }
            th {
                background-color: #3498db;
                color: white;
                text-align: right;
                padding: 8px;
            }
            td {
                text-align: right;
                padding: 8px;
                border-bottom: 1px solid #ddd;
            }
            tr:hover {background-color: #f5f5f5;}

            /* Mobile Adjustments */
            @media (max-width: 768px) {
                .main .block-container {
                    padding-top: 1rem;
                    padding-bottom: 1rem;
                    padding-left: 1rem;
                    padding-right: 1rem;
                }
                div[data-testid="stMetric"] {
                    margin-bottom: 1rem;
                }
            }
        </style>
    ''', unsafe_allow_html=True)

    st.title("📊 Rastreador de Ingresos y EPS")

    # --- Sidebar --- 
    with st.sidebar:
        st.header("⚙️ Configuración")

        # Sección de Watchlists
        st.subheader("📋 Watchlists")
        
        # Cargar todas las watchlists disponibles
        watchlists = load_financials.get_all_watchlists()
        
        # Crear una nueva watchlist
        with st.expander("➕ Crear Nueva Watchlist"):
            new_watchlist_name = st.text_input("Nombre de la Watchlist", key="new_watchlist_name")
            new_watchlist_tickers = st.text_input(
                "Tickers (separados por coma)", 
                value="AAPL,MSFT,GOOG,AMZN,NVDA",
                key="new_watchlist_tickers"
            )
            if st.button("💾 Guardar Watchlist"):
                if new_watchlist_name.strip():
                    tickers_list = [t.strip().upper() for t in new_watchlist_tickers.split(',') if t.strip()]
                    if tickers_list:
                        if load_financials.save_watchlist(new_watchlist_name, tickers_list):
                            st.success(f"Watchlist '{new_watchlist_name}' guardada correctamente.")
                            # Recargar watchlists después de guardar
                            watchlists = load_financials.get_all_watchlists()
                            # Limpiar campos
                            st.session_state.new_watchlist_name = ""
                            st.session_state.new_watchlist_tickers = ""
                        else:
                            st.error("Error al guardar la watchlist.")
                    else:
                        st.warning("Debes introducir al menos un ticker.")
                else:
                    st.warning("Debes introducir un nombre para la watchlist.")
        
        # Seleccionar una watchlist existente o usar tickers personalizados
        watchlist_options = list(watchlists.keys())
        watchlist_options.insert(0, "Personalizado")  # Opción para usar tickers personalizados
        
        selected_watchlist = st.selectbox(
            "Seleccionar Watchlist",
            options=watchlist_options,
            index=0,
            key="selected_watchlist"
        )
        
        # Mostrar tickers según la selección
        if selected_watchlist == "Personalizado":
            # Ticker Input con instrucciones para tickers europeos
            ticker_input = st.text_input(
                "Introduce Tickers (separados por coma)", 
                value="AAPL,MSFT,GOOG,AMZN,NVDA" # Default tickers
            )
            st.caption("📌 Para tickers europeos, usa el formato MERCADO/TICKER (ejemplo: BME/TEF para Telefónica en Mercado Continuo)")
            tickers = [ticker.strip().upper() for ticker in ticker_input.split(',') if ticker.strip()]
        else:
            # Mostrar y permitir editar los tickers de la watchlist seleccionada
            watchlist_tickers = watchlists.get(selected_watchlist, [])
            ticker_input = st.text_input(
                "Tickers en esta Watchlist",
                value=",".join(watchlist_tickers),
                key=f"edit_{selected_watchlist}"
            )
            tickers = [ticker.strip().upper() for ticker in ticker_input.split(',') if ticker.strip()]
            
            # Botones para actualizar o eliminar la watchlist
            col1, col2 = st.columns(2)
            with col1:
                if st.button("🔄 Actualizar", key=f"update_{selected_watchlist}"):
                    if load_financials.save_watchlist(selected_watchlist, tickers):
                        st.success(f"Watchlist '{selected_watchlist}' actualizada.")
                        # Recargar watchlists
                        watchlists = load_financials.get_all_watchlists()
                    else:
                        st.error("Error al actualizar la watchlist.")
            with col2:
                if st.button("🗑️ Eliminar", key=f"delete_{selected_watchlist}"):
                    if load_financials.delete_watchlist(selected_watchlist):
                        st.success(f"Watchlist '{selected_watchlist}' eliminada.")
                        # Recargar watchlists y resetear selección
                        watchlists = load_financials.get_all_watchlists()
                        st.session_state.selected_watchlist = "Personalizado"
                        st.experimental_rerun()
                    else:
                        st.error("Error al eliminar la watchlist.")

        # Date Range Selector
        st.info("El rango de fechas filtra los datos mostrados en tablas y gráficos.")
        min_date_default = datetime.now() - timedelta(days=10*365) # Default 10 years back
        max_date_default = datetime.now()

        date_range = st.date_input(
            "Selecciona Rango de Fechas",
            value=(min_date_default, max_date_default),
            min_value=datetime(1990, 1, 1), # Wider historical range
            max_value=datetime.now() + timedelta(days=1)
        )

        start_date = date_range[0] if len(date_range) > 0 else min_date_default
        end_date = date_range[1] if len(date_range) > 1 else max_date_default

        # Update Button
        if st.button("🔄 Actualizar Datos", key="update_data", help="Obtiene los últimos datos de las fuentes y los guarda localmente."):
            if tickers:
                progress_bar = st.progress(0)
                status_text = st.empty()
                error_container = st.empty()
                success_count = 0
                error_count = 0
                error_tickers = []
                
                # Validar tickers antes de procesar
                invalid_tickers = [t for t in tickers if len(t.strip()) == 0 or len(t.strip()) > 10]
                if invalid_tickers:
                    st.warning(f"Los siguientes tickers tienen formato inválido y serán omitidos: {', '.join(invalid_tickers)}")
                    tickers = [t for t in tickers if t not in invalid_tickers]
                    if not tickers:
                        st.error("No hay tickers válidos para procesar.")
                        return
                
                # Clear cached data before update
                st.session_state.all_data = {}
                st.session_state.last_updated_times = {}
                
                for i, ticker in enumerate(tickers):
                    ticker = ticker.strip().upper()  # Normalizar ticker
                    status_text.text(f"Actualizando {ticker} ({i+1}/{len(tickers)})...")
                    try:
                        with st.spinner(f"Obteniendo datos para {ticker}..."):
                            df_fresh = fetch_quarterly_financials(ticker)
                            
                        if not df_fresh.empty:
                            load_financials.save_financials(df_fresh, ticker)
                            # Recalculate metrics and store in session state immediately
                            df_processed = calculate_growth_metrics(df_fresh)
                            st.session_state.all_data[ticker] = df_processed
                            if 'last_updated' in df_fresh.columns and not df_fresh['last_updated'].isnull().all():
                                try:
                                    last_updated_series = pd.to_datetime(df_fresh['last_updated'], errors='coerce')
                                    if not last_updated_series.isnull().all():
                                        st.session_state.last_updated_times[ticker] = last_updated_series.max()
                                    else:
                                        st.session_state.last_updated_times[ticker] = None
                                except Exception:
                                    st.session_state.last_updated_times[ticker] = None
                            else:
                                st.session_state.last_updated_times[ticker] = None
                            status_text.text(f"✅ Datos de {ticker} actualizados.")
                            success_count += 1
                        else:
                            status_text.text(f"⚠️ No se pudieron obtener datos para {ticker}.")
                            error_count += 1
                            error_tickers.append(ticker)
                    except Exception as e:
                        status_text.text(f"❌ Error actualizando {ticker}: {str(e)[:100]}")
                        error_count += 1
                        error_tickers.append(ticker)
                    progress_bar.progress((i + 1) / len(tickers))
                
                # Mostrar resumen de la actualización
                status_text.text("Actualización completada.")
                if success_count > 0:
                    st.success(f"Se actualizaron correctamente {success_count} de {len(tickers)} tickers.")
                if error_count > 0:
                    error_container.warning(f"No se pudieron actualizar {error_count} tickers: {', '.join(error_tickers)}")
                    st.info("Prueba a actualizar los tickers problemáticos individualmente o verifica que sean símbolos válidos.")
                
                # Recargar explícitamente los datos para todos los tickers después de la actualización
                # Esto asegura que la interfaz muestre los datos actualizados
                for ticker in tickers:
                    ticker = ticker.strip().upper()
                    try:
                        # Cargar datos desde la base de datos
                        df = load_financials.load_financials(ticker)
                        if not df.empty:
                            # Calcular métricas y almacenar en el estado de la sesión
                            df_processed = calculate_growth_metrics(df)
                            st.session_state.all_data[ticker] = df_processed
                            if 'last_updated' in df.columns and not df['last_updated'].isnull().all():
                                try:
                                    last_updated_series = pd.to_datetime(df['last_updated'], errors='coerce')
                                    if not last_updated_series.isnull().all():
                                        st.session_state.last_updated_times[ticker] = last_updated_series.max()
                                    else:
                                        st.session_state.last_updated_times[ticker] = None
                                except Exception:
                                    st.session_state.last_updated_times[ticker] = None
                            else:
                                st.session_state.last_updated_times[ticker] = None
                    except Exception as e:
                        print(f"Error recargando datos para {ticker} después de la actualización: {e}")
                
                # Si estamos usando una watchlist, actualizar la watchlist con los tickers actualizados
                if selected_watchlist != "Personalizado":
                    load_financials.save_watchlist(selected_watchlist, tickers)
                    st.success(f"Watchlist '{selected_watchlist}' actualizada con los tickers procesados.")
            else:
                st.warning("Por favor, introduce al menos un ticker.")
                
        # Botón para actualizar todas las watchlists
        if watchlists and st.button("🔄 Actualizar Todas las Watchlists", key="update_all_watchlists"):
            all_tickers = []
            for watchlist_name, watchlist_tickers in watchlists.items():
                all_tickers.extend(watchlist_tickers)
            
            # Eliminar duplicados
            all_tickers = list(set(all_tickers))
            
            if all_tickers:
                st.info(f"Actualizando {len(all_tickers)} tickers de todas las watchlists...")
                
                progress_bar = st.progress(0)
                status_text = st.empty()
                error_container = st.empty()
                success_count = 0
                error_count = 0
                error_tickers = []
                
                # Validar tickers antes de procesar
                invalid_tickers = [t for t in all_tickers if len(t.strip()) == 0 or len(t.strip()) > 10]
                if invalid_tickers:
                    st.warning(f"Los siguientes tickers tienen formato inválido y serán omitidos: {', '.join(invalid_tickers)}")
                    all_tickers = [t for t in all_tickers if t not in invalid_tickers]
                
                # Clear cached data before update
                st.session_state.all_data = {}
                st.session_state.last_updated_times = {}
                
                for i, ticker in enumerate(all_tickers):
                    ticker = ticker.strip().upper()  # Normalizar ticker
                    status_text.text(f"Actualizando {ticker} ({i+1}/{len(all_tickers)})...")
                    try:
                        with st.spinner(f"Obteniendo datos para {ticker}..."):
                            df_fresh = fetch_quarterly_financials(ticker)
                            
                        if not df_fresh.empty:
                            load_financials.save_financials(df_fresh, ticker)
                            # Recalculate metrics and store in session state immediately
                            df_processed = calculate_growth_metrics(df_fresh)
                            st.session_state.all_data[ticker] = df_processed
                            if 'last_updated' in df_fresh.columns and not df_fresh['last_updated'].isnull().all():
                                try:
                                    last_updated_series = pd.to_datetime(df_fresh['last_updated'], errors='coerce')
                                    if not last_updated_series.isnull().all():
                                        st.session_state.last_updated_times[ticker] = last_updated_series.max()
                                    else:
                                        st.session_state.last_updated_times[ticker] = None
                                except Exception:
                                    st.session_state.last_updated_times[ticker] = None
                            else:
                                st.session_state.last_updated_times[ticker] = None
                            status_text.text(f"✅ Datos de {ticker} actualizados.")
                            success_count += 1
                        else:
                            status_text.text(f"⚠️ No se pudieron obtener datos para {ticker}.")
                            error_count += 1
                            error_tickers.append(ticker)
                    except Exception as e:
                        status_text.text(f"❌ Error actualizando {ticker}: {str(e)[:100]}")
                        error_count += 1
                        error_tickers.append(ticker)
                    progress_bar.progress((i + 1) / len(all_tickers))
                
                # Mostrar resumen de la actualización
                status_text.text("Actualización completada.")
                if success_count > 0:
                    st.success(f"Se actualizaron correctamente {success_count} de {len(all_tickers)} tickers.")
                if error_count > 0:
                    error_container.warning(f"No se pudieron actualizar {error_count} tickers: {', '.join(error_tickers)}")
                    st.info("Prueba a actualizar los tickers problemáticos individualmente o verifica que sean símbolos válidos.")
            else:
                st.warning("No hay tickers en ninguna watchlist para actualizar.")

    # --- Main Area Logic ---

    # Load data if not already in session state (e.g., on first run or after script restart)
    if not st.session_state.all_data:
        with st.spinner("Cargando datos locales iniciales..."):
            # Validar tickers antes de cargar datos
            valid_tickers = [t.strip().upper() for t in tickers if len(t.strip()) > 0 and len(t.strip()) <= 10]
            invalid_tickers = [t for t in tickers if t.strip() not in valid_tickers and len(t.strip()) > 0]
            
            if invalid_tickers:
                st.warning(f"Los siguientes tickers tienen formato inválido: {', '.join(invalid_tickers)}")
            
            load_errors = []
            for ticker in valid_tickers:
                try:
                    if ticker not in st.session_state.all_data:
                        df_loaded = load_financials.load_financials(ticker)
                        if not df_loaded.empty:
                            df_processed = calculate_growth_metrics(df_loaded)
                            st.session_state.all_data[ticker] = df_processed
                            if 'last_updated' in df_loaded.columns and not df_loaded['last_updated'].isnull().all():
                                try:
                                    last_updated_series = pd.to_datetime(df_loaded['last_updated'], errors='coerce')
                                    if not last_updated_series.isnull().all():
                                        st.session_state.last_updated_times[ticker] = last_updated_series.max()
                                    else:
                                        st.session_state.last_updated_times[ticker] = None
                                except Exception:
                                    st.session_state.last_updated_times[ticker] = None
                            else:
                                st.session_state.last_updated_times[ticker] = None
                except Exception as e:
                    load_errors.append(ticker)
                    print(f"Error cargando datos para {ticker}: {str(e)}")
            
            if load_errors:
                st.info(f"No se pudieron cargar datos locales para: {', '.join(load_errors)}. Usa el botón 'Actualizar Datos'.")
            elif not st.session_state.all_data and valid_tickers:
                st.info(f"No hay datos locales para los tickers seleccionados. Usa el botón 'Actualizar Datos'.")


    # --- View Rendering --- 
    current_view = st.session_state.current_view
    chart_ticker = st.session_state.chart_ticker
    chart_type = st.session_state.chart_type

    if current_view == 'main':
        if not tickers:
            st.warning("Por favor, introduce uno o más tickers en la barra lateral.")
            return # Use return instead of st.stop() for cleaner flow

        # Display data for each ticker
        for ticker in tickers:
            if ticker in st.session_state.all_data:
                # Obtener el nombre completo de la empresa si está disponible
                df_ticker = st.session_state.all_data[ticker]
                company_name = df_ticker['company_name'].iloc[-1] if 'company_name' in df_ticker.columns and not df_ticker.empty else ticker
                
                # Filter data based on date range selector for display
                # Asegurar que start_date y end_date sean objetos datetime antes de convertirlos a Timestamp
                if isinstance(start_date, str):
                    start_date = pd.to_datetime(start_date, errors='coerce')
                if isinstance(end_date, str):
                    end_date = pd.to_datetime(end_date, errors='coerce')
                    
                start_date_ts = pd.Timestamp(start_date)
                end_date_ts = pd.Timestamp(end_date)
                
                # Asegurar que la columna 'date' sea de tipo datetime antes de comparar
                if 'date' in df_ticker.columns:
                    df_ticker['date'] = pd.to_datetime(df_ticker['date'], errors='coerce')
                    # Eliminar filas con fechas inválidas para evitar errores de comparación
                    df_ticker = df_ticker.dropna(subset=['date'])
                
                # Verificar que hay datos después de la conversión
                if df_ticker.empty:
                    df_filtered = pd.DataFrame(columns=df_ticker.columns)
                else:
                    # Realizar la comparación solo si hay datos válidos
                    df_filtered = df_ticker[(df_ticker['date'] >= start_date_ts) & (df_ticker['date'] <= end_date_ts)].copy()

                # Get the latest data row from the original (unfiltered) data for metrics
                latest_data = df_ticker.iloc[-1] if not df_ticker.empty else None
                
                # Verificar si hay alerta de caída consecutiva en EPS para mostrarla junto al ticker
                alert_icon = "⚠️ " if latest_data is not None and latest_data.get('eps_consecutive_decline', False) else ""
                # Formatear el nombre para mostrar como "[ALERTA] TICKER - Nombre Completo"
                display_name = f"{alert_icon}{ticker} - {company_name}"
                st.subheader(display_name)
                
                df_ticker = st.session_state.all_data[ticker]

                if df_filtered.empty:
                    st.info(f"No hay datos disponibles para {ticker} en el rango de fechas seleccionado.")
                    # Display last updated time even if no data in range
                    last_updated_str = "No disponible"
                    if ticker in st.session_state.last_updated_times and st.session_state.last_updated_times[ticker] is not None:
                        try:
                            last_updated_str = st.session_state.last_updated_times[ticker].strftime('%Y-%m-%d %H:%M:%S')
                        except AttributeError:
                            last_updated_str = str(st.session_state.last_updated_times[ticker])
                    st.caption(f"Última actualización local: {last_updated_str}")
                    continue # Skip to next ticker if no data in range

                # Get the latest data row from the original (unfiltered) data for metrics
                latest_data = df_ticker.iloc[-1] if not df_ticker.empty else None

                # --- Display Metrics & Chart Buttons --- 
                if latest_data is not None:
                    cols = st.columns(4)
                    with cols[0]:
                        st.metric(
                            label=f"Ingresos (Últ. Trim. {latest_data['date'].strftime('%Y-%m-%d')})",
                            value=format_large_number(latest_data['totalRevenue']),
                            delta=f"{latest_data['revenue_qoq']:.2f}% QoQ" if pd.notna(latest_data['revenue_qoq']) else None,
                            delta_color="normal" # Streamlit handles color based on sign
                        )
                        st.metric(
                            label="Ingresos (TTM)",
                            value=format_large_number(latest_data['ttmRevenue']),
                            delta=f"{latest_data['revenue_yoy']:.2f}% YoY" if pd.notna(latest_data['revenue_yoy']) else None,
                            delta_color="normal"
                        )
                        st.button("Ver Gráfico Ingresos", key=f"rev_chart_{ticker}", on_click=set_view, args=('chart', ticker, 'revenue'))

                    with cols[1]:
                        # Eliminamos la alerta de aquí ya que ahora se muestra junto al ticker
                            
                        st.metric(
                            label=f"EPS Diluido (Últ. Trim. {latest_data['date'].strftime('%Y-%m-%d')})",
                            value=f"{latest_data['epsDiluted']:.2f}" if pd.notna(latest_data['epsDiluted']) else "N/A",
                            delta=f"{latest_data['eps_qoq']:.2f}% QoQ" if pd.notna(latest_data['eps_qoq']) else None,
                            delta_color="normal"
                        )
                        st.metric(
                            label="EPS Diluido (TTM)",
                            value=f"{latest_data['ttmEpsDiluted']:.2f}" if pd.notna(latest_data['ttmEpsDiluted']) else "N/A",
                            delta=f"{latest_data['eps_yoy']:.2f}% YoY" if pd.notna(latest_data['eps_yoy']) else None,
                            delta_color="normal"
                        )
                        st.button("Ver Gráfico EPS", key=f"eps_chart_{ticker}", on_click=set_view, args=('chart', ticker, 'eps'))

                    with cols[2]:
                        rev_streak = latest_data['revenue_streak']
                        st.metric(
                            label="Racha Ingresos (QoQ)",
                            value=f"{int(rev_streak)} Trim." if pd.notna(rev_streak) and rev_streak != 0 else "-",
                            help="Trimestres consecutivos de crecimiento (+) o decrecimiento (-) QoQ."
                        )
                        if pd.notna(latest_data['revenue_ath']) and latest_data['revenue_ath']:
                             st.markdown("📈 **¡Ingresos ATH!**")

                    with cols[3]:
                        eps_streak = latest_data['eps_streak']
                        st.metric(
                            label="Racha EPS (QoQ)",
                            value=f"{int(eps_streak)} Trim." if pd.notna(eps_streak) and eps_streak != 0 else "-",
                            help="Trimestres consecutivos de crecimiento (+) o decrecimiento (-) QoQ."
                        )
                        # Mostrar trimestres sin alcanzar un máximo histórico de EPS
                        quarters_since_ath = latest_data['quarters_since_eps_ath']
                        st.metric(
                            label="Trimestres sin ATH de EPS",
                            value=f"{int(quarters_since_ath)} Trim." if pd.notna(quarters_since_ath) else "-",
                            help="Número de trimestres transcurridos desde el último máximo histórico de EPS."
                        )
                        if pd.notna(latest_data['eps_ath']) and latest_data['eps_ath']:
                             st.markdown("🏆 **¡EPS ATH!**")
                else:
                    st.info(f"No hay datos suficientes para mostrar métricas para {ticker}.")

                # Display last updated time
                last_updated_str = "No disponible"
                if ticker in st.session_state.last_updated_times and st.session_state.last_updated_times[ticker] is not None:
                     try:
                         last_updated_str = st.session_state.last_updated_times[ticker].strftime('%Y-%m-%d %H:%M:%S')
                     except AttributeError: # Handle cases where it might not be a datetime object
                         last_updated_str = str(st.session_state.last_updated_times[ticker])
                st.caption(f"Última actualización local: {last_updated_str}")

                # --- Display Detailed Data Table (Optional) ---
                with st.expander(f"Ver tabla de datos detallados para {ticker}"):
                    # Select and format columns for the table
                    cols_to_show = [
                        'date', 'totalRevenue', 'revenue_qoq', 'revenue_yoy', 'revenue_streak', 'revenue_ath',
                        'epsDiluted', 'eps_qoq', 'eps_yoy', 'eps_streak', 'eps_ath', 'quarters_since_eps_ath',
                        'ttmRevenue', 'ttmEpsDiluted'
                    ]
                    df_display = df_filtered[[col for col in cols_to_show if col in df_filtered.columns]].copy()

                    # Formatting
                    df_display['date'] = df_display['date'].dt.strftime('%Y-%m-%d')
                    for col in ['totalRevenue', 'ttmRevenue']:
                        if col in df_display.columns:
                            df_display[col] = df_display[col].apply(format_large_number)
                    for col in ['epsDiluted', 'ttmEpsDiluted']:
                         if col in df_display.columns:
                            df_display[col] = df_display[col].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
                    for col in ['revenue_qoq', 'revenue_yoy', 'eps_qoq', 'eps_yoy']:
                         if col in df_display.columns:
                            df_display[col] = df_display[col].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "N/A")
                    for col in ['revenue_streak', 'eps_streak']:
                         if col in df_display.columns:
                            df_display[col] = df_display[col].apply(lambda x: f"{int(x)}" if pd.notna(x) and x != 0 else "-")
                    for col in ['revenue_ath', 'eps_ath']:
                         if col in df_display.columns:
                            df_display[col] = df_display[col].apply(lambda x: "✅" if x else "")

                    # Rename columns for better readability
                    rename_map = {
                        'date': 'Fecha',
                        'totalRevenue': 'Ingresos Trim.',
                        'revenue_qoq': 'Ing. QoQ (%)',
                        'revenue_yoy': 'Ing. YoY (%)',
                        'revenue_streak': 'Racha Ing.',
                        'revenue_ath': 'Ing. ATH',
                        'epsDiluted': 'EPS Trim.',
                        'eps_qoq': 'EPS QoQ (%)',
                        'eps_yoy': 'EPS YoY (%)',
                        'eps_streak': 'Racha EPS',
                        'eps_ath': 'EPS ATH',
                        'ttmRevenue': 'Ingresos TTM',
                        'ttmEpsDiluted': 'EPS TTM'
                    }
                    df_display.rename(columns={k: v for k, v in rename_map.items() if k in df_display.columns}, inplace=True)

                    # Display the table (use st.dataframe for better interaction or st.table for static)
                    st.dataframe(df_display.set_index('Fecha').sort_index(ascending=False), use_container_width=True)

            else:
                st.info(f"No hay datos locales para {ticker}. Usa el botón 'Actualizar Datos' en la barra lateral.")

    elif current_view == 'chart':
        if chart_ticker and chart_ticker in st.session_state.all_data:
            df_ticker_chart = st.session_state.all_data[chart_ticker]
            # Filter data based on date range selector for the chart
            # Asegurar que start_date y end_date sean objetos datetime antes de convertirlos a Timestamp
            if isinstance(start_date, str):
                start_date = pd.to_datetime(start_date, errors='coerce')
            if isinstance(end_date, str):
                end_date = pd.to_datetime(end_date, errors='coerce')
                
            start_date_ts = pd.Timestamp(start_date)
            end_date_ts = pd.Timestamp(end_date)
            
            # Asegurar que la columna 'date' sea de tipo datetime antes de comparar
            if 'date' in df_ticker_chart.columns:
                df_ticker_chart['date'] = pd.to_datetime(df_ticker_chart['date'], errors='coerce')
                # Eliminar filas con fechas inválidas para evitar errores de comparación
                df_ticker_chart = df_ticker_chart.dropna(subset=['date'])
            
            # Verificar que hay datos después de la conversión
            if df_ticker_chart.empty:
                df_filtered_chart = pd.DataFrame(columns=df_ticker_chart.columns)
            else:
                # Realizar la comparación solo si hay datos válidos
                df_filtered_chart = df_ticker_chart[(df_ticker_chart['date'] >= start_date_ts) & (df_ticker_chart['date'] <= end_date_ts)].copy()

            if chart_type == 'revenue':
                display_revenue_chart(chart_ticker, df_filtered_chart)
            elif chart_type == 'eps':
                display_eps_chart(chart_ticker, df_filtered_chart)
            else:
                st.warning("Tipo de gráfico no reconocido.")

            # Add a button to go back
            if st.button("⬅️ Volver a la vista principal", key="back_to_main"): 
                set_view('main') # Call the callback to change the view
                st.rerun() # Rerun the script to reflect the view change

        else:
            st.error("No se pudo encontrar el ticker o los datos para mostrar el gráfico.")
            if st.button("⬅️ Volver", key="back_error"):
                set_view('main')
                st.rerun()

# Make sure the main_ui function is called if this script is run directly
# (Although it's typically imported and called from app.py)
if __name__ == "__main__":
    main_ui()