import streamlit as st
from modules.ui import main_ui

if __name__ == "__main__":
    # Configure Streamlit page
    # st.set_page_config(page_title="Revenue & EPS Tracker", layout="wide") # Config moved to ui.py
    
    try:
        # Run the main UI function defined in modules/ui.py
        main_ui()
    except Exception as e:
        print(f"ERROR: An unhandled exception occurred: {e}")
        import traceback
        traceback.print_exc() # Print detailed traceback
        # Optionally, display the error in Streamlit if possible
        # st.error(f"An unexpected error occurred: {e}")

# Note: The command to run the app with specific server settings
# (streamlit run app.py --server.address=0.0.0.0 --server.port=8501)
# should be executed in the terminal, not placed inside the Python script.