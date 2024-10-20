import streamlit as st
import pandas as pd
from utils.db.model_db_util import get_results
from utils.display.display_model_util import display_model_results

# Set page config to wide layout
st.set_page_config(layout="wide")

st.title("Macro Hive Morning Sales Copilot")
st.subheader("Direction Prediction Model Results")

results_df = get_results()

st.write("Filter by Run ID:")
selected_run_id = st.selectbox("Select a Run ID", options=["All"] + list(results_df['run_id'].unique()))

if selected_run_id != "All":
    filtered_df = results_df[results_df['run_id'] == selected_run_id]
else:
    filtered_df = results_df

# Use the display_model_results function to show the full table
display_model_results()

# Add markdown with link to Methodology page
st.markdown("""
---
To understand the methodology behind these direction predictions, please check our [Methodology](/Methodology) page.
""")
