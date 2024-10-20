import streamlit as st
import pandas as pd
from utils.model_db_util import get_regression_results
from utils.display_model_util import display_regression_model_results

# Set page config to wide layout
st.set_page_config(layout="wide")

st.title("Macro Hive Morning Sales Copilot")
st.subheader("Percentage Move Prediction Model Results")    

results_df = get_regression_results()

st.write("Filter by Run ID:")
selected_run_id = st.selectbox("Select a Run ID", options=["All"] + list(results_df['run_id'].unique()))

if selected_run_id != "All":
    filtered_df = results_df[results_df['run_id'] == selected_run_id]
else:
    filtered_df = results_df

# Use the display_regression_model_results function to show the full table
display_regression_model_results(filtered_df)

# Add markdown with link to Methodology page
st.markdown("""
---
To learn more about how these predictions are generated, please visit our [Methodology](/Methodology) page.
""")
