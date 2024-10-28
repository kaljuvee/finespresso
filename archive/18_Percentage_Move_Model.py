import streamlit as st
import pandas as pd
from utils.db.model_db_util import get_regression_results
from utils.display.display_model_util import display_regression_model_results

# Set page config to wide layout
st.set_page_config(layout="wide")

st.title("Finespresso - Why is it Moving?")
st.subheader("Percentage Move Prediction Model Results")    

results_df = get_regression_results()

# Sort the dataframe by timestamp in descending order
results_df_sorted = results_df.sort_values('timestamp', ascending=False)

# Get unique timestamps
timestamps = results_df_sorted['timestamp'].unique()

# Select the latest timestamp by default
selected_timestamp = st.selectbox("Select a Timestamp", options=timestamps)

filtered_df = results_df[results_df['timestamp'] == selected_timestamp]

# Use the display_regression_model_results function to show the full table
display_regression_model_results(filtered_df)

# Add markdown with link to Methodology page
st.markdown("""
---
To learn more about how these predictions are generated, please visit our [Methodology](/Methodology) page.
""")
