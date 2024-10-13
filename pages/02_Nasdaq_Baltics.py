import streamlit as st
from utils.display_util import get_cached_dataframe, display_baltics

# Set page configuration to wide mode
st.set_page_config(layout="wide")

# Streamlit app title
st.title("NASDAQ Baltic Market News")

# Add a button to refresh the data
if st.button("Refresh Data"):
    st.cache_data.clear()
    st.rerun()

df = get_cached_dataframe('baltics')

# Pagination
items_per_page = 25

# Calculate total pages
total_pages = len(df) // items_per_page + (1 if len(df) % items_per_page > 0 else 0)

# Add a select box for page selection
page = st.selectbox("Select Page", options=range(1, total_pages + 1))

# Display the news using the new display_baltics function
actual_total_pages = display_baltics(df, page, items_per_page)

# Display pagination information
st.write(f"Page {page} of {actual_total_pages}")
