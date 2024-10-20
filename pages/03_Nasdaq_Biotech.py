import streamlit as st
from utils.display.display_publisher import display_publisher

# Set page configuration to wide mode
st.set_page_config(layout="wide")

# Streamlit app title
st.title("Nasdaq US Biotech Market News")

# Add a button to refresh the data
if st.button("Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# Pagination
items_per_page = 25

# Add a select box for page selection
page = st.selectbox("Select Page", options=range(1, 101))  # Assuming max 100 pages

# Display the news
total_pages = display_publisher('globenewswire_biotech', page, items_per_page)

# Display pagination information
st.write(f"Page {page} of {total_pages}")
