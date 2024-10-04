import streamlit as st
from utils.display_util import get_cached_dataframe, display_news

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

# Add a select box for page selection
page = st.selectbox("Select Page", options=range(1, len(df) // items_per_page + 2))

# Display the news
total_pages = display_news(df, page, items_per_page)