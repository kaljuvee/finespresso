import streamlit as st
from utils.display.display_publisher import display_publisher

# Set page configuration to wide mode
st.set_page_config(layout="wide")

# Streamlit app title
st.title("NASDAQ Baltic Market News")

# Add a button to refresh the data
if st.button("Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# Pagination
items_per_page = 25

# Add a select box for page selection
page = st.selectbox("Select Page", options=range(1, 101))  # Assuming max 100 pages

# Cached function to display news and get total pages
@st.cache_data(ttl=3600)  # Cache for 1 hour
def cached_display_publisher(publisher, page, items_per_page):
    return display_publisher(publisher, page, items_per_page)

# Display the news and get total pages
total_pages = cached_display_publisher('baltics', page, items_per_page)

# Display pagination information
st.write(f"Page {page} of {total_pages}")
