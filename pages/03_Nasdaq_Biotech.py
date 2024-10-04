import streamlit as st
import pandas as pd
from utils.display_util import get_cached_dataframe, display_news, make_clickable

# Set page configuration to wide mode
st.set_page_config(layout="wide")

# Streamlit app title
st.title("Nasdaq US Biotech Market News")

# Add a button to refresh the data
if st.button("Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# Get the cached dataframe
df = get_cached_dataframe('globenewswire_biotech')

# Function to create a clickable ticker
def make_ticker_clickable(ticker):
    return make_clickable(ticker, f"https://www.google.com/finance/quote/{ticker}:NASDAQ?hl=en")

# Apply the function to create clickable tickers
df['Ticker'] = df['Ticker'].apply(make_ticker_clickable)

# Remove '\n' from Summary
df['Summary'] = df['Summary'].str.replace('\n', '')

# Pagination
items_per_page = 25

# Add a select box for page selection
page = st.selectbox("Select Page", options=range(1, len(df) // items_per_page + 2))

# Display the news
total_pages = display_news(df, page, items_per_page)

# Display pagination information
st.write(f"Page {page} of {total_pages}")