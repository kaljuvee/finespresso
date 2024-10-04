import streamlit as st
import pandas as pd
from utils.display_util import get_cached_dataframe, display_news, make_clickable

# Streamlit app title
st.title("NASDAQ Biotech Market News")

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

# Pagination
items_per_page = 25

# Add a select box for page selection
page = st.selectbox("Select Page", options=range(1, len(df) // items_per_page + 2))

# Display the news
total_pages = display_news(df, page, items_per_page)