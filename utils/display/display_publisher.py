import streamlit as st
import pandas as pd
from utils.db.news_db_util import get_news_df
from utils.display.display_util import make_clickable
from datetime import timedelta

@st.cache_data(ttl=timedelta(hours=1))
def cached_get_news_df(publisher):
    return get_news_df(publisher)

def display_publisher(publisher, page, items_per_page):
    # Get the cached dataframe for the specific publisher
    df = cached_get_news_df(publisher)

    # Sort the dataframe by published_date in descending order
    df = df.sort_values('published_date', ascending=False)

    # Create clickable links for ticker and title
    df['Ticker'] = df.apply(lambda row: make_clickable(row['ticker'], row['ticker_url']), axis=1)
    df['Title'] = df.apply(lambda row: make_clickable(row['title'], row['link']), axis=1)

    # Format the event column: remove underscores and capitalize the first word
    # Handle None/null values safely
    df['event'] = df['event'].apply(lambda x: x.replace('_', ' ').capitalize() if isinstance(x, str) else '')

    # Select columns to display, including the 'reason' column
    columns_to_display = ['Ticker', 'Title', 'company', 'predicted_move', 'predicted_side', 'event', 'reason', 'published_date']
    df_display = df[columns_to_display]

    # Rename columns to start with capital letter and replace underscore with space
    df_display.columns = [col.replace('_', ' ').title() for col in df_display.columns]

    # Calculate start and end indices for the current page
    start_idx = (page - 1) * items_per_page
    end_idx = start_idx + items_per_page

    # Create a styled DataFrame with left-aligned headers
    styled_df = df_display.iloc[start_idx:end_idx].style.set_table_styles([
        {'selector': 'th', 'props': [('text-align', 'left')]}
    ])

    # Display the styled dataframe for the current page without index
    st.write(styled_df.hide(axis="index").to_html(escape=False), unsafe_allow_html=True)

    # Calculate total pages
    total_pages = len(df_display) // items_per_page + (1 if len(df_display) % items_per_page > 0 else 0)

    return total_pages
