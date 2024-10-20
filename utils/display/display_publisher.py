import streamlit as st
import pandas as pd
from utils.news_db_util import get_news_df
from utils.display_util import make_clickable

def display_publisher(publisher, page, items_per_page):
    # Get the dataframe for the specific publisher
    df = get_news_df(publisher)

    # Filter out rows where any of the specified fields are null
    #df = df.dropna(subset=['ticker', 'title', 'company', 'predicted_move', 'predicted_side', 'ticker_url', 'event', 'published_date'])

    # Sort the dataframe by published_date in descending order
    df = df.sort_values('published_date', ascending=False)

    # Create clickable links for ticker and title
    df['Ticker'] = df.apply(lambda row: make_clickable(row['ticker'], row['ticker_url']), axis=1)
    df['Title'] = df.apply(lambda row: make_clickable(row['title'], row['link']), axis=1)

    # Select columns to display
    columns_to_display = ['Ticker', 'Title', 'company', 'predicted_move', 'predicted_side', 'event', 'published_date']
    df_display = df[columns_to_display]

    # Rename columns to start with capital letter and replace underscore with space
    df_display.columns = [col.replace('_', ' ').title() for col in df_display.columns]

    # Calculate start and end indices for the current page
    start_idx = (page - 1) * items_per_page
    end_idx = start_idx + items_per_page

    # Display the dataframe for the current page
    st.write(df_display.iloc[start_idx:end_idx].to_html(escape=False, index=False), unsafe_allow_html=True)

    # Calculate total pages
    total_pages = len(df_display) // items_per_page + (1 if len(df_display) % items_per_page > 0 else 0)

    return total_pages
