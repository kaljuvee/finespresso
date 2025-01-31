import streamlit as st
import pandas as pd
from datetime import datetime
from utils.db.news_db_util import get_news_df
import re
from utils.db.instrument_db_util import get_instrument_by_company_name
import logging

@st.cache_data(ttl=3600)
def get_cached_dataframe(publisher):
    return get_news_df(publisher)

def make_clickable(text, link):
    return f'<a target="_blank" href="{link}">{text}</a>'

def format_event(event):
    if event:
        words = event.split('_')
        return ' '.join(word.capitalize() for word in words)
    return event

def display_news(df, page, items_per_page):
    # Calculate start and end indices for the current page
    start_idx = (page - 1) * items_per_page
    end_idx = start_idx + items_per_page

    # Slice the dataframe for the current page
    df_page = df.iloc[start_idx:end_idx]

    # Create a copy of the dataframe to modify for display
    df_display = df_page.copy()

    # Make the Title clickable
    df_display['Title'] = df_display.apply(lambda row: make_clickable(row['Title'], row['Link']), axis=1)

    # Remove the Link column
    df_display = df_display.drop(columns=['Link'])

    # Format the Event column
    df_display['Event'] = df_display['Event'].apply(format_event)

    # Remove all types of newline characters from Reason
    df_display['Reason'] = df_display['Reason'].apply(lambda x: re.sub(r'\s+', ' ', x).strip() if x else '')

    # Display the table with left-aligned headers and custom CSS
    st.markdown("""
    <style>
    #news_table th {
        text-align: left;
    }
    </style>
    """, unsafe_allow_html=True)
    st.write(df_display.to_html(escape=False, index=False, classes=['dataframe'], table_id='news_table'), unsafe_allow_html=True)

    # Calculate total pages
    total_pages = len(df) // items_per_page + (1 if len(df) % items_per_page > 0 else 0)

    return total_pages

def format_ticker(ticker):
    """Create a clickable link to Yahoo Finance for a ticker."""
    if pd.notna(ticker) and ticker:  # Check if ticker is not NaN and not empty
        yf_url = f"https://finance.yahoo.com/quote/{ticker.strip()}"
        return f'<a target="_blank" href="{yf_url}">{ticker}</a>'
    return ticker

def format_baltics(df):
    logging.info(f"Input DataFrame columns: {df.columns.tolist()}")
    """Format the Baltic news dataframe for display."""
    df_display = df.copy()

    # Ensure column names are case-insensitive
    df_display.columns = df_display.columns.str.lower()

    # Make the Title clickable
    if 'title' in df_display.columns and 'link' in df_display.columns:
        df_display['title'] = df_display.apply(lambda row: make_clickable(row['title'], row['link']), axis=1)

    # Make the Ticker clickable with Yahoo Finance URL - using direct HTML
    if 'ticker' in df_display.columns:
        df_display['ticker'] = df_display['ticker'].apply(format_ticker)

    # Format the Event column
    if 'event' in df_display.columns:
        df_display['event'] = df_display['event'].apply(format_event)

    # Reorder and rename columns
    columns = ['ticker', 'company', 'title', 'published_date', 'event', 'reason', 'publisher']
    available_columns = [col for col in columns if col in df_display.columns]
    df_display = df_display[available_columns].rename(columns={
        'ticker': 'Ticker',
        'company': 'Company',
        'title': 'Title',
        'published_date': 'Date',
        'event': 'Event',
        'reason': 'Reason',
        'publisher': 'Publisher'
    })

    # Convert Date to datetime and format it
    if 'Date' in df_display.columns:
        df_display['Date'] = pd.to_datetime(df_display['Date']).dt.strftime('%Y-%m-%d %H:%M:%S')

    logging.info(f"Output DataFrame columns: {df_display.columns.tolist()}")
    return df_display

def display_baltics(df, page, items_per_page):
    # Format the dataframe
    df_display = format_baltics(df)

    # Calculate pagination
    total_items = len(df_display)
    total_pages = total_items // items_per_page + (1 if total_items % items_per_page > 0 else 0)
    page = min(page, total_pages)
    start_idx = (page - 1) * items_per_page
    end_idx = start_idx + items_per_page

    # Slice the dataframe for the current page
    df_page = df_display.iloc[start_idx:end_idx]

    # Display the table with enhanced custom CSS
    st.markdown("""
    <style>
    #news_table {
        width: 100%;
        border-collapse: collapse;
    }
    #news_table th {
        text-align: left;
        background-color: #f0f2f6;
        padding: 8px;
        border: 1px solid #ddd;
    }
    #news_table td {
        white-space: normal;
        word-wrap: break-word;
        padding: 8px;
        border: 1px solid #ddd;
        max-width: 300px;  /* Limit cell width */
    }
    #news_table tr:nth-child(even) {
        background-color: #f9f9f9;
    }
    #news_table tr:hover {
        background-color: #f5f5f5;
    }
    #news_table a {
        color: #0066cc;
        text-decoration: none;
    }
    #news_table a:hover {
        text-decoration: underline;
    }
    </style>
    """, unsafe_allow_html=True)
    
    st.write(df_page.to_html(
        escape=False,
        index=False,
        classes=['dataframe'],
        table_id='news_table',
        justify='left'
    ), unsafe_allow_html=True)

    return total_pages
