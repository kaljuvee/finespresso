import streamlit as st
import pandas as pd
from datetime import datetime
from utils.db_util import get_news_df

def make_clickable(title, link):
    return f'<a target="_blank" href="{link}">{title}</a>'

def format_event(event):
    if event:
        words = event.split('_')
        return ' '.join(word.capitalize() for word in words)
    return event

@st.cache_data(ttl=3600)  # Cache for 1 hour
def get_cached_dataframe(publisher):
    df = get_news_df(publisher)
    df['Title'] = df.apply(lambda x: make_clickable(x['Title'], x['Link']), axis=1)
    df['Event'] = df['Event'].apply(format_event)
    
    # Add an empty 'Summary' column if it doesn't exist
    if 'Summary' not in df.columns:
        df['Summary'] = ''
    
    return df

def display_news(df, page, items_per_page):
    total_pages = len(df) // items_per_page + (1 if len(df) % items_per_page > 0 else 0)

    start_idx = (page - 1) * items_per_page
    end_idx = start_idx + items_per_page

    columns_to_display = ['Ticker', 'Title', 'Date', 'Company', 'Event', 'Summary']
    
    # Only include columns that exist in the DataFrame
    existing_columns = [col for col in columns_to_display if col in df.columns]
    df_display = df[existing_columns][start_idx:end_idx]

    # Convert the entire DataFrame to HTML
    html = df_display.to_html(escape=False, index=False)
    
    # Modify the table style
    html = html.replace('<table', '<table style="width:100%; border-collapse: collapse;"')
    html = html.replace('<th>', '<th style="text-align: left; padding: 8px; border-bottom: 1px solid #ddd;">')
    html = html.replace('<td>', '<td style="text-align: left; padding: 8px; border-bottom: 1px solid #ddd;">')

    # Display the table
    st.write(html, unsafe_allow_html=True)

    st.write(f"Page {page} of {total_pages}")
    st.write(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    return total_pages