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
    return df

def display_news(df, page, items_per_page):
    total_pages = len(df) // items_per_page + (1 if len(df) % items_per_page > 0 else 0)

    start_idx = (page - 1) * items_per_page
    end_idx = start_idx + items_per_page

    columns_to_display = ['Title', 'Date', 'Company', 'Event', 'Why it Moves?']
    df_display = df[columns_to_display][start_idx:end_idx]

    for _, row in df_display.iterrows():
        html = row.to_frame().T.to_html(escape=False, index=False)
        html = html.replace('<th>', '<th style="text-align: left;">')
        st.write(html, unsafe_allow_html=True)
        st.write("---")  # Add a separator between items

    st.write(f"Page {page} of {total_pages}")
    st.write(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    return total_pages