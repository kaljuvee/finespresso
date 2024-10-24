import streamlit as st
import pandas as pd
import numpy as np
from utils.db.news_db_util import get_news_df
from utils.display.display_util import make_clickable
from datetime import timedelta
from utils.db.model_db_util import get_accuracy

@st.cache_data(ttl=timedelta(hours=1))
def cached_get_news_df(publisher):
    return get_news_df(publisher)

def format_percentage(value):
    if pd.isna(value):
        return ""
    color = "green" if value > 0 else "red"
    return f'<span style="color: {color}">{value:.2f}%</span>'

def display_publisher(publisher, page, items_per_page):
    # Get the cached dataframe for the specific publisher
    df = cached_get_news_df(publisher)

    # Sort the dataframe by published_date in descending order
    df = df.sort_values('published_date', ascending=False)

    # Create clickable links for ticker and title
    df['Ticker'] = df.apply(lambda row: make_clickable(row['ticker'], row['ticker_url']), axis=1)
    df['Title'] = df.apply(lambda row: make_clickable(row['title'], row['link']), axis=1)

    # Format the event column: remove underscores and capitalize the first word
    df['Probability % (Direction)'] = df['event'].apply(lambda x: get_accuracy(x))
    df['Probability % (Direction)'] = df['Probability % (Direction)'].apply(
        lambda x: f'{x*100:.2f}%' if pd.notnull(x) and not np.isnan(x) else ''
    )
    # Handle None/null values safely
    df['event'] = df['event'].apply(lambda x: x.replace('_', ' ').capitalize() if isinstance(x, str) else '')

    # Multiply predicted_move by 100 and format as percentage
    df['Expected Move (%)'] = df['predicted_move'].apply(lambda x: x * 100 if pd.notnull(x) else x)
    df['Expected Move (%)'] = df['Expected Move (%)'].apply(format_percentage)

    # Add new column for Probability % (Direction)

    # Select columns to display, excluding 'predicted_side'
    columns_to_display = ['Ticker', 'Title', 'company', 'Expected Move (%)', 'Probability % (Direction)', 'event', 'reason', 'published_date']
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
