import streamlit as st
import pandas as pd
from datetime import datetime
from archive.db_util import get_news_df
import re
from utils.instrument_db_util import get_instrument_by_company_name

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

    # Remove all types of newline characters from Summary and create expandable widgets
    df_display['Summary'] = df_display['Summary'].apply(lambda x: re.sub(r'\s+', ' ', x).strip())
    df_display['Summary'] = df_display.apply(lambda row: create_expandable_summary(row['Summary']), axis=1)

    # Display the table with left-aligned headers and custom CSS
    st.markdown("""
    <style>
    #news_table th {
        text-align: left;
    }
    .summary-expander {
        display: inline-block;
        width: 100%;
    }
    .summary-expander summary {
        cursor: pointer;
        list-style: none;
    }
    .summary-expander summary::-webkit-details-marker {
        display: none;
    }
    .summary-expander p {
        margin: 0;
        padding: 5px 0;
        white-space: normal;
        word-wrap: break-word;
    }
    </style>
    """, unsafe_allow_html=True)
    st.write(df_display.to_html(escape=False, index=False, classes=['dataframe'], table_id='news_table'), unsafe_allow_html=True)

    # Calculate total pages
    total_pages = len(df) // items_per_page + (1 if len(df) % items_per_page > 0 else 0)

    return total_pages

def create_expandable_summary(summary):
    # Create a unique key for each expander
    key = f"summary_{hash(summary)}"
    
    # Create the HTML for the expander with custom class
    expander_html = f"""
    <details class="summary-expander">
        <summary>▼ See summary</summary>
        <p>{summary}</p>
    </details>
    """
    
    return expander_html

def format_baltics(df):
    """Format the Baltic news dataframe for display."""
    df_display = df.copy()

    # Ensure column names are case-insensitive
    df_display.columns = df_display.columns.str.lower()

    # Make the Title clickable
    if 'title' in df_display.columns and 'link' in df_display.columns:
        df_display['title'] = df_display.apply(lambda row: make_clickable(row['title'], row['link']), axis=1)

    # Make the Ticker clickable
    if 'ticker' in df_display.columns and 'ticker_url' in df_display.columns:
        df_display['ticker'] = df_display.apply(lambda row: make_clickable(row['ticker'], row['ticker_url']) if row['ticker'] and row['ticker_url'] else row['ticker'], axis=1)

    # Format the Event column
    if 'ai_topic' in df_display.columns:
        df_display['ai_topic'] = df_display['ai_topic'].apply(format_event)

    # Create expandable summaries
    if 'ai_summary' in df_display.columns:
        df_display['ai_summary'] = df_display['ai_summary'].apply(lambda x: create_expandable_summary(x) if x else '')

    # Reorder and rename columns
    columns = ['ticker', 'company', 'title', 'published_date', 'ai_topic', 'ai_summary', 'publisher']
    available_columns = [col for col in columns if col in df_display.columns]
    df_display = df_display[available_columns].rename(columns={
        'ticker': 'Ticker',
        'company': 'Company',
        'title': 'Title',
        'published_date': 'Date',
        'ai_topic': 'Event',
        'ai_summary': 'Summary',
        'publisher': 'Publisher'
    })

    # Convert Date to datetime and format it
    if 'Date' in df_display.columns:
        df_display['Date'] = pd.to_datetime(df_display['Date']).dt.strftime('%Y-%m-%d %H:%M:%S')

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

    # Display the table with custom CSS
    st.markdown("""
    <style>
    #news_table th { text-align: left; }
    #news_table td { white-space: normal; word-wrap: break-word; }
    .summary-expander {
        display: inline-block;
        width: 100%;
    }
    .summary-expander summary {
        cursor: pointer;
        list-style: none;
    }
    .summary-expander summary::-webkit-details-marker {
        display: none;
    }
    .summary-expander p {
        margin: 0;
        padding: 5px 0;
        white-space: normal;
        word-wrap: break-word;
    }
    </style>
    """, unsafe_allow_html=True)
    st.write(df_page.to_html(escape=False, index=False, classes=['dataframe'], table_id='news_table'), unsafe_allow_html=True)

    return total_pages
